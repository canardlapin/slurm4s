package io.github.bbuchsbaum.scalaslurm.worker

import cats.effect.kernel.Sync
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest

import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.UUID
import scala.util.control.NonFatal

/** The shared atomic-file kernel: secure temporary staging, fsync-forced writes, atomic
  * publication, and an atomic claim-by-rename primitive.
  *
  * This object reconciles the previously duplicated write/publish logic (result publication, output
  * staging, launch-artifact staging) behind one audited implementation. Every write goes through a
  * sibling temporary file (`.{name}.tmp-{uuid}`, `CREATE_NEW`, private permissions), is forced to
  * stable storage (`FileChannel.force(true)`) before rename, and reaches its final name only via
  * `ATOMIC_MOVE`. There is never a fallback to copy-and-delete: a filesystem that cannot rename
  * atomically fails with a typed [[AtomicFiles.WriteFailure.AtomicMoveUnavailable]] (or
  * [[AtomicFiles.ClaimFailure.AtomicMoveUnavailable]]) so the caller can refuse the site rather
  * than silently weaken its guarantees.
  *
  * Every operation exists in two forms: a total `...Blocking` function (never throws, returns a
  * typed `Either`; for callers already executing on a blocking context) and an `F[_]: Sync` wrapper
  * that runs it via `Sync[F].blocking`.
  *
  * Crash-window contract (all operations): a crash before the rename leaves at most an orphaned
  * private temporary file, never a partial target; a crash after the rename leaves the complete
  * target. The rename itself is durable once the parent directory's metadata reaches stable storage
  * — this kernel forces file *contents* before rename but, matching the established publication
  * behavior, does not fsync the parent directory; recovery layers must tolerate a completed
  * operation disappearing across a power loss in the narrow window before the directory entry is
  * journaled by the filesystem.
  *
  * In-process concurrency: operations here provide cross-process exclusion only where documented
  * ([[AtomicFiles.publishOnce]] via an advisory sidecar lock, [[AtomicFiles.claim]] via rename's
  * source-side exclusivity). Callers racing the same target from within one JVM must serialize
  * per-target themselves (the established pattern is a per-target monitor or `Semaphore[F](1)`),
  * because overlapping advisory locks within a JVM raise rather than block.
  */
object AtomicFiles:
  /** Why an atomic write/publication could not be completed. */
  enum WriteFailure derives CanEqual:
    /** The target already exists and the operation is write-once. */
    case TargetExists(path: String)

    /** The target exists with different content (or is not a regular file) and the operation is
      * idempotent-stable, so proceeding would change already-published bytes.
      */
    case TargetConflict(path: String, detail: String)

    /** The filesystem cannot rename atomically; the operation was not performed. */
    case AtomicMoveUnavailable(path: String)

    /** A bounded I/O fault, with the observed detail. */
    case Io(detail: String)

  /** Why an atomic claim could not be completed. */
  enum ClaimFailure derives CanEqual:
    /** The claim destination already exists. Because destinations are claimant-private, this means
      * the caller (possibly a previous incarnation of itself) already claimed this entry; restart
      * handling treats it as an idempotent success signal, never as another contender.
      */
    case AlreadyClaimed(to: String)

    /** The source no longer exists — for contended queues this is the losing side of a race
      * (another contender renamed it first); for uncontended paths, a missing entry. `rename(2)`
      * cannot distinguish the two; the caller's protocol gives it meaning.
      */
    case SourceMissing(from: String)

    /** The source exists but is not a regular non-symlink file; claiming it would follow a link
      * outside the claim root.
      */
    case SourceNotRegular(from: String)

    /** The filesystem cannot rename atomically; the claim was not performed. */
    case AtomicMoveUnavailable(from: String)

    /** A bounded I/O fault, with the observed detail. */
    case Io(detail: String)

  /** Write `bytes` to a target that must not yet exist. Fsync-forced, atomic, private permissions;
    * `executable` selects `rwx------` for the published file. Fails with
    * [[WriteFailure.TargetExists]] when the target is already present (checked before the rename;
    * single-writer contexts only — for cross-process write-once use [[publishOnce]]).
    */
  def writeNewBlocking(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): Either[WriteFailure, Unit] =
    try
      if Files.exists(target, LinkOption.NOFOLLOW_LINKS) then
        Left(WriteFailure.TargetExists(target.toString))
      else stageAndMove(target, bytes, executable, replaceExisting = false)
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Effect wrapper for [[writeNewBlocking]]. */
  def writeNew[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): F[Either[WriteFailure, Unit]] =
    Sync[F].blocking(writeNewBlocking(target, bytes, executable))

  /** Idempotently write `bytes`: an absent target is written atomically; a present target with the
    * same bytes is success; a present target with different bytes (or a non-regular file) is a
    * typed [[WriteFailure.TargetConflict]]. This is the launch-artifact staging semantic.
    */
  def writeStableBlocking(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): Either[WriteFailure, Unit] =
    try
      if Files.exists(target, LinkOption.NOFOLLOW_LINKS) then
        if !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
          Left(WriteFailure.TargetConflict(target.toString, "existing entry is not a regular file"))
        else if readAtMost(target, bytes.size + 1) == bytes then Right(())
        else
          Left(WriteFailure.TargetConflict(target.toString, "existing entry has different bytes"))
      else stageAndMove(target, bytes, executable, replaceExisting = false)
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Effect wrapper for [[writeStableBlocking]]. */
  def writeStable[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): F[Either[WriteFailure, Unit]] =
    Sync[F].blocking(writeStableBlocking(target, bytes, executable))

  /** Atomically replace the target with `bytes` (creating it if absent). Readers observe either the
    * previous complete content or the new complete content, never a torn write. This is the
    * latest-value-file semantic (heartbeats, cursors).
    */
  def replaceBlocking(target: Path, bytes: Vector[Byte]): Either[WriteFailure, Unit] =
    try stageAndMove(target, bytes, executable = false, replaceExisting = true)
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Effect wrapper for [[replaceBlocking]]. */
  def replace[F[_]: Sync](target: Path, bytes: Vector[Byte]): F[Either[WriteFailure, Unit]] =
    Sync[F].blocking(replaceBlocking(target, bytes))

  /** Cross-process write-once publication: exactly one publisher of a given target succeeds,
    * enforced by an advisory sidecar lock (`.{name}.publish.lock`) held across the
    * exists-check-then-rename window (JDK `ATOMIC_MOVE` may silently replace an existing target, so
    * the check must be lock-guarded). Returns the published bytes' `sha256:` digest. Concurrent
    * publishes of the same target from within one JVM must be serialized by the caller (overlapping
    * advisory locks raise, reported as [[WriteFailure.Io]]).
    */
  def publishOnceBlocking(
      target: Path,
      bytes: Vector[Byte]
  ): Either[WriteFailure, ContentDigest] =
    try
      val normalized = target.toAbsolutePath.normalize()
      Option(normalized.getParent) match
        case None         => Left(WriteFailure.Io("publication target must have a parent"))
        case Some(parent) =>
          Files.createDirectories(parent)
          val lockPath = parent.resolve(s".${normalized.getFileName}.publish.lock")
          val lockChannel = FileChannel.open(
            lockPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
          )
          setPrivate(lockPath, executable = false)
          try
            val lock = lockChannel.lock()
            try
              if Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) then
                Left(WriteFailure.TargetExists(normalized.toString))
              else
                stageAndMove(normalized, bytes, executable = false, replaceExisting = false)
                  .map(_ => digestOf(bytes))
            finally lock.release()
          finally lockChannel.close()
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Effect wrapper for [[publishOnceBlocking]]. */
  def publishOnce[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte]
  ): F[Either[WriteFailure, ContentDigest]] =
    Sync[F].blocking(publishOnceBlocking(target, bytes))

  /** Atomically claim `from` by renaming it to `to`: of any number of contenders renaming the same
    * source, exactly one succeeds; every loser observes [[ClaimFailure.SourceMissing]]. The
    * destination's parent directory is created (privately) if absent. The destination must be
    * claimant-private: an existing destination is reported as [[ClaimFailure.AlreadyClaimed]]
    * (checked before the rename — contenders never share a destination, so the only writer that can
    * race this check is a previous incarnation of the claimant itself). The source must be a
    * regular non-symlink file. No copy/delete fallback exists: cross-filesystem claims fail with
    * [[ClaimFailure.AtomicMoveUnavailable]].
    */
  def claimBlocking(from: Path, to: Path): Either[ClaimFailure, Path] =
    try
      if !Files.exists(from, LinkOption.NOFOLLOW_LINKS) then
        Left(ClaimFailure.SourceMissing(from.toString))
      else if !Files.isRegularFile(from, LinkOption.NOFOLLOW_LINKS) then
        Left(ClaimFailure.SourceNotRegular(from.toString))
      else if Files.exists(to, LinkOption.NOFOLLOW_LINKS) then
        Left(ClaimFailure.AlreadyClaimed(to.toString))
      else
        Option(to.getParent).foreach { parent =>
          Files.createDirectories(parent)
          setPrivate(parent, executable = true)
        }
        Right(Files.move(from, to, StandardCopyOption.ATOMIC_MOVE))
    catch
      case _: NoSuchFileException             => Left(ClaimFailure.SourceMissing(from.toString))
      case _: FileAlreadyExistsException      => Left(ClaimFailure.AlreadyClaimed(to.toString))
      case _: AtomicMoveNotSupportedException =>
        Left(ClaimFailure.AtomicMoveUnavailable(from.toString))
      case NonFatal(error) =>
        Left(ClaimFailure.Io(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  /** Effect wrapper for [[claimBlocking]]. */
  def claim[F[_]: Sync](from: Path, to: Path): F[Either[ClaimFailure, Path]] =
    Sync[F].blocking(claimBlocking(from, to))

  /** The `sha256:` content digest of `bytes` — the canonical digest form for published files. */
  def digestOf(bytes: Vector[Byte]): ContentDigest =
    val hex = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    ContentDigest
      .from(s"sha256:$hex")
      .fold(problem => throw new IllegalStateException(problem.reason), identity)

  private def stageAndMove(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean,
      replaceExisting: Boolean
  ): Either[WriteFailure, Unit] =
    val temporary = target.resolveSibling(s".${target.getFileName}.tmp-${UUID.randomUUID()}")
    try
      val channel = FileChannel.open(
        temporary,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
      )
      setPrivate(temporary, executable)
      try
        val buffer = java.nio.ByteBuffer.wrap(bytes.toArray)
        while buffer.hasRemaining do
          val _ = channel.write(buffer)
        channel.force(true)
      finally channel.close()
      try
        val _ =
          if replaceExisting then
            Files.move(
              temporary,
              target,
              StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING
            )
          else Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE)
        Right(())
      catch
        case _: AtomicMoveNotSupportedException =>
          Left(WriteFailure.AtomicMoveUnavailable(target.toString))
        case _: FileAlreadyExistsException =>
          Left(WriteFailure.TargetExists(target.toString))
    finally
      val _ = Files.deleteIfExists(temporary)

  private def readAtMost(path: Path, maximum: Int): Vector[Byte] =
    val input = Files.newInputStream(path, StandardOpenOption.READ)
    try
      val output = new java.io.ByteArrayOutputStream()
      val buffer = new Array[Byte](8192)
      var total = 0
      var count = input.read(buffer)
      while count >= 0 && total <= maximum do
        output.write(buffer, 0, count)
        total += count
        count = input.read(buffer)
      output.toByteArray.toVector
    finally input.close()

  private def setPrivate(path: Path, executable: Boolean): Unit =
    try
      val permissions =
        if Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) then "rwx------"
        else if executable then "rwx------"
        else "rw-------"
      val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(permissions))
    catch case _: UnsupportedOperationException => ()

  private def writeFailureOf(error: Throwable, target: Path): WriteFailure = error match
    case _: AtomicMoveNotSupportedException =>
      WriteFailure.AtomicMoveUnavailable(target.toString)
    case _: FileAlreadyExistsException => WriteFailure.TargetExists(target.toString)
    case other                         =>
      WriteFailure.Io(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))
