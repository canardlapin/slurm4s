package io.github.bbuchsbaum.remoteexec.kernel

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

/** Scheduler-neutral atomic filesystem operations shared by launch staging, result publication,
  * stores, and filesystem spools.
  *
  * Every write stages into a private sibling temporary opened with `CREATE_NEW`, forces its
  * contents, and publishes only with `ATOMIC_MOVE`. There is no copy/delete fallback; unsupported
  * atomic rename is a typed failure so higher layers can refuse a filesystem whose guarantees are
  * too weak.
  *
  * ==Three separate guarantees==
  *
  * These were previously described as one. They have different implementations, different tests,
  * and different platform support, and a caller usually needs only some of them:
  *
  *   1. '''Atomic visibility.''' A reader sees either no file or the complete file, never a partial
  *      one. Provided by `rename(2)` on every supported filesystem. Unconditional.
  *   1. '''Single publisher / no replace.''' At most one writer succeeds for a destination.
  *      Provided by the sidecar lock, because the exists-check/move sequence is not itself atomic —
  *      verified across real processes by `AtomicCrossProcessSuite`, which observed two and three
  *      simultaneous "winners" before the lock was applied to every path.
  *   1. '''Crash durability.''' The published file survives power loss. File contents are forced
  *      before the rename, and [[forceDirectory]] attempts to force the containing directory after
  *      it. That second force is best-effort: opening a directory as a channel is not portable, and
  *      where the platform refuses it (notably macOS) the rename itself may not be durable even
  *      though the bytes are.
  *
  * ==Filesystem qualification==
  *
  * Verified on local POSIX filesystems (APFS, ext4). '''Not''' verified on NFS or Lustre: advisory
  * `FileLock` semantics over NFS depend on server and mount options, and cross-client rename
  * atomicity is weaker than local `rename(2)`. A consumer that needs guarantee 2 or 3 on a network
  * filesystem must establish it for that deployment rather than assume it here.
  *
  * The kernel owns mechanics only. It contains no scheduler, worker, queue, lease, retry, or spool
  * policy — and no effect type. Every operation here is a blocking call returning a value; wrapping
  * it in `F` is the caller's business. ADR 0001 confines Cats Effect to interpreter and application
  * modules, and `Sync`-shaped convenience wrappers were the sole reason this artifact depended on
  * it. Callers use `Sync[F].blocking(AtomicFiles.…Blocking(…))`.
  */
object AtomicFiles:
  private val localLockStripes: Array[AnyRef] = Array.fill(64)(new AnyRef)

  enum WriteFailure derives CanEqual:
    case TargetExists(path: String)
    case TargetConflict(path: String, detail: String)
    case AtomicMoveUnavailable(path: String)
    case Io(detail: String)

  enum ClaimFailure derives CanEqual:
    case AlreadyClaimed(to: String)
    case SourceMissing(from: String)
    case SourceNotRegular(from: String)
    case AtomicMoveUnavailable(from: String)
    case Io(detail: String)

  def writeNewBlocking(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): Either[WriteFailure, Unit] =
    try
      withTargetLock(target, WriteFailure.Io.apply) { normalized =>
        if Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) then
          Left(WriteFailure.TargetExists(normalized.toString))
        else stageAndMove(normalized, bytes, executable, replaceExisting = false)
      }
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Idempotently publish stable bytes.
    *
    * The post-move collision check is essential: two concurrent identical writers both succeed
    * logically, while concurrent different writers yield one success and one `TargetConflict`.
    */
  def writeStableBlocking(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): Either[WriteFailure, Unit] =
    try
      withTargetLock(target, WriteFailure.Io.apply) { normalized =>
        if Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) then
          validateExisting(normalized, bytes)
        else stageAndMove(normalized, bytes, executable, replaceExisting = false)
      }
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  def replaceBlocking(target: Path, bytes: Vector[Byte]): Either[WriteFailure, Unit] =
    try stageAndMove(target, bytes, executable = false, replaceExisting = true)
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Publish exactly once across processes.
    *
    * The sidecar lock closes the exists-check/move race because the JDK permits `ATOMIC_MOVE` to
    * replace an existing target on some providers. A striped process-local monitor also prevents
    * overlapping advisory locks when callers race within one JVM.
    */
  def publishOnceBlocking(
      target: Path,
      bytes: Vector[Byte]
  ): Either[WriteFailure, ContentDigest] =
    try
      withTargetLock(target, WriteFailure.Io.apply) { normalized =>
        if Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) then
          Left(WriteFailure.TargetExists(normalized.toString))
        else
          stageAndMove(normalized, bytes, executable = false, replaceExisting = false)
            .map(_ => digestOf(bytes))
      }
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  /** Atomically move a regular non-symlink source into a claimant-private destination. */
  def claimBlocking(from: Path, to: Path): Either[ClaimFailure, Path] =
    try
      if !Files.exists(from, LinkOption.NOFOLLOW_LINKS) then
        Left(ClaimFailure.SourceMissing(from.toString))
      else if !Files.isRegularFile(from, LinkOption.NOFOLLOW_LINKS) then
        // Another contender may have moved the source between the existence and type checks.
        // Preserve the claim protocol's loser classification instead of fabricating corruption.
        if !Files.exists(from, LinkOption.NOFOLLOW_LINKS) then
          Left(ClaimFailure.SourceMissing(from.toString))
        else Left(ClaimFailure.SourceNotRegular(from.toString))
      else
        withTargetLock(to, ClaimFailure.Io.apply) { destination =>
          if Files.exists(destination, LinkOption.NOFOLLOW_LINKS) then
            Left(ClaimFailure.AlreadyClaimed(destination.toString))
          else
            Option(destination.getParent).foreach(parent => setPrivate(parent, executable = true))
            Right(Files.move(from, destination, StandardCopyOption.ATOMIC_MOVE))
        }
    catch
      case _: NoSuchFileException             => Left(ClaimFailure.SourceMissing(from.toString))
      case _: FileAlreadyExistsException      => Left(ClaimFailure.AlreadyClaimed(to.toString))
      case _: AtomicMoveNotSupportedException =>
        Left(ClaimFailure.AtomicMoveUnavailable(from.toString))
      case NonFatal(error) =>
        Left(ClaimFailure.Io(safeMessage(error)))

  /** True for files this object creates as publication machinery rather than content.
    *
    * The sidecar lock must live beside its target on the shared filesystem — that is what lets it
    * serialize writers on different nodes — so it necessarily appears inside directories that
    * consumers also enumerate. Callers that scan a directory must skip these rather than mistake
    * them for content; the naming rule is stated here, once, instead of as a magic string in every
    * consumer.
    */
  def isInfrastructure(path: Path): Boolean =
    Option(path.getFileName).map(_.toString).exists { name =>
      name.startsWith(".") && (name.endsWith(".atomic.lock") || name.contains(".tmp-"))
    }

  def digestOf(bytes: Vector[Byte]): ContentDigest =
    val hex = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    ContentDigest.unsafeFrom(s"sha256:$hex")

  private def validateExisting(
      target: Path,
      expected: Vector[Byte]
  ): Either[WriteFailure, Unit] =
    if !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
      Left(WriteFailure.TargetConflict(target.toString, "existing entry is not a regular file"))
    else if readAtMost(target, expected.size + 1) == expected then Right(())
    else Left(WriteFailure.TargetConflict(target.toString, "existing entry has different bytes"))

  /** Serialize every operation that publishes to `target`, across processes.
    *
    * Polymorphic in the failure type so that write and claim share one destination-collision
    * protocol rather than each inventing its own. The exists-check/move sequence is not atomic:
    * `ATOMIC_MOVE` maps to `rename(2)`, which silently replaces an existing target, so two callers
    * that both pass the existence check both "succeed" and one overwrites the other.
    */
  private def withTargetLock[E, A](
      target: Path,
      ioFailure: String => E
  )(operation: Path => Either[E, A]): Either[E, A] =
    val normalized = target.toAbsolutePath.normalize()
    Option(normalized.getParent) match
      case None         => Left(ioFailure("atomic target must have a parent"))
      case Some(parent) =>
        Files.createDirectories(parent)
        val lockPath = parent.resolve(s".${normalized.getFileName}.atomic.lock")
        val stripe = localLockStripes(
          (normalized.toString.hashCode & Int.MaxValue) % localLockStripes.length
        )
        stripe.synchronized {
          val lockChannel = FileChannel.open(
            lockPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
          )
          setPrivate(lockPath, executable = false)
          try
            val lock = lockChannel.lock()
            try operation(normalized)
            finally lock.release()
          finally lockChannel.close()
        }

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
        // Forcing the file contents makes the BYTES durable; only forcing the containing directory
        // makes the RENAME durable. Without this a crash can lose a published file whose contents
        // were already on stable storage.
        Option(target.toAbsolutePath.getParent).foreach(forceDirectory)
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
      var done = false
      while !done && total < maximum do
        val requested = math.min(buffer.length, maximum - total)
        val count = input.read(buffer, 0, requested)
        if count < 0 then done = true
        else
          output.write(buffer, 0, count)
          total += count
      output.toByteArray.toVector
    finally input.close()

  /** Best-effort directory force. Returns whether the platform allowed it.
    *
    * Opening a directory as a channel is not portable — Linux permits it, macOS rejects it with
    * `IsADirectory` — so this cannot be a guarantee. It is attempted rather than skipped because
    * where it does work it closes a real durability gap, and reported rather than swallowed because
    * a caller that genuinely needs crash durability deserves to know it was not achieved.
    */
  def forceDirectory(directory: Path): Boolean =
    try
      val channel = FileChannel.open(directory, StandardOpenOption.READ)
      try
        channel.force(true)
        true
      finally channel.close()
    catch case NonFatal(_) => false

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
    case other                         => WriteFailure.Io(safeMessage(other))

  private def safeMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
