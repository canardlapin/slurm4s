package io.github.bbuchsbaum.remoteexec.kernel

import cats.effect.kernel.Sync

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
  * Every write uses a private sibling temporary file opened with `CREATE_NEW`, forces its contents
  * to stable storage, and publishes only with `ATOMIC_MOVE`. There is no copy/delete fallback.
  * Unsupported atomic rename is a typed failure, allowing higher layers to refuse a filesystem
  * whose guarantees are too weak.
  *
  * The kernel owns mechanics only. It contains no scheduler, worker, queue, lease, retry, or spool
  * policy.
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
      if Files.exists(target, LinkOption.NOFOLLOW_LINKS) then
        Left(WriteFailure.TargetExists(target.toString))
      else stageAndMove(target, bytes, executable, replaceExisting = false)
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  def writeNew[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): F[Either[WriteFailure, Unit]] =
    Sync[F].blocking(writeNewBlocking(target, bytes, executable))

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
      withTargetLock(target) { normalized =>
        if Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) then
          validateExisting(normalized, bytes)
        else stageAndMove(normalized, bytes, executable, replaceExisting = false)
      }
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  def writeStable[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): F[Either[WriteFailure, Unit]] =
    Sync[F].blocking(writeStableBlocking(target, bytes, executable))

  def replaceBlocking(target: Path, bytes: Vector[Byte]): Either[WriteFailure, Unit] =
    try stageAndMove(target, bytes, executable = false, replaceExisting = true)
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  def replace[F[_]: Sync](target: Path, bytes: Vector[Byte]): F[Either[WriteFailure, Unit]] =
    Sync[F].blocking(replaceBlocking(target, bytes))

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
      withTargetLock(target) { normalized =>
        if Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) then
          Left(WriteFailure.TargetExists(normalized.toString))
        else
          stageAndMove(normalized, bytes, executable = false, replaceExisting = false)
            .map(_ => digestOf(bytes))
      }
    catch case NonFatal(error) => Left(writeFailureOf(error, target))

  def publishOnce[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte]
  ): F[Either[WriteFailure, ContentDigest]] =
    Sync[F].blocking(publishOnceBlocking(target, bytes))

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
        Left(ClaimFailure.Io(safeMessage(error)))

  def claim[F[_]: Sync](from: Path, to: Path): F[Either[ClaimFailure, Path]] =
    Sync[F].blocking(claimBlocking(from, to))

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

  private def withTargetLock[A](
      target: Path
  )(operation: Path => Either[WriteFailure, A]): Either[WriteFailure, A] =
    val normalized = target.toAbsolutePath.normalize()
    Option(normalized.getParent) match
      case None         => Left(WriteFailure.Io("atomic target must have a parent"))
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
