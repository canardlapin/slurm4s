package io.github.bbuchsbaum.slurm4s.local

import cats.effect.Async
import cats.effect.Clock
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.core.*

import scodec.bits.ByteVector

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import scala.concurrent.duration.*

final class LocalLogReader[F[_]: Async](
    root: Path,
    maximumPageBytes: ByteLimit = ByteLimit.maximumLogPage
):
  private val normalizedRoot = root.toAbsolutePath.normalize()

  def read(ref: LogRef, cursor: LogCursor, maxBytes: ByteLimit): F[LogReadResult] =
    Clock[F].realTimeInstant.flatMap { observedAt =>
      if maxBytes.value > maximumPageBytes.value then
        LogReadResult
          .Failed(
            Diagnostics.one(
              Diagnostic(
                "log-page-too-large",
                "the requested log page exceeds the configured local safety limit"
              )
            ),
            observedAt
          )
          .pure[F]
      else
        Async[F].blocking(readBlocking(ref, cursor, maxBytes, observedAt)).handleError { error =>
          LogReadResult.Failed(
            Diagnostics.one(
              Diagnostic(
                "log-read-failed",
                "the log page could not be read safely",
                Map("cause" -> error.getClass.getSimpleName)
              )
            ),
            observedAt
          )
        }
    }

  def follow(
      ref: LogRef,
      from: LogCursor,
      pageSize: ByteLimit,
      pollEvery: DurationMillis
  ): Stream[F, LogReadResult] =
    Stream.eval(read(ref, from, pageSize)).flatMap {
      case LogReadResult.Page(page) if page.bytes.isEmpty && page.endOfFile =>
        Stream.sleep_[F](pollEvery.value.millis) ++ follow(ref, page.next, pageSize, pollEvery)
      case pageResult @ LogReadResult.Page(page) =>
        Stream.emit(pageResult) ++
          (if page.endOfFile then Stream.sleep_[F](pollEvery.value.millis) else Stream.empty) ++
          follow(ref, page.next, pageSize, pollEvery)
      case waiting @ LogReadResult.WaitingForFile(cursor, _) =>
        Stream.emit(waiting) ++ Stream.sleep_[F](pollEvery.value.millis) ++
          follow(ref, cursor, pageSize, pollEvery)
      case terminal => Stream.emit(terminal)
    }

  private def readBlocking(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit,
      observedAt: java.time.Instant
  ): LogReadResult =
    val path = Path.of(ref.locator).toAbsolutePath.normalize()
    if !path.startsWith(normalizedRoot) || path.equals(normalizedRoot) then
      LogReadResult.Failed(
        Diagnostics.one(
          Diagnostic("log-path-outside-root", "the log locator is outside the configured workspace")
        ),
        observedAt
      )
    else if !Files.exists(path, LinkOption.NOFOLLOW_LINKS) then
      LogReadResult.WaitingForFile(cursor, observedAt)
    else if Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) then
      LogReadResult.Failed(
        Diagnostics.one(
          Diagnostic("log-path-not-regular", "the log locator is not a regular non-symlink file")
        ),
        observedAt
      )
    else
      val realRoot = normalizedRoot.toRealPath()
      val realPath = path.toRealPath()
      if !realPath.startsWith(realRoot) then
        LogReadResult.Failed(
          Diagnostics.one(
            Diagnostic(
              "log-path-symlink-escape",
              "the resolved log path escapes the workspace root"
            )
          ),
          observedAt
        )
      else readRegularFile(realPath, cursor, maxBytes, observedAt)

  private def readRegularFile(
      path: Path,
      cursor: LogCursor,
      maxBytes: ByteLimit,
      observedAt: java.time.Instant
  ): LogReadResult =
    val attributes =
      Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
    val identity = fileIdentity(path, attributes)
    val size = attributes.size()
    if cursor.fileIdentity.exists(_.value != identity.value) || cursor.offset.value > size then
      LogReadResult.CursorInvalid(cursor, Some(identity), size, observedAt)
    else
      val bytes = readPage(path, cursor.offset.value, maxBytes.value)
      val nextOffset =
        LogOffset.unsafeFrom(Math.addExact(cursor.offset.value, bytes.size.toLong))
      LogReadResult.Page(
        LogPage(
          bytes = bytes,
          next = LogCursor(nextOffset, Some(identity)),
          endOfFile = nextOffset.value >= size,
          observedAt = observedAt
        )
      )

  private def readPage(path: Path, offset: Long, maxBytes: Int): ByteVector =
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try
      channel.position(offset)
      val buffer = ByteBuffer.allocate(maxBytes)
      val count = channel.read(buffer)
      if count <= 0 then ByteVector.empty
      else
        buffer.flip()
        val result = new Array[Byte](count)
        buffer.get(result)
        ByteVector.view(result)
    finally channel.close()

  /** Identity of the open log file, stable for as long as the same file keeps growing.
    *
    * The material is deliberately limited to the path and the POSIX `(device, inode)` file key.
    * Creation time must not participate: `sun.nio.fs.UnixFileAttributes.creationTime()` falls back
    * to `lastModifiedTime()` wherever `birthtime` is unsupported, which includes Linux, so
    * including it would change the identity on every append and invalidate every live cursor.
    * Rotation and truncation are still detected -- rotation replaces the inode, and truncation is
    * caught by the separate size check in `readRegularFile`.
    */
  private def fileIdentity(path: Path, attributes: BasicFileAttributes): FileIdentity =
    val material =
      s"${path.toString}|${Option(attributes.fileKey()).fold("none")(_.toString)}"
        .getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(material)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    FileIdentity.unsafeFrom(digest)
