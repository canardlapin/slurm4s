package io.github.bbuchsbaum.scalaslurm.worker

import cats.effect.IO
import cats.effect.kernel.Clock
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.ResultEnvelopeCodec

import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

enum ResultPublicationFailure derives CanEqual:
  case EnvelopeInvalid(message: String)
  case AlreadyPublished(path: String)
  case AtomicMoveUnavailable(path: String)
  case Io(message: String)

final case class ResultPublication(
    path: Path,
    envelopeBytes: Vector[Byte],
    digest: ContentDigest,
    publishedAt: Instant
)

trait ResultPublisher[F[_]]:
  def publish(envelope: ResultEnvelope): F[Either[ResultPublicationFailure, ResultPublication]]

object FileResultPublisher:
  def apply(
      target: Path,
      maximumEnvelopeBytes: ByteLimit,
      maximumValueBytes: ByteLimit
  ): ResultPublisher[IO] = new ResultPublisher[IO]:
    private val monitor = new AnyRef

    def publish(
        envelope: ResultEnvelope
    ): IO[Either[ResultPublicationFailure, ResultPublication]] =
      ResultEnvelopeCodec.encode(envelope, maximumEnvelopeBytes, maximumValueBytes) match
        case Left(failure) =>
          IO.pure(Left(ResultPublicationFailure.EnvelopeInvalid(failure.toString)))
        case Right(bytes) =>
          Clock[IO].realTimeInstant.flatMap { now =>
            IO.blocking(monitor.synchronized(publishBlocking(target, bytes, now))).attempt.map {
              case Right(value)                                      => Right(value)
              case Left(_: java.nio.file.FileAlreadyExistsException) =>
                Left(ResultPublicationFailure.AlreadyPublished(target.toString))
              case Left(_: AtomicMoveNotSupportedException) =>
                Left(ResultPublicationFailure.AtomicMoveUnavailable(target.toString))
              case Left(error) =>
                Left(
                  ResultPublicationFailure.Io(
                    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                  )
                )
            }
          }

  private def publishBlocking(
      target: Path,
      bytes: Vector[Byte],
      publishedAt: Instant
  ): ResultPublication =
    val parent = Option(target.toAbsolutePath.normalize().getParent).getOrElse(
      throw new IllegalArgumentException("result target must have a parent")
    )
    Files.createDirectories(parent)
    val normalizedTarget = target.toAbsolutePath.normalize()
    val lockPath = parent.resolve(s".${target.getFileName}.publish.lock")
    val lockChannel = FileChannel.open(
      lockPath,
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE
    )
    setPrivate(lockPath)
    try
      val lock = lockChannel.lock()
      try
        if Files.exists(normalizedTarget, LinkOption.NOFOLLOW_LINKS) then
          throw new java.nio.file.FileAlreadyExistsException(normalizedTarget.toString)
        val temporary = parent.resolve(s".${target.getFileName}.tmp-${UUID.randomUUID()}")
        try
          val channel = FileChannel.open(
            temporary,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE
          )
          setPrivate(temporary)
          try
            val buffer = java.nio.ByteBuffer.wrap(bytes.toArray)
            while buffer.hasRemaining do
              val _ = channel.write(buffer)
            channel.force(true)
          finally channel.close()
          val _ = Files.move(temporary, normalizedTarget, StandardCopyOption.ATOMIC_MOVE)
          ResultPublication(normalizedTarget, bytes, digest(bytes), publishedAt)
        finally
          val _ = Files.deleteIfExists(temporary)
      finally lock.release()
    finally lockChannel.close()

  private def setPrivate(path: Path): Unit =
    try
      val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
    catch case _: UnsupportedOperationException => ()

  private def digest(bytes: Vector[Byte]): ContentDigest =
    val hex = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    ContentDigest
      .from(s"sha256:$hex")
      .fold(problem => throw new IllegalStateException(problem.reason), identity)
