package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.IO
import cats.effect.kernel.Clock
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.ResultEnvelopeCodec

import java.nio.file.Path
import java.time.Instant

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
            IO.blocking(monitor.synchronized(AtomicFiles.publishOnceBlocking(target, bytes)))
              .map {
                case Right(digest) =>
                  Right(
                    ResultPublication(target.toAbsolutePath.normalize(), bytes, digest, now)
                  )
                case Left(AtomicFiles.WriteFailure.TargetExists(path)) =>
                  Left(ResultPublicationFailure.AlreadyPublished(path))
                case Left(AtomicFiles.WriteFailure.TargetConflict(path, _)) =>
                  Left(ResultPublicationFailure.AlreadyPublished(path))
                case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(path)) =>
                  Left(ResultPublicationFailure.AtomicMoveUnavailable(path))
                case Left(AtomicFiles.WriteFailure.Io(detail)) =>
                  Left(ResultPublicationFailure.Io(detail))
              }
          }
