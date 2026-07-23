package io.github.bbuchsbaum.scalaslurm.ssh

import cats.effect.Async
import cats.effect.Clock
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.implicits.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.io.process.Process
import fs2.io.process.ProcessBuilder
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.core.BoundedEvidence
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.EvidenceSource
import io.github.bbuchsbaum.scalaslurm.protocol.FrameCodec

import java.io.IOException
import scala.concurrent.duration.*

final case class SshExchangePolicy(
    timeout: DurationMillis,
    maximumCaptureBytes: ByteLimit = SshExchangePolicy.maximumFrameCapture
) derives CanEqual

object SshExchangePolicy:
  val maximumFrameCapture: ByteLimit = ByteLimit
    .from(ByteLimit.maximumCommandCapture.value + FrameCodec.HeaderBytes)
    .fold(problem => throw new IllegalStateException(problem.reason), identity)

enum SshProcessOutcome derives CanEqual:
  case Exited(
      exitCode: Int,
      requestWriteCompleted: Boolean,
      stdout: BoundedEvidence,
      stderr: BoundedEvidence
  )
  case TimedOut(
      requestWriteCompleted: Boolean,
      stdout: BoundedEvidence,
      stderr: BoundedEvidence
  )
  case SpawnFailed(diagnostic: String, evidence: BoundedEvidence)

trait SshProcessRunner[F[_]]:
  def exchange(
      launch: SshLaunch,
      request: Vector[Byte],
      policy: SshExchangePolicy
  ): F[SshProcessOutcome]

final class SystemSshProcessRunner[F[_]: Async](using Processes[F]) extends SshProcessRunner[F]:
  def exchange(
      launch: SshLaunch,
      request: Vector[Byte],
      policy: SshExchangePolicy
  ): F[SshProcessOutcome] =
    for
      stdout <- Ref.of[F, SshCapture](SshCapture.empty)
      stderr <- Ref.of[F, SshCapture](SshCapture.empty)
      writeCompleted <- Ref.of[F, Boolean](false)
      result <- process(launch).use {
        case Left(failed)   => failed.pure[F]
        case Right(running) => run(running, request, policy, stdout, stderr, writeCompleted)
      }
    yield result

  private def process(
      launch: SshLaunch
  ): Resource[F, Either[SshProcessOutcome, Process[F]]] =
    Processes[F]
      .spawn(
        ProcessBuilder(launch.executablePath, launch.arguments.toList)
          .withInheritEnv(true)
      )
      .map(running => Right(running): Either[SshProcessOutcome, Process[F]])
      .handleErrorWith {
        case error: IOException       => Resource.eval(spawnFailure(error))
        case error: SecurityException => Resource.eval(spawnFailure(error))
        case error                    => Resource.eval(Async[F].raiseError(error))
      }

  private def run(
      process: Process[F],
      request: Vector[Byte],
      policy: SshExchangePolicy,
      stdout: Ref[F, SshCapture],
      stderr: Ref[F, SshCapture],
      writeCompleted: Ref[F, Boolean]
  ): F[SshProcessOutcome] =
    val write = Stream
      .emits(request)
      .covary[F]
      .through(process.stdin)
      .compile
      .drain
      .attempt
      .flatTap(result => writeCompleted.set(result.isRight))
      .map(_.isRight)
    val operations = (
      write,
      drain(process.stdout, stdout, policy.maximumCaptureBytes),
      drain(process.stderr, stderr, policy.maximumCaptureBytes),
      process.exitValue
    ).parTupled

    operations
      .map(value => Right((value._1, value._4)): Either[Unit, (Boolean, Int)])
      .timeoutTo(policy.timeout.value.millis, Left(()).pure[F])
      .flatMap {
        case Right((writeCompleted, exitCode)) =>
          evidence(stdout, stderr).map { case (out, err) =>
            SshProcessOutcome.Exited(exitCode, writeCompleted, out, err)
          }
        case Left(_) =>
          (writeCompleted.get, evidence(stdout, stderr)).mapN { case (completed, (out, err)) =>
            SshProcessOutcome.TimedOut(completed, out, err)
          }
      }

  private def drain(
      stream: Stream[F, Byte],
      target: Ref[F, SshCapture],
      limit: ByteLimit
  ): F[Unit] =
    stream.chunks.evalMap(chunk => target.update(_.append(chunk, limit))).compile.drain

  private def evidence(
      stdout: Ref[F, SshCapture],
      stderr: Ref[F, SshCapture]
  ): F[(BoundedEvidence, BoundedEvidence)] =
    (stdout.get, stderr.get, Clock[F].realTimeInstant).mapN { (out, err, observedAt) =>
      (
        out.evidence(EvidenceSource.CommandStdout("ssh"), observedAt),
        err.evidence(EvidenceSource.CommandStderr("ssh"), observedAt)
      )
    }

  private def spawnFailure(error: Throwable): F[Either[SshProcessOutcome, Process[F]]] =
    Clock[F].realTimeInstant.map { observedAt =>
      Left(
        SshProcessOutcome.SpawnFailed(
          diagnostic = s"system ssh could not be started: ${error.getClass.getSimpleName}",
          evidence = BoundedEvidence.capture(
            EvidenceSource.CommandLaunch("ssh"),
            observedAt,
            Vector.empty
          )
        )
      )
    }

final private case class SshCapture(bytes: Vector[Byte], totalBytes: Long):
  def append(chunk: Chunk[Byte], limit: ByteLimit): SshCapture =
    val remaining = math.max(0, limit.value - bytes.size)
    val retained = if remaining == 0 then Vector.empty else chunk.take(remaining).toVector
    SshCapture(bytes ++ retained, totalBytes + chunk.size.toLong)

  def evidence(source: EvidenceSource, observedAt: java.time.Instant): BoundedEvidence =
    BoundedEvidence.fromCapture(source, observedAt, bytes, totalBytes)

private object SshCapture:
  val empty: SshCapture = SshCapture(Vector.empty, 0L)
