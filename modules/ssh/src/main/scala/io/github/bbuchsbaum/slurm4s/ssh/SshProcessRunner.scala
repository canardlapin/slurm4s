package io.github.bbuchsbaum.slurm4s.ssh

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
import io.github.bbuchsbaum.slurm4s.core.BoundedEvidence
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.DurationMillis
import io.github.bbuchsbaum.slurm4s.core.EvidenceSource
import io.github.bbuchsbaum.slurm4s.protocol.FrameCodec

import java.io.IOException
import scala.concurrent.duration.*

final case class SshExchangePolicy(
    timeout: DurationMillis,
    maximumCaptureBytes: ByteLimit = SshExchangePolicy.maximumFrameCapture
) derives CanEqual

object SshExchangePolicy:
  val maximumFrameCapture: ByteLimit =
    ByteLimit.unsafeFrom(ByteLimit.maximumCommandCapture.value + FrameCodec.HeaderBytes)

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
  case Failed(
      stage: SshProcessStage,
      diagnostic: String,
      requestWriteCompleted: Boolean,
      stdout: BoundedEvidence,
      stderr: BoundedEvidence
  )
  case SpawnFailed(diagnostic: String, evidence: BoundedEvidence)

enum SshProcessStage derives CanEqual:
  case RequestWrite
  case StdoutDrain
  case StderrDrain
  case ExitWait
  case Teardown

  def code: String = this match
    case RequestWrite => "request-write"
    case StdoutDrain  => "stdout-drain"
    case StderrDrain  => "stderr-drain"
    case ExitWait     => "exit-wait"
    case Teardown     => "teardown"

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
      attempted <- process(launch).use {
        case Left(failed)   => failed.pure[F]
        case Right(running) =>
          run(running, request, policy, stdout, stderr, writeCompleted)
      }.attempt
      result <- attempted match
        case Right(outcome)           => outcome.pure[F]
        case Left(error: IOException) =>
          processFailure(SshProcessStage.Teardown, error, writeCompleted, stdout, stderr)
        case Left(error: SecurityException) =>
          processFailure(SshProcessStage.Teardown, error, writeCompleted, stdout, stderr)
        case Left(error) => Async[F].raiseError(error)
    yield result

  private[ssh] def exchangeStreams(
      requestWrite: F[Unit],
      processStdout: Stream[F, Byte],
      processStderr: Stream[F, Byte],
      exitValue: F[Int],
      policy: SshExchangePolicy
  ): F[SshProcessOutcome] =
    for
      stdout <- Ref.of[F, SshCapture](SshCapture.empty)
      stderr <- Ref.of[F, SshCapture](SshCapture.empty)
      writeCompleted <- Ref.of[F, Boolean](false)
      result <- runStreams(
        requestWrite,
        processStdout,
        processStderr,
        exitValue,
        policy,
        stdout,
        stderr,
        writeCompleted
      )
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
    runStreams(
      Stream.emits(request).covary[F].through(process.stdin).compile.drain,
      process.stdout,
      process.stderr,
      process.exitValue,
      policy,
      stdout,
      stderr,
      writeCompleted
    )

  private def runStreams(
      requestWrite: F[Unit],
      processStdout: Stream[F, Byte],
      processStderr: Stream[F, Byte],
      exitValue: F[Int],
      policy: SshExchangePolicy,
      stdout: Ref[F, SshCapture],
      stderr: Ref[F, SshCapture],
      writeCompleted: Ref[F, Boolean]
  ): F[SshProcessOutcome] =
    val write =
      stage(SshProcessStage.RequestWrite)(requestWrite) *> writeCompleted.set(true)
    val operations = (
      write,
      stage(SshProcessStage.StdoutDrain)(
        drain(processStdout, stdout, policy.maximumCaptureBytes)
      ),
      stage(SshProcessStage.StderrDrain)(
        drain(processStderr, stderr, policy.maximumCaptureBytes)
      ),
      stage(SshProcessStage.ExitWait)(exitValue)
    ).parTupled

    operations
      .map(value => Right(value._4): Either[Unit, Int])
      .timeoutTo(policy.timeout.value.millis, Left(()).pure[F])
      .attempt
      .flatMap {
        case Right(Right(exitCode)) =>
          (writeCompleted.get, evidence(stdout, stderr)).mapN { case (completed, (out, err)) =>
            SshProcessOutcome.Exited(exitCode, completed, out, err)
          }
        case Right(Left(_)) =>
          (writeCompleted.get, evidence(stdout, stderr)).mapN { case (completed, (out, err)) =>
            SshProcessOutcome.TimedOut(completed, out, err)
          }
        case Left(error: SshProcessStageException) =>
          processFailure(error.stage, error.getCause, writeCompleted, stdout, stderr)
        case Left(error) => Async[F].raiseError(error)
      }

  private def stage[A](stage: SshProcessStage)(effect: F[A]): F[A] =
    effect.handleErrorWith {
      case error: IOException =>
        Async[F].raiseError(SshProcessStageException(stage, error))
      case error: SecurityException =>
        Async[F].raiseError(SshProcessStageException(stage, error))
      case error => Async[F].raiseError(error)
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

  private def processFailure(
      stage: SshProcessStage,
      error: Throwable,
      writeCompleted: Ref[F, Boolean],
      stdout: Ref[F, SshCapture],
      stderr: Ref[F, SshCapture]
  ): F[SshProcessOutcome] =
    (writeCompleted.get, evidence(stdout, stderr)).mapN { case (completed, (out, err)) =>
      SshProcessOutcome.Failed(
        stage,
        failureDiagnostic(error),
        completed,
        out,
        err
      )
    }

  private def failureDiagnostic(error: Throwable): String =
    val kind = error match
      case _: IOException       => "io-error"
      case _: SecurityException => "security-error"
      case _                    => "process-error"
    Option(error.getMessage)
      .filter(_.nonEmpty)
      .fold(kind)(message => s"$kind: ${message.take(512)}")

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

final private case class SshProcessStageException(stage: SshProcessStage, underlying: Throwable)
    extends RuntimeException(null, underlying, false, false)

final private case class SshCapture(bytes: Vector[Byte], totalBytes: Long):
  def append(chunk: Chunk[Byte], limit: ByteLimit): SshCapture =
    val remaining = math.max(0, limit.value - bytes.size)
    val retained = if remaining == 0 then Vector.empty else chunk.take(remaining).toVector
    SshCapture(bytes ++ retained, totalBytes + chunk.size.toLong)

  def evidence(source: EvidenceSource, observedAt: java.time.Instant): BoundedEvidence =
    BoundedEvidence.fromCapture(source, observedAt, bytes, totalBytes)

private object SshCapture:
  val empty: SshCapture = SshCapture(Vector.empty, 0L)
