package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.Async
import cats.effect.Clock
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.implicits.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.io.file.Path
import fs2.io.process.Process
import fs2.io.process.ProcessBuilder
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.cli.CommandExecutor
import io.github.bbuchsbaum.scalaslurm.cli.CommandPolicy
import io.github.bbuchsbaum.scalaslurm.cli.SlurmCommand
import io.github.bbuchsbaum.scalaslurm.cli.SlurmExecutable
import io.github.bbuchsbaum.scalaslurm.core.*

import java.io.IOException
import scala.concurrent.duration.*

final case class LocalCommandSettings(
    executablePaths: Map[SlurmExecutable, String],
    baseEnvironment: Map[String, String],
    allowedEnvironmentOverrides: Set[String],
    maximumCaptureBytes: ByteLimit = ByteLimit.maximumCommandCapture
)

object LocalCommandSettings:
  val standardEnvironmentNames: Set[String] = Set(
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "PATH",
    "SLURM_CONF",
    "TZ"
  )
  val blockedEnvironmentNames: Set[String] = Set(
    "BASH_ENV",
    "ENV",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
    "DYLD_INSERT_LIBRARIES",
    "PYTHONPATH"
  )

final class Fs2CommandExecutor[F[_]: Async](settings: LocalCommandSettings)(using Processes[F])
    extends CommandExecutor[F]:

  def execute(command: SlurmCommand, policy: CommandPolicy): F[InvocationResult] =
    settings.executablePaths.get(command.executable) match
      case None                 => missingExecutable(command)
      case Some(executablePath) =>
        for
          stdout <- Ref.of[F, CaptureState](CaptureState.empty)
          stderr <- Ref.of[F, CaptureState](CaptureState.empty)
          builder = processBuilder(executablePath, command)
          result <- processResource(builder, command).use {
            case Left(failed)   => failed.pure[F]
            case Right(process) => run(process, command, policy, stdout, stderr)
          }
        yield result

  private def processResource(
      builder: ProcessBuilder,
      command: SlurmCommand
  ): Resource[F, Either[InvocationResult, Process[F]]] =
    Processes[F]
      .spawn(builder)
      .map(process => Right(process): Either[InvocationResult, Process[F]])
      .handleErrorWith {
        case error: IOException =>
          Resource.eval(spawnFailure(command, SpawnFailureKind.Unknown, error))
        case error: SecurityException =>
          Resource.eval(spawnFailure(command, SpawnFailureKind.PermissionDenied, error))
        case error => Resource.eval(Async[F].raiseError(error))
      }

  private def run(
      process: Process[F],
      command: SlurmCommand,
      policy: CommandPolicy,
      stdout: Ref[F, CaptureState],
      stderr: Ref[F, CaptureState]
  ): F[InvocationResult] =
    val effectiveLimit = ByteLimit
      .from(math.min(policy.captureLimit.value, settings.maximumCaptureBytes.value))
      .toOption
      .get
    val drains = (
      drain(process.stdout, stdout, effectiveLimit),
      drain(process.stderr, stderr, effectiveLimit),
      process.exitValue
    ).parTupled

    drains
      .map(tuple => Right(tuple._3): Either[Unit, Int])
      .timeoutTo(
        policy.timeout.value.millis,
        Left(()).pure[F]
      )
      .flatMap {
        case Right(exitCode) =>
          evidence(command, stdout, stderr).map { case (out, err) =>
            InvocationResult.Exited(exitCode, out, err)
          }
        case Left(_) =>
          evidence(command, stdout, stderr).map { case (out, err) =>
            InvocationResult.TimedOut(policy.timeout, out, err)
          }
      }

  private def drain(
      stream: Stream[F, Byte],
      target: Ref[F, CaptureState],
      limit: ByteLimit
  ): F[Unit] =
    stream.chunks.evalMap(chunk => target.update(_.append(chunk, limit))).compile.drain

  private def evidence(
      command: SlurmCommand,
      stdout: Ref[F, CaptureState],
      stderr: Ref[F, CaptureState]
  ): F[(BoundedEvidence, BoundedEvidence)] =
    (stdout.get, stderr.get, Clock[F].realTimeInstant).mapN { (out, err, observedAt) =>
      (
        out.evidence(EvidenceSource.CommandStdout(command.executable.fileName), observedAt),
        err.evidence(EvidenceSource.CommandStderr(command.executable.fileName), observedAt)
      )
    }

  private def processBuilder(executablePath: String, command: SlurmCommand): ProcessBuilder =
    val allowedOverrides =
      settings.allowedEnvironmentOverrides -- LocalCommandSettings.blockedEnvironmentNames
    val overrides = command.environment.view.filterKeys(allowedOverrides).toMap
    val sanitized = settings.baseEnvironment.view
      .filterKeys(LocalCommandSettings.standardEnvironmentNames)
      .toMap ++ overrides
    val base = ProcessBuilder(executablePath, command.arguments.toList)
      .withInheritEnv(false)
      .withExtraEnv(sanitized)
    command.workingDirectory.fold(base)(directory => base.withWorkingDirectory(Path(directory)))

  private def missingExecutable(command: SlurmCommand): F[InvocationResult] =
    Clock[F].realTimeInstant.map { observedAt =>
      val evidence = BoundedEvidence.capture(
        EvidenceSource.CommandLaunch(command.executable.fileName),
        observedAt,
        Vector.empty
      )
      InvocationResult.SpawnFailed(
        SpawnFailureKind.ExecutableMissing,
        Diagnostics.one(
          Diagnostic(
            "executable-not-configured",
            "no executable path is configured",
            Map("command" -> command.executable.fileName)
          )
        ),
        EvidenceBundle(evidence)
      )
    }

  private def spawnFailure(
      command: SlurmCommand,
      kind: SpawnFailureKind,
      error: Throwable
  ): F[Either[InvocationResult, Process[F]]] =
    Clock[F].realTimeInstant.map { observedAt =>
      val evidence = BoundedEvidence.capture(
        EvidenceSource.CommandLaunch(command.executable.fileName),
        observedAt,
        Vector.empty
      )
      Left(
        InvocationResult.SpawnFailed(
          kind,
          Diagnostics.one(
            Diagnostic(
              "process-spawn-failed",
              "the configured executable could not be started",
              Map("command" -> command.executable.fileName, "cause" -> error.getClass.getSimpleName)
            )
          ),
          EvidenceBundle(evidence)
        )
      )
    }

final private case class CaptureState(bytes: Vector[Byte], totalBytes: Long):
  def append(chunk: Chunk[Byte], limit: ByteLimit): CaptureState =
    val remaining = math.max(0, limit.value - bytes.size)
    val retained = if remaining == 0 then Vector.empty else chunk.take(remaining).toVector
    CaptureState(bytes ++ retained, totalBytes + chunk.size.toLong)

  def evidence(source: EvidenceSource, observedAt: java.time.Instant): BoundedEvidence =
    BoundedEvidence.fromCapture(source, observedAt, bytes, totalBytes)

private object CaptureState:
  val empty: CaptureState = CaptureState(Vector.empty, 0L)
