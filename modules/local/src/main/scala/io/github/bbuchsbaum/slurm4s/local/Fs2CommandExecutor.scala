package io.github.bbuchsbaum.slurm4s.local

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
import io.github.bbuchsbaum.slurm4s.cli.CommandExecutor
import io.github.bbuchsbaum.slurm4s.cli.CommandPolicy
import io.github.bbuchsbaum.slurm4s.cli.SlurmCommand
import io.github.bbuchsbaum.slurm4s.cli.SlurmExecutable
import io.github.bbuchsbaum.slurm4s.core.*

import java.io.IOException
import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.util.Locale
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
    val rejectedOverrides = rejectedEnvironmentOverrides(command)
    if rejectedOverrides.nonEmpty then environmentRejected(command, rejectedOverrides)
    else
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
          Resource.eval(spawnFailure(command, spawnFailureKindOf(error), error))
        case error: SecurityException =>
          Resource.eval(spawnFailure(command, SpawnFailureKind.PermissionDenied, error))
        case error => Resource.eval(Async[F].raiseError(error))
      }

  /** Classify a spawn `IOException` into the narrowest kind the JDK makes recoverable.
    *
    * `ProcessBuilder` reports most spawn failures as a plain `IOException` whose message is the
    * platform `execvp` diagnostic, so recognition is deliberately conservative: only unambiguous
    * exception types and errno spellings narrow the kind, and everything else stays `Unknown`.
    */
  private def spawnFailureKindOf(error: IOException): SpawnFailureKind = error match
    case _: NoSuchFileException   => SpawnFailureKind.ExecutableMissing
    case _: AccessDeniedException => SpawnFailureKind.PermissionDenied
    case other                    =>
      Option(other.getMessage).map(_.toLowerCase(Locale.ROOT)) match
        case Some(message) if message.contains("no such file or directory") =>
          SpawnFailureKind.ExecutableMissing
        case Some(message) if message.contains("permission denied") =>
          SpawnFailureKind.PermissionDenied
        case Some(message) if message.contains("not a directory") =>
          SpawnFailureKind.WorkingDirectoryMissing
        case Some(message)
            if message.contains("cannot allocate memory") ||
              message.contains("resource temporarily unavailable") ||
              message.contains("too many open files") =>
          SpawnFailureKind.ResourceUnavailable
        case _ => SpawnFailureKind.Unknown

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
    val sanitized = settings.baseEnvironment.view
      .filterKeys(LocalCommandSettings.standardEnvironmentNames)
      .toMap ++ command.environment
    val base = ProcessBuilder(executablePath, command.arguments.toList)
      .withInheritEnv(false)
      .withExtraEnv(sanitized)
    command.workingDirectory.fold(base)(directory => base.withWorkingDirectory(Path(directory)))

  private def rejectedEnvironmentOverrides(command: SlurmCommand): Vector[String] =
    val allowed =
      settings.allowedEnvironmentOverrides -- LocalCommandSettings.blockedEnvironmentNames
    command.environment.keySet.toVector
      .filter(name => EnvName.from(name).isLeft || !allowed.contains(name))
      .distinct
      .sorted

  private def environmentRejected(
      command: SlurmCommand,
      rejectedNames: Vector[String]
  ): F[InvocationResult] =
    Clock[F].realTimeInstant.map { observedAt =>
      val evidence = BoundedEvidence.capture(
        EvidenceSource.CommandLaunch(command.executable.fileName),
        observedAt,
        Vector.empty
      )
      InvocationResult.SpawnFailed(
        SpawnFailureKind.EnvironmentInvalid,
        Diagnostics.one(
          Diagnostic(
            "environment-override-rejected",
            "the local executor policy cannot apply every requested environment override",
            Map("names" -> rejectedNames.mkString(","))
          )
        ),
        EvidenceBundle(evidence)
      )
    }

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
