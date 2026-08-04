package io.github.bbuchsbaum.slurm4s.agent

import cats.effect.IO
import cats.effect.IOApp
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.cli.CommandPolicy
import io.github.bbuchsbaum.slurm4s.cli.DataParserVersion
import io.github.bbuchsbaum.slurm4s.cli.SlurmCliScheduler
import io.github.bbuchsbaum.slurm4s.cli.SlurmCliSettings
import io.github.bbuchsbaum.slurm4s.cli.SlurmExecutable
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.ContentDigest
import io.github.bbuchsbaum.slurm4s.core.DurationMillis
import io.github.bbuchsbaum.slurm4s.core.EnvName
import io.github.bbuchsbaum.slurm4s.core.WorkerRelease
import io.github.bbuchsbaum.slurm4s.core.WorkerReleaseId
import io.github.bbuchsbaum.slurm4s.local.Fs2CommandExecutor
import io.github.bbuchsbaum.slurm4s.local.LocalCommandSettings
import io.github.bbuchsbaum.slurm4s.local.LocalLogReader
import io.github.bbuchsbaum.slurm4s.local.LocalSubmissionPlanner
import io.github.bbuchsbaum.slurm4s.local.LocalWorkspaceSettings
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits
import io.github.bbuchsbaum.slurm4s.worker.RegisteredTaskLauncher
import io.github.bbuchsbaum.slurm4s.worker.WorkerLaunchSettings

import java.nio.file.Path

object AgentMain extends IOApp:
  given Processes[IO] = Processes.forIO

  /** Parse the agent command and run it to its process exit status. */
  override def run(arguments: List[String]): IO[cats.effect.ExitCode] =
    AgentCommand.parse(arguments) match
      case Left(problem)                  => fail(problem)
      case Right(AgentCommand.ServeStdio) =>
        AgentRuntimeConfig.fromEnvironment(sys.env) match
          case Left(problem) => fail(problem)
          case Right(config) => serve(config).as(cats.effect.ExitCode.Success)

  private def serve(config: AgentRuntimeConfig): IO[Unit] =
    val commandExecutor = Fs2CommandExecutor[IO](
      LocalCommandSettings(
        executablePaths =
          SlurmExecutable.values.map(executable => executable -> executable.fileName).toMap,
        baseEnvironment = config.environment,
        allowedEnvironmentOverrides = config.allowedEnvironmentOverrides,
        maximumCaptureBytes = config.maximumCommandBytes
      )
    )
    val planner = LocalSubmissionPlanner[IO](
      LocalWorkspaceSettings(config.workspace, config.maximumScriptBytes)
    )
    val scheduler = SlurmCliScheduler[IO](
      commandExecutor,
      planner,
      SlurmCliSettings(
        config.dataParser,
        CommandPolicy(
          config.commandTimeout,
          captureLimit = config.maximumCommandBytes,
          evidenceLimit = config.maximumEvidenceBytes
        )
      )
    )
    val localLogs = LocalLogReader[IO](config.workspace)
    val registeredTasks = config.worker.map(settings =>
      WorkerRegisteredTaskService(RegisteredTaskLauncher(settings), scheduler)
    )
    val service = AgentService[IO](
      scheduler,
      AgentLogReader(localLogs.read),
      registeredTasks
    )
    val handler = ServiceRequestHandler[IO](service, SchedulerRequestHandler[IO](service))
    val server = AgentStdioServer[IO](handler, FrameLimits(config.maximumFrameBytes))

    fs2.io
      .stdin[IO](64 * 1024)
      .through(server.pipe)
      .through(fs2.io.stdout[IO])
      .compile
      .drain

  private def fail(problem: String): IO[cats.effect.ExitCode] =
    IO.blocking(System.err.println(s"slurm4s-agent: $problem")).as(cats.effect.ExitCode.Error)

final case class AgentRuntimeConfig(
    workspace: Path,
    environment: Map[String, String],
    allowedEnvironmentOverrides: Set[String],
    dataParser: DataParserVersion,
    commandTimeout: DurationMillis,
    maximumCommandBytes: ByteLimit,
    maximumEvidenceBytes: ByteLimit,
    maximumScriptBytes: ByteLimit,
    maximumFrameBytes: ByteLimit,
    worker: Option[WorkerLaunchSettings]
)

object AgentRuntimeConfig:
  val WorkspaceEnvironment: String = "SLURM4S_WORKSPACE"
  val AllowedEnvironmentNames: String = "SLURM4S_ALLOWED_ENVIRONMENT"
  val WorkerExecutableEnvironment: String = "SLURM4S_WORKER_EXECUTABLE"
  val WorkerReleaseIdEnvironment: String = "SLURM4S_WORKER_RELEASE_ID"
  val WorkerReleaseDigestEnvironment: String = "SLURM4S_WORKER_RELEASE_DIGEST"

  def fromEnvironment(environment: Map[String, String]): Either[String, AgentRuntimeConfig] =
    for
      workspaceText <- environment
        .get(WorkspaceEnvironment)
        .filter(_.nonEmpty)
        .toRight(s"$WorkspaceEnvironment must name the private agent workspace")
      workspace <- parsePath(workspaceText)
      parser <- DataParserVersion.from("v0.0.43").left.map(_.reason)
      timeout <- DurationMillis.from(30_000L).left.map(_.reason)
      allowedEnvironment <- parseAllowedEnvironment(environment.get(AllowedEnvironmentNames))
      worker <- parseWorker(environment, workspace)
    yield AgentRuntimeConfig(
      workspace = workspace,
      environment = environment,
      allowedEnvironmentOverrides = allowedEnvironment,
      dataParser = parser,
      commandTimeout = timeout,
      // Capture wide enough to parse structured squeue output for many jobs; retain far less.
      maximumCommandBytes = ByteLimit.maximumCommandCapture,
      maximumEvidenceBytes = ByteLimit.defaultEvidence,
      maximumScriptBytes = ByteLimit.maximumCommandCapture,
      maximumFrameBytes = ByteLimit.maximumCommandCapture,
      worker = worker
    )

  private def parseAllowedEnvironment(raw: Option[String]): Either[String, Set[String]] =
    raw.filter(_.nonEmpty).fold[Either[String, Set[String]]](Right(Set.empty)) { value =>
      value
        .split(",", -1)
        .toVector
        .foldLeft[Either[String, Set[String]]](Right(Set.empty)) { (result, name) =>
          for
            names <- result
            validated <- EnvName.from(name).left.map(_.reason)
          yield names + validated.value
        }
    }

  private def parseWorker(
      environment: Map[String, String],
      workspace: Path
  ): Either[String, Option[WorkerLaunchSettings]] =
    val configured = Vector(
      WorkerExecutableEnvironment -> environment.get(WorkerExecutableEnvironment),
      WorkerReleaseIdEnvironment -> environment.get(WorkerReleaseIdEnvironment),
      WorkerReleaseDigestEnvironment -> environment.get(WorkerReleaseDigestEnvironment)
    )
    if configured.forall(_._2.isEmpty) then Right(None)
    else
      for
        _ <- Either.cond(
          configured.forall(_._2.exists(_.nonEmpty)),
          (),
          s"${configured.collect { case (name, value) if value.forall(_.isEmpty) => name }.mkString(",")} must be configured together"
        )
        executableText <- configured(0)._2.toRight(s"$WorkerExecutableEnvironment is required")
        releaseIdText <- configured(1)._2.toRight(s"$WorkerReleaseIdEnvironment is required")
        releaseDigestText <- configured(2)._2.toRight(
          s"$WorkerReleaseDigestEnvironment is required"
        )
        executable <- parsePath(executableText)
        releaseId <- WorkerReleaseId.from(releaseIdText).left.map(_.reason)
        releaseDigest <- ContentDigest.from(releaseDigestText).left.map(_.reason)
      yield Some(
        WorkerLaunchSettings(
          workspace.resolve("registered-tasks").toAbsolutePath.normalize(),
          executable,
          WorkerRelease(releaseId, releaseDigest),
          ByteLimit.defaultEvidence,
          ByteLimit.maximumCommandCapture,
          ByteLimit.defaultEvidence,
          ByteLimit.maximumCommandCapture
        )
      )

  private def parsePath(raw: String): Either[String, Path] =
    try
      val path = Path.of(raw).toAbsolutePath.normalize()
      Either.cond(path.getParent != null, path, "agent workspace must not be a filesystem root")
    catch case error: RuntimeException => Left(s"invalid agent workspace: ${error.getMessage}")
