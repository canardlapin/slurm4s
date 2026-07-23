package io.github.bbuchsbaum.scalaslurm.agent

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.cli.CommandPolicy
import io.github.bbuchsbaum.scalaslurm.cli.DataParserVersion
import io.github.bbuchsbaum.scalaslurm.cli.SlurmCliScheduler
import io.github.bbuchsbaum.scalaslurm.cli.SlurmCliSettings
import io.github.bbuchsbaum.scalaslurm.cli.SlurmExecutable
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.local.Fs2CommandExecutor
import io.github.bbuchsbaum.scalaslurm.local.LocalCommandSettings
import io.github.bbuchsbaum.scalaslurm.local.LocalLogReader
import io.github.bbuchsbaum.scalaslurm.local.LocalSubmissionPlanner
import io.github.bbuchsbaum.scalaslurm.local.LocalWorkspaceSettings
import io.github.bbuchsbaum.scalaslurm.protocol.FrameLimits

import java.nio.file.Path

object AgentMain extends IOApp:
  given Processes[IO] = Processes.forIO

  def run(arguments: List[String]): IO[ExitCode] =
    AgentCommand.parse(arguments) match
      case Left(problem)                  => fail(problem)
      case Right(AgentCommand.ServeStdio) =>
        AgentRuntimeConfig.fromEnvironment(sys.env) match
          case Left(problem) => fail(problem)
          case Right(config) => serve(config).as(ExitCode.Success)

  private def serve(config: AgentRuntimeConfig): IO[Unit] =
    val commandExecutor = Fs2CommandExecutor[IO](
      LocalCommandSettings(
        executablePaths =
          SlurmExecutable.values.map(executable => executable -> executable.fileName).toMap,
        baseEnvironment = config.environment,
        allowedEnvironmentOverrides = Set.empty,
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
        CommandPolicy(config.commandTimeout, config.maximumCommandBytes)
      )
    )
    val localLogs = LocalLogReader[IO](config.workspace)
    val service = AgentService[IO](scheduler, AgentLogReader(localLogs.read))
    val handler = ServiceRequestHandler[IO](service, SchedulerRequestHandler[IO](service))
    val server = AgentStdioServer[IO](handler, FrameLimits(config.maximumFrameBytes))

    fs2.io
      .stdin[IO](64 * 1024)
      .through(server.pipe)
      .through(fs2.io.stdout[IO])
      .compile
      .drain

  private def fail(problem: String): IO[ExitCode] =
    IO.blocking(System.err.println(s"scala-slurm-agent: $problem")).as(ExitCode.Error)

final case class AgentRuntimeConfig(
    workspace: Path,
    environment: Map[String, String],
    dataParser: DataParserVersion,
    commandTimeout: DurationMillis,
    maximumCommandBytes: ByteLimit,
    maximumScriptBytes: ByteLimit,
    maximumFrameBytes: ByteLimit
)

object AgentRuntimeConfig:
  val WorkspaceEnvironment: String = "SCALA_SLURM_WORKSPACE"

  def fromEnvironment(environment: Map[String, String]): Either[String, AgentRuntimeConfig] =
    for
      workspaceText <- environment
        .get(WorkspaceEnvironment)
        .filter(_.nonEmpty)
        .toRight(s"$WorkspaceEnvironment must name the private agent workspace")
      workspace <- parsePath(workspaceText)
      parser <- DataParserVersion.from("v0.0.43").left.map(_.reason)
      timeout <- DurationMillis.from(30_000L).left.map(_.reason)
    yield AgentRuntimeConfig(
      workspace = workspace,
      environment = environment,
      dataParser = parser,
      commandTimeout = timeout,
      maximumCommandBytes = ByteLimit.defaultEvidence,
      maximumScriptBytes = ByteLimit.maximumCommandCapture,
      maximumFrameBytes = ByteLimit.maximumCommandCapture
    )

  private def parsePath(raw: String): Either[String, Path] =
    try
      val path = Path.of(raw).toAbsolutePath.normalize()
      Either.cond(path.getParent != null, path, "agent workspace must not be a filesystem root")
    catch case error: RuntimeException => Left(s"invalid agent workspace: ${error.getMessage}")
