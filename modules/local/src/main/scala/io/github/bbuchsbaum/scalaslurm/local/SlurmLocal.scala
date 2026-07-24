package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.Async
import cats.effect.Resource
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.cli.*
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.file.Path

final case class SlurmLocalConfig(
    workspace: Path,
    executablePaths: Map[SlurmExecutable, String],
    baseEnvironment: Map[String, String],
    allowedEnvironmentOverrides: Set[String],
    dataParser: DataParserVersion,
    commandPolicy: CommandPolicy,
    maximumScriptBytes: ByteLimit,
    maximumLogPageBytes: ByteLimit
) derives CanEqual

object SlurmLocalConfig:
  def default(
      workspace: Path,
      baseEnvironment: Map[String, String]
  ): Either[ValidationFailure, SlurmLocalConfig] =
    for
      parser <- DataParserVersion.from("v0.0.43")
      timeout <- DurationMillis.from(30_000L)
    yield SlurmLocalConfig(
      workspace = workspace,
      executablePaths = SlurmExecutable.values.iterator.map(value => value -> value.fileName).toMap,
      baseEnvironment = baseEnvironment,
      allowedEnvironmentOverrides = Set.empty,
      dataParser = parser,
      commandPolicy = CommandPolicy(timeout, ByteLimit.defaultEvidence),
      maximumScriptBytes = ByteLimit.maximumCommandCapture,
      maximumLogPageBytes = ByteLimit.maximumLogPage
    )

final class SlurmLocal[F[_]] private[local] (
    val scheduler: SlurmCliScheduler[F],
    val logs: LocalLogReader[F]
) extends Scheduler[F]:
  export scheduler.{accounting, cancel, capabilities, observe, submit}

object SlurmLocal:
  def default[F[_]: Async](
      config: SlurmLocalConfig
  )(using Processes[F]): Resource[F, SlurmLocal[F]] =
    val executor = Fs2CommandExecutor[F](
      LocalCommandSettings(
        executablePaths = config.executablePaths,
        baseEnvironment = config.baseEnvironment,
        allowedEnvironmentOverrides = config.allowedEnvironmentOverrides,
        maximumCaptureBytes = config.commandPolicy.captureLimit
      )
    )
    val planner = LocalSubmissionPlanner[F](
      LocalWorkspaceSettings(config.workspace, config.maximumScriptBytes)
    )
    val scheduler = SlurmCliScheduler[F](
      executor,
      planner,
      SlurmCliSettings(config.dataParser, config.commandPolicy)
    )
    Resource.pure(
      SlurmLocal(
        scheduler,
        LocalLogReader[F](config.workspace, config.maximumLogPageBytes)
      )
    )
