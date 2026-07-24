package io.github.bbuchsbaum.scalaslurm.ssh

import cats.Applicative
import cats.data.NonEmptyVector
import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all.*
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.agent.AgentApi
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.FrameLimits

final case class SlurmSshConfig(
    connection: SshConnection,
    frameLimits: FrameLimits,
    exchangePolicy: SshExchangePolicy
) derives CanEqual

object SlurmSshConfig:
  def default(connection: SshConnection): Either[ValidationFailure, SlurmSshConfig] =
    DurationMillis
      .from(30_000L)
      .map(timeout =>
        SlurmSshConfig(
          connection,
          FrameLimits.default,
          SshExchangePolicy(timeout)
        )
      )

final class RemoteSlurm[F[_]: Applicative] private[ssh] (
    val connection: AgentCall[SshAgentApi[F]]
) extends AgentApi[F]:
  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
    connected(_.capabilities)

  def submitOpaque(request: JobRequest[NoResult]): F[AgentCall[SubmissionAttempt]] =
    connected(_.submitOpaque(request))

  def observe(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[ObservationBatch]]] =
    connected(_.observe(jobs))

  def accounting(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[AccountingBatch]]] =
    connected(_.accounting(jobs))

  def cancel(job: JobRef): F[AgentCall[CancellationAttempt]] =
    connected(_.cancel(job))

  def readLog(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    connected(_.readLog(ref, cursor, maxBytes))

  private def connected[A](run: SshAgentApi[F] => F[AgentCall[A]]): F[AgentCall[A]] =
    connection match
      case AgentCall.Succeeded(api)  => run(api)
      case AgentCall.Failed(failure) => AgentCall.Failed(failure).pure[F]

object Slurm:
  def overSsh[F[_]: Async](
      config: SlurmSshConfig
  )(using Processes[F]): Either[ValidationFailure, Resource[F, RemoteSlurm[F]]] =
    SshCommand.agent(config.connection).map { launch =>
      val wire = SshAgentWireClient(
        launch,
        SystemSshProcessRunner[F],
        config.frameLimits,
        config.exchangePolicy
      )
      Resource.eval(SshAgentApi.connect[F](wire).map(RemoteSlurm(_)))
    }
