package io.github.bbuchsbaum.scalaslurm.agent

import cats.Applicative
import cats.data.NonEmptyVector
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.AccountingBatch
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.CancellationAttempt
import io.github.bbuchsbaum.scalaslurm.core.JobRef
import io.github.bbuchsbaum.scalaslurm.core.JobRequest
import io.github.bbuchsbaum.scalaslurm.core.LogCursor
import io.github.bbuchsbaum.scalaslurm.core.LogReadResult
import io.github.bbuchsbaum.scalaslurm.core.LogRef
import io.github.bbuchsbaum.scalaslurm.core.NoResult
import io.github.bbuchsbaum.scalaslurm.core.ObservationBatch
import io.github.bbuchsbaum.scalaslurm.core.ProtocolVersion
import io.github.bbuchsbaum.scalaslurm.core.Scheduler
import io.github.bbuchsbaum.scalaslurm.core.SchedulerCapabilities
import io.github.bbuchsbaum.scalaslurm.core.SchedulerQueryResult
import io.github.bbuchsbaum.scalaslurm.core.SubmissionAttempt
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.AgentFailure
import io.github.bbuchsbaum.scalaslurm.protocol.AgentFeature
import io.github.bbuchsbaum.scalaslurm.protocol.HandshakeRequest
import io.github.bbuchsbaum.scalaslurm.protocol.HandshakeResponse

trait AgentLogReader[F[_]]:
  def read(ref: LogRef, cursor: LogCursor, maxBytes: ByteLimit): F[LogReadResult]

object AgentLogReader:
  def apply[F[_]](
      run: (LogRef, LogCursor, ByteLimit) => F[LogReadResult]
  ): AgentLogReader[F] =
    new AgentLogReader[F]:
      def read(ref: LogRef, cursor: LogCursor, maxBytes: ByteLimit): F[LogReadResult] =
        run(ref, cursor, maxBytes)

trait AgentApi[F[_]]:
  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]]
  def submitOpaque(request: JobRequest[NoResult]): F[AgentCall[SubmissionAttempt]]
  def observe(jobs: NonEmptyVector[JobRef]): F[AgentCall[SchedulerQueryResult[ObservationBatch]]]
  def accounting(jobs: NonEmptyVector[JobRef]): F[AgentCall[SchedulerQueryResult[AccountingBatch]]]
  def cancel(job: JobRef): F[AgentCall[CancellationAttempt]]
  def readLog(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit
  ): F[AgentCall[LogReadResult]]

final case class AgentServiceConfig(
    protocol: ProtocolVersion,
    maximumFrameBytes: ByteLimit,
    build: String,
    features: Set[AgentFeature]
) derives CanEqual

object AgentServiceConfig:
  val default: AgentServiceConfig = AgentServiceConfig(
    protocol = ProtocolVersion.v1,
    maximumFrameBytes = ByteLimit.maximumCommandCapture,
    build = "scala-slurm-agent/0.1",
    features = Set(
      AgentFeature.PagedLogs,
      AgentFeature.OpaqueScripts,
      AgentFeature.SchedulerQueries,
      AgentFeature.Cancellation
    )
  )

final class AgentService[F[_]: Applicative](
    scheduler: Scheduler[F],
    logs: AgentLogReader[F],
    config: AgentServiceConfig = AgentServiceConfig.default
):
  def handshake(
      clientProtocol: ProtocolVersion,
      request: HandshakeRequest
  ): F[AgentCall[HandshakeResponse]] =
    if clientProtocol.major != config.protocol.major then
      AgentCall
        .Failed(
          AgentFailure.ProtocolMismatch(
            clientMajor = clientProtocol.major,
            agentMajor = config.protocol.major
          )
        )
        .pure[F]
    else
      AgentCall
        .Succeeded(
          HandshakeResponse(
            agentProtocol = config.protocol,
            maximumFrameBytes = ByteLimit
              .from(
                math.min(request.maximumFrameBytes.value, config.maximumFrameBytes.value)
              )
              .fold(problem => throw new IllegalStateException(problem.reason), identity),
            availableFeatures = request.requestedFeatures.intersect(config.features),
            agentBuild = config.build
          )
        )
        .pure[F]

  private[agent] val api: AgentApi[F] = new AgentApi[F]:
    def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
      scheduler.capabilities.map(AgentCall.Succeeded.apply)

    def submitOpaque(request: JobRequest[NoResult]): F[AgentCall[SubmissionAttempt]] =
      scheduler.submit(request).map(AgentCall.Succeeded.apply)

    def observe(
        jobs: NonEmptyVector[JobRef]
    ): F[AgentCall[SchedulerQueryResult[ObservationBatch]]] =
      scheduler.observe(jobs).map(AgentCall.Succeeded.apply)

    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): F[AgentCall[SchedulerQueryResult[AccountingBatch]]] =
      scheduler.accounting(jobs).map(AgentCall.Succeeded.apply)

    def cancel(job: JobRef): F[AgentCall[CancellationAttempt]] =
      scheduler.cancel(job).map(AgentCall.Succeeded.apply)

    def readLog(
        ref: LogRef,
        cursor: LogCursor,
        maxBytes: ByteLimit
    ): F[AgentCall[LogReadResult]] =
      logs.read(ref, cursor, maxBytes).map(AgentCall.Succeeded.apply)

final class InProcessAgentClient[F[_]: Async] private (
    connected: Ref[F, Boolean],
    delegate: AgentApi[F]
) extends AgentApi[F]:
  private def whileConnected[A](operation: F[AgentCall[A]]): F[AgentCall[A]] =
    connected.get.ifM(
      operation,
      AgentCall
        .Failed(
          AgentFailure.TransportDisconnected(
            afterRequestWrite = false,
            diagnostic = "agent session is disconnected",
            evidence = None
          )
        )
        .pure[F]
    )

  def disconnect: F[Unit] = connected.set(false)

  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
    whileConnected(delegate.capabilities)

  def submitOpaque(request: JobRequest[NoResult]): F[AgentCall[SubmissionAttempt]] =
    whileConnected(delegate.submitOpaque(request))

  def observe(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[ObservationBatch]]] =
    whileConnected(delegate.observe(jobs))

  def accounting(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[AccountingBatch]]] =
    whileConnected(delegate.accounting(jobs))

  def cancel(job: JobRef): F[AgentCall[CancellationAttempt]] =
    whileConnected(delegate.cancel(job))

  def readLog(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    whileConnected(delegate.readLog(ref, cursor, maxBytes))

object InProcessAgentClient:
  def connect[F[_]: Async](
      service: AgentService[F],
      clientProtocol: ProtocolVersion = ProtocolVersion.v1,
      maximumFrameBytes: ByteLimit = ByteLimit.maximumCommandCapture,
      features: Set[AgentFeature] = AgentServiceConfig.default.features
  ): F[AgentCall[InProcessAgentClient[F]]] =
    val request = HandshakeRequest(maximumFrameBytes, features)
    service.handshake(clientProtocol, request).flatMap {
      case AgentCall.Succeeded(_) =>
        Ref
          .of[F, Boolean](true)
          .map(ref => AgentCall.Succeeded(InProcessAgentClient(ref, service.api)))
      case AgentCall.Failed(failure) => AgentCall.Failed(failure).pure[F]
    }
