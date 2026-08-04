package io.github.bbuchsbaum.slurm4s.agent

import cats.Applicative
import cats.data.NonEmptyVector
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.AccountingBatch
import io.github.bbuchsbaum.slurm4s.protocol.AgentApi
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.CancellationAttempt
import io.github.bbuchsbaum.slurm4s.core.JobRef
import io.github.bbuchsbaum.slurm4s.core.LaunchSpec
import io.github.bbuchsbaum.slurm4s.core.LogCursor
import io.github.bbuchsbaum.slurm4s.core.LogReadResult
import io.github.bbuchsbaum.slurm4s.core.LogRef
import io.github.bbuchsbaum.slurm4s.core.ObservationBatch
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.Scheduler
import io.github.bbuchsbaum.slurm4s.core.SchedulerCapabilities
import io.github.bbuchsbaum.slurm4s.core.SchedulerQueryResult
import io.github.bbuchsbaum.slurm4s.core.SubmissionAttempt
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFrameBudget
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import io.github.bbuchsbaum.slurm4s.protocol.AgentFeature
import io.github.bbuchsbaum.slurm4s.protocol.HandshakeRequest
import io.github.bbuchsbaum.slurm4s.protocol.HandshakeResponse
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredSubmission
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchRequest
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchSubmission
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredTaskRequest
import io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRead
import io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRef
import io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchRequest
import io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchSubmission
import io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRead
import io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRef

trait AgentLogReader[F[_]]:
  def read(ref: LogRef, cursor: LogCursor, maxBytes: ByteLimit): F[LogReadResult]

object AgentLogReader:
  def apply[F[_]](
      run: (LogRef, LogCursor, ByteLimit) => F[LogReadResult]
  ): AgentLogReader[F] =
    new AgentLogReader[F]:
      def read(ref: LogRef, cursor: LogCursor, maxBytes: ByteLimit): F[LogReadResult] =
        run(ref, cursor, maxBytes)

trait AgentRegisteredTaskService[F[_]]:
  def submit(request: RemoteRegisteredTaskRequest): F[AgentCall[RemoteRegisteredSubmission]]
  def submitBatch(
      request: RemoteRegisteredBatchRequest
  ): F[AgentCall[RemoteRegisteredBatchSubmission]]
  def submitScriptBatch(
      request: RemoteScriptBatchRequest
  ): F[AgentCall[RemoteScriptBatchSubmission]]
  def readResult(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit
  ): F[AgentCall[RemoteResultRead]]
  def readScriptExit(
      ref: RemoteScriptExitRef
  ): F[AgentCall[RemoteScriptExitRead]]

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
    build = "slurm4s-agent/0.1",
    features = Set(
      AgentFeature.PagedLogs,
      AgentFeature.OpaqueScripts,
      AgentFeature.SchedulerQueries,
      AgentFeature.Cancellation,
      AgentFeature.RegisteredTasks,
      AgentFeature.TypedResults,
      AgentFeature.TypedBatches,
      AgentFeature.ScriptBatches,
      AgentFeature.TerminationNotices
    )
  )

final class AgentService[F[_]: Applicative](
    scheduler: Scheduler[F],
    logs: AgentLogReader[F],
    registeredTasks: Option[AgentRegisteredTaskService[F]] = None,
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
      val negotiatedFrameBytes =
        if request.maximumFrameBytes.value <= config.maximumFrameBytes.value then
          request.maximumFrameBytes
        else config.maximumFrameBytes
      val maximumLogPageBytes = AgentFrameBudget.maximumLogPageBytes(negotiatedFrameBytes)
      val maximumTypedResultBytes =
        AgentFrameBudget.maximumTypedResultBytes(negotiatedFrameBytes)
      val negotiatedFeatures =
        request.requestedFeatures
          .intersect(config.features)
          .removedAll(Option.when(maximumLogPageBytes.isEmpty)(AgentFeature.PagedLogs))
          .removedAll(
            Option
              .when(registeredTasks.isEmpty || maximumTypedResultBytes.isEmpty)(
                Set(
                  AgentFeature.RegisteredTasks,
                  AgentFeature.TypedResults,
                  AgentFeature.TypedBatches
                )
              )
              .getOrElse(Set.empty)
          )
          .removedAll(
            Option.when(registeredTasks.isEmpty)(AgentFeature.ScriptBatches)
          )
      AgentCall
        .Succeeded(
          HandshakeResponse(
            agentProtocol = config.protocol,
            maximumFrameBytes = negotiatedFrameBytes,
            availableFeatures = negotiatedFeatures,
            agentBuild = config.build,
            maximumLogPageBytes = maximumLogPageBytes
          )
        )
        .pure[F]

  private val maximumLogPageBytes =
    AgentFrameBudget.maximumLogPageBytes(config.maximumFrameBytes)
  private val maximumTypedResultBytes =
    AgentFrameBudget.maximumTypedResultBytes(config.maximumFrameBytes)

  private[agent] val api: AgentApi[F] = new AgentApi[F]:
    def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
      scheduler.capabilities.map(AgentCall.Succeeded.apply)

    def submitOpaque(spec: LaunchSpec): F[AgentCall[SubmissionAttempt]] =
      scheduler.submit(spec).map(AgentCall.Succeeded.apply)

    def submitRegistered(
        request: RemoteRegisteredTaskRequest
    ): F[AgentCall[RemoteRegisteredSubmission]] =
      registeredTasks.fold(
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation(
              "registered tasks are not configured on this agent",
              None
            )
          )
          .pure[F]
      )(_.submit(request))

    def submitBatch(
        request: RemoteRegisteredBatchRequest
    ): F[AgentCall[RemoteRegisteredBatchSubmission]] =
      registeredTasks.fold(
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation(
              "registered task batches are not configured on this agent",
              None
            )
          )
          .pure[F]
      )(_.submitBatch(request))

    def submitScriptBatch(
        request: RemoteScriptBatchRequest
    ): F[AgentCall[RemoteScriptBatchSubmission]] =
      registeredTasks.fold(
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation(
              "script batches are not configured on this agent",
              None
            )
          )
          .pure[F]
      )(_.submitScriptBatch(request))

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
      maximumLogPageBytes match
        case Some(maximum) if maxBytes.value <= maximum.value =>
          logs.read(ref, cursor, maxBytes).map(AgentCall.Succeeded.apply)
        case Some(maximum) =>
          AgentCall
            .Failed(
              AgentFailure.ProtocolViolation(
                s"requested log page ${maxBytes.value} exceeds maximum ${maximum.value}",
                None
              )
            )
            .pure[F]
        case None =>
          AgentCall
            .Failed(
              AgentFailure.ProtocolViolation(
                "the configured agent frame limit cannot carry a log page",
                None
              )
            )
            .pure[F]

    def readResult(
        ref: RemoteResultRef,
        maximumBytes: ByteLimit
    ): F[AgentCall[RemoteResultRead]] =
      maximumTypedResultBytes match
        case Some(maximum) if maximumBytes.value <= maximum.value =>
          registeredTasks.fold(
            AgentCall
              .Failed(
                AgentFailure.ProtocolViolation(
                  "typed results are not configured on this agent",
                  None
                )
              )
              .pure[F]
          )(_.readResult(ref, maximumBytes))
        case Some(maximum) =>
          AgentCall
            .Failed(
              AgentFailure.ProtocolViolation(
                s"requested result envelope ${maximumBytes.value} exceeds maximum ${maximum.value}",
                None
              )
            )
            .pure[F]
        case None =>
          AgentCall
            .Failed(
              AgentFailure.ProtocolViolation(
                "the configured agent frame limit cannot carry a typed result",
                None
              )
            )
            .pure[F]

    def readScriptExit(
        ref: RemoteScriptExitRef
    ): F[AgentCall[RemoteScriptExitRead]] =
      registeredTasks.fold(
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation(
              "script batch exits are not configured on this agent",
              None
            )
          )
          .pure[F]
      )(_.readScriptExit(ref))

final class InProcessAgentClient[F[_]: Async] private (
    connected: Ref[F, Boolean],
    delegate: AgentApi[F],
    maximumLogPageBytes: Option[ByteLimit],
    maximumTypedResultBytes: Option[ByteLimit]
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

  def submitOpaque(spec: LaunchSpec): F[AgentCall[SubmissionAttempt]] =
    whileConnected(delegate.submitOpaque(spec))

  def submitRegistered(
      request: RemoteRegisteredTaskRequest
  ): F[AgentCall[RemoteRegisteredSubmission]] =
    whileConnected(delegate.submitRegistered(request))

  def submitBatch(
      request: RemoteRegisteredBatchRequest
  ): F[AgentCall[RemoteRegisteredBatchSubmission]] =
    whileConnected(delegate.submitBatch(request))

  def submitScriptBatch(
      request: RemoteScriptBatchRequest
  ): F[AgentCall[RemoteScriptBatchSubmission]] =
    whileConnected(delegate.submitScriptBatch(request))

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
    maximumLogPageBytes match
      case Some(maximum) if maxBytes.value <= maximum.value =>
        whileConnected(delegate.readLog(ref, cursor, maxBytes))
      case Some(maximum) =>
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation(
              s"requested log page ${maxBytes.value} exceeds negotiated maximum ${maximum.value}",
              None
            )
          )
          .pure[F]
      case None =>
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation("paged logs were not negotiated", None)
          )
          .pure[F]

  def readResult(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit
  ): F[AgentCall[RemoteResultRead]] =
    maximumTypedResultBytes match
      case Some(maximum) if maximumBytes.value <= maximum.value =>
        whileConnected(delegate.readResult(ref, maximumBytes))
      case Some(maximum) =>
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation(
              s"requested result envelope ${maximumBytes.value} exceeds negotiated maximum ${maximum.value}",
              None
            )
          )
          .pure[F]
      case None =>
        AgentCall
          .Failed(
            AgentFailure.ProtocolViolation("typed results were not negotiated", None)
          )
          .pure[F]

  def readScriptExit(
      ref: RemoteScriptExitRef
  ): F[AgentCall[RemoteScriptExitRead]] =
    whileConnected(delegate.readScriptExit(ref))

object InProcessAgentClient:
  def connect[F[_]: Async](
      service: AgentService[F],
      clientProtocol: ProtocolVersion = ProtocolVersion.v1,
      maximumFrameBytes: ByteLimit = ByteLimit.maximumCommandCapture,
      features: Set[AgentFeature] = AgentServiceConfig.default.features
  ): F[AgentCall[InProcessAgentClient[F]]] =
    val request = HandshakeRequest(maximumFrameBytes, features)
    service.handshake(clientProtocol, request).flatMap {
      case AgentCall.Succeeded(response) =>
        Ref
          .of[F, Boolean](true)
          .map(ref =>
            AgentCall.Succeeded(
              InProcessAgentClient(
                ref,
                service.api,
                response.maximumLogPageBytes,
                AgentFrameBudget.maximumTypedResultBytes(response.maximumFrameBytes)
              )
            )
          )
      case AgentCall.Failed(failure) => AgentCall.Failed(failure).pure[F]
    }
