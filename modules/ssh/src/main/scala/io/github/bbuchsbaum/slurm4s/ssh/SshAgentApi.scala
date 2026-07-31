package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.Ref
import cats.effect.kernel.Concurrent
import cats.syntax.all.*
import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*

final class SshAgentApi[F[_]: Concurrent] private (
    wire: SshAgentWireClient[F],
    sequence: Ref[F, Long],
    val handshake: HandshakeResponse
) extends AgentApi[F]:
  val remoteCapabilities: RemoteCapabilities =
    val pagedLogs =
      handshake.availableFeatures.contains(AgentFeature.PagedLogs) &&
        handshake.maximumLogPageBytes.nonEmpty
    RemoteCapabilities(
      mode = RemoteMode.Agent,
      protocolFrames = true,
      pagedLogs = pagedLogs,
      durableControl = false,
      degradationReasons = Option.when(!pagedLogs)("agent did not negotiate paged logs").toVector :+
        "durable control requires the P3 controller"
    )

  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
    call(AgentMethod.Capabilities, Json.obj(), AgentDomainJson.decodeCapabilities)

  def submitOpaque(spec: LaunchSpec): F[AgentCall[SubmissionAttempt]] =
    AgentDomainJson.encodeSubmitRequest(spec) match
      case Left(problem)  => protocolFailure(problem).pure[F]
      case Right(payload) =>
        call(AgentMethod.SubmitOpaque, payload, AgentDomainJson.decodeSubmission)

  def submitRegistered(
      request: RemoteRegisteredTaskRequest
  ): F[AgentCall[RemoteRegisteredSubmission]] =
    if !handshake.availableFeatures.contains(AgentFeature.RegisteredTasks) then
      protocolFailure("registered tasks were not negotiated").pure[F]
    else
      AgentDomainJson.encodeRemoteTaskRequest(request) match
        case Left(problem)  => protocolFailure(problem).pure[F]
        case Right(payload) =>
          call(
            AgentMethod.SubmitRegistered,
            payload,
            AgentDomainJson.decodeRemoteSubmission
          )

  def submitBatch(
      request: RemoteRegisteredBatchRequest
  ): F[AgentCall[RemoteRegisteredBatchSubmission]] =
    if !handshake.availableFeatures.contains(AgentFeature.TypedBatches) then
      protocolFailure("typed batches were not negotiated").pure[F]
    else
      AgentDomainJson.encodeRemoteBatchRequest(request) match
        case Left(problem)  => protocolFailure(problem).pure[F]
        case Right(payload) =>
          call(
            AgentMethod.SubmitBatch,
            payload,
            AgentDomainJson.decodeRemoteBatchSubmission
          )

  def submitScriptBatch(
      request: RemoteScriptBatchRequest
  ): F[AgentCall[RemoteScriptBatchSubmission]] =
    if !handshake.availableFeatures.contains(AgentFeature.ScriptBatches) then
      protocolFailure("script batches were not negotiated").pure[F]
    else
      AgentDomainJson.encodeRemoteScriptBatchRequest(request) match
        case Left(problem)  => protocolFailure(problem).pure[F]
        case Right(payload) =>
          call(
            AgentMethod.SubmitScriptBatch,
            payload,
            AgentDomainJson.decodeRemoteScriptBatchSubmission
          )

  def observe(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[ObservationBatch]]] =
    call(
      AgentMethod.Observe,
      AgentDomainJson.encodeJobRefs(jobs),
      AgentDomainJson.decodeObservation
    )

  def accounting(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[AccountingBatch]]] =
    call(
      AgentMethod.Accounting,
      AgentDomainJson.encodeJobRefs(jobs),
      AgentDomainJson.decodeAccounting
    )

  def cancel(job: JobRef): F[AgentCall[CancellationAttempt]] =
    call(AgentMethod.Cancel, AgentDomainJson.encodeJobRef(job), AgentDomainJson.decodeCancellation)

  def readLog(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    handshake.maximumLogPageBytes match
      case Some(maximum) if maxBytes.value <= maximum.value =>
        call(
          AgentMethod.ReadLog,
          AgentDomainJson.encodeLogRequest(ref, cursor, maxBytes),
          AgentDomainJson.decodeLogResult
        )
      case Some(maximum) =>
        protocolFailure(
          s"requested log page ${maxBytes.value} exceeds negotiated maximum ${maximum.value}"
        ).pure[F]
      case None =>
        protocolFailure("paged logs were not negotiated").pure[F]

  def readResult(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit
  ): F[AgentCall[RemoteResultRead]] =
    if !handshake.availableFeatures.contains(AgentFeature.TypedResults) then
      protocolFailure("typed results were not negotiated").pure[F]
    else
      AgentFrameBudget.maximumTypedResultBytes(handshake.maximumFrameBytes) match
        case Some(maximum) if maximumBytes.value <= maximum.value =>
          call(
            AgentMethod.ReadResult,
            AgentDomainJson.encodeRemoteResultReadRequest(ref, maximumBytes),
            AgentDomainJson.decodeRemoteResultRead(_, maximumBytes)
          )
        case Some(maximum) =>
          protocolFailure(
            s"requested result envelope ${maximumBytes.value} exceeds negotiated maximum ${maximum.value}"
          ).pure[F]
        case None =>
          protocolFailure("the negotiated frame cannot carry typed results").pure[F]

  def readScriptExit(
      ref: RemoteScriptExitRef
  ): F[AgentCall[RemoteScriptExitRead]] =
    if !handshake.availableFeatures.contains(AgentFeature.ScriptBatches) then
      protocolFailure("script batch exits were not negotiated").pure[F]
    else
      call(
        AgentMethod.ReadScriptExit,
        AgentDomainJson.encodeRemoteScriptExitReadRequest(ref),
        AgentDomainJson.decodeRemoteScriptExitRead
      )

  private[ssh] def performHandshake(request: HandshakeRequest): F[AgentCall[HandshakeResponse]] =
    call(AgentMethod.Handshake, HandshakeJson.request(request), HandshakeJson.decodeResponse).map {
      case AgentCall.Succeeded(response)
          if response.agentProtocol.major != ProtocolVersion.v1.major =>
        AgentCall.Failed(
          AgentFailure.ProtocolMismatch(ProtocolVersion.v1.major, response.agentProtocol.major)
        )
      case result => result
    }

  private def call[A](
      method: AgentMethod,
      payload: Json,
      decode: Json => Either[String, A]
  ): F[AgentCall[A]] =
    nextRequestId.flatMap { requestId =>
      wire
        .roundTrip(
          AgentEnvelope(
            requestId = requestId,
            protocol = ProtocolVersion.v1,
            body = AgentBody.Request(method, payload)
          )
        )
        .map {
          case AgentCall.Succeeded(AgentEnvelope(_, _, AgentBody.Response(_, body), _)) =>
            decode(body).fold(problem => protocolFailure(problem), AgentCall.Succeeded.apply)
          case AgentCall.Succeeded(_) =>
            protocolFailure("agent returned a request instead of a response")
          case AgentCall.Failed(failure) => AgentCall.Failed(failure)
        }
    }

  private def nextRequestId: F[RequestId] = sequence.modify { current =>
    val next = current + 1L
    val requestId = RequestId.unsafeFrom(s"ssh-$next")
    (next, requestId)
  }

  private def protocolFailure[A](problem: String): AgentCall[A] =
    AgentCall.Failed(AgentFailure.ProtocolViolation(problem, None))

object SshAgentApi:
  def connect[F[_]: Concurrent](
      wire: SshAgentWireClient[F],
      maximumFrameBytes: ByteLimit = ByteLimit.maximumCommandCapture,
      features: Set[AgentFeature] = Set(
        AgentFeature.PagedLogs,
        AgentFeature.OpaqueScripts,
        AgentFeature.SchedulerQueries,
        AgentFeature.Cancellation,
        AgentFeature.RegisteredTasks,
        AgentFeature.TypedResults,
        AgentFeature.TypedBatches,
        AgentFeature.ScriptBatches
      )
  ): F[AgentCall[SshAgentApi[F]]] =
    Ref.of[F, Long](0L).flatMap { sequence =>
      val provisional = SshAgentApi(
        wire,
        sequence,
        HandshakeResponse(
          ProtocolVersion.v1,
          maximumFrameBytes,
          Set.empty,
          "handshake-pending",
          None
        )
      )
      provisional.performHandshake(HandshakeRequest(maximumFrameBytes, features)).map {
        case AgentCall.Succeeded(response) =>
          AgentCall.Succeeded(
            SshAgentApi(wire.withMaximumFrameBytes(response.maximumFrameBytes), sequence, response)
          )
        case AgentCall.Failed(failure) => AgentCall.Failed(failure)
      }
    }

object DirectSshCompatibility:
  val capabilities: RemoteCapabilities = RemoteCapabilities(
    mode = RemoteMode.DirectCompatibility,
    protocolFrames = false,
    pagedLogs = false,
    durableControl = false,
    degradationReasons = Vector(
      "remote agent is unavailable",
      "direct SSH cannot promise reconnectable protocol sessions",
      "direct SSH cannot provide agent-paged logs"
    )
  )
