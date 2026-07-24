package io.github.bbuchsbaum.scalaslurm.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.scalaslurm.core.BoundedEvidence
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ProtocolVersion
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure

object RequestId:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("requestId", "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure("requestId", "must not be empty"))
    else if raw.length > 200 then
      Left(ValidationFailure("requestId", "must contain at most 200 characters"))
    else if raw.exists(character => character.isControl || character.isWhitespace) then
      Left(ValidationFailure("requestId", "must not contain whitespace or control characters"))
    else Right(raw)

  extension (requestId: Type) def value: String = requestId

type RequestId = RequestId.Type

enum AgentMethod(val wireName: String) derives CanEqual:
  case Handshake extends AgentMethod("handshake")
  case Capabilities extends AgentMethod("capabilities")
  case SubmitOpaque extends AgentMethod("submit-opaque")
  case Observe extends AgentMethod("observe")
  case Accounting extends AgentMethod("accounting")
  case Cancel extends AgentMethod("cancel")
  case ReadLog extends AgentMethod("read-log")

object AgentMethod:
  def fromWireName(raw: String): Option[AgentMethod] = values.find(_.wireName == raw)

enum AgentResponseStatus(val wireName: String) derives CanEqual:
  case Ok extends AgentResponseStatus("ok")
  case DomainFailure extends AgentResponseStatus("domain-failure")
  case ProtocolFailure extends AgentResponseStatus("protocol-failure")
  case InternalFailure extends AgentResponseStatus("internal-failure")

object AgentResponseStatus:
  def fromWireName(raw: String): Option[AgentResponseStatus] = values.find(_.wireName == raw)

enum AgentBody:
  case Request(method: AgentMethod, payload: Json)
  case Response(status: AgentResponseStatus, payload: Json)

final case class AgentEnvelope(
    requestId: RequestId,
    protocol: ProtocolVersion,
    body: AgentBody,
    extensions: JsonObject = JsonObject.empty
)

enum AgentFeature(val wireName: String) derives CanEqual:
  case PagedLogs extends AgentFeature("paged-logs")
  case OpaqueScripts extends AgentFeature("opaque-scripts")
  case SchedulerQueries extends AgentFeature("scheduler-queries")
  case Cancellation extends AgentFeature("cancellation")

object AgentFeature:
  def fromWireName(raw: String): Option[AgentFeature] = values.find(_.wireName == raw)

final case class HandshakeRequest(
    maximumFrameBytes: ByteLimit,
    requestedFeatures: Set[AgentFeature]
) derives CanEqual

final case class HandshakeResponse(
    agentProtocol: ProtocolVersion,
    maximumFrameBytes: ByteLimit,
    availableFeatures: Set[AgentFeature],
    agentBuild: String
) derives CanEqual

enum RemoteMode derives CanEqual:
  case Agent
  case DirectCompatibility

final case class RemoteCapabilities(
    mode: RemoteMode,
    protocolFrames: Boolean,
    pagedLogs: Boolean,
    durableControl: Boolean,
    degradationReasons: Vector[String]
) derives CanEqual

enum AgentFailure derives CanEqual:
  case ProtocolMismatch(clientMajor: Int, agentMajor: Int)
  case AgentUnavailable(diagnostic: String, evidence: Option[BoundedEvidence])
  case AuthenticationFailed(diagnostic: String, evidence: Option[BoundedEvidence])
  case TransportDisconnected(
      afterRequestWrite: Boolean,
      diagnostic: String,
      evidence: Option[BoundedEvidence]
  )
  case RemoteCliFailure(diagnostic: String, evidence: Option[BoundedEvidence])
  case RemoteAgentFailure(diagnostic: String, evidence: Option[BoundedEvidence])
  case ProtocolViolation(diagnostic: String, evidence: Option[BoundedEvidence])

enum AgentCall[+A]:
  case Succeeded(value: A)
  case Failed(failure: AgentFailure)
