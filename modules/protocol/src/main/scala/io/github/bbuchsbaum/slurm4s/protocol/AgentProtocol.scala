package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.remoteexec.kernel.TextIdentifier
import io.github.bbuchsbaum.slurm4s.core.BoundedEvidence
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion

object RequestId extends TextIdentifier("requestId", 200)
type RequestId = RequestId.Type

enum AgentMethod(val wireName: String) derives CanEqual:
  case Handshake extends AgentMethod("handshake")
  case Capabilities extends AgentMethod("capabilities")
  case SubmitOpaque extends AgentMethod("submit-opaque")
  case SubmitRegistered extends AgentMethod("submit-registered")
  case SubmitBatch extends AgentMethod("submit-batch")
  case SubmitScriptBatch extends AgentMethod("submit-script-batch")
  case Observe extends AgentMethod("observe")
  case Accounting extends AgentMethod("accounting")
  case Cancel extends AgentMethod("cancel")
  case ReadLog extends AgentMethod("read-log")
  case ReadResult extends AgentMethod("read-result")
  case ReadResults extends AgentMethod("read-results")
  case ReadScriptExit extends AgentMethod("read-script-exit")

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

final case class AgentExtensionFailure(name: String) derives CanEqual

final case class AgentEnvelope private (
    requestId: RequestId,
    protocol: ProtocolVersion,
    body: AgentBody,
    extensions: JsonObject
):
  def withBody(nextBody: AgentBody): AgentEnvelope =
    new AgentEnvelope(requestId, protocol, nextBody, extensions)

object AgentEnvelope:
  private[protocol] val reservedFields = Set("requestId", "kind", "method", "status", "body")

  def apply(requestId: RequestId, protocol: ProtocolVersion, body: AgentBody): AgentEnvelope =
    new AgentEnvelope(requestId, protocol, body, JsonObject.empty)

  def withExtensions(
      requestId: RequestId,
      protocol: ProtocolVersion,
      body: AgentBody,
      extensions: JsonObject
  ): Either[AgentExtensionFailure, AgentEnvelope] =
    extensions.keys.toVector.sorted.find(reservedFields.contains) match
      case Some(name) => Left(AgentExtensionFailure(name))
      case None       => Right(new AgentEnvelope(requestId, protocol, body, extensions))

  private[protocol] def decoded(
      requestId: RequestId,
      protocol: ProtocolVersion,
      body: AgentBody,
      extensions: JsonObject
  ): AgentEnvelope =
    new AgentEnvelope(requestId, protocol, body, extensions)

enum AgentFeature(val wireName: String) derives CanEqual:
  case PagedLogs extends AgentFeature("paged-logs")
  case OpaqueScripts extends AgentFeature("opaque-scripts")
  case SchedulerQueries extends AgentFeature("scheduler-queries")
  case Cancellation extends AgentFeature("cancellation")
  case RegisteredTasks extends AgentFeature("registered-tasks")
  case TypedResults extends AgentFeature("typed-results")
  case TypedBatches extends AgentFeature("typed-batches")
  case ScriptBatches extends AgentFeature("script-batches")
  case TerminationNotices extends AgentFeature("termination-notices")

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
    agentBuild: String,
    maximumLogPageBytes: Option[ByteLimit]
) derives CanEqual

object AgentFrameBudget:
  /** Space reserved for the versioned agent envelope, the owned log-result JSON fields, a maximum
    * length request id and file identity, and future additive metadata.
    *
    * A boundary test encodes the largest advertised page with maximum-width metadata. If the wire
    * shape grows beyond this reserve, that test requires the budget to be revised deliberately.
    */
  val LogResponseOverheadBytes: Int = 8 * 1024
  val TypedResultResponseOverheadBytes: Int = 128 * 1024

  /** Maximum raw log bytes whose base64 representation plus the reserved envelope overhead fits in
    * one frame. `None` means the frame is too small to advertise paged-log support.
    */
  def maximumLogPageBytes(maximumFrameBytes: ByteLimit): Option[ByteLimit] =
    val base64Capacity = maximumFrameBytes.value - LogResponseOverheadBytes
    val rawCapacity = (base64Capacity / 4) * 3
    ByteLimit.from(math.min(rawCapacity, ByteLimit.maximumLogPage.value)).toOption

  /** Maximum raw result-envelope bytes that fit beside the base64 durable handle, evidence fields,
    * and the versioned response envelope. The remote client and agent independently enforce this
    * derived limit, so an otherwise legal result request cannot terminate a session by overflowing
    * the negotiated frame.
    */
  def maximumTypedResultBytes(maximumFrameBytes: ByteLimit): Option[ByteLimit] =
    val base64Capacity = maximumFrameBytes.value - TypedResultResponseOverheadBytes
    val rawCapacity = (base64Capacity / 4) * 3
    ByteLimit.from(math.min(rawCapacity, ByteLimit.maximumCommandCapture.value)).toOption

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
