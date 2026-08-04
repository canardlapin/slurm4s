package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.BoundedEvidence

enum AgentFailureCode(val wireName: String) derives CanEqual:
  case ProtocolMismatch extends AgentFailureCode("protocol-mismatch")
  case AgentUnavailable extends AgentFailureCode("agent-unavailable")
  case AuthenticationFailed extends AgentFailureCode("authentication-failed")
  case TransportDisconnected extends AgentFailureCode("transport-disconnected")
  case RemoteCliFailure extends AgentFailureCode("remote-cli-failure")
  case RemoteAgentFailure extends AgentFailureCode("remote-agent-failure")
  case ProtocolViolation extends AgentFailureCode("protocol-violation")
  case AgentHandlerFailed extends AgentFailureCode("agent-handler-failed")

object AgentFailureCode:
  def fromWireName(raw: String): Option[AgentFailureCode] = values.find(_.wireName == raw)

/** The additive, explicitly owned body of a non-OK agent response.
  *
  * `message` is the complete legacy shape. Older clients ignore the added fields, while this
  * decoder accepts a body containing only that field. Unknown future codes degrade according to the
  * response status instead of making an otherwise intelligible failure undecodable.
  */
final case class AgentFailurePayload private (
    message: String,
    code: Option[AgentFailureCode],
    clientMajor: Option[Int],
    agentMajor: Option[Int],
    afterRequestWrite: Option[Boolean],
    causeClass: Option[String],
    evidence: Option[BoundedEvidence]
) derives CanEqual:
  def asJson: Json =
    val fields = Vector("message" -> Json.fromString(message)) ++
      code.toVector.map(value => "code" -> Json.fromString(value.wireName)) ++
      clientMajor.toVector.map(value => "clientMajor" -> Json.fromInt(value)) ++
      agentMajor.toVector.map(value => "agentMajor" -> Json.fromInt(value)) ++
      afterRequestWrite.toVector.map(value => "afterRequestWrite" -> Json.fromBoolean(value)) ++
      causeClass.toVector.map(value => "causeClass" -> Json.fromString(value)) ++
      evidence.toVector.map(value => "evidence" -> AgentDomainJson.encodeBoundedEvidence(value))
    Json.obj(fields*)

  def toFailure(
      status: AgentResponseStatus,
      transportEvidence: Option[BoundedEvidence]
  ): Either[String, AgentFailure] =
    val selectedEvidence = evidence.orElse(transportEvidence)
    code match
      case Some(value) => typedFailure(value, selectedEvidence)
      case None        => Right(fallback(status, selectedEvidence))

  private def typedFailure(
      value: AgentFailureCode,
      evidence: Option[BoundedEvidence]
  ): Either[String, AgentFailure] = value match
    case AgentFailureCode.ProtocolMismatch =>
      for
        client <- clientMajor.toRight("protocol-mismatch requires clientMajor")
        agent <- agentMajor.toRight("protocol-mismatch requires agentMajor")
      yield AgentFailure.ProtocolMismatch(client, agent)
    case AgentFailureCode.AgentUnavailable =>
      Right(AgentFailure.AgentUnavailable(message, evidence))
    case AgentFailureCode.AuthenticationFailed =>
      Right(AgentFailure.AuthenticationFailed(message, evidence))
    case AgentFailureCode.TransportDisconnected =>
      afterRequestWrite
        .toRight("transport-disconnected requires afterRequestWrite")
        .map(AgentFailure.TransportDisconnected(_, message, evidence))
    case AgentFailureCode.RemoteCliFailure =>
      Right(AgentFailure.RemoteCliFailure(message, evidence))
    case AgentFailureCode.RemoteAgentFailure | AgentFailureCode.AgentHandlerFailed =>
      Right(AgentFailure.RemoteAgentFailure(message, evidence))
    case AgentFailureCode.ProtocolViolation =>
      Right(AgentFailure.ProtocolViolation(message, evidence))

  private def fallback(
      status: AgentResponseStatus,
      evidence: Option[BoundedEvidence]
  ): AgentFailure = status match
    case AgentResponseStatus.DomainFailure   => AgentFailure.RemoteCliFailure(message, evidence)
    case AgentResponseStatus.ProtocolFailure => AgentFailure.ProtocolViolation(message, evidence)
    case AgentResponseStatus.InternalFailure => AgentFailure.RemoteAgentFailure(message, evidence)
    case AgentResponseStatus.Ok              =>
      AgentFailure.ProtocolViolation("an OK response carried a failure payload", evidence)

object AgentFailurePayload:
  def fromFailure(failure: AgentFailure): AgentFailurePayload = failure match
    case AgentFailure.ProtocolMismatch(clientMajor, agentMajor) =>
      payload(
        s"protocol major mismatch: client $clientMajor, agent $agentMajor",
        Some(AgentFailureCode.ProtocolMismatch),
        clientMajor = Some(clientMajor),
        agentMajor = Some(agentMajor)
      )
    case AgentFailure.AgentUnavailable(diagnostic, evidence) =>
      payload(
        diagnostic,
        Some(AgentFailureCode.AgentUnavailable),
        evidence = evidence
      )
    case AgentFailure.AuthenticationFailed(diagnostic, evidence) =>
      payload(
        diagnostic,
        Some(AgentFailureCode.AuthenticationFailed),
        evidence = evidence
      )
    case AgentFailure.TransportDisconnected(afterRequestWrite, diagnostic, evidence) =>
      payload(
        diagnostic,
        Some(AgentFailureCode.TransportDisconnected),
        afterRequestWrite = Some(afterRequestWrite),
        evidence = evidence
      )
    case AgentFailure.RemoteCliFailure(diagnostic, evidence) =>
      payload(
        diagnostic,
        Some(AgentFailureCode.RemoteCliFailure),
        evidence = evidence
      )
    case AgentFailure.RemoteAgentFailure(diagnostic, evidence) =>
      payload(
        diagnostic,
        Some(AgentFailureCode.RemoteAgentFailure),
        evidence = evidence
      )
    case AgentFailure.ProtocolViolation(diagnostic, evidence) =>
      payload(
        diagnostic,
        Some(AgentFailureCode.ProtocolViolation),
        evidence = evidence
      )

  def protocolViolation(message: String): AgentFailurePayload =
    payload(message, Some(AgentFailureCode.ProtocolViolation))

  def handlerFailure(message: String, causeClass: String): AgentFailurePayload =
    payload(
      message,
      Some(AgentFailureCode.AgentHandlerFailed),
      causeClass = Some(causeClass)
    )

  def decode(json: Json): Either[String, AgentFailurePayload] =
    for
      root <- json.asObject.toRight("agent failure payload must be an object")
      message <- root("message").flatMap(_.asString).toRight("failure message must be a string")
      codeText <- optionalString(root("code"), "failure code")
      clientMajor <- optionalInt(root("clientMajor"), "clientMajor")
      agentMajor <- optionalInt(root("agentMajor"), "agentMajor")
      afterRequestWrite <- optionalBoolean(root("afterRequestWrite"), "afterRequestWrite")
      causeClass <- optionalString(root("causeClass"), "causeClass")
      evidence <- optionalEvidence(root("evidence"))
      payload = AgentFailurePayload(
        message,
        codeText.flatMap(AgentFailureCode.fromWireName),
        clientMajor,
        agentMajor,
        afterRequestWrite,
        causeClass,
        evidence
      )
      _ <- payload.code match
        case Some(AgentFailureCode.ProtocolMismatch) if clientMajor.isEmpty || agentMajor.isEmpty =>
          Left("protocol-mismatch requires clientMajor and agentMajor")
        case Some(AgentFailureCode.TransportDisconnected) if afterRequestWrite.isEmpty =>
          Left("transport-disconnected requires afterRequestWrite")
        case _ => Right(())
    yield payload

  private def payload(
      message: String,
      code: Option[AgentFailureCode],
      clientMajor: Option[Int] = None,
      agentMajor: Option[Int] = None,
      afterRequestWrite: Option[Boolean] = None,
      causeClass: Option[String] = None,
      evidence: Option[BoundedEvidence] = None
  ): AgentFailurePayload =
    AgentFailurePayload(
      message,
      code,
      clientMajor,
      agentMajor,
      afterRequestWrite,
      causeClass,
      evidence
    )

  private def optionalString(value: Option[Json], field: String): Either[String, Option[String]] =
    value match
      case None                      => Right(None)
      case Some(json) if json.isNull => Right(None)
      case Some(json) => json.asString.toRight(s"$field must be a string").map(Some(_))

  private def optionalInt(value: Option[Json], field: String): Either[String, Option[Int]] =
    value match
      case None                      => Right(None)
      case Some(json) if json.isNull => Right(None)
      case Some(json)                =>
        json.asNumber.flatMap(_.toInt).toRight(s"$field must be an integer").map(Some(_))

  private def optionalBoolean(
      value: Option[Json],
      field: String
  ): Either[String, Option[Boolean]] =
    value match
      case None                      => Right(None)
      case Some(json) if json.isNull => Right(None)
      case Some(json)                =>
        json.asBoolean.toRight(s"$field must be a boolean").map(Some(_))

  private def optionalEvidence(value: Option[Json]): Either[String, Option[BoundedEvidence]] =
    value match
      case None                      => Right(None)
      case Some(json) if json.isNull => Right(None)
      case Some(json)                => AgentDomainJson.decodeBoundedEvidence(json).map(Some(_))
