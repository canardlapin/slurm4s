package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.SchemaId
import io.github.bbuchsbaum.slurm4s.core.codec.CodecFailure
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson
import io.github.bbuchsbaum.slurm4s.core.codec.WireEnvelope
import scodec.bits.ByteVector

enum AgentCodecFailure derives CanEqual:
  case Envelope(failure: CodecFailure)
  case WrongSchema(received: String)
  case InvalidMessage(message: String)

object AgentMessageCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.agent-message")

  def encode(message: AgentEnvelope): ByteVector =
    val bodyFields = message.body match
      case AgentBody.Request(method, payload) =>
        JsonObject(
          "requestId" -> Json.fromString(message.requestId.value),
          "kind" -> Json.fromString("request"),
          "method" -> Json.fromString(method.wireName),
          "body" -> payload
        )
      case AgentBody.Response(status, payload) =>
        JsonObject(
          "requestId" -> Json.fromString(message.requestId.value),
          "kind" -> Json.fromString("response"),
          "status" -> Json.fromString(status.wireName),
          "body" -> payload
        )
    val payload = message.extensions.toIterable.foldLeft(bodyFields) {
      case (fields, (name, value)) => fields.add(name, value)
    }
    VersionedJson.encode(
      WireEnvelope(
        protocol = message.protocol,
        schema = schema,
        payload = Json.fromJsonObject(payload)
      )
    )

  def decode(bytes: ByteVector): Either[AgentCodecFailure, AgentEnvelope] =
    for
      envelope <- VersionedJson.decode(bytes).left.map(AgentCodecFailure.Envelope.apply)
      _ <- Either.cond(
        envelope.schema == schema,
        (),
        AgentCodecFailure.WrongSchema(envelope.schema.value)
      )
      root <- envelope.payload.asObject.toRight(
        AgentCodecFailure.InvalidMessage("message payload must be an object")
      )
      requestIdText <- requiredString(root, "requestId")
      requestId <- RequestId
        .from(requestIdText)
        .left
        .map(problem => AgentCodecFailure.InvalidMessage(problem.reason))
      kind <- requiredString(root, "kind")
      payload <- root("body").toRight(AgentCodecFailure.InvalidMessage("missing body"))
      body <- kind match
        case "request" =>
          for
            methodText <- requiredString(root, "method")
            method <- AgentMethod
              .fromWireName(methodText)
              .toRight(AgentCodecFailure.InvalidMessage(s"unknown method: $methodText"))
          yield AgentBody.Request(method, payload)
        case "response" =>
          for
            statusText <- requiredString(root, "status")
            status <- AgentResponseStatus
              .fromWireName(statusText)
              .toRight(AgentCodecFailure.InvalidMessage(s"unknown status: $statusText"))
          yield AgentBody.Response(status, payload)
        case other => Left(AgentCodecFailure.InvalidMessage(s"unknown message kind: $other"))
    yield AgentEnvelope.decoded(
      requestId = requestId,
      protocol = envelope.protocol,
      body = body,
      extensions = JsonObject.fromIterable(
        root.toIterable.filterNot(entry => AgentEnvelope.reservedFields.contains(entry._1))
      )
    )

  private def requiredString(
      fields: JsonObject,
      name: String
  ): Either[AgentCodecFailure, String] =
    fields(name)
      .flatMap(_.asString)
      .toRight(AgentCodecFailure.InvalidMessage(s"$name must be a string"))

object HandshakeJson:
  def request(value: HandshakeRequest): Json =
    Json.obj(
      "maximumFrameBytes" -> Json.fromInt(value.maximumFrameBytes.value),
      "requestedFeatures" -> Json.fromValues(
        value.requestedFeatures.toVector
          .sortBy(_.wireName)
          .map(feature => Json.fromString(feature.wireName))
      )
    )

  def decodeRequest(json: Json): Either[String, HandshakeRequest] =
    for
      root <- json.asObject.toRight("handshake request must be an object")
      maximum <- root("maximumFrameBytes")
        .flatMap(_.asNumber.flatMap(_.toInt))
        .toRight("maximumFrameBytes must be an integer")
      limit <- io.github.bbuchsbaum.slurm4s.core.ByteLimit
        .from(maximum)
        .left
        .map(_.reason)
      features <- decodeFeatures(root("requestedFeatures"))
    yield HandshakeRequest(limit, features)

  def response(value: HandshakeResponse): Json =
    Json.obj(
      "agentProtocol" -> Json.obj(
        "major" -> Json.fromInt(value.agentProtocol.major),
        "minor" -> Json.fromInt(value.agentProtocol.minor)
      ),
      "maximumFrameBytes" -> Json.fromInt(value.maximumFrameBytes.value),
      "maximumLogPageBytes" -> value.maximumLogPageBytes
        .map(limit => Json.fromInt(limit.value))
        .getOrElse(Json.Null),
      "availableFeatures" -> Json.fromValues(
        value.availableFeatures.toVector
          .sortBy(_.wireName)
          .map(feature => Json.fromString(feature.wireName))
      ),
      "agentBuild" -> Json.fromString(value.agentBuild)
    )

  def decodeResponse(json: Json): Either[String, HandshakeResponse] =
    for
      root <- json.asObject.toRight("handshake response must be an object")
      protocol <- decodeProtocol(root("agentProtocol"))
      maximum <- root("maximumFrameBytes")
        .flatMap(_.asNumber.flatMap(_.toInt))
        .toRight("maximumFrameBytes must be an integer")
      limit <- io.github.bbuchsbaum.slurm4s.core.ByteLimit
        .from(maximum)
        .left
        .map(_.reason)
      maximumLogPage <- root("maximumLogPageBytes") match
        case None                        => Right(AgentFrameBudget.maximumLogPageBytes(limit))
        case Some(value) if value.isNull =>
          Right(AgentFrameBudget.maximumLogPageBytes(limit))
        case Some(value) =>
          value.asNumber
            .flatMap(_.toInt)
            .toRight("maximumLogPageBytes must be an integer or null")
            .flatMap(raw =>
              io.github.bbuchsbaum.slurm4s.core.ByteLimit.from(raw).left.map(_.reason)
            )
            .map(Some(_))
      features <- decodeFeatures(root("availableFeatures"))
      build <- root("agentBuild").flatMap(_.asString).toRight("agentBuild must be a string")
      _ <- Either.cond(
        maximumLogPage.forall(
          _.value <= AgentFrameBudget.maximumLogPageBytes(limit).fold(0)(_.value)
        ),
        (),
        "maximumLogPageBytes exceeds the safe frame budget"
      )
    yield HandshakeResponse(protocol, limit, features, build, maximumLogPage)

  private def decodeProtocol(json: Option[Json]): Either[String, ProtocolVersion] =
    for
      value <- json.toRight("missing agentProtocol")
      root <- value.asObject.toRight("agentProtocol must be an object")
      major <- root("major")
        .flatMap(_.asNumber.flatMap(_.toInt))
        .toRight("agentProtocol.major must be an integer")
      minor <- root("minor")
        .flatMap(_.asNumber.flatMap(_.toInt))
        .toRight("agentProtocol.minor must be an integer")
      protocol <- ProtocolVersion.from(major, minor).left.map(_.reason)
    yield protocol

  private def decodeFeatures(json: Option[Json]): Either[String, Set[AgentFeature]] =
    json.flatMap(_.asArray).toRight("features must be an array").flatMap { values =>
      values.foldLeft[Either[String, Set[AgentFeature]]](Right(Set.empty)) { (result, value) =>
        for
          accumulated <- result
          name <- value.asString.toRight("feature names must be strings")
          feature <- AgentFeature.fromWireName(name).toRight(s"unknown feature: $name")
        yield accumulated + feature
      }
    }
