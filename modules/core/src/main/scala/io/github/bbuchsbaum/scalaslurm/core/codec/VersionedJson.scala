package io.github.bbuchsbaum.scalaslurm.core.codec

import io.circe.Json
import io.circe.JsonObject
import io.circe.Printer
import io.circe.parser
import io.github.bbuchsbaum.scalaslurm.core.ProtocolVersion
import io.github.bbuchsbaum.scalaslurm.core.SchemaId

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

final case class WireEnvelope(
    protocol: ProtocolVersion,
    schema: SchemaId,
    payload: Json,
    extensions: JsonObject = JsonObject.empty
)

enum CodecFailure derives CanEqual:
  case InvalidUtf8(message: String)
  case InvalidJson(message: String)
  case InvalidEnvelope(message: String)
  case UnsupportedMajor(received: Int, supported: Int)

object VersionedJson:
  val supportedMajor: Int = 1
  private val reservedFields = Set("protocol", "schema", "payload")
  private val canonicalPrinter = Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  def encode(envelope: WireEnvelope): Vector[Byte] =
    val protocol = Json.obj(
      "major" -> Json.fromInt(envelope.protocol.major),
      "minor" -> Json.fromInt(envelope.protocol.minor)
    )
    val base = JsonObject(
      "protocol" -> protocol,
      "schema" -> Json.fromString(envelope.schema.value),
      "payload" -> envelope.payload
    )
    val withExtensions = envelope.extensions.toIterable.foldLeft(base) {
      case (fields, (name, value)) if !reservedFields.contains(name) => fields.add(name, value)
      case (fields, _)                                               => fields
    }
    val canonical = canonicalPrinter.print(Json.fromJsonObject(withExtensions)) + "\n"
    canonical.getBytes(StandardCharsets.UTF_8).toVector

  def decode(bytes: Vector[Byte]): Either[CodecFailure, WireEnvelope] =
    for
      text <- decodeUtf8(bytes)
      json <- parser.parse(text).left.map(error => CodecFailure.InvalidJson(error.message))
      root <- json.asObject.toRight(
        CodecFailure.InvalidEnvelope("top-level JSON must be an object")
      )
      protocolJson <- root("protocol").toRight(CodecFailure.InvalidEnvelope("missing protocol"))
      protocolObject <- protocolJson.asObject.toRight(
        CodecFailure.InvalidEnvelope("protocol must be an object")
      )
      major <- protocolObject("major")
        .flatMap(_.asNumber.flatMap(_.toInt))
        .toRight(
          CodecFailure.InvalidEnvelope("protocol.major must be an integer")
        )
      minor <- protocolObject("minor")
        .flatMap(_.asNumber.flatMap(_.toInt))
        .toRight(
          CodecFailure.InvalidEnvelope("protocol.minor must be an integer")
        )
      _ <- Either.cond(
        major == supportedMajor,
        (),
        CodecFailure.UnsupportedMajor(major, supportedMajor)
      )
      protocol <- ProtocolVersion
        .from(major, minor)
        .left
        .map(problem => CodecFailure.InvalidEnvelope(problem.reason))
      schemaText <- root("schema")
        .flatMap(_.asString)
        .toRight(
          CodecFailure.InvalidEnvelope("schema must be a string")
        )
      schema <- SchemaId
        .from(schemaText)
        .left
        .map(problem => CodecFailure.InvalidEnvelope(problem.reason))
      payload <- root("payload").toRight(CodecFailure.InvalidEnvelope("missing payload"))
    yield WireEnvelope(
      protocol = protocol,
      schema = schema,
      payload = payload,
      extensions = JsonObject.fromIterable(
        root.toIterable.filterNot(entry => reservedFields.contains(entry._1))
      )
    )

  private def decodeUtf8(bytes: Vector[Byte]): Either[CodecFailure, String] =
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try Right(decoder.decode(ByteBuffer.wrap(bytes.toArray)).toString)
    catch case error: CharacterCodingException => Left(CodecFailure.InvalidUtf8(error.getMessage))
