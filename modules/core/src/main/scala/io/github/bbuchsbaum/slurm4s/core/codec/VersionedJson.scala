package io.github.bbuchsbaum.slurm4s.core.codec

import io.circe.Json
import io.circe.JsonObject
import io.circe.Printer
import io.circe.parser
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.SchemaId

import scodec.bits.ByteVector

import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

enum CodecFailure derives CanEqual:
  case InvalidUtf8(message: String)
  case InvalidJson(message: String)
  case InvalidEnvelope(message: String)
  case UnsupportedMajor(received: Int, supported: Int)
  case ReservedExtension(name: String)

final case class WireEnvelope private (
    protocol: ProtocolVersion,
    schema: SchemaId,
    payload: Json,
    extensions: JsonObject
)

object WireEnvelope:
  private[codec] val reservedFields = Set("protocol", "schema", "payload")

  def apply(protocol: ProtocolVersion, schema: SchemaId, payload: Json): WireEnvelope =
    new WireEnvelope(protocol, schema, payload, JsonObject.empty)

  def withExtensions(
      protocol: ProtocolVersion,
      schema: SchemaId,
      payload: Json,
      extensions: JsonObject
  ): Either[CodecFailure, WireEnvelope] =
    extensions.keys.toVector.sorted.find(reservedFields.contains) match
      case Some(name) => Left(CodecFailure.ReservedExtension(name))
      case None       => Right(new WireEnvelope(protocol, schema, payload, extensions))

  private[codec] def decoded(
      protocol: ProtocolVersion,
      schema: SchemaId,
      payload: Json,
      extensions: JsonObject
  ): WireEnvelope =
    new WireEnvelope(protocol, schema, payload, extensions)

object VersionedJson:
  val supportedMajor: Int = 1
  private val canonicalPrinter = Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  def encode(envelope: WireEnvelope): ByteVector =
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
      case (fields, (name, value)) => fields.add(name, value)
    }
    val canonical = canonicalPrinter.print(Json.fromJsonObject(withExtensions)) + "\n"
    ByteVector.view(canonical.getBytes(StandardCharsets.UTF_8))

  def decode(bytes: ByteVector): Either[CodecFailure, WireEnvelope] =
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
    yield WireEnvelope.decoded(
      protocol = protocol,
      schema = schema,
      payload = payload,
      extensions = JsonObject.fromIterable(
        root.toIterable.filterNot(entry => WireEnvelope.reservedFields.contains(entry._1))
      )
    )

  private def decodeUtf8(bytes: ByteVector): Either[CodecFailure, String] =
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try Right(decoder.decode(bytes.toByteBuffer).toString)
    catch case error: CharacterCodingException => Left(CodecFailure.InvalidUtf8(error.getMessage))
