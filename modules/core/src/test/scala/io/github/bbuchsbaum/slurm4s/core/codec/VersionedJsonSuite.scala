package io.github.bbuchsbaum.slurm4s.core.codec

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.SchemaId

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

class VersionedJsonSuite extends munit.FunSuite:
  private val schema = SchemaId.from("example.echo.v1").toOption.get
  private val envelope = WireEnvelope(
    protocol = ProtocolVersion.v1,
    schema = schema,
    payload = Json.obj("message" -> Json.fromString("hello"), "ordinal" -> Json.fromInt(7))
  )

  test("canonical bytes match the golden fixture exactly") {
    val stream = Option(getClass.getResourceAsStream("/fixtures/wire-envelope-v1.json")).get
    val expected = try ByteVector.view(stream.readAllBytes())
    finally stream.close()

    assertEquals(VersionedJson.encode(envelope), expected)
  }

  test("codec round trips and preserves unknown top-level fields") {
    val withExtension = WireEnvelope
      .withExtensions(
        envelope.protocol,
        envelope.schema,
        envelope.payload,
        io.circe.JsonObject("future" -> Json.obj("enabled" -> Json.True))
      )
      .toOption
      .get
    val decoded = VersionedJson.decode(VersionedJson.encode(withExtension)).toOption.get

    assertEquals(decoded.protocol, ProtocolVersion.v1)
    assertEquals(decoded.schema.value, "example.echo.v1")
    assertEquals(decoded.extensions("future"), withExtension.extensions("future"))
  }

  test("reserved extensions are rejected at construction rather than silently discarded") {
    val collision = WireEnvelope.withExtensions(
      envelope.protocol,
      envelope.schema,
      envelope.payload,
      io.circe.JsonObject(
        "payload" -> Json.fromString("shadow"),
        "future" -> Json.True
      )
    )

    assertEquals(collision, Left(CodecFailure.ReservedExtension("payload")))
  }

  test("unknown major protocol versions are rejected before payload interpretation") {
    val bytes = ByteVector.view(
      ("""{"payload":null,"protocol":{"major":2,"minor":0},"schema":"example.echo.v1"}""" + "\n")
        .getBytes(StandardCharsets.UTF_8)
    )

    assertEquals(
      VersionedJson.decode(bytes),
      Left(CodecFailure.UnsupportedMajor(received = 2, supported = 1))
    )
  }

  test("malformed UTF-8 is classified") {
    assert(VersionedJson.decode(ByteVector(0xc3, 0x28)).left.exists {
      case CodecFailure.InvalidUtf8(_) => true
      case _                           => false
    })
  }
