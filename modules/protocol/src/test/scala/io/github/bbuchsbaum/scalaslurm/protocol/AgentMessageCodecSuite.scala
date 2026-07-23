package io.github.bbuchsbaum.scalaslurm.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ProtocolVersion
import io.github.bbuchsbaum.scalaslurm.core.codec.CodecFailure

class AgentMessageCodecSuite extends munit.FunSuite:
  private val requestId = RequestId.from("req-1").toOption.get

  test("message codec is canonical and preserves extensions") {
    val message = AgentEnvelope(
      requestId,
      ProtocolVersion.v1,
      AgentBody.Request(AgentMethod.ReadLog, Json.obj("cursor" -> Json.fromLong(42L))),
      JsonObject("future-field" -> Json.fromBoolean(true))
    )
    val encoded = AgentMessageCodec.encode(message)
    val decoded = AgentMessageCodec.decode(encoded).toOption.get

    assertEquals(
      new String(encoded.toArray, "UTF-8"),
      """{"payload":{"body":{"cursor":42},"future-field":true,"kind":"request","method":"read-log","requestId":"req-1"},"protocol":{"major":1,"minor":0},"schema":"scala-slurm.agent-message"}
"""
    )
    assertEquals(AgentMessageCodec.encode(decoded), encoded)
    assertEquals(decoded.extensions("future-field"), Some(Json.fromBoolean(true)))
  }

  test("unsupported protocol major is typed") {
    val incompatible = ProtocolVersion.from(7, 0).toOption.get
    val bytes = AgentMessageCodec.encode(
      AgentEnvelope(
        requestId,
        incompatible,
        AgentBody.Request(AgentMethod.Handshake, Json.obj())
      )
    )

    assert(
      AgentMessageCodec.decode(bytes).left.exists {
        case AgentCodecFailure.Envelope(CodecFailure.UnsupportedMajor(7, 1)) => true
        case _                                                               => false
      }
    )
  }

  test("handshake round trips negotiated bounds and features") {
    val response = HandshakeResponse(
      ProtocolVersion.v1,
      ByteLimit.from(1024).toOption.get,
      Set(AgentFeature.PagedLogs, AgentFeature.OpaqueScripts),
      "test-build"
    )

    assertEquals(HandshakeJson.decodeResponse(HandshakeJson.response(response)), Right(response))
  }
