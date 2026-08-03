package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.FileIdentity
import io.github.bbuchsbaum.slurm4s.core.LogCursor
import io.github.bbuchsbaum.slurm4s.core.LogOffset
import io.github.bbuchsbaum.slurm4s.core.LogPage
import io.github.bbuchsbaum.slurm4s.core.LogReadResult
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.codec.CodecFailure

import scodec.bits.ByteVector

import java.time.Instant

class AgentMessageCodecSuite extends munit.FunSuite:
  private val requestId = RequestId.from("req-1").toOption.get

  test("message codec is canonical and preserves extensions") {
    val message = AgentEnvelope
      .withExtensions(
        requestId,
        ProtocolVersion.v1,
        AgentBody.Request(AgentMethod.ReadLog, Json.obj("cursor" -> Json.fromLong(42L))),
        JsonObject("future-field" -> Json.fromBoolean(true))
      )
      .toOption
      .get
    val encoded = AgentMessageCodec.encode(message)
    val decoded = AgentMessageCodec.decode(encoded).toOption.get

    assertEquals(
      new String(encoded.toArray, "UTF-8"),
      """{"payload":{"body":{"cursor":42},"future-field":true,"kind":"request","method":"read-log","requestId":"req-1"},"protocol":{"major":1,"minor":0},"schema":"slurm4s.agent-message"}
"""
    )
    assertEquals(AgentMessageCodec.encode(decoded), encoded)
    assertEquals(decoded.extensions("future-field"), Some(Json.fromBoolean(true)))
  }

  test("reserved message extensions are rejected at construction") {
    val collision = AgentEnvelope.withExtensions(
      requestId,
      ProtocolVersion.v1,
      AgentBody.Request(AgentMethod.ReadLog, Json.obj()),
      JsonObject("requestId" -> Json.fromString("shadow"))
    )

    assertEquals(collision, Left(AgentExtensionFailure("requestId")))
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
    val frameLimit = ByteLimit.from(16 * 1024).toOption.get
    val response = HandshakeResponse(
      ProtocolVersion.v1,
      frameLimit,
      Set(AgentFeature.PagedLogs, AgentFeature.OpaqueScripts),
      "test-build",
      AgentFrameBudget.maximumLogPageBytes(frameLimit)
    )

    assertEquals(HandshakeJson.decodeResponse(HandshakeJson.response(response)), Right(response))
  }

  test("handshake rejects a log-page budget larger than its frame can carry") {
    val json = Json.obj(
      "agentProtocol" -> Json.obj(
        "major" -> Json.fromInt(1),
        "minor" -> Json.fromInt(0)
      ),
      "maximumFrameBytes" -> Json.fromInt(16 * 1024),
      "maximumLogPageBytes" -> Json.fromInt(16 * 1024),
      "availableFeatures" -> Json.arr(Json.fromString("paged-logs")),
      "agentBuild" -> Json.fromString("invalid-agent")
    )

    assert(HandshakeJson.decodeResponse(json).left.exists(_.contains("safe frame budget")))
  }

  test("the largest advertised log page fits its response frame with maximum-width metadata") {
    val frameLimit = ByteLimit.from(64 * 1024).toOption.get
    val pageLimit = AgentFrameBudget.maximumLogPageBytes(frameLimit).get
    val response = AgentEnvelope(
      RequestId.from("r" * 200).toOption.get,
      ProtocolVersion.v1,
      AgentBody.Response(
        AgentResponseStatus.Ok,
        AgentDomainJson.encodeLogResult(
          LogReadResult.Page(
            LogPage(
              ByteVector.fill(pageLimit.value.toLong)(0xff.toByte),
              LogCursor(
                LogOffset.from(Long.MaxValue).toOption.get,
                Some(FileIdentity.from("f" * 512).toOption.get)
              ),
              endOfFile = false,
              Instant.parse("9999-12-31T23:59:59.999999999Z")
            )
          )
        )
      )
    )
    val payload = AgentMessageCodec.encode(response)

    assert(payload.size <= frameLimit.value)
    assert(FrameCodec.encode(payload, FrameLimits(frameLimit)).isRight)
  }
