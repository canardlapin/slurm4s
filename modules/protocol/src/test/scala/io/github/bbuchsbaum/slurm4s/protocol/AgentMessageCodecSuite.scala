package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.*
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

  test("the language-neutral method registry covers every request method") {
    val fixture = io.circe.parser.parse(resource("/fixtures/agent-methods-v1.json")).toOption.get
    val wireNames = fixture.hcursor.get[Vector[String]]("methods").toOption.get
    val expected = AgentMethod.values.toVector.map(_.wireName)

    assertEquals(wireNames, expected)
    AgentMethod.values.foreach { method =>
      val body = Json.obj("text" -> Json.fromString("héllø λ"))
      val encoded = AgentMessageCodec.encode(
        AgentEnvelope(requestId, ProtocolVersion.v1, AgentBody.Request(method, body))
      )
      AgentMessageCodec.decode(encoded).toOption.get.body match
        case AgentBody.Request(decodedMethod, decodedBody) =>
          assertEquals(decodedMethod, method)
          assertEquals(decodedBody, body)
        case other => fail(s"expected request for ${method.wireName}, received $other")
    }
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
      AgentFrameBudget.maximumLogPageBytes(frameLimit),
      None
    )

    assertEquals(HandshakeJson.decodeResponse(HandshakeJson.response(response)), Right(response))
  }

  test("handshake feature negotiation ignores names introduced by a newer peer") {
    val request = Json.obj(
      "maximumFrameBytes" -> Json.fromInt(16 * 1024),
      "requestedFeatures" -> Json.arr(
        Json.fromString("opaque-scripts"),
        Json.fromString("future-feature")
      )
    )

    assertEquals(
      HandshakeJson.decodeRequest(request).map(_.requestedFeatures),
      Right(Set(AgentFeature.OpaqueScripts))
    )
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

  test("handshake rejects a queue-page budget larger than its frame can carry") {
    val frameLimit = ByteLimit.from(512 * 1024).toOption.get
    val maximum = AgentFrameBudget.maximumQueuePage(frameLimit).get
    val json = Json.obj(
      "agentProtocol" -> Json.obj(
        "major" -> Json.fromInt(1),
        "minor" -> Json.fromInt(0)
      ),
      "maximumFrameBytes" -> Json.fromInt(frameLimit.value),
      "maximumLogPageBytes" -> Json.Null,
      "maximumQueuePageItems" -> Json.fromInt(maximum.maximumItems + 1),
      "availableFeatures" -> Json.arr(Json.fromString("queue-listing")),
      "agentBuild" -> Json.fromString("invalid-agent")
    )

    assert(HandshakeJson.decodeResponse(json).left.exists(_.contains("safe frame budget")))
  }

  test("small frames do not advertise queue listing") {
    val frameLimit = ByteLimit.from(64 * 1024).toOption.get
    assertEquals(AgentFrameBudget.maximumQueuePage(frameLimit), None)
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

  test("the largest advertised queue page fits its response frame at the owned wire budget") {
    val frameLimit = ByteLimit.maximumCommandCapture
    val pageLimit = AgentFrameBudget.maximumQueuePage(frameLimit).get
    val flags = Vector.tabulate(64)(index => SlurmStateFlag.Unknown(s"F$index" + "x" * 120))
    val jobs = Vector.tabulate(pageLimit.maximumItems) { index =>
      QueueJob(
        JobRef(JobId.unsafeFrom((index + 1).toString), Some(ArrayIndex.unsafeFrom(index))),
        Some(JobName.unsafeFrom("n" * 128)),
        Some(UserName.unsafeFrom("u" * 255)),
        Some(PartitionName.unsafeFrom("p" * 255)),
        SlurmState.Unknown("s" * 128),
        flags,
        Some("r" * 512),
        JobTiming.unknown,
        Some(ClusterName.unsafeFrom("c" * 255)),
        StateExpressionCompleteness.Complete
      )
    }
    val evidence = EvidenceBundle(
      BoundedEvidence.capture(
        EvidenceSource.CommandStdout("squeue"),
        Instant.parse("9999-12-31T23:59:59.999999999Z"),
        ByteVector.fill(ByteLimit.defaultEvidence.value.toLong)(0xff.toByte)
      ),
      Vector(
        BoundedEvidence.capture(
          EvidenceSource.CommandStderr("squeue"),
          Instant.parse("9999-12-31T23:59:59.999999999Z"),
          ByteVector.fill(ByteLimit.defaultEvidence.value.toLong)(0xff.toByte)
        )
      )
    )
    val response = AgentEnvelope(
      RequestId.unsafeFrom("r" * 200),
      ProtocolVersion.v1,
      AgentBody.Response(
        AgentResponseStatus.Ok,
        AgentDomainJson.encodeQueueResult(
          SchedulerQueryResult.Succeeded(
            QueuePage(
              jobs,
              QueuePageCompleteness.Complete,
              Freshness.Current(Instant.MAX),
              evidence
            )
          )
        )
      )
    )
    val payload = AgentMessageCodec.encode(response)

    assert(payload.size <= frameLimit.value)
    assert(FrameCodec.encode(payload, FrameLimits(frameLimit)).isRight)
  }

  private def resource(path: String): String =
    scala.io.Source
      .fromInputStream(
        Option(getClass.getResourceAsStream(path))
          .getOrElse(throw new IllegalStateException(s"missing fixture: $path"))
      )
      .mkString
