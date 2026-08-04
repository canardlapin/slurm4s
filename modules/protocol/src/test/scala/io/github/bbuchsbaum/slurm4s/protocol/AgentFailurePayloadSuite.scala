package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.BoundedEvidence
import io.github.bbuchsbaum.slurm4s.core.EvidenceSource

import java.time.Instant

import scodec.bits.ByteVector

class AgentFailurePayloadSuite extends munit.FunSuite:
  test("every typed agent failure kind survives the additive payload") {
    val failures = Vector[AgentFailure](
      AgentFailure.ProtocolMismatch(1, 2),
      AgentFailure.AgentUnavailable("agent unavailable", None),
      AgentFailure.AuthenticationFailed("authentication failed", None),
      AgentFailure.TransportDisconnected(afterRequestWrite = true, "disconnected", None),
      AgentFailure.RemoteCliFailure("remote CLI failed", None),
      AgentFailure.RemoteAgentFailure("remote agent failed", None),
      AgentFailure.ProtocolViolation("protocol failed", None)
    )

    failures.foreach { failure =>
      val payload = AgentFailurePayload.fromFailure(failure)
      val decoded = AgentFailurePayload.decode(payload.asJson)
      assertEquals(decoded, Right(payload))
      assertEquals(
        decoded.flatMap(_.toFailure(AgentResponseStatus.DomainFailure, None)),
        Right(failure)
      )
    }
  }

  test("legacy message-only bodies retain their status-based classification") {
    val legacy = Json.obj("message" -> Json.fromString("legacy failure"))
    val payload = AgentFailurePayload.decode(legacy).toOption.get

    assertEquals(
      payload.toFailure(AgentResponseStatus.DomainFailure, None),
      Right(AgentFailure.RemoteCliFailure("legacy failure", None))
    )
    assertEquals(
      payload.toFailure(AgentResponseStatus.ProtocolFailure, None),
      Right(AgentFailure.ProtocolViolation("legacy failure", None))
    )
    assertEquals(
      payload.toFailure(AgentResponseStatus.InternalFailure, None),
      Right(AgentFailure.RemoteAgentFailure("legacy failure", None))
    )
  }

  test("an unknown future code degrades by status while known fields remain strict") {
    val future = Json.obj(
      "message" -> Json.fromString("future failure"),
      "code" -> Json.fromString("future-failure"),
      "futureField" -> Json.fromInt(1)
    )
    val payload = AgentFailurePayload.decode(future).toOption.get

    assertEquals(payload.code, None)
    assertEquals(
      payload.toFailure(AgentResponseStatus.DomainFailure, None),
      Right(AgentFailure.RemoteCliFailure("future failure", None))
    )
    assert(
      AgentFailurePayload
        .decode(future.mapObject(_.add("clientMajor", Json.fromString("1"))))
        .isLeft
    )
  }

  test("typed payload evidence wins over unrelated SSH transport evidence") {
    val remote = evidence("remote scheduler stderr")
    val transport = evidence("ssh wrapper stderr")
    val failure = AgentFailure.RemoteCliFailure("sbatch failed", Some(remote))

    val reconstructed = AgentFailurePayload
      .decode(AgentFailurePayload.fromFailure(failure).asJson)
      .flatMap(_.toFailure(AgentResponseStatus.DomainFailure, Some(transport)))

    assertEquals(reconstructed, Right(failure))
  }

  test("known typed codes reject missing required fields instead of downgrading") {
    val mismatch = Json.obj(
      "message" -> Json.fromString("mismatch"),
      "code" -> Json.fromString(AgentFailureCode.ProtocolMismatch.wireName)
    )
    val disconnected = Json.obj(
      "message" -> Json.fromString("disconnected"),
      "code" -> Json.fromString(AgentFailureCode.TransportDisconnected.wireName)
    )

    assert(AgentFailurePayload.decode(mismatch).isLeft)
    assert(AgentFailurePayload.decode(disconnected).isLeft)
  }

  private def evidence(text: String): BoundedEvidence =
    BoundedEvidence.capture(
      EvidenceSource.CommandStderr("test"),
      Instant.parse("2026-08-03T00:00:00Z"),
      ByteVector.encodeUtf8(text).toOption.get
    )
