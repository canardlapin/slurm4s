package io.github.bbuchsbaum.scalaslurm.ssh

import cats.effect.IO
import io.circe.Json
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.*

import java.time.Instant

class SshFailureClassificationSuite extends munit.CatsEffectSuite:
  test("authentication failure, agent absence, and disconnect are distinct") {
    val authentication = SshProcessOutcome.Exited(
      255,
      requestWriteCompleted = false,
      emptyEvidence(EvidenceSource.CommandStdout("ssh")),
      textEvidence("Permission denied (publickey,password).")
    )
    val absent = SshProcessOutcome.Exited(
      127,
      requestWriteCompleted = true,
      emptyEvidence(EvidenceSource.CommandStdout("ssh")),
      textEvidence("scala-slurm-agent: not found")
    )
    val disconnected = SshProcessOutcome.Exited(
      255,
      requestWriteCompleted = true,
      emptyEvidence(EvidenceSource.CommandStdout("ssh")),
      textEvidence("Connection reset by peer")
    )

    for
      auth <- client(authentication).roundTrip(request)
      missing <- client(absent).roundTrip(request)
      dropped <- client(disconnected).roundTrip(request)
      _ = assert(failure(auth).isInstanceOf[AgentFailure.AuthenticationFailed])
      _ = assert(failure(missing).isInstanceOf[AgentFailure.AgentUnavailable])
      _ = assert(failure(dropped).isInstanceOf[AgentFailure.TransportDisconnected])
    yield ()
  }

  test("valid remote CLI failure is not collapsed into a transport failure") {
    val response = request.copy(
      body = AgentBody.Response(
        AgentResponseStatus.DomainFailure,
        Json.obj("message" -> Json.fromString("sacct exited 1"))
      )
    )
    client(success(response)).roundTrip(request).map { result =>
      assert(failure(result).isInstanceOf[AgentFailure.RemoteCliFailure])
    }
  }

  test("contained remote agent failure is not misclassified as CLI or transport failure") {
    val response = request.copy(
      body = AgentBody.Response(
        AgentResponseStatus.InternalFailure,
        Json.obj(
          "code" -> Json.fromString("agent-handler-failed"),
          "message" -> Json.fromString("the remote agent could not complete the request")
        )
      )
    )
    client(success(response)).roundTrip(request).map { result =>
      assert(failure(result).isInstanceOf[AgentFailure.RemoteAgentFailure])
    }
  }

  test("handshake reports an incompatible agent major distinctly") {
    val incompatible = ProtocolVersion.from(2, 0).toOption.get
    val response = request.copy(
      body = AgentBody.Response(
        AgentResponseStatus.Ok,
        HandshakeJson.response(
          HandshakeResponse(
            incompatible,
            ByteLimit.from(1024).toOption.get,
            Set.empty,
            "future-agent"
          )
        )
      )
    )
    val wire = client(success(response))

    SshAgentApi.connect[IO](wire).map { result =>
      assertEquals(result, AgentCall.Failed(AgentFailure.ProtocolMismatch(1, 2)))
    }
  }

  private val request = AgentEnvelope(
    RequestId.from("ssh-1").toOption.get,
    ProtocolVersion.v1,
    AgentBody.Request(AgentMethod.Handshake, Json.obj())
  )

  private def failure[A](result: AgentCall[A]): AgentFailure = result match
    case AgentCall.Failed(value) => value
    case AgentCall.Succeeded(_)  => throw new AssertionError("expected failure")

  private def client(outcome: SshProcessOutcome): SshAgentWireClient[IO] =
    val runner = new SshProcessRunner[IO]:
      def exchange(
          launch: SshLaunch,
          request: Vector[Byte],
          policy: SshExchangePolicy
      ): IO[SshProcessOutcome] = IO.pure(outcome)
    val target = SshTarget.from("cluster").toOption.get
    val launch = SshCommand.agent(SshConnection("/usr/bin/ssh", target)).toOption.get
    SshAgentWireClient(
      launch,
      runner,
      FrameLimits.default,
      SshExchangePolicy(DurationMillis.from(1000).toOption.get)
    )

  private def success(response: AgentEnvelope): SshProcessOutcome =
    val frame = FrameCodec
      .encode(AgentMessageCodec.encode(response), FrameLimits.default)
      .toOption
      .get
    SshProcessOutcome.Exited(
      0,
      requestWriteCompleted = true,
      BoundedEvidence.capture(EvidenceSource.CommandStdout("ssh"), Instant.EPOCH, frame),
      emptyEvidence(EvidenceSource.CommandStderr("ssh"))
    )

  private def textEvidence(value: String): BoundedEvidence =
    BoundedEvidence.capture(
      EvidenceSource.CommandStderr("ssh"),
      Instant.EPOCH,
      value.getBytes("UTF-8").toVector
    )

  private def emptyEvidence(source: EvidenceSource): BoundedEvidence =
    BoundedEvidence.capture(source, Instant.EPOCH, Vector.empty)
