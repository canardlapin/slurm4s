package io.github.bbuchsbaum.scalaslurm.ssh

import cats.Monad
import cats.syntax.all.*
import io.circe.Json
import io.github.bbuchsbaum.scalaslurm.core.BoundedEvidence
import io.github.bbuchsbaum.scalaslurm.protocol.AgentBody
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCodecFailure
import io.github.bbuchsbaum.scalaslurm.protocol.AgentEnvelope
import io.github.bbuchsbaum.scalaslurm.protocol.AgentFailure
import io.github.bbuchsbaum.scalaslurm.protocol.AgentMessageCodec
import io.github.bbuchsbaum.scalaslurm.protocol.AgentResponseStatus
import io.github.bbuchsbaum.scalaslurm.protocol.FrameCodec
import io.github.bbuchsbaum.scalaslurm.protocol.FrameDecoder
import io.github.bbuchsbaum.scalaslurm.protocol.FrameFailure
import io.github.bbuchsbaum.scalaslurm.protocol.FrameLimits
import io.github.bbuchsbaum.scalaslurm.protocol.RequestId

final class SshAgentWireClient[F[_]: Monad](
    launch: SshLaunch,
    runner: SshProcessRunner[F],
    frameLimits: FrameLimits,
    policy: SshExchangePolicy
):
  private[ssh] def withMaximumFrameBytes(
      maximum: io.github.bbuchsbaum.scalaslurm.core.ByteLimit
  ): SshAgentWireClient[F] =
    SshAgentWireClient(launch, runner, FrameLimits(maximum), policy)

  def roundTrip(request: AgentEnvelope): F[AgentCall[AgentEnvelope]] =
    FrameCodec.encode(AgentMessageCodec.encode(request), frameLimits) match
      case Left(failure) =>
        AgentCall
          .Failed(AgentFailure.ProtocolViolation(showFrameFailure(failure), None))
          .pure[F]
      case Right(frame) =>
        runner
          .exchange(launch, frame, policy)
          .flatMap(outcome => classify(request.requestId, outcome))

  private def classify(
      expectedRequestId: RequestId,
      outcome: SshProcessOutcome
  ): F[AgentCall[AgentEnvelope]] = outcome match
    case SshProcessOutcome.SpawnFailed(diagnostic, evidence) =>
      AgentCall.Failed(AgentFailure.AgentUnavailable(diagnostic, Some(evidence))).pure[F]
    case SshProcessOutcome.TimedOut(writeCompleted, _, stderr) =>
      AgentCall
        .Failed(
          AgentFailure.TransportDisconnected(
            afterRequestWrite = writeCompleted,
            diagnostic = "ssh agent exchange timed out",
            evidence = Some(stderr)
          )
        )
        .pure[F]
    case SshProcessOutcome.Exited(exitCode, writeCompleted, stdout, stderr) if exitCode != 0 =>
      classifyExit(exitCode, writeCompleted, stderr).pure[F]
    case SshProcessOutcome.Exited(_, _, stdout, stderr) =>
      decodeResponse(expectedRequestId, stdout, stderr)

  private def decodeResponse(
      expectedRequestId: RequestId,
      stdout: BoundedEvidence,
      stderr: BoundedEvidence
  ): F[AgentCall[AgentEnvelope]] =
    val decoded = for
      fed <- FrameDecoder.empty(frameLimits).feed(stdout.bytes).left.map(showFrameFailure)
      _ <- fed._1.finish.left.map(showFrameFailure)
      frame <- fed._2 match
        case Vector(single) => Right(single)
        case other          => Left(s"expected one response frame, received ${other.size}")
      message <- AgentMessageCodec.decode(frame).left.map(showCodecFailure)
      _ <- Either.cond(
        message.requestId == expectedRequestId,
        (),
        s"response correlation mismatch: expected ${expectedRequestId.value}"
      )
      response <- message.body match
        case AgentBody.Response(AgentResponseStatus.Ok, _)                  => Right(message)
        case AgentBody.Response(AgentResponseStatus.DomainFailure, payload) =>
          Left(s"remote-cli:${diagnostic(payload)}")
        case AgentBody.Response(AgentResponseStatus.InternalFailure, payload) =>
          Left(s"remote-agent:${diagnostic(payload)}")
        case AgentBody.Response(AgentResponseStatus.ProtocolFailure, payload) =>
          Left(s"protocol:${diagnostic(payload)}")
        case AgentBody.Request(_, _) =>
          Left("agent returned a request where a response was required")
    yield response

    decoded match
      case Right(message) => AgentCall.Succeeded(message).pure[F]
      case Left(problem) if problem.startsWith("remote-cli:") =>
        AgentCall
          .Failed(
            AgentFailure.RemoteCliFailure(
              problem.stripPrefix("remote-cli:"),
              Some(stderr)
            )
          )
          .pure[F]
      case Left(problem) if problem.startsWith("remote-agent:") =>
        AgentCall
          .Failed(
            AgentFailure.RemoteAgentFailure(
              problem.stripPrefix("remote-agent:"),
              Some(stderr)
            )
          )
          .pure[F]
      case Left(problem) =>
        AgentCall
          .Failed(AgentFailure.ProtocolViolation(problem, Some(stdout)))
          .pure[F]

  private def classifyExit(
      exitCode: Int,
      writeCompleted: Boolean,
      stderr: BoundedEvidence
  ): AgentCall[AgentEnvelope] =
    val text = new String(stderr.bytes.toArray, java.nio.charset.StandardCharsets.UTF_8)
    val normalized = text.toLowerCase(java.util.Locale.ROOT)
    if exitCode == 255 && authenticationMarker(normalized) then
      AgentCall.Failed(
        AgentFailure.AuthenticationFailed("OpenSSH authentication failed", Some(stderr))
      )
    else if exitCode == 126 || exitCode == 127 || normalized.contains("not found") then
      AgentCall.Failed(
        AgentFailure.AgentUnavailable("scala-slurm-agent is unavailable", Some(stderr))
      )
    else
      AgentCall.Failed(
        AgentFailure.TransportDisconnected(
          afterRequestWrite = writeCompleted,
          diagnostic = s"ssh exited before a protocol response (exit $exitCode)",
          evidence = Some(stderr)
        )
      )

  private def authenticationMarker(text: String): Boolean =
    text.contains("permission denied") ||
      text.contains("authentication failed") ||
      text.contains("too many authentication failures")

  private def diagnostic(payload: Json): String =
    payload.hcursor.get[String]("message").getOrElse(payload.noSpaces)

  private def showFrameFailure(failure: FrameFailure): String = failure.toString

  private def showCodecFailure(failure: AgentCodecFailure): String = failure.toString
