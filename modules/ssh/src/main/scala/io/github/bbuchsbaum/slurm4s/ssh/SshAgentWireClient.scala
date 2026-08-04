package io.github.bbuchsbaum.slurm4s.ssh

import cats.Monad
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.BoundedEvidence
import io.github.bbuchsbaum.slurm4s.protocol.AgentBody
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentCodecFailure
import io.github.bbuchsbaum.slurm4s.protocol.AgentEnvelope
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailurePayload
import io.github.bbuchsbaum.slurm4s.protocol.AgentMessageCodec
import io.github.bbuchsbaum.slurm4s.protocol.AgentResponseStatus
import io.github.bbuchsbaum.slurm4s.protocol.FrameCodec
import io.github.bbuchsbaum.slurm4s.protocol.FrameDecoder
import io.github.bbuchsbaum.slurm4s.protocol.FrameFailure
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits
import io.github.bbuchsbaum.slurm4s.protocol.RequestId

final class SshAgentWireClient[F[_]: Monad](
    launch: SshLaunch,
    runner: SshProcessRunner[F],
    frameLimits: FrameLimits,
    policy: SshExchangePolicy
):
  private[ssh] def withMaximumFrameBytes(
      maximum: io.github.bbuchsbaum.slurm4s.core.ByteLimit
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
    case SshProcessOutcome.Failed(stage, diagnostic, writeCompleted, stdout, stderr) =>
      val evidence = if stderr.bytes.nonEmpty then stderr else stdout
      AgentCall
        .Failed(
          AgentFailure.TransportDisconnected(
            afterRequestWrite = writeCompleted,
            diagnostic = s"ssh process ${stage.code} failed: $diagnostic",
            evidence = Some(evidence)
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
    val decoded: Either[ResponseFailure, AgentEnvelope] = for
      fed <- FrameDecoder
        .empty(frameLimits)
        .feed(stdout.bytes)
        .left
        .map(failure => ResponseFailure.Protocol(showFrameFailure(failure)))
      _ <- fed._1.finish.left.map(failure => ResponseFailure.Protocol(showFrameFailure(failure)))
      frame <- fed._2 match
        case Vector(single) => Right(single)
        case other          =>
          Left(ResponseFailure.Protocol(s"expected one response frame, received ${other.size}"))
      message <- AgentMessageCodec
        .decode(frame)
        .left
        .map(failure => ResponseFailure.Protocol(showCodecFailure(failure)))
      _ <- Either.cond(
        message.requestId == expectedRequestId,
        (),
        ResponseFailure.Protocol(
          s"response correlation mismatch: expected ${expectedRequestId.value}"
        )
      )
      response <- message.body match
        case AgentBody.Response(AgentResponseStatus.Ok, _) => Right(message)
        case AgentBody.Response(status, payload)           =>
          Left(decodeFailure(status, payload, stdout, stderr))
        case AgentBody.Request(_, _) =>
          Left(ResponseFailure.Protocol("agent returned a request where a response was required"))
    yield response

    decoded match
      case Right(message)                       => AgentCall.Succeeded(message).pure[F]
      case Left(ResponseFailure.Typed(failure)) =>
        AgentCall.Failed(failure).pure[F]
      case Left(ResponseFailure.Protocol(problem)) =>
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
    else if (exitCode == 126 || exitCode == 127) &&
      (!writeCompleted || agentUnavailableMarker(normalized))
    then
      // A remote program can itself exit 126 or 127 after accepting a request. Once the request was
      // written, classify those exits as definite agent absence only when stderr names the agent
      // command; otherwise preserve submission uncertainty in TransportDisconnected.
      AgentCall.Failed(
        AgentFailure.AgentUnavailable("slurm4s-agent is unavailable", Some(stderr))
      )
    else
      AgentCall.Failed(
        AgentFailure.TransportDisconnected(
          afterRequestWrite = writeCompleted,
          diagnostic = s"ssh exited before a protocol response (exit $exitCode)",
          evidence = Some(stderr)
        )
      )

  /** OpenSSH-shaped authentication diagnostics only.
    *
    * A bare "permission denied" also matches a remote program reporting an inaccessible file, and
    * misreading that as an authentication failure loses the write flag exactly as the "not found"
    * match did. Where the message is ambiguous the classification falls through to a transport
    * disconnect, which preserves uncertainty instead of asserting a cause.
    */
  private def authenticationMarker(text: String): Boolean =
    text.contains("permission denied (") ||
      text.contains("permission denied, please try again") ||
      text.contains("too many authentication failures") ||
      text.contains("no supported authentication methods available")

  private def agentUnavailableMarker(text: String): Boolean =
    text.linesIterator.exists { raw =>
      val line = raw.trim
      line.contains("slurm4s-agent") &&
      (line.endsWith("not found") ||
        line.contains("command not found: slurm4s-agent") ||
        line.endsWith("permission denied"))
    }

  private def decodeFailure(
      status: AgentResponseStatus,
      payload: io.circe.Json,
      stdout: BoundedEvidence,
      stderr: BoundedEvidence
  ): ResponseFailure =
    val evidence =
      if status == AgentResponseStatus.ProtocolFailure then Some(stdout)
      else Some(stderr)
    AgentFailurePayload.decode(payload) match
      case Right(failure) =>
        failure.toFailure(status, evidence) match
          case Right(value)  => ResponseFailure.Typed(value)
          case Left(problem) =>
            ResponseFailure.Protocol(s"invalid ${status.wireName} payload: $problem")
      case Left(problem) =>
        ResponseFailure.Protocol(s"invalid ${status.wireName} payload: $problem")

  private def showFrameFailure(failure: FrameFailure): String = failure.toString

  private def showCodecFailure(failure: AgentCodecFailure): String = failure.toString

  private enum ResponseFailure:
    case Typed(failure: AgentFailure)
    case Protocol(diagnostic: String)
