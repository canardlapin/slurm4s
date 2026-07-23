package io.github.bbuchsbaum.scalaslurm.agent

import cats.effect.Ref
import cats.effect.kernel.Concurrent
import cats.syntax.all.*
import fs2.Pipe
import fs2.Stream
import io.circe.Json
import io.github.bbuchsbaum.scalaslurm.protocol.AgentBody
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.AgentEnvelope
import io.github.bbuchsbaum.scalaslurm.protocol.AgentMessageCodec
import io.github.bbuchsbaum.scalaslurm.protocol.AgentResponseStatus
import io.github.bbuchsbaum.scalaslurm.protocol.FrameCodec
import io.github.bbuchsbaum.scalaslurm.protocol.FrameDecoder
import io.github.bbuchsbaum.scalaslurm.protocol.FrameLimits
import io.github.bbuchsbaum.scalaslurm.protocol.HandshakeJson

enum AgentCommand derives CanEqual:
  case ServeStdio

object AgentCommand:
  def parse(arguments: List[String]): Either[String, AgentCommand] = arguments match
    case List("serve", "--stdio") => Right(AgentCommand.ServeStdio)
    case _                        => Left("expected exactly: serve --stdio")

trait AgentRequestHandler[F[_]]:
  def handle(request: AgentEnvelope): F[AgentEnvelope]

final class ServiceRequestHandler[F[_]: Concurrent](
    service: AgentService[F],
    delegate: AgentRequestHandler[F]
) extends AgentRequestHandler[F]:
  def handle(request: AgentEnvelope): F[AgentEnvelope] = request.body match
    case AgentBody.Request(
          io.github.bbuchsbaum.scalaslurm.protocol.AgentMethod.Handshake,
          payload
        ) =>
      HandshakeJson.decodeRequest(payload) match
        case Left(problem)    => protocolFailure(request, problem).pure[F]
        case Right(handshake) =>
          service.handshake(request.protocol, handshake).map {
            case AgentCall.Succeeded(response) =>
              request.copy(
                body = AgentBody.Response(
                  AgentResponseStatus.Ok,
                  HandshakeJson.response(response)
                )
              )
            case AgentCall.Failed(failure) => protocolFailure(request, failure.toString)
          }
    case AgentBody.Request(_, _)  => delegate.handle(request)
    case AgentBody.Response(_, _) =>
      protocolFailure(request, "the agent accepts request messages only").pure[F]

  private def protocolFailure(request: AgentEnvelope, message: String): AgentEnvelope =
    request.copy(
      body = AgentBody.Response(
        AgentResponseStatus.ProtocolFailure,
        Json.obj("message" -> Json.fromString(message))
      )
    )

final class AgentStdioServer[F[_]: Concurrent](
    handler: AgentRequestHandler[F],
    limits: FrameLimits = FrameLimits.default
):
  def pipe: Pipe[F, Byte, Byte] = input =>
    Stream.eval(Ref.of[F, FrameDecoder](FrameDecoder.empty(limits))).flatMap { decoder =>
      val responses = input.chunks
        .evalMap { chunk =>
          decoder.modify { current =>
            current.feed(chunk.toVector) match
              case Left(failure)         => (current, Left(AgentWireException(failure.toString)))
              case Right((next, frames)) => (next, Right(frames))
          }
        }
        .rethrow
        .flatMap(Stream.emits)
        .evalMap { bytes =>
          AgentMessageCodec.decode(bytes) match
            case Left(failure) =>
              Concurrent[F].raiseError[AgentEnvelope](AgentWireException(failure.toString))
            case Right(request) => handler.handle(request)
        }
        .evalMap { response =>
          FrameCodec.encode(AgentMessageCodec.encode(response), limits) match
            case Left(failure) =>
              Concurrent[F].raiseError[Vector[Byte]](AgentWireException(failure.toString))
            case Right(frame) => frame.pure[F]
        }
        .flatMap(Stream.emits)

      responses ++ Stream
        .eval(
          decoder.get.flatMap(current =>
            current.finish.leftMap(failure => AgentWireException(failure.toString)).liftTo[F]
          )
        )
        .drain
    }

final case class AgentWireException(message: String) extends RuntimeException(message)
