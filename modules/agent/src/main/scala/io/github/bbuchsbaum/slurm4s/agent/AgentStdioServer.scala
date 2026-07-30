package io.github.bbuchsbaum.slurm4s.agent

import cats.effect.Ref
import cats.effect.kernel.Concurrent
import cats.syntax.all.*
import fs2.Pipe
import fs2.Stream
import io.circe.Json
import io.github.bbuchsbaum.slurm4s.protocol.AgentBody
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentEnvelope
import io.github.bbuchsbaum.slurm4s.protocol.AgentMessageCodec
import io.github.bbuchsbaum.slurm4s.protocol.AgentResponseStatus
import io.github.bbuchsbaum.slurm4s.protocol.FrameCodec
import io.github.bbuchsbaum.slurm4s.protocol.FrameDecoder
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits
import io.github.bbuchsbaum.slurm4s.protocol.HandshakeJson

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
          io.github.bbuchsbaum.slurm4s.protocol.AgentMethod.Handshake,
          payload
        ) =>
      HandshakeJson.decodeRequest(payload) match
        case Left(problem)    => protocolFailure(request, problem).pure[F]
        case Right(handshake) =>
          service.handshake(request.protocol, handshake).map {
            case AgentCall.Succeeded(response) =>
              request.withBody(
                AgentBody.Response(
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
    request.withBody(
      AgentBody.Response(
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
            case Right(request) =>
              handler.handle(request).attempt.map {
                case Right(response) => response
                case Left(error)     => handlerFailure(request, error)
              }
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

  private def handlerFailure(request: AgentEnvelope, error: Throwable): AgentEnvelope =
    val causeClass = Option(error.getClass.getSimpleName)
      .filter(_.nonEmpty)
      .getOrElse("Throwable")
      .take(128)
    request.withBody(
      AgentBody.Response(
        AgentResponseStatus.InternalFailure,
        Json.obj(
          "code" -> Json.fromString("agent-handler-failed"),
          "message" -> Json.fromString("the remote agent could not complete the request"),
          "causeClass" -> Json.fromString(causeClass)
        )
      )
    )

final case class AgentWireException(message: String) extends RuntimeException(message)
