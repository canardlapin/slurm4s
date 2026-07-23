package io.github.bbuchsbaum.scalaslurm.agent

import cats.Monad
import cats.syntax.all.*
import io.circe.Json
import io.github.bbuchsbaum.scalaslurm.protocol.AgentBody
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.AgentDomainJson
import io.github.bbuchsbaum.scalaslurm.protocol.AgentEnvelope
import io.github.bbuchsbaum.scalaslurm.protocol.AgentMethod
import io.github.bbuchsbaum.scalaslurm.protocol.AgentResponseStatus

final class SchedulerRequestHandler[F[_]: Monad](service: AgentService[F])
    extends AgentRequestHandler[F]:
  def handle(request: AgentEnvelope): F[AgentEnvelope] = request.body match
    case AgentBody.Request(AgentMethod.Capabilities, _) =>
      service.api.capabilities.map(response(request, _, AgentDomainJson.encodeCapabilities))
    case AgentBody.Request(AgentMethod.SubmitOpaque, payload) =>
      decodeAndRun(
        request,
        AgentDomainJson.decodeSubmitRequest(payload),
        service.api.submitOpaque,
        AgentDomainJson.encodeSubmission
      )
    case AgentBody.Request(AgentMethod.Observe, payload) =>
      decodeAndRun(
        request,
        AgentDomainJson.decodeJobRefs(payload),
        service.api.observe,
        AgentDomainJson.encodeObservation
      )
    case AgentBody.Request(AgentMethod.Accounting, payload) =>
      decodeAndRun(
        request,
        AgentDomainJson.decodeJobRefs(payload),
        service.api.accounting,
        AgentDomainJson.encodeAccounting
      )
    case AgentBody.Request(AgentMethod.Cancel, payload) =>
      decodeAndRun(
        request,
        AgentDomainJson.decodeJobRef(payload),
        service.api.cancel,
        AgentDomainJson.encodeCancellation
      )
    case AgentBody.Request(AgentMethod.ReadLog, payload) =>
      AgentDomainJson.decodeLogRequest(payload) match
        case Left(problem)               => protocolFailure(request, problem).pure[F]
        case Right((ref, cursor, limit)) =>
          service.api
            .readLog(ref, cursor, limit)
            .map(response(request, _, AgentDomainJson.encodeLogResult))
    case AgentBody.Request(AgentMethod.Handshake, _) =>
      protocolFailure(request, "handshake must be handled by ServiceRequestHandler").pure[F]
    case AgentBody.Response(_, _) =>
      protocolFailure(request, "the scheduler handler accepts request messages only").pure[F]

  private def decodeAndRun[A, B](
      request: AgentEnvelope,
      decoded: Either[String, A],
      run: A => F[AgentCall[B]],
      encode: B => Json
  ): F[AgentEnvelope] = decoded match
    case Left(problem) => protocolFailure(request, problem).pure[F]
    case Right(value)  => run(value).map(response(request, _, encode))

  private def response[A](
      request: AgentEnvelope,
      call: AgentCall[A],
      encode: A => Json
  ): AgentEnvelope = call match
    case AgentCall.Succeeded(value) =>
      request.copy(body = AgentBody.Response(AgentResponseStatus.Ok, encode(value)))
    case AgentCall.Failed(failure) =>
      request.copy(
        body = AgentBody.Response(
          AgentResponseStatus.DomainFailure,
          Json.obj("message" -> Json.fromString(failure.toString))
        )
      )

  private def protocolFailure(request: AgentEnvelope, problem: String): AgentEnvelope =
    request.copy(
      body = AgentBody.Response(
        AgentResponseStatus.ProtocolFailure,
        Json.obj("message" -> Json.fromString(problem))
      )
    )
