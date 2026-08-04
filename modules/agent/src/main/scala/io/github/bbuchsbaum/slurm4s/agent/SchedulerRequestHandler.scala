package io.github.bbuchsbaum.slurm4s.agent

import cats.Monad
import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.circe.Json
import io.github.bbuchsbaum.slurm4s.protocol.AgentBody
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRead
import io.github.bbuchsbaum.slurm4s.protocol.AgentDomainJson
import io.github.bbuchsbaum.slurm4s.protocol.AgentEnvelope
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailurePayload
import io.github.bbuchsbaum.slurm4s.protocol.AgentMethod
import io.github.bbuchsbaum.slurm4s.protocol.AgentResponseStatus

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
    case AgentBody.Request(AgentMethod.SubmitRegistered, payload) =>
      AgentDomainJson.decodeRemoteTaskRequest(payload) match
        case Left(problem) => protocolFailure(request, problem).pure[F]
        case Right(value)  =>
          service.api
            .submitRegistered(value)
            .map(responseEither(request, _, AgentDomainJson.encodeRemoteSubmission))
    case AgentBody.Request(AgentMethod.SubmitBatch, payload) =>
      AgentDomainJson.decodeRemoteBatchRequest(payload) match
        case Left(problem) => protocolFailure(request, problem).pure[F]
        case Right(value)  =>
          service.api
            .submitBatch(value)
            .map(responseEither(request, _, AgentDomainJson.encodeRemoteBatchSubmission))
    case AgentBody.Request(AgentMethod.SubmitScriptBatch, payload) =>
      AgentDomainJson.decodeRemoteScriptBatchRequest(payload) match
        case Left(problem) => protocolFailure(request, problem).pure[F]
        case Right(value)  =>
          service.api
            .submitScriptBatch(value)
            .map(response(request, _, AgentDomainJson.encodeRemoteScriptBatchSubmission))
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
    case AgentBody.Request(AgentMethod.ReadResult, payload) =>
      AgentDomainJson.decodeRemoteResultReadRequest(payload) match
        case Left(problem)              => protocolFailure(request, problem).pure[F]
        case Right((ref, maximumBytes)) =>
          service.api
            .readResult(ref, maximumBytes)
            .map(
              responseEither(
                request,
                _,
                AgentDomainJson.encodeRemoteResultRead
              )
            )
    case AgentBody.Request(AgentMethod.ReadResults, payload) =>
      AgentDomainJson.decodeRemoteResultReadsRequest(payload) match
        case Left(problem)               => protocolFailure(request, problem).pure[F]
        case Right((refs, maximumBytes)) =>
          // Deliberately the same per-ref read the single method performs. The server does no new
          // work; the saving is transport — one SSH process instead of one per pending element.
          refs
            .traverse(service.api.readResult(_, maximumBytes))
            .map { reads =>
              // One failed read fails the group: a partial answer would leave the caller unable to
              // tell which refs were even attempted.
              val collapsed = reads.toVector.foldLeft(
                AgentCall.Succeeded(Vector.empty[RemoteResultRead]): AgentCall[Vector[
                  RemoteResultRead
                ]]
              ) {
                case (AgentCall.Failed(failure), _) => AgentCall.Failed(failure)
                case (_, AgentCall.Failed(failure)) => AgentCall.Failed(failure)
                case (AgentCall.Succeeded(acc), AgentCall.Succeeded(value)) =>
                  AgentCall.Succeeded(acc :+ value)
              }
              val grouped = collapsed match
                case AgentCall.Failed(failure)   => AgentCall.Failed(failure)
                case AgentCall.Succeeded(values) =>
                  NonEmptyVector.fromVector(values) match
                    case Some(nonEmpty) => AgentCall.Succeeded(nonEmpty)
                    case None           =>
                      AgentCall.Failed(AgentFailure.ProtocolViolation("empty result group", None))
              responseEither(request, grouped, AgentDomainJson.encodeRemoteResultReads)
            }
    case AgentBody.Request(AgentMethod.ReadScriptExit, payload) =>
      AgentDomainJson.decodeRemoteScriptExitReadRequest(payload) match
        case Left(problem) => protocolFailure(request, problem).pure[F]
        case Right(ref)    =>
          service.api
            .readScriptExit(ref)
            .map(response(request, _, AgentDomainJson.encodeRemoteScriptExitRead))
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
      request.withBody(AgentBody.Response(AgentResponseStatus.Ok, encode(value)))
    case AgentCall.Failed(failure) =>
      request.withBody(
        AgentBody.Response(
          AgentResponseStatus.DomainFailure,
          AgentFailurePayload.fromFailure(failure).asJson
        )
      )

  private def responseEither[A](
      request: AgentEnvelope,
      call: AgentCall[A],
      encode: A => Either[String, Json]
  ): AgentEnvelope = call match
    case AgentCall.Succeeded(value) =>
      encode(value) match
        case Right(payload) =>
          request.withBody(AgentBody.Response(AgentResponseStatus.Ok, payload))
        case Left(problem) => protocolFailure(request, problem)
    case AgentCall.Failed(failure) =>
      request.withBody(
        AgentBody.Response(
          AgentResponseStatus.DomainFailure,
          AgentFailurePayload.fromFailure(failure).asJson
        )
      )

  private def protocolFailure(request: AgentEnvelope, problem: String): AgentEnvelope =
    request.withBody(
      AgentBody.Response(
        AgentResponseStatus.ProtocolFailure,
        AgentFailurePayload.protocolViolation(problem).asJson
      )
    )
