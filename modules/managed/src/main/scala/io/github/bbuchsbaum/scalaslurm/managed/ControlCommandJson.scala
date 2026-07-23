package io.github.bbuchsbaum.scalaslurm.managed

import io.circe.HCursor
import io.circe.Json
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.AgentDomainJson

import java.time.Instant
import java.util.Base64
import scala.util.Try

private[managed] object ControlCommandJson:
  def encode(command: ControlCommand): Json = command match
    case ControlCommand.RecordIntent(intent) =>
      Json.obj(
        "type" -> Json.fromString("record-intent"),
        "submissionKey" -> Json.fromString(intent.submissionKey.value),
        "attemptId" -> Json.fromString(intent.attemptId.value),
        "epoch" -> Json.fromLong(intent.epoch.value),
        "requestBase64" -> Json.fromString(
          Base64.getEncoder.encodeToString(intent.request.bytes.toArray)
        ),
        "requestDigest" -> Json.fromString(intent.request.digest.value),
        "at" -> Json.fromString(intent.recordedAt.toString)
      )
    case ControlCommand.ClaimSubmission(key, at) =>
      base("claim-submission", key, at)
    case ControlCommand.RecordSubmission(key, epoch, result, at) =>
      base("record-submission", key, at).deepMerge(
        Json.obj(
          "epoch" -> Json.fromLong(epoch.value),
          "result" -> AgentDomainJson.encodeSubmission(result)
        )
      )
    case ControlCommand.RecoverSubmissionClaim(key, epoch, evidence, at) =>
      base("recover-submission", key, at).deepMerge(
        Json.obj(
          "epoch" -> Json.fromLong(epoch.value),
          "evidence" -> AgentDomainJson.encodeEvidence(evidence)
        )
      )
    case ControlCommand.ReconcileBinding(key, epoch, job, evidence, at) =>
      base("reconcile-binding", key, at).deepMerge(
        Json.obj(
          "epoch" -> Json.fromLong(epoch.value),
          "job" -> AgentDomainJson.encodeJobRef(job),
          "evidence" -> AgentDomainJson.encodeEvidence(evidence)
        )
      )
    case ControlCommand.RecordObservations(requested, result, at) =>
      Json.obj(
        "type" -> Json.fromString("record-observations"),
        "at" -> Json.fromString(at.toString),
        "requested" -> AgentDomainJson.encodeJobRefs(requested),
        "result" -> AgentDomainJson.encodeObservation(result)
      )
    case ControlCommand.RecordAccounting(requested, result, at) =>
      Json.obj(
        "type" -> Json.fromString("record-accounting"),
        "at" -> Json.fromString(at.toString),
        "requested" -> AgentDomainJson.encodeJobRefs(requested),
        "result" -> AgentDomainJson.encodeAccounting(result)
      )
    case ControlCommand.RequestCancellation(key, at) =>
      base("request-cancellation", key, at)
    case ControlCommand.ClaimCancellation(key, at) =>
      base("claim-cancellation", key, at)
    case ControlCommand.RecoverCancellationClaim(key, evidence, at) =>
      base("recover-cancellation", key, at).deepMerge(
        Json.obj("evidence" -> AgentDomainJson.encodeEvidence(evidence))
      )
    case ControlCommand.RecordCancellation(key, result, at) =>
      base("record-cancellation", key, at).deepMerge(
        Json.obj("result" -> AgentDomainJson.encodeCancellation(result))
      )

  def decode(json: Json): Either[String, ControlCommand] =
    for
      cursor <- Either.cond(json.isObject, json.hcursor, "control command must be an object")
      commandType <- string(cursor, "type")
      command <- commandType match
        case "record-intent"    => decodeIntent(cursor)
        case "claim-submission" =>
          common(cursor).map((key, at) => ControlCommand.ClaimSubmission(key, at))
        case "record-submission" =>
          for
            common <- common(cursor)
            epoch <- epoch(cursor)
            resultJson <- jsonField(cursor, "result")
            result <- AgentDomainJson.decodeSubmission(resultJson)
          yield ControlCommand.RecordSubmission(common._1, epoch, result, common._2)
        case "recover-submission" =>
          for
            common <- common(cursor)
            epoch <- epoch(cursor)
            evidenceJson <- jsonField(cursor, "evidence")
            evidence <- AgentDomainJson.decodeEvidence(evidenceJson)
          yield ControlCommand.RecoverSubmissionClaim(common._1, epoch, evidence, common._2)
        case "reconcile-binding" =>
          for
            common <- common(cursor)
            epoch <- epoch(cursor)
            jobJson <- jsonField(cursor, "job")
            job <- AgentDomainJson.decodeJobRef(jobJson)
            evidenceJson <- jsonField(cursor, "evidence")
            evidence <- AgentDomainJson.decodeEvidence(evidenceJson)
          yield ControlCommand.ReconcileBinding(common._1, epoch, job, evidence, common._2)
        case "record-observations" =>
          for
            at <- instant(cursor, "at")
            requestedJson <- jsonField(cursor, "requested")
            requested <- AgentDomainJson.decodeJobRefs(requestedJson)
            resultJson <- jsonField(cursor, "result")
            result <- AgentDomainJson.decodeObservation(resultJson)
          yield ControlCommand.RecordObservations(requested, result, at)
        case "record-accounting" =>
          for
            at <- instant(cursor, "at")
            requestedJson <- jsonField(cursor, "requested")
            requested <- AgentDomainJson.decodeJobRefs(requestedJson)
            resultJson <- jsonField(cursor, "result")
            result <- AgentDomainJson.decodeAccounting(resultJson)
          yield ControlCommand.RecordAccounting(requested, result, at)
        case "request-cancellation" =>
          common(cursor).map((key, at) => ControlCommand.RequestCancellation(key, at))
        case "claim-cancellation" =>
          common(cursor).map((key, at) => ControlCommand.ClaimCancellation(key, at))
        case "recover-cancellation" =>
          for
            common <- common(cursor)
            evidenceJson <- jsonField(cursor, "evidence")
            evidence <- AgentDomainJson.decodeEvidence(evidenceJson)
          yield ControlCommand.RecoverCancellationClaim(common._1, evidence, common._2)
        case "record-cancellation" =>
          for
            common <- common(cursor)
            resultJson <- jsonField(cursor, "result")
            result <- AgentDomainJson.decodeCancellation(resultJson)
          yield ControlCommand.RecordCancellation(common._1, result, common._2)
        case other => Left(s"unknown control command: $other")
    yield command

  private def decodeIntent(cursor: HCursor): Either[String, ControlCommand] =
    for
      keyText <- string(cursor, "submissionKey")
      key <- SubmissionKey.from(keyText).left.map(_.reason)
      attemptText <- string(cursor, "attemptId")
      attempt <- AttemptId.from(attemptText).left.map(_.reason)
      epoch <- epoch(cursor)
      encoded <- string(cursor, "requestBase64")
      bytes <- Try(Base64.getDecoder.decode(encoded).toVector).toEither.left.map(_.getMessage)
      digestText <- string(cursor, "requestDigest")
      digest <- ContentDigest.from(digestText).left.map(_.reason)
      request <- CanonicalRequest.validated(bytes, digest)
      at <- instant(cursor, "at")
      decoded <- request.decode
      _ <- Either.cond(
        decoded.submissionKey == key,
        (),
        "stored submission key does not match canonical request"
      )
    yield ControlCommand.RecordIntent(ManagedIntent(key, attempt, epoch, request, at))

  private def common(cursor: HCursor): Either[String, (SubmissionKey, Instant)] =
    for
      keyText <- string(cursor, "submissionKey")
      key <- SubmissionKey.from(keyText).left.map(_.reason)
      at <- instant(cursor, "at")
    yield key -> at

  private def base(commandType: String, key: SubmissionKey, at: Instant): Json =
    Json.obj(
      "type" -> Json.fromString(commandType),
      "submissionKey" -> Json.fromString(key.value),
      "at" -> Json.fromString(at.toString)
    )

  private def epoch(cursor: HCursor): Either[String, AttemptEpoch] =
    cursor
      .get[Long]("epoch")
      .left
      .map(_.message)
      .flatMap(value => AttemptEpoch.from(value).left.map(_.reason))

  private def instant(cursor: HCursor, name: String): Either[String, Instant] =
    string(cursor, name).flatMap(raw => Try(Instant.parse(raw)).toEither.left.map(_.getMessage))

  private def string(cursor: HCursor, name: String): Either[String, String] =
    cursor.get[String](name).left.map(_.message)

  private def jsonField(cursor: HCursor, name: String): Either[String, Json] =
    cursor.downField(name).focus.toRight(s"missing $name")
