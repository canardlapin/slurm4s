package io.github.bbuchsbaum.scalaslurm.protocol

import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.circe.Decoder
import io.circe.DecodingFailure
import io.circe.Encoder
import io.circe.HCursor
import io.circe.Json
import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto.deriveEncoder
import io.github.bbuchsbaum.scalaslurm.core.*

import java.time.Instant
import java.time.LocalDateTime
import java.util.Base64
import scala.util.Try

object AgentDomainJson:
  def encodeSubmitRequest(value: JobRequest[NoResult]): Either[String, Json] = value.payload match
    case Payload.Script(source, arguments, ResultContract.ExitOnly) =>
      val fields = Vector(
        "submissionKey" -> Json.fromString(value.submissionKey.value),
        "name" -> Json.fromString(value.name.value),
        "source" -> source.asJson,
        "arguments" -> arguments.asJson,
        "resources" -> value.resources.asJson,
        "environment" -> value.environment.asJson,
        "resultContract" -> Json.fromString("exit-only")
      ) ++ value.array.toVector.map(array => "array" -> encodeArray(array)) ++
        Option
          .when(value.retrySafety != RetrySafety.Unknown)(
            "retrySafety" -> Json.fromString(encodeRetrySafety(value.retrySafety))
          )
          .toVector
      Right(
        Json.obj(fields*)
      )
    case Payload.Script(_, _, _)         => Left("P2 remote submission supports ExitOnly scripts")
    case _: Payload.RegisteredTask[?, ?] =>
      Left("registered typed tasks require the typed-workload protocol")

  def decodeSubmitRequest(json: Json): Either[String, JobRequest[NoResult]] =
    for
      cursor <- objectCursor(json, "submit request")
      submissionKeyText <- field[String](cursor, "submissionKey")
      submissionKey <- SubmissionKey.from(submissionKeyText).left.map(_.reason)
      nameText <- field[String](cursor, "name")
      name <- JobName.from(nameText).left.map(_.reason)
      source <- field[ScriptSource](cursor, "source")
      arguments <- field[Vector[String]](cursor, "arguments")
      resources <- field[ResourceRequest](cursor, "resources")
      environment <- field[Map[String, String]](cursor, "environment")
      array <- optionalArray(cursor)
      retrySafety <- optionalRetrySafety(cursor)
      contract <- field[String](cursor, "resultContract")
      _ <- Either.cond(contract == "exit-only", (), "unsupported result contract")
    yield JobRequest(
      submissionKey,
      name,
      Payload.Script(source, arguments, ResultContract.ExitOnly),
      resources,
      environment,
      array,
      retrySafety
    )

  def encodeJobRefs(value: NonEmptyVector[JobRef]): Json = value.toVector.asJson

  def decodeJobRefs(json: Json): Either[String, NonEmptyVector[JobRef]] =
    decode[Vector[JobRef]](json).flatMap(values =>
      NonEmptyVector.fromVector(values).toRight("job list must not be empty")
    )

  def encodeJobRef(value: JobRef): Json = value.asJson
  def decodeJobRef(json: Json): Either[String, JobRef] = decode[JobRef](json)

  def encodeEvidence(value: EvidenceBundle): Json = value.asJson
  def decodeEvidence(json: Json): Either[String, EvidenceBundle] = decode[EvidenceBundle](json)

  def encodeLogRequest(
      ref: LogRef,
      cursor: LogCursor,
      maximumBytes: ByteLimit
  ): Json =
    Json.obj(
      "ref" -> ref.asJson,
      "cursor" -> cursor.asJson,
      "maximumBytes" -> Json.fromInt(maximumBytes.value)
    )

  def decodeLogRequest(json: Json): Either[String, (LogRef, LogCursor, ByteLimit)] =
    for
      cursor <- objectCursor(json, "log request")
      ref <- field[LogRef](cursor, "ref")
      position <- field[LogCursor](cursor, "cursor")
      maximum <- field[Int](cursor, "maximumBytes")
      limit <- ByteLimit.from(maximum).left.map(_.reason)
    yield (ref, position, limit)

  def encodeCapabilities(value: SchedulerQueryResult[SchedulerCapabilities]): Json = value.asJson
  def decodeCapabilities(json: Json): Either[String, SchedulerQueryResult[SchedulerCapabilities]] =
    decode(json)

  def encodeSubmission(value: SubmissionAttempt): Json = value.asJson
  def decodeSubmission(json: Json): Either[String, SubmissionAttempt] = decode(json)

  def encodeObservation(value: SchedulerQueryResult[ObservationBatch]): Json = value.asJson
  def decodeObservation(json: Json): Either[String, SchedulerQueryResult[ObservationBatch]] =
    decode(json)

  def encodeAccounting(value: SchedulerQueryResult[AccountingBatch]): Json = value.asJson
  def decodeAccounting(json: Json): Either[String, SchedulerQueryResult[AccountingBatch]] =
    decode(json)

  def encodeCancellation(value: CancellationAttempt): Json = value.asJson
  def decodeCancellation(json: Json): Either[String, CancellationAttempt] = decode(json)

  def encodeLogResult(value: LogReadResult): Json = value.asJson
  def decodeLogResult(json: Json): Either[String, LogReadResult] = decode(json)

  private def decode[A: Decoder](json: Json): Either[String, A] =
    json.as[A].left.map(_.message)

  private def objectCursor(json: Json, name: String): Either[String, HCursor] =
    Either.cond(json.isObject, json.hcursor, s"$name must be an object")

  private def field[A: Decoder](cursor: HCursor, name: String): Either[String, A] =
    cursor.get[A](name).left.map(_.message)

  private def encodeArray(value: JobArrayRequest): Json =
    Json.obj(
      "indices" -> value.indices.toVector.map(_.value).asJson,
      "maximumConcurrent" -> value.maximumConcurrent.map(_.toInt).asJson
    )

  private def optionalArray(cursor: HCursor): Either[String, Option[JobArrayRequest]] =
    cursor.downField("array").focus match
      case None | Some(Json.Null) => Right(None)
      case Some(value)            =>
        for
          indicesRaw <- value.hcursor.get[Vector[Int]]("indices").left.map(_.message)
          indices <- indicesRaw.traverse(raw => ArrayIndex.from(raw).left.map(_.reason))
          maximumRaw <- value.hcursor.get[Option[Int]]("maximumConcurrent").left.map(_.message)
          maximum <- maximumRaw.traverse(raw =>
            PositiveInt.from("maximumConcurrent", raw).left.map(_.reason)
          )
          request <- JobArrayRequest
            .from(indices, maximum)
            .left
            .map(_.toChain.toVector.map(_.message).mkString("; "))
        yield Some(request)

  private def optionalRetrySafety(cursor: HCursor): Either[String, RetrySafety] =
    cursor.downField("retrySafety").focus match
      case None | Some(Json.Null) => Right(RetrySafety.Unknown)
      case Some(value)            =>
        value.asString.toRight("retrySafety must be a string").flatMap(decodeRetrySafety)

  private def encodeRetrySafety(value: RetrySafety): String = value match
    case RetrySafety.Unknown               => "unknown"
    case RetrySafety.NoAutomaticRetry      => "no-automatic-retry"
    case RetrySafety.SafeForAutomaticRetry => "safe-for-automatic-retry"

  private def decodeRetrySafety(value: String): Either[String, RetrySafety] = value match
    case "unknown"                  => Right(RetrySafety.Unknown)
    case "no-automatic-retry"       => Right(RetrySafety.NoAutomaticRetry)
    case "safe-for-automatic-retry" => Right(RetrySafety.SafeForAutomaticRetry)
    case other                      => Left(s"unknown retrySafety: $other")

  extension [A: Encoder](value: A) private def asJson: Json = summon[Encoder[A]].apply(value)

  private given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)
  private given Decoder[Instant] = Decoder.decodeString.emap { raw =>
    Try(Instant.parse(raw)).toEither.left.map(_.getMessage)
  }
  private given Encoder[LocalDateTime] = Encoder.encodeString.contramap(_.toString)
  private given Decoder[LocalDateTime] = Decoder.decodeString.emap { raw =>
    Try(LocalDateTime.parse(raw)).toEither.left.map(_.getMessage)
  }

  private def stringEncoder[A](value: A => String): Encoder[A] =
    Encoder.encodeString.contramap(value)
  private def stringDecoder[A](construct: String => Either[ValidationFailure, A]): Decoder[A] =
    Decoder.decodeString.emap(raw => construct(raw).left.map(_.reason))

  private given Encoder[AttemptId] = stringEncoder(_.value)
  private given Decoder[AttemptId] = stringDecoder(AttemptId.from)
  private given Encoder[JobId] = stringEncoder(_.value)
  private given Decoder[JobId] = stringDecoder(JobId.from)
  private given Encoder[ClusterName] = stringEncoder(_.value)
  private given Decoder[ClusterName] = stringDecoder(ClusterName.from)
  private given Encoder[FileIdentity] = stringEncoder(_.value)
  private given Decoder[FileIdentity] = stringDecoder(FileIdentity.from)

  private given Encoder[AttemptEpoch] = Encoder.encodeLong.contramap(_.value)
  private given Decoder[AttemptEpoch] =
    Decoder.decodeLong.emap(raw => AttemptEpoch.from(raw).left.map(_.reason))
  private given Encoder[LogOffset] = Encoder.encodeLong.contramap(_.value)
  private given Decoder[LogOffset] =
    Decoder.decodeLong.emap(raw => LogOffset.from(raw).left.map(_.reason))
  private given Encoder[ArrayIndex] = Encoder.encodeInt.contramap(_.value)
  private given Decoder[ArrayIndex] =
    Decoder.decodeInt.emap(raw => ArrayIndex.from(raw).left.map(_.reason))
  private given Encoder[DurationMillis] = Encoder.encodeLong.contramap(_.value)
  private given Decoder[DurationMillis] =
    Decoder.decodeLong.emap(raw => DurationMillis.from(raw).left.map(_.reason))
  private given Encoder[PositiveInt] = Encoder.encodeInt.contramap(_.toInt)
  private given Decoder[PositiveInt] =
    Decoder.decodeInt.emap(raw => PositiveInt.from("positiveInt", raw).left.map(_.reason))
  private given Encoder[Mebibytes] = Encoder.encodeLong.contramap(_.toLong)
  private given Decoder[Mebibytes] =
    Decoder.decodeLong.emap(raw => Mebibytes.from(raw).left.map(_.reason))
  private given Encoder[WallTimeMinutes] = Encoder.encodeLong.contramap(_.toLong)
  private given Decoder[WallTimeMinutes] =
    Decoder.decodeLong.emap(raw => WallTimeMinutes.from(raw).left.map(_.reason))

  private given Encoder[EvidenceSource] = deriveEncoder
  private given Decoder[EvidenceSource] = deriveDecoder
  private given Encoder[BoundedEvidence] = Encoder.instance { value =>
    Json.obj(
      "source" -> value.source.asJson,
      "observedAt" -> value.observedAt.asJson,
      "bytesBase64" -> Json.fromString(Base64.getEncoder.encodeToString(value.bytes.toArray)),
      "originalByteCount" -> Json.fromLong(value.originalByteCount)
    )
  }
  private given Decoder[BoundedEvidence] = Decoder.instance { cursor =>
    for
      source <- cursor.get[EvidenceSource]("source")
      observedAt <- cursor.get[Instant]("observedAt")
      encoded <- cursor.get[String]("bytesBase64")
      bytes <- Try(Base64.getDecoder.decode(encoded).toVector).toEither.left
        .map(error => io.circe.DecodingFailure(error.getMessage, cursor.history))
      original <- cursor.get[Long]("originalByteCount")
      value <- Either
        .cond(
          original >= bytes.size.toLong,
          BoundedEvidence.fromCapture(source, observedAt, bytes, original),
          io.circe
            .DecodingFailure("originalByteCount is smaller than retained bytes", cursor.history)
        )
    yield value
  }
  private given Encoder[EvidenceBundle] = deriveEncoder
  private given Decoder[EvidenceBundle] = deriveDecoder
  private given Encoder[Diagnostic] = deriveEncoder
  private given Decoder[Diagnostic] = deriveDecoder
  private given Encoder[Diagnostics] = Encoder.encodeVector[Diagnostic].contramap(_.toVector)
  private given Decoder[Diagnostics] = Decoder
    .decodeVector[Diagnostic]
    .emap(values => Diagnostics.fromVector(values).left.map(_.reason))

  private given Encoder[MemoryRequest] = deriveEncoder
  private given Decoder[MemoryRequest] = deriveDecoder
  private given Encoder[ResourceRequest] = deriveEncoder
  private given Decoder[ResourceRequest] = deriveDecoder
  private given Encoder[ScriptSource] = deriveEncoder
  private given Decoder[ScriptSource] = deriveDecoder

  private given Encoder[SpawnFailureKind] = deriveEncoder
  private given Decoder[SpawnFailureKind] = deriveDecoder
  private given Encoder[InvocationResult] = deriveEncoder
  private given Decoder[InvocationResult] = deriveDecoder
  private given Encoder[JobRef] = deriveEncoder
  private given Decoder[JobRef] = deriveDecoder
  private given Encoder[AcceptanceUncertainty] = deriveEncoder
  private given Decoder[AcceptanceUncertainty] = deriveDecoder
  private given Encoder[Submission] = deriveEncoder
  private given Decoder[Submission] = deriveDecoder
  private given Encoder[SubmissionAttempt] = deriveEncoder
  private given Decoder[SubmissionAttempt] = deriveDecoder
  private given Encoder[CancellationResult] = deriveEncoder
  private given Decoder[CancellationResult] = deriveDecoder
  private given Encoder[CancellationAttempt] = deriveEncoder
  private given Decoder[CancellationAttempt] = deriveDecoder

  private given Encoder[CapabilitySupport] = deriveEncoder
  private given Decoder[CapabilitySupport] = deriveDecoder
  private given Encoder[SchedulerCapabilities] = deriveEncoder
  private given Decoder[SchedulerCapabilities] = deriveDecoder
  private given [A: Encoder]: Encoder[SchedulerQueryResult[A]] = deriveEncoder
  private given [A: Decoder]: Decoder[SchedulerQueryResult[A]] = deriveDecoder

  private given Encoder[SlurmState] = Encoder.instance {
    case SlurmState.Unknown(raw) =>
      Json.obj(
        "code" -> Json.fromString("unknown"),
        "raw" -> Json.fromString(raw)
      )
    case state =>
      Json.obj("code" -> Json.fromString(slurmStateCode(state)))
  }
  private given Decoder[SlurmState] = Decoder.instance { cursor =>
    cursor.value.asString match
      case Some(code) => decodeSlurmState(code, None, cursor)
      case None       =>
        cursor.get[Option[String]]("code").flatMap {
          case Some(code) =>
            cursor.get[Option[String]]("raw").flatMap(raw => decodeSlurmState(code, raw, cursor))
          case None => decodeLegacySlurmState(cursor)
        }
  }
  private given Encoder[Freshness] = deriveEncoder
  private given Decoder[Freshness] = deriveDecoder
  private given Encoder[WorkloadOutcome] = deriveEncoder
  private given Decoder[WorkloadOutcome] = deriveDecoder
  private given Encoder[SchedulerTimestamp] = Encoder.instance {
    case SchedulerTimestamp.Absolute(value) =>
      Json.obj(
        "kind" -> Json.fromString("absolute"),
        "value" -> value.asJson
      )
    case SchedulerTimestamp.SiteLocal(value) =>
      Json.obj(
        "kind" -> Json.fromString("site-local"),
        "value" -> value.asJson
      )
  }
  private given Decoder[SchedulerTimestamp] = Decoder.instance { cursor =>
    cursor.get[String]("kind").flatMap {
      case "absolute"   => cursor.get[Instant]("value").map(SchedulerTimestamp.Absolute.apply)
      case "site-local" =>
        cursor.get[LocalDateTime]("value").map(SchedulerTimestamp.SiteLocal.apply)
      case other =>
        Left(DecodingFailure(s"unknown scheduler timestamp kind: $other", cursor.history))
    }
  }
  private given Encoder[JobStart] = Encoder.instance {
    case JobStart.Actual(at) =>
      Json.obj("kind" -> Json.fromString("actual"), "at" -> at.asJson)
    case JobStart.Expected(at) =>
      Json.obj("kind" -> Json.fromString("expected"), "at" -> at.asJson)
    case JobStart.Reported(at) =>
      Json.obj("kind" -> Json.fromString("reported"), "at" -> at.asJson)
  }
  private given Decoder[JobStart] = Decoder.instance { cursor =>
    cursor.get[String]("kind").flatMap {
      case "actual"   => cursor.get[SchedulerTimestamp]("at").map(JobStart.Actual.apply)
      case "expected" => cursor.get[SchedulerTimestamp]("at").map(JobStart.Expected.apply)
      case "reported" => cursor.get[SchedulerTimestamp]("at").map(JobStart.Reported.apply)
      case other      => Left(DecodingFailure(s"unknown job start kind: $other", cursor.history))
    }
  }
  private given Encoder[ObservedTimeLimit] = Encoder.instance {
    case ObservedTimeLimit.Limited(value) =>
      Json.obj(
        "kind" -> Json.fromString("limited"),
        "minutes" -> Json.fromLong(value.toLong)
      )
    case ObservedTimeLimit.Unlimited =>
      Json.obj("kind" -> Json.fromString("unlimited"))
    case ObservedTimeLimit.PartitionDefault =>
      Json.obj("kind" -> Json.fromString("partition-default"))
    case ObservedTimeLimit.Unknown(raw) =>
      Json.obj(
        "kind" -> Json.fromString("unknown"),
        "raw" -> raw.asJson
      )
  }
  private given Decoder[ObservedTimeLimit] = Decoder.instance { cursor =>
    cursor.get[String]("kind").flatMap {
      case "limited" =>
        cursor
          .get[Long]("minutes")
          .flatMap(raw =>
            WallTimeMinutes
              .from(raw)
              .left
              .map(problem => DecodingFailure(problem.reason, cursor.history))
          )
          .map(ObservedTimeLimit.Limited.apply)
      case "unlimited"         => Right(ObservedTimeLimit.Unlimited)
      case "partition-default" =>
        Right(ObservedTimeLimit.PartitionDefault)
      case "unknown" => cursor.get[Option[String]]("raw").map(ObservedTimeLimit.Unknown.apply)
      case other     =>
        Left(DecodingFailure(s"unknown observed time-limit kind: $other", cursor.history))
    }
  }
  private given Encoder[JobTiming] = Encoder.instance { value =>
    Json.obj(
      "start" -> value.start.asJson,
      "projectedEndAt" -> value.projectedEndAt.asJson,
      "timeLimit" -> value.timeLimit.asJson
    )
  }
  private given Decoder[JobTiming] = Decoder.instance { cursor =>
    for
      start <- cursor.get[Option[JobStart]]("start")
      projectedEndAt <- cursor.get[Option[SchedulerTimestamp]]("projectedEndAt")
      timeLimit <- cursor.get[ObservedTimeLimit]("timeLimit")
    yield JobTiming(start, projectedEndAt, timeLimit)
  }
  private given Encoder[JobObservation] = Encoder.instance { value =>
    Json.obj(
      "job" -> value.job.asJson,
      "state" -> value.state.asJson,
      "freshness" -> value.freshness.asJson,
      "reason" -> value.reason.asJson,
      "rawFields" -> value.rawFields.asJson,
      "evidence" -> value.evidence.asJson,
      "timing" -> value.timing.asJson
    )
  }
  private given Decoder[JobObservation] = Decoder.instance { cursor =>
    for
      job <- cursor.get[JobRef]("job")
      state <- cursor.get[SlurmState]("state")
      freshness <- cursor.get[Freshness]("freshness")
      reason <- cursor.get[Option[String]]("reason")
      rawFields <- cursor.get[Map[String, String]]("rawFields")
      evidence <- cursor.get[EvidenceBundle]("evidence")
      timing <- cursor.get[Option[JobTiming]]("timing").map(_.getOrElse(JobTiming.unknown))
    yield JobObservation(job, state, freshness, reason, rawFields, evidence, timing)
  }
  private given Encoder[ExitStatus] = deriveEncoder
  private given Decoder[ExitStatus] = deriveDecoder
  private given Encoder[AccountingRecord] = deriveEncoder
  private given Decoder[AccountingRecord] = deriveDecoder
  private given Encoder[ObservationResult] = deriveEncoder
  private given Decoder[ObservationResult] = deriveDecoder
  private given Encoder[ObservationBatch] = Encoder
    .encodeVector[ObservationResult]
    .contramap(
      _.results.toVector
    )
  private given Decoder[ObservationBatch] = Decoder
    .decodeVector[ObservationResult]
    .emap(values =>
      NonEmptyVector
        .fromVector(values)
        .map(ObservationBatch.apply)
        .toRight("observations must not be empty")
    )
  private given Encoder[AccountingBatch] = deriveEncoder
  private given Decoder[AccountingBatch] = deriveDecoder

  private given Encoder[LogStream] = deriveEncoder
  private given Decoder[LogStream] = deriveDecoder
  private given Encoder[LogRef] = deriveEncoder
  private given Decoder[LogRef] = deriveDecoder
  private given Encoder[LogCursor] = deriveEncoder
  private given Decoder[LogCursor] = deriveDecoder
  private given Encoder[LogPage] = deriveEncoder
  private given Decoder[LogPage] = deriveDecoder
  private given Encoder[LogReadResult] = deriveEncoder
  private given Decoder[LogReadResult] = deriveDecoder

  private def slurmStateCode(state: SlurmState): String = state match
    case SlurmState.Pending           => "pending"
    case SlurmState.Running           => "running"
    case SlurmState.Completing        => "completing"
    case SlurmState.Completed         => "completed"
    case SlurmState.Failed            => "failed"
    case SlurmState.Cancelled         => "cancelled"
    case SlurmState.OutOfMemory       => "out-of-memory"
    case SlurmState.TimedOut          => "timed-out"
    case SlurmState.NodeFailure       => "node-failure"
    case SlurmState.Preempted         => "preempted"
    case SlurmState.Requeued          => "requeued"
    case SlurmState.RequeueHeld       => "requeue-held"
    case SlurmState.RequeueFederation => "requeue-federation"
    case SlurmState.SpecialExit       => "special-exit"
    case SlurmState.Unknown(_)        => "unknown"

  private def decodeSlurmState(
      code: String,
      raw: Option[String],
      cursor: HCursor
  ): Decoder.Result[SlurmState] =
    code match
      case "pending"            => Right(SlurmState.Pending)
      case "running"            => Right(SlurmState.Running)
      case "completing"         => Right(SlurmState.Completing)
      case "completed"          => Right(SlurmState.Completed)
      case "failed"             => Right(SlurmState.Failed)
      case "cancelled"          => Right(SlurmState.Cancelled)
      case "out-of-memory"      => Right(SlurmState.OutOfMemory)
      case "timed-out"          => Right(SlurmState.TimedOut)
      case "node-failure"       => Right(SlurmState.NodeFailure)
      case "preempted"          => Right(SlurmState.Preempted)
      case "requeued"           => Right(SlurmState.Requeued)
      case "requeue-held"       => Right(SlurmState.RequeueHeld)
      case "requeue-federation" => Right(SlurmState.RequeueFederation)
      case "special-exit"       => Right(SlurmState.SpecialExit)
      case "unknown"            =>
        raw
          .filter(_.nonEmpty)
          .map(value => Right(SlurmState.Unknown(value)))
          .getOrElse(
            Left(DecodingFailure("unknown Slurm state requires non-empty raw text", cursor.history))
          )
      case other =>
        Left(DecodingFailure(s"unknown Slurm state code: $other", cursor.history))

  private def decodeLegacySlurmState(cursor: HCursor): Decoder.Result[SlurmState] =
    cursor.value.asObject.flatMap(_.toVector match
      case Vector((name, payload)) => Some(name -> payload)
      case _                       => None) match
      case Some(("Pending", _))       => Right(SlurmState.Pending)
      case Some(("Running", _))       => Right(SlurmState.Running)
      case Some(("Completing", _))    => Right(SlurmState.Completing)
      case Some(("Completed", _))     => Right(SlurmState.Completed)
      case Some(("Failed", _))        => Right(SlurmState.Failed)
      case Some(("Cancelled", _))     => Right(SlurmState.Cancelled)
      case Some(("OutOfMemory", _))   => Right(SlurmState.OutOfMemory)
      case Some(("TimedOut", _))      => Right(SlurmState.TimedOut)
      case Some(("NodeFailure", _))   => Right(SlurmState.NodeFailure)
      case Some(("Preempted", _))     => Right(SlurmState.Preempted)
      case Some(("Unknown", payload)) =>
        payload.hcursor
          .get[String]("raw")
          .flatMap(raw => decodeSlurmState("unknown", Some(raw), cursor))
      case _ =>
        Left(DecodingFailure("invalid legacy Slurm state", cursor.history))
