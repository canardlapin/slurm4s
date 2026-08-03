package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.HCursor
import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.core.codec.CodecFailure
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson
import io.github.bbuchsbaum.slurm4s.core.codec.WireEnvelope

import scodec.bits.ByteVector

import java.time.Instant
import java.util.Base64
import scala.util.Try

enum StructuredCodecFailure derives CanEqual:
  case TooLarge(actualBytes: Long, maximumBytes: Int)
  case Envelope(failure: CodecFailure)
  case WrongSchema(received: String)
  case Invalid(message: String)

import StructuredJson.*

object ResultEnvelopeCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.result-envelope")
  private val MaximumOutputs = 1024

  def encode(
      value: ResultEnvelope,
      maximumEnvelopeBytes: ByteLimit,
      maximumValueBytes: ByteLimit
  ): Either[StructuredCodecFailure, ByteVector] =
    for
      _ <- bounded(value.value.fold(0L)(_.size.toLong), maximumValueBytes)
      _ <- Either.cond(
        value.outputs.entries.size <= MaximumOutputs,
        (),
        StructuredCodecFailure.Invalid(s"result envelope exceeds $MaximumOutputs outputs")
      )
      bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, encodePayload(value)))
      _ <- bounded(bytes.size.toLong, maximumEnvelopeBytes)
    yield bytes

  def decode(
      bytes: ByteVector,
      maximumEnvelopeBytes: ByteLimit,
      maximumValueBytes: ByteLimit
  ): Either[StructuredCodecFailure, ResultEnvelope] =
    for
      _ <- bounded(bytes.size.toLong, maximumEnvelopeBytes)
      envelope <- VersionedJson.decode(bytes).left.map(StructuredCodecFailure.Envelope.apply)
      _ <- Either.cond(
        envelope.schema == schema,
        (),
        StructuredCodecFailure.WrongSchema(envelope.schema.value)
      )
      value <- decodePayload(envelope.payload, maximumValueBytes)
    yield value

  private def encodePayload(value: ResultEnvelope): Json =
    Json.obj(
      "submissionKey" -> Json.fromString(value.submissionKey.value),
      "attemptId" -> Json.fromString(value.attemptId.value),
      "attemptEpoch" -> Json.fromLong(value.attemptEpoch.value),
      "job" -> value.job.fold(Json.Null)(encodeJob),
      "operation" -> encodeOperation(value.operation),
      "resultSchema" -> Json.fromString(value.resultSchema.value),
      "status" -> encodeStatus(value.status),
      "valueBase64" -> value.value.fold(Json.Null)(bytes =>
        Json.fromString(Base64.getEncoder.encodeToString(bytes.toArray))
      ),
      "outputs" -> Json.arr(value.outputs.entries.map(encodeOutput)*),
      "workerRelease" -> encodeWorkerRelease(value.workerRelease),
      "completedAt" -> Json.fromString(value.completedAt.toString)
    )

  private def decodePayload(
      json: Json,
      maximumValueBytes: ByteLimit
  ): Either[StructuredCodecFailure, ResultEnvelope] =
    for
      cursor <- objectCursor(json, "result envelope")
      submissionKey <- identifier(cursor, "submissionKey", SubmissionKey.from)
      attemptId <- identifier(cursor, "attemptId", AttemptId.from)
      epochValue <- field[Long](cursor, "attemptEpoch")
      epoch <- AttemptEpoch.from(epochValue).left.map(problem => invalid(problem.reason))
      job <- optionalObject(cursor, "job", decodeJob)
      operationJson <- requiredJson(cursor, "operation")
      operation <- decodeOperation(operationJson)
      resultSchema <- identifier(cursor, "resultSchema", ResultSchemaId.from)
      statusJson <- requiredJson(cursor, "status")
      status <- decodeStatus(statusJson)
      encodedValue <- optionalField[String](cursor, "valueBase64")
      value <- encodedValue.traverse(decodeBase64(_, maximumValueBytes))
      outputsJson <- field[Vector[Json]](cursor, "outputs")
      _ <- Either.cond(
        outputsJson.size <= MaximumOutputs,
        (),
        invalid(s"result envelope exceeds $MaximumOutputs outputs")
      )
      outputEntries <- outputsJson.traverse(decodeOutput)
      outputs <- OutputManifest.from(outputEntries).left.map(problem => invalid(problem.reason))
      releaseJson <- requiredJson(cursor, "workerRelease")
      release <- decodeWorkerRelease(releaseJson)
      completedText <- field[String](cursor, "completedAt")
      completed <- parseInstant(completedText)
      envelope <- ResultEnvelope
        .decoded(
          submissionKey,
          attemptId,
          epoch,
          job,
          operation,
          resultSchema,
          status,
          value,
          outputs,
          release,
          completed
        )
        .left
        .map(invalid)
    yield envelope

  private def bounded(
      actual: Long,
      maximum: ByteLimit
  ): Either[StructuredCodecFailure, Unit] =
    Either.cond(
      actual <= maximum.value.toLong,
      (),
      StructuredCodecFailure.TooLarge(actual, maximum.value)
    )

  private def decodeBase64(
      value: String,
      maximum: ByteLimit
  ): Either[StructuredCodecFailure, ByteVector] =
    val maximumEncoded = ((maximum.value.toLong + 2L) / 3L) * 4L
    for
      _ <- Either.cond(
        value.length.toLong <= maximumEncoded,
        (),
        StructuredCodecFailure.TooLarge(
          value.length.toLong,
          math.min(maximumEncoded, Int.MaxValue.toLong).toInt
        )
      )
      bytes <- Try(ByteVector.view(Base64.getDecoder.decode(value))).toEither.left.map(error =>
        invalid(s"valueBase64 is invalid: ${error.getMessage}")
      )
      _ <- bounded(bytes.size.toLong, maximum)
    yield bytes

  private def invalid(message: String): StructuredCodecFailure =
    StructuredCodecFailure.Invalid(message)

object DurableResultHandleCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.result-handle")
  private val MaximumOutputs = 1024

  def encode(
      value: DurableResultHandle,
      maximumBytes: ByteLimit
  ): Either[StructuredCodecFailure, ByteVector] =
    for
      _ <- Either.cond(
        value.declaredOutputs.size <= MaximumOutputs &&
          value.declaredOutputs.distinct.size == value.declaredOutputs.size,
        (),
        StructuredCodecFailure.Invalid("result handle outputs must be distinct and bounded")
      )
      fields = Vector(
        "submissionKey" -> Json.fromString(value.submissionKey.value),
        "attemptId" -> Json.fromString(value.attemptId.value),
        "attemptEpoch" -> Json.fromLong(value.attemptEpoch.value),
        "job" -> value.job.fold(Json.Null)(encodeJob),
        "operation" -> encodeOperation(value.operation),
        "resultSchema" -> Json.fromString(value.resultSchema.value),
        "maximumResultBytes" -> Json.fromInt(value.maximumResultBytes.value),
        "maximumEnvelopeBytes" -> Json.fromInt(value.maximumEnvelopeBytes.value),
        "declaredOutputs" -> Json.arr(
          value.declaredOutputs.map(path => Json.fromString(path.value))*
        ),
        "workerRelease" -> encodeWorkerRelease(value.workerRelease)
      ) ++ encodeRetrySafetyField(value.retrySafety)
      payload = Json.obj(fields*)
      bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, payload))
      _ <- Either.cond(
        bytes.size <= maximumBytes.value,
        (),
        StructuredCodecFailure.TooLarge(bytes.size.toLong, maximumBytes.value)
      )
    yield bytes

  def decode(
      bytes: ByteVector,
      maximumBytes: ByteLimit
  ): Either[StructuredCodecFailure, DurableResultHandle] =
    for
      _ <- Either.cond(
        bytes.size <= maximumBytes.value,
        (),
        StructuredCodecFailure.TooLarge(bytes.size.toLong, maximumBytes.value)
      )
      envelope <- VersionedJson.decode(bytes).left.map(StructuredCodecFailure.Envelope.apply)
      _ <- Either.cond(
        envelope.schema == schema,
        (),
        StructuredCodecFailure.WrongSchema(envelope.schema.value)
      )
      cursor <- objectCursor(envelope.payload, "result handle")
      submissionKey <- identifier(cursor, "submissionKey", SubmissionKey.from)
      attemptId <- identifier(cursor, "attemptId", AttemptId.from)
      epochValue <- field[Long](cursor, "attemptEpoch")
      epoch <- AttemptEpoch.from(epochValue).left.map(problem => invalid(problem.reason))
      job <- optionalObject(cursor, "job", decodeJob)
      operationJson <- requiredJson(cursor, "operation")
      operation <- decodeOperation(operationJson)
      resultSchema <- identifier(cursor, "resultSchema", ResultSchemaId.from)
      resultMaximum <- field[Int](cursor, "maximumResultBytes")
      maximumResultBytes <- ByteLimit
        .from(resultMaximum)
        .left
        .map(problem => invalid(problem.reason))
      envelopeMaximum <- field[Int](cursor, "maximumEnvelopeBytes")
      maximumEnvelopeBytes <- ByteLimit
        .from(envelopeMaximum)
        .left
        .map(problem => invalid(problem.reason))
      outputTexts <- field[Vector[String]](cursor, "declaredOutputs")
      _ <- Either.cond(
        outputTexts.size <= MaximumOutputs,
        (),
        invalid(s"result handle exceeds $MaximumOutputs outputs")
      )
      outputs <- outputTexts.traverse(raw =>
        RelativeOutputPath.from(raw).left.map(problem => invalid(problem.reason))
      )
      _ <- Either.cond(
        outputs.distinct.size == outputs.size,
        (),
        invalid("result handle outputs must be distinct")
      )
      releaseJson <- requiredJson(cursor, "workerRelease")
      release <- decodeWorkerRelease(releaseJson)
      retrySafety <- decodeRetrySafetyField(cursor)
      // Decode is where the bounds are checked, not where they are assumed.
      handle <- DurableResultHandle
        .from(
          submissionKey,
          attemptId,
          epoch,
          job,
          operation,
          resultSchema,
          maximumResultBytes,
          maximumEnvelopeBytes,
          outputs,
          release,
          retrySafety
        )
        .left
        .map(problem => invalid(problem.reason))
    yield handle

object TaskInvocationCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.task-invocation")
  private val MaximumOutputs = 1024

  def encode(
      value: TaskInvocation,
      maximumBytes: ByteLimit
  ): Either[StructuredCodecFailure, ByteVector] =
    for
      _ <- Either.cond(
        value.inputBytes.size <= value.maximumInputBytes.value,
        (),
        StructuredCodecFailure.TooLarge(
          value.inputBytes.size.toLong,
          value.maximumInputBytes.value
        )
      )
      _ <- Either.cond(
        value.declaredOutputs.size <= MaximumOutputs &&
          value.declaredOutputs.distinct.size == value.declaredOutputs.size,
        (),
        invalid("task invocation outputs must be distinct and bounded")
      )
      fields = Vector(
        "submissionKey" -> Json.fromString(value.submissionKey.value),
        "attemptId" -> Json.fromString(value.attemptId.value),
        "attemptEpoch" -> Json.fromLong(value.attemptEpoch.value),
        "job" -> value.job.fold(Json.Null)(encodeJob),
        "operation" -> encodeRegisteredOperation(value.operation),
        "inputBase64" -> Json.fromString(
          Base64.getEncoder.encodeToString(value.inputBytes.toArray)
        ),
        "declaredOutputs" -> Json.arr(
          value.declaredOutputs.map(path => Json.fromString(path.value))*
        ),
        "maximumInputBytes" -> Json.fromInt(value.maximumInputBytes.value),
        "maximumResultBytes" -> Json.fromInt(value.maximumResultBytes.value),
        "maximumEnvelopeBytes" -> Json.fromInt(value.maximumEnvelopeBytes.value),
        "maximumOutputBytes" -> Json.fromInt(value.maximumOutputBytes.value),
        "workerRelease" -> encodeWorkerRelease(value.workerRelease)
      ) ++ encodeRetrySafetyField(value.retrySafety)
      payload = Json.obj(fields*)
      bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, payload))
      _ <- Either.cond(
        bytes.size <= maximumBytes.value,
        (),
        StructuredCodecFailure.TooLarge(bytes.size.toLong, maximumBytes.value)
      )
    yield bytes

  def decode(
      bytes: ByteVector,
      maximumBytes: ByteLimit,
      maximumAllowedInputBytes: ByteLimit
  ): Either[StructuredCodecFailure, TaskInvocation] =
    for
      _ <- Either.cond(
        bytes.size <= maximumBytes.value,
        (),
        StructuredCodecFailure.TooLarge(bytes.size.toLong, maximumBytes.value)
      )
      envelope <- VersionedJson.decode(bytes).left.map(StructuredCodecFailure.Envelope.apply)
      _ <- Either.cond(
        envelope.schema == schema,
        (),
        StructuredCodecFailure.WrongSchema(envelope.schema.value)
      )
      cursor <- objectCursor(envelope.payload, "task invocation")
      submissionKey <- identifier(cursor, "submissionKey", SubmissionKey.from)
      attemptId <- identifier(cursor, "attemptId", AttemptId.from)
      epochValue <- field[Long](cursor, "attemptEpoch")
      epoch <- AttemptEpoch.from(epochValue).left.map(problem => invalid(problem.reason))
      job <- optionalObject(cursor, "job", decodeJob)
      operationJson <- requiredJson(cursor, "operation")
      operation <- decodeRegisteredOperation(operationJson)
      inputMaximum <- field[Int](cursor, "maximumInputBytes")
      maximumInputBytes <- ByteLimit
        .from(inputMaximum)
        .left
        .map(problem => invalid(problem.reason))
      _ <- Either.cond(
        maximumInputBytes.value <= maximumAllowedInputBytes.value,
        (),
        StructuredCodecFailure.TooLarge(
          maximumInputBytes.value.toLong,
          maximumAllowedInputBytes.value
        )
      )
      inputText <- field[String](cursor, "inputBase64")
      input <- decodeBase64(inputText, maximumInputBytes)
      outputTexts <- field[Vector[String]](cursor, "declaredOutputs")
      _ <- Either.cond(
        outputTexts.size <= MaximumOutputs,
        (),
        invalid(s"task invocation exceeds $MaximumOutputs outputs")
      )
      outputs <- outputTexts.traverse(raw =>
        RelativeOutputPath.from(raw).left.map(problem => invalid(problem.reason))
      )
      _ <- Either.cond(
        outputs.distinct.size == outputs.size,
        (),
        invalid("task invocation outputs must be distinct")
      )
      resultMaximum <- field[Int](cursor, "maximumResultBytes")
      maximumResultBytes <- ByteLimit
        .from(resultMaximum)
        .left
        .map(problem => invalid(problem.reason))
      envelopeMaximum <- field[Int](cursor, "maximumEnvelopeBytes")
      maximumEnvelopeBytes <- ByteLimit
        .from(envelopeMaximum)
        .left
        .map(problem => invalid(problem.reason))
      outputMaximum <- field[Int](cursor, "maximumOutputBytes")
      maximumOutputBytes <- ByteLimit
        .from(outputMaximum)
        .left
        .map(problem => invalid(problem.reason))
      releaseJson <- requiredJson(cursor, "workerRelease")
      release <- decodeWorkerRelease(releaseJson)
      retrySafety <- decodeRetrySafetyField(cursor)
      invocation <- TaskInvocation
        .from(
          submissionKey,
          attemptId,
          epoch,
          job,
          operation,
          input,
          outputs,
          maximumInputBytes,
          maximumResultBytes,
          maximumEnvelopeBytes,
          maximumOutputBytes,
          release,
          retrySafety
        )
        .left
        .map(problem => invalid(problem.reason))
    yield invocation

  private def decodeBase64(
      value: String,
      maximum: ByteLimit
  ): Either[StructuredCodecFailure, ByteVector] =
    val maximumEncoded = ((maximum.value.toLong + 2L) / 3L) * 4L
    for
      _ <- Either.cond(
        value.length.toLong <= maximumEncoded,
        (),
        StructuredCodecFailure.TooLarge(
          value.length.toLong,
          math.min(maximumEncoded, Int.MaxValue.toLong).toInt
        )
      )
      decoded <- Try(ByteVector.view(Base64.getDecoder.decode(value))).toEither.left.map(error =>
        invalid(s"inputBase64 is invalid: ${error.getMessage}")
      )
      _ <- Either.cond(
        decoded.size <= maximum.value,
        (),
        StructuredCodecFailure.TooLarge(decoded.size.toLong, maximum.value)
      )
    yield decoded

private[protocol] object StructuredJson:
  def encodeRetrySafetyField(value: RetrySafety): Vector[(String, Json)] =
    Option
      .when(value != RetrySafety.Unknown)(
        "retrySafety" -> Json.fromString(
          value match
            case RetrySafety.Unknown               => "unknown"
            case RetrySafety.NoAutomaticRetry      => "no-automatic-retry"
            case RetrySafety.SafeForAutomaticRetry => "safe-for-automatic-retry"
        )
      )
      .toVector

  def decodeRetrySafetyField(cursor: HCursor): Either[StructuredCodecFailure, RetrySafety] =
    optionalField[String](cursor, "retrySafety").flatMap {
      case None | Some("unknown")           => Right(RetrySafety.Unknown)
      case Some("no-automatic-retry")       => Right(RetrySafety.NoAutomaticRetry)
      case Some("safe-for-automatic-retry") => Right(RetrySafety.SafeForAutomaticRetry)
      case Some(other)                      => Left(invalid(s"unknown retrySafety: $other"))
    }

  def encodeRegisteredOperation(value: RegisteredOperation): Json =
    Json.obj(
      "id" -> Json.fromString(value.id.value),
      "version" -> Json.fromString(value.version.value),
      "inputSchema" -> Json.fromString(value.inputSchema.value),
      "outputSchema" -> Json.fromString(value.outputSchema.value)
    )

  def decodeRegisteredOperation(
      json: Json
  ): Either[StructuredCodecFailure, RegisteredOperation] =
    for
      cursor <- objectCursor(json, "registered operation")
      id <- identifier(cursor, "id", OperationId.from)
      version <- identifier(cursor, "version", OperationVersion.from)
      inputSchema <- identifier(cursor, "inputSchema", SchemaId.from)
      outputSchema <- identifier(cursor, "outputSchema", ResultSchemaId.from)
    yield RegisteredOperation(id, version, inputSchema, outputSchema)

  def encodeOperation(value: WorkloadOperation): Json = value match
    case WorkloadOperation.Script(digest) =>
      Json.obj(
        "kind" -> Json.fromString("script"),
        "sourceDigest" -> Json.fromString(digest.value)
      )
    case WorkloadOperation.Registered(id, version) =>
      Json.obj(
        "kind" -> Json.fromString("registered"),
        "id" -> Json.fromString(id.value),
        "version" -> Json.fromString(version.value)
      )

  def decodeOperation(json: Json): Either[StructuredCodecFailure, WorkloadOperation] =
    for
      cursor <- objectCursor(json, "operation")
      kind <- field[String](cursor, "kind")
      value <- kind match
        case "script" =>
          identifier(cursor, "sourceDigest", ContentDigest.from).map(WorkloadOperation.Script.apply)
        case "registered" =>
          (
            identifier(cursor, "id", OperationId.from),
            identifier(cursor, "version", OperationVersion.from)
          ).mapN(WorkloadOperation.Registered.apply)
        case other => Left(invalid(s"unknown operation kind: $other"))
    yield value

  def encodeWorkerRelease(value: WorkerRelease): Json =
    Json.obj(
      "id" -> Json.fromString(value.id.value),
      "digest" -> Json.fromString(value.digest.value)
    )

  def decodeWorkerRelease(json: Json): Either[StructuredCodecFailure, WorkerRelease] =
    for
      cursor <- objectCursor(json, "worker release")
      id <- identifier(cursor, "id", WorkerReleaseId.from)
      digest <- identifier(cursor, "digest", ContentDigest.from)
    yield WorkerRelease(id, digest)

  def encodeJob(value: JobRef): Json =
    Json.obj(
      "jobId" -> Json.fromString(value.jobId.value),
      "arrayIndex" -> value.arrayIndex.fold(Json.Null)(item => Json.fromInt(item.value))
    )

  def decodeJob(json: Json): Either[StructuredCodecFailure, JobRef] =
    for
      cursor <- objectCursor(json, "job")
      id <- identifier(cursor, "jobId", JobId.from)
      // A legacy record may still carry `cluster`. `JobRef` no longer models it, so it is ignored
      // outright: validating a value this decoder discards can only reject a record it would
      // otherwise read correctly.
      indexValue <- optionalField[Int](cursor, "arrayIndex")
      index <- indexValue.traverse(raw =>
        ArrayIndex.from(raw).left.map(problem => invalid(problem.reason))
      )
    yield JobRef(id, index)

  def encodeOutput(value: OutputEntry): Json =
    Json.obj(
      "path" -> Json.fromString(value.path.value),
      "sizeBytes" -> Json.fromLong(value.sizeBytes),
      "digest" -> Json.fromString(value.digest.value)
    )

  def decodeOutput(json: Json): Either[StructuredCodecFailure, OutputEntry] =
    for
      cursor <- objectCursor(json, "output")
      path <- identifier(cursor, "path", RelativeOutputPath.from)
      size <- field[Long](cursor, "sizeBytes")
      digest <- identifier(cursor, "digest", ContentDigest.from)
      value <- OutputEntry.from(path, size, digest).left.map(problem => invalid(problem.reason))
    yield value

  def encodeStatus(value: ResultEnvelopeStatus): Json = value match
    case ResultEnvelopeStatus.Succeeded => Json.obj("kind" -> Json.fromString("succeeded"))
    case ResultEnvelopeStatus.Failed(code, message) =>
      Json.obj(
        "kind" -> Json.fromString("failed"),
        "code" -> Json.fromString(code),
        "message" -> Json.fromString(message)
      )

  def decodeStatus(json: Json): Either[StructuredCodecFailure, ResultEnvelopeStatus] =
    for
      cursor <- objectCursor(json, "result status")
      kind <- field[String](cursor, "kind")
      status <- kind match
        case "succeeded" => Right(ResultEnvelopeStatus.Succeeded)
        case "failed"    =>
          (field[String](cursor, "code"), field[String](cursor, "message"))
            .mapN(ResultEnvelopeStatus.Failed.apply)
        case other => Left(invalid(s"unknown result status: $other"))
    yield status

  def objectCursor(json: Json, name: String): Either[StructuredCodecFailure, HCursor] =
    Either.cond(json.isObject, json.hcursor, invalid(s"$name must be an object"))

  def requiredJson(cursor: HCursor, name: String): Either[StructuredCodecFailure, Json] =
    cursor.downField(name).focus.toRight(invalid(s"missing $name"))

  def field[A: io.circe.Decoder](
      cursor: HCursor,
      name: String
  ): Either[StructuredCodecFailure, A] =
    cursor.get[A](name).left.map(error => invalid(error.message))

  def optionalField[A: io.circe.Decoder](
      cursor: HCursor,
      name: String
  ): Either[StructuredCodecFailure, Option[A]] =
    cursor.get[Option[A]](name).left.map(error => invalid(error.message))

  def optionalObject[A](
      cursor: HCursor,
      name: String,
      decode: Json => Either[StructuredCodecFailure, A]
  ): Either[StructuredCodecFailure, Option[A]] =
    cursor.downField(name).focus match
      case None                        => Right(None)
      case Some(value) if value.isNull => Right(None)
      case Some(value)                 => decode(value).map(Some(_))

  def identifier[A](
      cursor: HCursor,
      name: String,
      construct: String => Either[ValidationFailure, A]
  ): Either[StructuredCodecFailure, A] =
    field[String](cursor, name).flatMap(raw =>
      construct(raw).left.map(problem => invalid(problem.reason))
    )

  def parseInstant(value: String): Either[StructuredCodecFailure, Instant] =
    Try(Instant.parse(value)).toEither.left.map(error => invalid(error.getMessage))

  def invalid(message: String): StructuredCodecFailure = StructuredCodecFailure.Invalid(message)

  extension [A](values: Vector[A])
    def traverse[B](
        f: A => Either[StructuredCodecFailure, B]
    ): Either[StructuredCodecFailure, Vector[B]] =
      values.foldLeft[Either[StructuredCodecFailure, Vector[B]]](Right(Vector.empty)) {
        case (result, value) => result.flatMap(items => f(value).map(items :+ _))
      }

  extension [A](value: Option[A])
    def traverse[B](
        f: A => Either[StructuredCodecFailure, B]
    ): Either[StructuredCodecFailure, Option[B]] =
      value match
        case Some(item) => f(item).map(Some(_))
        case None       => Right(None)

  extension [A, B](values: (Either[StructuredCodecFailure, A], Either[StructuredCodecFailure, B]))
    def mapN[C](f: (A, B) => C): Either[StructuredCodecFailure, C] =
      values._1.flatMap(left => values._2.map(right => f(left, right)))

import StructuredJson.*

object WorkerEventCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.worker-event")

  def encode(
      value: WorkerEvent,
      maximumBytes: ByteLimit
  ): Either[StructuredCodecFailure, ByteVector] =
    val bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, encodePayload(value)))
    Either.cond(
      bytes.size <= maximumBytes.value,
      bytes,
      StructuredCodecFailure.TooLarge(bytes.size.toLong, maximumBytes.value)
    )

  def decode(
      bytes: ByteVector,
      maximumBytes: ByteLimit
  ): Either[StructuredCodecFailure, WorkerEvent] =
    for
      _ <- Either.cond(
        bytes.size <= maximumBytes.value,
        (),
        StructuredCodecFailure.TooLarge(bytes.size.toLong, maximumBytes.value)
      )
      envelope <- VersionedJson.decode(bytes).left.map(StructuredCodecFailure.Envelope.apply)
      _ <- Either.cond(
        envelope.schema == schema,
        (),
        StructuredCodecFailure.WrongSchema(envelope.schema.value)
      )
      event <- decodePayload(envelope.payload)
    yield event

  private def encodePayload(value: WorkerEvent): Json =
    Json.obj(
      "sequence" -> Json.fromLong(value.sequence),
      "submissionKey" -> Json.fromString(value.submissionKey.value),
      "attemptId" -> Json.fromString(value.attemptId.value),
      "attemptEpoch" -> Json.fromLong(value.attemptEpoch.value),
      "operation" -> encodeOperation(value.operation),
      "workerRelease" -> encodeWorkerRelease(value.workerRelease),
      "observedAt" -> Json.fromString(value.observedAt.toString),
      "payload" -> encodeEventPayload(value.payload)
    )

  private def decodePayload(json: Json): Either[StructuredCodecFailure, WorkerEvent] =
    for
      cursor <- objectCursor(json, "worker event")
      sequence <- field[Long](cursor, "sequence")
      _ <- Either.cond(sequence >= 0L, (), invalid("worker event sequence must not be negative"))
      submissionKey <- identifier(cursor, "submissionKey", SubmissionKey.from)
      attemptId <- identifier(cursor, "attemptId", AttemptId.from)
      epochValue <- field[Long](cursor, "attemptEpoch")
      epoch <- AttemptEpoch.from(epochValue).left.map(problem => invalid(problem.reason))
      operationJson <- requiredJson(cursor, "operation")
      operation <- decodeOperation(operationJson)
      releaseJson <- requiredJson(cursor, "workerRelease")
      release <- decodeWorkerRelease(releaseJson)
      observedText <- field[String](cursor, "observedAt")
      observed <- parseInstant(observedText)
      payloadJson <- requiredJson(cursor, "payload")
      payload <- decodeEventPayload(payloadJson)
    yield WorkerEvent(
      sequence,
      submissionKey,
      attemptId,
      epoch,
      operation,
      release,
      observed,
      payload
    )

  private def encodeEventPayload(value: WorkerEventPayload): Json = value match
    case WorkerEventPayload.Started => Json.obj("kind" -> Json.fromString("started"))
    case WorkerEventPayload.ProcessExited(exitCode) =>
      Json.obj(
        "kind" -> Json.fromString("process-exited"),
        "exitCode" -> Json.fromInt(exitCode)
      )
    case WorkerEventPayload.ResultPublished(digest) =>
      Json.obj(
        "kind" -> Json.fromString("result-published"),
        "envelopeDigest" -> Json.fromString(digest.value)
      )
    case WorkerEventPayload.Failed(code, message) =>
      Json.obj(
        "kind" -> Json.fromString("failed"),
        "code" -> Json.fromString(code),
        "message" -> Json.fromString(message)
      )
    case WorkerEventPayload.Progress(progress) =>
      Json.obj(
        "kind" -> Json.fromString("progress"),
        "message" -> Json.fromString(progress.message),
        "completed" -> progress.completed.fold(Json.Null)(Json.fromLong),
        "total" -> progress.total.fold(Json.Null)(Json.fromLong),
        "fields" -> Json.obj(progress.fields.toVector.sortBy(_._1).map { case (key, item) =>
          key -> Json.fromString(item)
        }*)
      )

  private def decodeEventPayload(json: Json): Either[StructuredCodecFailure, WorkerEventPayload] =
    for
      cursor <- objectCursor(json, "worker event payload")
      kind <- field[String](cursor, "kind")
      payload <- kind match
        case "started"        => Right(WorkerEventPayload.Started)
        case "process-exited" =>
          field[Int](cursor, "exitCode").map(WorkerEventPayload.ProcessExited.apply)
        case "result-published" =>
          identifier(cursor, "envelopeDigest", ContentDigest.from)
            .map(WorkerEventPayload.ResultPublished.apply)
        case "failed" =>
          (field[String](cursor, "code"), field[String](cursor, "message"))
            .mapN(WorkerEventPayload.Failed.apply)
        case "progress" =>
          for
            message <- field[String](cursor, "message")
            completed <- optionalField[Long](cursor, "completed")
            total <- optionalField[Long](cursor, "total")
            fields <- field[Map[String, String]](cursor, "fields")
          yield WorkerEventPayload.Progress(ProgressEvent(message, completed, total, fields))
        case other => Left(invalid(s"unknown worker event kind: $other"))
    yield payload
