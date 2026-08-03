package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.circe.Decoder
import io.circe.DecodingFailure
import io.circe.Encoder
import io.circe.HCursor
import io.circe.Json
import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto.deriveEncoder
import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*

import scodec.bits.ByteVector

import java.time.Instant
import java.time.LocalDateTime
import java.util.Base64
import scala.util.Try

object AgentDomainJson:
  private val RemoteTaskWireVersion = 1
  private val RemoteBatchWireVersion = 1

  /** Encode a launch specification for the opaque submission protocol.
    *
    * The field set is deliberately unchanged from when this encoded a `JobRequest`: it was already
    * launch-shaped, with the payload destructured into `source`/`arguments`. Keeping it
    * byte-identical means existing journals and the attempt identities derived from their digests
    * remain valid.
    *
    * A contract this protocol cannot carry is REFUSED rather than rewritten. The previous SSH
    * lowering silently replaced any contract with `ExitOnly`, so a caller asking for a structured
    * result got a successful submission that had quietly discarded what they asked for.
    */
  def encodeSubmitRequest(value: LaunchSpec): Either[String, Json] =
    value.resultContract.mode match
      case ResultMode.ExitOnly =>
        encodeEnvironment(value.environment).map { environment =>
          val fields = Vector(
            "submissionKey" -> Json.fromString(value.submissionKey.value),
            "name" -> Json.fromString(value.name.value),
            "source" -> value.source.asJson,
            "arguments" -> value.arguments.asJson,
            "resources" -> value.resources.asJson,
            "environment" -> environment,
            "resultContract" -> Json.fromString("exit-only")
          ) ++ value.array.toVector.map(array => "array" -> encodeArray(array)) ++
            Option
              .when(value.retrySafety != RetrySafety.Unknown)(
                "retrySafety" -> Json.fromString(encodeRetrySafety(value.retrySafety))
              )
              .toVector
          Json.obj(fields*)
        }
      case ResultMode.DeclaredOutputs | ResultMode.Structured =>
        Left("the opaque submission protocol carries only exit-only result contracts")

  def decodeSubmitRequest(json: Json): Either[String, LaunchSpec] =
    for
      cursor <- objectCursor(json, "submit request")
      submissionKeyText <- field[String](cursor, "submissionKey")
      submissionKey <- SubmissionKey.from(submissionKeyText).left.map(_.reason)
      nameText <- field[String](cursor, "name")
      name <- JobName.from(nameText).left.map(_.reason)
      source <- field[ScriptSource](cursor, "source")
      arguments <- field[Vector[String]](cursor, "arguments")
      resources <- field[ResourceRequest](cursor, "resources")
      environment <- decodeEnvironment(cursor)
      array <- optionalArray(cursor)
      retrySafety <- optionalRetrySafety(cursor)
      contract <- field[String](cursor, "resultContract")
      _ <- Either.cond(contract == "exit-only", (), "unsupported result contract")
    yield LaunchSpec(
      submissionKey,
      name,
      source,
      arguments,
      ResultContract.ExitOnly.descriptor,
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

  def encodeRemoteTaskRequest(value: RemoteRegisteredTaskRequest): Either[String, Json] =
    for environment <- encodeEnvironment(value.environment)
    yield Json.obj(
      "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
      "submissionKey" -> Json.fromString(value.submissionKey.value),
      "name" -> Json.fromString(value.name.value),
      "operation" -> StructuredJson.encodeRegisteredOperation(value.operation),
      "inputBase64" -> Json.fromString(Base64.getEncoder.encodeToString(value.inputBytes.toArray)),
      "resources" -> encodeRemoteResources(value.resources),
      "environment" -> environment,
      "maximumResultBytes" -> Json.fromInt(value.maximumResultBytes.value),
      "declaredOutputs" -> Json.arr(
        value.declaredOutputs.map(path => Json.fromString(path.value))*
      ),
      "retrySafety" -> Json.fromString(encodeRetrySafety(value.retrySafety))
    )

  def decodeRemoteTaskRequest(json: Json): Either[String, RemoteRegisteredTaskRequest] =
    for
      cursor <- objectCursor(json, "remote registered-task request")
      _ <- requireRemoteTaskWireVersion(cursor)
      submissionKeyText <- field[String](cursor, "submissionKey")
      submissionKey <- SubmissionKey.from(submissionKeyText).left.map(_.reason)
      nameText <- field[String](cursor, "name")
      name <- JobName.from(nameText).left.map(_.reason)
      operationJson <- cursor.downField("operation").focus.toRight("missing operation")
      operation <- StructuredJson.decodeRegisteredOperation(operationJson).left.map(_.toString)
      inputBase64 <- field[String](cursor, "inputBase64")
      inputBytes <- decodeBase64Bounded(
        inputBase64,
        ByteLimit.maximumCommandCapture,
        "inputBase64"
      )
      resourcesJson <- cursor.downField("resources").focus.toRight("missing resources")
      resources <- decodeRemoteResources(resourcesJson)
      environment <- decodeEnvironment(cursor)
      maximumResultRaw <- field[Int](cursor, "maximumResultBytes")
      maximumResult <- ByteLimit.from(maximumResultRaw).left.map(_.reason)
      outputTexts <- field[Vector[String]](cursor, "declaredOutputs")
      _ <- Either.cond(outputTexts.size <= 1024, (), "declaredOutputs exceeds 1024 entries")
      outputs <- outputTexts.traverse(raw => RelativeOutputPath.from(raw).left.map(_.reason))
      _ <- Either.cond(outputs.distinct.size == outputs.size, (), "declaredOutputs has duplicates")
      retryText <- field[String](cursor, "retrySafety")
      retrySafety <- decodeRetrySafety(retryText)
    yield RemoteRegisteredTaskRequest(
      submissionKey,
      name,
      operation,
      inputBytes,
      resources,
      environment,
      maximumResult,
      outputs,
      retrySafety
    )

  def encodeRemoteSubmission(value: RemoteRegisteredSubmission): Either[String, Json] =
    DurableResultHandleCodec
      .encode(value.resultHandle, RemoteTaskWireLimits.MaximumHandleBytes)
      .left
      .map(_.toString)
      .map { handleBytes =>
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
          "resultRef" -> encodeRemoteResultRef(value.resultRef),
          "resultHandleBase64" -> Json.fromString(
            Base64.getEncoder.encodeToString(handleBytes.toArray)
          ),
          "submission" -> encodeRemoteSubmissionAttempt(value.submission)
        )
      }

  def decodeRemoteSubmission(json: Json): Either[String, RemoteRegisteredSubmission] =
    for
      cursor <- objectCursor(json, "remote registered-task response")
      _ <- requireRemoteTaskWireVersion(cursor)
      refJson <- cursor.downField("resultRef").focus.toRight("missing resultRef")
      ref <- decodeRemoteResultRef(refJson)
      handleBase64 <- field[String](cursor, "resultHandleBase64")
      handleBytes <- decodeBase64Bounded(
        handleBase64,
        RemoteTaskWireLimits.MaximumHandleBytes,
        "resultHandleBase64"
      )
      handle <- DurableResultHandleCodec
        .decode(handleBytes, RemoteTaskWireLimits.MaximumHandleBytes)
        .left
        .map(_.toString)
      submissionJson <- cursor.downField("submission").focus.toRight("missing submission")
      submission <- decodeRemoteSubmissionAttempt(submissionJson)
      _ <- Either.cond(
        handle.attemptId == ref.attemptId && handle.attemptEpoch == ref.attemptEpoch,
        (),
        "result reference and durable handle identify different attempts"
      )
    yield RemoteRegisteredSubmission(ref, handle, submission)

  def encodeRemoteBatchRequest(value: RemoteRegisteredBatchRequest): Either[String, Json] =
    for environment <- encodeEnvironment(value.environment)
    yield Json.obj(
      "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
      "submissionKey" -> Json.fromString(value.submissionKey.value),
      "name" -> Json.fromString(value.name.value),
      "operation" -> StructuredJson.encodeRegisteredOperation(value.operation),
      "topology" -> encodeBatchTopology(value.topology),
      "elements" -> Json.arr(
        value.elements.toVector.map { element =>
          Json.obj(
            "index" -> Json.fromInt(element.index.value),
            "submissionKey" -> Json.fromString(element.submissionKey.value),
            "inputBase64" -> Json.fromString(
              Base64.getEncoder.encodeToString(element.inputBytes.toArray)
            )
          )
        }*
      ),
      "environment" -> environment,
      "maximumResultBytes" -> Json.fromInt(value.maximumResultBytes.value),
      "declaredOutputs" -> Json.arr(
        value.declaredOutputs.map(path => Json.fromString(path.value))*
      ),
      "retrySafety" -> Json.fromString(encodeRetrySafety(value.retrySafety))
    )

  def decodeRemoteBatchRequest(json: Json): Either[String, RemoteRegisteredBatchRequest] =
    for
      cursor <- objectCursor(json, "remote registered batch request")
      version <- field[Int](cursor, "wireVersion")
      _ <- Either.cond(
        version == RemoteBatchWireVersion,
        (),
        s"unsupported remote-batch wire version: $version"
      )
      submissionKeyText <- field[String](cursor, "submissionKey")
      submissionKey <- SubmissionKey.from(submissionKeyText).left.map(_.reason)
      nameText <- field[String](cursor, "name")
      name <- JobName.from(nameText).left.map(_.reason)
      operationJson <- cursor.downField("operation").focus.toRight("missing operation")
      operation <- StructuredJson.decodeRegisteredOperation(operationJson).left.map(_.toString)
      topologyJson <- cursor.downField("topology").focus.toRight("missing topology")
      topology <- decodeBatchTopology(topologyJson)
      elementJson <- field[Vector[Json]](cursor, "elements")
      _ <- Either.cond(elementJson.nonEmpty, (), "batch elements must not be empty")
      _ <- Either.cond(
        elementJson.sizeIs <= RemoteTaskWireLimits.MaximumBatchEntries,
        (),
        s"batch elements exceed ${RemoteTaskWireLimits.MaximumBatchEntries} entries"
      )
      elements <- elementJson.traverse(decodeRemoteBatchElement)
      nonEmptyElements <- NonEmptyVector
        .fromVector(elements)
        .toRight("batch elements must not be empty")
      _ <- validateBatchElements(topology, nonEmptyElements)
      environment <- decodeEnvironment(cursor)
      maximumResultRaw <- field[Int](cursor, "maximumResultBytes")
      maximumResult <- ByteLimit.from(maximumResultRaw).left.map(_.reason)
      outputTexts <- field[Vector[String]](cursor, "declaredOutputs")
      _ <- Either.cond(outputTexts.size <= 1024, (), "declaredOutputs exceeds 1024 entries")
      outputs <- outputTexts.traverse(raw => RelativeOutputPath.from(raw).left.map(_.reason))
      _ <- Either.cond(outputs.distinct.size == outputs.size, (), "declaredOutputs has duplicates")
      retryText <- field[String](cursor, "retrySafety")
      retrySafety <- decodeRetrySafety(retryText)
    yield RemoteRegisteredBatchRequest(
      submissionKey,
      name,
      operation,
      topology,
      nonEmptyElements,
      environment,
      maximumResult,
      outputs,
      retrySafety
    )

  def encodeRemoteBatchSubmission(
      value: RemoteRegisteredBatchSubmission
  ): Either[String, Json] =
    value.elements.toVector
      .traverse { element =>
        DurableResultHandleCodec
          .encode(element.resultHandle, RemoteTaskWireLimits.MaximumHandleBytes)
          .left
          .map(_.toString)
          .map { handleBytes =>
            Json.obj(
              "index" -> Json.fromInt(element.index.value),
              "resultRef" -> encodeRemoteResultRef(element.resultRef),
              "resultHandleBase64" -> Json.fromString(
                Base64.getEncoder.encodeToString(handleBytes.toArray)
              ),
              "stdout" -> element.stdout.asJson,
              "stderr" -> element.stderr.asJson
            )
          }
      }
      .map { elements =>
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
          "topology" -> encodeBatchTopology(value.topology),
          "elements" -> Json.arr(elements*),
          "submission" -> encodeRemoteSubmissionAttempt(value.submission)
        )
      }

  def decodeRemoteBatchSubmission(json: Json): Either[String, RemoteRegisteredBatchSubmission] =
    for
      cursor <- objectCursor(json, "remote registered batch response")
      version <- field[Int](cursor, "wireVersion")
      _ <- Either.cond(
        version == RemoteBatchWireVersion,
        (),
        s"unsupported remote-batch wire version: $version"
      )
      topologyJson <- cursor.downField("topology").focus.toRight("missing topology")
      topology <- decodeBatchTopology(topologyJson)
      elementJson <- field[Vector[Json]](cursor, "elements")
      elements <- elementJson.traverse(decodeRemoteBatchElementSubmission)
      nonEmptyElements <- NonEmptyVector
        .fromVector(elements)
        .toRight("batch response elements must not be empty")
      indices = nonEmptyElements.toVector.map(_.index)
      _ <- Either.cond(
        indices.distinct.size == indices.size,
        (),
        "batch response has duplicate element indices"
      )
      _ <- Either.cond(
        indices.toSet == topology.elementIndices.toVector.toSet,
        (),
        "batch response elements do not match the topology"
      )
      submissionJson <- cursor.downField("submission").focus.toRight("missing submission")
      submission <- decodeRemoteSubmissionAttempt(submissionJson)
    yield RemoteRegisteredBatchSubmission(topology, nonEmptyElements, submission)

  def encodeRemoteScriptBatchRequest(
      value: RemoteScriptBatchRequest
  ): Either[String, Json] =
    for
      environment <- encodeEnvironment(value.environment)
      program <- encodeScriptProgram(value.program)
    yield Json.obj(
      "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
      "submissionKey" -> Json.fromString(value.submissionKey.value),
      "name" -> Json.fromString(value.name.value),
      "program" -> program,
      "topology" -> encodeBatchTopology(value.topology),
      "elements" -> Json.arr(
        value.elements.toVector.map { element =>
          Json.obj(
            "index" -> Json.fromInt(element.index.value),
            "submissionKey" -> Json.fromString(element.submissionKey.value),
            "arguments" -> Json.arr(
              element.arguments.map(argument => Json.fromString(argument.value))*
            )
          )
        }*
      ),
      "environment" -> environment,
      "retrySafety" -> Json.fromString(encodeRetrySafety(value.retrySafety))
    )

  def decodeRemoteScriptBatchRequest(json: Json): Either[String, RemoteScriptBatchRequest] =
    for
      cursor <- objectCursor(json, "remote script batch request")
      version <- field[Int](cursor, "wireVersion")
      _ <- Either.cond(
        version == RemoteBatchWireVersion,
        (),
        s"unsupported remote-batch wire version: $version"
      )
      submissionKeyText <- field[String](cursor, "submissionKey")
      submissionKey <- SubmissionKey.from(submissionKeyText).left.map(_.reason)
      nameText <- field[String](cursor, "name")
      name <- JobName.from(nameText).left.map(_.reason)
      programJson <- cursor.downField("program").focus.toRight("missing program")
      program <- decodeScriptProgram(programJson)
      topologyJson <- cursor.downField("topology").focus.toRight("missing topology")
      topology <- decodeBatchTopology(topologyJson)
      elementJson <- field[Vector[Json]](cursor, "elements")
      _ <- Either.cond(
        elementJson.nonEmpty && elementJson.sizeIs <= RemoteTaskWireLimits.MaximumBatchEntries,
        (),
        "script batch elements must contain between 1 and " +
          s"${RemoteTaskWireLimits.MaximumBatchEntries} entries"
      )
      elements <- elementJson.traverse(decodeRemoteScriptBatchElement)
      nonEmptyElements <- NonEmptyVector
        .fromVector(elements)
        .toRight("script batch elements must not be empty")
      _ <- validateScriptBatchElements(topology, nonEmptyElements)
      environment <- decodeEnvironment(cursor)
      retryText <- field[String](cursor, "retrySafety")
      retrySafety <- decodeRetrySafety(retryText)
    yield RemoteScriptBatchRequest(
      submissionKey,
      name,
      program,
      topology,
      nonEmptyElements,
      environment,
      retrySafety
    )

  def encodeRemoteScriptBatchSubmission(value: RemoteScriptBatchSubmission): Json =
    Json.obj(
      "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
      "topology" -> encodeBatchTopology(value.topology),
      "elements" -> Json.arr(
        value.elements.toVector.map { element =>
          Json.obj(
            "index" -> Json.fromInt(element.index.value),
            "exitRef" -> encodeRemoteScriptExitRef(element.exitRef),
            "stdout" -> element.stdout.asJson,
            "stderr" -> element.stderr.asJson
          )
        }*
      ),
      "submission" -> encodeRemoteSubmissionAttempt(value.submission)
    )

  def decodeRemoteScriptBatchSubmission(
      json: Json
  ): Either[String, RemoteScriptBatchSubmission] =
    for
      cursor <- objectCursor(json, "remote script batch response")
      version <- field[Int](cursor, "wireVersion")
      _ <- Either.cond(
        version == RemoteBatchWireVersion,
        (),
        s"unsupported remote-batch wire version: $version"
      )
      topologyJson <- cursor.downField("topology").focus.toRight("missing topology")
      topology <- decodeBatchTopology(topologyJson)
      elementJson <- field[Vector[Json]](cursor, "elements")
      elements <- elementJson.traverse(decodeRemoteScriptBatchElementSubmission)
      nonEmptyElements <- NonEmptyVector
        .fromVector(elements)
        .toRight("script batch response elements must not be empty")
      indices = nonEmptyElements.toVector.map(_.index)
      _ <- Either.cond(
        indices.distinct.size == indices.size &&
          indices.toSet == topology.elementIndices.toVector.toSet,
        (),
        "script batch response elements do not match the topology"
      )
      submissionJson <- cursor.downField("submission").focus.toRight("missing submission")
      submission <- decodeRemoteSubmissionAttempt(submissionJson)
    yield RemoteScriptBatchSubmission(topology, nonEmptyElements, submission)

  def encodeRemoteScriptExitReadRequest(ref: RemoteScriptExitRef): Json =
    Json.obj(
      "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
      "exitRef" -> encodeRemoteScriptExitRef(ref)
    )

  def decodeRemoteScriptExitReadRequest(json: Json): Either[String, RemoteScriptExitRef] =
    for
      cursor <- objectCursor(json, "remote script exit-read request")
      version <- field[Int](cursor, "wireVersion")
      _ <- Either.cond(
        version == RemoteBatchWireVersion,
        (),
        s"unsupported remote-batch wire version: $version"
      )
      refJson <- cursor.downField("exitRef").focus.toRight("missing exitRef")
      ref <- decodeRemoteScriptExitRef(refJson)
    yield ref

  def encodeRemoteScriptExitRead(value: RemoteScriptExitRead): Json =
    value match
      case RemoteScriptExitRead.Pending(observedAt) =>
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
          "kind" -> Json.fromString("pending"),
          "observedAt" -> Json.fromString(observedAt.toString)
        )
      case RemoteScriptExitRead.Exited(exitCode, observedAt) =>
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
          "kind" -> Json.fromString("exited"),
          "exitCode" -> Json.fromInt(exitCode),
          "observedAt" -> Json.fromString(observedAt.toString)
        )
      case RemoteScriptExitRead.Failed(diagnostics, evidence, observedAt) =>
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteBatchWireVersion),
          "kind" -> Json.fromString("failed"),
          "diagnostics" -> encodeRemoteDiagnostics(diagnostics),
          "evidence" -> encodeRemoteEvidenceBundle(evidence),
          "observedAt" -> Json.fromString(observedAt.toString)
        )

  def decodeRemoteScriptExitRead(json: Json): Either[String, RemoteScriptExitRead] =
    for
      cursor <- objectCursor(json, "remote script exit-read response")
      version <- field[Int](cursor, "wireVersion")
      _ <- Either.cond(
        version == RemoteBatchWireVersion,
        (),
        s"unsupported remote-batch wire version: $version"
      )
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "pending" =>
          field[Instant](cursor, "observedAt").map(RemoteScriptExitRead.Pending.apply)
        case "exited" =>
          for
            exitCode <- field[Int](cursor, "exitCode")
            _ <- Either.cond(
              exitCode >= 0 && exitCode <= 255,
              (),
              "script exit code must be between 0 and 255"
            )
            observedAt <- field[Instant](cursor, "observedAt")
          yield RemoteScriptExitRead.Exited(exitCode, observedAt)
        case "failed" =>
          for
            diagnosticsJson <- cursor.downField("diagnostics").focus.toRight("missing diagnostics")
            diagnostics <- decodeRemoteDiagnostics(diagnosticsJson)
            evidenceJson <- cursor.downField("evidence").focus.toRight("missing evidence")
            evidence <- decodeRemoteEvidenceBundle(evidenceJson)
            observedAt <- field[Instant](cursor, "observedAt")
          yield RemoteScriptExitRead.Failed(diagnostics, evidence, observedAt)
        case other => Left(s"unknown remote script exit-read kind: $other")
    yield result

  def encodeRemoteResultReadRequest(ref: RemoteResultRef, maximumBytes: ByteLimit): Json =
    Json.obj(
      "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
      "resultRef" -> encodeRemoteResultRef(ref),
      "maximumBytes" -> Json.fromInt(maximumBytes.value)
    )

  /** Read several results in one exchange.
    *
    * ADR 0003 gives every request its own SSH process, so N pending elements cost N processes per
    * tick. The agent-side work is unchanged — it reads each ref exactly as before — and the saving
    * is purely transport: one process instead of N.
    *
    * `maximumBytes` bounds EACH result, so a caller must size the group against the negotiated
    * frame rather than sending an unbounded list.
    */
  def encodeRemoteResultReadsRequest(
      refs: NonEmptyVector[RemoteResultRef],
      maximumBytes: ByteLimit
  ): Json =
    Json.obj(
      "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
      "resultRefs" -> Json.fromValues(refs.toVector.map(encodeRemoteResultRef)),
      "maximumBytes" -> Json.fromInt(maximumBytes.value)
    )

  def decodeRemoteResultReadsRequest(
      json: Json
  ): Either[String, (NonEmptyVector[RemoteResultRef], ByteLimit)] =
    for
      cursor <- objectCursor(json, "remote result-reads request")
      _ <- requireRemoteTaskWireVersion(cursor)
      refsJson <- cursor
        .downField("resultRefs")
        .focus
        .flatMap(_.asArray)
        .toRight("missing resultRefs")
      // Each reference is individually bounded, which says nothing about how many arrive.
      _ <- Either.cond(
        refsJson.sizeIs <= RemoteTaskWireLimits.MaximumBatchEntries,
        (),
        s"resultRefs exceed ${RemoteTaskWireLimits.MaximumBatchEntries} entries"
      )
      refs <- refsJson.toVector.traverse(decodeRemoteResultRef)
      nonEmpty <- NonEmptyVector.fromVector(refs).toRight("resultRefs must not be empty")
      maximumRaw <- field[Int](cursor, "maximumBytes")
      maximum <- ByteLimit.from(maximumRaw).left.map(_.reason)
    yield nonEmpty -> maximum

  def encodeRemoteResultReads(
      values: NonEmptyVector[RemoteResultRead]
  ): Either[String, Json] =
    values.toVector
      .traverse(encodeRemoteResultRead)
      .map(entries =>
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
          "results" -> Json.fromValues(entries)
        )
      )

  def decodeRemoteResultReads(
      json: Json,
      maximumEnvelopeBytes: ByteLimit
  ): Either[String, NonEmptyVector[RemoteResultRead]] =
    for
      cursor <- objectCursor(json, "remote result-reads response")
      _ <- requireRemoteTaskWireVersion(cursor)
      entries <- cursor.downField("results").focus.flatMap(_.asArray).toRight("missing results")
      reads <- entries.toVector.traverse(decodeRemoteResultRead(_, maximumEnvelopeBytes))
      nonEmpty <- NonEmptyVector.fromVector(reads).toRight("results must not be empty")
    yield nonEmpty

  def decodeRemoteResultReadRequest(
      json: Json
  ): Either[String, (RemoteResultRef, ByteLimit)] =
    for
      cursor <- objectCursor(json, "remote result-read request")
      _ <- requireRemoteTaskWireVersion(cursor)
      refJson <- cursor.downField("resultRef").focus.toRight("missing resultRef")
      ref <- decodeRemoteResultRef(refJson)
      maximumRaw <- field[Int](cursor, "maximumBytes")
      maximum <- ByteLimit.from(maximumRaw).left.map(_.reason)
    yield ref -> maximum

  def encodeRemoteResultRead(value: RemoteResultRead): Either[String, Json] = value match
    case RemoteResultRead.Pending(observedAt) =>
      Right(
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
          "kind" -> Json.fromString("pending"),
          "observedAt" -> Json.fromString(observedAt.toString)
        )
      )
    case RemoteResultRead.Available(storedHandle, envelopeBytes, observedAt) =>
      DurableResultHandleCodec
        .encode(storedHandle, RemoteTaskWireLimits.MaximumHandleBytes)
        .left
        .map(_.toString)
        .map { handleBytes =>
          Json.obj(
            "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
            "kind" -> Json.fromString("available"),
            "storedHandleBase64" -> Json.fromString(
              Base64.getEncoder.encodeToString(handleBytes.toArray)
            ),
            "envelopeBase64" -> Json.fromString(
              Base64.getEncoder.encodeToString(envelopeBytes.toArray)
            ),
            "observedAt" -> Json.fromString(observedAt.toString)
          )
        }
    case RemoteResultRead.Failed(diagnostics, evidence, observedAt) =>
      Right(
        Json.obj(
          "wireVersion" -> Json.fromInt(RemoteTaskWireVersion),
          "kind" -> Json.fromString("failed"),
          "diagnostics" -> encodeRemoteDiagnostics(diagnostics),
          "evidence" -> encodeRemoteEvidenceBundle(evidence),
          "observedAt" -> Json.fromString(observedAt.toString)
        )
      )

  def decodeRemoteResultRead(
      json: Json,
      maximumEnvelopeBytes: ByteLimit
  ): Either[String, RemoteResultRead] =
    for
      cursor <- objectCursor(json, "remote result-read response")
      _ <- requireRemoteTaskWireVersion(cursor)
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "pending" =>
          field[Instant](cursor, "observedAt").map(RemoteResultRead.Pending.apply)
        case "available" =>
          for
            handleBase64 <- field[String](cursor, "storedHandleBase64")
            handleBytes <- decodeBase64Bounded(
              handleBase64,
              RemoteTaskWireLimits.MaximumHandleBytes,
              "storedHandleBase64"
            )
            handle <- DurableResultHandleCodec
              .decode(handleBytes, RemoteTaskWireLimits.MaximumHandleBytes)
              .left
              .map(_.toString)
            envelopeBase64 <- field[String](cursor, "envelopeBase64")
            envelopeBytes <- decodeBase64Bounded(
              envelopeBase64,
              maximumEnvelopeBytes,
              "envelopeBase64"
            )
            observedAt <- field[Instant](cursor, "observedAt")
          yield RemoteResultRead.Available(handle, envelopeBytes, observedAt)
        case "failed" =>
          for
            diagnosticsJson <- cursor.downField("diagnostics").focus.toRight("missing diagnostics")
            diagnostics <- decodeRemoteDiagnostics(diagnosticsJson)
            evidenceJson <- cursor.downField("evidence").focus.toRight("missing evidence")
            evidence <- decodeRemoteEvidenceBundle(evidenceJson)
            observedAt <- field[Instant](cursor, "observedAt")
          yield RemoteResultRead.Failed(diagnostics, evidence, observedAt)
        case other => Left(s"unknown remote result-read kind: $other")
    yield result

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
      case None                        => Right(None)
      case Some(value) if value.isNull => Right(None)
      case Some(value)                 =>
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

  private def encodeBatchTopology(value: BatchTopology): Json =
    Json.obj(
      "execution" -> encodeBatchExecution(value.execution),
      "resources" -> encodeRemoteResources(value.resources),
      "array" -> value.array.fold(Json.Null)(encodeArray),
      "shards" -> Json.arr(
        value.shards.toVector.map { shard =>
          Json.obj(
            "index" -> Json.fromInt(shard.index.value),
            "elements" -> Json.arr(
              shard.elements.toVector.map(index => Json.fromInt(index.value))*
            )
          )
        }*
      )
    )

  private def decodeBatchTopology(json: Json): Either[String, BatchTopology] =
    for
      cursor <- objectCursor(json, "batch topology")
      executionJson <- cursor.downField("execution").focus.toRight("missing execution")
      execution <- decodeBatchExecution(executionJson)
      resourcesJson <- cursor.downField("resources").focus.toRight("missing resources")
      resources <- decodeRemoteResources(resourcesJson)
      array <- optionalArray(cursor)
      shardJson <- field[Vector[Json]](cursor, "shards")
      shards <- shardJson.traverse(decodeBatchTopologyShard)
      nonEmptyShards <- NonEmptyVector.fromVector(shards).toRight("batch shards must not be empty")
      shardIndices = nonEmptyShards.toVector.map(_.index)
      elementIndices = nonEmptyShards.toVector.flatMap(_.elements.toVector)
      _ <- Either.cond(
        shardIndices.distinct.size == shardIndices.size,
        (),
        "batch topology has duplicate shard indices"
      )
      _ <- Either.cond(
        elementIndices.distinct.size == elementIndices.size,
        (),
        "batch topology assigns an element more than once"
      )
      _ <- validateBatchTopologyShape(execution, array, nonEmptyShards)
    yield BatchTopology(execution, resources, array, nonEmptyShards)

  private def encodeBatchExecution(value: BatchExecutionPlan): Json = value match
    case BatchExecutionPlan.Independent(maximumRunning) =>
      Json.obj(
        "kind" -> Json.fromString("independent"),
        "maximumRunning" -> maximumRunning.map(_.toInt).asJson
      )
    case BatchExecutionPlan.Sharded(
          shards,
          slotsPerShard,
          assignment,
          maximumRunningShards
        ) =>
      Json.obj(
        "kind" -> Json.fromString("sharded"),
        "shards" -> Json.fromInt(shards.toInt),
        "slotsPerShard" -> Json.fromInt(slotsPerShard.toInt),
        "assignment" -> encodeShardAssignment(assignment),
        "maximumRunningShards" -> maximumRunningShards.map(_.toInt).asJson
      )
    case BatchExecutionPlan.Gang(nodes, tasksPerNode) =>
      Json.obj(
        "kind" -> Json.fromString("gang"),
        "nodes" -> Json.fromInt(nodes.toInt),
        "tasksPerNode" -> Json.fromInt(tasksPerNode.toInt)
      )

  private def decodeBatchExecution(json: Json): Either[String, BatchExecutionPlan] =
    for
      cursor <- objectCursor(json, "batch execution")
      kind <- field[String](cursor, "kind")
      execution <- kind match
        case "independent" =>
          optionalPositive(cursor, "maximumRunning").map(BatchExecutionPlan.Independent.apply)
        case "sharded" =>
          for
            shardsRaw <- field[Int](cursor, "shards")
            shards <- PositiveInt.from("shards", shardsRaw).left.map(_.reason)
            slotsRaw <- field[Int](cursor, "slotsPerShard")
            slots <- PositiveInt.from("slotsPerShard", slotsRaw).left.map(_.reason)
            assignmentJson <- cursor.downField("assignment").focus.toRight("missing assignment")
            assignment <- decodeShardAssignment(assignmentJson)
            maximum <- optionalPositive(cursor, "maximumRunningShards")
          yield BatchExecutionPlan.Sharded(shards, slots, assignment, maximum)
        case "gang" =>
          for
            nodesRaw <- field[Int](cursor, "nodes")
            nodes <- PositiveInt.from("nodes", nodesRaw).left.map(_.reason)
            tasksRaw <- field[Int](cursor, "tasksPerNode")
            tasks <- PositiveInt.from("tasksPerNode", tasksRaw).left.map(_.reason)
          yield BatchExecutionPlan.Gang(nodes, tasks)
        case other => Left(s"unknown batch execution kind: $other")
    yield execution

  private def encodeShardAssignment(value: ShardAssignment): Json = value match
    case ShardAssignment.BalancedContiguous =>
      Json.obj("kind" -> Json.fromString("balanced-contiguous"))
    case ShardAssignment.RoundRobin =>
      Json.obj("kind" -> Json.fromString("round-robin"))
    case ShardAssignment.Exactly(rowsPerShard) =>
      Json.obj(
        "kind" -> Json.fromString("exactly"),
        "rowsPerShard" -> Json.fromInt(rowsPerShard.toInt)
      )

  private def decodeShardAssignment(json: Json): Either[String, ShardAssignment] =
    for
      cursor <- objectCursor(json, "shard assignment")
      kind <- field[String](cursor, "kind")
      assignment <- kind match
        case "balanced-contiguous" => Right(ShardAssignment.BalancedContiguous)
        case "round-robin"         => Right(ShardAssignment.RoundRobin)
        case "exactly"             =>
          for
            raw <- field[Int](cursor, "rowsPerShard")
            value <- PositiveInt.from("rowsPerShard", raw).left.map(_.reason)
          yield ShardAssignment.Exactly(value)
        case other => Left(s"unknown shard assignment kind: $other")
    yield assignment

  private def decodeBatchTopologyShard(json: Json): Either[String, BatchTopologyShard] =
    for
      cursor <- objectCursor(json, "batch topology shard")
      indexRaw <- field[Int](cursor, "index")
      index <- ArrayIndex.from(indexRaw).left.map(_.reason)
      elementRaw <- field[Vector[Int]](cursor, "elements")
      elements <- elementRaw.traverse(raw => ArrayIndex.from(raw).left.map(_.reason))
      nonEmptyElements <- NonEmptyVector
        .fromVector(elements)
        .toRight("batch topology shard elements must not be empty")
    yield BatchTopologyShard(index, nonEmptyElements)

  private def decodeRemoteBatchElement(json: Json): Either[String, RemoteRegisteredBatchElement] =
    for
      cursor <- objectCursor(json, "remote batch element")
      indexRaw <- field[Int](cursor, "index")
      index <- ArrayIndex.from(indexRaw).left.map(_.reason)
      keyText <- field[String](cursor, "submissionKey")
      key <- SubmissionKey.from(keyText).left.map(_.reason)
      inputBase64 <- field[String](cursor, "inputBase64")
      input <- decodeBase64Bounded(
        inputBase64,
        ByteLimit.maximumCommandCapture,
        "inputBase64"
      )
    yield RemoteRegisteredBatchElement(index, key, input)

  private def decodeRemoteBatchElementSubmission(
      json: Json
  ): Either[String, RemoteRegisteredBatchElementSubmission] =
    for
      cursor <- objectCursor(json, "remote batch element response")
      indexRaw <- field[Int](cursor, "index")
      index <- ArrayIndex.from(indexRaw).left.map(_.reason)
      refJson <- cursor.downField("resultRef").focus.toRight("missing resultRef")
      ref <- decodeRemoteResultRef(refJson)
      handleBase64 <- field[String](cursor, "resultHandleBase64")
      handleBytes <- decodeBase64Bounded(
        handleBase64,
        RemoteTaskWireLimits.MaximumHandleBytes,
        "resultHandleBase64"
      )
      handle <- DurableResultHandleCodec
        .decode(handleBytes, RemoteTaskWireLimits.MaximumHandleBytes)
        .left
        .map(_.toString)
      stdout <- field[LogRef](cursor, "stdout")
      stderr <- field[LogRef](cursor, "stderr")
      _ <- Either.cond(
        handle.attemptId == ref.attemptId && handle.attemptEpoch == ref.attemptEpoch,
        (),
        "batch element reference and handle identify different attempts"
      )
      _ <- Either.cond(
        stdout.attemptId == ref.attemptId &&
          stdout.epoch == ref.attemptEpoch &&
          stdout.stream == LogStream.Stdout,
        (),
        "batch element stdout reference does not match its result attempt"
      )
      _ <- Either.cond(
        stderr.attemptId == ref.attemptId &&
          stderr.epoch == ref.attemptEpoch &&
          stderr.stream == LogStream.Stderr,
        (),
        "batch element stderr reference does not match its result attempt"
      )
    yield RemoteRegisteredBatchElementSubmission(index, ref, handle, stdout, stderr)

  private def encodeScriptProgram(program: ScriptProgram): Either[String, Json] =
    val source = program.source match
      case ScriptSource.Inline(name, bytes) =>
        Right(
          Json.obj(
            "kind" -> Json.fromString("inline"),
            "name" -> Json.fromString(name),
            "bytesBase64" -> Json.fromString(
              Base64.getEncoder.encodeToString(bytes.toArray)
            )
          )
        )
      case ScriptSource.ExistingRemote(path) =>
        Right(
          Json.obj(
            "kind" -> Json.fromString("existing-remote"),
            "path" -> Json.fromString(path)
          )
        )
      case ScriptSource.StagedLocal(_) =>
        Left("staged-local script sources must be materialized before remote transport")
    source.map { encodedSource =>
      val invocation = program.invocation match
        case ScriptInvocation.Direct =>
          Json.obj("kind" -> Json.fromString("direct"))
        case ScriptInvocation.Via(prefix) =>
          Json.obj(
            "kind" -> Json.fromString("via"),
            "prefix" -> Json.arr(
              prefix.arguments.toVector.map(argument => Json.fromString(argument.value))*
            )
          )
      Json.obj(
        "source" -> encodedSource,
        "invocation" -> invocation
      )
    }

  private def decodeScriptProgram(json: Json): Either[String, ScriptProgram] =
    for
      cursor <- objectCursor(json, "script program")
      sourceJson <- cursor.downField("source").focus.toRight("missing script source")
      sourceCursor <- objectCursor(sourceJson, "script source")
      sourceKind <- field[String](sourceCursor, "kind")
      source <- sourceKind match
        case "inline" =>
          for
            name <- field[String](sourceCursor, "name")
            encoded <- field[String](sourceCursor, "bytesBase64")
            bytes <- decodeBase64Bounded(
              encoded,
              ByteLimit.maximumCommandCapture,
              "bytesBase64"
            )
          yield ScriptSource.Inline(name, bytes)
        case "existing-remote" =>
          field[String](sourceCursor, "path").map(ScriptSource.ExistingRemote.apply)
        case other => Left(s"unknown script source kind: $other")
      invocationJson <- cursor.downField("invocation").focus.toRight("missing invocation")
      invocationCursor <- objectCursor(invocationJson, "script invocation")
      invocationKind <- field[String](invocationCursor, "kind")
      invocation <- invocationKind match
        case "direct" => Right(ScriptInvocation.Direct)
        case "via"    =>
          for
            raw <- field[Vector[String]](invocationCursor, "prefix")
            nonEmpty <- NonEmptyVector.fromVector(raw).toRight("command prefix must not be empty")
            prefix <- CommandPrefix
              .from(nonEmpty.head, nonEmpty.tail*)
              .left
              .map(_.toChain.toList.map(_.reason).mkString(", "))
          yield ScriptInvocation.Via(prefix)
        case other => Left(s"unknown script invocation kind: $other")
    yield ScriptProgram(source, invocation)

  private def decodeRemoteScriptBatchElement(
      json: Json
  ): Either[String, RemoteScriptBatchElement] =
    for
      cursor <- objectCursor(json, "remote script batch element")
      indexRaw <- field[Int](cursor, "index")
      index <- ArrayIndex.from(indexRaw).left.map(_.reason)
      keyText <- field[String](cursor, "submissionKey")
      key <- SubmissionKey.from(keyText).left.map(_.reason)
      rawArguments <- field[Vector[String]](cursor, "arguments")
      _ <- Either.cond(
        rawArguments.size <= 4096,
        (),
        "script batch element has more than 4096 arguments"
      )
      arguments <- rawArguments.traverse(Argument.from(_).left.map(_.reason))
    yield RemoteScriptBatchElement(index, key, arguments)

  private def decodeRemoteScriptBatchElementSubmission(
      json: Json
  ): Either[String, RemoteScriptBatchElementSubmission] =
    for
      cursor <- objectCursor(json, "remote script batch element response")
      indexRaw <- field[Int](cursor, "index")
      index <- ArrayIndex.from(indexRaw).left.map(_.reason)
      exitJson <- cursor.downField("exitRef").focus.toRight("missing exitRef")
      exitRef <- decodeRemoteScriptExitRef(exitJson)
      stdout <- field[LogRef](cursor, "stdout")
      stderr <- field[LogRef](cursor, "stderr")
      _ <- Either.cond(
        stdout.attemptId == exitRef.attemptId &&
          stdout.epoch == exitRef.attemptEpoch &&
          stdout.stream == LogStream.Stdout,
        (),
        "script batch stdout reference does not match its exit attempt"
      )
      _ <- Either.cond(
        stderr.attemptId == exitRef.attemptId &&
          stderr.epoch == exitRef.attemptEpoch &&
          stderr.stream == LogStream.Stderr,
        (),
        "script batch stderr reference does not match its exit attempt"
      )
    yield RemoteScriptBatchElementSubmission(index, exitRef, stdout, stderr)

  private def encodeRemoteScriptExitRef(ref: RemoteScriptExitRef): Json =
    Json.obj(
      "attemptId" -> Json.fromString(ref.attemptId.value),
      "attemptEpoch" -> Json.fromLong(ref.attemptEpoch.value)
    )

  private def decodeRemoteScriptExitRef(json: Json): Either[String, RemoteScriptExitRef] =
    for
      cursor <- objectCursor(json, "remote script exit reference")
      attemptText <- field[String](cursor, "attemptId")
      attempt <- AttemptId.from(attemptText).left.map(_.reason)
      epochRaw <- field[Long](cursor, "attemptEpoch")
      epoch <- AttemptEpoch.from(epochRaw).left.map(_.reason)
    yield RemoteScriptExitRef(attempt, epoch)

  private def validateScriptBatchElements(
      topology: BatchTopology,
      elements: NonEmptyVector[RemoteScriptBatchElement]
  ): Either[String, Unit] =
    val indices = elements.toVector.map(_.index)
    val keys = elements.toVector.map(_.submissionKey)
    Either.cond(
      indices.distinct.size == indices.size &&
        keys.distinct.size == keys.size &&
        indices.toSet == topology.elementIndices.toVector.toSet,
      (),
      "script batch elements must have unique indices and keys matching the topology"
    )

  private def validateBatchElements(
      topology: BatchTopology,
      elements: NonEmptyVector[RemoteRegisteredBatchElement]
  ): Either[String, Unit] =
    val indices = elements.toVector.map(_.index)
    val keys = elements.toVector.map(_.submissionKey)
    for
      _ <- Either.cond(
        indices.distinct.size == indices.size,
        (),
        "batch request has duplicate element indices"
      )
      _ <- Either.cond(
        keys.distinct.size == keys.size,
        (),
        "batch request has duplicate element submission keys"
      )
      _ <- Either.cond(
        indices.toSet == topology.elementIndices.toVector.toSet,
        (),
        "batch request elements do not match the topology"
      )
    yield ()

  private def validateBatchTopologyShape(
      execution: BatchExecutionPlan,
      array: Option[JobArrayRequest],
      shards: NonEmptyVector[BatchTopologyShard]
  ): Either[String, Unit] = execution match
    case BatchExecutionPlan.Independent(_) | BatchExecutionPlan.Sharded(_, _, _, _) =>
      array match
        case None          => Left("array-backed batch execution requires an array request")
        case Some(request) =>
          Either.cond(
            request.indices.toVector.toSet == shards.toVector.map(_.index).toSet,
            (),
            "batch array indices do not match shard indices"
          )
    case BatchExecutionPlan.Gang(_, _) =>
      Either.cond(
        array.isEmpty && shards.length == 1,
        (),
        "gang execution requires exactly one non-array shard"
      )

  private def optionalPositive(
      cursor: HCursor,
      fieldName: String
  ): Either[String, Option[PositiveInt]] =
    cursor
      .get[Option[Int]](fieldName)
      .left
      .map(_.message)
      .flatMap(
        _.traverse(raw => PositiveInt.from(fieldName, raw).left.map(_.reason))
      )

  private def optionalRetrySafety(cursor: HCursor): Either[String, RetrySafety] =
    cursor.downField("retrySafety").focus match
      case None                        => Right(RetrySafety.Unknown)
      case Some(value) if value.isNull => Right(RetrySafety.Unknown)
      case Some(value)                 =>
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

  private def encodeEnvironment(value: Map[EnvName, String]): Either[String, Json] =
    value.toVector
      .sortBy(_._1)
      .traverse { case (name, environmentValue) =>
        Either
          .cond(
            environmentValue != null,
            name.value -> Json.fromString(environmentValue),
            s"environment value for ${name.value} must not be null"
          )
      }
      .map(fields => Json.obj(fields*))

  private def decodeEnvironment(cursor: HCursor): Either[String, Map[EnvName, String]] =
    cursor
      .downField("environment")
      .focus
      .toRight("missing environment")
      .flatMap(_.asObject.toRight("environment must be an object"))
      .flatMap(
        _.toVector
          .traverse { case (rawName, value) =>
            for
              name <- EnvName.from(rawName).left.map(_.reason)
              environmentValue <- value.asString.toRight(
                s"environment value for $rawName must be a string"
              )
            yield name -> environmentValue
          }
          .map(_.toMap)
      )

  private def encodeRemoteResultRef(value: RemoteResultRef): Json =
    Json.obj(
      "attemptId" -> Json.fromString(value.attemptId.value),
      "attemptEpoch" -> Json.fromLong(value.attemptEpoch.value)
    )

  private def decodeRemoteResultRef(json: Json): Either[String, RemoteResultRef] =
    for
      cursor <- objectCursor(json, "remote result reference")
      attemptText <- field[String](cursor, "attemptId")
      attempt <- AttemptId.from(attemptText).left.map(_.reason)
      epochRaw <- field[Long](cursor, "attemptEpoch")
      epoch <- AttemptEpoch.from(epochRaw).left.map(_.reason)
    yield RemoteResultRef(attempt, epoch)

  /** The longest base64 text that can decode to within `maximumBytes`.
    *
    * The point of comparing against this rather than against the decoded size is ordering:
    * `Base64.getDecoder.decode` allocates an array sized from its input, so a bound checked only
    * afterwards has already permitted the allocation it exists to prevent. Every base64 field in
    * this object is bounded before decoding for that reason.
    */
  private def maximumEncodedLength(maximumBytes: ByteLimit): Long =
    ((maximumBytes.value.toLong + 2L) / 3L) * 4L

  /** Decodes base64 while refusing anything that would decode past `maximumBytes`, checking the
    * encoded length first so the refusal happens before the allocation.
    *
    * Shared rather than private because the durable control journal decodes base64 under the same
    * rule, and two copies of this arithmetic would be two places for the ordering to regress.
    */
  def decodeBase64Bounded(
      encoded: String,
      maximumBytes: ByteLimit,
      fieldName: String
  ): Either[String, ByteVector] =
    for
      _ <- Either.cond(
        encoded.length.toLong <= maximumEncodedLength(maximumBytes),
        (),
        s"$fieldName exceeds its encoded size limit"
      )
      bytes <- Try(ByteVector.view(Base64.getDecoder.decode(encoded))).toEither.left.map(error =>
        Option(error.getMessage).filter(_.nonEmpty).getOrElse(s"$fieldName is invalid base64")
      )
      _ <- Either.cond(
        bytes.size <= maximumBytes.value,
        (),
        s"$fieldName exceeds ${maximumBytes.value} decoded bytes"
      )
    yield bytes

  private def requireRemoteTaskWireVersion(cursor: HCursor): Either[String, Unit] =
    field[Int](cursor, "wireVersion").flatMap { received =>
      Either.cond(
        received == RemoteTaskWireVersion,
        (),
        s"unsupported remote-task wire version: $received"
      )
    }

  private def encodeRemoteSubmissionAttempt(value: SubmissionAttempt): Json = value match
    case SubmissionAttempt.Completed(submission) =>
      Json.obj(
        "kind" -> Json.fromString("completed"),
        "submission" -> encodeRemoteSubmissionValue(submission)
      )
    case SubmissionAttempt.InvocationFailed(result) =>
      Json.obj(
        "kind" -> Json.fromString("invocation-failed"),
        "invocation" -> encodeRemoteInvocationResult(result)
      )
    case SubmissionAttempt.PreparationFailed(diagnostics) =>
      Json.obj(
        "kind" -> Json.fromString("preparation-failed"),
        "diagnostics" -> encodeRemoteDiagnostics(diagnostics)
      )

  private def decodeRemoteSubmissionAttempt(json: Json): Either[String, SubmissionAttempt] =
    for
      cursor <- objectCursor(json, "remote submission attempt")
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "completed" =>
          cursor
            .downField("submission")
            .focus
            .toRight("missing submission")
            .flatMap(decodeRemoteSubmissionValue)
            .map(SubmissionAttempt.Completed.apply)
        case "invocation-failed" =>
          cursor
            .downField("invocation")
            .focus
            .toRight("missing invocation")
            .flatMap(decodeRemoteInvocationResult)
            .map(SubmissionAttempt.InvocationFailed.apply)
        case "preparation-failed" =>
          cursor
            .downField("diagnostics")
            .focus
            .toRight("missing diagnostics")
            .flatMap(decodeRemoteDiagnostics)
            .map(SubmissionAttempt.PreparationFailed.apply)
        case other => Left(s"unknown remote submission-attempt kind: $other")
    yield result

  private def encodeRemoteSubmissionValue(value: Submission): Json = value match
    case Submission.Accepted(job, evidence) =>
      Json.obj(
        "kind" -> Json.fromString("accepted"),
        "job" -> encodeRemoteJobRef(job),
        "evidence" -> encodeRemoteEvidenceBundle(evidence)
      )
    case Submission.Rejected(diagnostics, evidence) =>
      Json.obj(
        "kind" -> Json.fromString("rejected"),
        "diagnostics" -> encodeRemoteDiagnostics(diagnostics),
        "evidence" -> encodeRemoteEvidenceBundle(evidence)
      )
    case Submission.AcceptanceUnknown(reason, evidence) =>
      Json.obj(
        "kind" -> Json.fromString("acceptance-unknown"),
        "reason" -> Json.fromString(acceptanceUncertaintyCode(reason)),
        "evidence" -> encodeRemoteEvidenceBundle(evidence)
      )

  private def decodeRemoteSubmissionValue(json: Json): Either[String, Submission] =
    for
      cursor <- objectCursor(json, "remote submission")
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "accepted" =>
          (
            cursor
              .downField("job")
              .focus
              .toRight("missing job")
              .flatMap(decodeRemoteJobRef),
            cursor
              .downField("evidence")
              .focus
              .toRight("missing evidence")
              .flatMap(decodeRemoteEvidenceBundle)
          )
            .mapN(Submission.Accepted.apply)
        case "rejected" =>
          (
            cursor
              .downField("diagnostics")
              .focus
              .toRight("missing diagnostics")
              .flatMap(decodeRemoteDiagnostics),
            cursor
              .downField("evidence")
              .focus
              .toRight("missing evidence")
              .flatMap(decodeRemoteEvidenceBundle)
          )
            .mapN(Submission.Rejected.apply)
        case "acceptance-unknown" =>
          (
            field[String](cursor, "reason").flatMap(decodeAcceptanceUncertainty),
            cursor
              .downField("evidence")
              .focus
              .toRight("missing evidence")
              .flatMap(decodeRemoteEvidenceBundle)
          ).mapN(Submission.AcceptanceUnknown.apply)
        case other => Left(s"unknown remote submission kind: $other")
    yield result

  private def encodeRemoteInvocationResult(value: InvocationResult): Json = value match
    case InvocationResult.Exited(exitCode, stdout, stderr) =>
      Json.obj(
        "kind" -> Json.fromString("exited"),
        "exitCode" -> Json.fromInt(exitCode),
        "stdout" -> encodeRemoteBoundedEvidence(stdout),
        "stderr" -> encodeRemoteBoundedEvidence(stderr)
      )
    case InvocationResult.SpawnFailed(kind, diagnostics, evidence) =>
      Json.obj(
        "kind" -> Json.fromString("spawn-failed"),
        "spawnKind" -> Json.fromString(spawnFailureKindCode(kind)),
        "diagnostics" -> encodeRemoteDiagnostics(diagnostics),
        "evidence" -> encodeRemoteEvidenceBundle(evidence)
      )
    case InvocationResult.TimedOut(after, stdout, stderr) =>
      Json.obj(
        "kind" -> Json.fromString("timed-out"),
        "afterMillis" -> Json.fromLong(after.value),
        "stdout" -> encodeRemoteBoundedEvidence(stdout),
        "stderr" -> encodeRemoteBoundedEvidence(stderr)
      )

  private def decodeRemoteInvocationResult(json: Json): Either[String, InvocationResult] =
    for
      cursor <- objectCursor(json, "remote invocation result")
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "exited" =>
          (
            field[Int](cursor, "exitCode"),
            cursor
              .downField("stdout")
              .focus
              .toRight("missing stdout")
              .flatMap(decodeRemoteBoundedEvidence),
            cursor
              .downField("stderr")
              .focus
              .toRight("missing stderr")
              .flatMap(decodeRemoteBoundedEvidence)
          ).mapN(InvocationResult.Exited.apply)
        case "spawn-failed" =>
          (
            field[String](cursor, "spawnKind").flatMap(decodeSpawnFailureKind),
            cursor
              .downField("diagnostics")
              .focus
              .toRight("missing diagnostics")
              .flatMap(decodeRemoteDiagnostics),
            cursor
              .downField("evidence")
              .focus
              .toRight("missing evidence")
              .flatMap(decodeRemoteEvidenceBundle)
          ).mapN(InvocationResult.SpawnFailed.apply)
        case "timed-out" =>
          (
            field[Long](cursor, "afterMillis").flatMap(raw =>
              DurationMillis.from(raw).left.map(_.reason)
            ),
            cursor
              .downField("stdout")
              .focus
              .toRight("missing stdout")
              .flatMap(decodeRemoteBoundedEvidence),
            cursor
              .downField("stderr")
              .focus
              .toRight("missing stderr")
              .flatMap(decodeRemoteBoundedEvidence)
          ).mapN(InvocationResult.TimedOut.apply)
        case other => Left(s"unknown remote invocation-result kind: $other")
    yield result

  private def acceptanceUncertaintyCode(value: AcceptanceUncertainty): String = value match
    case AcceptanceUncertainty.ResponseLost           => "response-lost"
    case AcceptanceUncertainty.TransportInterrupted   => "transport-interrupted"
    case AcceptanceUncertainty.ResponseUnparseable    => "response-unparseable"
    case AcceptanceUncertainty.PersistenceInterrupted => "persistence-interrupted"
    case AcceptanceUncertainty.Unclassified           => "unclassified"

  private def decodeAcceptanceUncertainty(value: String): Either[String, AcceptanceUncertainty] =
    value match
      case "response-lost"           => Right(AcceptanceUncertainty.ResponseLost)
      case "transport-interrupted"   => Right(AcceptanceUncertainty.TransportInterrupted)
      case "response-unparseable"    => Right(AcceptanceUncertainty.ResponseUnparseable)
      case "persistence-interrupted" => Right(AcceptanceUncertainty.PersistenceInterrupted)
      case "unclassified"            => Right(AcceptanceUncertainty.Unclassified)
      case other                     => Left(s"unknown acceptance-uncertainty code: $other")

  private def spawnFailureKindCode(value: SpawnFailureKind): String = value match
    case SpawnFailureKind.ExecutableMissing       => "executable-missing"
    case SpawnFailureKind.PermissionDenied        => "permission-denied"
    case SpawnFailureKind.WorkingDirectoryMissing => "working-directory-missing"
    case SpawnFailureKind.EnvironmentInvalid      => "environment-invalid"
    case SpawnFailureKind.ResourceUnavailable     => "resource-unavailable"
    case SpawnFailureKind.Unknown                 => "unknown"

  private def decodeSpawnFailureKind(value: String): Either[String, SpawnFailureKind] =
    value match
      case "executable-missing"        => Right(SpawnFailureKind.ExecutableMissing)
      case "permission-denied"         => Right(SpawnFailureKind.PermissionDenied)
      case "working-directory-missing" => Right(SpawnFailureKind.WorkingDirectoryMissing)
      case "environment-invalid"       => Right(SpawnFailureKind.EnvironmentInvalid)
      case "resource-unavailable"      => Right(SpawnFailureKind.ResourceUnavailable)
      case "unknown"                   => Right(SpawnFailureKind.Unknown)
      case other                       => Left(s"unknown spawn-failure kind: $other")

  private def encodeRemoteResources(value: ResourceRequest): Json =
    Json.obj(
      "cpusPerTask" -> Json.fromInt(value.cpusPerTask.toInt),
      "tasks" -> Json.fromInt(value.tasks.toInt),
      "nodes" -> value.nodes.fold(Json.Null)(nodes => Json.fromInt(nodes.toInt)),
      "memory" -> value.memory.fold(Json.Null)(encodeRemoteMemory),
      "wallTimeMinutes" -> value.wallTime.fold(Json.Null)(minutes => Json.fromLong(minutes.toLong))
    )

  private def decodeRemoteResources(json: Json): Either[String, ResourceRequest] =
    for
      cursor <- objectCursor(json, "remote resource request")
      cpusRaw <- field[Int](cursor, "cpusPerTask")
      cpus <- PositiveInt.from("cpusPerTask", cpusRaw).left.map(_.reason)
      tasksRaw <- field[Int](cursor, "tasks")
      tasks <- PositiveInt.from("tasks", tasksRaw).left.map(_.reason)
      nodesRaw <- field[Option[Int]](cursor, "nodes")
      nodes <- nodesRaw.traverse(raw => PositiveInt.from("nodes", raw).left.map(_.reason))
      memory <- cursor.downField("memory").focus match
        case None                        => Right(None)
        case Some(value) if value.isNull => Right(None)
        case Some(value)                 => decodeRemoteMemory(value).map(Some(_))
      wallRaw <- field[Option[Long]](cursor, "wallTimeMinutes")
      wallTime <- wallRaw.traverse(raw => WallTimeMinutes.from(raw).left.map(_.reason))
    yield ResourceRequest(cpus, tasks, nodes, memory, wallTime)

  private def encodeRemoteMemory(value: MemoryRequest): Json = value match
    case MemoryRequest.PerNode(amount) =>
      Json.obj(
        "kind" -> Json.fromString("per-node"),
        "mebibytes" -> Json.fromLong(amount.toLong)
      )
    case MemoryRequest.PerCpu(amount) =>
      Json.obj(
        "kind" -> Json.fromString("per-cpu"),
        "mebibytes" -> Json.fromLong(amount.toLong)
      )
    case MemoryRequest.AllNodeMemory =>
      Json.obj("kind" -> Json.fromString("all-node-memory"))

  private def decodeRemoteMemory(json: Json): Either[String, MemoryRequest] =
    for
      cursor <- objectCursor(json, "remote memory request")
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "per-node" =>
          field[Long](cursor, "mebibytes")
            .flatMap(raw => Mebibytes.from(raw).left.map(_.reason))
            .map(MemoryRequest.PerNode.apply)
        case "per-cpu" =>
          field[Long](cursor, "mebibytes")
            .flatMap(raw => Mebibytes.from(raw).left.map(_.reason))
            .map(MemoryRequest.PerCpu.apply)
        case "all-node-memory" => Right(MemoryRequest.AllNodeMemory)
        case other             => Left(s"unknown remote memory kind: $other")
    yield result

  private def encodeRemoteEvidenceBundle(value: EvidenceBundle): Json =
    Json.obj(
      "primary" -> encodeRemoteBoundedEvidence(value.primary),
      "related" -> Json.arr(value.related.map(encodeRemoteBoundedEvidence)*)
    )

  private def decodeRemoteEvidenceBundle(json: Json): Either[String, EvidenceBundle] =
    for
      cursor <- objectCursor(json, "remote evidence bundle")
      primaryJson <- cursor.downField("primary").focus.toRight("missing primary evidence")
      primary <- decodeRemoteBoundedEvidence(primaryJson)
      relatedJson <- field[Vector[Json]](cursor, "related")
      related <- relatedJson.traverse(decodeRemoteBoundedEvidence)
    yield EvidenceBundle(primary, related)

  private def encodeRemoteBoundedEvidence(value: BoundedEvidence): Json =
    Json.obj(
      "source" -> encodeRemoteEvidenceSource(value.source),
      "observedAt" -> Json.fromString(value.observedAt.toString),
      "bytesBase64" -> Json.fromString(Base64.getEncoder.encodeToString(value.bytes.toArray)),
      "originalByteCount" -> Json.fromLong(value.originalByteCount)
    )

  private def decodeRemoteBoundedEvidence(json: Json): Either[String, BoundedEvidence] =
    for
      cursor <- objectCursor(json, "remote bounded evidence")
      sourceJson <- cursor.downField("source").focus.toRight("missing evidence source")
      source <- decodeRemoteEvidenceSource(sourceJson)
      observedAt <- field[Instant](cursor, "observedAt")
      encoded <- field[String](cursor, "bytesBase64")
      bytes <- decodeBase64Bounded(
        encoded,
        ByteLimit.maximumCommandCapture,
        "evidence.bytesBase64"
      )
      original <- field[Long](cursor, "originalByteCount")
      _ <- Either.cond(
        original >= bytes.size.toLong,
        (),
        "evidence originalByteCount is smaller than retained bytes"
      )
    yield BoundedEvidence.fromCapture(source, observedAt, bytes, original)

  private def encodeRemoteEvidenceSource(value: EvidenceSource): Json = value match
    case EvidenceSource.CommandStdout(command) =>
      Json.obj(
        "kind" -> Json.fromString("command-stdout"),
        "command" -> Json.fromString(command)
      )
    case EvidenceSource.CommandStderr(command) =>
      Json.obj(
        "kind" -> Json.fromString("command-stderr"),
        "command" -> Json.fromString(command)
      )
    case EvidenceSource.CommandLaunch(command) =>
      Json.obj(
        "kind" -> Json.fromString("command-launch"),
        "command" -> Json.fromString(command)
      )
    case EvidenceSource.SchedulerJson(command, dataParser) =>
      Json.obj(
        "kind" -> Json.fromString("scheduler-json"),
        "command" -> Json.fromString(command),
        "dataParser" -> Json.fromString(dataParser)
      )
    case EvidenceSource.SchedulerText(command) =>
      Json.obj(
        "kind" -> Json.fromString("scheduler-text"),
        "command" -> Json.fromString(command)
      )
    case EvidenceSource.AgentProtocol =>
      Json.obj("kind" -> Json.fromString("agent-protocol"))
    case EvidenceSource.DurableJournal =>
      Json.obj("kind" -> Json.fromString("durable-journal"))
    case EvidenceSource.WorkerEvent =>
      Json.obj("kind" -> Json.fromString("worker-event"))
    case EvidenceSource.ResultEnvelope =>
      Json.obj("kind" -> Json.fromString("result-envelope"))

  private def decodeRemoteEvidenceSource(json: Json): Either[String, EvidenceSource] =
    for
      cursor <- objectCursor(json, "remote evidence source")
      kind <- field[String](cursor, "kind")
      result <- kind match
        case "command-stdout" =>
          field[String](cursor, "command").map(EvidenceSource.CommandStdout.apply)
        case "command-stderr" =>
          field[String](cursor, "command").map(EvidenceSource.CommandStderr.apply)
        case "command-launch" =>
          field[String](cursor, "command").map(EvidenceSource.CommandLaunch.apply)
        case "scheduler-json" =>
          (
            field[String](cursor, "command"),
            field[String](cursor, "dataParser")
          ).mapN(EvidenceSource.SchedulerJson.apply)
        case "scheduler-text" =>
          field[String](cursor, "command").map(EvidenceSource.SchedulerText.apply)
        case "agent-protocol"  => Right(EvidenceSource.AgentProtocol)
        case "durable-journal" => Right(EvidenceSource.DurableJournal)
        case "worker-event"    => Right(EvidenceSource.WorkerEvent)
        case "result-envelope" => Right(EvidenceSource.ResultEnvelope)
        case other             => Left(s"unknown remote evidence-source kind: $other")
    yield result

  private def encodeRemoteJobRef(value: JobRef): Json =
    Json.obj(
      "jobId" -> Json.fromString(value.jobId.value),
      "arrayIndex" -> value.arrayIndex.fold(Json.Null)(index => Json.fromInt(index.value))
    )

  private def decodeRemoteJobRef(json: Json): Either[String, JobRef] =
    for
      cursor <- objectCursor(json, "remote job reference")
      jobText <- field[String](cursor, "jobId")
      jobId <- JobId.from(jobText).left.map(_.reason)
      // As in `StructuredJson.decodeJob`, a legacy `cluster` is ignored rather than validated:
      // `JobRef` dropped cluster identity, so this decoder must not fail on a field it discards.
      arrayRaw <- field[Option[Int]](cursor, "arrayIndex")
      arrayIndex <- arrayRaw.traverse(raw => ArrayIndex.from(raw).left.map(_.reason))
    yield JobRef(jobId, arrayIndex)

  private def encodeRemoteDiagnostics(value: Diagnostics): Json =
    Json.obj(
      "entries" -> Json.arr(
        value.toVector.map { diagnostic =>
          Json.obj(
            "code" -> Json.fromString(diagnostic.code),
            "message" -> Json.fromString(diagnostic.message),
            "fields" -> Json.obj(
              diagnostic.fields.toVector.sortBy(_._1).map { case (name, fieldValue) =>
                name -> Json.fromString(fieldValue)
              }*
            )
          )
        }*
      )
    )

  private def decodeRemoteDiagnostics(json: Json): Either[String, Diagnostics] =
    for
      cursor <- objectCursor(json, "remote diagnostics")
      entries <- field[Vector[Json]](cursor, "entries")
      values <- entries.traverse { entry =>
        for
          entryCursor <- objectCursor(entry, "remote diagnostic")
          code <- field[String](entryCursor, "code")
          message <- field[String](entryCursor, "message")
          fields <- field[Map[String, String]](entryCursor, "fields")
        yield Diagnostic(code, message, fields)
      }
      diagnostics <- Diagnostics.fromVector(values).left.map(_.reason)
    yield diagnostics

  extension [A: Encoder](value: A) private def asJson: Json = summon[Encoder[A]].apply(value)

  private given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)
  private given Decoder[Instant] = Decoder.decodeString.emap { raw =>
    Try(Instant.parse(raw)).toEither.left.map(_.getMessage)
  }
  private given Encoder[LocalDateTime] = Encoder.encodeString.contramap(_.toString)
  private given Decoder[LocalDateTime] = Decoder.decodeString.emap { raw =>
    Try(LocalDateTime.parse(raw)).toEither.left.map(_.getMessage)
  }

  /** A JSON array of byte numbers, which is what the derived codecs in this object emitted while
    * bytes were a `Vector[Byte]` and circe supplied the instance implicitly.
    *
    * Reproducing that shape is the point: these instances feed `deriveEncoder`/`deriveDecoder`, so
    * anything else would silently change an existing wire format. It is deliberately not base64,
    * even though every hand-written codec here uses base64 for bytes — that inconsistency predates
    * this change and belongs to the codec-ownership work, not to a representation swap.
    */
  private given Encoder[ByteVector] = Encoder.encodeVector[Byte].contramap(_.toIndexedSeq.toVector)
  private given Decoder[ByteVector] = Decoder.decodeVector[Byte].map(ByteVector.apply)

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
      // Same ceiling the remote path applies to this same field via decodeRemoteBoundedEvidence,
      // so the agent and remote decoders no longer disagree about whether evidence is bounded.
      bytes <- decodeBase64(
        encoded,
        cursor,
        ByteLimit.maximumCommandCapture,
        "evidence.bytesBase64"
      )
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

  private given Encoder[SlurmStateFlag] = Encoder.instance {
    case SlurmStateFlag.Unknown(raw) =>
      Json.obj(
        "code" -> Json.fromString("unknown"),
        "raw" -> Json.fromString(raw)
      )
    case flag =>
      Json.obj("code" -> Json.fromString(slurmStateFlagCode(flag)))
  }
  private given Decoder[SlurmStateFlag] = Decoder.instance { cursor =>
    for
      code <- cursor.get[String]("code")
      raw <- cursor.get[Option[String]]("raw")
      flag <- decodeSlurmStateFlag(code, raw, cursor)
    yield flag
  }

  private def slurmStateFlagCode(flag: SlurmStateFlag): String = flag match
    case SlurmStateFlag.Completing            => "completing"
    case SlurmStateFlag.Configuring           => "configuring"
    case SlurmStateFlag.PowerUpNode           => "power-up-node"
    case SlurmStateFlag.StageOut              => "stage-out"
    case SlurmStateFlag.Resizing              => "resizing"
    case SlurmStateFlag.Requeued              => "requeued"
    case SlurmStateFlag.RequeueFederation     => "requeue-federation"
    case SlurmStateFlag.RequeueHold           => "requeue-hold"
    case SlurmStateFlag.Revoked               => "revoked"
    case SlurmStateFlag.Signaling             => "signaling"
    case SlurmStateFlag.SpecialExit           => "special-exit"
    case SlurmStateFlag.Stopped               => "stopped"
    case SlurmStateFlag.ReservationDeleteHold => "reservation-delete-hold"
    case SlurmStateFlag.LaunchFailed          => "launch-failed"
    case SlurmStateFlag.UpdateDb              => "update-db"
    case SlurmStateFlag.Unknown(_)            => "unknown"

  private def decodeSlurmStateFlag(
      code: String,
      raw: Option[String],
      cursor: HCursor
  ): Decoder.Result[SlurmStateFlag] =
    code match
      case "completing"              => Right(SlurmStateFlag.Completing)
      case "configuring"             => Right(SlurmStateFlag.Configuring)
      case "power-up-node"           => Right(SlurmStateFlag.PowerUpNode)
      case "stage-out"               => Right(SlurmStateFlag.StageOut)
      case "resizing"                => Right(SlurmStateFlag.Resizing)
      case "requeued"                => Right(SlurmStateFlag.Requeued)
      case "requeue-federation"      => Right(SlurmStateFlag.RequeueFederation)
      case "requeue-hold"            => Right(SlurmStateFlag.RequeueHold)
      case "revoked"                 => Right(SlurmStateFlag.Revoked)
      case "signaling"               => Right(SlurmStateFlag.Signaling)
      case "special-exit"            => Right(SlurmStateFlag.SpecialExit)
      case "stopped"                 => Right(SlurmStateFlag.Stopped)
      case "reservation-delete-hold" => Right(SlurmStateFlag.ReservationDeleteHold)
      case "launch-failed"           => Right(SlurmStateFlag.LaunchFailed)
      case "update-db"               => Right(SlurmStateFlag.UpdateDb)
      case "unknown"                 =>
        raw
          .filter(_.nonEmpty)
          .map(value => Right(SlurmStateFlag.Unknown(value)))
          .getOrElse(
            Left(
              DecodingFailure(
                "unknown Slurm state flag requires non-empty raw text",
                cursor.history
              )
            )
          )
      case other =>
        Left(DecodingFailure(s"unsupported Slurm state flag code: $other", cursor.history))

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
    // `flags` and `reportedCluster` are emitted only when actually reported, so an observation
    // without them keeps the wire shape it had before they existed and the decoder's defaults
    // cover the rest.
    Json.obj(
      Vector(
        "job" -> value.job.asJson,
        "state" -> value.state.asJson,
        "freshness" -> value.freshness.asJson,
        "reason" -> value.reason.asJson,
        "rawFields" -> value.rawFields.asJson,
        "evidence" -> value.evidence.asJson,
        "timing" -> value.timing.asJson
      ) ++ Option.when(value.flags.nonEmpty)("flags" -> value.flags.asJson)
        ++ value.reportedCluster.map(cluster => "reportedCluster" -> cluster.asJson)*
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
      flags <- cursor
        .get[Option[Vector[SlurmStateFlag]]]("flags")
        .map(_.getOrElse(Vector.empty))
      reportedCluster <- cursor.get[Option[ClusterName]]("reportedCluster")
    yield JobObservation(
      job,
      state,
      freshness,
      reason,
      rawFields,
      evidence,
      timing,
      flags,
      reportedCluster
    )
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
  private given Encoder[LogPage] = Encoder.instance { value =>
    Json.obj(
      "bytesBase64" -> Json.fromString(Base64.getEncoder.encodeToString(value.bytes.toArray)),
      "next" -> value.next.asJson,
      "endOfFile" -> Json.fromBoolean(value.endOfFile),
      "observedAt" -> value.observedAt.asJson
    )
  }
  private given Decoder[LogPage] = Decoder.instance { cursor =>
    for
      encoded <- cursor.get[String]("bytesBase64")
      bytes <- decodeBase64(encoded, cursor, ByteLimit.maximumLogPage, "logPage.bytesBase64")
      next <- cursor.get[LogCursor]("next")
      endOfFile <- cursor.get[Boolean]("endOfFile")
      observedAt <- cursor.get[Instant]("observedAt")
    yield LogPage(bytes, next, endOfFile, observedAt)
  }
  private given Encoder[LogReadResult] = Encoder.instance {
    case LogReadResult.Page(value) =>
      value.asJson.deepMerge(Json.obj("kind" -> Json.fromString("page")))
    case LogReadResult.WaitingForFile(cursor, observedAt) =>
      Json.obj(
        "kind" -> Json.fromString("waiting-for-file"),
        "cursor" -> cursor.asJson,
        "observedAt" -> observedAt.asJson
      )
    case LogReadResult.CursorInvalid(requested, currentIdentity, currentSize, observedAt) =>
      Json.obj(
        "kind" -> Json.fromString("cursor-invalid"),
        "requested" -> requested.asJson,
        "currentIdentity" -> currentIdentity.asJson,
        "currentSize" -> Json.fromLong(currentSize),
        "observedAt" -> observedAt.asJson
      )
    case LogReadResult.Failed(diagnostics, observedAt) =>
      Json.obj(
        "kind" -> Json.fromString("failed"),
        "diagnostics" -> diagnostics.asJson,
        "observedAt" -> observedAt.asJson
      )
  }
  private given Decoder[LogReadResult] = Decoder.instance { cursor =>
    cursor.get[String]("kind").flatMap {
      case "page" =>
        summon[Decoder[LogPage]].apply(cursor).map(LogReadResult.Page.apply)
      case "waiting-for-file" =>
        for
          position <- cursor.get[LogCursor]("cursor")
          observedAt <- cursor.get[Instant]("observedAt")
        yield LogReadResult.WaitingForFile(position, observedAt)
      case "cursor-invalid" =>
        for
          requested <- cursor.get[LogCursor]("requested")
          currentIdentity <- cursor.get[Option[FileIdentity]]("currentIdentity")
          currentSize <- cursor.get[Long]("currentSize")
          observedAt <- cursor.get[Instant]("observedAt")
        yield LogReadResult.CursorInvalid(requested, currentIdentity, currentSize, observedAt)
      case "failed" =>
        for
          diagnostics <- cursor.get[Diagnostics]("diagnostics")
          observedAt <- cursor.get[Instant]("observedAt")
        yield LogReadResult.Failed(diagnostics, observedAt)
      case other =>
        Left(DecodingFailure(s"unknown log-read result kind: $other", cursor.history))
    }
  }

  private def decodeBase64(
      encoded: String,
      cursor: HCursor,
      maximumBytes: ByteLimit,
      fieldName: String
  ): Decoder.Result[ByteVector] =
    if encoded.length.toLong > maximumEncodedLength(maximumBytes) then
      Left(DecodingFailure(s"$fieldName exceeds its encoded size limit", cursor.history))
    else
      Try(ByteVector.view(Base64.getDecoder.decode(encoded))).toEither.left
        .map { error =>
          DecodingFailure(
            Option(error.getMessage).filter(_.nonEmpty).getOrElse("invalid base64"),
            cursor.history
          )
        }
        .flatMap { bytes =>
          Either.cond(
            bytes.size <= maximumBytes.value.toLong,
            bytes,
            DecodingFailure(
              s"$fieldName exceeds ${maximumBytes.value} decoded bytes",
              cursor.history
            )
          )
        }

  private def slurmStateCode(state: SlurmState): String = state match
    case SlurmState.Pending     => "pending"
    case SlurmState.Running     => "running"
    case SlurmState.Completed   => "completed"
    case SlurmState.Failed      => "failed"
    case SlurmState.Cancelled   => "cancelled"
    case SlurmState.OutOfMemory => "out-of-memory"
    case SlurmState.TimedOut    => "timed-out"
    case SlurmState.NodeFailure => "node-failure"
    case SlurmState.Preempted   => "preempted"
    case SlurmState.BootFail    => "boot-fail"
    case SlurmState.Deadline    => "deadline"
    case SlurmState.Suspended   => "suspended"
    case SlurmState.Unknown(_)  => "unknown"

  private def decodeSlurmState(
      code: String,
      raw: Option[String],
      cursor: HCursor
  ): Decoder.Result[SlurmState] =
    code match
      case "pending"       => Right(SlurmState.Pending)
      case "running"       => Right(SlurmState.Running)
      case "completed"     => Right(SlurmState.Completed)
      case "failed"        => Right(SlurmState.Failed)
      case "cancelled"     => Right(SlurmState.Cancelled)
      case "out-of-memory" => Right(SlurmState.OutOfMemory)
      case "timed-out"     => Right(SlurmState.TimedOut)
      case "node-failure"  => Right(SlurmState.NodeFailure)
      case "preempted"     => Right(SlurmState.Preempted)
      case "boot-fail"     => Right(SlurmState.BootFail)
      case "deadline"      => Right(SlurmState.Deadline)
      case "suspended"     => Right(SlurmState.Suspended)
      case "unknown"       =>
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
      case Some(("Pending", _)) => Right(SlurmState.Pending)
      case Some(("Running", _)) => Right(SlurmState.Running)
      // Legacy wire carried COMPLETING as a base state. It is a flag in SchedMD's vocabulary, so
      // it decodes to an unknown base state with the token preserved rather than a fabricated one.
      case Some(("Completing", _))    => Right(SlurmState.Unknown("COMPLETING"))
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
