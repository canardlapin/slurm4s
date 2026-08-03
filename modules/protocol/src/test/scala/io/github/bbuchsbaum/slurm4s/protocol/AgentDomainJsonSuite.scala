package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.*

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.time.Instant

class AgentDomainJsonSuite extends munit.FunSuite:
  test("agent submit JSON round-trips array identity and accepts legacy absence") {
    val array = JobArrayRequest
      .contiguous(
        PositiveInt.from("size", 3).toOption.get,
        Some(PositiveInt.from("maximumConcurrent", 2).toOption.get)
      )
      .toOption
      .get
    val request = LaunchSpec(
      SubmissionKey.from("wire-array").toOption.get,
      JobName.from("wire-array").toOption.get,
      ScriptSource.ExistingRemote("/work/array.sh"),
      Vector.empty,
      ResultContract.ExitOnly.descriptor,
      ResourceRequest.validate(1, 1, None, None, None).toOption.get,
      Map.empty,
      Some(array)
    )

    val encoded = AgentDomainJson.encodeSubmitRequest(request).toOption.get
    assertEquals(AgentDomainJson.decodeSubmitRequest(encoded), Right(request))

    val legacy = encoded.mapObject(_.remove("array"))
    assertEquals(
      AgentDomainJson.decodeSubmitRequest(legacy).map(_.array),
      Right(None)
    )

    val ordinary = request.copy(array = None)
    val ordinaryJson = AgentDomainJson.encodeSubmitRequest(ordinary).toOption.get
    assert(!ordinaryJson.hcursor.downField("array").succeeded)
    assert(!ordinaryJson.hcursor.downField("retrySafety").succeeded)
    assertEquals(AgentDomainJson.decodeSubmitRequest(ordinaryJson), Right(ordinary))

    val retryable = ordinary.copy(retrySafety = RetrySafety.SafeForAutomaticRetry)
    val retryableJson = AgentDomainJson.encodeSubmitRequest(retryable).toOption.get
    assertEquals(
      retryableJson.hcursor.get[String]("retrySafety").toOption,
      Some("safe-for-automatic-retry")
    )
    assertEquals(AgentDomainJson.decodeSubmitRequest(retryableJson), Right(retryable))
    assertEquals(
      AgentDomainJson
        .decodeSubmitRequest(retryableJson.mapObject(_.remove("retrySafety")))
        .map(_.retrySafety),
      Right(RetrySafety.Unknown)
    )
  }

  test("agent submit wire round-trips validated environment names and rejects export injection") {
    val request = LaunchSpec(
      SubmissionKey.from("wire-environment").toOption.get,
      JobName.from("wire-environment").toOption.get,
      ScriptSource.ExistingRemote("/work/environment.sh"),
      Vector.empty,
      ResultContract.ExitOnly.descriptor,
      ResourceRequest.validate(1, 1, None, None, None).toOption.get,
      Map(EnvName.unsafeFrom("LANG") -> "C.UTF-8")
    )
    val encoded = AgentDomainJson.encodeSubmitRequest(request).toOption.get
    val injected = encoded.mapObject(
      _.add(
        "environment",
        Json.obj("FOO,ALL" -> Json.fromString("unsafe"))
      )
    )

    assertEquals(AgentDomainJson.decodeSubmitRequest(encoded), Right(request))
    assert(AgentDomainJson.decodeSubmitRequest(injected).left.exists(_.contains("portable")))
  }

  test("observation timing has an owned golden wire shape and round-trips") {
    val observedAt = Instant.parse("2026-07-23T12:00:00Z")
    val evidence = BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      observedAt,
      ByteVector.view("queue-evidence".getBytes(StandardCharsets.UTF_8))
    )
    val observation = JobObservation(
      job = JobRef(JobId.from("2001").toOption.get, None),
      state = SlurmState.Running,
      freshness = Freshness.Current(observedAt),
      reason = None,
      rawFields = Map("job_state" -> "[\"RUNNING\"]"),
      evidence = EvidenceBundle(evidence),
      timing = JobTiming(
        Some(
          JobStart.Actual(
            SchedulerTimestamp.Absolute(Instant.parse("2026-07-23T10:00:00Z"))
          )
        ),
        Some(SchedulerTimestamp.Absolute(Instant.parse("2026-07-23T11:30:00Z"))),
        ObservedTimeLimit.Limited(WallTimeMinutes.from(90).toOption.get)
      )
    )
    val result: SchedulerQueryResult[ObservationBatch] =
      SchedulerQueryResult.Succeeded(
        ObservationBatch(NonEmptyVector.one(ObservationResult.Observed(observation)))
      )
    val encoded = AgentDomainJson.encodeObservation(result)

    assertEquals(AgentDomainJson.decodeObservation(encoded), Right(result))
    assertEquals(
      encoded,
      io.circe.parser
        .parse(resource("/fixtures/observation-timing-v1.json"))
        .toOption
        .get
    )
  }

  test("observations carry reported cluster evidence across the wire") {
    val observedAt = Instant.parse("2026-07-23T12:00:00Z")
    val cluster = ClusterName.from("alpha").toOption.get
    val observation = JobObservation(
      job = JobRef(JobId.from("2003").toOption.get, None),
      state = SlurmState.Running,
      freshness = Freshness.Current(observedAt),
      reason = None,
      rawFields = Map.empty,
      evidence = EvidenceBundle(
        BoundedEvidence.capture(
          EvidenceSource.CommandStdout("squeue"),
          observedAt,
          ByteVector.empty
        )
      ),
      reportedCluster = Some(cluster)
    )
    val result: SchedulerQueryResult[ObservationBatch] =
      SchedulerQueryResult.Succeeded(
        ObservationBatch(NonEmptyVector.one(ObservationResult.Observed(observation)))
      )
    val encoded = AgentDomainJson.encodeObservation(result)

    assertEquals(AgentDomainJson.decodeObservation(encoded), Right(result))

    // A peer that never learned the field must still decode, so absence stays `None` rather than
    // becoming a decode failure.
    assertEquals(
      AgentDomainJson
        .decodeObservation(removeField(encoded, "reportedCluster"))
        .map(observedCluster),
      Right(None)
    )
  }

  test("legacy observations without timing decode conservatively") {
    val observedAt = Instant.parse("2026-07-23T12:00:00Z")
    val evidence = BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      observedAt,
      ByteVector.empty
    )
    val observation = JobObservation(
      JobRef(JobId.from("2002").toOption.get, None),
      SlurmState.Pending,
      Freshness.Current(observedAt),
      Some("Priority"),
      Map.empty,
      EvidenceBundle(evidence),
      JobTiming(
        Some(
          JobStart.Expected(
            SchedulerTimestamp.Absolute(Instant.parse("2026-07-23T13:00:00Z"))
          )
        ),
        None,
        ObservedTimeLimit.Unlimited
      )
    )
    val encoded = AgentDomainJson.encodeObservation(
      SchedulerQueryResult.Succeeded(
        ObservationBatch(NonEmptyVector.one(ObservationResult.Observed(observation)))
      )
    )
    val decoded = AgentDomainJson.decodeObservation(removeField(encoded, "timing")).toOption.get
    val legacyTiming = decoded match
      case SchedulerQueryResult.Succeeded(batch) =>
        batch.results.head match
          case ObservationResult.Observed(value) => value.timing
          case other                             => fail(s"unexpected observation result: $other")
      case other => fail(s"unexpected query result: $other")

    assertEquals(legacyTiming, JobTiming.unknown)
  }

  test("log pages use an owned base64 wire shape and preserve unsigned byte patterns") {
    val result = LogReadResult.Page(
      LogPage(
        ByteVector(0x00, 0x7f, 0x80, 0xff),
        LogCursor(
          LogOffset.from(4L).toOption.get,
          Some(FileIdentity.from("fixture-file").toOption.get)
        ),
        endOfFile = true,
        Instant.parse("2026-07-24T12:00:00Z")
      )
    )
    val encoded = AgentDomainJson.encodeLogResult(result)

    assertEquals(AgentDomainJson.decodeLogResult(encoded), Right(result))
    assertEquals(
      encoded,
      io.circe.parser.parse(resource("/fixtures/log-read-page-v1.json")).toOption.get
    )
    assertEquals(encoded.hcursor.get[String]("bytesBase64").toOption, Some("AH+A/w=="))
    assert(!encoded.noSpaces.contains("[0,127"))
  }

  test("invalid base64 log bytes are rejected") {
    val invalid = Json.obj(
      "kind" -> Json.fromString("page"),
      "bytesBase64" -> Json.fromString("not base64!"),
      "next" -> Json.obj(
        "offset" -> Json.fromLong(0L),
        "fileIdentity" -> Json.Null
      ),
      "endOfFile" -> Json.fromBoolean(false),
      "observedAt" -> Json.fromString("2026-07-24T12:00:00Z")
    )

    assert(AgentDomainJson.decodeLogResult(invalid).isLeft)
  }

  test("remote registered-task requests have a versioned owned wire shape") {
    val request = RemoteRegisteredTaskRequest(
      SubmissionKey.from("remote-increment-41").toOption.get,
      JobName.from("remote-increment").toOption.get,
      RegisteredOperation(
        OperationId.from("example.increment").toOption.get,
        OperationVersion.from("1").toOption.get,
        SchemaId.from("example.int-input.v1").toOption.get,
        ResultSchemaId.from("example.int-result.v1").toOption.get
      ),
      ByteVector(0x00, 0x7f, 0x80, 0xff),
      ResourceRequest
        .validate(
          2,
          1,
          None,
          Some(MemoryRequest.PerNode(Mebibytes.from(2048).toOption.get)),
          Some(WallTimeMinutes.from(30).toOption.get)
        )
        .toOption
        .get,
      Map(EnvName.unsafeFrom("LANG") -> "C.UTF-8"),
      ByteLimit.from(2048).toOption.get,
      Vector(RelativeOutputPath.from("results/out.bin").toOption.get),
      RetrySafety.SafeForAutomaticRetry
    )
    val encoded = AgentDomainJson.encodeRemoteTaskRequest(request).toOption.get

    assertEquals(AgentDomainJson.decodeRemoteTaskRequest(encoded), Right(request))
    assertEquals(
      encoded,
      io.circe.parser
        .parse(resource("/fixtures/remote-registered-task-request-v1.json"))
        .toOption
        .get
    )
    assertEquals(encoded.hcursor.get[String]("inputBase64").toOption, Some("AH+A/w=="))
    assert(!encoded.noSpaces.contains("[0,127"))
    assert(!encoded.noSpaces.contains("\"PerNode\""))
    assert(
      AgentDomainJson
        .decodeRemoteTaskRequest(encoded.mapObject(_.add("wireVersion", Json.fromInt(2))))
        .left
        .exists(_.contains("unsupported"))
    )
  }

  test("remote task responses own their discriminators and reject invalid bounded bytes") {
    val observedAt = Instant.parse("2026-07-24T12:00:00Z")
    val pending = RemoteResultRead.Pending(observedAt)
    val pendingJson = AgentDomainJson.encodeRemoteResultRead(pending).toOption.get

    assertEquals(
      pendingJson,
      io.circe.parser
        .parse(resource("/fixtures/remote-result-pending-v1.json"))
        .toOption
        .get
    )
    assertEquals(
      AgentDomainJson.decodeRemoteResultRead(pendingJson, ByteLimit.from(2048).toOption.get),
      Right(pending)
    )

    val evidence = EvidenceBundle(
      BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, ByteVector.empty)
    )
    val attemptId = AttemptId.from("remote-response-attempt").toOption.get
    val epoch = AttemptEpoch.initial
    val handle = DurableResultHandle
      .from(
        SubmissionKey.from("remote-response").toOption.get,
        attemptId,
        epoch,
        None,
        WorkloadOperation.Registered(
          OperationId.from("example.increment").toOption.get,
          OperationVersion.from("1").toOption.get
        ),
        ResultSchemaId.from("example.int-result.v1").toOption.get,
        ByteLimit.from(2048).toOption.get,
        ByteLimit.defaultEvidence,
        Vector.empty,
        WorkerRelease(
          WorkerReleaseId.from("worker-1").toOption.get,
          ContentDigest
            .from("sha256:13029f9e83d15b3d437c2a7568fc1ca7990ecf3ff79bef6da08f13ff5ae12af8")
            .toOption
            .get
        ),
        RetrySafety.SafeForAutomaticRetry
      )
      .toOption
      .get
    val submission = RemoteRegisteredSubmission(
      RemoteResultRef(attemptId, epoch),
      handle,
      SubmissionAttempt.Completed(
        Submission.AcceptanceUnknown(AcceptanceUncertainty.ResponseLost, evidence)
      )
    )
    val submissionJson = AgentDomainJson.encodeRemoteSubmission(submission).toOption.get
    assertEquals(AgentDomainJson.decodeRemoteSubmission(submissionJson), Right(submission))
    val descriptorBytes = RemoteRegisteredSubmissionCodec.encode(submission).toOption.get
    assertEquals(RemoteRegisteredSubmissionCodec.decode(descriptorBytes), Right(submission))
    assertEquals(
      submissionJson.hcursor
        .downField("submission")
        .get[String]("kind")
        .toOption,
      Some("completed")
    )
    assertEquals(
      submissionJson.hcursor
        .downField("submission")
        .downField("submission")
        .get[String]("kind")
        .toOption,
      Some("acceptance-unknown")
    )
    assert(!submissionJson.noSpaces.contains("\"AcceptanceUnknown\""))
    assert(!submissionJson.noSpaces.contains("\"AgentProtocol\""))

    val oversizedBase64 = java.util.Base64.getEncoder.encodeToString(Array.fill[Byte](2049)(1))
    val invalidAvailable = Json.obj(
      "wireVersion" -> Json.fromInt(1),
      "kind" -> Json.fromString("available"),
      "storedHandleBase64" -> Json.fromString("e30="),
      "envelopeBase64" -> Json.fromString(oversizedBase64),
      "observedAt" -> Json.fromString(observedAt.toString)
    )
    assert(
      AgentDomainJson
        .decodeRemoteResultRead(invalidAvailable, ByteLimit.from(2048).toOption.get)
        .isLeft
    )
  }

  test("Slurm state wire codes are explicit, exhaustive, and legacy-readable") {
    val states = Vector(
      SlurmState.Pending,
      SlurmState.Running,
      SlurmState.Completed,
      SlurmState.Failed,
      SlurmState.Cancelled,
      SlurmState.OutOfMemory,
      SlurmState.TimedOut,
      SlurmState.NodeFailure,
      SlurmState.Preempted,
      SlurmState.BootFail,
      SlurmState.Deadline,
      SlurmState.Suspended,
      SlurmState.Unknown("FUTURE_STATE")
    )
    val encoded = states.map(observationWithState)
    val stateJson = encoded.map(
      _.hcursor
        .downField("Succeeded")
        .downField("value")
        .downArray
        .downField("Observed")
        .downField("value")
        .downField("state")
        .focus
        .get
    )

    assertEquals(
      Json.fromValues(stateJson),
      io.circe.parser
        .parse(resource("/fixtures/slurm-state-codes-v1.json"))
        .toOption
        .get
    )
    states.zip(encoded).foreach { case (expected, wire) =>
      assertEquals(observationState(AgentDomainJson.decodeObservation(wire).toOption.get), expected)
    }

    val legacy = replaceField(
      observationWithState(SlurmState.Running),
      "state",
      Json.obj("Running" -> Json.obj())
    )
    assertEquals(
      observationState(AgentDomainJson.decodeObservation(legacy).toOption.get),
      SlurmState.Running
    )
  }

  private def observationWithState(state: SlurmState): Json =
    val observedAt = Instant.parse("2026-07-23T12:00:00Z")
    val evidence = BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      observedAt,
      ByteVector.empty
    )
    AgentDomainJson.encodeObservation(
      SchedulerQueryResult.Succeeded(
        ObservationBatch(
          NonEmptyVector.one(
            ObservationResult.Observed(
              JobObservation(
                JobRef(JobId.from("state-fixture").toOption.get, None),
                state,
                Freshness.Current(observedAt),
                None,
                Map.empty,
                EvidenceBundle(evidence)
              )
            )
          )
        )
      )
    )

  private def observationState(
      result: SchedulerQueryResult[ObservationBatch]
  ): SlurmState = result match
    case SchedulerQueryResult.Succeeded(batch) =>
      batch.results.head match
        case ObservationResult.Observed(value) => value.state
        case other                             => fail(s"unexpected observation result: $other")
    case other => fail(s"unexpected query result: $other")

  private def observedCluster(result: SchedulerQueryResult[ObservationBatch]): Option[ClusterName] =
    result match
      case SchedulerQueryResult.Succeeded(batch) =>
        batch.results.head match
          case ObservationResult.Observed(value) => value.reportedCluster
          case other                             => fail(s"unexpected observation result: $other")
      case other => fail(s"unexpected query result: $other")

  private def removeField(json: Json, name: String): Json =
    json.arrayOrObject(
      json,
      values => Json.fromValues(values.map(removeField(_, name))),
      value =>
        Json.fromJsonObject(
          JsonObject.fromIterable(
            value.toVector.collect {
              case (key, nested) if key != name => key -> removeField(nested, name)
            }
          )
        )
    )

  private def replaceField(json: Json, name: String, replacement: Json): Json =
    json.arrayOrObject(
      json,
      values => Json.fromValues(values.map(replaceField(_, name, replacement))),
      value =>
        Json.fromJsonObject(
          JsonObject.fromIterable(
            value.toVector.map { case (key, nested) =>
              key ->
                (if key == name then replacement
                 else replaceField(nested, name, replacement))
            }
          )
        )
    )

  private def resource(path: String): String =
    val stream = Option(getClass.getResourceAsStream(path)).get
    try String(stream.readAllBytes(), StandardCharsets.UTF_8)
    finally stream.close()
