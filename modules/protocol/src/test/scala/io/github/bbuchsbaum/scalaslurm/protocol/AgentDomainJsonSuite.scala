package io.github.bbuchsbaum.scalaslurm.protocol

import cats.data.NonEmptyVector
import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.scalaslurm.core.*

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
    val request = JobRequest(
      SubmissionKey.from("wire-array").toOption.get,
      JobName.from("wire-array").toOption.get,
      Payload.Script(
        ScriptSource.ExistingRemote("/work/array.sh"),
        Vector.empty,
        ResultContract.ExitOnly
      ),
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

  test("observation timing has an owned golden wire shape and round-trips") {
    val observedAt = Instant.parse("2026-07-23T12:00:00Z")
    val evidence = BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      observedAt,
      "queue-evidence".getBytes(StandardCharsets.UTF_8).toVector
    )
    val observation = JobObservation(
      job = JobRef(JobId.from("2001").toOption.get, None, None),
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

  test("legacy observations without timing decode conservatively") {
    val observedAt = Instant.parse("2026-07-23T12:00:00Z")
    val evidence = BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      observedAt,
      Vector.empty
    )
    val observation = JobObservation(
      JobRef(JobId.from("2002").toOption.get, None, None),
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

  test("Slurm state wire codes are explicit, exhaustive, and legacy-readable") {
    val states = Vector(
      SlurmState.Pending,
      SlurmState.Running,
      SlurmState.Completing,
      SlurmState.Completed,
      SlurmState.Failed,
      SlurmState.Cancelled,
      SlurmState.OutOfMemory,
      SlurmState.TimedOut,
      SlurmState.NodeFailure,
      SlurmState.Preempted,
      SlurmState.Requeued,
      SlurmState.RequeueHeld,
      SlurmState.RequeueFederation,
      SlurmState.SpecialExit,
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
      Vector.empty
    )
    AgentDomainJson.encodeObservation(
      SchedulerQueryResult.Succeeded(
        ObservationBatch(
          NonEmptyVector.one(
            ObservationResult.Observed(
              JobObservation(
                JobRef(JobId.from("state-fixture").toOption.get, None, None),
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
