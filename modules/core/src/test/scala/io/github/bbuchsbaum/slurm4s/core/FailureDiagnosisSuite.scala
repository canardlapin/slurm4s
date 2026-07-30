package io.github.bbuchsbaum.slurm4s.core

import java.nio.charset.StandardCharsets
import java.time.Instant

class FailureDiagnosisSuite extends munit.FunSuite:
  private val observedAt = Instant.parse("2026-07-22T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.SchedulerText("test"), observedAt, Vector(1, 2, 3))
  )
  private val attempt =
    AttemptId.from("diagnosis-attempt").fold(problem => fail(problem.toString), identity)
  private val job = JobRef(
    JobId.from("42").fold(problem => fail(problem.toString), identity),
    None,
    None
  )
  private val maximumItems =
    PositiveInt.from("maximumItems", 16).fold(problem => fail(problem.toString), identity)

  test("current accounting confirms OOM and a log hint remains only suspected") {
    val hints = recognize("No such file or directory", start = 100L)
    val accounting = AccountingRecord(
      job,
      SlurmState.OutOfMemory,
      Some(ExitStatus(0, Some(9))),
      Some(WorkloadOutcome.OutOfMemory),
      Freshness.Current(observedAt),
      Map.empty,
      evidence
    )
    val assessment = FailureDiagnosis.assess(
      input(accounting = Some(accounting), logHints = hints)
    )

    assessment match
      case Assessment.Confirmed(value, supporting) =>
        assertEquals(value.primary, FailureCause.OutOfMemory)
        assertEquals(value.suspectedContributors, Vector(FailureCause.FileNotFound))
        assert(supporting.toVector.exists(_.isInstanceOf[DiagnosisEvidence.Accounting]))
        assert(supporting.toVector.exists(_.isInstanceOf[DiagnosisEvidence.Log]))
      case other => fail(s"expected confirmed diagnosis, received $other")
  }

  test("common log recognition is suspected, byte-exact, and excerpt-bounded") {
    val bytes = "prefix Traceback (most recent call last): suffix"
    val hints = recognize(bytes, start = 200L, maximumExcerptBytes = 12)
    assertEquals(hints.map(_.kind), Vector(LogHintKind.PythonTraceback))
    val hint = hints.head
    assert(hint.excerpt.bytes.size <= 12)
    assertEquals(
      hint.excerpt.matched.start.value,
      200L + bytes.indexOf("Traceback").toLong
    )

    FailureDiagnosis.assess(input(logHints = hints)) match
      case Assessment.Suspected(candidates) =>
        assertEquals(candidates.head.value.primary, FailureCause.RuntimeError("python"))
      case other => fail(s"expected suspected diagnosis, received $other")
  }

  test("diagnostic ordering uses explicit stable cause keys, not source names or input order") {
    val hints =
      recognize("Traceback (most recent call last): No such file or directory", start = 0L)
    val forward = suspectedCauses(FailureDiagnosis.assess(input(logHints = hints)))
    val reverse = suspectedCauses(FailureDiagnosis.assess(input(logHints = hints.reverse)))

    assertEquals(forward, reverse)
    assertEquals(
      FailureCause.ProgramLaunchFailed(SpawnFailureKind.ExecutableMissing).deterministicKey,
      "program-launch-failed:executable-missing"
    )
    assertEquals(
      FailureCause.WorkerFailure("a:b").deterministicKey,
      "worker-failure:3:a:b"
    )
    assertEquals(
      Vector(
        LogHintKind.PythonTraceback,
        LogHintKind.RExecutionError,
        LogHintKind.JvmException,
        LogHintKind.FileNotFound,
        LogHintKind.MemoryPressure,
        LogHintKind.ProcessKilled
      ).map(kind => kind -> kind.stablePriority),
      Vector(
        LogHintKind.PythonTraceback -> 0,
        LogHintKind.RExecutionError -> 1,
        LogHintKind.JvmException -> 2,
        LogHintKind.FileNotFound -> 3,
        LogHintKind.MemoryPressure -> 4,
        LogHintKind.ProcessKilled -> 5
      )
    )
  }

  test("scheduler unavailability remains undetermined") {
    val diagnostics = Diagnostics.one(Diagnostic("squeue-unavailable", "temporary failure"))
    val observation =
      ObservationResult.Failed(
        job,
        Freshness.Unknown(observedAt, diagnostics),
        diagnostics,
        evidence
      )

    FailureDiagnosis.assess(input(observation = Some(observation))) match
      case Assessment.Undetermined(supporting) =>
        assert(supporting.exists(_.isInstanceOf[DiagnosisEvidence.Scheduler]))
        assert(supporting.exists(_.isInstanceOf[DiagnosisEvidence.Diagnostics]))
      case other => fail(s"expected undetermined diagnosis, received $other")
  }

  test("a stale terminal scheduler state is suspected rather than confirmed") {
    val age = DurationMillis.from(5000).fold(problem => fail(problem.toString), identity)
    val observation = ObservationResult.Observed(
      JobObservation(
        job,
        SlurmState.TimedOut,
        Freshness.Stale(observedAt, age),
        Some("TimeLimit"),
        Map.empty,
        evidence
      )
    )

    FailureDiagnosis.assess(input(observation = Some(observation))) match
      case Assessment.Suspected(candidates) =>
        assertEquals(candidates.head.value.primary, FailureCause.TimeLimitExceeded)
      case other => fail(s"expected suspected diagnosis, received $other")
  }

  test("strong conflicting planes retain a deterministic primary and contributor") {
    val accounting = AccountingRecord(
      job,
      SlurmState.OutOfMemory,
      None,
      Some(WorkloadOutcome.OutOfMemory),
      Freshness.Current(observedAt),
      Map.empty,
      evidence
    )
    val worker = WorkerEvent(
      1L,
      SubmissionKey.from("diagnosis").fold(problem => fail(problem.toString), identity),
      attempt,
      AttemptEpoch.initial,
      WorkloadOperation.Registered(
        OperationId.from("example").fold(problem => fail(problem.toString), identity),
        OperationVersion.from("1").fold(problem => fail(problem.toString), identity)
      ),
      WorkerRelease(
        WorkerReleaseId.from("worker-1").fold(problem => fail(problem.toString), identity),
        ContentDigest.from("sha256:worker").fold(problem => fail(problem.toString), identity)
      ),
      observedAt,
      WorkerEventPayload.Failed("python-error", "task raised")
    )

    FailureDiagnosis.assess(
      input(accounting = Some(accounting), workerEvents = Vector(worker))
    ) match
      case Assessment.Confirmed(value, _) =>
        assertEquals(value.primary, FailureCause.OutOfMemory)
        assertEquals(
          value.confirmedContributors,
          Vector(FailureCause.WorkerFailure("python-error"))
        )
      case other => fail(s"expected confirmed diagnosis, received $other")
  }

  test("acceptance uncertainty never becomes workload failure") {
    val submission = SubmissionAttempt.Completed(
      Submission.AcceptanceUnknown(AcceptanceUncertainty.ResponseLost, evidence)
    )

    FailureDiagnosis.assess(input(submission = Some(submission))) match
      case Assessment.Undetermined(supporting) =>
        assert(supporting.exists(_.isInstanceOf[DiagnosisEvidence.Submission]))
      case other => fail(s"expected undetermined diagnosis, received $other")
  }

  test("diagnosis input rejects more worker events and hints than its explicit bound") {
    val hint = recognize("Killed", start = 0L).head
    val one = PositiveInt.from("maximumItems", 1).fold(problem => fail(problem.toString), identity)
    val result = DiagnosisInput.from(
      logHints = Vector(hint, hint),
      maximumEventAndHintItems = one
    )
    assert(result.isLeft)
  }

  test("log recognition rejects an incoherent page cursor without throwing") {
    val bytes = "Traceback (most recent call last)".getBytes(StandardCharsets.UTF_8).toVector
    val invalidPage = LogPage(
      bytes,
      LogCursor(LogOffset.from(1L).fold(problem => fail(problem.toString), identity), None),
      endOfFile = false,
      observedAt
    )
    val maximumHints =
      PositiveInt.from("maximumHints", 8).fold(problem => fail(problem.toString), identity)
    val excerptBytes =
      ByteLimit.from(32).fold(problem => fail(problem.toString), identity)
    val ref = LogRef(attempt, AttemptEpoch.initial, LogStream.Stderr, "stderr.log")

    assert(
      CommonLogRecognizer
        .recognize(ref, invalidPage, LogRecognitionLimits(maximumHints, excerptBytes))
        .isLeft
    )
  }

  private def input(
      submission: Option[SubmissionAttempt] = None,
      observation: Option[ObservationResult] = None,
      accounting: Option[AccountingRecord] = None,
      workerEvents: Vector[WorkerEvent] = Vector.empty,
      result: Option[ExecutionResult[?]] = None,
      logHints: Vector[LogHint] = Vector.empty
  ): DiagnosisInput =
    DiagnosisInput
      .from(
        submission,
        observation,
        accounting,
        workerEvents,
        result,
        logHints,
        maximumItems
      )
      .fold(problem => fail(problem.toString), identity)

  private def suspectedCauses(
      assessment: Assessment[FailureDiagnosis]
  ): Vector[FailureCause] =
    assessment match
      case Assessment.Suspected(candidates) => candidates.toVector.map(_.value.primary)
      case other => fail(s"expected suspected diagnosis, received $other")

  private def recognize(
      text: String,
      start: Long,
      maximumExcerptBytes: Int = 32
  ): Vector[LogHint] =
    val bytes = text.getBytes(StandardCharsets.UTF_8).toVector
    val next = LogOffset
      .from(start + bytes.size.toLong)
      .fold(problem => fail(problem.toString), identity)
    val ref = LogRef(attempt, AttemptEpoch.initial, LogStream.Stderr, "stderr.log")
    val page = LogPage(bytes, LogCursor(next, None), endOfFile = true, observedAt)
    val maximumHints =
      PositiveInt.from("maximumHints", 8).fold(problem => fail(problem.toString), identity)
    val excerptBytes = ByteLimit
      .from(maximumExcerptBytes)
      .fold(problem => fail(problem.toString), identity)
    CommonLogRecognizer
      .recognize(
        ref,
        page,
        LogRecognitionLimits(maximumHints, excerptBytes)
      )
      .fold(problem => fail(problem.toString), identity)
