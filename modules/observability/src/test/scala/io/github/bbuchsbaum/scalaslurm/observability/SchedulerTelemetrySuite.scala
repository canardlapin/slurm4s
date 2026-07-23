package io.github.bbuchsbaum.scalaslurm.observability

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.scalaslurm.core.*

import java.time.Instant
import scala.concurrent.duration.*

class SchedulerTelemetrySuite extends munit.CatsEffectSuite:
  private val observedAt = Instant.parse("2026-07-22T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.SchedulerText("test"), observedAt, Vector(1, 2, 3))
  )
  private val job = JobRef(
    JobId.from("42").fold(problem => fail(problem.toString), identity),
    None,
    None
  )
  private val request = JobRequest(
    SubmissionKey.from("telemetry-submit").fold(problem => fail(problem.toString), identity),
    JobName.from("telemetry").fold(problem => fail(problem.toString), identity),
    Payload.Script(
      ScriptSource.ExistingRemote("/work/job.sh"),
      Vector.empty,
      ResultContract.ExitOnly
    ),
    ResourceRequest
      .validate(1, 1, None, None, None)
      .toEither
      .fold(problem => fail(problem.toString), identity)
  )
  private val shortPolicy = SchedulerTelemetryPolicy(
    DurationMillis.from(10L).fold(problem => fail(problem.toString), identity),
    PositiveInt.from("maximumDiagnosticCodes", 2).fold(problem => fail(problem.toString), identity)
  )

  test("submission rejection remains a domain value and records only bounded codes") {
    val diagnostics = Diagnostics
      .fromVector(
        Vector(
          Diagnostic("z-code", "contains details"),
          Diagnostic("a-code", "contains details"),
          Diagnostic("third-code", "contains details")
        )
      )
      .fold(problem => fail(problem.toString), identity)
    val rejected = SubmissionAttempt.Completed(Submission.Rejected(diagnostics, evidence))

    for
      events <- Ref.of[IO, Vector[SchedulerOperationEvent]](Vector.empty)
      wrapper = TelemetryScheduler[IO](
        fixedScheduler(submission = IO.pure(rejected)),
        collecting(events),
        shortPolicy
      )
      result <- wrapper.submit(request)
      recorded <- events.get
    yield
      assertEquals(result, rejected)
      assertEquals(recorded.map(_.outcome), Vector(OperationalOutcome.Rejected))
      assertEquals(recorded.head.diagnosticCodes, Vector("a-code", "third-code"))
      assertEquals(
        recorded.head.subject,
        OperationalSubject.Submission(request.submissionKey)
      )
  }

  test("parse failure is not flattened into effect failure") {
    val diagnostics = Diagnostics.one(Diagnostic("invalid-json", "raw content omitted"))
    val parseFailure: SchedulerQueryResult[SchedulerCapabilities] =
      SchedulerQueryResult.ParseFailed(diagnostics, evidence)

    for
      events <- Ref.of[IO, Vector[SchedulerOperationEvent]](Vector.empty)
      wrapper = TelemetryScheduler[IO](
        fixedScheduler(capabilityResult = IO.pure(parseFailure)),
        collecting(events),
        shortPolicy
      )
      result <- wrapper.capabilities
      recorded <- events.get
    yield
      assertEquals(result, parseFailure)
      assertEquals(recorded.head.outcome, OperationalOutcome.ParseFailed)
      assertEquals(recorded.head.diagnosticCodes, Vector("invalid-json"))
  }

  test("a failing telemetry sink cannot fail a successful scheduler call") {
    val accepted = SubmissionAttempt.Completed(Submission.Accepted(job, evidence))
    val sink = new SchedulerTelemetrySink[IO]:
      def record(event: SchedulerOperationEvent): IO[Unit] =
        IO.raiseError(new RuntimeException("telemetry unavailable"))

    TelemetryScheduler[IO](
      fixedScheduler(submission = IO.pure(accepted)),
      sink,
      shortPolicy
    ).submit(request).map(result => assertEquals(result, accepted))
  }

  test("a nonresponsive telemetry sink is bounded by policy") {
    val accepted = SubmissionAttempt.Completed(Submission.Accepted(job, evidence))
    val sink = new SchedulerTelemetrySink[IO]:
      def record(event: SchedulerOperationEvent): IO[Unit] = IO.never

    TelemetryScheduler[IO](
      fixedScheduler(submission = IO.pure(accepted)),
      sink,
      shortPolicy
    ).submit(request).timeout(1.second).map(result => assertEquals(result, accepted))
  }

  test("scheduler effect failure is recorded by class and then re-raised") {
    val failure = new IllegalStateException("private effect detail")

    for
      events <- Ref.of[IO, Vector[SchedulerOperationEvent]](Vector.empty)
      wrapper = TelemetryScheduler[IO](
        fixedScheduler(submission = IO.raiseError(failure)),
        collecting(events),
        shortPolicy
      )
      result <- wrapper.submit(request).attempt
      recorded <- events.get
    yield
      assertEquals(result, Left(failure))
      assertEquals(
        recorded.head.outcome,
        OperationalOutcome.EffectFailed(classOf[IllegalStateException].getName)
      )
      assertEquals(recorded.head.diagnosticCodes, Vector.empty)
  }

  test("cancellation uncertainty remains distinct and job-correlated") {
    val diagnostics = Diagnostics.one(Diagnostic("cancel-unknown", "response lost"))
    val cancellation = CancellationAttempt.Completed(
      CancellationResult.Unknown(diagnostics, evidence)
    )

    for
      events <- Ref.of[IO, Vector[SchedulerOperationEvent]](Vector.empty)
      wrapper = TelemetryScheduler[IO](
        fixedScheduler(cancellation = IO.pure(cancellation)),
        collecting(events),
        shortPolicy
      )
      result <- wrapper.cancel(job)
      recorded <- events.get
    yield
      assertEquals(result, cancellation)
      assertEquals(recorded.head.outcome, OperationalOutcome.CancellationUnknown)
      assertEquals(recorded.head.subject, OperationalSubject.Job(job.jobId, None))
  }

  private def collecting(
      target: Ref[IO, Vector[SchedulerOperationEvent]]
  ): SchedulerTelemetrySink[IO] =
    new SchedulerTelemetrySink[IO]:
      def record(event: SchedulerOperationEvent): IO[Unit] =
        target.update(_ :+ event)

  private def fixedScheduler(
      submission: IO[SubmissionAttempt] = IO.raiseError(new AssertionError("unexpected submit")),
      capabilityResult: IO[SchedulerQueryResult[SchedulerCapabilities]] =
        IO.raiseError(new AssertionError("unexpected capabilities")),
      cancellation: IO[CancellationAttempt] =
        IO.raiseError(new AssertionError("unexpected cancellation"))
  ): Scheduler[IO] =
    new Scheduler[IO]:
      def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = capabilityResult
      def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] = submission
      def observe(
          jobs: NonEmptyVector[JobRef]
      ): IO[SchedulerQueryResult[ObservationBatch]] =
        IO.raiseError(new AssertionError(s"unexpected observation: $jobs"))
      def accounting(
          jobs: NonEmptyVector[JobRef]
      ): IO[SchedulerQueryResult[AccountingBatch]] =
        IO.raiseError(new AssertionError(s"unexpected accounting: $jobs"))
      def cancel(job: JobRef): IO[CancellationAttempt] = cancellation
