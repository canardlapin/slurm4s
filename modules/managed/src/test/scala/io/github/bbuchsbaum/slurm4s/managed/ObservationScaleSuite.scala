package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.slurm4s.core.*

import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.*

class ObservationScaleSuite extends munit.CatsEffectSuite:
  private val jobCount = 4096
  private val batchSize = 256
  private val observedAt = Instant.parse("2026-07-22T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.DurableJournal, observedAt, ByteVector.empty)
  )

  test("4096 active jobs use bounded coalesced controller calls with complete fair coverage") {
    val attempts = (0 until jobCount).toVector.map(attempt)
    val initial = ControlState(
      StoreRevision.initial,
      attempts.map(value => value.intent.submissionKey -> value).toMap,
      Map.empty,
      Vector.empty
    )
    val expectedCalls = (jobCount + batchSize - 1) / batchSize
    for
      store <- InMemoryControlStore.create[IO](initial)
      observedCalls <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accountingCalls <- Ref.of[IO, Int](0)
      scheduler = scaleScheduler(observedCalls, accountingCalls)
      coordinator = ObservationCoordinator[IO](
        SiteId.from("scale-site").toOption.get,
        store,
        scheduler,
        FocusedReconciler.noop[IO],
        Jitter.fixed[IO](0.5),
        ObservationPolicy(batchSize, 1.second, 5.seconds, 0.0, focusedProbeLimit = 8)
      )
      _ <- coordinator.tick.replicateA(expectedCalls)
      calls <- observedCalls.get
      accounting <- accountingCalls.get
      expectedJobs = attempts.flatMap(_.currentJob).toSet
      _ = assertEquals(calls.size, expectedCalls)
      _ = assert(calls.forall(_.size <= batchSize))
      _ = assertEquals(calls.flatten.toSet, expectedJobs)
      _ = assertEquals(accounting, 0)
    yield ()
  }

  private def attempt(index: Int): ManagedAttempt =
    val key = SubmissionKey.from(f"scale-$index%05d").toOption.get
    val request = LaunchSpec(
      key,
      JobName.from("scale-observer").toOption.get,
      ScriptSource.unsafeInlineScript("scale.sh", ByteVector.view("true\n".getBytes("UTF-8"))),
      Vector.empty,
      ResultContract.ExitOnly.descriptor,
      ResourceRequest.validate(1, 1, None, None, None).toOption.get
    )
    val intent = ManagedIntent.from(request, observedAt.minusSeconds(1)).toOption.get
    val job = JobRef(JobId.from((100000 + index).toString).toOption.get, None)
    ManagedAttempt(
      intent,
      ManagedPhase.Bound(job),
      Vector(BindingRecord(AttemptEpoch.initial, job, observedAt.minusSeconds(1), evidence, false)),
      ManagedObservation.Unobserved,
      ManagedAccounting.Unobserved,
      ManagedCancellation.NotRequested,
      observedAt.minusSeconds(1)
    )

  private def scaleScheduler(
      observedCalls: Ref[IO, Vector[Vector[JobRef]]],
      accountingCalls: Ref[IO, Int]
  ): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unexpected
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] = unexpected
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] =
      observedCalls.update(_ :+ jobs.toVector) *> IO.pure(
        SchedulerQueryResult.Succeeded(
          ObservationBatch(
            jobs.map(job =>
              ObservationResult.Observed(
                JobObservation(
                  job,
                  SlurmState.Running,
                  Freshness.Current(observedAt),
                  None,
                  Map.empty,
                  evidence
                )
              )
            )
          )
        )
      )
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] =
      accountingCalls.update(_ + 1) *> unexpected
    def cancel(job: JobRef): IO[CancellationAttempt] = unexpected

  private def unexpected[A]: IO[A] =
    IO.raiseError(new AssertionError("unexpected scheduler call"))
