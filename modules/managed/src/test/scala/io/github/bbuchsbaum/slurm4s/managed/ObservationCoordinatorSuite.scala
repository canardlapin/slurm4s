package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

import scala.concurrent.duration.*

class ObservationCoordinatorSuite extends munit.CatsEffectSuite:
  import ManagedTestSupport.*

  test("one site tick coalesces active jobs and accounts only terminal candidates") {
    val firstJob = JobRef(JobId.from("8101").toOption.get, None, None)
    val secondJob = JobRef(JobId.from("8102").toOption.get, None, None)
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- bind(store, "coalesce-1", firstJob)
      _ <- bind(store, "coalesce-2", secondJob)
      observedBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accountingBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      focusedBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      scheduler = observingScheduler(
        observedBatches,
        accountingBatches,
        observationResult(firstJob, secondJob),
        accountingResult(secondJob)
      )
      focused = recordingFocused(focusedBatches)
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-a").toOption.get,
        store,
        scheduler,
        focused,
        Jitter.fixed[IO](0.5),
        policy(maximumBatch = 16)
      )
      tick <- coordinator.tick
      observed <- observedBatches.get
      accounted <- accountingBatches.get
      focusedValues <- focusedBatches.get
      snapshot <- store.snapshot
      second = snapshot.attempts(SubmissionKey.from("coalesce-2").toOption.get)
      _ = assertEquals(observed.map(_.toSet), Vector(Set(firstJob, secondJob)))
      _ = assertEquals(accounted, Vector(Vector(secondJob)))
      _ = assertEquals(focusedValues, Vector.empty)
      _ = assertEquals(tick.activeCandidates, 2)
      _ = assertEquals(tick.accountingCandidates, 1)
      _ = assert(second.isTerminal)
    yield ()
  }

  test("batch limit prevents a per-job loop and leaves excess work for later ticks") {
    val jobs =
      (1 to 5).toVector.map(index => JobRef(JobId.from(s"82$index").toOption.get, None, None))
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- jobs.zipWithIndex.traverse_ { case (job, index) =>
        bind(store, s"bounded-$index", job)
      }
      observedBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accountingBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      scheduler = observingScheduler(
        observedBatches,
        accountingBatches,
        SchedulerQueryResult.Succeeded(
          ObservationBatch(
            NonEmptyVector.fromVectorUnsafe(
              jobs.take(2).map(job => ObservationResult.Observed(running(job)))
            )
          )
        ),
        SchedulerQueryResult.Empty(later, evidence)
      )
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-b").toOption.get,
        store,
        scheduler,
        FocusedReconciler.noop[IO],
        Jitter.fixed[IO](0.5),
        policy(maximumBatch = 2)
      )
      tick <- coordinator.tick
      batches <- observedBatches.get
      _ = assertEquals(batches.size, 1)
      _ = assertEquals(batches.head.size, 2)
      _ = assertEquals(tick.activeCandidates, 2)
    yield ()
  }

  test("bounded batches rotate fairly instead of starving later jobs") {
    val jobs =
      (1 to 5).toVector.map(index => JobRef(JobId.from(s"824$index").toOption.get, None, None))
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- jobs.zipWithIndex.traverse_ { case (job, index) => bind(store, s"fair-$index", job) }
      observed <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accountingBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      scheduler = new Scheduler[IO]:
        def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
        def submit(spec: LaunchSpec): IO[SubmissionAttempt] = unused
        def observe(values: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] =
          observed.update(_ :+ values.toVector) *> IO.pure(
            SchedulerQueryResult.Succeeded(
              ObservationBatch(values.map(job => ObservationResult.Observed(running(job))))
            )
          )
        def accounting(values: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] =
          accountingBatches.update(_ :+ values.toVector) *> unused
        def cancel(job: JobRef): IO[CancellationAttempt] = unused
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-fair").toOption.get,
        store,
        scheduler,
        FocusedReconciler.noop[IO],
        Jitter.fixed[IO](0.5),
        policy(maximumBatch = 2)
      )
      _ <- coordinator.tick.replicateA(3)
      calls <- observed.get
      _ = assertEquals(calls.flatten.toSet, jobs.toSet)
      _ = assertEquals(calls.map(_.size), Vector(2, 2, 2))
    yield ()
  }

  test("a failed bounded query marks only the jobs that were actually requested") {
    val jobs =
      (1 to 3).toVector.map(index => JobRef(JobId.from(s"825$index").toOption.get, None, None))
    val timeout = InvocationResult.TimedOut(
      DurationMillis.from(1000L).toOption.get,
      evidence.primary,
      evidence.primary
    )
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- jobs.zipWithIndex.traverse_ { case (job, index) => bind(store, s"scoped-$index", job) }
      observed <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accounted <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-bounded-failure").toOption.get,
        store,
        observingScheduler(
          observed,
          accounted,
          SchedulerQueryResult.InvocationFailed(timeout),
          SchedulerQueryResult.Empty(later, evidence)
        ),
        FocusedReconciler.noop[IO],
        Jitter.fixed[IO](0.5),
        policy(maximumBatch = 2)
      )
      _ <- coordinator.tick
      snapshot <- store.snapshot
      statuses = snapshot.attempts.values.toVector.map(_.observation)
      _ = assertEquals(statuses.count(_.isInstanceOf[ManagedObservation.Unavailable]), 2)
      _ = assertEquals(statuses.count(_ == ManagedObservation.Unobserved), 1)
    yield ()
  }

  test("squeue failure marks last evidence stale and never calls accounting") {
    val boundJob = JobRef(JobId.from("8301").toOption.get, None, None)
    val timeout = InvocationResult.TimedOut(
      DurationMillis.from(1000L).toOption.get,
      evidence.primary,
      evidence.primary
    )
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- bind(store, "unavailable", boundJob)
      observedBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accountingBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      scheduler = observingScheduler(
        observedBatches,
        accountingBatches,
        SchedulerQueryResult.InvocationFailed(timeout),
        SchedulerQueryResult.Empty(later, evidence)
      )
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-c").toOption.get,
        store,
        scheduler,
        FocusedReconciler.noop[IO],
        Jitter.fixed[IO](0.5),
        policy(maximumBatch = 16)
      )
      _ <- coordinator.tick
      accountingCalls <- accountingBatches.get
      snapshot <- store.snapshot
      attempt = snapshot.attempts(SubmissionKey.from("unavailable").toOption.get)
      _ = assertEquals(accountingCalls, Vector.empty)
      _ = assert(attempt.observation.isInstanceOf[ManagedObservation.Unavailable])
      _ = assert(!attempt.isTerminal)
    yield ()
  }

  test("delayed accounting triggers one bounded focused batch and stays unresolved") {
    val boundJob = JobRef(JobId.from("8401").toOption.get, None, None)
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- bind(store, "focused", boundJob)
      observedBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accountingBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      focusedBatches <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      scheduler = observingScheduler(
        observedBatches,
        accountingBatches,
        SchedulerQueryResult.Succeeded(
          ObservationBatch(NonEmptyVector.one(ObservationResult.Observed(completed(boundJob))))
        ),
        SchedulerQueryResult.Empty(later, evidence)
      )
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-d").toOption.get,
        store,
        scheduler,
        recordingFocused(focusedBatches),
        Jitter.fixed[IO](0.5),
        policy(maximumBatch = 16)
      )
      tick <- coordinator.tick
      focused <- focusedBatches.get
      snapshot <- store.snapshot
      attempt = snapshot.attempts(SubmissionKey.from("focused").toOption.get)
      _ = assertEquals(focused, Vector(Vector(boundJob)))
      _ = assertEquals(tick.focusedCandidates, 1)
      _ = assert(!attempt.isTerminal)
    yield ()
  }

  test("adaptive cadence and jitter are deterministic dependencies") {
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      accounting <- Ref.of[IO, Vector[Vector[JobRef]]](Vector.empty)
      coordinator = ObservationCoordinator[IO](
        SiteId.from("site-e").toOption.get,
        store,
        observingScheduler(
          calls,
          accounting,
          SchedulerQueryResult.Empty(later, evidence),
          SchedulerQueryResult.Empty(later, evidence)
        ),
        FocusedReconciler.noop[IO],
        Jitter.fixed[IO](0.0),
        policy(maximumBatch = 16)
      )
      busy <- coordinator.nextDelay(ObserverTick(SiteId.from("site-e").toOption.get, 1, 1, 0, 0))
      idle <- coordinator.nextDelay(ObserverTick(SiteId.from("site-e").toOption.get, 0, 0, 0, 0))
      _ = assertEquals(busy, 80.millis)
      _ = assertEquals(idle, 800.millis)
    yield ()
  }

  test("site registry permits exactly one live observation loop per site") {
    val site = SiteId.from("single-observer").toOption.get
    for
      registry <- SiteObserverRegistry.create[IO]
      duplicate <- registry.lease(site).use { _ =>
        registry.lease(site).use(_ => IO.unit).attempt
      }
      afterRelease <- registry.lease(site).use(_ => IO.pure("acquired"))
      _ = assert(duplicate.isLeft)
      _ = assertEquals(afterRelease, "acquired")
    yield ()
  }

  private def bind(
      store: ControlStore[IO],
      keyText: String,
      binding: JobRef
  ): IO[Unit] =
    val value = intent(keyText)
    val accepted = SubmissionAttempt.Completed(Submission.Accepted(binding, evidence))
    for
      _ <- store.transact(ControlCommand.RecordIntent(value))
      _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
      _ <- store.transact(
        ControlCommand.RecordSubmission(value.submissionKey, value.epoch, accepted, later)
      )
    yield ()

  private def observationResult(
      runningJob: JobRef,
      completedJob: JobRef
  ): SchedulerQueryResult[ObservationBatch] =
    SchedulerQueryResult.Succeeded(
      ObservationBatch(
        NonEmptyVector.of(
          ObservationResult.Observed(running(runningJob)),
          ObservationResult.Observed(completed(completedJob))
        )
      )
    )

  private def accountingResult(job: JobRef): SchedulerQueryResult[AccountingBatch] =
    SchedulerQueryResult.Succeeded(
      AccountingBatch(
        NonEmptyVector.one(
          AccountingRecord(
            job,
            SlurmState.OutOfMemory,
            Some(ExitStatus(0, Some(9))),
            Some(WorkloadOutcome.OutOfMemory),
            Freshness.Current(later),
            Map.empty,
            evidence
          )
        ),
        Vector.empty
      )
    )

  private def running(job: JobRef): JobObservation =
    JobObservation(
      job,
      SlurmState.Running,
      Freshness.Current(later),
      None,
      Map.empty,
      evidence
    )

  private def completed(job: JobRef): JobObservation =
    running(job).copy(state = SlurmState.Completed)

  private def observingScheduler(
      observationCalls: Ref[IO, Vector[Vector[JobRef]]],
      accountingCalls: Ref[IO, Vector[Vector[JobRef]]],
      observationResult: SchedulerQueryResult[ObservationBatch],
      accountingResult: SchedulerQueryResult[AccountingBatch]
  ): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] = unused
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] =
      observationCalls.update(_ :+ jobs.toVector) *> IO.pure(observationResult)
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] =
      accountingCalls.update(_ :+ jobs.toVector) *> IO.pure(accountingResult)
    def cancel(job: JobRef): IO[CancellationAttempt] = unused

  private def recordingFocused(
      values: Ref[IO, Vector[Vector[JobRef]]]
  ): FocusedReconciler[IO] = new FocusedReconciler[IO]:
    def inspect(jobs: NonEmptyVector[JobRef]): IO[Unit] = values.update(_ :+ jobs.toVector)

  private def policy(maximumBatch: Int): ObservationPolicy =
    ObservationPolicy(maximumBatch, 100.millis, 1.second, 0.2, focusedProbeLimit = 2)

  private def unused[A]: IO[A] = IO.raiseError(new AssertionError("unexpected scheduler call"))
