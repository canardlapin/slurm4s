package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import cats.effect.Deferred
import cats.effect.Fiber
import cats.effect.IO
import cats.effect.Outcome
import cats.effect.Ref
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

import scala.concurrent.duration.*

class ManagedControllerSuite extends munit.CatsEffectSuite:
  import ManagedTestSupport.*

  test("submission is persisted and digest-checked before the scheduler is invoked") {
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Int](0)
      scheduler = schedulerWithSubmit(calls, IO.pure(accepted))
      controller = ManagedController[IO](store, scheduler)
      created <- controller.submit(request("managed-key"))
      repeated <- controller.submit(request("managed-key"))
      conflict <- controller.submit(request("managed-key", "false"))
      before <- calls.get
      first <- controller.dispatchSubmission(SubmissionKey.from("managed-key").toOption.get)
      second <- controller.dispatchSubmission(SubmissionKey.from("managed-key").toOption.get)
      after <- calls.get
      _ = assert(created.isInstanceOf[ManagedSubmitResult.Created])
      _ = assert(repeated.isInstanceOf[ManagedSubmitResult.Existing])
      _ = assert(conflict.isInstanceOf[ManagedSubmitResult.Conflict])
      _ = assertEquals(before, 0)
      _ = assert(first.exists(_.phase.isInstanceOf[ManagedPhase.Bound]))
      _ = assert(second.left.exists(_.isInstanceOf[ControlFailure.InvalidPhase]))
      _ = assertEquals(after, 1)
    yield ()
  }

  test("inspect and observeSubmission return the committed durable projection") {
    val running = JobObservation(
      job,
      SlurmState.Running,
      Freshness.Current(later),
      None,
      Map("state" -> "RUNNING"),
      evidence
    )
    for
      store <- InMemoryControlStore.create[IO]()
      submitCalls <- Ref.of[IO, Int](0)
      scheduler = new Scheduler[IO]:
        def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
        def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
          submitCalls.update(_ + 1) *> IO.pure(accepted)
        def observe(
            jobs: NonEmptyVector[JobRef]
        ): IO[SchedulerQueryResult[ObservationBatch]] =
          IO.pure(
            SchedulerQueryResult.Succeeded(
              ObservationBatch(NonEmptyVector.one(ObservationResult.Observed(running)))
            )
          )
        def accounting(
            jobs: NonEmptyVector[JobRef]
        ): IO[SchedulerQueryResult[AccountingBatch]] = unused
        def cancel(job: JobRef): IO[CancellationAttempt] = unused
      controller = ManagedController[IO](store, scheduler)
      key = SubmissionKey.from("managed-observe").toOption.get
      _ <- controller.submit(request("managed-observe"))
      _ <- controller.dispatchSubmission(key)
      before <- controller.inspect(key)
      observed <- controller.observeSubmission(key)
      after <- controller.inspect(key)
      _ = assert(before.exists(_.observation == ManagedObservation.Unobserved))
      _ = assert(
        observed.exists(
          _.observation == ManagedObservation.Current(ObservationResult.Observed(running))
        )
      )
      _ = assertEquals(after, observed.toOption)
    yield ()
  }

  test("lost acceptance response never causes an automatic or repeated submit") {
    val unknown = SubmissionAttempt.Completed(
      Submission.AcceptanceUnknown(AcceptanceUncertainty.ResponseLost, evidence)
    )
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(unknown)))
      _ <- controller.submit(request("unknown-key"))
      first <- controller.dispatchSubmission(SubmissionKey.from("unknown-key").toOption.get)
      second <- controller.dispatchSubmission(SubmissionKey.from("unknown-key").toOption.get)
      count <- calls.get
      _ = assert(first.exists(_.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown]))
      _ = assert(second.isLeft)
      _ = assertEquals(count, 1)
    yield ()
  }

  test("explicit retry queues a new epoch without dispatching sbatch implicitly") {
    val unavailable = SubmissionAttempt.InvocationFailed(
      InvocationResult.SpawnFailed(
        SpawnFailureKind.ExecutableMissing,
        Diagnostics.one(Diagnostic("sbatch-missing", "sbatch was not found")),
        evidence
      )
    )
    val key = SubmissionKey.from("explicit-retry").toOption.get
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(unavailable)))
      _ <- controller.submit(request("explicit-retry"))
      first <- controller.dispatchSubmission(key)
      retried <- controller.retrySubmission(
        key,
        AttemptEpoch.initial,
        RetryAuthorization.Automatic(
          RetryReason.from("sbatch executable was restored").toOption.get
        )
      )
      count <- calls.get
      snapshot <- store.snapshot
      _ = assert(first.exists(_.phase.isInstanceOf[ManagedPhase.SubmissionUnavailable]))
      _ = assert(retried.exists(_.intent.epoch.value == 2L))
      _ = assertEquals(count, 1)
      _ = assert(
        snapshot.outbox.values.exists(
          _.action == OutboxAction.Submit(key, AttemptEpoch.from(2L).toOption.get)
        )
      )
    yield ()
  }

  test("concurrent dispatchers share one durable claim and invoke sbatch once") {
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      _ <- controller.submit(request("concurrent-key"))
      key = SubmissionKey.from("concurrent-key").toOption.get
      _ <- (controller.dispatchSubmission(key), controller.dispatchSubmission(key)).parTupled
      count <- calls.get
      _ = assertEquals(count, 1)
    yield ()
  }

  test("an effect failure after the durable claim is recorded as acceptance unknown") {
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Int](0)
      scheduler = schedulerWithSubmit(
        calls,
        IO.raiseError(new RuntimeException("connection disappeared"))
      )
      controller = ManagedController[IO](store, scheduler)
      _ <- controller.submit(request("effect-loss"))
      result <- controller
        .dispatchSubmission(SubmissionKey.from("effect-loss").toOption.get)
        .attempt
      snapshot <- store.snapshot
      _ = assert(result.isLeft)
      _ = assert(snapshot.attempts.values.head.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown])
      _ = assert(snapshot.outbox.values.head.status.isInstanceOf[OutboxStatus.Uncertain])
    yield ()
  }

  test("cancellation after a submission claim recovers before invoking sbatch") {
    for
      underlying <- InMemoryControlStore.create[IO]()
      reached <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      store = pausingStore(
        underlying,
        {
          case ControlCommand.ClaimSubmission(_, _) => true
          case _                                    => false
        },
        before = false,
        reached,
        release
      )
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      _ <- controller.submit(request("cancel-before-sbatch"))
      dispatch <- controller
        .dispatchSubmission(SubmissionKey.from("cancel-before-sbatch").toOption.get)
        .start
      _ <- reached.get
      outcome <- cancelAndRelease(dispatch, release)
      snapshot <- underlying.snapshot
      _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, ?]])
      _ = assert(snapshot.attempts.values.head.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown])
      _ = assert(snapshot.outbox.values.head.status.isInstanceOf[OutboxStatus.Uncertain])
      count <- calls.get
      _ = assertEquals(count, 0)
    yield ()
  }

  test("cancellation during sbatch recovers the claim as acceptance unknown") {
    for
      store <- InMemoryControlStore.create[IO]()
      calls <- Ref.of[IO, Int](0)
      invoked <- Deferred[IO, Unit]
      result <- Deferred[IO, SubmissionAttempt]
      scheduler = schedulerWithSubmit(calls, invoked.complete(()).void *> result.get)
      controller = ManagedController[IO](store, scheduler)
      _ <- controller.submit(request("cancel-during-sbatch"))
      dispatch <- controller
        .dispatchSubmission(SubmissionKey.from("cancel-during-sbatch").toOption.get)
        .start
      _ <- invoked.get
      _ <- dispatch.cancel
      outcome <- dispatch.join
      snapshot <- store.snapshot
      _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, ?]])
      _ = assert(snapshot.attempts.values.head.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown])
      _ = assert(snapshot.outbox.values.head.status.isInstanceOf[OutboxStatus.Uncertain])
      count <- calls.get
      _ = assertEquals(count, 1)
    yield ()
  }

  test("cancellation after the sbatch result cannot prevent binding persistence") {
    for
      underlying <- InMemoryControlStore.create[IO]()
      reached <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      store = pausingStore(
        underlying,
        {
          case ControlCommand.RecordSubmission(_, _, _, _) => true
          case _                                           => false
        },
        before = true,
        reached,
        release
      )
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      _ <- controller.submit(request("cancel-after-sbatch"))
      dispatch <- controller
        .dispatchSubmission(SubmissionKey.from("cancel-after-sbatch").toOption.get)
        .start
      _ <- reached.get
      outcome <- cancelAndRelease(dispatch, release)
      snapshot <- underlying.snapshot
      _ = assert(outcome.isInstanceOf[Outcome.Succeeded[IO, Throwable, ?]])
      _ = assert(snapshot.attempts.values.head.phase.isInstanceOf[ManagedPhase.Bound])
      _ = assert(snapshot.outbox.values.head.status.isInstanceOf[OutboxStatus.Completed])
      count <- calls.get
      _ = assertEquals(count, 1)
    yield ()
  }

  test("a restarted controller recovers a persisted in-flight claim without invoking sbatch") {
    val value = intent("restart-claim")
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- store.transact(ControlCommand.RecordIntent(value))
      _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
      calls <- Ref.of[IO, Int](0)
      restarted = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      recovered <- restarted.recoverInFlight()
      count <- calls.get
      _ = assert(recovered.head.exists(_.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown]))
      _ = assertEquals(count, 0)
    yield ()
  }

  test("a rejected submission record recovers the claim rather than stranding it") {
    for
      underlying <- InMemoryControlStore.create[IO]()
      store = rejectingStore(
        underlying,
        {
          case ControlCommand.RecordSubmission(_, _, _, _) => true
          case _                                           => false
        }
      )
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      key = SubmissionKey.from("record-rejected").toOption.get
      _ <- controller.submit(request("record-rejected"))
      dispatched <- controller.dispatchSubmission(key)
      count <- calls.get
      snapshot <- underlying.snapshot
      // sbatch ran, so the claim cannot stay in Submitting: a typed rejection of the record is
      // still an effect that ended without a persisted result, and the attempt must say so.
      _ = assertEquals(count, 1)
      _ = assert(dispatched.isLeft)
      _ = assert(
        snapshot.attempts(key).phase.isInstanceOf[ManagedPhase.AcceptanceUnknown],
        s"expected AcceptanceUnknown, found ${snapshot.attempts(key).phase}"
      )
      _ = assert(snapshot.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Uncertain]))
    yield ()
  }

  test("cancellation after a cancellation claim recovers before invoking scancel") {
    for
      underlying <- InMemoryControlStore.create[IO]()
      key <- prepareCancellation(underlying, "cancel-before-scancel")
      reached <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      store = pausingStore(
        underlying,
        {
          case ControlCommand.ClaimCancellation(_, _) => true
          case _                                      => false
        },
        before = false,
        reached,
        release
      )
      submitCalls <- Ref.of[IO, Int](0)
      cancelCalls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithCancel(submitCalls, cancelCalls))
      dispatch <- controller.dispatchCancellation(key).start
      _ <- reached.get
      outcome <- cancelAndRelease(dispatch, release)
      snapshot <- underlying.snapshot
      _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, ?]])
      _ =
        assert(
          snapshot.attempts(key).cancellation.isInstanceOf[ManagedCancellation.Unknown]
        )
      _ = assert(snapshot.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Uncertain]))
      count <- cancelCalls.get
      _ = assertEquals(count, 0)
    yield ()
  }

  test("a scancel effect failure records cancellation as unknown immediately") {
    for
      store <- InMemoryControlStore.create[IO]()
      key <- prepareCancellation(store, "scancel-failure")
      submitCalls <- Ref.of[IO, Int](0)
      cancelCalls <- Ref.of[IO, Int](0)
      scheduler = schedulerWithCancel(
        submitCalls,
        cancelCalls,
        IO.raiseError(new RuntimeException("ssh transport disappeared"))
      )
      controller = ManagedController[IO](store, scheduler)
      result <- controller.dispatchCancellation(key).attempt
      snapshot <- store.snapshot
      _ = assert(result.isLeft)
      _ =
        assert(
          snapshot.attempts(key).cancellation.isInstanceOf[ManagedCancellation.Unknown]
        )
      _ = assert(snapshot.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Uncertain]))
      count <- cancelCalls.get
      _ = assertEquals(count, 1)
    yield ()
  }

  test("cancellation during scancel recovers dispatch as unknown") {
    for
      store <- InMemoryControlStore.create[IO]()
      key <- prepareCancellation(store, "cancel-during-scancel")
      submitCalls <- Ref.of[IO, Int](0)
      cancelCalls <- Ref.of[IO, Int](0)
      invoked <- Deferred[IO, Unit]
      result <- Deferred[IO, CancellationAttempt]
      scheduler = schedulerWithCancel(
        submitCalls,
        cancelCalls,
        invoked.complete(()).void *> result.get
      )
      controller = ManagedController[IO](store, scheduler)
      dispatch <- controller.dispatchCancellation(key).start
      _ <- invoked.get
      _ <- dispatch.cancel
      outcome <- dispatch.join
      snapshot <- store.snapshot
      _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, ?]])
      _ =
        assert(
          snapshot.attempts(key).cancellation.isInstanceOf[ManagedCancellation.Unknown]
        )
      _ = assert(snapshot.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Uncertain]))
      count <- cancelCalls.get
      _ = assertEquals(count, 1)
    yield ()
  }

  test("cancellation after the scancel result cannot suppress its durable acknowledgement") {
    for
      underlying <- InMemoryControlStore.create[IO]()
      key <- prepareCancellation(underlying, "cancel-after-scancel")
      reached <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      store = pausingStore(
        underlying,
        {
          case ControlCommand.RecordCancellation(_, _, _) => true
          case _                                          => false
        },
        before = true,
        reached,
        release
      )
      submitCalls <- Ref.of[IO, Int](0)
      cancelCalls <- Ref.of[IO, Int](0)
      scheduler = schedulerWithCancel(submitCalls, cancelCalls, acknowledgedCancellation)
      controller = ManagedController[IO](store, scheduler)
      dispatch <- controller.dispatchCancellation(key).start
      _ <- reached.get
      outcome <- cancelAndRelease(dispatch, release)
      snapshot <- underlying.snapshot
      _ = assert(outcome.isInstanceOf[Outcome.Succeeded[IO, Throwable, ?]])
      _ =
        assert(
          snapshot.attempts(key).cancellation.isInstanceOf[ManagedCancellation.Acknowledged]
        )
      _ = assert(snapshot.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Completed]))
      count <- cancelCalls.get
      _ = assertEquals(count, 1)
    yield ()
  }

  test("restart makes an in-flight cancellation unknown without issuing scancel again") {
    val value = intent("cancel-restart")
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- store.transact(ControlCommand.RecordIntent(value))
      _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
      _ <- store.transact(
        ControlCommand.RecordSubmission(value.submissionKey, value.epoch, accepted, later)
      )
      _ <- store.transact(ControlCommand.RequestCancellation(value.submissionKey, later))
      _ <- store.transact(ControlCommand.ClaimCancellation(value.submissionKey, later))
      submitCalls <- Ref.of[IO, Int](0)
      cancelCalls <- Ref.of[IO, Int](0)
      scheduler = schedulerWithCancel(submitCalls, cancelCalls)
      restarted = ManagedController[IO](store, scheduler)
      recovered <- restarted.recoverInFlight()
      count <- cancelCalls.get
      snapshot <- store.snapshot
      _ = assert(recovered.head.exists(_.cancellation.isInstanceOf[ManagedCancellation.Unknown]))
      _ = assertEquals(count, 0)
      _ = assert(snapshot.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Uncertain]))
    yield ()
  }

  test("acceptance search may bind a unique candidate but never retries no-match or ambiguity") {
    val value = intent("search-key")
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- store.transact(ControlCommand.RecordIntent(value))
      _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
      _ <- store.transact(
        ControlCommand.RecoverSubmissionClaim(value.submissionKey, value.epoch, evidence, later)
      )
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      search = new AcceptanceSearch[IO]:
        def find(intent: ManagedIntent): IO[AcceptanceSearchResult] =
          IO.pure(AcceptanceSearchResult.Unique(job, evidence))
      found <- controller.reconcileUnknown(search, 10)
      snapshot <- store.snapshot
      count <- calls.get
      _ = assertEquals(found.map(_._2).head, AcceptanceSearchResult.Unique(job, evidence))
      _ = assert(snapshot.attempts(value.submissionKey).phase.isInstanceOf[ManagedPhase.Bound])
      _ = assertEquals(count, 0)
    yield ()
  }

  test("a slow event consumer cannot block a durable transition") {
    val value = intent("slow-consumer")
    for
      store <- InMemoryControlStore.create[IO]()
      _ <- store.transact(ControlCommand.RecordIntent(value))
      calls <- Ref.of[IO, Int](0)
      controller = ManagedController[IO](store, schedulerWithSubmit(calls, IO.pure(accepted)))
      received <- Deferred[IO, Unit]
      consumer <- controller
        .eventStream(EventCursor.origin, 1, 10.millis)
        .evalMap(_ => received.complete(()).void *> IO.sleep(5.seconds))
        .compile
        .drain
        .start
      _ <- received.get
      result <- IO.race(
        IO.sleep(500.millis),
        store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
      )
      _ <- consumer.cancel
      _ = assert(result.isRight)
    yield ()
  }

  private def schedulerWithSubmit(
      calls: Ref[IO, Int],
      result: IO[SubmissionAttempt]
  ): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] = calls.update(_ + 1) *> result
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] = unused

  private def schedulerWithCancel(
      submitCalls: Ref[IO, Int],
      cancelCalls: Ref[IO, Int],
      result: IO[CancellationAttempt] = acknowledgedCancellation
  ): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
      submitCalls.update(_ + 1) *> IO.pure(accepted)
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] =
      cancelCalls.update(_ + 1) *> result

  private def acknowledgedCancellation: IO[CancellationAttempt] =
    IO.pure(CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)))

  private def prepareCancellation(
      store: ControlStore[IO],
      rawKey: String
  ): IO[SubmissionKey] =
    val value = intent(rawKey)
    store.transact(ControlCommand.RecordIntent(value)) *>
      store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later)) *>
      store.transact(
        ControlCommand.RecordSubmission(value.submissionKey, value.epoch, accepted, later)
      ) *>
      store
        .transact(ControlCommand.RequestCancellation(value.submissionKey, later))
        .as(value.submissionKey)

  private def cancelAndRelease[A](
      fiber: Fiber[IO, Throwable, A],
      release: Deferred[IO, Unit]
  ): IO[Outcome[IO, Throwable, A]] =
    for
      cancellation <- fiber.cancel.start
      _ <- IO.cede.replicateA_(8)
      _ <- release.complete(()).void
      _ <- cancellation.join
      outcome <- fiber.join
    yield outcome

  /** Fails a matched command with a typed rejection rather than an effect failure. */
  private def rejectingStore(
      underlying: ControlStore[IO],
      matches: ControlCommand => Boolean
  ): ControlStore[IO] = new ControlStore[IO]:
    def transact(command: ControlCommand): IO[Either[ControlFailure, ControlCommit]] =
      if !matches(command) then underlying.transact(command)
      else
        IO.pure(
          Left(ControlFailure.JournalCorrupt("record rejected by a store fault injection"))
        )

    def snapshot: IO[ControlState] = underlying.snapshot
    def attempt(submissionKey: SubmissionKey): IO[Option[ManagedAttempt]] =
      underlying.attempt(submissionKey)
    def events(after: EventCursor, maximum: Int): IO[EventPage] =
      underlying.events(after, maximum)
    def pendingOutbox(maximum: Int): IO[Vector[OutboxEntry]] =
      underlying.pendingOutbox(maximum)
    def nonTerminal(maximum: Int): IO[Vector[ManagedAttempt]] =
      underlying.nonTerminal(maximum)
    def bound(maximum: Int): IO[Vector[ManagedAttempt]] =
      underlying.bound(maximum)

  private def pausingStore(
      underlying: ControlStore[IO],
      matches: ControlCommand => Boolean,
      before: Boolean,
      reached: Deferred[IO, Unit],
      release: Deferred[IO, Unit]
  ): ControlStore[IO] = new ControlStore[IO]:
    private def pause: IO[Unit] = reached.complete(()).void *> release.get

    def transact(command: ControlCommand): IO[Either[ControlFailure, ControlCommit]] =
      if !matches(command) then underlying.transact(command)
      else if before then pause *> underlying.transact(command)
      else underlying.transact(command).flatTap(_ => pause)

    def snapshot: IO[ControlState] = underlying.snapshot
    def attempt(submissionKey: SubmissionKey): IO[Option[ManagedAttempt]] =
      underlying.attempt(submissionKey)
    def events(after: EventCursor, maximum: Int): IO[EventPage] =
      underlying.events(after, maximum)
    def pendingOutbox(maximum: Int): IO[Vector[OutboxEntry]] =
      underlying.pendingOutbox(maximum)
    def nonTerminal(maximum: Int): IO[Vector[ManagedAttempt]] =
      underlying.nonTerminal(maximum)
    def bound(maximum: Int): IO[Vector[ManagedAttempt]] =
      underlying.bound(maximum)

  private def unused[A]: IO[A] = IO.raiseError(new AssertionError("unexpected scheduler call"))
