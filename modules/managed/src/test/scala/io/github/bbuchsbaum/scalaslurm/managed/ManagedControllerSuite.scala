package io.github.bbuchsbaum.scalaslurm.managed

import cats.data.NonEmptyVector
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*

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
    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] = calls.update(_ + 1) *> result
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] = unused

  private def schedulerWithCancel(
      submitCalls: Ref[IO, Int],
      cancelCalls: Ref[IO, Int]
  ): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] =
      submitCalls.update(_ + 1) *> IO.pure(accepted)
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] =
      cancelCalls.update(_ + 1) *>
        IO.pure(CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)))

  private def unused[A]: IO[A] = IO.raiseError(new AssertionError("unexpected scheduler call"))
