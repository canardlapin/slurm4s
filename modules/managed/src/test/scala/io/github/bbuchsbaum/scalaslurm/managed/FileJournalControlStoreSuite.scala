package io.github.bbuchsbaum.scalaslurm.managed

import cats.data.NonEmptyVector
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Outcome
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import scala.jdk.CollectionConverters.*

class FileJournalControlStoreSuite extends munit.CatsEffectSuite:
  import ManagedTestSupport.*

  test("journal reopens nonterminal state and replays all committed cursors from disk") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("control.journal")
      val value = intent("file-restart")
      val limits = JournalLimits(JournalLimits.default.maximumRecordBytes, recentEventCacheSize = 1)
      val write = FileJournalControlStore.open[IO](path, limits).use { store =>
        for
          _ <- store.transact(ControlCommand.RecordIntent(value))
          _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
          snapshot <- store.snapshot
          _ = assertEquals(snapshot.events.size, 1)
        yield ()
      }

      write *> FileJournalControlStore.open[IO](path, limits).use { reopened =>
        for
          snapshot <- reopened.snapshot
          page <- reopened.events(EventCursor.origin, 10)
          resumed <- reopened.events(EventCursor.from(1L).toOption.get, 10)
          _ = assert(
            snapshot.attempts(value.submissionKey).phase.isInstanceOf[ManagedPhase.Submitting]
          )
          _ = assertEquals(page.events.map(_.cursor.value), Vector(1L, 2L))
          _ = assertEquals(resumed.events.map(_.cursor.value), Vector(2L))
          _ = assert(page.endOfJournal)
        yield ()
      }
    }
  }

  test("controller restart turns a durable submit claim into unknown without scheduler action") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("controller-restart.journal")
      val value = intent("durable-recovery")
      val claim = FileJournalControlStore.open[IO](path).use { store =>
        store.transact(ControlCommand.RecordIntent(value)) *>
          store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later)).void
      }
      claim *> FileJournalControlStore.open[IO](path).use { store =>
        for
          submitCalls <- Ref.of[IO, Int](0)
          controller = ManagedController[IO](store, countingScheduler(submitCalls))
          recovered <- controller.recoverInFlight()
          count <- submitCalls.get
          events <- store.events(EventCursor.origin, 10)
          _ = assert(recovered.head.exists(_.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown]))
          _ = assertEquals(count, 0)
          _ = assertEquals(events.events.map(_.cursor.value), Vector(1L, 2L, 3L))
        yield ()
      }
    }
  }

  test("persisted crash edges replay before, during, and after scheduler submission") {
    temporaryDirectory.use { directory =>
      val before = intent("edge-before")
      val during = intent("edge-during")
      val after = intent("edge-after")
      for
        beforeState <- persistAndReopen(
          directory.resolve("before.journal"),
          Vector(ControlCommand.RecordIntent(before))
        )
        duringState <- persistAndReopen(
          directory.resolve("during.journal"),
          Vector(
            ControlCommand.RecordIntent(during),
            ControlCommand.ClaimSubmission(during.submissionKey, later)
          )
        )
        afterState <- persistAndReopen(
          directory.resolve("after.journal"),
          Vector(
            ControlCommand.RecordIntent(after),
            ControlCommand.ClaimSubmission(after.submissionKey, later),
            ControlCommand.RecordSubmission(after.submissionKey, after.epoch, accepted, later)
          )
        )
        _ = assert(
          beforeState.attempts(before.submissionKey).phase == ManagedPhase.IntentRecorded
        )
        _ = assert(
          beforeState.outbox.values.exists(_.status == OutboxStatus.Pending)
        )
        _ = assert(
          duringState.attempts(during.submissionKey).phase.isInstanceOf[ManagedPhase.Submitting]
        )
        _ = assert(
          afterState.attempts(after.submissionKey).phase.isInstanceOf[ManagedPhase.Bound]
        )
      yield ()
    }
  }

  test("an incomplete final transaction is truncated as uncommitted and prior state survives") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("partial.journal")
      val value = intent("partial-tail")
      for
        committedSize <- FileJournalControlStore.open[IO](path).use { store =>
          store.transact(ControlCommand.RecordIntent(value)) *>
            IO.blocking(Files.size(path))
        }
        _ <- IO.blocking(
          Files.write(
            path,
            Array[Byte](0, 0, 1),
            StandardOpenOption.APPEND
          )
        )
        damagedSize <- IO.blocking(Files.size(path))
        _ = assertEquals(damagedSize, committedSize + 3L)
        _ <- FileJournalControlStore.open[IO](path).use { reopened =>
          for
            snapshot <- reopened.snapshot
            repairedSize <- IO.blocking(Files.size(path))
            _ = assertEquals(reopened.recovery.truncatedUncommittedBytes, 3L)
            _ = assert(snapshot.attempts.contains(value.submissionKey))
            _ = assertEquals(repairedSize, committedSize)
          yield ()
        }
      yield ()
    }
  }

  test("a complete but corrupted transaction is rejected rather than truncated or replayed") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("corrupt.journal")
      val value = intent("corrupt-record")
      for
        _ <- FileJournalControlStore
          .open[IO](path)
          .use(
            _.transact(ControlCommand.RecordIntent(value))
          )
        bytes <- IO.blocking(Files.readAllBytes(path))
        _ <- IO.blocking {
          val index = bytes.length / 2
          bytes(index) = (bytes(index) ^ 1).toByte
          Files.write(path, bytes, StandardOpenOption.TRUNCATE_EXISTING)
        }
        reopened <- FileJournalControlStore.open[IO](path).use(_ => IO.unit).attempt
        _ = assert(reopened.isLeft)
      yield ()
    }
  }

  test("exclusive journal lock prevents two controller writers") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("locked.journal")
      FileJournalControlStore.open[IO](path).use { _ =>
        FileJournalControlStore.open[IO](path).use(_ => IO.unit).attempt.map { second =>
          assert(second.isLeft)
        }
      }
    }
  }

  test("every persisted command codec is canonical on decode and re-encode") {
    val value = intent("codec-command")
    val observation = SchedulerQueryResult.Empty(later, evidence)
    val accounting = SchedulerQueryResult.Empty(later, evidence)
    val cancellation = CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence))
    val commands = Vector[ControlCommand](
      ControlCommand.RecordIntent(value),
      ControlCommand.ClaimSubmission(value.submissionKey, later),
      ControlCommand.RecordSubmission(value.submissionKey, value.epoch, accepted, later),
      ControlCommand.RecoverSubmissionClaim(value.submissionKey, value.epoch, evidence, later),
      ControlCommand.RetrySubmission(
        value.submissionKey,
        value.epoch,
        RetryAuthorization.Manual(
          RetryReason.from("operator authorized a new scheduler submission").toOption.get
        ),
        later
      ),
      ControlCommand.ReconcileBinding(value.submissionKey, value.epoch, job, evidence, later),
      ControlCommand.RecordObservations(NonEmptyVector.one(job), observation, later),
      ControlCommand.RecordAccounting(NonEmptyVector.one(job), accounting, later),
      ControlCommand.RequestCancellation(value.submissionKey, later),
      ControlCommand.ClaimCancellation(value.submissionKey, later),
      ControlCommand.RecoverCancellationClaim(value.submissionKey, evidence, later),
      ControlCommand.RecordCancellation(value.submissionKey, cancellation, later)
    )

    commands.foreach { command =>
      val encoded = ControlCommandJson.encode(command)
      val decoded = ControlCommandJson.decode(encoded).toOption.get
      assertEquals(ControlCommandJson.encode(decoded), encoded)
    }
  }

  test("event paging does not hide a second event committed in the same transaction") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("multi-event.journal")
      val value = intent("multi-event")
      val record = AccountingRecord(
        job,
        SlurmState.Completed,
        Some(ExitStatus(0, None)),
        Some(WorkloadOutcome.Completed(0)),
        Freshness.Current(later),
        Map.empty,
        evidence
      )
      FileJournalControlStore.open[IO](path).use { store =>
        for
          _ <- store.transact(ControlCommand.RecordIntent(value))
          _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
          _ <- store.transact(
            ControlCommand.RecordSubmission(value.submissionKey, value.epoch, accepted, later)
          )
          _ <- store.transact(
            ControlCommand.RecordAccounting(
              NonEmptyVector.one(job),
              SchedulerQueryResult.Succeeded(
                AccountingBatch(NonEmptyVector.one(record), Vector.empty)
              ),
              later
            )
          )
          first <- store.events(EventCursor.from(3L).toOption.get, 1)
          second <- store.events(first.next, 1)
          _ = assertEquals(first.events.map(_.cursor.value), Vector(4L))
          _ = assert(!first.endOfJournal)
          _ = assertEquals(second.events.map(_.cursor.value), Vector(5L))
          _ = assert(second.endOfJournal)
        yield ()
      }
    }
  }

  test("cancellation before append leaves no record and the next revision remains replayable") {
    exerciseCommitCancellation(JournalCommitBoundary.BeforeAppend, firstCommitSurvives = false)
  }

  test("cancellation after force publishes the committed state before it is observed") {
    exerciseCommitCancellation(JournalCommitBoundary.AfterForce, firstCommitSurvives = true)
  }

  test("cancellation after publication preserves unique revisions across restart") {
    exerciseCommitCancellation(JournalCommitBoundary.AfterPublication, firstCommitSurvives = true)
  }

  test("an epoch retry remains atomic under cancellation and replays with its new outbox") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("retry-cancellation.journal")
      val value = intent("retry-cancellation")
      val retry = ControlCommand.RetrySubmission(
        value.submissionKey,
        value.epoch,
        RetryAuthorization.Automatic(
          RetryReason.from("sbatch executable was restored").toOption.get
        ),
        later.plusSeconds(2L)
      )
      val unavailable = SubmissionAttempt.InvocationFailed(
        InvocationResult.SpawnFailed(
          SpawnFailureKind.ExecutableMissing,
          Diagnostics.one(Diagnostic("sbatch-missing", "sbatch was not found")),
          evidence
        )
      )
      for
        reached <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        armed <- Ref.of[IO, Boolean](false)
        probe = new JournalCommitProbe[IO]:
          def checkpoint(boundary: JournalCommitBoundary): IO[Unit] =
            if boundary != JournalCommitBoundary.AfterForce then IO.unit
            else
              armed.get.ifM(
                reached.complete(()).void *> release.get,
                IO.unit
              )
        _ <- FileJournalControlStore
          .openWithProbe[IO](path, JournalLimits.default, probe)
          .use { store =>
            for
              _ <- store.transact(ControlCommand.RecordIntent(value))
              _ <- store.transact(ControlCommand.ClaimSubmission(value.submissionKey, later))
              _ <- store.transact(
                ControlCommand.RecordSubmission(
                  value.submissionKey,
                  value.epoch,
                  unavailable,
                  later.plusSeconds(1L)
                )
              )
              _ <- armed.set(true)
              transaction <- store.transact(retry).start
              _ <- reached.get
              cancellation <- transaction.cancel.start
              _ <- IO.cede.replicateA_(8)
              _ <- release.complete(()).void
              _ <- cancellation.join
              outcome <- transaction.join
              _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, ?]])
              snapshot <- store.snapshot
              _ = assertEquals(snapshot.attempts(value.submissionKey).intent.epoch.value, 2L)
              _ = assertEquals(
                snapshot.attempts(value.submissionKey).phase,
                ManagedPhase.IntentRecorded
              )
              _ <- armed.set(false)
              _ <- store.transact(
                ControlCommand.ClaimSubmission(value.submissionKey, later.plusSeconds(3L))
              )
            yield ()
          }
        _ <- FileJournalControlStore.open[IO](path).use { reopened =>
          for
            snapshot <- reopened.snapshot
            attempt = snapshot.attempts(value.submissionKey)
            _ = assertEquals(attempt.intent.epoch.value, 2L)
            _ = assert(attempt.phase.isInstanceOf[ManagedPhase.Submitting])
            _ = assert(
              snapshot.outbox.values.exists(
                _.action == OutboxAction.Submit(
                  value.submissionKey,
                  AttemptEpoch.from(2L).toOption.get
                )
              )
            )
          yield ()
        }
      yield ()
    }
  }

  private def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("scala-slurm-managed-")))(directory =>
      IO.blocking {
        Files
          .walk(directory)
          .iterator()
          .asScala
          .toVector
          .sortBy(_.getNameCount)
          .reverse
          .foreach(path => Files.deleteIfExists(path))
      }.void
    )

  private def exerciseCommitCancellation(
      boundary: JournalCommitBoundary,
      firstCommitSurvives: Boolean
  ): IO[Unit] =
    temporaryDirectory.use { directory =>
      val path = directory.resolve(s"cancel-${boundary.toString}.journal")
      val value = intent(s"cancel-${boundary.toString}")
      for
        reached <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        visited <- Ref.of[IO, Boolean](false)
        probe = blockingProbe(boundary, reached, release, visited)
        expectedRevision <- FileJournalControlStore
          .openWithProbe[IO](path, JournalLimits.default, probe)
          .use { store =>
            for
              transaction <- store.transact(ControlCommand.RecordIntent(value)).start
              _ <- reached.get
              cancellation <- transaction.cancel.start
              _ <- IO.cede.replicateA_(8)
              _ <- release.complete(()).void
              _ <- cancellation.join
              outcome <- transaction.join
              afterCancellation <- store.snapshot
              _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, ?]])
              _ =
                if firstCommitSurvives then
                  assertEquals(afterCancellation.revision.value, 1L)
                  assert(afterCancellation.attempts.contains(value.submissionKey))
                else assertEquals(afterCancellation, ControlState.empty)
              next =
                if firstCommitSurvives then
                  ControlCommand.ClaimSubmission(value.submissionKey, later)
                else ControlCommand.RecordIntent(value)
              result <- store.transact(next)
              _ = assert(result.isRight)
              snapshot <- store.snapshot
            yield snapshot.revision.value
          }
        _ <- FileJournalControlStore.open[IO](path).use { reopened =>
          for
            snapshot <- reopened.snapshot
            page <- reopened.events(EventCursor.origin, 10)
            cursors = page.events.map(_.cursor.value)
            _ = assertEquals(snapshot.revision.value, expectedRevision)
            _ = assertEquals(cursors, (1L to expectedRevision).toVector)
            _ = assertEquals(cursors.distinct, cursors)
            _ = assert(snapshot.attempts.contains(value.submissionKey))
          yield ()
        }
      yield ()
    }

  private def blockingProbe(
      target: JournalCommitBoundary,
      reached: Deferred[IO, Unit],
      release: Deferred[IO, Unit],
      visited: Ref[IO, Boolean]
  ): JournalCommitProbe[IO] = new JournalCommitProbe[IO]:
    def checkpoint(boundary: JournalCommitBoundary): IO[Unit] =
      if boundary != target then IO.unit
      else
        visited
          .modify(alreadyVisited => true -> !alreadyVisited)
          .flatMap(firstVisit =>
            if firstVisit then reached.complete(()).void *> release.get
            else IO.unit
          )

  private def persistAndReopen(
      path: Path,
      commands: Vector[ControlCommand]
  ): IO[ControlState] =
    FileJournalControlStore
      .open[IO](path)
      .use(store => commands.traverse_(store.transact)) *> FileJournalControlStore
      .open[IO](path)
      .use(_.snapshot)

  private def countingScheduler(calls: Ref[IO, Int]): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] =
      calls.update(_ + 1) *> IO.pure(accepted)
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] = unused

  private def unused[A]: IO[A] = IO.raiseError(new AssertionError("unexpected scheduler call"))
