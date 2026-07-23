package io.github.bbuchsbaum.scalaslurm.managed

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.scalaslurm.core.*

class ControlTransitionSuite extends munit.FunSuite:
  import ManagedTestSupport.*

  test("intent and submit outbox commit atomically; identical digest is idempotent") {
    val value = intent("stable-key")
    val created = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val repeated = applyCommand(created.state, ControlCommand.RecordIntent(value))

    assertEquals(created.state.revision.value, 1L)
    assertEquals(created.committedEvents.map(_.cursor.value), Vector(1L))
    assertEquals(created.state.outbox.values.count(_.status == OutboxStatus.Pending), 1)
    assert(repeated.result.isInstanceOf[ControlResult.IntentExisting])
    assertEquals(repeated.state.revision, created.state.revision)
    assertEquals(repeated.committedEvents, Vector.empty)
  }

  test("same submission key with changed canonical request conflicts before a claim") {
    val original = applyCommand(
      ControlState.empty,
      ControlCommand.RecordIntent(intent("conflict-key", "true"))
    )
    val changed = ControlTransition(
      original.state,
      ControlCommand.RecordIntent(intent("conflict-key", "false"))
    )

    assert(changed.left.exists(_.isInstanceOf[ControlFailure.DigestConflict]))
    assert(original.state.outbox.values.forall(_.status == OutboxStatus.Pending))
  }

  test("restart with an in-flight submission becomes unknown and cannot be reclaimed") {
    val recorded = applyCommand(
      ControlState.empty,
      ControlCommand.RecordIntent(intent("crash-edge"))
    )
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(SubmissionKey.from("crash-edge").toOption.get, later)
    )
    val attempt = claimed.state.attempts.values.head
    val recovered = applyCommand(
      claimed.state,
      ControlCommand.RecoverSubmissionClaim(
        attempt.intent.submissionKey,
        attempt.intent.epoch,
        evidence,
        later.plusSeconds(1L)
      )
    )

    assert(recovered.state.attempts.values.head.phase.isInstanceOf[ManagedPhase.AcceptanceUnknown])
    assert(recovered.state.outbox.values.head.status.isInstanceOf[OutboxStatus.Uncertain])
    assert(
      ControlTransition(
        recovered.state,
        ControlCommand.ClaimSubmission(attempt.intent.submissionKey, later.plusSeconds(2L))
      ).left.exists(_.isInstanceOf[ControlFailure.InvalidPhase])
    )
  }

  test("accepted submission records binding history and stale epochs are fenced") {
    val value = intent("binding")
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(value.submissionKey, later)
    )
    val completed = applyCommand(
      claimed.state,
      ControlCommand.RecordSubmission(
        value.submissionKey,
        value.epoch,
        accepted,
        later.plusSeconds(1L)
      )
    )
    val attempt = completed.state.attempts(value.submissionKey)

    assertEquals(attempt.currentJob, Some(job))
    assertEquals(attempt.bindings.size, 1)
    assertEquals(EpochFence.validate(attempt, value.epoch), EpochFence.Current)
    val stale = AttemptEpoch.from(2L).toOption.get
    assertEquals(EpochFence.validate(attempt, stale), EpochFence.Stale(value.epoch, stale))
  }

  test("temporary observation failure makes evidence stale rather than terminal") {
    val bound = boundState("stale-query")
    val current = applyCommand(
      bound,
      ControlCommand.RecordObservations(
        NonEmptyVector.one(job),
        SchedulerQueryResult.Succeeded(
          ObservationBatch(
            NonEmptyVector.one(
              ObservationResult.Observed(
                JobObservation(
                  job,
                  SlurmState.Running,
                  Freshness.Current(later),
                  None,
                  Map.empty,
                  evidence
                )
              )
            )
          )
        ),
        later
      )
    )
    val failure = SchedulerQueryResult.InvocationFailed(
      InvocationResult.TimedOut(
        DurationMillis.from(1000L).toOption.get,
        evidence.primary,
        evidence.primary
      )
    )
    val updated = applyCommand(
      current.state,
      ControlCommand.RecordObservations(
        NonEmptyVector.one(job),
        failure,
        later.plusSeconds(2L)
      )
    )
    val attempt = updated.state.attempts.values.head

    assert(attempt.observation.isInstanceOf[ManagedObservation.Stale])
    assert(!attempt.isTerminal)
  }

  test("terminal accounting reconciles an asynchronous cancellation race") {
    val bound = boundState("cancel-race")
    val key = bound.attempts.keys.head
    val requested = applyCommand(
      bound,
      ControlCommand.RequestCancellation(key, later.plusSeconds(2L))
    )
    val claimed = applyCommand(
      requested.state,
      ControlCommand.ClaimCancellation(key, later.plusSeconds(3L))
    )
    val record = AccountingRecord(
      job,
      SlurmState.Completed,
      Some(ExitStatus(0, None)),
      Some(WorkloadOutcome.Completed(0)),
      Freshness.Current(later),
      Map.empty,
      evidence
    )
    val terminal = applyCommand(
      claimed.state,
      ControlCommand.RecordAccounting(
        NonEmptyVector.one(job),
        SchedulerQueryResult.Succeeded(AccountingBatch(NonEmptyVector.one(record), Vector.empty)),
        later.plusSeconds(4L)
      )
    )
    val attempt = terminal.state.attempts(key)

    assert(attempt.phase.isInstanceOf[ManagedPhase.Terminal])
    assert(attempt.cancellation.isInstanceOf[ManagedCancellation.Reconciled])
    assert(terminal.state.outbox.values.exists(_.status.isInstanceOf[OutboxStatus.Superseded]))
  }

  private def boundState(key: String): ControlState =
    val value = intent(key)
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(value.submissionKey, later)
    )
    applyCommand(
      claimed.state,
      ControlCommand.RecordSubmission(
        value.submissionKey,
        value.epoch,
        accepted,
        later.plusSeconds(1L)
      )
    ).state
