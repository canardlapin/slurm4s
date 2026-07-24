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

  test("authorized terminal retry bumps epoch, resets local state, and retains binding history") {
    val terminal = terminalState("retry-terminal", RetrySafety.SafeForAutomaticRetry)
    val before = terminal.attempts.values.head
    val authorization = RetryAuthorization.Automatic(retryReason("replace interrupted pilot"))
    val retried = applyCommand(
      terminal,
      ControlCommand.RetrySubmission(
        before.intent.submissionKey,
        before.intent.epoch,
        authorization,
        later.plusSeconds(5L)
      )
    )
    val attempt = retried.state.attempts(before.intent.submissionKey)
    val nextEpoch = AttemptEpoch.from(2L).toOption.get

    assertEquals(attempt.intent.epoch, nextEpoch)
    assertEquals(attempt.phase, ManagedPhase.IntentRecorded)
    assertEquals(attempt.bindings, before.bindings)
    assertEquals(attempt.currentJob, None)
    assertEquals(attempt.observation, ManagedObservation.Unobserved)
    assertEquals(attempt.accounting, ManagedAccounting.Unobserved)
    assertEquals(attempt.cancellation, ManagedCancellation.NotRequested)
    assert(
      retried.state.outbox.values.exists(
        _.action == OutboxAction.Submit(before.intent.submissionKey, nextEpoch)
      )
    )
    assert(
      retried.committedEvents.exists(
        _.event == ManagedEvent.SubmissionRetried(
          before.intent.submissionKey,
          before.intent.epoch,
          nextEpoch,
          authorization
        )
      )
    )
  }

  test("automatic execution retry requires provenance while manual retry remains explicit") {
    val terminal = terminalState("retry-manual", RetrySafety.Unknown)
    val attempt = terminal.attempts.values.head
    val automatic = ControlTransition(
      terminal,
      ControlCommand.RetrySubmission(
        attempt.intent.submissionKey,
        attempt.intent.epoch,
        RetryAuthorization.Automatic(retryReason("automatic replacement")),
        later.plusSeconds(5L)
      )
    )
    val manual = ControlTransition(
      terminal,
      ControlCommand.RetrySubmission(
        attempt.intent.submissionKey,
        attempt.intent.epoch,
        RetryAuthorization.Manual(retryReason("operator inspected the prior attempt")),
        later.plusSeconds(5L)
      )
    )

    assert(automatic.left.exists(_.isInstanceOf[ControlFailure.RetryNotAuthorized]))
    assert(manual.exists(_.result.isInstanceOf[ControlResult.SubmissionRetried]))
    assert(RetryReason.from("   ").isLeft)
    assert(RetryReason.from("line one\nline two").isLeft)
  }

  test("retry rejects ambiguous acceptance and fences stale commands after an epoch bump") {
    val value = intent("retry-fence")
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(value.submissionKey, later)
    )
    val unknown = applyCommand(
      claimed.state,
      ControlCommand.RecoverSubmissionClaim(value.submissionKey, value.epoch, evidence, later)
    )
    val authorization = RetryAuthorization.Manual(retryReason("operator requested replacement"))
    val ambiguous = ControlTransition(
      unknown.state,
      ControlCommand.RetrySubmission(
        value.submissionKey,
        value.epoch,
        authorization,
        later.plusSeconds(1L)
      )
    )
    val unavailable = unavailableState("retry-fence-safe")
    val unavailableAttempt = unavailable.attempts.values.head
    val retried = applyCommand(
      unavailable,
      ControlCommand.RetrySubmission(
        unavailableAttempt.intent.submissionKey,
        unavailableAttempt.intent.epoch,
        RetryAuthorization.Automatic(retryReason("sbatch executable became available")),
        later.plusSeconds(2L)
      )
    )
    val staleSubmission = ControlTransition(
      retried.state,
      ControlCommand.RecordSubmission(
        unavailableAttempt.intent.submissionKey,
        unavailableAttempt.intent.epoch,
        accepted,
        later.plusSeconds(3L)
      )
    )
    val staleRetry = ControlTransition(
      retried.state,
      ControlCommand.RetrySubmission(
        unavailableAttempt.intent.submissionKey,
        unavailableAttempt.intent.epoch,
        authorization,
        later.plusSeconds(3L)
      )
    )

    assert(ambiguous.left.exists(_.isInstanceOf[ControlFailure.InvalidPhase]))
    assert(staleSubmission.left.exists(_.isInstanceOf[ControlFailure.EpochMismatch]))
    assert(staleRetry.left.exists(_.isInstanceOf[ControlFailure.EpochMismatch]))
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

  private def terminalState(key: String, retrySafety: RetrySafety): ControlState =
    val value = ManagedIntent
      .from(request(key).copy(retrySafety = retrySafety), instant)
      .toOption
      .get
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(value.submissionKey, later)
    )
    val bound = applyCommand(
      claimed.state,
      ControlCommand.RecordSubmission(
        value.submissionKey,
        value.epoch,
        accepted,
        later.plusSeconds(1L)
      )
    )
    val observed = applyCommand(
      bound.state,
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
        later.plusSeconds(2L)
      )
    )
    val record = AccountingRecord(
      job,
      SlurmState.NodeFailure,
      None,
      Some(WorkloadOutcome.NodeFailure),
      Freshness.Current(later),
      Map.empty,
      evidence
    )
    applyCommand(
      observed.state,
      ControlCommand.RecordAccounting(
        NonEmptyVector.one(job),
        SchedulerQueryResult.Succeeded(
          AccountingBatch(NonEmptyVector.one(record), Vector.empty)
        ),
        later.plusSeconds(3L)
      )
    ).state

  private def unavailableState(key: String): ControlState =
    val value = intent(key)
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(value.submissionKey, later)
    )
    val unavailable = SubmissionAttempt.InvocationFailed(
      InvocationResult.SpawnFailed(
        SpawnFailureKind.ExecutableMissing,
        Diagnostics.one(Diagnostic("sbatch-missing", "sbatch was not found")),
        evidence
      )
    )
    applyCommand(
      claimed.state,
      ControlCommand.RecordSubmission(
        value.submissionKey,
        value.epoch,
        unavailable,
        later.plusSeconds(1L)
      )
    ).state

  private def retryReason(value: String): RetryReason = RetryReason.from(value).toOption.get
