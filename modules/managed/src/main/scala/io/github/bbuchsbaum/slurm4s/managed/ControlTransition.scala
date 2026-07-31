package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant

enum ControlCommand:
  case RecordIntent(intent: ManagedIntent)
  case ClaimSubmission(submissionKey: SubmissionKey, at: Instant)
  case RecordSubmission(
      submissionKey: SubmissionKey,
      epoch: AttemptEpoch,
      result: SubmissionAttempt,
      at: Instant
  )
  case RecoverSubmissionClaim(
      submissionKey: SubmissionKey,
      epoch: AttemptEpoch,
      evidence: EvidenceBundle,
      at: Instant
  )
  case RetrySubmission(
      submissionKey: SubmissionKey,
      expectedEpoch: AttemptEpoch,
      authorization: RetryAuthorization,
      at: Instant
  )
  case ReconcileBinding(
      submissionKey: SubmissionKey,
      epoch: AttemptEpoch,
      job: JobRef,
      evidence: EvidenceBundle,
      at: Instant
  )
  case RecordObservations(
      requested: NonEmptyVector[JobRef],
      result: SchedulerQueryResult[ObservationBatch],
      at: Instant
  )
  case RecordAccounting(
      requested: NonEmptyVector[JobRef],
      result: SchedulerQueryResult[AccountingBatch],
      at: Instant
  )
  case RequestCancellation(submissionKey: SubmissionKey, at: Instant)
  case ClaimCancellation(submissionKey: SubmissionKey, at: Instant)
  case RecoverCancellationClaim(
      submissionKey: SubmissionKey,
      evidence: EvidenceBundle,
      at: Instant
  )
  case RecordCancellation(
      submissionKey: SubmissionKey,
      result: CancellationAttempt,
      at: Instant
  )

enum ControlFailure derives CanEqual:
  case RequestRejected(diagnostics: Diagnostics)
  case AttemptNotFound(submissionKey: SubmissionKey)
  case DigestConflict(
      submissionKey: SubmissionKey,
      existing: ContentDigest,
      received: ContentDigest
  )
  case InvalidPhase(submissionKey: SubmissionKey, phase: ManagedPhase, operation: String)
  case EpochMismatch(
      submissionKey: SubmissionKey,
      expected: AttemptEpoch,
      received: AttemptEpoch
  )
  case RetryNotAuthorized(submissionKey: SubmissionKey, retrySafety: RetrySafety)
  case EpochExhausted(submissionKey: SubmissionKey)
  case OutboxInvariant(submissionKey: SubmissionKey, message: String)
  case JournalCorrupt(message: String)
  case JournalExhausted(message: String)
  case JournalLocked(path: String)

enum ControlResult derives CanEqual:
  case IntentCreated(attempt: ManagedAttempt)
  case IntentExisting(attempt: ManagedAttempt)
  case SubmissionClaimed(attempt: ManagedAttempt, outbox: OutboxEntry)
  case SubmissionAlreadyClaimed(attempt: ManagedAttempt)
  case SubmissionRetried(
      attempt: ManagedAttempt,
      outbox: OutboxEntry,
      authorization: RetryAuthorization
  )
  case Updated(attempts: Vector[ManagedAttempt])
  case CancellationQueued(attempt: ManagedAttempt, outbox: OutboxEntry)
  case NoChange(attempt: Option[ManagedAttempt])

final case class ControlCommit(
    result: ControlResult,
    state: ControlState,
    committedEvents: Vector[CommittedEvent]
) derives CanEqual

object ControlTransition:
  def apply(
      state: ControlState,
      command: ControlCommand
  ): Either[ControlFailure, ControlCommit] =
    command match
      case ControlCommand.RecordIntent(intent)     => recordIntent(state, intent)
      case ControlCommand.ClaimSubmission(key, at) => claimSubmission(state, key, at)
      case ControlCommand.RecordSubmission(key, epoch, result, at) =>
        recordSubmission(state, key, epoch, result, at)
      case ControlCommand.RecoverSubmissionClaim(key, epoch, evidence, at) =>
        recoverSubmission(state, key, epoch, evidence, at)
      case ControlCommand.RetrySubmission(key, expectedEpoch, authorization, at) =>
        retrySubmission(state, key, expectedEpoch, authorization, at)
      case ControlCommand.ReconcileBinding(key, epoch, job, evidence, at) =>
        reconcileBinding(state, key, epoch, job, evidence, at)
      case ControlCommand.RecordObservations(requested, result, at) =>
        recordObservations(state, requested, result, at)
      case ControlCommand.RecordAccounting(requested, result, at) =>
        recordAccounting(state, requested, result, at)
      case ControlCommand.RequestCancellation(key, at) =>
        requestCancellation(state, key, at)
      case ControlCommand.ClaimCancellation(key, at) =>
        claimCancellation(state, key, at)
      case ControlCommand.RecoverCancellationClaim(key, evidence, at) =>
        recoverCancellation(state, key, evidence, at)
      case ControlCommand.RecordCancellation(key, result, at) =>
        recordCancellation(state, key, result, at)

  private def recordIntent(
      state: ControlState,
      intent: ManagedIntent
  ): Either[ControlFailure, ControlCommit] =
    state.attempts.get(intent.submissionKey) match
      case Some(existing) if existing.intent.request.digest == intent.request.digest =>
        Right(noChange(state, ControlResult.IntentExisting(existing)))
      case Some(existing) =>
        Left(
          ControlFailure.DigestConflict(
            intent.submissionKey,
            existing.intent.request.digest,
            intent.request.digest
          )
        )
      case None =>
        val attempt = ManagedAttempt(
          intent,
          ManagedPhase.IntentRecorded,
          Vector.empty,
          ManagedObservation.Unobserved,
          ManagedAccounting.Unobserved,
          ManagedCancellation.NotRequested,
          intent.recordedAt
        )
        val outbox = OutboxEntry(
          submitOutboxId(intent),
          OutboxAction.Submit(intent.submissionKey, intent.epoch),
          OutboxStatus.Pending,
          intent.recordedAt
        )
        commit(
          state.copy(
            attempts = state.attempts.updated(intent.submissionKey, attempt),
            outbox = state.outbox.updated(outbox.id, outbox)
          ),
          ControlResult.IntentCreated(attempt),
          Vector(ManagedEvent.IntentRecorded(intent)),
          intent.recordedAt
        )

  private def retrySubmission(
      state: ControlState,
      key: SubmissionKey,
      expectedEpoch: AttemptEpoch,
      authorization: RetryAuthorization,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    checkedEpoch(state, key, expectedEpoch).flatMap { current =>
      retryAuthorized(current, authorization).flatMap { _ =>
        current.intent.epoch.next.left
          .map(_ => ControlFailure.EpochExhausted(key))
          .flatMap { nextEpoch =>
            val nextIntent = current.intent.copy(epoch = nextEpoch)
            val updated = current.copy(
              intent = nextIntent,
              phase = ManagedPhase.IntentRecorded,
              observation = ManagedObservation.Unobserved,
              accounting = ManagedAccounting.Unobserved,
              cancellation = ManagedCancellation.NotRequested,
              updatedAt = at
            )
            val outbox = OutboxEntry(
              submitOutboxId(nextIntent),
              OutboxAction.Submit(key, nextEpoch),
              OutboxStatus.Pending,
              at
            )
            commit(
              state.copy(
                attempts = state.attempts.updated(key, updated),
                outbox = state.outbox.updated(outbox.id, outbox)
              ),
              ControlResult.SubmissionRetried(updated, outbox, authorization),
              Vector(
                ManagedEvent.SubmissionRetried(
                  key,
                  current.intent.epoch,
                  nextEpoch,
                  authorization
                )
              ),
              at
            )
          }
      }
    }

  private def retryAuthorized(
      current: ManagedAttempt,
      authorization: RetryAuthorization
  ): Either[ControlFailure, Unit] =
    current.phase match
      case _: ManagedPhase.SubmissionRejected | _: ManagedPhase.SubmissionUnavailable =>
        Right(())
      case _: ManagedPhase.Terminal =>
        authorization match
          case _: RetryAuthorization.Manual => Right(())
          case _: RetryAuthorization.Automatic
              if current.intent.retrySafety == RetrySafety.SafeForAutomaticRetry =>
            Right(())
          case _: RetryAuthorization.Automatic =>
            Left(
              ControlFailure.RetryNotAuthorized(
                current.intent.submissionKey,
                current.intent.retrySafety
              )
            )
      case phase =>
        Left(
          ControlFailure.InvalidPhase(
            current.intent.submissionKey,
            phase,
            "retry-submission"
          )
        )

  private def claimSubmission(
      state: ControlState,
      key: SubmissionKey,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    attempt(state, key).flatMap { current =>
      current.phase match
        case ManagedPhase.IntentRecorded =>
          val outboxId = submitOutboxId(current.intent)
          state.outbox.get(outboxId) match
            case Some(
                  entry @ OutboxEntry(_, OutboxAction.Submit(_, epoch), OutboxStatus.Pending, _)
                ) if epoch == current.intent.epoch =>
              val updatedAttempt = current.copy(phase = ManagedPhase.Submitting(at), updatedAt = at)
              val updatedOutbox = entry.copy(status = OutboxStatus.InFlight(at))
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updatedAttempt),
                  outbox = state.outbox.updated(outboxId, updatedOutbox)
                ),
                ControlResult.SubmissionClaimed(updatedAttempt, updatedOutbox),
                Vector(ManagedEvent.SubmissionClaimed(key, current.intent.epoch)),
                at
              )
            case _ => Left(ControlFailure.OutboxInvariant(key, "pending submit outbox is missing"))
        case _: ManagedPhase.Submitting =>
          Right(noChange(state, ControlResult.SubmissionAlreadyClaimed(current)))
        case phase => Left(ControlFailure.InvalidPhase(key, phase, "claim-submission"))
    }

  private def recordSubmission(
      state: ControlState,
      key: SubmissionKey,
      epoch: AttemptEpoch,
      result: SubmissionAttempt,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    checkedEpoch(state, key, epoch).flatMap { current =>
      current.phase match
        case _: ManagedPhase.Submitting =>
          val outboxId = submitOutboxId(current.intent)
          state.outbox.get(outboxId) match
            case Some(entry @ OutboxEntry(_, _, _: OutboxStatus.InFlight, _)) =>
              val (phase, bindings, outboxStatus) = result match
                case SubmissionAttempt.Completed(Submission.Accepted(job, evidence)) =>
                  (
                    ManagedPhase.Bound(job),
                    current.bindings :+ BindingRecord(epoch, job, at, evidence, reconciled = false),
                    OutboxStatus.Completed(at)
                  )
                case SubmissionAttempt.Completed(Submission.Rejected(diagnostics, evidence)) =>
                  (
                    ManagedPhase.SubmissionRejected(diagnostics, Some(evidence)),
                    current.bindings,
                    OutboxStatus.Completed(at)
                  )
                case SubmissionAttempt.Completed(
                      Submission.AcceptanceUnknown(reason, evidence)
                    ) =>
                  (
                    ManagedPhase.AcceptanceUnknown(reason, evidence),
                    current.bindings,
                    OutboxStatus.Uncertain(at, "scheduler acceptance is unknown")
                  )
                case SubmissionAttempt.PreparationFailed(diagnostics) =>
                  (
                    ManagedPhase.SubmissionRejected(diagnostics, None),
                    current.bindings,
                    OutboxStatus.Completed(at)
                  )
                case failed @ SubmissionAttempt.InvocationFailed(_) =>
                  (
                    ManagedPhase.SubmissionUnavailable(failed),
                    current.bindings,
                    OutboxStatus.Completed(at)
                  )
              val updated = current.copy(phase = phase, bindings = bindings, updatedAt = at)
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updated),
                  outbox = state.outbox.updated(outboxId, entry.copy(status = outboxStatus))
                ),
                ControlResult.Updated(Vector(updated)),
                Vector(ManagedEvent.SubmissionResolved(key, epoch, result)),
                at
              )
            case _ =>
              Left(ControlFailure.OutboxInvariant(key, "in-flight submit outbox is missing"))
        case phase => Left(ControlFailure.InvalidPhase(key, phase, "record-submission"))
    }

  private def recoverSubmission(
      state: ControlState,
      key: SubmissionKey,
      epoch: AttemptEpoch,
      evidence: EvidenceBundle,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    checkedEpoch(state, key, epoch).flatMap { current =>
      current.phase match
        case _: ManagedPhase.Submitting =>
          val outboxId = submitOutboxId(current.intent)
          state.outbox.get(outboxId) match
            case Some(entry) =>
              val updated = current.copy(
                phase = ManagedPhase.AcceptanceUnknown(
                  AcceptanceUncertainty.PersistenceInterrupted,
                  evidence
                ),
                updatedAt = at
              )
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updated),
                  outbox = state.outbox.updated(
                    outboxId,
                    entry.copy(
                      status = OutboxStatus.Uncertain(
                        at,
                        "process restarted with submission in flight"
                      )
                    )
                  )
                ),
                ControlResult.Updated(Vector(updated)),
                Vector(ManagedEvent.SubmissionRecoveryRequired(key, epoch)),
                at
              )
            case None => Left(ControlFailure.OutboxInvariant(key, "submit outbox is missing"))
        case _: ManagedPhase.AcceptanceUnknown =>
          Right(noChange(state, ControlResult.NoChange(Some(current))))
        case phase => Left(ControlFailure.InvalidPhase(key, phase, "recover-submission"))
    }

  private def reconcileBinding(
      state: ControlState,
      key: SubmissionKey,
      epoch: AttemptEpoch,
      job: JobRef,
      evidence: EvidenceBundle,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    checkedEpoch(state, key, epoch).flatMap { current =>
      current.phase match
        case _: ManagedPhase.AcceptanceUnknown =>
          val binding = BindingRecord(epoch, job, at, evidence, reconciled = true)
          val updated = current.copy(
            phase = ManagedPhase.Bound(job),
            bindings = current.bindings :+ binding,
            updatedAt = at
          )
          commit(
            state.copy(attempts = state.attempts.updated(key, updated)),
            ControlResult.Updated(Vector(updated)),
            Vector(ManagedEvent.BindingReconciled(key, epoch, job)),
            at
          )
        case ManagedPhase.Bound(existing)
            if existing.key == job.key && !existing.clusterConflictsWith(job) =>
          Right(noChange(state, ControlResult.NoChange(Some(current))))
        case phase => Left(ControlFailure.InvalidPhase(key, phase, "reconcile-binding"))
    }

  private def recordObservations(
      state: ControlState,
      requested: NonEmptyVector[JobRef],
      result: SchedulerQueryResult[ObservationBatch],
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    val candidates = activeBound(state, requested.toVector.toSet)
    val updated = result match
      case SchedulerQueryResult.Succeeded(batch) =>
        val byJob = batch.results.toVector.map(value => observationJob(value).key -> value).toMap
        candidates.flatMap { case (key, current) =>
          current.currentJob.flatMap(job => byJob.get(job.key).map(job -> _)).map {
            case (job, value) =>
              val observed = observationJob(value)
              if job.clusterConflictsWith(observed) then
                key -> current.copy(
                  observation = ManagedObservation.Unavailable(
                    at,
                    clusterConflict("observation-cluster-conflict", job, observed),
                    observationEvidence(value)
                  ),
                  updatedAt = at
                )
              else
                key -> current.copy(
                  observation = ManagedObservation.Current(value),
                  updatedAt = at
                )
          }
        }
      case failure =>
        val (diagnostics, evidence) = queryFailure("active-observation-unavailable", failure)
        candidates.map { case (key, current) =>
          val next = current.observation match
            case ManagedObservation.Current(value) =>
              ManagedObservation.Stale(Some(value), at, diagnostics, evidence)
            case ManagedObservation.Stale(last, _, _, _) =>
              ManagedObservation.Stale(last, at, diagnostics, evidence)
            case ManagedObservation.Unobserved | _: ManagedObservation.Unavailable =>
              ManagedObservation.Unavailable(at, diagnostics, evidence)
          key -> current.copy(
            observation = next,
            updatedAt = at
          )
        }
    updateAttempts(
      state,
      updated,
      key =>
        result match
          case SchedulerQueryResult.Succeeded(_) => ManagedEvent.ObservationRecorded(key)
          case _                                 => ManagedEvent.ObservationUnavailable(key),
      at
    )

  private def recordAccounting(
      state: ControlState,
      requested: NonEmptyVector[JobRef],
      result: SchedulerQueryResult[AccountingBatch],
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    val candidates = activeBound(state, requested.toVector.toSet)
    val updated = result match
      case SchedulerQueryResult.Succeeded(batch) =>
        val byJob = batch.records.toVector.map(record => record.job.key -> record).toMap
        candidates.flatMap { case (key, current) =>
          current.currentJob.flatMap(job => byJob.get(job.key).map(job -> _)).map {
            case (job, record) =>
              if job.clusterConflictsWith(record.job) then
                key -> current.copy(
                  accounting = ManagedAccounting.Unavailable(
                    at,
                    clusterConflict("accounting-cluster-conflict", job, record.job),
                    record.evidence
                  ),
                  updatedAt = at
                )
              else
                val withAccounting = current.copy(
                  accounting = ManagedAccounting.Current(record),
                  updatedAt = at
                )
                key -> record.outcome.fold(withAccounting)(outcome =>
                  terminal(withAccounting, outcome, at)
                )
          }
        }
      case failure =>
        val (diagnostics, evidence) = queryFailure("accounting-unavailable", failure)
        candidates.map { case (key, current) =>
          key -> current.copy(
            accounting = ManagedAccounting.Unavailable(at, diagnostics, evidence),
            updatedAt = at
          )
        }
    updateAttempts(
      state,
      updated,
      key => ManagedEvent.AccountingRecorded(key),
      at,
      addTerminalEvents = true
    )

  private def requestCancellation(
      state: ControlState,
      key: SubmissionKey,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    attempt(state, key).flatMap { current =>
      current.phase match
        case _: ManagedPhase.Terminal =>
          Right(noChange(state, ControlResult.NoChange(Some(current))))
        case ManagedPhase.Bound(job) =>
          current.cancellation match
            case ManagedCancellation.NotRequested =>
              val entry = OutboxEntry(
                cancelOutboxId(current.intent, job),
                OutboxAction.Cancel(key, job),
                OutboxStatus.Pending,
                at
              )
              val updated = current.copy(
                cancellation = ManagedCancellation.Requested(at),
                updatedAt = at
              )
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updated),
                  outbox = state.outbox.updated(entry.id, entry)
                ),
                ControlResult.CancellationQueued(updated, entry),
                Vector(ManagedEvent.CancellationRequested(key)),
                at
              )
            case _ => Right(noChange(state, ControlResult.NoChange(Some(current))))
        case phase => Left(ControlFailure.InvalidPhase(key, phase, "request-cancellation"))
    }

  private def claimCancellation(
      state: ControlState,
      key: SubmissionKey,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    attempt(state, key).flatMap { current =>
      (current.phase, current.currentJob, current.cancellation) match
        case (ManagedPhase.Bound(_), Some(job), _: ManagedCancellation.Requested) =>
          val outboxId = cancelOutboxId(current.intent, job)
          state.outbox.get(outboxId) match
            case Some(entry @ OutboxEntry(_, _, OutboxStatus.Pending, _)) =>
              val updated = current.copy(
                cancellation = ManagedCancellation.Dispatching(at),
                updatedAt = at
              )
              val claimed = entry.copy(status = OutboxStatus.InFlight(at))
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updated),
                  outbox = state.outbox.updated(outboxId, claimed)
                ),
                ControlResult.CancellationQueued(updated, claimed),
                Vector(ManagedEvent.CancellationClaimed(key)),
                at
              )
            case _ => Left(ControlFailure.OutboxInvariant(key, "pending cancel outbox is missing"))
        case (_, _, _: ManagedCancellation.Dispatching) =>
          Right(noChange(state, ControlResult.NoChange(Some(current))))
        case (phase, _, _) => Left(ControlFailure.InvalidPhase(key, phase, "claim-cancellation"))
    }

  private def recordCancellation(
      state: ControlState,
      key: SubmissionKey,
      result: CancellationAttempt,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    attempt(state, key).flatMap { current =>
      (current.currentJob, current.cancellation) match
        case (Some(job), _: ManagedCancellation.Dispatching) =>
          val outboxId = cancelOutboxId(current.intent, job)
          state.outbox.get(outboxId) match
            case Some(entry @ OutboxEntry(_, _, _: OutboxStatus.InFlight, _)) =>
              val cancellation = result match
                case CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)) =>
                  ManagedCancellation.Acknowledged(evidence)
                case CancellationAttempt.Completed(CancellationResult.NotFound(evidence)) =>
                  ManagedCancellation.NotFound(evidence)
                case CancellationAttempt.Completed(
                      CancellationResult.Rejected(diagnostics, evidence)
                    ) =>
                  ManagedCancellation.Rejected(diagnostics, evidence)
                case CancellationAttempt.Completed(
                      CancellationResult.Unknown(diagnostics, evidence)
                    ) =>
                  ManagedCancellation.Unknown(diagnostics, evidence)
                case CancellationAttempt.InvocationFailed(invocation) =>
                  val evidence = evidenceOf(invocation)
                  ManagedCancellation.Unknown(
                    Diagnostics.one(
                      Diagnostic(
                        "cancellation-invocation-failed",
                        "the cancellation command did not produce an acknowledgement"
                      )
                    ),
                    evidence
                  )
              val status = cancellation match
                case _: ManagedCancellation.Unknown =>
                  OutboxStatus.Uncertain(at, "cancellation acknowledgement is unknown")
                case _ => OutboxStatus.Completed(at)
              val updated = current.copy(cancellation = cancellation, updatedAt = at)
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updated),
                  outbox = state.outbox.updated(outboxId, entry.copy(status = status))
                ),
                ControlResult.Updated(Vector(updated)),
                Vector(ManagedEvent.CancellationResolved(key, result)),
                at
              )
            case _ =>
              Left(ControlFailure.OutboxInvariant(key, "in-flight cancel outbox is missing"))
        case _ => Left(ControlFailure.InvalidPhase(key, current.phase, "record-cancellation"))
    }

  private def recoverCancellation(
      state: ControlState,
      key: SubmissionKey,
      evidence: EvidenceBundle,
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    attempt(state, key).flatMap { current =>
      (current.currentJob, current.cancellation) match
        case (Some(job), _: ManagedCancellation.Dispatching) =>
          val outboxId = cancelOutboxId(current.intent, job)
          state.outbox.get(outboxId) match
            case Some(entry) =>
              val diagnostics = Diagnostics.one(
                Diagnostic(
                  "cancellation-recovery-required",
                  "controller restarted while cancellation was in flight"
                )
              )
              val updated = current.copy(
                cancellation = ManagedCancellation.Unknown(diagnostics, evidence),
                updatedAt = at
              )
              commit(
                state.copy(
                  attempts = state.attempts.updated(key, updated),
                  outbox = state.outbox.updated(
                    outboxId,
                    entry.copy(
                      status = OutboxStatus.Uncertain(
                        at,
                        "process restarted with cancellation in flight"
                      )
                    )
                  )
                ),
                ControlResult.Updated(Vector(updated)),
                Vector(ManagedEvent.CancellationRecoveryRequired(key)),
                at
              )
            case None => Left(ControlFailure.OutboxInvariant(key, "cancel outbox is missing"))
        case (_, _: ManagedCancellation.Unknown) =>
          Right(noChange(state, ControlResult.NoChange(Some(current))))
        case _ => Left(ControlFailure.InvalidPhase(key, current.phase, "recover-cancellation"))
    }

  private def terminal(
      current: ManagedAttempt,
      outcome: WorkloadOutcome,
      at: Instant
  ): ManagedAttempt =
    val evidence = current.accounting match
      case ManagedAccounting.Current(record) => record.evidence
      case _ => throw new IllegalStateException("terminal needs accounting")
    current.copy(
      phase = ManagedPhase.Terminal(outcome, evidence),
      cancellation = current.cancellation match
        case ManagedCancellation.NotRequested => ManagedCancellation.NotRequested
        case _                                => ManagedCancellation.Reconciled(outcome, evidence),
      updatedAt = at
    )

  private def updateAttempts(
      state: ControlState,
      updates: Vector[(SubmissionKey, ManagedAttempt)],
      event: SubmissionKey => ManagedEvent,
      at: Instant,
      addTerminalEvents: Boolean = false
  ): Either[ControlFailure, ControlCommit] =
    if updates.isEmpty then Right(noChange(state, ControlResult.Updated(Vector.empty)))
    else
      val attempts = state.attempts ++ updates
      val terminalKeys = updates.collect { case (key, value) if value.isTerminal => key }
      val outbox = terminalKeys.foldLeft(state.outbox) { (entries, key) =>
        entries.map { case (id, entry) =>
          entry.action match
            case OutboxAction.Cancel(cancelKey, _)
                if cancelKey == key &&
                  (entry.status == OutboxStatus.Pending || entry.status
                    .isInstanceOf[OutboxStatus.InFlight]) =>
              id -> entry.copy(status = OutboxStatus.Superseded(at, "job reached terminal state"))
            case _ => id -> entry
        }
      }
      val ordinary = updates.map(pair => event(pair._1))
      val terminalEvents =
        if addTerminalEvents then
          updates.flatMap { case (key, value) =>
            value.phase match
              case ManagedPhase.Terminal(outcome, _) =>
                Vector(ManagedEvent.TerminalReconciled(key, outcome))
              case _ => Vector.empty
          }
        else Vector.empty
      commit(
        state.copy(attempts = attempts, outbox = outbox),
        ControlResult.Updated(updates.map(_._2)),
        ordinary ++ terminalEvents,
        at
      )

  private def activeBound(
      state: ControlState,
      requested: Set[JobRef]
  ): Vector[(SubmissionKey, ManagedAttempt)] =
    val keys = requested.map(_.key)
    state.attempts.iterator.collect {
      case entry @ (_, value)
          if value.phase.isInstanceOf[ManagedPhase.Bound] &&
            value.currentJob.exists(job => keys.contains(job.key)) =>
        entry
    }.toVector

  private def observationJob(result: ObservationResult): JobRef = result match
    case ObservationResult.Observed(value)      => value.job
    case ObservationResult.NotFound(job, _, _)  => job
    case ObservationResult.Failed(job, _, _, _) => job

  private def observationEvidence(result: ObservationResult): EvidenceBundle = result match
    case ObservationResult.Observed(value)           => value.evidence
    case ObservationResult.NotFound(_, _, evidence)  => evidence
    case ObservationResult.Failed(_, _, _, evidence) => evidence

  /** A same-numbered job on a different named cluster is a different job. Refusing the match is
    * only half the answer; the refusal must be visible, or it reads exactly like a lost row.
    */
  private def clusterConflict(code: String, bound: JobRef, reported: JobRef): Diagnostics =
    Diagnostics.one(
      Diagnostic(
        code,
        "reported cluster differs from the bound cluster; refusing to attribute the report",
        Map(
          "jobId" -> bound.jobId.value,
          "boundCluster" -> bound.cluster.fold("<none>")(_.value),
          "reportedCluster" -> reported.cluster.fold("<none>")(_.value)
        )
      )
    )

  private def queryFailure[A](
      code: String,
      result: SchedulerQueryResult[A]
  ): (Diagnostics, EvidenceBundle) = result match
    case SchedulerQueryResult.Empty(_, evidence) =>
      Diagnostics.one(Diagnostic(code, "scheduler query returned no rows")) -> evidence
    case SchedulerQueryResult.InvocationFailed(invocation) =>
      Diagnostics.one(Diagnostic(code, "scheduler query invocation failed")) -> evidenceOf(
        invocation
      )
    case SchedulerQueryResult.ParseFailed(diagnostics, evidence) => diagnostics -> evidence
    case SchedulerQueryResult.Succeeded(_)                       =>
      throw new IllegalArgumentException("successful query is not a failure")

  private def evidenceOf(invocation: InvocationResult): EvidenceBundle = invocation match
    case InvocationResult.Exited(_, stdout, stderr)   => EvidenceBundle(stdout, Vector(stderr))
    case InvocationResult.SpawnFailed(_, _, evidence) => evidence
    case InvocationResult.TimedOut(_, stdout, stderr) => EvidenceBundle(stdout, Vector(stderr))

  private def checkedEpoch(
      state: ControlState,
      key: SubmissionKey,
      received: AttemptEpoch
  ): Either[ControlFailure, ManagedAttempt] =
    attempt(state, key).flatMap { current =>
      Either.cond(
        current.intent.epoch == received,
        current,
        ControlFailure.EpochMismatch(key, current.intent.epoch, received)
      )
    }

  private def attempt(
      state: ControlState,
      key: SubmissionKey
  ): Either[ControlFailure, ManagedAttempt] =
    state.attempts.get(key).toRight(ControlFailure.AttemptNotFound(key))

  private def submitOutboxId(intent: ManagedIntent): OutboxId =
    OutboxId.unsafeFrom(s"submit-${intent.attemptId.value}-e${intent.epoch.value}")

  private def cancelOutboxId(intent: ManagedIntent, job: JobRef): OutboxId =
    OutboxId.unsafeFrom(s"cancel-${intent.attemptId.value}-${job.jobId.value}")

  private def noChange(state: ControlState, result: ControlResult): ControlCommit =
    ControlCommit(result, state, Vector.empty)

  /** Advance the journal, refusing rather than wrapping.
    *
    * `StoreRevision.next` and `EventCursor.next` return `Either` because `+ 1L` past
    * `Long.MaxValue` wraps to a negative value, silently violating each type's own non-negative
    * invariant and inverting its `Order`. Exhaustion is unreachable in practice; representing it as
    * a typed failure rather than an assumption is the point.
    */
  private def commit(
      state: ControlState,
      result: ControlResult,
      events: Vector[ManagedEvent],
      at: Instant
  ): Either[ControlFailure, ControlCommit] =
    if events.isEmpty then Right(noChange(state, result))
    else
      val origin = state.events.lastOption.map(_.cursor).getOrElse(EventCursor.origin)
      for
        revision <- state.revision.next.left.map(problem =>
          ControlFailure.JournalExhausted(problem.reason)
        )
        committed <- events.foldLeft(
          Right((origin, Vector.empty[CommittedEvent])): Either[
            ControlFailure,
            (EventCursor, Vector[CommittedEvent])
          ]
        ) { case (accumulated, event) =>
          accumulated.flatMap { case (cursor, entries) =>
            cursor.next.left
              .map(problem => ControlFailure.JournalExhausted(problem.reason))
              .map(advanced => (advanced, entries :+ CommittedEvent(advanced, revision, at, event)))
          }
        }
      yield
        val entries = committed._2
        ControlCommit(
          result,
          state.copy(revision = revision, events = state.events ++ entries),
          entries
        )
