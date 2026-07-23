package io.github.bbuchsbaum.scalaslurm.core

enum AttemptPhase derives CanEqual:
  case Prepared
  case Submitting
  case AcceptanceUnknown(reason: AcceptanceUncertainty, evidence: EvidenceBundle)
  case Bound(job: JobRef, evidence: EvidenceBundle)
  case Rejected(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case Terminal(job: JobRef, outcome: WorkloadOutcome, evidence: EvidenceBundle)

final case class AttemptState(
    id: AttemptId,
    epoch: AttemptEpoch,
    phase: AttemptPhase,
    lastCursor: EventCursor
) derives CanEqual

object AttemptState:
  def prepared(id: AttemptId, epoch: AttemptEpoch): AttemptState =
    AttemptState(id, epoch, AttemptPhase.Prepared, EventCursor.origin)

enum AttemptEvent derives CanEqual:
  case SubmissionStarted
  case SubmissionAccepted(job: JobRef, evidence: EvidenceBundle)
  case SubmissionRejected(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case SubmissionResponseLost(reason: AcceptanceUncertainty, evidence: EvidenceBundle)
  case Reconciled(job: JobRef, evidence: EvidenceBundle)
  case TerminalObserved(job: JobRef, outcome: WorkloadOutcome, evidence: EvidenceBundle)

final case class VersionedAttemptEvent(
    cursor: EventCursor,
    epoch: AttemptEpoch,
    event: AttemptEvent
) derives CanEqual

enum AttemptTransitionFailure derives CanEqual:
  case StaleEpoch(expected: AttemptEpoch, received: AttemptEpoch)
  case OutOfOrderCursor(previous: EventCursor, received: EventCursor)
  case InvalidTransition(phase: AttemptPhase, event: AttemptEvent)
  case JobBindingMismatch(expected: JobRef, received: JobRef)

object AttemptReducer:
  def reduce(
      state: AttemptState,
      versioned: VersionedAttemptEvent
  ): Either[AttemptTransitionFailure, AttemptState] =
    if versioned.epoch.value != state.epoch.value then
      Left(AttemptTransitionFailure.StaleEpoch(state.epoch, versioned.epoch))
    else if versioned.cursor.value <= state.lastCursor.value then
      Left(AttemptTransitionFailure.OutOfOrderCursor(state.lastCursor, versioned.cursor))
    else
      transition(state.phase, versioned.event).map { nextPhase =>
        state.copy(phase = nextPhase, lastCursor = versioned.cursor)
      }

  private def transition(
      phase: AttemptPhase,
      event: AttemptEvent
  ): Either[AttemptTransitionFailure, AttemptPhase] =
    (phase, event) match
      case (AttemptPhase.Prepared, AttemptEvent.SubmissionStarted) =>
        Right(AttemptPhase.Submitting)
      case (AttemptPhase.Submitting, AttemptEvent.SubmissionAccepted(job, evidence)) =>
        Right(AttemptPhase.Bound(job, evidence))
      case (AttemptPhase.Submitting, AttemptEvent.SubmissionRejected(diagnostics, evidence)) =>
        Right(AttemptPhase.Rejected(diagnostics, evidence))
      case (AttemptPhase.Submitting, AttemptEvent.SubmissionResponseLost(reason, evidence)) =>
        Right(AttemptPhase.AcceptanceUnknown(reason, evidence))
      case (AttemptPhase.AcceptanceUnknown(_, _), AttemptEvent.Reconciled(job, evidence)) =>
        Right(AttemptPhase.Bound(job, evidence))
      case (
            AttemptPhase.Bound(expected, _),
            AttemptEvent.TerminalObserved(received, outcome, evidence)
          ) =>
        if expected == received then Right(AttemptPhase.Terminal(received, outcome, evidence))
        else Left(AttemptTransitionFailure.JobBindingMismatch(expected, received))
      case _ => Left(AttemptTransitionFailure.InvalidTransition(phase, event))
