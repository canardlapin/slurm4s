package io.github.bbuchsbaum.slurm4s.core

import java.time.Instant
import java.time.LocalDateTime

enum SlurmState derives CanEqual:
  case Pending
  case Running
  case Completing
  case Completed
  case Failed
  case Cancelled
  case OutOfMemory
  case TimedOut
  case NodeFailure
  case Preempted
  case Requeued
  case RequeueHeld
  case RequeueFederation
  case SpecialExit
  case BootFail
  case Deadline
  case Suspended
  case Unknown(raw: String)

/** A state flag reported alongside a base state.
  *
  * SchedMD documents base states and flags as separate vocabularies. This enum carries the flag
  * half so it survives parsing; folding the two together is what made `CANCELLED+`
  * indistinguishable from `CANCELLED`. Separating them inside `SlurmState` itself is P8.D3.
  */
enum SlurmStateFlag derives CanEqual:
  case Completing
  case Configuring
  case PowerUpNode
  case StageOut
  case Resizing
  case Requeued
  case RequeueFederation
  case RequeueHold
  case Revoked
  case Signaling
  case SpecialExit
  case Stopped
  case ReservationDeleteHold
  case LaunchFailed
  case UpdateDb
  case Unknown(raw: String)

/** Whether the scheduler will report further transitions for a job.
  *
  * Derived in exactly one place. Three classifiers previously hand-wrote their own terminal sets
  * and all three omitted `BOOT_FAIL` and `DEADLINE`, so a job finished at the site was polled until
  * the awaiter's 24-hour default expired.
  */
enum Terminality derives CanEqual:
  case Terminal
  case Active
  case Indeterminate

object Terminality:

  /** Total over `SlurmState` on purpose: no catch-all, so a new state cannot be added without
    * deciding its terminality here.
    */
  def of(state: SlurmState): Terminality = state match
    case SlurmState.Completed | SlurmState.Failed | SlurmState.Cancelled | SlurmState.OutOfMemory |
        SlurmState.TimedOut | SlurmState.NodeFailure | SlurmState.Preempted | SlurmState.BootFail |
        SlurmState.Deadline =>
      Terminality.Terminal
    case SlurmState.Pending | SlurmState.Running | SlurmState.Completing | SlurmState.Suspended |
        SlurmState.Requeued | SlurmState.RequeueHeld | SlurmState.RequeueFederation |
        SlurmState.SpecialExit =>
      Terminality.Active
    case SlurmState.Unknown(_) =>
      Terminality.Indeterminate

/** A scheduler-reported state: at most one base state plus any number of flags.
  *
  * `state` stays `Unknown` when a surface reports only a flag, because some CLI surfaces show a
  * flag in place of the hidden base state. Manufacturing a base state to make the model total would
  * be a lie, and an `Indeterminate` terminality is the honest consequence.
  */
final case class StateReport(
    state: SlurmState,
    flags: Vector[SlurmStateFlag],
    truncated: Boolean
) derives CanEqual

object StateReport:
  def of(state: SlurmState): StateReport = StateReport(state, Vector.empty, truncated = false)

enum InterruptionClass derives CanEqual:
  case NotInterrupted
  case Requeueing
  case InfrastructureFailure
  case SchedulerPolicy
  case Cancellation
  case WorkloadFailure
  case Unknown

object InterruptionClass:
  def classify(state: SlurmState): InterruptionClass = state match
    case SlurmState.Pending | SlurmState.Running | SlurmState.Completing | SlurmState.Completed |
        SlurmState.Suspended =>
      InterruptionClass.NotInterrupted
    case SlurmState.Requeued | SlurmState.RequeueHeld | SlurmState.RequeueFederation |
        SlurmState.SpecialExit =>
      InterruptionClass.Requeueing
    case SlurmState.NodeFailure | SlurmState.BootFail =>
      InterruptionClass.InfrastructureFailure
    case SlurmState.Preempted | SlurmState.TimedOut | SlurmState.Deadline =>
      InterruptionClass.SchedulerPolicy
    case SlurmState.Cancelled =>
      InterruptionClass.Cancellation
    case SlurmState.Failed | SlurmState.OutOfMemory =>
      InterruptionClass.WorkloadFailure
    case SlurmState.Unknown(_) =>
      InterruptionClass.Unknown

enum SchedulerTimestamp derives CanEqual:
  case Absolute(value: Instant)
  case SiteLocal(value: LocalDateTime)

enum JobStart derives CanEqual:
  case Actual(at: SchedulerTimestamp)
  case Expected(at: SchedulerTimestamp)
  case Reported(at: SchedulerTimestamp)

enum ObservedTimeLimit derives CanEqual:
  case Limited(value: WallTimeMinutes)
  case Unlimited
  case PartitionDefault
  case Unknown(raw: Option[String])

final case class JobTiming(
    start: Option[JobStart],
    projectedEndAt: Option[SchedulerTimestamp],
    timeLimit: ObservedTimeLimit
) derives CanEqual

object JobTiming:
  val unknown: JobTiming =
    JobTiming(None, None, ObservedTimeLimit.Unknown(None))

final case class JobObservation(
    job: JobRef,
    state: SlurmState,
    freshness: Freshness,
    reason: Option[String],
    rawFields: Map[String, String],
    evidence: EvidenceBundle,
    timing: JobTiming = JobTiming.unknown,
    flags: Vector[SlurmStateFlag] = Vector.empty
) derives CanEqual:
  def terminality: Terminality = Terminality.of(state)

final case class ExitStatus(code: Int, signal: Option[Int]) derives CanEqual

final case class AccountingRecord(
    job: JobRef,
    state: SlurmState,
    exitStatus: Option[ExitStatus],
    outcome: Option[WorkloadOutcome],
    freshness: Freshness,
    rawFields: Map[String, String],
    evidence: EvidenceBundle
) derives CanEqual

enum ObservationResult derives CanEqual:
  case Observed(value: JobObservation)
  case NotFound(job: JobRef, freshness: Freshness, evidence: EvidenceBundle)
  case Failed(job: JobRef, freshness: Freshness, diagnostics: Diagnostics, evidence: EvidenceBundle)
