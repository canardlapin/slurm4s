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
  case Unknown(raw: String)

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
    case SlurmState.Pending | SlurmState.Running | SlurmState.Completing | SlurmState.Completed =>
      InterruptionClass.NotInterrupted
    case SlurmState.Requeued | SlurmState.RequeueHeld | SlurmState.RequeueFederation |
        SlurmState.SpecialExit =>
      InterruptionClass.Requeueing
    case SlurmState.NodeFailure =>
      InterruptionClass.InfrastructureFailure
    case SlurmState.Preempted | SlurmState.TimedOut =>
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
    timing: JobTiming = JobTiming.unknown
) derives CanEqual

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
