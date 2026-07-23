package io.github.bbuchsbaum.scalaslurm.core

import java.time.Instant

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
  case Unknown(raw: String)

enum Freshness derives CanEqual:
  case Current(observedAt: Instant)
  case Stale(observedAt: Instant, age: DurationMillis)
  case Unknown(lastAttemptAt: Instant, diagnostics: Diagnostics)

final case class JobObservation(
    job: JobRef,
    state: SlurmState,
    freshness: Freshness,
    reason: Option[String],
    rawFields: Map[String, String],
    evidence: EvidenceBundle
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
