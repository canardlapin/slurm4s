package io.github.bbuchsbaum.slurm4s.core

enum InvocationResult derives CanEqual:
  case Exited(exitCode: Int, stdout: BoundedEvidence, stderr: BoundedEvidence)
  case SpawnFailed(kind: SpawnFailureKind, diagnostics: Diagnostics, evidence: EvidenceBundle)
  case TimedOut(after: DurationMillis, stdout: BoundedEvidence, stderr: BoundedEvidence)

/** A job as this library identifies it: base id plus optional array element.
  *
  * Deliberately single-cluster. A cluster name used to sit here, in equality and in persistence,
  * while observation, accounting, cancellation and parser attribution all ignored it — so a bound
  * attempt carrying no cluster never matched a report carrying one, and every managed observation
  * was silently discarded (P8.A1).
  *
  * Federation and cross-cluster routing are UNSUPPORTED for v0.1. Genuine scoped identity requires
  * grouping, CLI routing (`-M`/`--clusters`), parser attribution, persistence and cancellation to
  * land together; carrying a decorative field until then bought nothing and cost correctness. A
  * cluster the scheduler reports is preserved as evidence on the observation, not as identity.
  */
final case class JobRef(jobId: JobId, arrayIndex: Option[ArrayIndex]) derives CanEqual

enum AcceptanceUncertainty derives CanEqual:
  case ResponseLost
  case TransportInterrupted
  case ResponseUnparseable
  case PersistenceInterrupted
  case Unclassified

enum Submission derives CanEqual:
  case Accepted(job: JobRef, evidence: EvidenceBundle)
  case Rejected(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case AcceptanceUnknown(reason: AcceptanceUncertainty, evidence: EvidenceBundle)

enum SubmissionAttempt derives CanEqual:
  case Completed(value: Submission)
  case InvocationFailed(result: InvocationResult)
  case PreparationFailed(diagnostics: Diagnostics)

enum SchedulerQueryResult[+A]:
  case Succeeded(value: A)
  case Empty(observedAt: java.time.Instant, evidence: EvidenceBundle)
  case InvocationFailed(result: InvocationResult)
  case ParseFailed(diagnostics: Diagnostics, evidence: EvidenceBundle)

enum CancellationResult derives CanEqual:
  case Acknowledged(evidence: EvidenceBundle)
  case NotFound(evidence: EvidenceBundle)
  case Rejected(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case Unknown(diagnostics: Diagnostics, evidence: EvidenceBundle)

enum CancellationAttempt derives CanEqual:
  case Completed(value: CancellationResult)
  case InvocationFailed(result: InvocationResult)
