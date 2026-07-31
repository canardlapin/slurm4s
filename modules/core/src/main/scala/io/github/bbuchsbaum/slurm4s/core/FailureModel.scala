package io.github.bbuchsbaum.slurm4s.core

enum InvocationResult derives CanEqual:
  case Exited(exitCode: Int, stdout: BoundedEvidence, stderr: BoundedEvidence)
  case SpawnFailed(kind: SpawnFailureKind, diagnostics: Diagnostics, evidence: EvidenceBundle)
  case TimedOut(after: DurationMillis, stdout: BoundedEvidence, stderr: BoundedEvidence)

/** Operational identity for matching a scheduler report to a submitted job.
  *
  * Cluster is deliberately excluded. `sbatch --parsable` yields a bare job id on a non-federated
  * site while `squeue` JSON reports a cluster name, so folding cluster into the match key aliases
  * every observation away from the attempt that submitted it.
  */
final case class JobKey(jobId: JobId, arrayIndex: Option[ArrayIndex]) derives CanEqual

final case class JobRef(jobId: JobId, cluster: Option[ClusterName], arrayIndex: Option[ArrayIndex])
    derives CanEqual:

  /** Match on this, never on whole-`JobRef` equality. A reported cluster is evidence, not identity.
    */
  def key: JobKey = JobKey(jobId, arrayIndex)

  /** True only when both clusters are known and disagree, which makes these genuinely different
    * jobs. An unknown cluster on either side is not a conflict, because absence is the normal
    * result of submitting without federation. Callers must diagnose a conflict, never drop it.
    */
  def clusterConflictsWith(other: JobRef): Boolean =
    (cluster, other.cluster) match
      case (Some(left), Some(right)) => left != right
      case _                         => false

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
