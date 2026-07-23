package io.github.bbuchsbaum.scalaslurm.core

import cats.data.NonEmptyVector

final case class Diagnostic(code: String, message: String, fields: Map[String, String] = Map.empty)
    derives CanEqual

final case class Diagnostics private (values: NonEmptyVector[Diagnostic]) derives CanEqual:
  def toVector: Vector[Diagnostic] = values.toVector

object Diagnostics:
  def one(diagnostic: Diagnostic): Diagnostics = Diagnostics(NonEmptyVector.one(diagnostic))

  def fromVector(values: Vector[Diagnostic]): Either[ValidationFailure, Diagnostics] =
    NonEmptyVector
      .fromVector(values)
      .map(Diagnostics(_))
      .toRight(ValidationFailure("diagnostics", "must contain at least one diagnostic"))

enum SpawnFailureKind derives CanEqual:
  case ExecutableMissing
  case PermissionDenied
  case WorkingDirectoryMissing
  case EnvironmentInvalid
  case ResourceUnavailable
  case Unknown

enum InvocationResult derives CanEqual:
  case Exited(exitCode: Int, stdout: BoundedEvidence, stderr: BoundedEvidence)
  case SpawnFailed(kind: SpawnFailureKind, diagnostics: Diagnostics, evidence: EvidenceBundle)
  case TimedOut(after: DurationMillis, stdout: BoundedEvidence, stderr: BoundedEvidence)

final case class JobRef(jobId: JobId, cluster: Option[ClusterName], arrayIndex: Option[ArrayIndex])
    derives CanEqual

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
