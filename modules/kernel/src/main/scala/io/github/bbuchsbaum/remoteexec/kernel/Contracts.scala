package io.github.bbuchsbaum.remoteexec.kernel

import cats.data.NonEmptyVector

import java.time.Instant

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

enum Freshness derives CanEqual:
  case Current(observedAt: Instant)
  case Stale(observedAt: Instant, age: DurationMillis)
  case Unknown(lastAttemptAt: Instant, diagnostics: Diagnostics)

enum RetrySafety derives CanEqual:
  case Unknown
  case NoAutomaticRetry
  case SafeForAutomaticRetry

final case class ResultCodecFailure(code: String, message: String) derives CanEqual

trait ResultCodec[A]:
  def schemaId: ResultSchemaId
  def encode(value: A): Either[ResultCodecFailure, Vector[Byte]]
  def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, A]

trait InputCodec[A]:
  def schemaId: SchemaId
  def encode(value: A): Either[ResultCodecFailure, Vector[Byte]]
  def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, A]

enum SpawnFailureKind derives CanEqual:
  case ExecutableMissing
  case PermissionDenied
  case WorkingDirectoryMissing
  case EnvironmentInvalid
  case ResourceUnavailable
  case Unknown

  def stableCode: String = this match
    case ExecutableMissing       => "executable-missing"
    case PermissionDenied        => "permission-denied"
    case WorkingDirectoryMissing => "working-directory-missing"
    case EnvironmentInvalid      => "environment-invalid"
    case ResourceUnavailable     => "resource-unavailable"
    case Unknown                 => "unknown"

enum FailureCause derives CanEqual:
  case RequestPreparationFailed(diagnosticCodes: Vector[String])
  case SubmissionRejected(diagnosticCodes: Vector[String])
  case ProgramLaunchFailed(kind: SpawnFailureKind)
  case ProgramFailed(exitCode: Option[Int], diagnosticCodes: Vector[String])
  case OutOfMemory
  case TimeLimitExceeded
  case Cancelled
  case NodeFailure
  case Preempted
  case WorkerFailure(code: String)
  case ResultInvalid(diagnosticCodes: Vector[String])
  case RuntimeError(runtime: String)
  case FileNotFound
  case ProcessKilled

  def stableCode: String = this match
    case RequestPreparationFailed(_) => "request-preparation-failed"
    case SubmissionRejected(_)       => "submission-rejected"
    case ProgramLaunchFailed(_)      => "program-launch-failed"
    case ProgramFailed(_, _)         => "program-failed"
    case OutOfMemory                 => "out-of-memory"
    case TimeLimitExceeded           => "time-limit-exceeded"
    case Cancelled                   => "cancelled"
    case NodeFailure                 => "node-failure"
    case Preempted                   => "preempted"
    case WorkerFailure(_)            => "worker-failure"
    case ResultInvalid(_)            => "result-invalid"
    case RuntimeError(_)             => "runtime-error"
    case FileNotFound                => "file-not-found"
    case ProcessKilled               => "process-killed"

  /** Stable total-order key for diagnostics and persistence.
    *
    * This key is deliberately independent of enum names, source order, and `toString`. Variable
    * text is length-prefixed so distinct payloads cannot collide through a delimiter.
    */
  def deterministicKey: String = this match
    case RequestPreparationFailed(codes) =>
      s"$stableCode:${FailureCause.vectorKey(codes)}"
    case SubmissionRejected(codes) =>
      s"$stableCode:${FailureCause.vectorKey(codes)}"
    case ProgramLaunchFailed(kind) =>
      s"$stableCode:${kind.stableCode}"
    case ProgramFailed(exitCode, codes) =>
      s"$stableCode:${exitCode.fold("none")(_.toString)}:${FailureCause.vectorKey(codes)}"
    case WorkerFailure(value) =>
      s"$stableCode:${FailureCause.textKey(value)}"
    case ResultInvalid(codes) =>
      s"$stableCode:${FailureCause.vectorKey(codes)}"
    case RuntimeError(runtime) =>
      s"$stableCode:${FailureCause.textKey(runtime)}"
    case _ => stableCode

object FailureCause:
  private def textKey(value: String): String = s"${value.length}:$value"

  private def vectorKey(values: Vector[String]): String =
    values.sorted.map(textKey).mkString

final case class FailureDiagnosis(
    primary: FailureCause,
    confirmedContributors: Vector[FailureCause],
    suspectedContributors: Vector[FailureCause]
) derives CanEqual

final case class WorkerRelease(
    id: WorkerReleaseId,
    digest: ContentDigest
) derives CanEqual
