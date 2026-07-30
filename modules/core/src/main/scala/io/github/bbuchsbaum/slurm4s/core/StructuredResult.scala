package io.github.bbuchsbaum.slurm4s.core

import cats.data.NonEmptyVector

import java.time.Instant

enum WorkloadOperation derives CanEqual:
  case Script(sourceDigest: ContentDigest)
  case Registered(id: OperationId, version: OperationVersion)

enum ResultEnvelopeStatus derives CanEqual:
  case Succeeded
  case Failed(code: String, message: String)

final case class ResultEnvelope private (
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    job: Option[JobRef],
    operation: WorkloadOperation,
    resultSchema: ResultSchemaId,
    status: ResultEnvelopeStatus,
    value: Option[Vector[Byte]],
    outputs: OutputManifest,
    workerRelease: WorkerRelease,
    completedAt: Instant
) derives CanEqual

final case class DurableResultHandle(
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    job: Option[JobRef],
    operation: WorkloadOperation,
    resultSchema: ResultSchemaId,
    maximumResultBytes: ByteLimit,
    maximumEnvelopeBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath],
    workerRelease: WorkerRelease,
    retrySafety: RetrySafety = RetrySafety.Unknown
) derives CanEqual

final case class TaskInvocation(
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    job: Option[JobRef],
    operation: RegisteredOperation,
    inputBytes: Vector[Byte],
    declaredOutputs: Vector[RelativeOutputPath],
    maximumInputBytes: ByteLimit,
    maximumResultBytes: ByteLimit,
    maximumEnvelopeBytes: ByteLimit,
    maximumOutputBytes: ByteLimit,
    workerRelease: WorkerRelease,
    retrySafety: RetrySafety = RetrySafety.Unknown
) derives CanEqual

object ResultEnvelope:
  def succeeded(
      submissionKey: SubmissionKey,
      attemptId: AttemptId,
      attemptEpoch: AttemptEpoch,
      job: Option[JobRef],
      operation: WorkloadOperation,
      resultSchema: ResultSchemaId,
      value: Vector[Byte],
      outputs: OutputManifest,
      workerRelease: WorkerRelease,
      completedAt: Instant
  ): ResultEnvelope =
    ResultEnvelope(
      submissionKey,
      attemptId,
      attemptEpoch,
      job,
      operation,
      resultSchema,
      ResultEnvelopeStatus.Succeeded,
      Some(value),
      outputs,
      workerRelease,
      completedAt
    )

  def failed(
      submissionKey: SubmissionKey,
      attemptId: AttemptId,
      attemptEpoch: AttemptEpoch,
      job: Option[JobRef],
      operation: WorkloadOperation,
      resultSchema: ResultSchemaId,
      code: String,
      message: String,
      outputs: OutputManifest,
      workerRelease: WorkerRelease,
      completedAt: Instant
  ): ResultEnvelope =
    ResultEnvelope(
      submissionKey,
      attemptId,
      attemptEpoch,
      job,
      operation,
      resultSchema,
      ResultEnvelopeStatus.Failed(code, message),
      None,
      outputs,
      workerRelease,
      completedAt
    )

  private[slurm4s] def decoded(
      submissionKey: SubmissionKey,
      attemptId: AttemptId,
      attemptEpoch: AttemptEpoch,
      job: Option[JobRef],
      operation: WorkloadOperation,
      resultSchema: ResultSchemaId,
      status: ResultEnvelopeStatus,
      value: Option[Vector[Byte]],
      outputs: OutputManifest,
      workerRelease: WorkerRelease,
      completedAt: Instant
  ): Either[String, ResultEnvelope] =
    status match
      case ResultEnvelopeStatus.Succeeded if value.isEmpty =>
        Left("a successful result envelope must contain a value")
      case _: ResultEnvelopeStatus.Failed if value.nonEmpty =>
        Left("a failed result envelope must not contain a value")
      case _ =>
        Right(
          ResultEnvelope(
            submissionKey,
            attemptId,
            attemptEpoch,
            job,
            operation,
            resultSchema,
            status,
            value,
            outputs,
            workerRelease,
            completedAt
          )
        )

final case class ProgressEvent(
    message: String,
    completed: Option[Long] = None,
    total: Option[Long] = None,
    fields: Map[String, String] = Map.empty
) derives CanEqual

enum WorkerEventPayload derives CanEqual:
  case Started
  case Progress(value: ProgressEvent)
  case ProcessExited(exitCode: Int)
  case ResultPublished(envelopeDigest: ContentDigest)
  case Failed(code: String, message: String)

final case class WorkerEvent(
    sequence: Long,
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    operation: WorkloadOperation,
    workerRelease: WorkerRelease,
    observedAt: Instant,
    payload: WorkerEventPayload
) derives CanEqual

enum OutputValidationFailure derives CanEqual:
  case DuplicateExpected(path: RelativeOutputPath)
  case DuplicateReported(path: RelativeOutputPath)
  case DuplicateObserved(path: RelativeOutputPath)
  case Missing(path: RelativeOutputPath)
  case Unexpected(path: RelativeOutputPath)
  case SizeMismatch(path: RelativeOutputPath, reported: Long, observed: Long)
  case DigestMismatch(
      path: RelativeOutputPath,
      reported: ContentDigest,
      observed: ContentDigest
  )

object OutputValidation:
  def verify(
      expected: Vector[RelativeOutputPath],
      reported: Vector[OutputEntry],
      observed: Vector[OutputEntry]
  ): Either[NonEmptyVector[OutputValidationFailure], OutputManifest] =
    val failures =
      duplicates(expected).map(OutputValidationFailure.DuplicateExpected.apply) ++
        duplicates(reported.map(_.path)).map(OutputValidationFailure.DuplicateReported.apply) ++
        duplicates(observed.map(_.path)).map(OutputValidationFailure.DuplicateObserved.apply) ++
        pathFailures(expected, reported, observed) ++
        contentFailures(reported, observed)

    NonEmptyVector.fromVector(failures) match
      case Some(values) => Left(values)
      case None         => Right(OutputManifest.verified(observed))

  private def pathFailures(
      expected: Vector[RelativeOutputPath],
      reported: Vector[OutputEntry],
      observed: Vector[OutputEntry]
  ): Vector[OutputValidationFailure] =
    val expectedSet = expected.toSet
    val reportedSet = reported.map(_.path).toSet
    val observedSet = observed.map(_.path).toSet
    val missing = (expectedSet -- reportedSet ++ (expectedSet -- observedSet)).toVector.distinct
    val unexpected = (reportedSet -- expectedSet ++ (observedSet -- expectedSet)).toVector.distinct
    missing.sorted.map(OutputValidationFailure.Missing.apply) ++
      unexpected.sorted.map(OutputValidationFailure.Unexpected.apply)

  private def contentFailures(
      reported: Vector[OutputEntry],
      observed: Vector[OutputEntry]
  ): Vector[OutputValidationFailure] =
    val reportedByPath = reported.map(value => value.path -> value).toMap
    val observedByPath = observed.map(value => value.path -> value).toMap
    (reportedByPath.keySet intersect observedByPath.keySet).toVector.sorted.flatMap { path =>
      val claimed = reportedByPath(path)
      val actual = observedByPath(path)
      Vector(
        Option.when(claimed.sizeBytes != actual.sizeBytes)(
          OutputValidationFailure.SizeMismatch(path, claimed.sizeBytes, actual.sizeBytes)
        ),
        Option.when(claimed.digest != actual.digest)(
          OutputValidationFailure.DigestMismatch(path, claimed.digest, actual.digest)
        )
      ).flatten
    }

  private def duplicates(values: Vector[RelativeOutputPath]): Vector[RelativeOutputPath] =
    values.groupBy(identity).collect { case (path, matches) if matches.size > 1 => path }.toVector
