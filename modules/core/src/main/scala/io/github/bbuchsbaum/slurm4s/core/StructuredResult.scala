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

/** How to read a result of type `A`: the typed half of a prepared workload.
  *
  * The schema is explicit rather than taken from the contract, because it is the *operation* that
  * names the result schema — an exit-only contract has no schema of its own, yet the attempt still
  * has one to record.
  */
final case class ResultPlan[A](
    schema: ResultSchemaId,
    contract: ResultContract[A],
    maximumResultBytes: ByteLimit,
    maximumEnvelopeBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath]
)

/** Reusable prepared content: what to launch, and how to read what it produces.
  *
  * Deliberately carries no attempt identity. A retry is a new attempt at the *same* prepared job,
  * so baking an attempt into this would force re-preparation — and re-digesting — for something the
  * launch never changed.
  */
final case class PreparedJob[A](
    launch: LaunchSpec,
    resultPlan: ResultPlan[A],
    digest: ContentDigest
)

/** One attempt at a prepared job: the prepared content plus the identity that fences it. */
final case class PreparedAttempt[A](
    job: PreparedJob[A],
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    operation: WorkloadOperation,
    workerRelease: WorkerRelease
):

  /** The durable, type-erased projection of this attempt.
    *
    * Derived rather than constructed alongside the plan. Building the two independently is how a
    * handle comes to disagree with the plan it is supposed to describe — a handle claiming one
    * result schema while the codec decodes another, or limits that drifted apart.
    */
  def durableHandle(bound: Option[JobRef]): DurableResultHandle =
    DurableResultHandle(
      job.launch.submissionKey,
      attemptId,
      epoch,
      bound,
      operation,
      job.resultPlan.schema,
      job.resultPlan.maximumResultBytes,
      job.resultPlan.maximumEnvelopeBytes,
      job.resultPlan.declaredOutputs,
      workerRelease,
      job.launch.retrySafety
    )

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
