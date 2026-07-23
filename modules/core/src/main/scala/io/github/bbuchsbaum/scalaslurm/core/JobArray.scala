package io.github.bbuchsbaum.scalaslurm.core

import cats.data.NonEmptyChain
import cats.data.NonEmptyVector

final case class ArrayPlanFailure(code: String, message: String) derives CanEqual

final case class JobArrayRequest private (
    indices: NonEmptyVector[ArrayIndex],
    maximumConcurrent: Option[PositiveInt]
) derives CanEqual:
  def size: Int = indices.length.toInt

object JobArrayRequest:
  def from(
      indices: Vector[ArrayIndex],
      maximumConcurrent: Option[PositiveInt]
  ): Either[NonEmptyChain[ArrayPlanFailure], JobArrayRequest] =
    val failures = Vector(
      Option.when(indices.isEmpty)(ArrayPlanFailure("array-empty", "an array must have elements")),
      Option.when(indices.distinct.size != indices.size)(
        ArrayPlanFailure("array-index-duplicate", "array indices must be unique")
      ),
      Option.when(maximumConcurrent.exists(_.toInt > indices.size))(
        ArrayPlanFailure(
          "array-concurrency-too-large",
          "maximum concurrent elements cannot exceed array size"
        )
      )
    ).flatten
    NonEmptyChain.fromSeq(failures) match
      case Some(values) => Left(values)
      case None         =>
        Right(JobArrayRequest(NonEmptyVector.fromVectorUnsafe(indices), maximumConcurrent))

  def contiguous(
      size: PositiveInt,
      maximumConcurrent: Option[PositiveInt]
  ): Either[NonEmptyChain[ArrayPlanFailure], JobArrayRequest] =
    val indices = (0 until size.toInt).toVector.map(value => ArrayIndex.from(value).toOption.get)
    from(indices, maximumConcurrent)

final case class ArrayElementIdentity(
    index: ArrayIndex,
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    stdout: LogRef,
    stderr: LogRef,
    resultContract: ResultContractDescriptor,
    resultHandle: Option[DurableResultHandle]
) derives CanEqual

final case class ArrayElementBinding(identity: ArrayElementIdentity, job: JobRef) derives CanEqual:
  def stdout: LogRef = resolve(identity.stdout)
  def stderr: LogRef = resolve(identity.stderr)
  def resultHandle: Option[DurableResultHandle] = identity.resultHandle.map(_.copy(job = Some(job)))

  private def resolve(ref: LogRef): LogRef =
    ref.copy(
      locator = ref.locator
        .replace("%A", job.jobId.value)
        .replace("%a", identity.index.value.toString)
    )

final case class JobArrayPlan private (
    request: JobArrayRequest,
    elements: NonEmptyVector[ArrayElementIdentity]
) derives CanEqual:
  def bind(parent: JobRef): Either[ArrayPlanFailure, NonEmptyVector[ArrayElementBinding]] =
    Either.cond(
      parent.arrayIndex.isEmpty,
      elements.map { element =>
        ArrayElementBinding(
          element,
          JobRef(parent.jobId, parent.cluster, Some(element.index))
        )
      },
      ArrayPlanFailure("array-parent-is-element", "an array parent binding cannot have an index")
    )

object JobArrayPlan:
  def from(
      request: JobArrayRequest,
      elements: Vector[ArrayElementIdentity]
  ): Either[NonEmptyChain[ArrayPlanFailure], JobArrayPlan] =
    val indexes = elements.map(_.index)
    val contracts = elements.map(_.resultContract).distinct
    val failures = Vector(
      Option.when(elements.isEmpty)(
        ArrayPlanFailure("array-elements-empty", "elements are required")
      ),
      Option.when(indexes.distinct.size != indexes.size)(
        ArrayPlanFailure("array-element-index-duplicate", "element indices must be unique")
      ),
      Option.when(elements.map(_.submissionKey).distinct.size != elements.size)(
        ArrayPlanFailure("array-submission-key-duplicate", "element submission keys must be unique")
      ),
      Option.when(elements.map(_.attemptId).distinct.size != elements.size)(
        ArrayPlanFailure("array-attempt-id-duplicate", "element attempt ids must be unique")
      ),
      Option.when(indexes.toSet != request.indices.toVector.toSet)(
        ArrayPlanFailure(
          "array-index-set-mismatch",
          "request and element indices must match exactly"
        )
      ),
      Option.when(contracts.size > 1)(
        ArrayPlanFailure(
          "array-result-contract-mismatch",
          "all elements must have the same result contract"
        )
      )
    ).flatten ++ elements.flatMap(validateIdentity)

    NonEmptyChain.fromSeq(failures) match
      case Some(values) => Left(values)
      case None         => Right(JobArrayPlan(request, NonEmptyVector.fromVectorUnsafe(elements)))

  private def validateIdentity(element: ArrayElementIdentity): Vector[ArrayPlanFailure] =
    Vector(
      Option.when(
        element.stdout.attemptId != element.attemptId || element.stdout.epoch != element.epoch
      )(
        ArrayPlanFailure(
          "array-stdout-identity-mismatch",
          "stdout must carry the element attempt identity and epoch"
        )
      ),
      Option.when(
        element.stderr.attemptId != element.attemptId || element.stderr.epoch != element.epoch
      )(
        ArrayPlanFailure(
          "array-stderr-identity-mismatch",
          "stderr must carry the element attempt identity and epoch"
        )
      ),
      Option.when(element.stdout.stream != LogStream.Stdout)(
        ArrayPlanFailure("array-stdout-stream-mismatch", "stdout must identify the stdout stream")
      ),
      Option.when(element.stderr.stream != LogStream.Stderr)(
        ArrayPlanFailure("array-stderr-stream-mismatch", "stderr must identify the stderr stream")
      ),
      Option.when(
        element.resultHandle.exists(handle =>
          handle.submissionKey != element.submissionKey ||
            handle.attemptId != element.attemptId ||
            handle.attemptEpoch != element.epoch
        )
      )(
        ArrayPlanFailure(
          "array-result-identity-mismatch",
          "result handle must carry the element submission and attempt identity"
        )
      )
    ).flatten
