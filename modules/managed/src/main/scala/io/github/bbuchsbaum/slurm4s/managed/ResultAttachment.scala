package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.kernel.Async
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.ResultEnvelopeCodec

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant

/** A successfully decoded value together with the exact validated bytes and every binding checked
  * by result attachment.
  *
  * The byte vector is immutable. Callers may persist it without reopening the envelope, and a later
  * replacement of the result path cannot alter what was verified.
  */
final case class VerifiedResultPayload[+A](
    value: A,
    encodedValue: Vector[Byte],
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    job: Option[JobRef],
    operation: WorkloadOperation,
    resultSchema: ResultSchemaId,
    workerRelease: WorkerRelease,
    outputs: OutputManifest,
    evidence: EvidenceBundle
)

enum VerifiedAttachment[+A]:
  case Succeeded(payload: VerifiedResultPayload[A])
  case WorkloadFailed(outcome: WorkloadOutcome, evidence: EvidenceBundle)
  case ResultInvalid(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case Indeterminate(diagnostics: Diagnostics, evidence: EvidenceBundle)

  def toExecutionResult: ExecutionResult[A] = this match
    case Succeeded(payload) =>
      ExecutionResult.Succeeded(payload.value, payload.outputs, payload.evidence)
    case WorkloadFailed(outcome, evidence) =>
      ExecutionResult.WorkloadFailed(outcome, evidence)
    case ResultInvalid(diagnostics, evidence) =>
      ExecutionResult.ResultInvalid(diagnostics, evidence)
    case Indeterminate(diagnostics, evidence) =>
      ExecutionResult.Indeterminate(diagnostics, evidence)

object ManagedResultHandle:
  def registered[A](
      attempt: ManagedAttempt,
      operation: RegisteredOperation,
      contract: ResultContract.Structured[A],
      maximumEnvelopeBytes: ByteLimit,
      workerRelease: WorkerRelease
  ): Either[ValidationFailure, DurableResultHandle] =
    Either.cond(
      operation.outputSchema == contract.codec.schemaId,
      DurableResultHandle(
        attempt.intent.submissionKey,
        attempt.intent.attemptId,
        attempt.intent.epoch,
        attempt.currentJob,
        WorkloadOperation.Registered(operation.id, operation.version),
        contract.codec.schemaId,
        contract.maxResultBytes,
        maximumEnvelopeBytes,
        contract.outputs,
        workerRelease,
        attempt.intent.retrySafety
      ),
      ValidationFailure("resultSchema", "operation and result contract schemas do not match")
    )

object ResultAttachment:
  /** Compatibility API returning the historical value-only result. New storage consumers should use
    * [[attachFileVerified]] so the validated bytes are not reread.
    */
  def attachFile[F[_]: Async, A](
      attempt: ManagedAttempt,
      handle: DurableResultHandle,
      contract: ResultContract.Structured[A],
      envelopePath: Path,
      observedOutputs: Vector[OutputEntry],
      observedAt: Instant
  ): F[ExecutionResult[A]] =
    attachFileVerified(
      attempt,
      handle,
      contract,
      envelopePath,
      observedOutputs,
      observedAt
    ).map(_.toExecutionResult)

  def attachFileVerified[F[_]: Async, A](
      attempt: ManagedAttempt,
      handle: DurableResultHandle,
      contract: ResultContract.Structured[A],
      envelopePath: Path,
      observedOutputs: Vector[OutputEntry],
      observedAt: Instant
  ): F[VerifiedAttachment[A]] =
    Async[F].blocking(readBounded(envelopePath, handle.maximumEnvelopeBytes)).attempt.map {
      case Right(bytes) =>
        attachVerified(attempt, handle, contract, bytes, observedOutputs, observedAt)
      case Left(error) =>
        val evidence = EvidenceBundle(
          BoundedEvidence.capture(
            EvidenceSource.ResultEnvelope,
            observedAt,
            Vector.empty,
            handle.maximumEnvelopeBytes
          )
        )
        VerifiedAttachment.Indeterminate(
          Diagnostics.one(
            Diagnostic(
              "result-envelope-unavailable",
              Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
            )
          ),
          evidence
        )
    }

  def attach[A](
      attempt: ManagedAttempt,
      handle: DurableResultHandle,
      contract: ResultContract.Structured[A],
      envelopeBytes: Vector[Byte],
      observedOutputs: Vector[OutputEntry],
      observedAt: Instant
  ): ExecutionResult[A] =
    attachVerified(
      attempt,
      handle,
      contract,
      envelopeBytes,
      observedOutputs,
      observedAt
    ).toExecutionResult

  def attachVerified[A](
      attempt: ManagedAttempt,
      handle: DurableResultHandle,
      contract: ResultContract.Structured[A],
      envelopeBytes: Vector[Byte],
      observedOutputs: Vector[OutputEntry],
      observedAt: Instant
  ): VerifiedAttachment[A] =
    val evidence = EvidenceBundle(
      BoundedEvidence.capture(
        EvidenceSource.ResultEnvelope,
        observedAt,
        envelopeBytes,
        handle.maximumEnvelopeBytes
      )
    )

    preflight(attempt, handle, contract) match
      case Some(diagnostic) =>
        VerifiedAttachment.ResultInvalid(Diagnostics.one(diagnostic), evidence)
      case None =>
        ResultEnvelopeCodec.decode(
          envelopeBytes,
          handle.maximumEnvelopeBytes,
          handle.maximumResultBytes
        ) match
          case Left(failure) =>
            invalid("malformed-result-envelope", failure.toString, evidence)
          case Right(envelope) =>
            validateEnvelope(handle, envelope) match
              case Some(diagnostic) =>
                VerifiedAttachment.ResultInvalid(Diagnostics.one(diagnostic), evidence)
              case None =>
                envelope.status match
                  case ResultEnvelopeStatus.Failed(code, message) =>
                    VerifiedAttachment.WorkloadFailed(
                      WorkloadOutcome.Failed(
                        None,
                        Diagnostics.one(
                          Diagnostic("worker-task-failed", message, Map("workerCode" -> code))
                        )
                      ),
                      evidence
                    )
                  case ResultEnvelopeStatus.Succeeded =>
                    validateSuccess(handle, contract, envelope, observedOutputs, evidence)

  private def preflight[A](
      attempt: ManagedAttempt,
      handle: DurableResultHandle,
      contract: ResultContract.Structured[A]
  ): Option[Diagnostic] =
    if handle.resultSchema != contract.codec.schemaId then
      Some(
        Diagnostic(
          "result-schema-mismatch",
          "the durable handle and requested result contract have different schemas",
          Map(
            "handleSchema" -> handle.resultSchema.value,
            "contractSchema" -> contract.codec.schemaId.value
          )
        )
      )
    else if handle.submissionKey != attempt.intent.submissionKey ||
      handle.attemptId != attempt.intent.attemptId
    then Some(Diagnostic("result-handle-mismatch", "the durable handle identifies another attempt"))
    else if handle.retrySafety != attempt.intent.retrySafety then
      Some(
        Diagnostic(
          "retry-safety-mismatch",
          "the durable handle and managed intent have different retry-safety provenance"
        )
      )
    else
      EpochFence.validate(attempt, handle.attemptEpoch) match
        case EpochFence.Stale(expected, received) =>
          Some(
            Diagnostic(
              "stale-result-epoch",
              "the durable result handle has a stale attempt epoch",
              Map("expected" -> expected.value.toString, "received" -> received.value.toString)
            )
          )
        case EpochFence.Current => None

  private def validateEnvelope(
      handle: DurableResultHandle,
      envelope: ResultEnvelope
  ): Option[Diagnostic] =
    val mismatch = Vector(
      Option.when(envelope.submissionKey != handle.submissionKey)("submissionKey"),
      Option.when(envelope.attemptId != handle.attemptId)("attemptId"),
      Option.when(envelope.attemptEpoch != handle.attemptEpoch)("attemptEpoch"),
      Option.when(envelope.job != handle.job)("job"),
      Option.when(envelope.operation != handle.operation)("operation"),
      Option.when(envelope.resultSchema != handle.resultSchema)("resultSchema"),
      Option.when(envelope.workerRelease != handle.workerRelease)("workerRelease")
    ).flatten
    mismatch.headOption.map(field =>
      Diagnostic(
        "result-envelope-binding-mismatch",
        s"the result envelope does not match the durable handle field: $field",
        Map("field" -> field)
      )
    )

  private def validateSuccess[A](
      handle: DurableResultHandle,
      contract: ResultContract.Structured[A],
      envelope: ResultEnvelope,
      observedOutputs: Vector[OutputEntry],
      evidence: EvidenceBundle
  ): VerifiedAttachment[A] =
    OutputValidation.verify(
      contract.outputs,
      envelope.outputs.entries,
      observedOutputs
    ) match
      case Left(failures) =>
        invalid(
          "output-validation-failed",
          failures.toVector.mkString(", "),
          evidence
        )
      case Right(outputs) =>
        envelope.value match
          case None => invalid("result-value-missing", "successful envelope has no value", evidence)
          case Some(bytes) =>
            contract.codec.decode(bytes) match
              case Left(failure) => invalid(failure.code, failure.message, evidence)
              case Right(value)  =>
                VerifiedAttachment.Succeeded(
                  VerifiedResultPayload(
                    value,
                    bytes,
                    handle.submissionKey,
                    handle.attemptId,
                    handle.attemptEpoch,
                    handle.job,
                    handle.operation,
                    handle.resultSchema,
                    handle.workerRelease,
                    outputs,
                    evidence
                  )
                )

  private def invalid[A](
      code: String,
      message: String,
      evidence: EvidenceBundle
  ): VerifiedAttachment[A] =
    VerifiedAttachment.ResultInvalid(Diagnostics.one(Diagnostic(code, message)), evidence)

  private def readBounded(path: Path, maximum: ByteLimit): Vector[Byte] =
    val input = Files.newInputStream(path, StandardOpenOption.READ)
    val output = ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try
      var total = 0L
      var done = false
      while !done && total <= maximum.value.toLong do
        val requested = math.min(buffer.length.toLong, maximum.value.toLong - total + 1L).toInt
        val count = input.read(buffer, 0, requested)
        if count < 0 then done = true
        else
          output.write(buffer, 0, count)
          total += count.toLong
      output.toByteArray.toVector
    finally
      input.close()
      output.close()
