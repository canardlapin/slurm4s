package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.ResultEnvelopeCodec

import java.nio.file.Files
import java.time.Instant

class ResultAttachmentSuite extends munit.CatsEffectSuite:
  import ManagedTestSupport.*

  private val resultLimit = ByteLimit.from(4096).toOption.get
  private val envelopeLimit = ByteLimit.from(65536).toOption.get
  private val outputPath = RelativeOutputPath.from("results/value.txt").toOption.get
  private val outputDigest = ContentDigest
    .from("sha256:e0ee8bb50685e05fa0f47ed04203ae953fdfd055f5bd2892ea186504254f8c3a")
    .toOption
    .get
  private val output = OutputEntry.from(outputPath, 8L, outputDigest).toOption.get
  private val release = WorkerRelease(
    WorkerReleaseId.from("managed-worker-1").toOption.get,
    ContentDigest
      .from("sha256:ff828dfdff707e2276692cf65d461572268370fdebfd7b584be0dc0efa5e08d7")
      .toOption
      .get
  )
  private val operation = RegisteredOperation(
    OperationId.from("example.typed").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("example.input.v1").toOption.get,
    ResultSchemaId.from("example.string.v1").toOption.get
  )
  private val contract = ResultContract.Structured
    .from(stringCodec(operation.outputSchema), resultLimit, Vector(outputPath))
    .toOption
    .get

  test("typed reattachment accepts only a schema and output validated value") {
    val attempt = boundAttempt
    val handle = durableHandle(attempt, contract)
    val envelope = successfulEnvelope(handle, OutputManifest.from(Vector(output)).toOption.get)
    val bytes = ResultEnvelopeCodec.encode(envelope, envelopeLimit, resultLimit).toOption.get

    ResultAttachment.attach(attempt, handle, contract, bytes, Vector(output), later) match
      case ExecutionResult.Succeeded(value, outputs, _) =>
        assertEquals(value, "typed-value")
        assertEquals(outputs.entries, Vector(output))
      case other => fail(s"expected typed success, received $other")
  }

  test("verified attachment returns the exact bytes and complete immutable binding") {
    val attempt = boundAttempt
    val handle = durableHandle(attempt, contract)
    val envelope = successfulEnvelope(handle, OutputManifest.from(Vector(output)).toOption.get)
    val bytes = ResultEnvelopeCodec.encode(envelope, envelopeLimit, resultLimit).toOption.get

    ResultAttachment.attachVerified(attempt, handle, contract, bytes, Vector(output), later) match
      case VerifiedAttachment.Succeeded(payload) =>
        assertEquals(payload.value, "typed-value")
        assertEquals(payload.encodedValue, "typed-value".getBytes("UTF-8").toVector)
        assertEquals(payload.submissionKey, handle.submissionKey)
        assertEquals(payload.attemptId, handle.attemptId)
        assertEquals(payload.attemptEpoch, handle.attemptEpoch)
        assertEquals(payload.job, handle.job)
        assertEquals(payload.operation, handle.operation)
        assertEquals(payload.resultSchema, handle.resultSchema)
        assertEquals(payload.workerRelease, handle.workerRelease)
        assertEquals(payload.outputs.entries, Vector(output))
      case other => fail(s"expected verified payload, received $other")
  }

  test("replacing the envelope after file attachment cannot change verified value bytes") {
    val attempt = boundAttempt
    val handle = durableHandle(attempt, contract)
    val envelope = successfulEnvelope(handle, OutputManifest.from(Vector(output)).toOption.get)
    val bytes = ResultEnvelopeCodec.encode(envelope, envelopeLimit, resultLimit).toOption.get
    IO.blocking(Files.createTempFile("slurm4s-verified-envelope", ".json"))
      .bracket { path =>
        for
          _ <- IO.blocking { val _ = Files.write(path, bytes.toArray) }
          attached <- ResultAttachment.attachFileVerified[IO, String](
            attempt,
            handle,
            contract,
            path,
            Vector(output),
            later
          )
          _ <- IO.blocking { val _ = Files.write(path, "replacement".getBytes("UTF-8")) }
        yield attached match
          case VerifiedAttachment.Succeeded(payload) =>
            assertEquals(payload.encodedValue, "typed-value".getBytes("UTF-8").toVector)
          case other => fail(s"expected verified payload, received $other")
      }(path => IO.blocking { val _ = Files.deleteIfExists(path) })
  }

  test("wrong-schema reattachment fails before malformed envelope bytes are decoded") {
    val attempt = boundAttempt
    val handle = durableHandle(attempt, contract)
    val wrongContract = ResultContract.Structured
      .from(
        stringCodec(ResultSchemaId.from("example.other.v1").toOption.get),
        resultLimit,
        Vector(outputPath)
      )
      .toOption
      .get

    val result = ResultAttachment.attach(
      attempt,
      handle,
      wrongContract,
      "not-json".getBytes("UTF-8").toVector,
      Vector.empty,
      later
    )
    assertEquals(invalidCode(result), "result-schema-mismatch")
  }

  test("malformed envelope, missing output, and stale epochs never become typed success") {
    val attempt = boundAttempt
    val handle = durableHandle(attempt, contract)
    val malformed = ResultAttachment.attach(
      attempt,
      handle,
      contract,
      "not-json".getBytes("UTF-8").toVector,
      Vector(output),
      later
    )
    val noOutputs = successfulEnvelope(handle, OutputManifest.empty)
    val missing = ResultAttachment.attach(
      attempt,
      handle,
      contract,
      ResultEnvelopeCodec.encode(noOutputs, envelopeLimit, resultLimit).toOption.get,
      Vector.empty,
      later
    )
    val staleHandle = rebuilt(handle, attemptEpoch = AttemptEpoch.from(2L).toOption)
    val stale = ResultAttachment.attach(
      attempt,
      staleHandle,
      contract,
      Vector.empty,
      Vector.empty,
      later
    )

    assertEquals(invalidCode(malformed), "malformed-result-envelope")
    assertEquals(invalidCode(missing), "output-validation-failed")
    assertEquals(invalidCode(stale), "stale-result-epoch")
  }

  test("resubmission fences old result handles and retry-safety provenance") {
    val bound = boundState
    val original = bound.attempts.values.head
    val originalHandle = durableHandle(original, contract)
    val record = AccountingRecord(
      job,
      SlurmState.NodeFailure,
      None,
      Some(WorkloadOutcome.NodeFailure),
      Freshness.Current(later),
      Map.empty,
      evidence
    )
    val terminal = applyCommand(
      bound,
      ControlCommand.RecordAccounting(
        cats.data.NonEmptyVector.one(job),
        SchedulerQueryResult.Succeeded(
          AccountingBatch(cats.data.NonEmptyVector.one(record), Vector.empty)
        ),
        later.plusSeconds(1L)
      )
    )
    val retried = applyCommand(
      terminal.state,
      ControlCommand.RetrySubmission(
        original.intent.submissionKey,
        original.intent.epoch,
        RetryAuthorization.Manual(
          RetryReason.from("operator authorized replacement").toOption.get
        ),
        later.plusSeconds(2L)
      )
    )
    val current = retried.state.attempts(original.intent.submissionKey)
    val stale = ResultAttachment.attach(
      current,
      originalHandle,
      contract,
      Vector.empty,
      Vector.empty,
      later.plusSeconds(3L)
    )
    val provenanceMismatch = ResultAttachment.attach(
      original,
      rebuilt(originalHandle, retrySafety = Some(RetrySafety.SafeForAutomaticRetry)),
      contract,
      Vector.empty,
      Vector.empty,
      later.plusSeconds(3L)
    )

    assertEquals(current.currentJob, None)
    assertEquals(invalidCode(stale), "stale-result-epoch")
    assertEquals(invalidCode(provenanceMismatch), "retry-safety-mismatch")
  }

  test("worker failure envelope remains a structured workload failure") {
    val attempt = boundAttempt
    val handle = durableHandle(attempt, contract)
    val envelope = ResultEnvelope.failed(
      handle.submissionKey,
      handle.attemptId,
      handle.attemptEpoch,
      handle.job,
      handle.operation,
      handle.resultSchema,
      "TaskRaised",
      "the registered task raised an exception",
      OutputManifest.empty,
      handle.workerRelease,
      later
    )
    val bytes = ResultEnvelopeCodec.encode(envelope, envelopeLimit, resultLimit).toOption.get

    ResultAttachment.attach(attempt, handle, contract, bytes, Vector.empty, later) match
      case ExecutionResult.WorkloadFailed(WorkloadOutcome.Failed(_, diagnostics), _) =>
        assertEquals(diagnostics.toVector.head.code, "worker-task-failed")
      case other => fail(s"expected structured workload failure, received $other")
  }

  test("file reattachment bounds reads and classifies an unavailable file") {
    val attempt = boundAttempt
    // Both bounds shrink together: an envelope smaller than the result it must carry is now a
    // rejected state rather than a constructible one.
    val handle = rebuilt(
      durableHandle(attempt, contract),
      maximumResultBytes = ByteLimit.from(16).toOption,
      maximumEnvelopeBytes = ByteLimit.from(16).toOption
    )
    IO.blocking(Files.createTempFile("slurm4s-envelope", ".json"))
      .bracket { path =>
        for
          _ <- IO.blocking { val _ = Files.write(path, Array.fill[Byte](64)(1)) }
          oversized <- ResultAttachment.attachFile[IO, String](
            attempt,
            handle,
            contract,
            path,
            Vector.empty,
            later
          )
          _ <- IO.blocking { val _ = Files.deleteIfExists(path) }
          unavailable <- ResultAttachment.attachFile[IO, String](
            attempt,
            handle,
            contract,
            path,
            Vector.empty,
            later
          )
        yield
          assertEquals(invalidCode(oversized), "malformed-result-envelope")
          unavailable match
            case ExecutionResult.Indeterminate(diagnostics, _) =>
              assertEquals(diagnostics.toVector.head.code, "result-envelope-unavailable")
            case other => fail(s"expected unavailable result, received $other")
      }(path => IO.blocking { val _ = Files.deleteIfExists(path) })
  }

  private def boundAttempt: ManagedAttempt =
    boundState.attempts.values.head

  private def boundState: ControlState =
    val value = intent("typed-result")
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value)).state
    val claimed = applyCommand(
      recorded,
      ControlCommand.ClaimSubmission(value.submissionKey, instant)
    ).state
    applyCommand(
      claimed,
      ControlCommand.RecordSubmission(
        value.submissionKey,
        value.epoch,
        accepted,
        later
      )
    ).state

  private def durableHandle[A](
      attempt: ManagedAttempt,
      value: ResultContract.Structured[A]
  ): DurableResultHandle =
    ManagedResultHandle
      .registered(attempt, operation, value, envelopeLimit, release)
      .toOption
      .get

  private def successfulEnvelope(handle: DurableResultHandle, outputs: OutputManifest) =
    ResultEnvelope.succeeded(
      handle.submissionKey,
      handle.attemptId,
      handle.attemptEpoch,
      handle.job,
      handle.operation,
      handle.resultSchema,
      "typed-value".getBytes("UTF-8").toVector,
      outputs,
      handle.workerRelease,
      Instant.parse("2026-07-22T12:00:02Z")
    )

  private def stringCodec(schema: ResultSchemaId): ResultCodec[String] = new ResultCodec[String]:
    val schemaId: ResultSchemaId = schema
    def encode(value: String): Either[ResultCodecFailure, Vector[Byte]] =
      Right(value.getBytes("UTF-8").toVector)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, "UTF-8"))

  private def invalidCode[A](result: ExecutionResult[A]): String = result match
    case ExecutionResult.ResultInvalid(diagnostics, _) => diagnostics.toVector.head.code
    case other => fail(s"expected invalid result, received $other")

  /** Rebuild a handle with overrides.
    *
    * `copy` is private now: a handle's bounds are its invariant, so every change goes back through
    * the validating constructor rather than around it.
    */
  private def rebuilt(
      handle: DurableResultHandle,
      attemptEpoch: Option[AttemptEpoch] = None,
      retrySafety: Option[RetrySafety] = None,
      maximumResultBytes: Option[ByteLimit] = None,
      maximumEnvelopeBytes: Option[ByteLimit] = None
  ): DurableResultHandle =
    DurableResultHandle
      .from(
        handle.submissionKey,
        handle.attemptId,
        attemptEpoch.getOrElse(handle.attemptEpoch),
        handle.job,
        handle.operation,
        handle.resultSchema,
        maximumResultBytes.getOrElse(handle.maximumResultBytes),
        maximumEnvelopeBytes.getOrElse(handle.maximumEnvelopeBytes),
        handle.declaredOutputs,
        handle.workerRelease,
        retrySafety.getOrElse(handle.retrySafety)
      )
      .fold(problem => fail(problem.reason), identity)
