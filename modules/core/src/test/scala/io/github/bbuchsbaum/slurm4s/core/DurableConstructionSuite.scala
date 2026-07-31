package io.github.bbuchsbaum.slurm4s.core

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest

/** P8.A5b: the durable descriptions of an attempt are constructible only in valid states.
  *
  * Both types had public `apply`/`copy`, so a handle could declare the same output twice, or an
  * envelope limit smaller than the result it must contain, and an invocation could carry input
  * larger than the bound it declared for itself — including on decode, where every value arriving
  * from the wire was taken at face value.
  */
class DurableConstructionSuite extends munit.FunSuite:

  private val key = SubmissionKey.unsafeFrom("durable-construction")
  private val attemptId = AttemptId.unsafeFrom("durable-attempt")
  private val schema = ResultSchemaId.unsafeFrom("durable.result.v1")
  private val operation = WorkloadOperation.Registered(
    OperationId.unsafeFrom("durable.operation"),
    OperationVersion.unsafeFrom("1")
  )
  private val registered = RegisteredOperation(
    OperationId.unsafeFrom("durable.operation"),
    OperationVersion.unsafeFrom("1"),
    SchemaId.unsafeFrom("durable.input.v1"),
    schema
  )
  private val release = WorkerRelease(
    WorkerReleaseId.unsafeFrom("durable-worker"),
    ContentDigest.unsafeFrom("sha256:" + "9a" * 32)
  )
  private val out = RelativeOutputPath.unsafeFrom("results/out.txt")
  private def bytes(value: Int): ByteLimit = ByteLimit.from(value).toOption.get

  private def handle(
      outputs: Vector[RelativeOutputPath],
      resultBytes: Int,
      envelopeBytes: Int
  ): Either[ValidationFailure, DurableResultHandle] =
    DurableResultHandle.from(
      key,
      attemptId,
      AttemptEpoch.initial,
      None,
      operation,
      schema,
      bytes(resultBytes),
      bytes(envelopeBytes),
      outputs,
      release
    )

  private def invocation(
      input: Vector[Byte],
      maximumInput: Int
  ): Either[ValidationFailure, TaskInvocation] =
    TaskInvocation.from(
      key,
      attemptId,
      AttemptEpoch.initial,
      None,
      registered,
      input,
      Vector.empty,
      bytes(maximumInput),
      bytes(1024),
      bytes(4096),
      bytes(4096),
      release
    )

  test("a handle cannot declare the same output twice") {
    assert(handle(Vector(out, out), 1024, 4096).isLeft)
    assert(handle(Vector(out), 1024, 4096).isRight)
  }

  test("an envelope cannot be smaller than the result it must carry") {
    assert(handle(Vector.empty, 4096, 1024).isLeft)
    assert(handle(Vector.empty, 1024, 1024).isRight)
    assert(handle(Vector.empty, 1024, 4096).isRight)
  }

  test("an invocation cannot carry more input than the bound it declares") {
    assert(invocation(Vector.fill(64)(1.toByte), 32).isLeft)
    assert(invocation(Vector.fill(32)(1.toByte), 32).isRight)
  }

  test("binding a handle to its job is the only mutation it admits") {
    val bound = JobRef(JobId.unsafeFrom("7700"), None)
    val value = handle(Vector(out), 1024, 4096).toOption.get

    assertEquals(value.job, None)
    assertEquals(value.boundTo(bound).job, Some(bound))
    // Binding cannot invalidate the bounds, which is why it is allowed and a general copy is not.
    assertEquals(value.boundTo(bound).maximumEnvelopeBytes, value.maximumEnvelopeBytes)
    assertEquals(value.boundTo(bound).declaredOutputs, value.declaredOutputs)
  }
