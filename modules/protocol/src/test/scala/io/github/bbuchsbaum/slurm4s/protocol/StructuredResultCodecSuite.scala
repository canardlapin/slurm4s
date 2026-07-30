package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.*

import java.io.InputStream
import java.time.Instant

class StructuredResultCodecSuite extends munit.FunSuite:
  private val envelopeLimit = ByteLimit.from(65536).toOption.get
  private val valueLimit = ByteLimit.from(4096).toOption.get

  test("Python reference envelope is canonical and decodes language-neutrally") {
    val bytes = fixture("/fixtures/python-result-envelope-v1.json")
    val envelope = ResultEnvelopeCodec.decode(bytes, envelopeLimit, valueLimit).toOption.get

    assertEquals(envelope.submissionKey.value, "python-example")
    assertEquals(new String(envelope.value.get.toArray, "UTF-8"), "{\"answer\":42}")
    assertEquals(envelope.outputs.entries.map(_.path.value), Vector("results/data.csv"))
    assertEquals(ResultEnvelopeCodec.encode(envelope, envelopeLimit, valueLimit), Right(bytes))
  }

  test("result envelope rejects missing successful value and bounded overflow") {
    val bytes = fixture("/fixtures/python-result-envelope-v1.json")
    val text = new String(bytes.toArray, "UTF-8")
    val missing = text.replace("\"eyJhbnN3ZXIiOjQyfQ==\"", "null").getBytes("UTF-8").toVector

    assert(
      ResultEnvelopeCodec.decode(missing, envelopeLimit, valueLimit).left.exists {
        case StructuredCodecFailure.Invalid(message) => message.contains("must contain a value")
        case _                                       => false
      }
    )
    assertEquals(
      ResultEnvelopeCodec.decode(bytes, ByteLimit.from(10).toOption.get, valueLimit),
      Left(StructuredCodecFailure.TooLarge(bytes.size.toLong, 10))
    )
  }

  test("worker progress events round trip with identity and release") {
    val event = WorkerEvent(
      sequence = 7L,
      submissionKey = SubmissionKey.from("event-example").toOption.get,
      attemptId = AttemptId.from("attempt-event-example").toOption.get,
      attemptEpoch = AttemptEpoch.initial,
      operation = WorkloadOperation.Registered(
        OperationId.from("example.echo").toOption.get,
        OperationVersion.from("1").toOption.get
      ),
      workerRelease = WorkerRelease(
        WorkerReleaseId.from("worker-1").toOption.get,
        ContentDigest.from("sha256:worker").toOption.get
      ),
      observedAt = Instant.parse("2026-07-22T12:00:00Z"),
      payload = WorkerEventPayload.Progress(
        ProgressEvent("halfway", Some(5L), Some(10L), Map("phase" -> "fit"))
      )
    )

    val encoded = WorkerEventCodec.encode(event, envelopeLimit).toOption.get
    assertEquals(WorkerEventCodec.decode(encoded, envelopeLimit), Right(event))
  }

  test("durable result handles round trip without a Scala type parameter") {
    val envelope = ResultEnvelopeCodec
      .decode(fixture("/fixtures/python-result-envelope-v1.json"), envelopeLimit, valueLimit)
      .toOption
      .get
    val handle = DurableResultHandle(
      envelope.submissionKey,
      envelope.attemptId,
      envelope.attemptEpoch,
      envelope.job,
      envelope.operation,
      envelope.resultSchema,
      valueLimit,
      envelopeLimit,
      envelope.outputs.entries.map(_.path),
      envelope.workerRelease,
      RetrySafety.SafeForAutomaticRetry
    )

    val bytes = DurableResultHandleCodec.encode(handle, envelopeLimit).toOption.get
    assertEquals(DurableResultHandleCodec.decode(bytes, envelopeLimit), Right(handle))
    val conservative = handle.copy(retrySafety = RetrySafety.Unknown)
    val conservativeBytes =
      DurableResultHandleCodec.encode(conservative, envelopeLimit).toOption.get
    assertEquals(
      DurableResultHandleCodec.decode(conservativeBytes, envelopeLimit),
      Right(conservative)
    )
  }

  test("typed task invocation contains only bounded encoded input and durable identities") {
    val invocation = TaskInvocation(
      SubmissionKey.from("typed-wire").toOption.get,
      AttemptId.from("typed-wire-attempt").toOption.get,
      AttemptEpoch.initial,
      None,
      RegisteredOperation(
        OperationId.from("example.wire").toOption.get,
        OperationVersion.from("1").toOption.get,
        SchemaId.from("example.input.v1").toOption.get,
        ResultSchemaId.from("example.output.v1").toOption.get
      ),
      "input".getBytes("UTF-8").toVector,
      Vector(RelativeOutputPath.from("results/value.txt").toOption.get),
      valueLimit,
      valueLimit,
      envelopeLimit,
      valueLimit,
      WorkerRelease(
        WorkerReleaseId.from("wire-worker").toOption.get,
        ContentDigest.from("sha256:wire-worker").toOption.get
      ),
      RetrySafety.NoAutomaticRetry
    )
    val encoded = TaskInvocationCodec.encode(invocation, envelopeLimit).toOption.get

    assertEquals(
      TaskInvocationCodec.decode(encoded, envelopeLimit, valueLimit),
      Right(invocation)
    )
    assert(
      TaskInvocationCodec
        .decode(encoded, envelopeLimit, ByteLimit.from(1).toOption.get)
        .left
        .exists(_.isInstanceOf[StructuredCodecFailure.TooLarge])
    )
  }

  private def fixture(path: String): Vector[Byte] =
    val stream: InputStream = Option(getClass.getResourceAsStream(path)).getOrElse(
      throw new IllegalStateException(s"missing fixture: $path")
    )
    try stream.readAllBytes().toVector
    finally stream.close()
