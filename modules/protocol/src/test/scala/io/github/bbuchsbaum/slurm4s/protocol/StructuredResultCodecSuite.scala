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

  test("a legacy cluster field cannot fail a job reference it no longer belongs to") {
    // `JobRef` deliberately dropped cluster identity, so the decoder discards any `cluster` it
    // finds. Validating a discarded value only turns a readable durable record into an
    // unreadable one, and this is the compatibility path where that costs the most.
    val legacy = io.circe.Json.obj(
      "jobId" -> io.circe.Json.fromString("4242"),
      "arrayIndex" -> io.circe.Json.Null,
      "cluster" -> io.circe.Json.fromString("not a valid cluster name!")
    )

    assertEquals(
      StructuredJson.decodeJob(legacy).map(_.jobId.value),
      Right("4242")
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
        ContentDigest
          .from("sha256:87eba76e7f3164534045ba922e7770fb58bbd14ad732bbf5ba6f11cc56989e6e")
          .toOption
          .get
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
    val handle = DurableResultHandle
      .from(
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
      .toOption
      .get

    val bytes = DurableResultHandleCodec.encode(handle, envelopeLimit).toOption.get
    assertEquals(DurableResultHandleCodec.decode(bytes, envelopeLimit), Right(handle))
    val conservative = DurableResultHandle
      .from(
        handle.submissionKey,
        handle.attemptId,
        handle.attemptEpoch,
        handle.job,
        handle.operation,
        handle.resultSchema,
        handle.maximumResultBytes,
        handle.maximumEnvelopeBytes,
        handle.declaredOutputs,
        handle.workerRelease,
        RetrySafety.Unknown
      )
      .fold(problem => fail(problem.reason), identity)
    val conservativeBytes =
      DurableResultHandleCodec.encode(conservative, envelopeLimit).toOption.get
    assertEquals(
      DurableResultHandleCodec.decode(conservativeBytes, envelopeLimit),
      Right(conservative)
    )
  }

  test("typed task invocation contains only bounded encoded input and durable identities") {
    val invocation = TaskInvocation
      .from(
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
          ContentDigest
            .from("sha256:3564222fe977cf468c6d61ae1cb5793096f8ffc4a7c8b02401b9a2402b7f5bf1")
            .toOption
            .get
        ),
        RetrySafety.NoAutomaticRetry
      )
      .toOption
      .get
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

  test("a batched result read round-trips every entry in order") {
    val refs = cats.data.NonEmptyVector.of(
      RemoteResultRef(AttemptId.unsafeFrom("batch-a"), AttemptEpoch.initial),
      RemoteResultRef(AttemptId.unsafeFrom("batch-b"), AttemptEpoch.initial)
    )
    val bound = ByteLimit.from(4096).toOption.get
    val request = AgentDomainJson.encodeRemoteResultReadsRequest(refs, bound)

    assertEquals(
      AgentDomainJson.decodeRemoteResultReadsRequest(request),
      Right(refs -> bound)
    )

    val at = java.time.Instant.parse("2026-07-31T00:00:00Z")
    val reads = cats.data.NonEmptyVector.of(
      RemoteResultRead.Pending(at),
      RemoteResultRead.Pending(at.plusSeconds(1))
    )
    val encoded = AgentDomainJson.encodeRemoteResultReads(reads).toOption.get

    // Order is the correlation: the caller pairs results with the refs it sent.
    assertEquals(AgentDomainJson.decodeRemoteResultReads(encoded, bound), Right(reads))
  }

  test("an empty batched read is rejected rather than silently succeeding") {
    val empty = io.circe.Json.obj(
      "wireVersion" -> io.circe.Json.fromInt(1),
      "resultRefs" -> io.circe.Json.arr(),
      "maximumBytes" -> io.circe.Json.fromInt(4096)
    )

    assert(AgentDomainJson.decodeRemoteResultReadsRequest(empty).isLeft)
  }
