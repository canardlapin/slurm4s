package io.github.bbuchsbaum.slurm4s.core

class JobArraySuite extends munit.FunSuite:
  test("array binding preserves distinct attempt, log, and result identities") {
    val request = JobArrayRequest.contiguous(positive(3), Some(positive(2))).toOption.get
    val elements = request.indices.toVector.map(element)
    val plan = JobArrayPlan.from(request, elements).toOption.get
    val parent = JobRef(JobId.from("9000").toOption.get, None)

    val bindings = plan.bind(parent).toOption.get.toVector

    assertEquals(bindings.map(_.job.arrayIndex), request.indices.toVector.map(Some(_)))
    assertEquals(bindings.map(_.identity.attemptId).distinct.size, 3)
    assertEquals(bindings.map(_.stdout.locator).distinct.size, 3)
    assertEquals(bindings.map(_.stderr.locator).distinct.size, 3)
    assertEquals(
      bindings.map(_.resultHandle.map(_.job)),
      bindings.map(value => Some(Some(value.job)))
    )
  }

  test("array construction rejects aliased identities and incompatible contracts") {
    val request = JobArrayRequest.contiguous(positive(2), None).toOption.get
    val first = element(request.indices.head)
    val secondIndex = request.indices.toVector(1)
    val second = element(secondIndex).copy(
      attemptId = first.attemptId,
      resultContract = ResultContract.ExitOnly.descriptor
    )

    val failures = JobArrayPlan
      .from(request, Vector(first, second))
      .swap
      .toOption
      .get
      .toChain
      .toVector
      .map(_.code)
      .toSet

    assert(failures.contains("array-attempt-id-duplicate"))
    assert(failures.contains("array-result-contract-mismatch"))
    assert(failures.contains("array-stdout-identity-mismatch"))
    assert(failures.contains("array-stderr-identity-mismatch"))
    assert(failures.contains("array-result-identity-mismatch"))
  }

  private def element(index: ArrayIndex): ArrayElementIdentity =
    val suffix = index.value.toString
    val key = SubmissionKey.from(s"array-key-$suffix").toOption.get
    val attempt = AttemptId.from(s"array-attempt-$suffix").toOption.get
    val schema = ResultSchemaId.from("array.result.v1").toOption.get
    val release = WorkerRelease(
      WorkerReleaseId.from("worker-v1").toOption.get,
      ContentDigest
        .from("sha256:87eba76e7f3164534045ba922e7770fb58bbd14ad732bbf5ba6f11cc56989e6e")
        .toOption
        .get
    )
    val contract = ResultContractDescriptor(
      ResultMode.Structured,
      Some(schema),
      Vector.empty
    )
    ArrayElementIdentity(
      index,
      key,
      attempt,
      AttemptEpoch.initial,
      LogRef(attempt, AttemptEpoch.initial, LogStream.Stdout, s"/work/$suffix/stdout.log"),
      LogRef(attempt, AttemptEpoch.initial, LogStream.Stderr, s"/work/$suffix/stderr.log"),
      contract,
      Some(
        DurableResultHandle
          .from(
            key,
            attempt,
            AttemptEpoch.initial,
            None,
            WorkloadOperation.Registered(
              OperationId.from("array.operation").toOption.get,
              OperationVersion.from("1").toOption.get
            ),
            schema,
            ByteLimit.defaultEvidence,
            ByteLimit.defaultEvidence,
            Vector.empty,
            release
          )
          .toOption
          .get
      )
    )

  private def positive(value: Int): PositiveInt = PositiveInt.from("test", value).toOption.get
