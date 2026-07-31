package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant

class RemoteBatchProtocolSuite extends munit.FunSuite:
  private val observedAt = Instant.parse("2026-07-24T12:00:00Z")
  private val operation = RegisteredOperation(
    OperationId.from("example.batch").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("example.batch-input.v1").toOption.get,
    ResultSchemaId.from("example.batch-result.v1").toOption.get
  )
  private val perTask = TaskResources(
    PositiveInt.from("cpus", 2).toOption.get,
    Some(MemoryRequest.PerNode(Mebibytes.from(1024).toOption.get)),
    Some(WallTimeMinutes.from(30).toOption.get)
  )

  test("owned batch wire round-trips independent, sharded, and gang topologies") {
    val executions = Vector(
      BatchExecutionPlan.Independent(
        Some(PositiveInt.from("maximumRunning", 3).toOption.get)
      ),
      BatchExecutionPlan.Sharded(
        PositiveInt.from("shards", 2).toOption.get,
        PositiveInt.from("slots", 2).toOption.get,
        ShardAssignment.RoundRobin,
        Some(PositiveInt.from("maximumRunningShards", 1).toOption.get)
      ),
      BatchExecutionPlan.Gang(
        PositiveInt.from("nodes", 2).toOption.get,
        PositiveInt.from("tasksPerNode", 3).toOption.get
      )
    )

    executions.foreach { execution =>
      val topology = compile(execution)
      val request = batchRequest(topology)
      val response = batchResponse(topology, request)
      val requestJson = AgentDomainJson.encodeRemoteBatchRequest(request).toOption.get
      val responseJson = AgentDomainJson.encodeRemoteBatchSubmission(response).toOption.get

      assertEquals(AgentDomainJson.decodeRemoteBatchRequest(requestJson), Right(request))
      assertEquals(
        AgentDomainJson.decodeRemoteBatchSubmission(responseJson),
        Right(response)
      )
      assert(!requestJson.noSpaces.contains("\"Independent\""))
      assert(!requestJson.noSpaces.contains("\"Sharded\""))
      assert(!requestJson.noSpaces.contains("\"Gang\""))
      assert(!requestJson.noSpaces.contains("[0,127,-128,-1]"))
    }
  }

  test("batch responses reject duplicate indices and mismatched log attempts") {
    val topology = compile(BatchExecutionPlan.Independent())
    val request = batchRequest(topology)
    val response = batchResponse(topology, request)
    val json = AgentDomainJson.encodeRemoteBatchSubmission(response).toOption.get
    val elements = json.hcursor.downField("elements").focus.flatMap(_.asArray).get
    val duplicate = json.mapObject(
      _.add("elements", io.circe.Json.fromValues(Vector(elements.head, elements.head)))
    )
    val wrongLog = json.mapObject { root =>
      root.add(
        "elements",
        io.circe.Json.fromValues(
          elements.updated(
            0,
            elements.head.mapObject(
              _.add(
                "stdout",
                elements.head.hcursor
                  .downField("stdout")
                  .focus
                  .get
                  .mapObject(
                    _.add("attemptId", io.circe.Json.fromString("another-attempt"))
                  )
              )
            )
          )
        )
      )
    }

    assert(AgentDomainJson.decodeRemoteBatchSubmission(duplicate).isLeft)
    assert(AgentDomainJson.decodeRemoteBatchSubmission(wrongLog).isLeft)
  }

  test("script batches own argv, log, and atomic exit-status wire shapes") {
    val topology = compile(BatchExecutionPlan.Independent())
    val base = SubmissionKey.from("wire-script-batch").toOption.get
    val request = RemoteScriptBatchRequest(
      base,
      JobName.from("wire-script-batch").toOption.get,
      ScriptProgram(
        ScriptSource.Inline(
          "analysis.sh",
          Vector(0x00.toByte, 0x7f.toByte, 0x80.toByte, 0xff.toByte)
        ),
        ScriptInvocation.Via(CommandPrefix.bash)
      ),
      topology,
      topology.elementIndices.map { index =>
        RemoteScriptBatchElement(
          index,
          BatchElementKey.derive(base, index).toOption.get,
          Vector(
            Argument.from("--label").toOption.get,
            Argument.from("space and ' quote").toOption.get
          )
        )
      },
      Map.empty,
      RetrySafety.NoAutomaticRetry
    )
    val elements = request.elements.map { element =>
      val attempt = AttemptId.from(s"script-${element.index.value}").toOption.get
      val ref = RemoteScriptExitRef(attempt, AttemptEpoch.initial)
      RemoteScriptBatchElementSubmission(
        element.index,
        ref,
        LogRef(attempt, AttemptEpoch.initial, LogStream.Stdout, s"/work/$attempt/out"),
        LogRef(attempt, AttemptEpoch.initial, LogStream.Stderr, s"/work/$attempt/err")
      )
    }
    val response = RemoteScriptBatchSubmission(
      topology,
      elements,
      SubmissionAttempt.Completed(
        Submission.AcceptanceUnknown(
          AcceptanceUncertainty.ResponseLost,
          EvidenceBundle(
            BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, Vector.empty)
          )
        )
      )
    )
    val requestJson = AgentDomainJson.encodeRemoteScriptBatchRequest(request).toOption.get
    val responseJson = AgentDomainJson.encodeRemoteScriptBatchSubmission(response)
    val exitRead = RemoteScriptExitRead.Exited(7, observedAt)

    assertEquals(
      AgentDomainJson.decodeRemoteScriptBatchRequest(requestJson),
      Right(request)
    )
    assertEquals(
      AgentDomainJson.decodeRemoteScriptBatchSubmission(responseJson),
      Right(response)
    )
    assertEquals(
      AgentDomainJson.decodeRemoteScriptExitRead(
        AgentDomainJson.encodeRemoteScriptExitRead(exitRead)
      ),
      Right(exitRead)
    )
    assert(requestJson.noSpaces.contains("AH+A/w=="))
    assert(!requestJson.noSpaces.contains("\"Via\""))
    assert(!requestJson.noSpaces.contains("\"Inline\""))
  }

  private def compile(execution: BatchExecutionPlan): BatchTopology =
    val grid = Grid.fromAxis(Axis.of(1, 2, 3, 4, 5, 6))
    val task = BatchTask.Script(
      ScriptProgram(
        ScriptSource.ExistingRemote("/work/batch.sh"),
        ScriptInvocation.Direct
      ),
      ScriptArguments.positional[Int]
    )
    BatchPlanner
      .compile(Batch(grid, task), execution, perTask)
      .toOption
      .get
      .topology

  private def batchRequest(topology: BatchTopology): RemoteRegisteredBatchRequest =
    val elements = topology.elementIndices.map { index =>
      RemoteRegisteredBatchElement(
        index,
        BatchElementKey
          .derive(SubmissionKey.from("wire-batch").toOption.get, index)
          .toOption
          .get,
        Vector(0x00.toByte, 0x7f.toByte, 0x80.toByte, 0xff.toByte)
      )
    }
    RemoteRegisteredBatchRequest(
      SubmissionKey.from("wire-batch").toOption.get,
      JobName.from("wire-batch").toOption.get,
      operation,
      topology,
      elements,
      Map(EnvName.unsafeFrom("LANG") -> "C.UTF-8"),
      ByteLimit.from(1024).toOption.get,
      Vector.empty,
      RetrySafety.SafeForAutomaticRetry
    )

  private def batchResponse(
      topology: BatchTopology,
      request: RemoteRegisteredBatchRequest
  ): RemoteRegisteredBatchSubmission =
    val release = WorkerRelease(
      WorkerReleaseId.from("worker-1").toOption.get,
      ContentDigest
        .from("sha256:13029f9e83d15b3d437c2a7568fc1ca7990ecf3ff79bef6da08f13ff5ae12af8")
        .toOption
        .get
    )
    val elements = request.elements.map { element =>
      val attempt = AttemptId.from(s"batch-${element.index.value}").toOption.get
      val epoch = AttemptEpoch.initial
      RemoteRegisteredBatchElementSubmission(
        element.index,
        RemoteResultRef(attempt, epoch),
        DurableResultHandle
          .from(
            element.submissionKey,
            attempt,
            epoch,
            None,
            WorkloadOperation.Registered(operation.id, operation.version),
            operation.outputSchema,
            request.maximumResultBytes,
            ByteLimit.defaultEvidence,
            Vector.empty,
            release,
            request.retrySafety
          )
          .toOption
          .get,
        LogRef(attempt, epoch, LogStream.Stdout, s"/work/$attempt/stdout.log"),
        LogRef(attempt, epoch, LogStream.Stderr, s"/work/$attempt/stderr.log")
      )
    }
    val evidence = EvidenceBundle(
      BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, Vector.empty)
    )
    RemoteRegisteredBatchSubmission(
      topology,
      elements,
      SubmissionAttempt.Completed(
        Submission.Accepted(
          JobRef(JobId.from("9001").toOption.get, None),
          evidence
        )
      )
    )
