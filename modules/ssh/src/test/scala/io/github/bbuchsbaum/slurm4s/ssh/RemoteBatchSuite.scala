package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.agent.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*
import io.github.bbuchsbaum.slurm4s.worker.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Comparator
import scala.util.Try

class RemoteBatchSuite extends munit.CatsEffectSuite:
  enum Method derives CanEqual:
    case Ridge, Lasso, ElasticNet

  final case class FitParams(alpha: Double, method: Method, seed: Int)

  given ArgValue[Method] = ArgValue.fromString(_.toString.toLowerCase)
  given ScriptArguments[FitParams] = ScriptArguments.derived

  private val observedAt = Instant.parse("2026-07-24T12:00:00Z")
  private val parentJob = JobRef(JobId.from("9100").toOption.get, None, None)
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, Vector.empty)
  )
  private val resultLimit = ByteLimit.from(128).toOption.get
  private val envelopeLimit = ByteLimit.from(4096).toOption.get
  private val release = WorkerRelease(
    WorkerReleaseId.from("remote-batch-suite").toOption.get,
    ContentDigest.from("sha256:remote-batch-suite").toOption.get
  )
  private val awaitPolicy = RemoteAwaitPolicy(
    DurationMillis.from(1L).toOption.get,
    DurationMillis.from(1000L).toOption.get
  )

  private val temporaryRoot = FunFixture[Path](
    setup = _ => Files.createTempDirectory("remote-batch-suite"),
    teardown = root =>
      val stream = Files.walk(root)
      try
        stream
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
      finally stream.close()
  )

  temporaryRoot.test(
    "a 27-row typed grid runs through remote sharding and returns ordered values and logs"
  ) { root =>
    for
      submittedRequest <- Ref.of[IO, Option[JobRequest[?]]](None)
      scheduler = BatchScheduler(submittedRequest)
      runtime = createRuntime(root, scheduler)
      remote <- connect(runtime.runner)
      batch = SlurmBatch.registered(
        Grid.fromAxis(Axis.of(1, (2 to 27)*)),
        Increment
      )
      handle <- remote.submitBatchOrRaise(
        batch,
        BatchExecutionPlan.Sharded(
          PositiveInt.from("shards", 3).toOption.get,
          PositiveInt.from("slots", 5).toOption.get,
          ShardAssignment.Exactly(PositiveInt.from("rows", 9).toOption.get)
        ),
        options
      )
      _ <- handle.elements.traverse_(element =>
        publishSuccess(
          runtime.workspace,
          element.descriptor.submission.resultHandle,
          element.input + 1
        )
      )
      values <- handle.awaitValues
      log <- handle.elements.head.readStdout(LogCursor.start, ByteLimit.from(256).toOption.get)
      schedulerRequest <- submittedRequest.get
    yield
      assertEquals(values.toVector, (2 to 28).toVector)
      assertEquals(
        handle.elements.toVector.groupMap(_.shardIndex.value)(_.index.value),
        Map(
          1 -> (1 to 9).toVector,
          2 -> (10 to 18).toVector,
          3 -> (19 to 27).toVector
        )
      )
      log match
        case AgentCall.Succeeded(LogReadResult.Page(page)) =>
          val text = new String(page.bytes.toArray, StandardCharsets.UTF_8)
          assert(text.contains("stdout.log"))
        case other => fail(s"expected a remote stdout page, received $other")
      schedulerRequest match
        case Some(request) =>
          assertEquals(request.array.map(_.indices.toVector.map(_.value)), Some(Vector(1, 2, 3)))
          assertEquals(request.resources.cpusPerTask.toInt, 5)
          assertEquals(
            request.resources.memory,
            Some(MemoryRequest.PerNode(Mebibytes.from(5L * 1024L).toOption.get))
          )
        case None => fail("the target scheduler did not receive the batch")
  }

  temporaryRoot.test("independent and gang plans preserve their distinct Slurm shapes") { root =>
    for
      submittedRequest <- Ref.of[IO, Option[JobRequest[?]]](None)
      runtime = createRuntime(root, BatchScheduler(submittedRequest))
      remote <- connect(runtime.runner)
      independent <- remote.submitBatch(
        SlurmBatch.registered(Grid.fromAxis(Axis.of(1, 2, 3)), Increment),
        BatchExecutionPlan.Independent(
          Some(PositiveInt.from("maximumRunning", 2).toOption.get)
        ),
        options.copy(submissionKey = SubmissionKey.from("remote-independent").toOption.get)
      )
      gang <- remote.submitBatch(
        SlurmBatch.registered(Grid.fromAxis(Axis.of(1, 2, 3, 4, 5, 6)), Increment),
        BatchExecutionPlan.Gang(
          PositiveInt.from("nodes", 2).toOption.get,
          PositiveInt.from("tasksPerNode", 3).toOption.get
        ),
        options.copy(submissionKey = SubmissionKey.from("remote-gang").toOption.get)
      )
    yield
      val independentHandle = independent.toOption.get
      assertEquals(
        independentHandle.topology.array.flatMap(_.maximumConcurrent).map(_.toInt),
        Some(2)
      )
      assertEquals(
        independentHandle.elements.toVector.map(_.shardIndex.value),
        Vector(1, 2, 3)
      )
      val gangHandle = gang.toOption.get
      assertEquals(gangHandle.topology.array, None)
      assertEquals(gangHandle.topology.resources.nodes.map(_.toInt), Some(2))
      assertEquals(gangHandle.topology.resources.tasks.toInt, 6)
      assert(gangHandle.elements.forall(_.shardIndex.value == 1))
  }

  temporaryRoot.test(
    "a local opaque script is transported once and receives exact named argv for every grid row"
  ) { root =>
    val localScript = root.resolve("fit-local.sh")
    val _ = Files.writeString(
      localScript,
      "#!/bin/sh\nprintf '%s\\n' \"$@\"\n"
    )
    val grid = Grid.cross(
      Axis.of(0.1, 0.5, 1.0),
      Axis.of(Method.Ridge, Method.Lasso, Method.ElasticNet),
      Axis.of(1, 2, 3)
    )(FitParams.apply)

    for
      submittedRequest <- Ref.of[IO, Option[JobRequest[?]]](None)
      runtime = createRuntime(root.resolve("remote"), BatchScheduler(submittedRequest))
      remote <- connect(runtime.runner)
      batch = SlurmBatch.script(
        grid,
        ScriptProgram(
          ScriptSource.StagedLocal(localScript.toString),
          ScriptInvocation.Direct
        )
      )
      handle <- remote.submitBatchOrRaise(
        batch,
        BatchExecutionPlan.Sharded(
          PositiveInt.from("shards", 3).toOption.get,
          PositiveInt.from("slots", 5).toOption.get,
          ShardAssignment.Exactly(PositiveInt.from("rows", 9).toOption.get)
        ),
        RemoteScriptBatchOptions(
          SubmissionKey.from("remote-script-grid-27").toOption.get,
          JobName.from("remote-script-grid").toOption.get,
          options.perTask,
          awaitPolicy = awaitPolicy
        )
      )
      exitCodes <- handle.elements.traverse { element =>
        val launch = runtime.workspace
          .resolve(s"${element.exitRef.attemptId.value}-e${element.exitRef.attemptEpoch.value}")
          .resolve("launch-script.sh")
        IO.blocking(new ProcessBuilder(launch.toString).start().waitFor())
      }
      results <- handle.await
      firstLog <- handle.elements.head.readStdout(
        LogCursor.start,
        ByteLimit.from(1024).toOption.get
      )
    yield
      assertEquals(grid.size, 27)
      assert(exitCodes.forall(_ == 0))
      assert(
        results.forall(_.result == RemoteScriptExecutionResult.Exited(0))
      )
      firstLog match
        case AgentCall.Succeeded(LogReadResult.Page(page)) =>
          val locator = new String(page.bytes.toArray, StandardCharsets.UTF_8)
          val contents = Files.readString(Path.of(locator))
          assertEquals(
            contents,
            "--alpha\n0.1\n--method\nridge\n--seed\n1\n"
          )
        case other => fail(s"expected a remote script stdout page, received $other")
  }

  private def createRuntime(
      root: Path,
      scheduler: Scheduler[IO]
  ): BatchTestRuntime =
    val _ = Files.createDirectories(root)
    val executable = root.resolve("slurm4s-worker")
    val _ = Files.writeString(executable, "#!/bin/sh\nexit 0\n")
    val _ = executable.toFile.setExecutable(true, true)
    val workspace = root.resolve("registered-tasks")
    val launcher = RegisteredTaskLauncher(
      WorkerLaunchSettings(
        workspace,
        executable,
        release,
        ByteLimit.from(1024).toOption.get,
        ByteLimit.maximumCommandCapture,
        envelopeLimit,
        ByteLimit.from(8192).toOption.get
      )
    )
    val service = AgentService[IO](
      scheduler,
      AgentLogReader((ref, cursor, _) =>
        IO.pure(
          LogReadResult.Page(
            LogPage(
              ref.locator.getBytes(StandardCharsets.UTF_8).toVector,
              cursor,
              endOfFile = true,
              observedAt
            )
          )
        )
      ),
      Some(WorkerRegisteredTaskService(launcher, scheduler))
    )
    val server = AgentStdioServer[IO](
      ServiceRequestHandler[IO](service, SchedulerRequestHandler[IO](service))
    )
    BatchTestRuntime(workspace, BatchLoopbackRunner(server))

  private def connect(runner: BatchLoopbackRunner): IO[RemoteSlurm[IO]] =
    val target = SshTarget.from("loopback-cluster").toOption.get
    val launch = SshCommand.agent(SshConnection("/usr/bin/ssh", target)).toOption.get
    val wire = SshAgentWireClient(
      launch,
      runner,
      FrameLimits.default,
      SshExchangePolicy(DurationMillis.from(5000).toOption.get)
    )
    SshAgentApi.connect[IO](wire).map(RemoteSlurm(_))

  private val options = RemoteBatchOptions(
    SubmissionKey.from("remote-grid-27").toOption.get,
    JobName.from("remote-grid").toOption.get,
    TaskResources(
      PositiveInt.from("cpus", 1).toOption.get,
      Some(MemoryRequest.PerNode(Mebibytes.from(1024).toOption.get)),
      Some(WallTimeMinutes.from(30).toOption.get)
    ),
    resultLimit,
    awaitPolicy = awaitPolicy
  )

  private def publishSuccess(
      workspace: Path,
      handle: DurableResultHandle,
      value: Int
  ): IO[Unit] =
    val envelope = ResultEnvelope.succeeded(
      handle.submissionKey,
      handle.attemptId,
      handle.attemptEpoch,
      handle.job,
      handle.operation,
      handle.resultSchema,
      Increment.outputCodec.encode(value).toOption.get,
      OutputManifest.empty,
      handle.workerRelease,
      observedAt
    )
    val bytes = ResultEnvelopeCodec
      .encode(envelope, envelopeLimit, resultLimit)
      .toOption
      .get
    IO.blocking {
      val path = workspace
        .resolve(s"${handle.attemptId.value}-e${handle.attemptEpoch.value}")
        .resolve("result.json")
      val _ = Files.write(path, bytes.toArray)
    }

  private object Increment extends SlurmTask[Int, Int]:
    val operation: OperationRef[Int, Int] = OperationRef(
      OperationId.from("example.remote-batch-increment").toOption.get,
      OperationVersion.from("1").toOption.get,
      SchemaId.from("example.remote-batch-input.v1").toOption.get,
      ResultSchemaId.from("example.remote-batch-result.v1").toOption.get
    )
    val inputCodec: InputCodec[Int] = new InputCodec[Int]:
      val schemaId: SchemaId = operation.inputSchema
      def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] = encodeInt(value)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] = decodeInt(bytes)
    val outputCodec: ResultCodec[Int] = new ResultCodec[Int]:
      val schemaId: ResultSchemaId = operation.outputSchema
      def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] = encodeInt(value)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] = decodeInt(bytes)
    override val retrySafety: RetrySafety = RetrySafety.SafeForAutomaticRetry
    def run(input: Int, context: TaskContext[IO]): IO[Int] = IO.pure(input + 1)

  private def encodeInt(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
    Right(value.toString.getBytes(StandardCharsets.UTF_8).toVector)

  private def decodeInt(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
    Try(new String(bytes.toArray, StandardCharsets.UTF_8).toInt).toEither.left.map(error =>
      ResultCodecFailure("invalid-int", Option(error.getMessage).getOrElse("invalid integer"))
    )

  final private case class BatchTestRuntime(
      workspace: Path,
      runner: BatchLoopbackRunner
  )

  final private case class BatchScheduler(
      submitted: Ref[IO, Option[JobRequest[?]]]
  ) extends Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.pure(
        SchedulerQueryResult.Succeeded(
          SchedulerCapabilities(
            Some("test"),
            None,
            CapabilitySupport.Supported,
            CapabilitySupport.Supported,
            CapabilitySupport.Supported,
            CapabilitySupport.Supported,
            Vector.empty
          )
        )
      )

    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] =
      submitted.set(Some(request)) *>
        IO.pure(
          SubmissionAttempt.Completed(Submission.Accepted(parentJob, evidence))
        )

    def observe(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[ObservationBatch]] =
      IO.pure(SchedulerQueryResult.Empty(observedAt, evidence))

    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[AccountingBatch]] =
      IO.pure(SchedulerQueryResult.Empty(observedAt, evidence))

    def cancel(job: JobRef): IO[CancellationAttempt] =
      IO.pure(
        CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence))
      )

final private class BatchLoopbackRunner(
    server: AgentStdioServer[IO]
) extends SshProcessRunner[IO]:
  def exchange(
      launch: SshLaunch,
      request: Vector[Byte],
      policy: SshExchangePolicy
  ): IO[SshProcessOutcome] =
    Stream
      .emits(request)
      .covary[IO]
      .chunkN(5)
      .flatMap(Stream.chunk)
      .through(server.pipe)
      .compile
      .toVector
      .map { response =>
        val now = Instant.parse("2026-07-24T12:00:00Z")
        SshProcessOutcome.Exited(
          0,
          requestWriteCompleted = true,
          BoundedEvidence.capture(EvidenceSource.CommandStdout("ssh"), now, response),
          BoundedEvidence.capture(
            EvidenceSource.CommandStderr("ssh"),
            now,
            Vector.empty
          )
        )
      }
