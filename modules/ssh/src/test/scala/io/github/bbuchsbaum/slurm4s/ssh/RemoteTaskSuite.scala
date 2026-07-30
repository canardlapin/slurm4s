package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.IO
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.agent.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*
import io.github.bbuchsbaum.slurm4s.testkit.SchedulerProgram
import io.github.bbuchsbaum.slurm4s.worker.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Comparator
import scala.util.Try

class RemoteTaskSuite extends munit.CatsEffectSuite:
  private val observedAt = Instant.parse("2026-07-24T12:00:00Z")
  private val resultLimit = ByteLimit.from(128).toOption.get
  private val envelopeLimit = ByteLimit.from(4096).toOption.get
  private val awaitPolicy = RemoteAwaitPolicy(
    DurationMillis.from(1L).toOption.get,
    DurationMillis.from(1000L).toOption.get
  )
  private val release = WorkerRelease(
    WorkerReleaseId.from("remote-suite-worker").toOption.get,
    ContentDigest.from("sha256:remote-suite-worker").toOption.get
  )
  private val job = JobRef(JobId.from("9001").toOption.get, None, None)
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, Vector.empty)
  )

  private val temporaryRoot = FunFixture[Path](
    setup = _ => Files.createTempDirectory("remote-task-suite"),
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

  temporaryRoot.test("a wait rides out transient transport failures") { root =>
    val scheduler = testScheduler(
      SubmissionAttempt.Completed(Submission.Accepted(job, evidence)),
      None
    )
    val runtime = createRuntime(root, scheduler)
    val flaky = FlakySshRunner(runtime.runner)

    for
      remote <- connect(flaky)
      handle <- remote.submitOrRaise(Increment(41), options("remote-transient-failure"))
      _ <- publishSuccess(runtime.workspace, handle, 42)
      // The result is already published; only the observation of it is briefly blinded.
      _ = flaky.injectFailures(2)
      value <- handle.awaitValue
    yield
      assertEquals(value, 42)
      assertEquals(flaky.injectedCount, 2, "both injected failures must have been exercised")
  }

  temporaryRoot.test("a wait surrenders to persistent blindness after the configured bound") {
    root =>
      val scheduler = testScheduler(
        SubmissionAttempt.Completed(Submission.Accepted(job, evidence)),
        None
      )
      val runtime = createRuntime(root, scheduler)
      val flaky = FlakySshRunner(runtime.runner)

      for
        remote <- connect(flaky)
        handle <- remote.submitOrRaise(Increment(41), options("remote-persistent-failure"))
        _ = flaky.injectFailures(100)
        result <- handle.await
      yield
        assert(
          result match
            case RemoteExecutionResult.AgentUnavailable(_) => true
            case _                                         => false,
          s"persistent blindness must report the observation failure, got $result"
        )
        // Exactly the configured bound, so the wait neither gave up early nor retried forever.
        assertEquals(
          flaky.injectedCount,
          awaitPolicy.maximumConsecutiveObservationFailures.toInt
        )
  }

  temporaryRoot.test("a typed task is submitted remotely and its value survives reconnect") {
    root =>
      val scheduler = testScheduler(
        SubmissionAttempt.Completed(Submission.Accepted(job, evidence)),
        None
      )
      val runtime = createRuntime(root, scheduler)

      for
        localValue <- FileTaskContext
          .managed(FileTaskWorkspace(root.resolve("local-parity")), Map.empty)
          .use(context => Increment.run(41, context))
        firstRemote <- connect(runtime.runner)
        handle <- firstRemote.submitOrRaise(Increment(41), options("remote-value-reconnect"))
        _ <- publishSuccess(runtime.workspace, handle, 42)
        descriptorBytes = RemoteTaskDescriptor.encode(handle.descriptor).toOption.get
        restoredDescriptor = RemoteTaskDescriptor.decode(descriptorBytes).toOption.get
        secondRemote <- connect(runtime.runner)
        attached = secondRemote
          .attach(restoredDescriptor, Increment.outputCodec, awaitPolicy)
          .toOption
          .get
        value <- attached.awaitValue
      yield
        assertEquals(value, localValue)
        assertEquals(attached.resultRef, handle.resultRef)
        assertEquals(attached.resultHandle, handle.resultHandle)
        assert(runtime.runner.exchangeCount >= 4)
  }

  temporaryRoot.test("acceptance-unknown remains awaitable through the durable result reference") {
    root =>
      val scheduler = testScheduler(
        SubmissionAttempt.Completed(
          Submission.AcceptanceUnknown(AcceptanceUncertainty.ResponseLost, evidence)
        ),
        None
      )
      val runtime = createRuntime(root, scheduler)

      for
        remote <- connect(runtime.runner)
        submitted <- remote.submit(Increment(9), options("remote-acceptance-unknown"))
        handle = submitted.toOption.get
        _ <- publishSuccess(runtime.workspace, handle, 10)
        value <- handle.awaitValue
      yield
        assertEquals(value, 10)
        assert(
          handle.submission
            .isInstanceOf[SubmissionAttempt.Completed]
        )
  }

  temporaryRoot.test("submission rejection and accounting-only OOM or timeout stay typed") { root =>
    def runRejected: IO[Unit] =
      val scheduler = testScheduler(
        SubmissionAttempt.Completed(
          Submission.Rejected(
            Diagnostics.one(Diagnostic("invalid-account", "account is required")),
            evidence
          )
        ),
        None
      )
      val runtime = createRuntime(root.resolve("rejected"), scheduler)
      for
        remote <- connect(runtime.runner)
        submitted <- remote.submit(Increment(1), options("remote-rejected"))
        result <- submitted.toOption.get.await
      yield result match
        case RemoteExecutionResult.SubmissionFailed(
              SubmissionAttempt.Completed(Submission.Rejected(diagnostics, _))
            ) =>
          assertEquals(diagnostics.toVector.map(_.code), Vector("invalid-account"))
        case other => fail(s"expected submission rejection, received $other")

    def runAccounting(name: String, outcome: WorkloadOutcome): IO[Unit] =
      val scheduler = testScheduler(
        SubmissionAttempt.Completed(Submission.Accepted(job, evidence)),
        Some(outcome)
      )
      val runtime = createRuntime(root.resolve(name), scheduler)
      for
        remote <- connect(runtime.runner)
        submitted <- remote.submit(Increment(1), options(s"remote-$name"))
        result <- submitted.toOption.get.await
      yield result match
        case RemoteExecutionResult.Completed(ExecutionResult.WorkloadFailed(actual, _)) =>
          assertEquals(actual, outcome)
        case other => fail(s"expected accounting failure $outcome, received $other")

    runRejected *> runAccounting("oom", WorkloadOutcome.OutOfMemory) *>
      runAccounting("timeout", WorkloadOutcome.TimeLimitExceeded)
  }

  temporaryRoot.test("oversized envelopes and stale handles cannot decode as typed success") {
    root =>
      val scheduler = testScheduler(
        SubmissionAttempt.Completed(Submission.Accepted(job, evidence)),
        None
      )
      val runtime = createRuntime(root, scheduler)

      for
        remote <- connect(runtime.runner)
        submitted <- remote.submit(Increment(5), options("remote-invalid-results"))
        handle = submitted.toOption.get
        resultPath = resultFile(runtime.workspace, handle.resultRef)
        _ <- IO.blocking {
          val _ = Files.write(
            resultPath,
            Array.fill[Byte](handle.resultHandle.maximumEnvelopeBytes.value + 1)(1)
          )
        }
        oversized <- handle.await
        staleHandle = handle.resultHandle.copy(
          attemptEpoch = AttemptEpoch.from(handle.resultHandle.attemptEpoch.value + 1L).toOption.get
        )
        validEnvelope = successEnvelope(handle, 6)
        validBytes = ResultEnvelopeCodec
          .encode(validEnvelope, envelopeLimit, resultLimit)
          .toOption
          .get
        staleResult = RemoteResultValidation.attach(
          staleHandle,
          handle.resultHandle,
          ResultContract.Structured(Increment.outputCodec, resultLimit),
          validBytes,
          observedAt
        )
      yield
        oversized match
          case RemoteExecutionResult.Completed(ExecutionResult.Indeterminate(diagnostics, _)) =>
            assertEquals(
              diagnostics.toVector.map(_.code),
              Vector("remote-result-envelope-too-large")
            )
          case other => fail(s"expected oversized-result failure, received $other")
        staleResult match
          case ExecutionResult.ResultInvalid(diagnostics, _) =>
            assertEquals(diagnostics.toVector.map(_.code), Vector("stale-result-epoch"))
          case other => fail(s"expected stale-handle rejection, received $other")
  }

  temporaryRoot.test("awaitValue raises a typed exception instead of inventing a value") { root =>
    val scheduler = testScheduler(
      SubmissionAttempt.Completed(
        Submission.Rejected(
          Diagnostics.one(Diagnostic("partition-policy", "request rejected")),
          evidence
        )
      ),
      None
    )
    val runtime = createRuntime(root, scheduler)

    for
      remote <- connect(runtime.runner)
      submitted <- remote.submit(Increment(1), options("remote-await-value-failure"))
      attempted <- submitted.toOption.get.awaitValue.attempt
    yield attempted match
      case Left(error: RemoteTaskException) =>
        error.result match
          case RemoteExecutionResult.SubmissionFailed(_) => ()
          case other => fail(s"expected submission-failed exception payload, received $other")
      case other => fail(s"expected RemoteTaskException, received $other")
  }

  private def createRuntime(root: Path, scheduler: Scheduler[IO]): TestRuntime =
    val executable = root.resolve("slurm4s-worker")
    val _ = Files.createDirectories(root)
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
    val registered = WorkerRegisteredTaskService(launcher, scheduler)
    val service = AgentService[IO](
      scheduler,
      AgentLogReader((_, _, _) =>
        IO.pure(LogReadResult.WaitingForFile(LogCursor.start, observedAt))
      ),
      Some(registered)
    )
    val server = AgentStdioServer[IO](
      ServiceRequestHandler[IO](service, SchedulerRequestHandler[IO](service))
    )
    TestRuntime(workspace, RemoteLoopbackRunner(server))

  private def connect(runner: SshProcessRunner[IO]): IO[RemoteSlurm[IO]] =
    val target = SshTarget.from("loopback-cluster").toOption.get
    val launch =
      SshCommand.agent(SshConnection("/usr/bin/ssh", target)).toOption.get
    val wire = SshAgentWireClient(
      launch,
      runner,
      FrameLimits.default,
      SshExchangePolicy(DurationMillis.from(5000).toOption.get)
    )
    SshAgentApi.connect[IO](wire).map(RemoteSlurm(_))

  private def options(key: String): RemoteTaskOptions =
    RemoteTaskOptions(
      SubmissionKey.from(key).toOption.get,
      JobName.from("remote-increment").toOption.get,
      ResourceRequest.validate(1, 1, None, None, None).toOption.get,
      resultLimit,
      awaitPolicy = awaitPolicy
    )

  private def publishSuccess(
      workspace: Path,
      handle: RemoteTaskHandle[IO, Int],
      value: Int
  ): IO[Unit] =
    val bytes = ResultEnvelopeCodec
      .encode(successEnvelope(handle, value), envelopeLimit, resultLimit)
      .toOption
      .get
    IO.blocking {
      val _ = Files.write(resultFile(workspace, handle.resultRef), bytes.toArray)
    }

  private def successEnvelope(
      handle: RemoteTaskHandle[IO, Int],
      value: Int
  ): ResultEnvelope =
    val durable = handle.resultHandle
    ResultEnvelope.succeeded(
      durable.submissionKey,
      durable.attemptId,
      durable.attemptEpoch,
      durable.job,
      durable.operation,
      durable.resultSchema,
      Increment.outputCodec.encode(value).toOption.get,
      OutputManifest.empty,
      durable.workerRelease,
      observedAt
    )

  private def resultFile(workspace: Path, ref: RemoteResultRef): Path =
    workspace.resolve(s"${ref.attemptId.value}-e${ref.attemptEpoch.value}").resolve("result.json")

  private object Increment extends SlurmTask[Int, Int]:
    val operation: OperationRef[Int, Int] = OperationRef(
      OperationId.from("example.remote-increment").toOption.get,
      OperationVersion.from("1").toOption.get,
      SchemaId.from("example.remote-int-input.v1").toOption.get,
      ResultSchemaId.from("example.remote-int-result.v1").toOption.get
    )
    val inputCodec: InputCodec[Int] = intCodec(operation.inputSchema)
    val outputCodec: ResultCodec[Int] = new ResultCodec[Int]:
      val schemaId: ResultSchemaId = operation.outputSchema
      def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] = encodeInt(value)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] = decodeInt(bytes)
    override val retrySafety: RetrySafety = RetrySafety.SafeForAutomaticRetry
    def run(input: Int, context: TaskContext[IO]): IO[Int] = IO.pure(input + 1)

  private def intCodec(schema: SchemaId): InputCodec[Int] = new InputCodec[Int]:
    val schemaId: SchemaId = schema
    def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] = encodeInt(value)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] = decodeInt(bytes)

  private def encodeInt(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
    Right(value.toString.getBytes(StandardCharsets.UTF_8).toVector)

  private def decodeInt(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
    Try(new String(bytes.toArray, StandardCharsets.UTF_8).toInt).toEither.left.map(error =>
      ResultCodecFailure("invalid-int", Option(error.getMessage).getOrElse("invalid integer"))
    )

  final private case class TestRuntime(
      workspace: Path,
      runner: RemoteLoopbackRunner
  )

  private def testScheduler(
      submission: SubmissionAttempt,
      accountingOutcome: Option[WorkloadOutcome]
  ): Scheduler[IO] =
    val capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
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

    val accounting: IO[SchedulerQueryResult[AccountingBatch]] =
      IO.pure(
        accountingOutcome.fold[SchedulerQueryResult[AccountingBatch]](
          SchedulerQueryResult.Empty(observedAt, evidence)
        ) { outcome =>
          SchedulerQueryResult.Succeeded(
            AccountingBatch(
              NonEmptyVector.one(
                AccountingRecord(
                  job,
                  outcome match
                    case WorkloadOutcome.OutOfMemory       => SlurmState.OutOfMemory
                    case WorkloadOutcome.TimeLimitExceeded => SlurmState.TimedOut
                    case _                                 => SlurmState.Failed,
                  None,
                  Some(outcome),
                  Freshness.Current(observedAt),
                  Map.empty,
                  evidence
                )
              ),
              Vector.empty
            )
          )
        }
      )

    SchedulerProgram[IO](
      capabilities,
      _ => IO.pure(submission),
      _ => IO.pure(SchedulerQueryResult.Empty(observedAt, evidence)),
      _ => accounting,
      _ => IO.pure(CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)))
    ).scheduler

/** Injects a bounded run of transport failures, then behaves normally.
  *
  * A long wait almost certainly loses an SSH round trip at some point; this makes that certainty
  * reproducible.
  */
final private class FlakySshRunner(delegate: SshProcessRunner[IO]) extends SshProcessRunner[IO]:
  private var remaining = 0
  private var injected = 0

  def injectFailures(count: Int): Unit = remaining = count
  def injectedCount: Int = injected

  def exchange(
      launch: SshLaunch,
      request: Vector[Byte],
      policy: SshExchangePolicy
  ): IO[SshProcessOutcome] =
    if remaining > 0 then
      remaining -= 1
      injected += 1
      IO.pure(
        SshProcessOutcome.SpawnFailed(
          "injected transient ssh failure",
          BoundedEvidence.capture(
            EvidenceSource.CommandLaunch("ssh"),
            Instant.parse("2026-07-24T12:00:00Z"),
            Vector.empty
          )
        )
      )
    else delegate.exchange(launch, request, policy)

final private class RemoteLoopbackRunner(server: AgentStdioServer[IO]) extends SshProcessRunner[IO]:
  private var count = 0
  def exchangeCount: Int = count

  def exchange(
      launch: SshLaunch,
      request: Vector[Byte],
      policy: SshExchangePolicy
  ): IO[SshProcessOutcome] =
    count += 1
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
          BoundedEvidence.capture(EvidenceSource.CommandStderr("ssh"), now, Vector.empty)
        )
      }
