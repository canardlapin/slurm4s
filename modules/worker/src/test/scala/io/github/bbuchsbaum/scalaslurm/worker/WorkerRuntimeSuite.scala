package io.github.bbuchsbaum.scalaslurm.worker

import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.ResultEnvelopeCodec
import io.github.bbuchsbaum.scalaslurm.protocol.TaskInvocationCodec

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import scala.jdk.CollectionConverters.*

class WorkerRuntimeSuite extends munit.CatsEffectSuite:
  private val outputPath = RelativeOutputPath.from("results/answer.txt").toOption.get
  private val extraPath = RelativeOutputPath.from("results/debug.txt").toOption.get
  private val release = WorkerRelease(
    WorkerReleaseId.from("test-worker-1").toOption.get,
    ContentDigest.from("sha256:test-worker-release").toOption.get
  )
  private val inputLimit = ByteLimit.from(1024).toOption.get
  private val resultLimit = ByteLimit.from(1024).toOption.get
  private val envelopeLimit = ByteLimit.from(65536).toOption.get
  private val outputLimit = ByteLimit.from(4096).toOption.get

  test("registered task runs effectfully and publishes typed value plus declared output") {
    temporaryDirectory.use { root =>
      for
        progress <- Ref.of[IO, Vector[ProgressEvent]](Vector.empty)
        events <- Ref.of[IO, Vector[WorkerEvent]](Vector.empty)
        task = ExampleTask(Vector(outputPath))
        registry = TaskRegistry.from(Vector(TaskRegistration(task))).toOption.get
        runtime <- WorkerRuntime.create(release, registry, collectingSink(events))
        encodedInvocation = invocationFor(task, "41", Vector(outputPath))
        invocation = TaskInvocationCodec
          .decode(
            TaskInvocationCodec.encode(encodedInvocation, envelopeLimit).toOption.get,
            envelopeLimit,
            inputLimit
          )
          .toOption
          .get
        result <- FileTaskContext
          .managed(
            FileTaskWorkspace(root),
            Map.empty,
            event => progress.update(_ :+ event) *> runtime.reportProgress(invocation)(event)
          )
          .use(context =>
            runtime.run(
              invocation,
              context,
              FileResultPublisher(root.resolve("result.json"), envelopeLimit, resultLimit)
            )
          )
        progressValues <- progress.get
        eventValues <- events.get
      yield
        result match
          case WorkerRunResult.Succeeded(envelope, publication) =>
            assertEquals(task.outputCodec.decode(envelope.value.get), Right(42))
            assertEquals(envelope.outputs.entries.map(_.path), Vector(outputPath))
            assertEquals(
              ResultEnvelopeCodec.decode(
                publication.envelopeBytes,
                envelopeLimit,
                resultLimit
              ),
              Right(envelope)
            )
          case other => fail(s"expected success, received $other")
        assertEquals(progressValues.map(_.message), Vector("computing"))
        assert(eventValues.head.payload == WorkerEventPayload.Started)
        assert(eventValues.exists(_.payload.isInstanceOf[WorkerEventPayload.Progress]))
        assert(eventValues.last.payload.isInstanceOf[WorkerEventPayload.ResultPublished])
    }
  }

  test("missing and extra declared outputs become structured failure envelopes") {
    def runCase(task: ExampleTask, declared: Vector[RelativeOutputPath]): IO[WorkerRunResult] =
      temporaryDirectory.use { root =>
        val registry = TaskRegistry.from(Vector(TaskRegistration(task))).toOption.get
        for
          runtime <- WorkerRuntime.create(release, registry)
          result <- FileTaskContext
            .managed(FileTaskWorkspace(root), Map.empty)
            .use(context =>
              runtime.run(
                invocationFor(task, "1", declared),
                context,
                FileResultPublisher(root.resolve("result.json"), envelopeLimit, resultLimit)
              )
            )
        yield result
      }

    for
      missing <- runCase(ExampleTask(Vector.empty), Vector(outputPath))
      extra <- runCase(ExampleTask(Vector(outputPath, extraPath)), Vector(outputPath))
    yield
      assertOutputFailure(missing, classOf[OutputValidationFailure.Missing])
      assertOutputFailure(extra, classOf[OutputValidationFailure.Unexpected])
  }

  test("schema mismatch and oversized result fail without a typed success") {
    temporaryDirectory.use { root =>
      val task = ExampleTask(Vector.empty)
      val registry = TaskRegistry.from(Vector(TaskRegistration(task))).toOption.get
      for
        runtime <- WorkerRuntime.create(release, registry)
        wrongSchema = invocationFor(task, "1", Vector.empty).copy(
          operation = task.operation.descriptor.copy(
            inputSchema = SchemaId.from("wrong.input.v1").toOption.get
          )
        )
        mismatch <- FileTaskContext
          .managed(FileTaskWorkspace(root.resolve("wrong")), Map.empty)
          .use { context =>
            runtime.run(
              wrongSchema,
              context,
              FileResultPublisher(root.resolve("wrong-result.json"), envelopeLimit, resultLimit)
            )
          }
        tiny = invocationFor(task, "100", Vector.empty).copy(
          maximumResultBytes = ByteLimit.from(1).toOption.get
        )
        oversized <- FileTaskContext
          .managed(FileTaskWorkspace(root.resolve("large")), Map.empty)
          .use { context =>
            runtime.run(
              tiny,
              context,
              FileResultPublisher(
                root.resolve("large-result.json"),
                envelopeLimit,
                tiny.maximumResultBytes
              )
            )
          }
      yield
        assert(failureOf(mismatch).isInstanceOf[TaskFailure.SchemaMismatch])
        assert(failureOf(oversized).isInstanceOf[TaskFailure.ResultTooLarge])
    }
  }

  test("managed context hides native paths while native context is explicitly lower assurance") {
    temporaryDirectory.use { root =>
      for
        managed <- FileTaskContext
          .managed(FileTaskWorkspace(root.resolve("managed")), Map.empty)
          .use { context =>
            IO.pure(context.assurance -> context.isInstanceOf[NativeTaskContext[IO]])
          }
        native <- FileTaskContext.native(FileTaskWorkspace(root.resolve("native")), Map.empty).use {
          context => IO.pure(context.assurance -> context.nativeRoot)
        }
      yield
        assertEquals(managed, TaskContextAssurance.ManagedCapabilities -> false)
        assertEquals(native._1, TaskContextAssurance.NativeFilesystem)
        assertEquals(native._2, root.resolve("native").toAbsolutePath.normalize())
    }
  }

  test("managed input and scratch capabilities preserve logical-name and path locality") {
    temporaryDirectory.use { root =>
      val inputName = InputName.from("dataset").toOption.get
      val missingName = InputName.from("undeclared").toOption.get
      val source = root.resolve("source.txt")
      for
        _ <- IO.blocking { val _ = Files.write(source, "input".getBytes("UTF-8")) }
        result <- FileTaskContext
          .managed(FileTaskWorkspace(root.resolve("workspace")), Map(inputName -> source))
          .use { context =>
            for
              available <- context.inputs.read(inputName, inputLimit)
              missing <- context.inputs.read(missingName, inputLimit)
              scratchPath <- context.scratch.use(directory => IO.pure(directory.path))
              scratchRemoved <- IO.blocking(!Files.exists(scratchPath))
            yield (available, missing, scratchPath, scratchRemoved)
          }
      yield
        assertEquals(
          result._1,
          Right("input".getBytes("UTF-8").toVector)
        )
        assertEquals(result._2, Left(TaskIoFailure.InputNotDeclared(missingName)))
        assert(result._3.startsWith(root.resolve("workspace").resolve("scratch")))
        assert(result._4)
    }
  }

  test("result publication is atomic-last and cannot overwrite an existing envelope") {
    temporaryDirectory.use { root =>
      val task = ExampleTask(Vector.empty)
      val envelope = ResultEnvelope.succeeded(
        SubmissionKey.from("publish-once").toOption.get,
        AttemptId.from("publish-once-attempt").toOption.get,
        AttemptEpoch.initial,
        None,
        WorkloadOperation.Registered(task.operation.id, task.operation.version),
        task.operation.outputSchema,
        "1".getBytes("UTF-8").toVector,
        OutputManifest.empty,
        release,
        java.time.Instant.parse("2026-07-22T12:00:00Z")
      )
      val publisher = FileResultPublisher(root.resolve("result.json"), envelopeLimit, resultLimit)
      for results <- (publisher.publish(envelope), publisher.publish(envelope)).parTupled
      yield
        assertEquals(Vector(results._1, results._2).count(_.isRight), 1)
        assertEquals(
          Vector(results._1, results._2).count(
            _.left.exists(_.isInstanceOf[ResultPublicationFailure.AlreadyPublished])
          ),
          1
        )
    }
  }

  test("opaque declared-output inspection rejects exit-zero work with a missing file") {
    temporaryDirectory.use { root =>
      FileTaskContext
        .inspectDeclaredOutputs(root, Vector(outputPath), outputLimit)
        .map {
          case Left(TaskIoFailure.OutputValidation(failures)) =>
            assert(failures.contains(OutputValidationFailure.Missing(outputPath)))
          case other => fail(s"expected missing declared output, received $other")
        }
    }
  }

  test("file worker event sink appends independently decodable canonical records") {
    temporaryDirectory.use { root =>
      val path = root.resolve("worker-events.ndjson")
      val events = (0L until 16L).toVector.map(sequence =>
        workerEvent(sequence, WorkerEventPayload.ProcessExited(0))
      )
      for
        sink <- FileWorkerEventSink.create(path, envelopeLimit)
        _ <- events.parTraverse_(sink.append)
        lines <- IO.blocking(Files.readAllLines(path).asScala.toVector)
      yield
        assertEquals(lines.size, events.size)
        val decoded = lines.map(line =>
          io.github.bbuchsbaum.scalaslurm.protocol.WorkerEventCodec
            .decode(s"$line\n".getBytes("UTF-8").toVector, envelopeLimit)
        )
        assert(decoded.forall(_.isRight))
        assertEquals(decoded.flatMap(_.toOption.map(_.sequence)).sorted, events.map(_.sequence))
    }
  }

  test("registered task submitter stages a wire invocation before calling Scheduler") {
    temporaryDirectory.use { root =>
      val task = ExampleTask(Vector(outputPath))
      val payload = Payload.RegisteredTask(
        task.operation,
        "41",
        task.inputCodec,
        ResultContract.Structured
          .from(task.outputCodec, resultLimit, Vector(outputPath))
          .toOption
          .get
      )
      val request = JobRequest(
        SubmissionKey.from("typed-scheduler-submit").toOption.get,
        JobName.from("typed-scheduler-submit").toOption.get,
        payload,
        ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
      )
      val executable = root.resolve("worker-distribution")
      for
        _ <- IO.blocking {
          val _ = Files.write(executable, "#!/bin/sh\nexit 0\n".getBytes("UTF-8"))
          val _ = Files.setPosixFilePermissions(
            executable,
            PosixFilePermissions.fromString("rwx------")
          )
        }
        observed <- Ref.of[IO, Option[ScriptSource]](None)
        scheduler = capturingScheduler(observed)
        launcher = RegisteredTaskLauncher(
          WorkerLaunchSettings(
            root.resolve("workspace"),
            executable,
            release,
            inputLimit,
            envelopeLimit,
            envelopeLimit,
            outputLimit
          )
        )
        result <- RegisteredTaskSubmitter(launcher, scheduler).submit(request)
        captured <- observed.get
      yield result match
        case RegisteredSubmissionResult.Submitted(prepared, SubmissionAttempt.Completed(_)) =>
          val invocationBytes = Files.readAllBytes(prepared.invocationPath).toVector
          assertEquals(
            TaskInvocationCodec.decode(invocationBytes, envelopeLimit, inputLimit),
            Right(prepared.invocation)
          )
          assertEquals(prepared.resultHandle.resultSchema, task.operation.outputSchema)
          val launchText = Files.readString(prepared.launchScript)
          assert(launchText.contains("--invocation"))
          assert(!launchText.contains("'41'"))
          assert(!launchText.contains("NDE="))
          assertEquals(captured, Some(ScriptSource.ExistingRemote(prepared.launchScript.toString)))
        case other => fail(s"expected scheduler submission, received $other")
    }
  }

  test("registered task arrays stage isolated inputs, logs, results, and element bindings") {
    temporaryDirectory.use { root =>
      val task = ExampleTask(Vector(outputPath))
      val contract = ResultContract.Structured
        .from(task.outputCodec, resultLimit, Vector(outputPath))
        .toOption
        .get
      val arrayRequest = RegisteredTaskArrayRequest(
        submissionKey = SubmissionKey.from("typed-array").toOption.get,
        name = JobName.from("typed-array").toOption.get,
        operation = task.operation,
        inputCodec = task.inputCodec,
        resultContract = contract,
        resources = ResourceRequest.validate(1, 1, None, None, None).toOption.get,
        environment = Map.empty,
        elements = NonEmptyVector.of(
          RegisteredTaskArrayElement(
            ArrayIndex.from(0).toOption.get,
            SubmissionKey.from("typed-array-0").toOption.get,
            "41"
          ),
          RegisteredTaskArrayElement(
            ArrayIndex.from(1).toOption.get,
            SubmissionKey.from("typed-array-1").toOption.get,
            "99"
          )
        ),
        maximumConcurrent = Some(PositiveInt.from("maximumConcurrent", 1).toOption.get)
      )
      val executable = root.resolve("worker-distribution")
      for
        _ <- IO.blocking {
          val _ = Files.write(executable, "#!/bin/sh\nexit 0\n".getBytes("UTF-8"))
          val _ = Files.setPosixFilePermissions(
            executable,
            PosixFilePermissions.fromString("rwx------")
          )
        }
        observed <- Ref.of[IO, Option[ScriptSource]](None)
        launcher = RegisteredTaskLauncher(
          WorkerLaunchSettings(
            root.resolve("workspace"),
            executable,
            release,
            inputLimit,
            envelopeLimit,
            envelopeLimit,
            outputLimit
          )
        )
        result <- RegisteredTaskArraySubmitter(launcher, capturingScheduler(observed)).submit(
          arrayRequest
        )
      yield result match
        case RegisteredArraySubmissionResult.Submitted(prepared, SubmissionAttempt.Completed(_)) =>
          assertEquals(prepared.schedulerRequest.array, Some(prepared.arrayRequest))
          assertEquals(
            prepared.elements.toVector.map(_.invocation.inputBytes),
            Vector("41", "99").map(_.getBytes("UTF-8").toVector)
          )
          assertEquals(prepared.elements.toVector.map(_.resultPath).distinct.size, 2)
          assertEquals(prepared.plan.elements.toVector.map(_.stdout.locator).distinct.size, 2)
          assertEquals(prepared.plan.elements.toVector.map(_.stderr.locator).distinct.size, 2)
          val parent = JobRef(JobId.from("8100").toOption.get, None, None)
          val bindings = prepared.plan.bind(parent).toOption.get.toVector
          assertEquals(
            bindings.map(_.job.arrayIndex),
            Vector(0, 1).map(value => ArrayIndex.from(value).toOption)
          )
          assertEquals(
            bindings.map(_.resultHandle.flatMap(_.job)),
            bindings.map(value => Some(value.job))
          )
          val script = Files.readString(prepared.launchScript)
          assert(script.contains("SLURM_ARRAY_TASK_ID"))
          assert(script.contains("'0')"))
          assert(script.contains("'1')"))
          assert(!script.contains("'41'"))
          assert(!script.contains("'99'"))
        case other => fail(s"expected array submission, received $other")
    }
  }

  final private case class ExampleTask(writes: Vector[RelativeOutputPath])
      extends ScalaTask[String, Int]:
    val operation: OperationRef[String, Int] = OperationRef(
      OperationId.from("example.increment").toOption.get,
      OperationVersion.from("1").toOption.get,
      SchemaId.from("example.string.v1").toOption.get,
      ResultSchemaId.from("example.int.v1").toOption.get
    )
    val inputCodec: InputCodec[String] = new InputCodec[String]:
      val schemaId: SchemaId = operation.inputSchema
      def encode(value: String): Either[ResultCodecFailure, Vector[Byte]] =
        Right(value.getBytes("UTF-8").toVector)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, String] =
        Right(new String(bytes.toArray, "UTF-8"))
    val outputCodec: ResultCodec[Int] = new ResultCodec[Int]:
      val schemaId: ResultSchemaId = operation.outputSchema
      def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
        Right(value.toString.getBytes("UTF-8").toVector)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
        bytesToInt(bytes)

    def run(input: String, context: TaskContext[IO]): IO[Int] =
      for
        _ <- context.progress(ProgressEvent("computing"))
        value <- IO.fromEither(
          bytesToInt(input.getBytes("UTF-8").toVector).left.map(failure =>
            new IllegalArgumentException(failure.message)
          )
        )
        _ <- writes.traverse_ { path =>
          context.outputs
            .write(path, s"answer=${value + 1}\n".getBytes("UTF-8").toVector, outputLimit)
            .flatMap {
              case Right(_)      => IO.unit
              case Left(failure) => IO.raiseError(new IllegalStateException(failure.toString))
            }
        }
      yield value + 1

  private def invocationFor(
      task: ExampleTask,
      input: String,
      declared: Vector[RelativeOutputPath]
  ): TaskInvocation =
    TaskInvocations
      .encode(
        task,
        input,
        SubmissionKey.from("typed-task").toOption.get,
        AttemptId.from("typed-task-attempt").toOption.get,
        AttemptEpoch.initial,
        None,
        declared,
        inputLimit,
        resultLimit,
        envelopeLimit,
        outputLimit,
        release
      )
      .toOption
      .get

  private def workerEvent(sequence: Long, payload: WorkerEventPayload): WorkerEvent =
    WorkerEvent(
      sequence,
      SubmissionKey.from("worker-events").toOption.get,
      AttemptId.from("worker-events-attempt").toOption.get,
      AttemptEpoch.initial,
      WorkloadOperation.Registered(
        OperationId.from("example.events").toOption.get,
        OperationVersion.from("1").toOption.get
      ),
      release,
      java.time.Instant.parse("2026-07-22T12:00:00Z").plusSeconds(sequence),
      payload
    )

  private def bytesToInt(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
    scala.util
      .Try(new String(bytes.toArray, "UTF-8").toInt)
      .toEither
      .left
      .map(error => ResultCodecFailure("invalid-int", error.getMessage))

  private def collectingSink(events: Ref[IO, Vector[WorkerEvent]]): WorkerEventSink[IO] =
    new WorkerEventSink[IO]:
      def append(event: WorkerEvent): IO[Unit] = events.update(_ :+ event)

  private def capturingScheduler(observed: Ref[IO, Option[ScriptSource]]): Scheduler[IO] =
    new Scheduler[IO]:
      def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
        IO.raiseError(new AssertionError("capabilities must not be queried"))

      def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] = request.payload match
        case Payload.Script(source, _, _) =>
          observed.set(Some(source)) *> IO.pure(
            SubmissionAttempt.Completed(
              Submission.Accepted(
                JobRef(JobId.from("8100").toOption.get, None, None),
                EvidenceBundle(
                  BoundedEvidence.capture(
                    EvidenceSource.WorkerEvent,
                    java.time.Instant.parse("2026-07-22T12:00:00Z"),
                    Vector.empty
                  )
                )
              )
            )
          )
        case _ => IO.raiseError(new AssertionError("registered task was not lowered to a script"))

      def observe(
          _jobs: NonEmptyVector[JobRef]
      ): IO[SchedulerQueryResult[ObservationBatch]] =
        IO.raiseError(new AssertionError("observe must not be called"))

      def accounting(
          _jobs: NonEmptyVector[JobRef]
      ): IO[SchedulerQueryResult[AccountingBatch]] =
        IO.raiseError(new AssertionError("accounting must not be called"))

      def cancel(_job: JobRef): IO[CancellationAttempt] =
        IO.raiseError(new AssertionError("cancel must not be called"))

  private def failureOf(result: WorkerRunResult): TaskFailure = result match
    case WorkerRunResult.Failed(failure, _, _) => failure
    case other                                 => fail(s"expected failure, received $other")

  private def assertOutputFailure(
      result: WorkerRunResult,
      expected: Class[? <: OutputValidationFailure]
  ): Unit =
    failureOf(result) match
      case TaskFailure.Outputs(TaskIoFailure.OutputValidation(failures)) =>
        assert(failures.exists(expected.isInstance))
      case other => fail(s"expected output validation failure, received $other")

  private def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("scala-slurm-worker-test")))(path =>
      IO.blocking {
        val stream = Files.walk(path)
        try
          stream
            .sorted(java.util.Comparator.reverseOrder())
            .iterator()
            .asScala
            .foreach { value =>
              val _ = Files.deleteIfExists(value)
            }
        finally stream.close()
      }
    )
