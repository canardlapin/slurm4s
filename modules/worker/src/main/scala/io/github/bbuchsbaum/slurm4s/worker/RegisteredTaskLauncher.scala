package io.github.bbuchsbaum.slurm4s.worker

import cats.data.NonEmptyChain
import cats.data.NonEmptyVector
import cats.effect.Clock
import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest

final case class WorkerLaunchSettings(
    workspace: Path,
    executable: Path,
    workerRelease: WorkerRelease,
    maximumInputBytes: ByteLimit,
    maximumInvocationBytes: ByteLimit,
    maximumEnvelopeBytes: ByteLimit,
    maximumOutputBytes: ByteLimit
)

final case class PreparedRegisteredSubmission[A](
    schedulerRequest: JobRequest[A],
    invocation: TaskInvocation,
    invocationPath: Path,
    outputRoot: Path,
    resultPath: Path,
    eventPath: Path,
    launchScript: Path,
    resultHandle: DurableResultHandle
)

final case class PreparedRemoteRegisteredSubmission(
    schedulerRequest: JobRequest[NoResult],
    invocation: TaskInvocation,
    invocationPath: Path,
    outputRoot: Path,
    resultPath: Path,
    eventPath: Path,
    launchScript: Path,
    resultRef: RemoteResultRef,
    resultHandle: DurableResultHandle
)

final case class PreparedRemoteRegisteredBatchSubmission(
    schedulerRequest: JobRequest[NoResult],
    topology: BatchTopology,
    elements: NonEmptyVector[(ArrayIndex, PreparedRemoteRegisteredSubmission)],
    launchScript: Path
)

final case class PreparedRemoteScriptBatchElement(
    index: ArrayIndex,
    exitRef: RemoteScriptExitRef,
    stdout: LogRef,
    stderr: LogRef,
    launchScript: Path
)

final case class PreparedRemoteScriptBatchSubmission(
    schedulerRequest: JobRequest[NoResult],
    topology: BatchTopology,
    elements: NonEmptyVector[PreparedRemoteScriptBatchElement],
    launchScript: Path
)

final case class RegisteredTaskArrayElement[I](
    index: ArrayIndex,
    submissionKey: SubmissionKey,
    input: I
)

final case class RegisteredTaskArrayRequest[I, O](
    submissionKey: SubmissionKey,
    name: JobName,
    operation: OperationRef[I, O],
    inputCodec: InputCodec[I],
    resultContract: ResultContract[O],
    resources: ResourceRequest,
    environment: Map[EnvName, String],
    elements: NonEmptyVector[RegisteredTaskArrayElement[I]],
    maximumConcurrent: Option[PositiveInt],
    retrySafety: RetrySafety = RetrySafety.Unknown
)

final case class PreparedRegisteredArraySubmission[O](
    schedulerRequest: JobRequest[O],
    arrayRequest: JobArrayRequest,
    elements: NonEmptyVector[PreparedRegisteredSubmission[O]],
    launchScript: Path,
    plan: JobArrayPlan
)

enum RegisteredSubmissionResult[A]:
  case PreparationFailed(diagnostics: Diagnostics)
  case Submitted(prepared: PreparedRegisteredSubmission[A], result: SubmissionAttempt)

enum RegisteredArraySubmissionResult[A]:
  case PreparationFailed(diagnostics: Diagnostics)
  case Submitted(prepared: PreparedRegisteredArraySubmission[A], result: SubmissionAttempt)

final class RegisteredTaskLauncher(settings: WorkerLaunchSettings):
  def prepare[A](
      request: JobRequest[A],
      attemptEpoch: AttemptEpoch = AttemptEpoch.initial
  ): IO[Either[Diagnostics, PreparedRegisteredSubmission[A]]] =
    request.payload match
      case task: Payload.RegisteredTask[?, A] @unchecked =>
        IO.blocking(prepareBlocking(request, task, attemptEpoch)).attempt.map {
          case Right(value) => value
          case Left(error)  => Left(diagnostics("typed-task-preparation-failed", error))
        }
      case _ =>
        IO.pure(
          Left(
            Diagnostics.one(
              Diagnostic(
                "not-a-registered-task",
                "the registered task launcher accepts Payload.RegisteredTask values only"
              )
            )
          )
        )

  def prepareRemote(
      request: RemoteRegisteredTaskRequest,
      attemptEpoch: AttemptEpoch = AttemptEpoch.initial
  ): IO[Either[Diagnostics, PreparedRemoteRegisteredSubmission]] =
    IO.blocking(prepareRemoteBlocking(request, attemptEpoch)).attempt.map {
      case Right(value) => value
      case Left(error)  => Left(diagnostics("remote-task-preparation-failed", error))
    }

  def prepareRemoteBatch(
      request: RemoteRegisteredBatchRequest,
      attemptEpoch: AttemptEpoch = AttemptEpoch.initial
  ): IO[Either[Diagnostics, PreparedRemoteRegisteredBatchSubmission]] =
    IO.blocking(prepareRemoteBatchBlocking(request, attemptEpoch)).attempt.map {
      case Right(value) => value
      case Left(error)  => Left(diagnostics("remote-batch-preparation-failed", error))
    }

  def prepareRemoteScriptBatch(
      request: RemoteScriptBatchRequest,
      attemptEpoch: AttemptEpoch = AttemptEpoch.initial
  ): IO[Either[Diagnostics, PreparedRemoteScriptBatchSubmission]] =
    IO.blocking(prepareRemoteScriptBatchBlocking(request, attemptEpoch)).attempt.map {
      case Right(value) => value
      case Left(error)  => Left(diagnostics("remote-script-batch-preparation-failed", error))
    }

  def readRemoteResult(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit
  ): IO[RemoteResultRead] =
    Clock[IO].realTimeInstant.flatMap { observedAt =>
      IO.blocking(readRemoteResultBlocking(ref, maximumBytes, observedAt)).handleError { error =>
        remoteReadFailure(
          "remote-result-read-failed",
          error.getClass.getSimpleName,
          observedAt,
          Vector.empty,
          0L
        )
      }
    }

  def readRemoteScriptExit(
      ref: RemoteScriptExitRef
  ): IO[RemoteScriptExitRead] =
    Clock[IO].realTimeInstant.flatMap { observedAt =>
      IO.blocking(readRemoteScriptExitBlocking(ref, observedAt)).handleError { error =>
        scriptExitFailure(
          "remote-script-exit-read-failed",
          error.getClass.getSimpleName,
          observedAt,
          Vector.empty,
          0L
        )
      }
    }

  def prepareArray[I, O](
      request: RegisteredTaskArrayRequest[I, O],
      attemptEpoch: AttemptEpoch = AttemptEpoch.initial
  ): IO[Either[Diagnostics, PreparedRegisteredArraySubmission[O]]] =
    IO.blocking(prepareArrayBlocking(request, attemptEpoch)).attempt.map {
      case Right(value) => value
      case Left(error)  => Left(diagnostics("typed-array-preparation-failed", error))
    }

  private def prepareArrayBlocking[I, O](
      request: RegisteredTaskArrayRequest[I, O],
      attemptEpoch: AttemptEpoch
  ): Either[Diagnostics, PreparedRegisteredArraySubmission[O]] =
    for
      _ <- validateExecutable()
      arrayRequest <- JobArrayRequest
        .from(request.elements.toVector.map(_.index), request.maximumConcurrent)
        .left
        .map(arrayDiagnostics)
      prepared <- request.elements.toVector.traverse { element =>
        val task = Payload.RegisteredTask(
          request.operation,
          element.input,
          request.inputCodec,
          request.resultContract
        )
        prepareBlocking(
          JobRequest(
            element.submissionKey,
            request.name,
            task,
            request.resources,
            request.environment,
            retrySafety = request.retrySafety
          ),
          task,
          attemptEpoch
        )
      }
      aggregateAttempt <- deterministicAttempt(request.submissionKey, request.operation).left.map(
        problem => Diagnostics.one(Diagnostic("invalid-array-attempt-id", problem.reason))
      )
      directory = epochDirectory(aggregateAttempt, attemptEpoch)
      _ <- createPrivateDirectory(directory)
      launchScript = directory.resolve("launch-worker-array.sh")
      identities = prepared.zip(request.elements.toVector).map { case (value, element) =>
        val elementDirectory = value.invocationPath.getParent
        ArrayElementIdentity(
          element.index,
          element.submissionKey,
          value.invocation.attemptId,
          value.invocation.attemptEpoch,
          LogRef(
            value.invocation.attemptId,
            value.invocation.attemptEpoch,
            LogStream.Stdout,
            elementDirectory.resolve("stdout.log").toString
          ),
          LogRef(
            value.invocation.attemptId,
            value.invocation.attemptEpoch,
            LogStream.Stderr,
            elementDirectory.resolve("stderr.log").toString
          ),
          request.resultContract.descriptor,
          Some(value.resultHandle)
        )
      }
      plan <- JobArrayPlan.from(arrayRequest, identities).left.map(arrayDiagnostics)
      _ <- writeStable(
        launchScript,
        arrayLaunchScriptBytes(prepared.zip(request.elements.toVector)),
        executable = true
      )
      schedulerRequest = JobRequest(
        request.submissionKey,
        request.name,
        Payload.Script(
          ScriptSource.ExistingRemote(launchScript.toString),
          Vector.empty,
          request.resultContract
        ),
        request.resources,
        request.environment,
        Some(arrayRequest),
        request.retrySafety
      )
    yield PreparedRegisteredArraySubmission(
      schedulerRequest,
      arrayRequest,
      NonEmptyVector.fromVectorUnsafe(prepared),
      launchScript,
      plan
    )

  private def prepareRemoteBatchBlocking(
      request: RemoteRegisteredBatchRequest,
      attemptEpoch: AttemptEpoch
  ): Either[Diagnostics, PreparedRemoteRegisteredBatchSubmission] =
    for
      _ <- validateExecutable()
      _ <- validateRemoteBatch(request)
      prepared <- request.elements.traverse { element =>
        prepareRemoteBlocking(
          RemoteRegisteredTaskRequest(
            element.submissionKey,
            request.name,
            request.operation,
            element.inputBytes,
            request.topology.resources,
            request.environment,
            request.maximumResultBytes,
            request.declaredOutputs,
            request.retrySafety
          ),
          attemptEpoch
        ).map(element.index -> _)
      }
      aggregateAttempt <- deterministicAttempt(request.submissionKey, request.operation).left.map(
        problem => Diagnostics.one(Diagnostic("invalid-batch-attempt-id", problem.reason))
      )
      directory = epochDirectory(aggregateAttempt, attemptEpoch)
      _ <- createPrivateDirectory(directory)
      launchScript = directory.resolve("launch-worker-batch.sh")
      _ <- writeRemoteBatchScripts(directory, launchScript, request.topology, prepared)
      schedulerRequest = JobRequest(
        request.submissionKey,
        request.name,
        Payload.Script(
          ScriptSource.ExistingRemote(launchScript.toString),
          Vector.empty,
          ResultContract.ExitOnly
        ),
        request.topology.resources,
        request.environment,
        request.topology.array,
        request.retrySafety
      )
    yield PreparedRemoteRegisteredBatchSubmission(
      schedulerRequest,
      request.topology,
      prepared,
      launchScript
    )

  private def prepareRemoteScriptBatchBlocking(
      request: RemoteScriptBatchRequest,
      attemptEpoch: AttemptEpoch
  ): Either[Diagnostics, PreparedRemoteScriptBatchSubmission] =
    for
      _ <- validateRemoteScriptBatch(request)
      aggregateAttempt <- scriptAttempt(request.submissionKey, request.program).left.map(problem =>
        Diagnostics.one(Diagnostic("invalid-script-batch-attempt-id", problem.reason))
      )
      directory = epochDirectory(aggregateAttempt, attemptEpoch)
      _ <- createPrivateDirectory(directory)
      program <- materializeRemoteScript(request.program, directory)
      prepared <- request.elements.traverse { element =>
        prepareRemoteScriptElement(
          element,
          request.program.invocation,
          program,
          attemptEpoch
        )
      }
      launchScript = directory.resolve("launch-script-batch.sh")
      _ <- writeRemoteScriptBatchScripts(
        directory,
        launchScript,
        request.topology,
        prepared
      )
      schedulerRequest = JobRequest(
        request.submissionKey,
        request.name,
        Payload.Script(
          ScriptSource.ExistingRemote(launchScript.toString),
          Vector.empty,
          ResultContract.ExitOnly
        ),
        request.topology.resources,
        request.environment,
        request.topology.array,
        request.retrySafety
      )
    yield PreparedRemoteScriptBatchSubmission(
      schedulerRequest,
      request.topology,
      prepared,
      launchScript
    )

  private def prepareBlocking[I, O](
      request: JobRequest[O],
      task: Payload.RegisteredTask[I, O],
      attemptEpoch: AttemptEpoch
  ): Either[Diagnostics, PreparedRegisteredSubmission[O]] =
    for
      _ <- validateExecutable()
      attemptId <- deterministicAttempt(request.submissionKey, task.operation).left
        .map(problem => Diagnostics.one(Diagnostic("invalid-attempt-id", problem.reason)))
      directory = epochDirectory(attemptId, attemptEpoch)
      _ <- createPrivateDirectory(directory)
      invocation <- TaskInvocations
        .encodeRegistered(
          task,
          request.retrySafety,
          request.submissionKey,
          attemptId,
          attemptEpoch,
          None,
          settings.maximumInputBytes,
          settings.maximumEnvelopeBytes,
          settings.maximumOutputBytes,
          settings.workerRelease
        )
        .left
        .map(failure => Diagnostics.one(Diagnostic("invalid-task-invocation", failure.toString)))
      bytes <- TaskInvocationCodec
        .encode(invocation, settings.maximumInvocationBytes)
        .left
        .map(failure => Diagnostics.one(Diagnostic("task-invocation-codec", failure.toString)))
      invocationPath = directory.resolve("task-invocation.json")
      resultPath = directory.resolve("result.json")
      eventPath = directory.resolve("worker-events.ndjson")
      launchScript = directory.resolve("launch-worker.sh")
      _ <- writeStable(invocationPath, bytes, executable = false)
      scriptBytes = launchScriptBytes(invocationPath, resultPath, eventPath)
      _ <- writeStable(launchScript, scriptBytes, executable = true)
      schedulerRequest = request.copy(
        payload = Payload.Script(
          ScriptSource.ExistingRemote(launchScript.toString),
          Vector.empty,
          task.resultContract
        )
      )
      // The handle is DERIVED from the prepared attempt rather than built beside it, so the
      // schema, limits and declared outputs it advertises cannot drift from the plan that reads
      // the result.
      launchSpec <- LaunchSpec.fromRequest(schedulerRequest)
      prepared = PreparedAttempt(
        PreparedJob(
          launchSpec,
          ResultPlan(
            task.operation.outputSchema,
            task.resultContract,
            invocation.maximumResultBytes,
            settings.maximumEnvelopeBytes,
            invocation.declaredOutputs
          ),
          io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles.digestOf(scriptBytes)
        ),
        attemptId,
        attemptEpoch,
        WorkloadOperation.Registered(task.operation.id, task.operation.version),
        settings.workerRelease
      )
      handle = prepared.durableHandle(None)
    yield PreparedRegisteredSubmission(
      schedulerRequest,
      invocation,
      invocationPath,
      directory.resolve("outputs"),
      resultPath,
      eventPath,
      launchScript,
      handle
    )

  private def prepareRemoteBlocking(
      request: RemoteRegisteredTaskRequest,
      attemptEpoch: AttemptEpoch
  ): Either[Diagnostics, PreparedRemoteRegisteredSubmission] =
    for
      _ <- validateExecutable()
      _ <- Either.cond(
        request.inputBytes.size <= settings.maximumInputBytes.value,
        (),
        Diagnostics.one(
          Diagnostic(
            "remote-task-input-too-large",
            "encoded task input exceeds the target worker limit"
          )
        )
      )
      _ <- Either.cond(
        request.maximumResultBytes.value <= settings.maximumEnvelopeBytes.value,
        (),
        Diagnostics.one(
          Diagnostic(
            "remote-task-result-limit-too-large",
            "requested result bytes exceed the target envelope limit"
          )
        )
      )
      attemptId <- deterministicAttempt(request.submissionKey, request.operation).left.map(
        problem => Diagnostics.one(Diagnostic("invalid-attempt-id", problem.reason))
      )
      directory = epochDirectory(attemptId, attemptEpoch)
      _ <- createPrivateDirectory(directory)
      invocation <- TaskInvocation
        .from(
          request.submissionKey,
          attemptId,
          attemptEpoch,
          None,
          request.operation,
          request.inputBytes,
          request.declaredOutputs,
          settings.maximumInputBytes,
          request.maximumResultBytes,
          settings.maximumEnvelopeBytes,
          settings.maximumOutputBytes,
          settings.workerRelease,
          request.retrySafety
        )
        .left
        .map(problem => Diagnostics.one(Diagnostic("invalid-task-invocation", problem.reason)))
      invocationBytes <- TaskInvocationCodec
        .encode(invocation, settings.maximumInvocationBytes)
        .left
        .map(failure => Diagnostics.one(Diagnostic("task-invocation-codec", failure.toString)))
      resultRef = RemoteResultRef(attemptId, attemptEpoch)
      handle <- DurableResultHandle
        .from(
          request.submissionKey,
          attemptId,
          attemptEpoch,
          None,
          WorkloadOperation.Registered(request.operation.id, request.operation.version),
          request.operation.outputSchema,
          request.maximumResultBytes,
          settings.maximumEnvelopeBytes,
          request.declaredOutputs,
          settings.workerRelease,
          request.retrySafety
        )
        .left
        .map(problem => Diagnostics.one(Diagnostic("invalid-result-handle", problem.reason)))
      handleBytes <- DurableResultHandleCodec
        .encode(handle, RemoteTaskWireLimits.MaximumHandleBytes)
        .left
        .map(failure => Diagnostics.one(Diagnostic("result-handle-codec", failure.toString)))
      invocationPath = directory.resolve("task-invocation.json")
      handlePath = directory.resolve("result-handle.json")
      resultPath = directory.resolve("result.json")
      eventPath = directory.resolve("worker-events.ndjson")
      launchScript = directory.resolve("launch-worker.sh")
      _ <- writeStable(invocationPath, invocationBytes, executable = false)
      _ <- writeStable(handlePath, handleBytes, executable = false)
      _ <- writeStable(
        launchScript,
        launchScriptBytes(invocationPath, resultPath, eventPath),
        executable = true
      )
      schedulerRequest = JobRequest(
        request.submissionKey,
        request.name,
        Payload.Script(
          ScriptSource.ExistingRemote(launchScript.toString),
          Vector.empty,
          ResultContract.ExitOnly
        ),
        request.resources,
        request.environment,
        retrySafety = request.retrySafety
      )
    yield PreparedRemoteRegisteredSubmission(
      schedulerRequest,
      invocation,
      invocationPath,
      directory.resolve("outputs"),
      resultPath,
      eventPath,
      launchScript,
      resultRef,
      handle
    )

  private def readRemoteResultBlocking(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit,
      observedAt: java.time.Instant
  ): RemoteResultRead =
    if maximumBytes.value > settings.maximumEnvelopeBytes.value then
      remoteReadFailure(
        "remote-result-limit-rejected",
        "requested read limit exceeds the target envelope limit",
        observedAt,
        Vector.empty,
        0L
      )
    else
      val directory = epochDirectory(ref.attemptId, ref.attemptEpoch)
      val handlePath = directory.resolve("result-handle.json")
      if !Files.isRegularFile(handlePath) then
        remoteReadFailure(
          "remote-result-reference-unknown",
          "no durable result metadata exists for the reference",
          observedAt,
          Vector.empty,
          0L
        )
      else
        val handleCapture = readBounded(handlePath, RemoteTaskWireLimits.MaximumHandleBytes)
        DurableResultHandleCodec
          .decode(handleCapture._1, RemoteTaskWireLimits.MaximumHandleBytes) match
          case Left(failure) =>
            remoteReadFailure(
              "remote-result-handle-invalid",
              failure.toString,
              observedAt,
              handleCapture._1,
              handleCapture._2
            )
          case Right(handle)
              if handle.attemptId != ref.attemptId ||
                handle.attemptEpoch != ref.attemptEpoch =>
            remoteReadFailure(
              "remote-result-reference-mismatch",
              "stored result metadata identifies another attempt",
              observedAt,
              handleCapture._1,
              handleCapture._2
            )
          case Right(handle) =>
            val resultPath = directory.resolve("result.json")
            if !Files.isRegularFile(resultPath) then RemoteResultRead.Pending(observedAt)
            else
              val capture = readBounded(resultPath, maximumBytes)
              if capture._2 > maximumBytes.value.toLong then
                remoteReadFailure(
                  "remote-result-envelope-too-large",
                  "result envelope exceeds the requested bounded read",
                  observedAt,
                  capture._1,
                  capture._2
                )
              else RemoteResultRead.Available(handle, capture._1, observedAt)

  private def readRemoteScriptExitBlocking(
      ref: RemoteScriptExitRef,
      observedAt: java.time.Instant
  ): RemoteScriptExitRead =
    val path = epochDirectory(ref.attemptId, ref.attemptEpoch).resolve("exit-status")
    if !Files.isRegularFile(path) then RemoteScriptExitRead.Pending(observedAt)
    else
      val capture = readBounded(path, ByteLimit.defaultEvidence)
      val text = new String(capture._1.toArray, StandardCharsets.US_ASCII).trim
      text.toIntOption match
        case Some(exitCode) if exitCode >= 0 && exitCode <= 255 && capture._2 <= 16L =>
          RemoteScriptExitRead.Exited(exitCode, observedAt)
        case _ =>
          scriptExitFailure(
            "remote-script-exit-invalid",
            "the atomic exit-status artifact is malformed",
            observedAt,
            capture._1,
            capture._2
          )

  private def readBounded(path: Path, maximum: ByteLimit): (Vector[Byte], Long) =
    val sizeBefore = Files.size(path)
    val input = Files.newInputStream(path, StandardOpenOption.READ)
    val output = ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try
      var done = false
      while !done && output.size() < maximum.value do
        val requested = math.min(buffer.length, maximum.value - output.size())
        val count = input.read(buffer, 0, requested)
        if count < 0 then done = true
        else output.write(buffer, 0, count)
      val retained = output.toByteArray.toVector
      val sizeAfter = Files.size(path)
      retained -> math.max(math.max(sizeBefore, sizeAfter), retained.size.toLong)
    finally
      input.close()
      output.close()

  private def remoteReadFailure(
      code: String,
      detail: String,
      observedAt: java.time.Instant,
      retained: Vector[Byte],
      originalBytes: Long
  ): RemoteResultRead =
    val evidence = BoundedEvidence.fromCapture(
      EvidenceSource.ResultEnvelope,
      observedAt,
      retained,
      originalBytes
    )
    RemoteResultRead.Failed(
      Diagnostics.one(
        Diagnostic(
          code,
          "the remote result could not be read safely",
          Map("detail" -> detail.take(512))
        )
      ),
      EvidenceBundle(evidence),
      observedAt
    )

  private def scriptExitFailure(
      code: String,
      detail: String,
      observedAt: java.time.Instant,
      retained: Vector[Byte],
      originalBytes: Long
  ): RemoteScriptExitRead =
    val evidence = BoundedEvidence.fromCapture(
      EvidenceSource.ResultEnvelope,
      observedAt,
      retained,
      originalBytes
    )
    RemoteScriptExitRead.Failed(
      Diagnostics.one(
        Diagnostic(
          code,
          "the remote script exit status could not be read safely",
          Map("detail" -> detail.take(512))
        )
      ),
      EvidenceBundle(evidence),
      observedAt
    )

  private def epochDirectory(attemptId: AttemptId, epoch: AttemptEpoch): Path =
    settings.workspace.toAbsolutePath.normalize().resolve(s"${attemptId.value}-e${epoch.value}")

  private def validateExecutable(): Either[Diagnostics, Unit] =
    val executable = settings.executable.toAbsolutePath.normalize()
    Either.cond(
      executable.isAbsolute && Files.isRegularFile(executable) && Files.isExecutable(executable),
      (),
      Diagnostics.one(
        Diagnostic(
          "worker-executable-unavailable",
          "the configured worker distribution is not an executable regular file"
        )
      )
    )

  private def validateRemoteBatch(
      request: RemoteRegisteredBatchRequest
  ): Either[Diagnostics, Unit] =
    val elementIndices = request.elements.toVector.map(_.index)
    val elementKeys = request.elements.toVector.map(_.submissionKey)
    val topologyIndices = request.topology.elementIndices.toVector
    val oversized = request.elements.toVector.collect {
      case element if element.inputBytes.size > settings.maximumInputBytes.value =>
        element.index
    }
    val failures = Vector(
      Option.when(elementIndices.distinct.size != elementIndices.size)(
        Diagnostic("batch-element-index-duplicate", "batch element indices must be unique")
      ),
      Option.when(elementKeys.distinct.size != elementKeys.size)(
        Diagnostic("batch-element-key-duplicate", "batch element submission keys must be unique")
      ),
      Option.when(elementIndices.toSet != topologyIndices.toSet)(
        Diagnostic(
          "batch-topology-mismatch",
          "batch elements do not match the execution topology"
        )
      ),
      Option.when(oversized.nonEmpty)(
        Diagnostic(
          "batch-input-too-large",
          "one or more encoded batch inputs exceed the target worker limit",
          Map("indices" -> oversized.map(_.value).sorted.mkString(","))
        )
      ),
      Option.when(request.maximumResultBytes.value > settings.maximumEnvelopeBytes.value)(
        Diagnostic(
          "batch-result-limit-too-large",
          "requested result bytes exceed the target envelope limit"
        )
      )
    ).flatten
    Diagnostics.fromVector(failures).fold(_ => Right(()), Left(_))

  private def validateRemoteScriptBatch(
      request: RemoteScriptBatchRequest
  ): Either[Diagnostics, Unit] =
    val indices = request.elements.toVector.map(_.index)
    val keys = request.elements.toVector.map(_.submissionKey)
    val topologyIndices = request.topology.elementIndices.toVector
    val failures = Vector(
      Option.when(indices.distinct.size != indices.size)(
        Diagnostic("script-batch-index-duplicate", "script batch indices must be unique")
      ),
      Option.when(keys.distinct.size != keys.size)(
        Diagnostic("script-batch-key-duplicate", "script batch submission keys must be unique")
      ),
      Option.when(indices.toSet != topologyIndices.toSet)(
        Diagnostic(
          "script-batch-topology-mismatch",
          "script batch elements do not match the execution topology"
        )
      ),
      request.program.source match
        case ScriptSource.StagedLocal(_) =>
          Some(
            Diagnostic(
              "remote-staged-local-source",
              "staged-local sources must be materialized by the client before transport"
            )
          )
        case ScriptSource.Inline(_, bytes) if bytes.size > settings.maximumInvocationBytes.value =>
          Some(
            Diagnostic(
              "remote-script-too-large",
              "inline script bytes exceed the target staging limit"
            )
          )
        case _ => None
    ).flatten
    Diagnostics.fromVector(failures).fold(_ => Right(()), Left(_))

  private def materializeRemoteScript(
      program: ScriptProgram,
      directory: Path
  ): Either[Diagnostics, Path] =
    program.source match
      case ScriptSource.Inline(_, bytes) =>
        val target = directory.resolve("program")
        writeStable(
          target,
          bytes,
          executable = program.invocation == ScriptInvocation.Direct
        ).as(target)
      case ScriptSource.ExistingRemote(raw) =>
        val path = Path.of(raw).toAbsolutePath.normalize()
        Either.cond(
          Files.isRegularFile(path) &&
            (program.invocation != ScriptInvocation.Direct || Files.isExecutable(path)),
          path,
          Diagnostics.one(
            Diagnostic(
              "remote-script-unavailable",
              "the existing remote script is not a usable regular file"
            )
          )
        )
      case ScriptSource.StagedLocal(_) =>
        Left(
          Diagnostics.one(
            Diagnostic(
              "remote-staged-local-source",
              "staged-local sources must be materialized before transport"
            )
          )
        )

  private def prepareRemoteScriptElement(
      element: RemoteScriptBatchElement,
      invocation: ScriptInvocation,
      program: Path,
      attemptEpoch: AttemptEpoch
  ): Either[Diagnostics, PreparedRemoteScriptBatchElement] =
    for
      attempt <- scriptAttempt(
        element.submissionKey,
        ScriptProgram(
          ScriptSource.ExistingRemote(program.toString),
          invocation
        )
      ).left.map(problem =>
        Diagnostics.one(Diagnostic("invalid-script-element-attempt-id", problem.reason))
      )
      directory = epochDirectory(attempt, attemptEpoch)
      _ <- createPrivateDirectory(directory)
      stdout = LogRef(
        attempt,
        attemptEpoch,
        LogStream.Stdout,
        directory.resolve("stdout.log").toString
      )
      stderr = LogRef(
        attempt,
        attemptEpoch,
        LogStream.Stderr,
        directory.resolve("stderr.log").toString
      )
      exitRef = RemoteScriptExitRef(attempt, attemptEpoch)
      launchScript = directory.resolve("launch-script.sh")
      _ <- writeStable(
        launchScript,
        remoteScriptLaunchBytes(
          invocation,
          program,
          element.arguments,
          stdout.locator,
          stderr.locator,
          directory.resolve("exit-status")
        ),
        executable = true
      )
    yield PreparedRemoteScriptBatchElement(
      element.index,
      exitRef,
      stdout,
      stderr,
      launchScript
    )

  private def scriptAttempt(
      key: SubmissionKey,
      program: ScriptProgram
  ): Either[ValidationFailure, AttemptId] =
    val sourceIdentity = program.source match
      case ScriptSource.Inline(name, bytes) =>
        s"inline\u0000$name\u0000${sha256(bytes)}"
      case ScriptSource.StagedLocal(path)    => s"staged-local\u0000$path"
      case ScriptSource.ExistingRemote(path) => s"existing-remote\u0000$path"
    val invocationIdentity = program.invocation match
      case ScriptInvocation.Direct      => "direct"
      case ScriptInvocation.Via(prefix) =>
        prefix.arguments.toVector.map(_.value).mkString("via\u0000", "\u0000", "")
    val identity =
      s"${key.value}\u0000$sourceIdentity\u0000$invocationIdentity"
    AttemptId.from(
      s"script-${sha256(identity.getBytes(StandardCharsets.UTF_8).toVector).take(32)}"
    )

  private def deterministicAttempt[I, O](
      key: SubmissionKey,
      operation: OperationRef[I, O]
  ): Either[ValidationFailure, AttemptId] =
    val identity = s"${key.value}\u0000${operation.id.value}\u0000${operation.version.value}"
    AttemptId.from(s"worker-${sha256(identity.getBytes(StandardCharsets.UTF_8).toVector).take(32)}")

  private def deterministicAttempt(
      key: SubmissionKey,
      operation: RegisteredOperation
  ): Either[ValidationFailure, AttemptId] =
    val identity = s"${key.value}\u0000${operation.id.value}\u0000${operation.version.value}"
    AttemptId.from(s"worker-${sha256(identity.getBytes(StandardCharsets.UTF_8).toVector).take(32)}")

  private def launchScriptBytes(
      invocation: Path,
      result: Path,
      events: Path
  ): Vector[Byte] =
    val command = Vector(
      settings.executable.toAbsolutePath.normalize().toString,
      "run",
      "--invocation",
      invocation.toString,
      "--result",
      result.toString,
      "--events",
      events.toString
    ).map(shellQuote).mkString(" ")
    s"#!/bin/sh\nexec $command\n".getBytes(StandardCharsets.UTF_8).toVector

  private def arrayLaunchScriptBytes[O](
      elements: Vector[(PreparedRegisteredSubmission[O], RegisteredTaskArrayElement[?])]
  ): Vector[Byte] =
    val cases = elements
      .map { case (prepared, element) =>
        val directory = prepared.invocationPath.getParent
        val command = Vector(
          settings.executable.toAbsolutePath.normalize().toString,
          "run",
          "--invocation",
          prepared.invocationPath.toString,
          "--result",
          prepared.resultPath.toString,
          "--events",
          prepared.eventPath.toString
        ).map(shellQuote).mkString(" ")
        val stdout = shellQuote(directory.resolve("stdout.log").toString)
        val stderr = shellQuote(directory.resolve("stderr.log").toString)
        s"  '${element.index.value}') exec $command >$stdout 2>$stderr ;;"
      }
      .mkString("\n")
    val script =
      s"""#!/bin/sh
         |set -eu
         |case "${'$'}{SLURM_ARRAY_TASK_ID-}" in
         |$cases
         |  *) exit 64 ;;
         |esac
         |""".stripMargin
    script.getBytes(StandardCharsets.UTF_8).toVector

  private def writeRemoteBatchScripts(
      directory: Path,
      launchScript: Path,
      topology: BatchTopology,
      prepared: NonEmptyVector[(ArrayIndex, PreparedRemoteRegisteredSubmission)]
  ): Either[Diagnostics, Unit] =
    val byIndex = prepared.toVector.toMap
    topology.execution match
      case BatchExecutionPlan.Independent(_) =>
        for
          branches <- topology.shards.toVector.traverse { shard =>
            shard.elements.toVector match
              case Vector(index) =>
                byIndex
                  .get(index)
                  .toRight(
                    Diagnostics.one(
                      Diagnostic(
                        "batch-element-missing",
                        s"no prepared element exists for index ${index.value}"
                      )
                    )
                  )
                  .map(element => shard.index -> remoteElementCommand(element))
              case _ =>
                Left(
                  Diagnostics.one(
                    Diagnostic(
                      "independent-shard-shape-invalid",
                      "independent execution requires exactly one element per shard"
                    )
                  )
                )
          }
          _ <- writeStable(
            launchScript,
            arrayDispatchScript(branches),
            executable = true
          )
        yield ()
      case BatchExecutionPlan.Sharded(_, slotsPerShard, _, _) =>
        for
          branches <- topology.shards.toVector.traverse { shard =>
            for
              elements <- shard.elements.toVector.traverse { index =>
                byIndex
                  .get(index)
                  .toRight(
                    Diagnostics.one(
                      Diagnostic(
                        "batch-element-missing",
                        s"no prepared element exists for index ${index.value}"
                      )
                    )
                  )
              }
              shardScript = directory.resolve(s"launch-shard-${shard.index.value}.sh")
              _ <- writeStable(
                shardScript,
                boundedShardScript(elements, slotsPerShard),
                executable = true
              )
            yield shard.index -> Vector(shardScript.toString).map(shellQuote).mkString(" ")
          }
          _ <- writeStable(
            launchScript,
            arrayDispatchScript(branches),
            executable = true
          )
        yield ()
      case BatchExecutionPlan.Gang(nodes, tasksPerNode) =>
        val ordered = prepared.toVector.sortBy(_._1).map(_._2)
        val dispatch = directory.resolve("launch-gang-rank.sh")
        for
          _ <- writeStable(dispatch, gangRankScript(ordered), executable = true)
          _ <- writeStable(
            launchScript,
            gangLaunchScript(dispatch, nodes, tasksPerNode),
            executable = true
          )
        yield ()

  private def writeRemoteScriptBatchScripts(
      directory: Path,
      launchScript: Path,
      topology: BatchTopology,
      prepared: NonEmptyVector[PreparedRemoteScriptBatchElement]
  ): Either[Diagnostics, Unit] =
    val byIndex = prepared.toVector.map(value => value.index -> value).toMap
    def command(index: ArrayIndex): Either[Diagnostics, String] =
      byIndex
        .get(index)
        .map(element => shellQuote(element.launchScript.toString))
        .toRight(
          Diagnostics.one(
            Diagnostic(
              "script-batch-element-missing",
              s"no prepared script element exists for index ${index.value}"
            )
          )
        )

    topology.execution match
      case BatchExecutionPlan.Independent(_) =>
        for
          branches <- topology.shards.toVector.traverse { shard =>
            shard.elements.toVector match
              case Vector(index) => command(index).map(shard.index -> _)
              case _             =>
                Left(
                  Diagnostics.one(
                    Diagnostic(
                      "script-independent-shape-invalid",
                      "independent execution requires exactly one element per shard"
                    )
                  )
                )
          }
          _ <- writeStable(launchScript, arrayDispatchScript(branches), executable = true)
        yield ()
      case BatchExecutionPlan.Sharded(_, slotsPerShard, _, _) =>
        for
          branches <- topology.shards.toVector.traverse { shard =>
            for
              commands <- shard.elements.toVector.traverse(command)
              shardScript = directory.resolve(s"launch-script-shard-${shard.index.value}.sh")
              _ <- writeStable(
                shardScript,
                boundedCommandScript(commands, slotsPerShard),
                executable = true
              )
            yield shard.index -> shellQuote(shardScript.toString)
          }
          _ <- writeStable(launchScript, arrayDispatchScript(branches), executable = true)
        yield ()
      case BatchExecutionPlan.Gang(nodes, tasksPerNode) =>
        val ordered = prepared.toVector.sortBy(_.index)
        val dispatch = directory.resolve("launch-script-gang-rank.sh")
        for
          _ <- writeStable(
            dispatch,
            gangCommandScript(
              ordered.map(element => shellQuote(element.launchScript.toString))
            ),
            executable = true
          )
          _ <- writeStable(
            launchScript,
            gangLaunchScript(dispatch, nodes, tasksPerNode),
            executable = true
          )
        yield ()

  private def remoteElementCommand(element: PreparedRemoteRegisteredSubmission): String =
    val command = Vector(
      settings.executable.toAbsolutePath.normalize().toString,
      "run",
      "--invocation",
      element.invocationPath.toString,
      "--result",
      element.resultPath.toString,
      "--events",
      element.eventPath.toString
    ).map(shellQuote).mkString(" ")
    val directory = element.invocationPath.getParent
    val stdout = shellQuote(directory.resolve("stdout.log").toString)
    val stderr = shellQuote(directory.resolve("stderr.log").toString)
    s"$command >$stdout 2>$stderr"

  private def arrayDispatchScript(branches: Vector[(ArrayIndex, String)]): Vector[Byte] =
    val cases = branches
      .sortBy(_._1)
      .map { case (index, command) =>
        s"  '${index.value}') exec $command ;;"
      }
      .mkString("\n")
    val script =
      s"""#!/bin/sh
         |set -eu
         |case "${'$'}{SLURM_ARRAY_TASK_ID-}" in
         |$cases
         |  *) exit 64 ;;
         |esac
         |""".stripMargin
    script.getBytes(StandardCharsets.UTF_8).toVector

  private def boundedShardScript(
      elements: Vector[PreparedRemoteRegisteredSubmission],
      slotsPerShard: PositiveInt
  ): Vector[Byte] =
    boundedCommandScript(elements.map(remoteElementCommand), slotsPerShard)

  private def boundedCommandScript(
      commands: Vector[String],
      slotsPerShard: PositiveInt
  ): Vector[Byte] =
    val launches = commands.map { command =>
      s"""($command) &
         |active=${'$'}((active + 1))
         |if [ "${'$'}active" -ge "${slotsPerShard.toInt}" ]; then
         |  wait_one
         |fi
         |""".stripMargin
    }.mkString
    s"""#!/bin/bash
       |set -u
       |if (( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 3) )); then
       |  echo 'slurm4s: bounded shards require Bash 4.3 or newer' >&2
       |  exit 69
       |fi
       |status=0
       |active=0
       |wait_one() {
       |  if ! wait -n; then
       |    status=1
       |  fi
       |  active=${'$'}((active - 1))
       |}
       |$launches
       |while [ "${'$'}active" -gt 0 ]; do
       |  wait_one
       |done
       |exit "${'$'}status"
       |""".stripMargin.getBytes(StandardCharsets.UTF_8).toVector

  private def gangRankScript(
      elements: Vector[PreparedRemoteRegisteredSubmission]
  ): Vector[Byte] =
    val cases = elements.zipWithIndex
      .map { case (element, rank) =>
        s"  '$rank') exec ${remoteElementCommand(element)} ;;"
      }
      .mkString("\n")
    val script =
      s"""#!/bin/sh
         |set -eu
         |case "${'$'}{SLURM_PROCID-}" in
         |$cases
         |  *) exit 64 ;;
         |esac
         |""".stripMargin
    script.getBytes(StandardCharsets.UTF_8).toVector

  private def gangCommandScript(commands: Vector[String]): Vector[Byte] =
    val cases = commands.zipWithIndex
      .map { case (command, rank) =>
        s"  '$rank') exec $command ;;"
      }
      .mkString("\n")
    s"""#!/bin/sh
       |set -eu
       |case "${'$'}{SLURM_PROCID-}" in
       |$cases
       |  *) exit 64 ;;
       |esac
       |""".stripMargin.getBytes(StandardCharsets.UTF_8).toVector

  private def gangLaunchScript(
      dispatch: Path,
      nodes: PositiveInt,
      tasksPerNode: PositiveInt
  ): Vector[Byte] =
    val tasks = nodes.toInt.toLong * tasksPerNode.toInt.toLong
    val command = Vector(
      "srun",
      s"--nodes=${nodes.toInt}",
      s"--ntasks=$tasks",
      s"--ntasks-per-node=${tasksPerNode.toInt}",
      "--exact",
      dispatch.toString
    ).map(shellQuote).mkString(" ")
    s"#!/bin/sh\nset -eu\nexec $command\n".getBytes(StandardCharsets.UTF_8).toVector

  private def remoteScriptLaunchBytes(
      invocation: ScriptInvocation,
      program: Path,
      arguments: Vector[Argument],
      stdout: String,
      stderr: String,
      exitStatus: Path
  ): Vector[Byte] =
    val prefix = invocation match
      case ScriptInvocation.Direct     => Vector(program.toString)
      case ScriptInvocation.Via(value) =>
        value.arguments.toVector.map(_.value) :+ program.toString
    val command = (prefix ++ arguments.map(_.value)).map(shellQuote).mkString(" ")
    val stdoutPath = shellQuote(stdout)
    val stderrPath = shellQuote(stderr)
    val exitPath = shellQuote(exitStatus.toString)
    // The element exit artifact has higher authority than aggregate Slurm accounting (ADR-0011),
    // so it is published on the same terms as `AtomicFiles`: a private temporary that cannot
    // clobber an existing file, contents forced to stable storage, then an atomic rename.
    //
    // The temporary name carries the shell PID because launch paths are deterministic in
    // (submission key, source, invocation): a requeued element reuses this exact script, and a
    // fixed `.tmp` sibling would let two live attempts write the same file. `set -C` closes the
    // remaining window. `umask` is set before anything opens a file so stdout and stderr are
    // created private too, rather than inheriting the site default.
    val temporaryName = shellQuote(s"${exitStatus.toString}.tmp.") + "\"$$\""
    s"""#!/bin/sh
       |umask 077
       |set +e
       |$command >$stdoutPath 2>$stderrPath
       |status=${'$'}?
       |temporary=$temporaryName
       |set -C
       |printf '%s\n' "${'$'}status" >"${'$'}temporary"
       |published=${'$'}?
       |set +C
       |if [ "${'$'}published" -eq 0 ]; then
       |  sync "${'$'}temporary" 2>/dev/null || sync 2>/dev/null || :
       |  mv -f "${'$'}temporary" $exitPath
       |fi
       |exit "${'$'}status"
       |""".stripMargin.getBytes(StandardCharsets.UTF_8).toVector

  private def shellQuote(value: String): String =
    s"'${value.replace("'", "'\"'\"'")}'"

  private def writeStable(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean
  ): Either[Diagnostics, Unit] =
    AtomicFiles.writeStableBlocking(target, bytes, executable).left.map {
      case AtomicFiles.WriteFailure.TargetConflict(_, detail) =>
        Diagnostics.one(Diagnostic("typed-launch-conflict", s"launch artifact conflict: $detail"))
      case AtomicFiles.WriteFailure.TargetExists(_) =>
        Diagnostics.one(
          Diagnostic("typed-launch-conflict", "a launch artifact appeared concurrently")
        )
      case AtomicFiles.WriteFailure.AtomicMoveUnavailable(_) =>
        Diagnostics.one(
          Diagnostic(
            "atomic-launch-staging-unavailable",
            "the worker workspace does not support atomic artifact publication"
          )
        )
      case AtomicFiles.WriteFailure.Io(detail) =>
        Diagnostics.one(Diagnostic("launch-staging-io", detail))
    }

  private def createPrivateDirectory(path: Path): Either[Diagnostics, Unit] =
    val root = settings.workspace.toAbsolutePath.normalize()
    Either.cond(
      path.toAbsolutePath.normalize().startsWith(root), {
        val _ = Files.createDirectories(root)
        setPermissions(root, executable = true)
        val _ = Files.createDirectories(path)
        setPermissions(path, executable = true)
      },
      Diagnostics.one(Diagnostic("worker-workspace-escape", "attempt path escapes workspace"))
    )

  private def setPermissions(path: Path, executable: Boolean): Unit =
    try
      val value = if executable then "rwx------" else "rw-------"
      val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(value))
    catch case _: UnsupportedOperationException => ()

  private def sha256(bytes: Vector[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def diagnostics(code: String, error: Throwable): Diagnostics =
    Diagnostics.one(
      Diagnostic(code, Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
    )

  private def arrayDiagnostics(failures: NonEmptyChain[ArrayPlanFailure]): Diagnostics =
    Diagnostics
      .fromVector(
        failures.toChain.toVector.map(failure => Diagnostic(failure.code, failure.message))
      )
      .toOption
      .get

final class RegisteredTaskSubmitter(
    launcher: RegisteredTaskLauncher,
    scheduler: Scheduler[IO]
):
  def submit[A](request: JobRequest[A]): IO[RegisteredSubmissionResult[A]] =
    launcher.prepare(request).flatMap {
      case Left(diagnostics) =>
        IO.pure(RegisteredSubmissionResult.PreparationFailed(diagnostics))
      case Right(prepared) =>
        scheduler
          .submitLowered(prepared.schedulerRequest)
          .map(result => RegisteredSubmissionResult.Submitted(prepared, result))
    }

final class RegisteredTaskArraySubmitter(
    launcher: RegisteredTaskLauncher,
    scheduler: Scheduler[IO]
):
  def submit[I, O](
      request: RegisteredTaskArrayRequest[I, O]
  ): IO[RegisteredArraySubmissionResult[O]] =
    launcher.prepareArray(request).flatMap {
      case Left(diagnostics) =>
        IO.pure(RegisteredArraySubmissionResult.PreparationFailed(diagnostics))
      case Right(prepared) =>
        scheduler
          .submitLowered(prepared.schedulerRequest)
          .map(result => RegisteredArraySubmissionResult.Submitted(prepared, result))
    }
