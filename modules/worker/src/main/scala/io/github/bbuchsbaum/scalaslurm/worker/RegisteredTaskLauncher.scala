package io.github.bbuchsbaum.scalaslurm.worker

import cats.data.NonEmptyChain
import cats.data.NonEmptyVector
import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.TaskInvocationCodec

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.UUID

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
    resultPath: Path,
    eventPath: Path,
    launchScript: Path,
    resultHandle: DurableResultHandle
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
    environment: Map[String, String],
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
      handle = DurableResultHandle(
        request.submissionKey,
        attemptId,
        attemptEpoch,
        None,
        WorkloadOperation.Registered(task.operation.id, task.operation.version),
        task.operation.outputSchema,
        invocation.maximumResultBytes,
        settings.maximumEnvelopeBytes,
        invocation.declaredOutputs,
        settings.workerRelease,
        request.retrySafety
      )
    yield PreparedRegisteredSubmission(
      schedulerRequest,
      invocation,
      invocationPath,
      resultPath,
      eventPath,
      launchScript,
      handle
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

  private def deterministicAttempt[I, O](
      key: SubmissionKey,
      operation: OperationRef[I, O]
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

  private def shellQuote(value: String): String =
    s"'${value.replace("'", "'\"'\"'")}'"

  private def writeStable(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean
  ): Either[Diagnostics, Unit] =
    if Files.exists(target, LinkOption.NOFOLLOW_LINKS) then
      if !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
        Left(
          Diagnostics.one(
            Diagnostic("typed-launch-conflict", "an existing launch artifact is not a regular file")
          )
        )
      else
        val existing = readAtMost(target, bytes.size)
        Either.cond(
          existing == bytes,
          (),
          Diagnostics.one(
            Diagnostic(
              "typed-launch-conflict",
              "an existing launch artifact has different bytes"
            )
          )
        )
    else
      val temporary = target.resolveSibling(s".${target.getFileName}.tmp-${UUID.randomUUID()}")
      try
        Files.write(
          temporary,
          bytes.toArray,
          StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE
        )
        setPermissions(temporary, executable)
        try
          val _ = Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE)
          Right(())
        catch
          case _: AtomicMoveNotSupportedException =>
            Left(
              Diagnostics.one(
                Diagnostic(
                  "atomic-launch-staging-unavailable",
                  "the worker workspace does not support atomic artifact publication"
                )
              )
            )
      finally
        val _ = Files.deleteIfExists(temporary)

  private def readAtMost(path: Path, maximum: Int): Vector[Byte] =
    val input = Files.newInputStream(path, StandardOpenOption.READ)
    val output = ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try
      var total = 0L
      var done = false
      while !done && total <= maximum.toLong do
        val requested =
          math.min(buffer.length.toLong, maximum.toLong - total + 1L).toInt
        val count = input.read(buffer, 0, requested)
        if count < 0 then done = true
        else
          output.write(buffer, 0, count)
          total += count.toLong
      output.toByteArray.toVector
    finally
      input.close()
      output.close()

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
          .submit(prepared.schedulerRequest)
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
          .submit(prepared.schedulerRequest)
          .map(result => RegisteredArraySubmissionResult.Submitted(prepared, result))
    }
