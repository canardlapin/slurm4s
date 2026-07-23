package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.Async
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.cli.PreparedSubmission
import io.github.bbuchsbaum.scalaslurm.cli.SubmissionPlanner
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.charset.StandardCharsets
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import scala.util.control.NonFatal

final case class LocalWorkspaceSettings(root: Path, maxScriptBytes: ByteLimit)

final case class PreparedLocalSubmission[A](
    submission: PreparedSubmission[A],
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    stdout: LogRef,
    stderr: LogRef
)

final class LocalSubmissionPlanner[F[_]: Async](settings: LocalWorkspaceSettings)
    extends SubmissionPlanner[F]:

  def prepare[A](request: JobRequest[A]): F[Either[Diagnostics, PreparedSubmission[A]]] =
    prepareLocal(request).map(_.map(_.submission))

  def prepareLocal[A](request: JobRequest[A]): F[Either[Diagnostics, PreparedLocalSubmission[A]]] =
    Async[F].blocking(prepareBlocking(request)).attempt.map {
      case Right(value) => value
      case Left(error)  => Left(preparationFailure(error))
    }

  private def prepareBlocking[A](
      request: JobRequest[A]
  ): Either[Diagnostics, PreparedLocalSubmission[A]] =
    request.payload match
      case script: Payload.Script[A] =>
        for
          attemptId <- AttemptId
            .from(
              s"local-${digest(request.submissionKey.value.getBytes(StandardCharsets.UTF_8)).take(24)}"
            )
            .left
            .map(validationFailure)
          directory = settings.root.resolve(attemptId.value).normalize()
          _ <- ensureContained(directory)
          _ <- createPrivateDirectory(directory)
          scriptPath <- materialize(script.source, directory)
          stdoutPath = directory.resolve(
            if request.array.nonEmpty then "stdout-%A_%a.log" else "stdout.log"
          )
          stderrPath = directory.resolve(
            if request.array.nonEmpty then "stderr-%A_%a.log" else "stderr.log"
          )
          prepared = PreparedSubmission(
            request = request,
            scriptPath = scriptPath.toString,
            stdoutPath = stdoutPath.toString,
            stderrPath = stderrPath.toString
          )
        yield PreparedLocalSubmission(
          submission = prepared,
          attemptId = attemptId,
          epoch = AttemptEpoch.initial,
          stdout = LogRef(attemptId, AttemptEpoch.initial, LogStream.Stdout, stdoutPath.toString),
          stderr = LogRef(attemptId, AttemptEpoch.initial, LogStream.Stderr, stderrPath.toString)
        )
      case _: Payload.RegisteredTask[?, ?] =>
        Left(
          Diagnostics.one(
            Diagnostic(
              "registered-task-requires-lowering",
              "lower the task with a target-side RegisteredTaskLauncher before local submission"
            )
          )
        )

  private def materialize(source: ScriptSource, directory: Path): Either[Diagnostics, Path] =
    source match
      case ScriptSource.Inline(_, bytes)  => materializeBytes(bytes, directory)
      case ScriptSource.StagedLocal(path) =>
        val sourcePath = Path.of(path)
        if !Files.isRegularFile(sourcePath) then
          Left(
            Diagnostics.one(
              Diagnostic("script-not-found", "the staged local script is not a regular file")
            )
          )
        else readBounded(sourcePath).flatMap(materializeBytes(_, directory))
      case ScriptSource.ExistingRemote(path) =>
        val remote = Path.of(path)
        if remote.toString.isEmpty then
          Left(
            Diagnostics.one(
              Diagnostic("remote-script-path-empty", "the existing remote script path is empty")
            )
          )
        else if !Files.isRegularFile(remote) then
          Left(
            Diagnostics.one(
              Diagnostic(
                "remote-script-not-found",
                "the existing remote script is not a regular file"
              )
            )
          )
        else Right(remote)

  private def materializeBytes(bytes: Vector[Byte], directory: Path): Either[Diagnostics, Path] =
    if bytes.size > settings.maxScriptBytes.value then
      Left(
        Diagnostics.one(
          Diagnostic("script-too-large", "the inline script exceeds the configured byte limit")
        )
      )
    else
      val target = directory.resolve(s"script-${digest(bytes.toArray).take(24)}.payload")
      if Files.exists(target) then
        readBounded(target).flatMap { existing =>
          if existing == bytes then Right(target)
          else
            Left(
              Diagnostics.one(
                Diagnostic(
                  "script-digest-collision",
                  "an existing staged script has different bytes"
                )
              )
            )
        }
      else
        val privateFile = PosixFilePermissions.asFileAttribute(
          PosixFilePermissions.fromString("rwx------")
        )
        Files.createFile(target, privateFile)
        Files.write(target, bytes.toArray, StandardOpenOption.WRITE)
        Right(target)

  private def readBounded(path: Path): Either[Diagnostics, Vector[Byte]] =
    val input = Files.newInputStream(path, StandardOpenOption.READ)
    val output = ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    val limit = settings.maxScriptBytes.value.toLong
    try
      var total = 0L
      var done = false
      while !done && total <= limit do
        val request = math.min(buffer.length.toLong, limit - total + 1L).toInt
        val count = input.read(buffer, 0, request)
        if count < 0 then done = true
        else
          output.write(buffer, 0, count)
          total += count.toLong
      if total > limit then
        Left(
          Diagnostics.one(
            Diagnostic("script-too-large", "the staged script exceeds the configured byte limit")
          )
        )
      else Right(output.toByteArray.toVector)
    finally
      input.close()
      output.close()

  private def createPrivateDirectory(directory: Path): Either[Diagnostics, Unit] =
    val privateDirectory = PosixFilePermissions.asFileAttribute(
      PosixFilePermissions.fromString("rwx------")
    )
    Files.createDirectories(settings.root, privateDirectory)
    Files.setPosixFilePermissions(settings.root, PosixFilePermissions.fromString("rwx------"))
    Files.createDirectories(directory, privateDirectory)
    Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"))
    Right(())

  private def ensureContained(directory: Path): Either[Diagnostics, Unit] =
    val root = settings.root.toAbsolutePath.normalize()
    val candidate = directory.toAbsolutePath.normalize()
    Either.cond(
      candidate.startsWith(root) && candidate != root,
      (),
      Diagnostics.one(
        Diagnostic("workspace-escape", "derived attempt directory escapes the workspace root")
      )
    )

  private def digest(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(byte => f"${byte & 0xff}%02x").mkString

  private def validationFailure(problem: ValidationFailure): Diagnostics =
    Diagnostics.one(Diagnostic("invalid-attempt-id", problem.reason, Map("field" -> problem.field)))

  private def preparationFailure(error: Throwable): Diagnostics =
    val kind = error match
      case _: UnsupportedOperationException => "posix-permissions-unavailable"
      case _: SecurityException             => "workspace-permission-denied"
      case NonFatal(_)                      => "workspace-io-failed"
    Diagnostics.one(
      Diagnostic(
        kind,
        "the local private launch workspace could not be prepared",
        Map("cause" -> error.getClass.getSimpleName)
      )
    )
