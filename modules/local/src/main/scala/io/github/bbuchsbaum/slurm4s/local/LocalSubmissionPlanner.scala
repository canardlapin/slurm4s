package io.github.bbuchsbaum.slurm4s.local

import cats.effect.Async
import cats.syntax.all.*
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.slurm4s.cli.PreparedSubmission
import io.github.bbuchsbaum.slurm4s.cli.SubmissionPlanner
import io.github.bbuchsbaum.slurm4s.core.*

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import scala.util.control.NonFatal

final case class LocalWorkspaceSettings(root: Path, maxScriptBytes: ByteLimit)

final case class PreparedLocalSubmission(
    submission: PreparedSubmission,
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    stdout: LogRef,
    stderr: LogRef
)

final class LocalSubmissionPlanner[F[_]: Async](settings: LocalWorkspaceSettings)
    extends SubmissionPlanner[F]:

  def prepare(spec: LaunchSpec): F[Either[Diagnostics, PreparedSubmission]] =
    prepareLocal(spec).map(_.map(_.submission))

  /** Stage one attempt of `request` at `epoch`.
    *
    * The attempt identity is stable across epochs -- that is what an epoch is for -- but its
    * private directory is not. Every other attempt-scoped artifact in the library is epoch fenced,
    * and logs must be too: without the epoch in the path, resubmitting the same submission key
    * overwrites the previous attempt's stdout and stderr, destroying the evidence needed to explain
    * why that attempt failed.
    */
  def prepareLocal(
      spec: LaunchSpec,
      epoch: AttemptEpoch = AttemptEpoch.initial
  ): F[Either[Diagnostics, PreparedLocalSubmission]] =
    Async[F].blocking(prepareBlocking(spec, epoch)).attempt.map {
      case Right(value) => value
      case Left(error)  => Left(preparationFailure(error))
    }

  private def prepareBlocking(
      spec: LaunchSpec,
      epoch: AttemptEpoch
  ): Either[Diagnostics, PreparedLocalSubmission] =
    for
      attemptId <- AttemptId
        .from(
          s"local-${digest(spec.submissionKey.value.getBytes(StandardCharsets.UTF_8)).take(24)}"
        )
        .left
        .map(validationFailure)
      // Mirrors the worker's `${attemptId}-e${epoch}` result layout.
      directory = settings.root.resolve(s"${attemptId.value}-e${epoch.value}").normalize()
      _ <- ensureContained(directory)
      _ <- createPrivateDirectory(directory)
      scriptPath <- materialize(spec.source, directory)
      stdoutPath = directory.resolve(
        if spec.array.nonEmpty then "stdout-%A_%a.log" else "stdout.log"
      )
      stderrPath = directory.resolve(
        if spec.array.nonEmpty then "stderr-%A_%a.log" else "stderr.log"
      )
      prepared = PreparedSubmission(
        spec = spec,
        scriptPath = scriptPath.toString,
        stdoutPath = stdoutPath.toString,
        stderrPath = stderrPath.toString
      )
    yield PreparedLocalSubmission(
      submission = prepared,
      attemptId = attemptId,
      epoch = epoch,
      stdout = LogRef(attemptId, epoch, LogStream.Stdout, stdoutPath.toString),
      stderr = LogRef(attemptId, epoch, LogStream.Stderr, stderrPath.toString)
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
      AtomicFiles.writeStableBlocking(target, bytes, executable = true) match
        case Right(())                                           => Right(target)
        case Left(AtomicFiles.WriteFailure.TargetConflict(_, _)) =>
          Left(
            Diagnostics.one(
              Diagnostic(
                "script-digest-collision",
                "an existing staged script has different bytes or is not a regular file"
              )
            )
          )
        case Left(AtomicFiles.WriteFailure.TargetExists(_)) =>
          Left(
            Diagnostics.one(
              Diagnostic(
                "script-staging-race",
                "a conflicting staged script appeared during atomic publication"
              )
            )
          )
        case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(_)) =>
          Left(
            Diagnostics.one(
              Diagnostic(
                "atomic-script-staging-unavailable",
                "the local workspace does not support atomic script publication"
              )
            )
          )
        case Left(AtomicFiles.WriteFailure.Io(detail)) =>
          Left(
            Diagnostics.one(
              Diagnostic(
                "script-staging-io",
                "the script could not be staged atomically",
                Map("detail" -> detail)
              )
            )
          )

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
      candidate.startsWith(root) && !candidate.equals(root),
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
