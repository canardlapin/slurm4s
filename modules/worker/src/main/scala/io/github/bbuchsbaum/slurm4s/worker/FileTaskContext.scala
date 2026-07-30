package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

final case class FileTaskWorkspace(
    root: Path,
    maximumOutputCount: Int = 1024
)

object FileTaskContext:
  def inspectDeclaredOutputs(
      outputRoot: Path,
      expected: Vector[RelativeOutputPath],
      maximumBytesPerOutput: ByteLimit,
      maximumOutputCount: Int = 1024
  ): IO[Either[TaskIoFailure, OutputManifest]] =
    new FileOutputStaging(outputRoot.toAbsolutePath.normalize(), maximumOutputCount)
      .seal(expected, maximumBytesPerOutput)

  def managed(
      workspace: FileTaskWorkspace,
      declaredInputs: Map[InputName, Path],
      reportProgress: ProgressEvent => IO[Unit] = _ => IO.unit,
      taskLogger: TaskLogger[IO] = TaskLogger.noop
  ): Resource[IO, TaskContext[IO]] =
    Resource.eval(IO.blocking(initialize(workspace))).map { roots =>
      new TaskContext[IO]:
        val assurance: TaskContextAssurance = TaskContextAssurance.ManagedCapabilities
        val inputs: InputAccess[IO] = new FileInputAccess(declaredInputs)
        val outputs: OutputStaging[IO] =
          new FileOutputStaging(roots.output, workspace.maximumOutputCount)
        val scratch: Resource[IO, ScratchDirectory] = scratchResource(roots.scratch)
        def progress(event: ProgressEvent): IO[Unit] = reportProgress(event)
        val logger: TaskLogger[IO] = taskLogger
    }

  def native(
      workspace: FileTaskWorkspace,
      declaredInputs: Map[InputName, Path],
      reportProgress: ProgressEvent => IO[Unit] = _ => IO.unit,
      taskLogger: TaskLogger[IO] = TaskLogger.noop
  ): Resource[IO, NativeTaskContext[IO]] =
    Resource.eval(IO.blocking(initialize(workspace))).map { roots =>
      new NativeTaskContext[IO]:
        val nativeRoot: Path = roots.root
        val inputs: InputAccess[IO] = new FileInputAccess(declaredInputs)
        val outputs: OutputStaging[IO] =
          new FileOutputStaging(roots.output, workspace.maximumOutputCount)
        val scratch: Resource[IO, ScratchDirectory] = scratchResource(roots.scratch)
        def progress(event: ProgressEvent): IO[Unit] = reportProgress(event)
        val logger: TaskLogger[IO] = taskLogger
    }

  final private case class Roots(root: Path, output: Path, scratch: Path)

  private def initialize(workspace: FileTaskWorkspace): Roots =
    require(workspace.maximumOutputCount > 0, "maximum output count must be positive")
    val root = workspace.root.toAbsolutePath.normalize()
    val output = root.resolve("outputs")
    val scratch = root.resolve("scratch")
    createPrivateDirectory(root)
    createPrivateDirectory(output)
    createPrivateDirectory(scratch)
    Roots(root, output, scratch)

  private def scratchResource(root: Path): Resource[IO, ScratchDirectory] =
    Resource.make(
      IO.blocking {
        val path = Files.createDirectory(root.resolve(s"task-${UUID.randomUUID()}"))
        setPrivate(path)
        ScratchDirectory(path)
      }
    )(directory => IO.blocking(deleteCreatedTree(root, directory.path)).handleError(_ => ()))

  final private class FileInputAccess(declared: Map[InputName, Path]) extends InputAccess[IO]:
    def read(
        name: InputName,
        maximumBytes: ByteLimit
    ): IO[Either[TaskIoFailure, Vector[Byte]]] =
      declared.get(name) match
        case None       => IO.pure(Left(TaskIoFailure.InputNotDeclared(name)))
        case Some(path) =>
          IO.blocking(readBounded(path, maximumBytes)).attempt.map {
            case Right(Right(bytes)) => Right(bytes)
            case Right(Left(error))  =>
              Left(TaskIoFailure.InputUnavailable(name, safeMessage(error)))
            case Left(error) =>
              Left(TaskIoFailure.InputUnavailable(name, safeMessage(error)))
          }

  final private class FileOutputStaging(root: Path, maximumOutputCount: Int)
      extends OutputStaging[IO]:
    require(
      maximumOutputCount > 0 && maximumOutputCount <= 4096,
      "maximum output count must be between 1 and 4096"
    )

    def write(
        path: RelativeOutputPath,
        bytes: Vector[Byte],
        maximumBytes: ByteLimit
    ): IO[Either[TaskIoFailure, OutputEntry]] =
      if bytes.size > maximumBytes.value then
        IO.pure(
          Left(TaskIoFailure.OutputTooLarge(path, bytes.size.toLong, maximumBytes.value))
        )
      else
        IO.blocking {
          val target = resolveWithin(root, path)
          Option(target.getParent).foreach(createPrivateDirectory)
          AtomicFiles.writeNewBlocking(target, bytes) match
            case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(_)) =>
              Left(
                TaskIoFailure.OutputUnavailable(
                  path,
                  "workspace does not support atomic output publication"
                )
              )
            case Left(failure) =>
              Left(TaskIoFailure.OutputUnavailable(path, failure.toString))
            case Right(()) =>
              OutputEntry
                .from(path, bytes.size.toLong, digest(bytes))
                .left
                .map(problem => TaskIoFailure.OutputUnavailable(path, problem.reason))
        }.handleError(error => Left(TaskIoFailure.OutputUnavailable(path, safeMessage(error))))

    def seal(
        expected: Vector[RelativeOutputPath],
        maximumBytesPerOutput: ByteLimit
    ): IO[Either[TaskIoFailure, OutputManifest]] =
      IO.blocking {
        val paths = listRegularFilesBounded(root, maximumOutputCount)
        paths
          .traverse { file =>
            val relativeText =
              root.relativize(file).iterator().asScala.map(_.toString).mkString("/")
            for
              relative <- RelativeOutputPath
                .from(relativeText)
                .left
                .map(problem => TaskIoFailure.OutputUnavailable(fallbackOutput, problem.reason))
              bytes <- readBounded(file, maximumBytesPerOutput).left
                .map(error => TaskIoFailure.OutputUnavailable(relative, safeMessage(error)))
              entry <- OutputEntry
                .from(relative, bytes.size.toLong, digest(bytes))
                .left
                .map(problem => TaskIoFailure.OutputUnavailable(relative, problem.reason))
            yield entry
          }
          .flatMap { observed =>
            OutputValidation
              .verify(expected, observed, observed)
              .left
              .map(failures => TaskIoFailure.OutputValidation(failures.toVector))
          }
      }.handleError {
        case _: TooManyFiles => Left(TaskIoFailure.TooManyOutputs(maximumOutputCount))
        case error           =>
          Left(TaskIoFailure.OutputUnavailable(fallbackOutput, safeMessage(error)))
      }

  final private class TooManyFiles extends RuntimeException

  private val fallbackOutput: RelativeOutputPath =
    RelativeOutputPath.unsafeFrom("unknown-output")

  private def listRegularFilesBounded(root: Path, maximum: Int): Vector[Path] =
    val stream = Files.walk(root)
    try
      val iterator = stream.iterator()
      val builder = Vector.newBuilder[Path]
      var count = 0
      var visited = 0
      val maximumVisited = maximum.toLong * 16L + 32L
      while iterator.hasNext do
        visited += 1
        if visited.toLong > maximumVisited then throw new TooManyFiles
        val path = iterator.next()
        if Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) then
          builder += path
          count += 1
          if count > maximum then throw new TooManyFiles
      builder.result()
    finally stream.close()

  private def readBounded(
      path: Path,
      maximum: ByteLimit
  ): Either[Throwable, Vector[Byte]] =
    val input = Files.newInputStream(path, StandardOpenOption.READ)
    val output = ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try
      var total = 0L
      var done = false
      while !done && total <= maximum.value.toLong do
        val requested = math.min(buffer.length.toLong, maximum.value.toLong - total + 1L).toInt
        val count = input.read(buffer, 0, requested)
        if count < 0 then done = true
        else
          output.write(buffer, 0, count)
          total += count.toLong
      Either.cond(
        total <= maximum.value.toLong,
        output.toByteArray.toVector,
        new IllegalArgumentException(s"file exceeds ${maximum.value} bytes")
      )
    catch case NonFatal(error) => Left(error)
    finally
      input.close()
      output.close()

  private def resolveWithin(root: Path, relative: RelativeOutputPath): Path =
    val target = root.resolve(relative.value).normalize()
    if !target.startsWith(root) then throw new IllegalArgumentException("output escapes workspace")
    var current = target.getParent
    while current != null && current.startsWith(root) do
      if Files.exists(current, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(current) then
        throw new IllegalArgumentException("output parent must not be a symbolic link")
      current = current.getParent
    target

  private def createPrivateDirectory(path: Path): Unit =
    Files.createDirectories(path)
    setPrivate(path)

  private def setPrivate(path: Path): Unit =
    try
      val permissions =
        if Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) then "rwx------" else "rw-------"
      val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(permissions))
    catch case _: UnsupportedOperationException => ()

  private def deleteCreatedTree(root: Path, candidate: Path): Unit =
    val safeRoot = root.toAbsolutePath.normalize()
    val safeCandidate = candidate.toAbsolutePath.normalize()
    if !safeCandidate.getParent.equals(safeRoot) then
      throw new IllegalArgumentException("scratch cleanup target is outside the scratch root")
    val stream = Files.walk(safeCandidate)
    try
      stream
        .sorted(java.util.Comparator.reverseOrder())
        .iterator()
        .asScala
        .foreach { path =>
          val _ = Files.deleteIfExists(path)
        }
    finally stream.close()

  private def digest(bytes: Vector[Byte]): ContentDigest =
    val hex = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    ContentDigest.unsafeFrom(s"sha256:$hex")

  private def safeMessage(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)
