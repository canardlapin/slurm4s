package io.github.bbuchsbaum.scalaslurm.worker

import cats.effect.Resource
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.file.Path

enum TaskContextAssurance derives CanEqual:
  case ManagedCapabilities
  case NativeFilesystem

enum TaskIoFailure derives CanEqual:
  case InputNotDeclared(name: InputName)
  case InputUnavailable(name: InputName, message: String)
  case OutputTooLarge(path: RelativeOutputPath, actualBytes: Long, maximumBytes: Int)
  case OutputUnavailable(path: RelativeOutputPath, message: String)
  case TooManyOutputs(maximum: Int)
  case OutputValidation(failures: Vector[OutputValidationFailure])

trait InputAccess[F[_]]:
  def read(name: InputName, maximumBytes: ByteLimit): F[Either[TaskIoFailure, Vector[Byte]]]

trait OutputStaging[F[_]]:
  def write(
      path: RelativeOutputPath,
      bytes: Vector[Byte],
      maximumBytes: ByteLimit
  ): F[Either[TaskIoFailure, OutputEntry]]

  def seal(
      expected: Vector[RelativeOutputPath],
      maximumBytesPerOutput: ByteLimit
  ): F[Either[TaskIoFailure, OutputManifest]]

final case class ScratchDirectory(path: Path)

trait TaskLogger[F[_]]:
  def info(message: String, fields: Map[String, String] = Map.empty): F[Unit]
  def warn(message: String, fields: Map[String, String] = Map.empty): F[Unit]

object TaskLogger:
  val noop: TaskLogger[cats.effect.IO] = new TaskLogger[cats.effect.IO]:
    def info(message: String, fields: Map[String, String]): cats.effect.IO[Unit] =
      cats.effect.IO.unit
    def warn(message: String, fields: Map[String, String]): cats.effect.IO[Unit] =
      cats.effect.IO.unit

trait TaskContext[F[_]]:
  def assurance: TaskContextAssurance
  def inputs: InputAccess[F]
  def outputs: OutputStaging[F]
  def scratch: Resource[F, ScratchDirectory]
  def progress(event: ProgressEvent): F[Unit]
  def logger: TaskLogger[F]

trait NativeTaskContext[F[_]] extends TaskContext[F]:
  def nativeRoot: Path
  final def assurance: TaskContextAssurance = TaskContextAssurance.NativeFilesystem
