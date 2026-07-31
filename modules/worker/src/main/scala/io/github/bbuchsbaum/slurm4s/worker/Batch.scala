package io.github.bbuchsbaum.slurm4s.worker

import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*

/** A typed collection of logical tasks awaiting an explicit execution plan. */
sealed trait SlurmBatch[I, O]:
  def grid: Grid[I]

object SlurmBatch:
  final case class Registered[I, O](
      grid: Grid[I],
      task: SlurmTask[I, O]
  ) extends SlurmBatch[I, O]

  final case class Script[I](
      grid: Grid[I],
      program: ScriptProgram,
      arguments: ScriptArguments[I]
  ) extends SlurmBatch[I, NoResult]

  def registered[I, O](grid: Grid[I], task: SlurmTask[I, O]): Registered[I, O] =
    Registered(grid, task)

  def script[I](
      grid: Grid[I],
      program: ScriptProgram
  )(using arguments: ScriptArguments[I]): Script[I] =
    Script(grid, program, arguments)

final case class SlurmBatchOptions(
    submissionKey: SubmissionKey,
    name: JobName,
    perTask: TaskResources,
    maximumResultBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath] = Vector.empty,
    environment: Map[EnvName, String] = Map.empty
) derives CanEqual

enum SlurmBatchCompileFailure derives CanEqual:
  case Plan(failures: cats.data.NonEmptyChain[BatchPlanFailure])
  case Arguments(index: ArrayIndex, failures: cats.data.NonEmptyChain[ValidationFailure])
  case Input(index: ArrayIndex, failure: ResultCodecFailure)
  case ElementKey(index: ArrayIndex, failure: ValidationFailure)
