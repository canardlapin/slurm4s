package io.github.bbuchsbaum.slurm4s.task

import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*

/** What a client needs to know about a task in order to submit it.
  *
  * Deliberately excludes `run`. A remote client sends an operation and encoded input and reads a
  * typed result back; it never executes the task, and requiring the executable definition is what
  * forced the SSH client to compile against the worker runtime. `ScalaTask` extends this with the
  * execution half, which stays in the worker.
  */
trait TaskDefinition[I, O]:
  def operation: OperationRef[I, O]
  def inputCodec: InputCodec[I]
  def outputCodec: ResultCodec[O]
  def retrySafety: RetrySafety = RetrySafety.Unknown

final case class SlurmTaskCall[I, O] private[task] (
    task: TaskDefinition[I, O],
    input: I
):
  def request(
      submissionKey: SubmissionKey,
      name: JobName,
      resources: ResourceRequest,
      maximumResultBytes: ByteLimit,
      environment: Map[EnvName, String] = Map.empty
  ): JobRequest[O] =
    JobRequest(
      submissionKey = submissionKey,
      name = name,
      payload = Payload.RegisteredTask(
        task.operation,
        input,
        task.inputCodec,
        ResultContract.Structured(task.outputCodec, maximumResultBytes)
      ),
      resources = resources,
      environment = environment,
      retrySafety = task.retrySafety
    )

object SlurmTaskCall:
  def of[I, O](task: TaskDefinition[I, O], input: I): SlurmTaskCall[I, O] =
    SlurmTaskCall(task, input)

/** A typed collection of logical tasks awaiting an explicit execution plan. */
sealed trait SlurmBatch[I, O]:
  def grid: Grid[I]

object SlurmBatch:
  final case class Registered[I, O](
      grid: Grid[I],
      task: TaskDefinition[I, O]
  ) extends SlurmBatch[I, O]

  final case class Script[I](
      grid: Grid[I],
      program: ScriptProgram,
      arguments: ScriptArguments[I]
  ) extends SlurmBatch[I, NoResult]

  def registered[I, O](grid: Grid[I], task: TaskDefinition[I, O]): Registered[I, O] =
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
