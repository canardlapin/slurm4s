package io.github.bbuchsbaum.slurm4s.worker

import io.github.bbuchsbaum.slurm4s.task.SlurmTaskCall
import io.github.bbuchsbaum.slurm4s.task.TaskDefinition

/** A task that can be both submitted and executed.
  *
  * The submittable half lives in
  * [[io.github.bbuchsbaum.slurm4s.task.TaskDefinition TaskDefinition]] so a client can name it
  * without depending on the worker runtime; this adds the execution half.
  */
trait SlurmTask[I, O] extends ScalaTask[I, O]:
  final def apply(input: I): SlurmTaskCall[I, O] =
    SlurmTaskCall.of(this: TaskDefinition[I, O], input)
