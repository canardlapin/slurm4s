package io.github.bbuchsbaum.slurm4s.worker

import io.github.bbuchsbaum.slurm4s.core.*

final case class SlurmTaskCall[I, O] private[worker] (
    task: SlurmTask[I, O],
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

trait SlurmTask[I, O] extends ScalaTask[I, O]:
  final def apply(input: I): SlurmTaskCall[I, O] =
    SlurmTaskCall(this, input)
