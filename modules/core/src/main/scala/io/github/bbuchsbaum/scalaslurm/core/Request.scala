package io.github.bbuchsbaum.scalaslurm.core

import cats.data.ValidatedNec
import cats.syntax.all.*

enum MemoryRequest derives CanEqual:
  case PerNode(amount: Mebibytes)
  case PerCpu(amount: Mebibytes)
  case AllNodeMemory

final case class ResourceRequest(
    cpusPerTask: PositiveInt,
    tasks: PositiveInt,
    nodes: Option[PositiveInt],
    memory: Option[MemoryRequest],
    wallTime: Option[WallTimeMinutes]
) derives CanEqual

object ResourceRequest:
  def validate(
      cpusPerTask: Int,
      tasks: Int,
      nodes: Option[Int],
      memory: Option[MemoryRequest],
      wallTime: Option[WallTimeMinutes]
  ): ValidatedNec[ValidationFailure, ResourceRequest] =
    (
      PositiveInt.from("cpusPerTask", cpusPerTask).toValidatedNec,
      PositiveInt.from("tasks", tasks).toValidatedNec,
      nodes.traverse(PositiveInt.from("nodes", _).toValidatedNec)
    ).mapN(ResourceRequest(_, _, _, memory, wallTime))

final case class JobRequest[A](
    submissionKey: SubmissionKey,
    name: JobName,
    payload: Payload[A],
    resources: ResourceRequest,
    environment: Map[String, String] = Map.empty,
    array: Option[JobArrayRequest] = None
)

enum CapabilitySupport derives CanEqual:
  case Supported
  case Unsupported
  case Unknown(diagnostics: Diagnostics)

final case class SchedulerCapabilities(
    slurmVersion: Option[String],
    cluster: Option[ClusterName],
    structuredQueue: CapabilitySupport,
    structuredAccounting: CapabilitySupport,
    accounting: CapabilitySupport,
    arrays: CapabilitySupport,
    rawEvidence: Vector[BoundedEvidence]
) derives CanEqual
