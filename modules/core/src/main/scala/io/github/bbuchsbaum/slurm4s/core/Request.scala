package io.github.bbuchsbaum.slurm4s.core

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
    environment: Map[EnvName, String] = Map.empty,
    array: Option[JobArrayRequest] = None,
    retrySafety: RetrySafety = RetrySafety.Unknown
)

/** Everything a scheduler needs in order to launch, and nothing about the result type.
  *
  * `Scheduler.submit` previously accepted `JobRequest[A]` and returned a type-erased
  * `SubmissionAttempt`, so `A` was phantom at the interface — a promise no backend could keep.
  * `DurableResultHandle` carries no type parameter, and typed results are recovered by matching
  * `ResultSchemaId` at runtime, so erasing `A` here removes a promise rather than a capability.
  *
  * A launch specification is script-shaped because that is what a scheduler launches. A registered
  * task is lowered to a script by the task layer first; that lowering and its typed result plan
  * live above this boundary.
  *
  * The result contract travels as a descriptor rather than being dropped. The scheduler does not
  * interpret it, but the worker and the result reader do, so erasing the type must not mean
  * destroying the contract — which is exactly what the previous SSH lowering did.
  */
final case class LaunchSpec(
    submissionKey: SubmissionKey,
    name: JobName,
    source: ScriptSource,
    arguments: Vector[String],
    resultContract: ResultContractDescriptor,
    resources: ResourceRequest,
    environment: Map[EnvName, String] = Map.empty,
    array: Option[JobArrayRequest] = None,
    retrySafety: RetrySafety = RetrySafety.Unknown
)

object LaunchSpec:

  /** Lower a script request. A registered task is not lowerable here by design: only the task layer
    * knows the worker invocation, so it must render the script itself.
    */
  def fromRequest[A](request: JobRequest[A]): Either[Diagnostics, LaunchSpec] =
    request.payload match
      case Payload.Script(source, arguments, contract) =>
        Right(
          LaunchSpec(
            request.submissionKey,
            request.name,
            source,
            arguments,
            contract.descriptor,
            request.resources,
            request.environment,
            request.array,
            request.retrySafety
          )
        )
      case _: Payload.RegisteredTask[?, ?] =>
        Left(
          Diagnostics.one(
            Diagnostic(
              "launch-spec-not-lowerable",
              "a registered task must be rendered to a script before it reaches a scheduler"
            )
          )
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
