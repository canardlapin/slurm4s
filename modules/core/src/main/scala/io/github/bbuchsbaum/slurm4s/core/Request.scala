package io.github.bbuchsbaum.slurm4s.core

import cats.data.ValidatedNec
import cats.syntax.all.*

enum MemoryRequest derives CanEqual:
  case PerNode(amount: Mebibytes)
  case PerCpu(amount: Mebibytes)
  case AllNodeMemory

/** A catchable signal Slurm may deliver before the requested wall-time deadline.
  *
  * Names rather than platform-specific numbers keep the request portable across controller and
  * worker hosts. `KILL` and `STOP` are deliberately absent because a task cannot use either as an
  * advance notice.
  */
enum TerminationNoticeSignal(val slurmName: String) derives CanEqual:
  case Hup extends TerminationNoticeSignal("HUP")
  case Int extends TerminationNoticeSignal("INT")
  case Quit extends TerminationNoticeSignal("QUIT")
  case Usr1 extends TerminationNoticeSignal("USR1")
  case Usr2 extends TerminationNoticeSignal("USR2")
  case Term extends TerminationNoticeSignal("TERM")
  case Xcpu extends TerminationNoticeSignal("XCPU")

/** Which Slurm execution surface receives the advance signal.
  *
  * `BatchShell` is Slurm's `B:` form. `JobSteps` is the unprefixed form: all job steps receive the
  * signal, but the batch shell does not.
  */
enum TerminationNoticeScope derives CanEqual:
  case BatchShell
  case JobSteps

object SignalLeadSeconds:
  opaque type Type = Int

  val Minimum: Int = 0
  val Maximum: Int = 65535

  def from(value: Int): Either[ValidationFailure, Type] =
    Either.cond(
      value >= Minimum && value <= Maximum,
      value,
      ValidationFailure(
        "terminationNoticeLeadSeconds",
        s"must be between $Minimum and $Maximum"
      )
    )

  def unsafeFrom(value: Int): Type =
    from(value).fold(problem => throw new IllegalArgumentException(problem.reason), identity)

  extension (value: Type) def toInt: Int = value
  given CanEqual[Type, Type] = CanEqual.derived
type SignalLeadSeconds = SignalLeadSeconds.Type

/** An advance warning request, not Slurm's terminal SIGTERM/SIGKILL sequence.
  *
  * Slurm may deliver the signal up to 60 seconds earlier than the requested lead because controller
  * event handling is not second-precise. Receiving this signal says only that the configured
  * deadline is approaching; it does not establish that a task drained, published a result, or will
  * survive the terminal sequence.
  */
final case class TerminationNotice(
    signal: TerminationNoticeSignal,
    scope: TerminationNoticeScope,
    leadSeconds: SignalLeadSeconds
) derives CanEqual

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
    retrySafety: RetrySafety = RetrySafety.Unknown,
    terminationNotice: Option[TerminationNotice] = None
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
    retrySafety: RetrySafety = RetrySafety.Unknown,
    terminationNotice: Option[TerminationNotice] = None
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
            request.retrySafety,
            request.terminationNotice
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
