package io.github.bbuchsbaum.slurm4s.core

import cats.data.NonEmptyChain
import cats.data.ValidatedNec
import cats.syntax.all.*
import io.github.bbuchsbaum.remoteexec.kernel.TextIdentifier

/** Shared validation for a site-supplied name.
  *
  * Each kind gets its own type. One `SiteToken` covering accounts, partitions, QoS, constraints,
  * modules, containers and accelerator kinds made every one of them freely interchangeable at the
  * type level — an account could be passed where a partition was meant and only a configured
  * allowlist, if present, would notice.
  */
abstract private[core] class SiteName(field: String):
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    TextIdentifier.validate(field, raw, 255)

  def unsafeFrom(raw: String): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)

  extension (name: Type) def value: String = name
  given CanEqual[Type, Type] = CanEqual.derived
  given Ordering[Type] = Ordering.String

object AccountName extends SiteName("account")
type AccountName = AccountName.Type

object PartitionName extends SiteName("partition")
type PartitionName = PartitionName.Type

object QosName extends SiteName("qos")
type QosName = QosName.Type

object ConstraintName extends SiteName("constraint")
type ConstraintName = ConstraintName.Type

object ModuleName extends SiteName("module")
type ModuleName = ModuleName.Type

object ContainerImage extends SiteName("container")
type ContainerImage = ContainerImage.Type

object AcceleratorKind extends SiteName("accelerator")
type AcceleratorKind = AcceleratorKind.Type

object SiteId extends SiteName("site")
type SiteId = SiteId.Type

enum MemoryMode derives CanEqual:
  case PerNode
  case PerCpu
  case AllNode

object MemoryMode:
  def of(request: MemoryRequest): MemoryMode = request match
    case MemoryRequest.PerNode(_)    => MemoryMode.PerNode
    case MemoryRequest.PerCpu(_)     => MemoryMode.PerCpu
    case MemoryRequest.AllNodeMemory => MemoryMode.AllNode

enum EnvironmentExportPolicy derives CanEqual:
  case Explicit
  case None
  case All

final case class AcceleratorRequest(kind: AcceleratorKind, count: PositiveInt) derives CanEqual

final case class NativeOption private (name: String, value: String) derives CanEqual

object NativeOption:
  private val Name = "[a-z][a-z0-9-]*".r

  /** Option names the library renders itself.
    *
    * Native options are appended after the typed ones and `sbatch` honours the last occurrence of a
    * repeated option, so a native `output` silently replaces the log path the planner established,
    * and a native `parsable` breaks the response parser outright. Name syntax was validated but
    * name *ownership* was not, and the site allowlist is an allowlist — it can admit these, never
    * reserve them. Rejecting here means a conflicting option cannot be constructed at all.
    */
  private val Reserved: Set[String] = Set(
    "account",
    "array",
    "constraint",
    "container",
    "cpus-per-task",
    "error",
    "export",
    "format",
    "gres",
    "job-name",
    "jobs",
    "json",
    "mem",
    "mem-per-cpu",
    "nodes",
    "ntasks",
    "output",
    "parsable",
    "partition",
    "qos",
    "time"
  )

  def from(name: String, value: String): Either[ValidationFailure, NativeOption] =
    for
      checkedName <- TextIdentifier.validate("nativeOptionName", name, 100)
      _ <- Either.cond(
        Name.matches(checkedName),
        (),
        ValidationFailure("nativeOptionName", "must be a lower-case Slurm option name")
      )
      _ <- Either.cond(
        !Reserved.contains(checkedName),
        (),
        ValidationFailure(
          "nativeOptionName",
          s"'$checkedName' is rendered by slurm4s and cannot be overridden natively"
        )
      )
      checkedValue <- TextIdentifier.validate("nativeOptionValue", value, 4096)
    yield NativeOption(checkedName, checkedValue)

final case class SiteIntent(
    account: Option[AccountName] = None,
    partition: Option[PartitionName] = None,
    qos: Option[QosName] = None,
    constraints: Vector[ConstraintName] = Vector.empty,
    modules: Vector[ModuleName] = Vector.empty,
    container: Option[ContainerImage] = None,
    accelerators: Vector[AcceleratorRequest] = Vector.empty,
    nativeOptions: Vector[NativeOption] = Vector.empty,
    environmentExport: Option[EnvironmentExportPolicy] = None
) derives CanEqual

final case class SiteLimits(
    maximumNodes: Option[PositiveInt] = None,
    maximumTasks: Option[PositiveInt] = None,
    maximumCpus: Option[PositiveInt] = None,
    maximumWallTime: Option[WallTimeMinutes] = None,
    maximumArraySize: Option[PositiveInt] = None,
    maximumArrayConcurrency: Option[PositiveInt] = None
) derives CanEqual

final case class SiteFeatures(
    liveLogs: Boolean = true,
    directExecution: Boolean = true,
    structuredResults: Boolean = true,
    modules: Boolean = true,
    containers: Boolean = true,
    arrays: Boolean = true
) derives CanEqual

final case class SitePolicyViolation(
    code: String,
    field: String,
    message: String,
    requested: Option[String] = None
) derives CanEqual

final case class SitePolicyAdvisory(code: String, message: String) derives CanEqual

enum PreflightAuthority derives CanEqual:
  case Advisory

final case class EffectiveSiteSpec(
    resources: ResourceRequest,
    account: Option[AccountName],
    partition: Option[PartitionName],
    qos: Option[QosName],
    constraints: Vector[ConstraintName],
    modules: Vector[ModuleName],
    container: Option[ContainerImage],
    accelerators: Vector[AcceleratorRequest],
    nativeOptions: Vector[NativeOption],
    environmentExport: EnvironmentExportPolicy,
    features: SiteFeatures
) derives CanEqual

final case class SiteResolution(
    site: SiteId,
    requestedResources: ResourceRequest,
    requestedIntent: SiteIntent,
    effective: EffectiveSiteSpec,
    advisories: Vector[SitePolicyAdvisory],
    authority: PreflightAuthority = PreflightAuthority.Advisory
) derives CanEqual

final case class SiteProfile(
    site: SiteId,
    defaultAccount: Option[AccountName] = None,
    defaultPartition: Option[PartitionName] = None,
    defaultQos: Option[QosName] = None,
    defaultMemory: Option[MemoryRequest] = None,
    defaultModules: Vector[ModuleName] = Vector.empty,
    defaultEnvironmentExport: EnvironmentExportPolicy = EnvironmentExportPolicy.Explicit,
    accountRequired: Boolean = false,
    allowedAccounts: Option[Set[AccountName]] = None,
    allowedPartitions: Option[Set[PartitionName]] = None,
    allowedQos: Option[Set[QosName]] = None,
    availableConstraints: Option[Set[ConstraintName]] = None,
    availableModules: Option[Set[ModuleName]] = None,
    availableAccelerators: Map[AcceleratorKind, PositiveInt] = Map.empty,
    allowedMemoryModes: Set[MemoryMode] = MemoryMode.values.toSet,
    maximumPerNodeMemory: Option[Mebibytes] = None,
    maximumPerCpuMemory: Option[Mebibytes] = None,
    allowedNativeOptions: Set[String] = Set.empty,
    limits: SiteLimits = SiteLimits(),
    features: SiteFeatures = SiteFeatures()
) derives CanEqual:

  def resolve(
      spec: LaunchSpec,
      intent: SiteIntent
  ): Either[NonEmptyChain[SitePolicyViolation], SiteResolution] =
    (
      resolve(spec.resources, intent).toValidated,
      spec.array.traverse(validateArray).toValidated
    ).mapN((resolution, _) => resolution).toEither

  def validateArray(
      request: JobArrayRequest
  ): Either[NonEmptyChain[SitePolicyViolation], JobArrayRequest] =
    Vector(
      validWhen(
        features.arrays,
        SitePolicyViolation("arrays-disabled", "array", "job arrays are disabled at this site")
      ),
      within(
        "array-size-limit-exceeded",
        "array",
        Some(request.size.toLong),
        limits.maximumArraySize
      ),
      within(
        "array-concurrency-limit-exceeded",
        "maximumConcurrent",
        request.maximumConcurrent.map(_.toInt.toLong),
        limits.maximumArrayConcurrency
      )
    ).sequence_.as(request).toEither

  def resolve(
      resources: ResourceRequest,
      intent: SiteIntent
  ): Either[NonEmptyChain[SitePolicyViolation], SiteResolution] =
    val account = intent.account.orElse(defaultAccount)
    val partition = intent.partition.orElse(defaultPartition)
    val qos = intent.qos.orElse(defaultQos)
    val effectiveResources =
      if resources.memory.isEmpty then resources.copy(memory = defaultMemory) else resources
    val modules = (defaultModules ++ intent.modules).distinct
    val effective = EffectiveSiteSpec(
      effectiveResources,
      account,
      partition,
      qos,
      intent.constraints.distinct,
      modules,
      intent.container,
      intent.accelerators,
      intent.nativeOptions,
      intent.environmentExport.getOrElse(defaultEnvironmentExport),
      features
    )

    (
      validateRequiredAccount(account),
      validateMember("account-not-allowed", "account", account, allowedAccounts, _.value),
      validateMember("partition-not-allowed", "partition", partition, allowedPartitions, _.value),
      validateMember("qos-not-allowed", "qos", qos, allowedQos, _.value),
      validateAll(
        "constraint-not-available",
        "constraints",
        effective.constraints,
        availableConstraints,
        _.value
      ),
      validateModules(effective.modules),
      validateContainer(effective.container),
      validateAccelerators(effective.accelerators),
      validateNativeOptions(effective.nativeOptions),
      validateResources(effective.resources)
    ).mapN((_, _, _, _, _, _, _, _, _, _) =>
      SiteResolution(
        site,
        resources,
        intent,
        effective,
        Vector(
          SitePolicyAdvisory(
            "controller-authoritative",
            "preflight reflects configured site policy; Slurm and submission plugins remain authoritative"
          )
        )
      )
    ).toEither

  private def validateRequiredAccount(
      value: Option[AccountName]
  ): ValidatedNec[SitePolicyViolation, Unit] =
    validWhen(
      !accountRequired || value.nonEmpty,
      SitePolicyViolation("account-required", "account", "this site profile requires an account")
    )

  // Generic in the name type: the policy check is the same shape for every kind, but the kinds
  // themselves must stay distinct so one cannot be passed where another is meant.
  private def validateMember[A](
      code: String,
      field: String,
      value: Option[A],
      allowed: Option[Set[A]],
      render: A => String
  ): ValidatedNec[SitePolicyViolation, Unit] =
    value match
      case Some(token) if allowed.exists(!_.contains(token)) =>
        SitePolicyViolation(
          code,
          field,
          "the requested value is not allowed",
          Some(render(token))
        ).invalidNec
      case _ => ().validNec

  private def validateAll[A](
      code: String,
      field: String,
      values: Vector[A],
      allowed: Option[Set[A]],
      render: A => String
  ): ValidatedNec[SitePolicyViolation, Unit] =
    values.traverse_(token =>
      validWhen(
        allowed.forall(_.contains(token)),
        SitePolicyViolation(
          code,
          field,
          "the requested value is not available",
          Some(render(token))
        )
      )
    )

  private def validateModules(values: Vector[ModuleName]): ValidatedNec[SitePolicyViolation, Unit] =
    val enabled = validWhen(
      features.modules || values.isEmpty,
      SitePolicyViolation("modules-disabled", "modules", "module loading is disabled at this site")
    )
    enabled.productR(
      validateAll("module-not-available", "modules", values, availableModules, _.value)
    )

  private def validateContainer(
      value: Option[ContainerImage]
  ): ValidatedNec[SitePolicyViolation, Unit] =
    validWhen(
      features.containers || value.isEmpty,
      SitePolicyViolation(
        "containers-disabled",
        "container",
        "container execution is disabled at this site"
      )
    )

  private def validateAccelerators(
      values: Vector[AcceleratorRequest]
  ): ValidatedNec[SitePolicyViolation, Unit] =
    values.traverse_ { request =>
      availableAccelerators.get(request.kind) match
        case None =>
          SitePolicyViolation(
            "accelerator-not-available",
            "accelerators",
            "the requested accelerator is not configured",
            Some(request.kind.value)
          ).invalidNec
        case Some(maximum) =>
          validWhen(
            request.count.toInt <= maximum.toInt,
            SitePolicyViolation(
              "accelerator-limit-exceeded",
              "accelerators",
              s"requested ${request.count.toInt}, profile maximum is ${maximum.toInt}",
              Some(request.kind.value)
            )
          )
    }

  private def validateNativeOptions(
      values: Vector[NativeOption]
  ): ValidatedNec[SitePolicyViolation, Unit] =
    values.traverse_ { option =>
      validWhen(
        allowedNativeOptions.contains(option.name),
        SitePolicyViolation(
          "native-option-not-allowed",
          "nativeOptions",
          "the native option is not admitted by this site profile",
          Some(option.name)
        )
      )
    }

  private def validateResources(
      resources: ResourceRequest
  ): ValidatedNec[SitePolicyViolation, Unit] =
    val mode = resources.memory.map(MemoryMode.of)
    val totalCpus = resources.cpusPerTask.toInt.toLong * resources.tasks.toInt.toLong
    val validations = Vector(
      validWhen(
        resources.nodes.forall(_.toInt <= resources.tasks.toInt),
        SitePolicyViolation(
          "invalid-task-topology",
          "nodes",
          "node count cannot exceed total task count"
        )
      ),
      validWhen(
        mode.forall(allowedMemoryModes.contains),
        SitePolicyViolation(
          "memory-mode-not-allowed",
          "memory",
          "the requested memory convention is disabled by this site profile",
          mode.map(_.toString)
        )
      ),
      validWhen(
        resources.memory.forall {
          case MemoryRequest.PerNode(amount) =>
            maximumPerNodeMemory.forall(amount.toLong <= _.toLong)
          case _ => true
        },
        SitePolicyViolation(
          "per-node-memory-limit-exceeded",
          "memory",
          "the requested per-node memory exceeds the site-profile maximum"
        )
      ),
      validWhen(
        resources.memory.forall {
          case MemoryRequest.PerCpu(amount) => maximumPerCpuMemory.forall(amount.toLong <= _.toLong)
          case _                            => true
        },
        SitePolicyViolation(
          "per-cpu-memory-limit-exceeded",
          "memory",
          "the requested per-CPU memory exceeds the site-profile maximum"
        )
      ),
      within(
        "node-limit-exceeded",
        "nodes",
        resources.nodes.map(_.toInt.toLong),
        limits.maximumNodes
      ),
      within(
        "task-limit-exceeded",
        "tasks",
        Some(resources.tasks.toInt.toLong),
        limits.maximumTasks
      ),
      within("cpu-limit-exceeded", "cpus", Some(totalCpus), limits.maximumCpus),
      validWhen(
        resources.wallTime.forall(requested =>
          limits.maximumWallTime.forall(requested.toLong <= _.toLong)
        ),
        SitePolicyViolation(
          "wall-time-limit-exceeded",
          "wallTime",
          "the requested wall time exceeds the site-profile maximum"
        )
      )
    )
    validations.sequence_.void

  private def within(
      code: String,
      field: String,
      requested: Option[Long],
      maximum: Option[PositiveInt]
  ): ValidatedNec[SitePolicyViolation, Unit] =
    validWhen(
      requested.forall(value => maximum.forall(value <= _.toInt.toLong)),
      SitePolicyViolation(code, field, "the requested value exceeds the site-profile maximum")
    )

  private def validWhen(
      condition: Boolean,
      failure: => SitePolicyViolation
  ): ValidatedNec[SitePolicyViolation, Unit] =
    if condition then ().validNec else failure.invalidNec
