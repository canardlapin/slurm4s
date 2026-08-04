package io.github.bbuchsbaum.slurm4s.cli

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.EnvName
import io.github.bbuchsbaum.slurm4s.core.JobRef
import io.github.bbuchsbaum.slurm4s.core.EnvironmentExportPolicy
import io.github.bbuchsbaum.slurm4s.core.MemoryRequest
import io.github.bbuchsbaum.slurm4s.core.NativeOption
import io.github.bbuchsbaum.slurm4s.core.ResourceRequest
import io.github.bbuchsbaum.slurm4s.core.TerminationNotice
import io.github.bbuchsbaum.slurm4s.core.TerminationNoticeScope

object SlurmCommands:
  def submit(prepared: PreparedSubmission): SlurmCommand =
    val spec = prepared.spec
    val effective = prepared.siteResolution.map(_.effective)
    val resourceArguments = resources(effective.map(_.resources).getOrElse(spec.resources))
    val noticeArguments =
      effective
        .flatMap(_.terminationNotice)
        .orElse(spec.terminationNotice)
        .toVector
        .map(signalOption)
    val siteArguments = effective.toVector.flatMap(siteOptions(_, spec.environment.keySet))
    val arrayArguments = spec.array.toVector.map(arrayOption)
    val scriptArguments = spec.arguments

    SlurmCommand(
      executable = SlurmExecutable.Sbatch,
      arguments = Vector(
        "--parsable",
        s"--job-name=${spec.name.value}",
        s"--output=${prepared.stdoutPath}",
        s"--error=${prepared.stderrPath}"
      ) ++ resourceArguments ++ noticeArguments ++ siteArguments ++ arrayArguments ++ Vector(
        prepared.scriptPath
      ) ++ scriptArguments,
      environment = spec.environment.iterator.map { case (name, value) =>
        name.value -> value
      }.toMap
    )

  def observe(jobs: NonEmptyVector[JobRef], parser: DataParserVersion): SlurmCommand =
    SlurmCommand(
      SlurmExecutable.Squeue,
      Vector(
        s"--json=${parser.value}",
        s"--jobs=${jobIds(jobs)}"
      )
    )

  def accounting(jobs: NonEmptyVector[JobRef]): SlurmCommand =
    SlurmCommand(
      SlurmExecutable.Sacct,
      Vector(
        "--noheader",
        "--parsable2",
        "--array",
        "--allocations",
        "--format=JobID,State,ExitCode,Reason",
        s"--jobs=${jobIds(jobs)}"
      )
    )

  def focused(job: JobRef): SlurmCommand =
    SlurmCommand(
      SlurmExecutable.Scontrol,
      Vector("--oneliner", "show", "job", slurmJobId(job))
    )

  def cancel(job: JobRef): SlurmCommand =
    SlurmCommand(SlurmExecutable.Scancel, Vector(slurmJobId(job)))

  val versionProbe: SlurmCommand = SlurmCommand(SlurmExecutable.Sbatch, Vector("--version"))
  val parserProbe: SlurmCommand = SlurmCommand(SlurmExecutable.Squeue, Vector("--json=list"))
  val accountingProbe: SlurmCommand = SlurmCommand(
    SlurmExecutable.Sacct,
    Vector(
      "--noheader",
      "--starttime=now",
      "--endtime=now",
      "--format=JobID"
    )
  )
  val configProbe: SlurmCommand = SlurmCommand(SlurmExecutable.Scontrol, Vector("show", "config"))

  private def resources(request: ResourceRequest): Vector[String] =
    Vector(
      s"--cpus-per-task=${request.cpusPerTask.toInt}",
      s"--ntasks=${request.tasks.toInt}"
    ) ++ request.nodes.toVector.map(value => s"--nodes=${value.toInt}") ++
      request.memory.toVector.map {
        case MemoryRequest.PerNode(amount) => s"--mem=${amount.toLong}M"
        case MemoryRequest.PerCpu(amount)  => s"--mem-per-cpu=${amount.toLong}M"
        case MemoryRequest.AllNodeMemory   => "--mem=0"
      } ++ request.wallTime.toVector.map(value => s"--time=${value.toLong}")

  private def signalOption(notice: TerminationNotice): String =
    val scope = notice.scope match
      case TerminationNoticeScope.BatchShell => "B:"
      case TerminationNoticeScope.JobSteps   => ""
    s"--signal=$scope${notice.signal.slurmName}@${notice.leadSeconds.toInt}"

  private def siteOptions(
      spec: io.github.bbuchsbaum.slurm4s.core.EffectiveSiteSpec,
      environmentNames: Set[EnvName]
  ): Vector[String] =
    spec.account.toVector.map(value => s"--account=${value.value}") ++
      spec.partition.toVector.map(value => s"--partition=${value.value}") ++
      spec.qos.toVector.map(value => s"--qos=${value.value}") ++
      Option
        .when(spec.constraints.nonEmpty)(
          s"--constraint=${spec.constraints.map(_.value).mkString("&")}"
        )
        .toVector ++
      spec.container.toVector.map(value => s"--container=${value.value}") ++
      Option
        .when(spec.accelerators.nonEmpty)(
          s"--gres=${spec.accelerators.map(value => s"${value.kind.value}:${value.count.toInt}").mkString(",")}"
        )
        .toVector ++
      spec.nativeOptions.map(renderNative) ++
      Vector(exportOption(spec.environmentExport, environmentNames))

  private def renderNative(option: NativeOption): String =
    s"--${option.name}=${option.value}"

  private def exportOption(
      policy: EnvironmentExportPolicy,
      environmentNames: Set[EnvName]
  ): String = policy match
    case EnvironmentExportPolicy.None     => "--export=NONE"
    case EnvironmentExportPolicy.All      => "--export=ALL"
    case EnvironmentExportPolicy.Explicit =>
      if environmentNames.isEmpty then "--export=NONE"
      else s"--export=${environmentNames.toVector.map(_.value).sorted.mkString(",")}"

  private def arrayOption(request: io.github.bbuchsbaum.slurm4s.core.JobArrayRequest): String =
    val indices = compactIndices(request.indices.toVector.map(_.value).sorted)
    val throttle = request.maximumConcurrent.fold("")(value => s"%${value.toInt}")
    s"--array=$indices$throttle"

  private def compactIndices(values: Vector[Int]): String =
    val ranges = values.tail.foldLeft(Vector(values.head -> values.head)) {
      case (current, value) if value == current.last._2 + 1 =>
        current.updated(current.size - 1, current.last._1 -> value)
      case (current, value) => current :+ (value -> value)
    }
    ranges
      .map { case (first, last) => if first == last then first.toString else s"$first-$last" }
      .mkString(",")

  private def jobIds(jobs: NonEmptyVector[JobRef]): String =
    jobs.toVector.map(slurmJobId).mkString(",")

  private def slurmJobId(job: JobRef): String =
    job.arrayIndex.fold(job.jobId.value)(index => s"${job.jobId.value}_${index.value}")
