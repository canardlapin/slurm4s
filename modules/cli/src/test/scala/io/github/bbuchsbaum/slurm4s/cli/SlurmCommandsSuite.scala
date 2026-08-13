package io.github.bbuchsbaum.slurm4s.cli

import io.github.bbuchsbaum.slurm4s.core.*

class SlurmCommandsSuite extends munit.FunSuite:
  test("sbatch command is a fixed executable plus argument vector") {
    val request = jobRequest(MemoryRequest.PerNode(Mebibytes.from(8192).toOption.get))
    val prepared = PreparedSubmission(
      request,
      scriptPath = "/private/work/script.R",
      stdoutPath = "/private/work/stdout.log",
      stderrPath = "/private/work/stderr.log"
    )

    val command = SlurmCommands.submit(prepared)

    assertEquals(command.executable, SlurmExecutable.Sbatch)
    assert(command.arguments.contains("--parsable"))
    assert(command.arguments.contains("--mem=8192M"))
    assert(command.arguments.contains("--output=/private/work/stdout.log"))
    assert(command.arguments.contains("--error=/private/work/stderr.log"))
    assertEquals(command.arguments.takeRight(2), Vector("/private/work/script.R", "--vanilla"))
    assert(!command.arguments.exists(_.contains("; sbatch")))
  }

  test("per-node and per-cpu memory produce different Slurm options") {
    def command(memory: MemoryRequest): SlurmCommand =
      SlurmCommands.submit(
        PreparedSubmission(jobRequest(memory), "/work/script", "/work/out", "/work/err")
      )

    val amount = Mebibytes.from(2048).toOption.get
    assert(command(MemoryRequest.PerNode(amount)).arguments.contains("--mem=2048M"))
    assert(command(MemoryRequest.PerCpu(amount)).arguments.contains("--mem-per-cpu=2048M"))
    assert(command(MemoryRequest.AllNodeMemory).arguments.contains("--mem=0"))
  }

  test("termination notice is one argv value with explicit Slurm scope") {
    def command(scope: TerminationNoticeScope): SlurmCommand =
      val notice = TerminationNotice(
        TerminationNoticeSignal.Usr1,
        scope,
        SignalLeadSeconds.unsafeFrom(120)
      )
      SlurmCommands.submit(
        PreparedSubmission(
          jobRequest(MemoryRequest.AllNodeMemory).copy(terminationNotice = Some(notice)),
          "/work/script",
          "/work/out",
          "/work/err"
        )
      )

    val batch = command(TerminationNoticeScope.BatchShell).arguments
    val steps = command(TerminationNoticeScope.JobSteps).arguments

    assertEquals(batch.filter(_.startsWith("--signal=")), Vector("--signal=B:USR1@120"))
    assertEquals(steps.filter(_.startsWith("--signal=")), Vector("--signal=USR1@120"))
  }

  test("resolved site settings are emitted as argv and retain explicit environment export") {
    val request = jobRequest(MemoryRequest.PerNode(Mebibytes.from(8192).toOption.get))
      .copy(
        environment = Map(
          EnvName.unsafeFrom("TOKEN_FILE") -> "/private/token",
          EnvName.unsafeFrom("LANG") -> "C.UTF-8"
        )
      )
    val account = AccountName.unsafeFrom("research")
    val partition = PartitionName.unsafeFrom("compute")
    val qos = QosName.unsafeFrom("normal")
    val gpu = AcceleratorKind.unsafeFrom("gpu:a100")
    val native = NativeOption.from("licenses", "matlab@server:1").toOption.get
    val resolution = SiteProfile(
      site = SiteId.unsafeFrom("example"),
      defaultAccount = Some(account),
      defaultPartition = Some(partition),
      defaultQos = Some(qos),
      availableAccelerators = Map(gpu -> PositiveInt.from("gpus", 4).toOption.get),
      allowedNativeOptions = Set("licenses")
    ).resolve(
      request.resources,
      SiteIntent(
        accelerators = Vector(AcceleratorRequest(gpu, PositiveInt.from("gpus", 2).toOption.get)),
        nativeOptions = Vector(native)
      )
    ).toOption
      .get
    val command = SlurmCommands.submit(
      PreparedSubmission(request, "/work/script", "/work/out", "/work/err", Some(resolution))
    )

    assert(command.arguments.contains("--account=research"))
    assert(command.arguments.contains("--partition=compute"))
    assert(command.arguments.contains("--qos=normal"))
    assert(command.arguments.contains("--gres=gpu:a100:2"))
    assert(command.arguments.contains("--licenses=matlab@server:1"))
    assert(command.arguments.contains("--export=LANG,TOKEN_FILE"))
    assert(!command.arguments.exists(_.contains("/private/token")))
  }

  test("array submission, focused probes, and cancellation retain element identity") {
    val request = jobRequest(MemoryRequest.PerCpu(Mebibytes.from(1024).toOption.get))
    val array = JobArrayRequest
      .contiguous(
        PositiveInt.from("size", 3).toOption.get,
        Some(PositiveInt.from("concurrency", 2).toOption.get)
      )
      .toOption
      .get
    val submit = SlurmCommands.submit(
      PreparedSubmission(
        request.copy(array = Some(array)),
        "/work/dispatch",
        "/work/%A_%a.out",
        "/work/%A_%a.err"
      )
    )
    val element = JobRef(
      JobId.from("9000").toOption.get,
      Some(ArrayIndex.from(2).toOption.get)
    )

    assert(submit.arguments.contains("--array=0-2%2"))
    assertEquals(SlurmCommands.cancel(element).arguments, Vector("9000_2"))
    assertEquals(SlurmCommands.focused(element).arguments.last, "9000_2")
  }

  test("queue listing is current-user, per-element, filtered, and shell-free") {
    val query = QueueQuery
      .currentUser(
        Vector(JobName.unsafeFrom("beta"), JobName.unsafeFrom("alpha")),
        Vector(PartitionName.unsafeFrom("gpu")),
        Vector(QueueStateFilter.Running, QueueStateFilter.Pending)
      )
      .toOption
      .get
    val command = SlurmCommands.listJobs(
      query,
      DataParserVersion.from("v0.0.43").toOption.get
    )

    assertEquals(command.executable, SlurmExecutable.Squeue)
    assertEquals(
      command.arguments,
      Vector(
        "--json=v0.0.43",
        "--me",
        "--array",
        "--name=alpha,beta",
        "--partition=gpu",
        "--states=PENDING,RUNNING"
      )
    )
    assertEquals(command.environment, Map.empty)
  }

  private def jobRequest(memory: MemoryRequest): LaunchSpec =
    LaunchSpec(
      submissionKey = SubmissionKey.from("submit-command-test").toOption.get,
      name = JobName.from("analysis").toOption.get,
      source = ScriptSource.ExistingRemote("/private/work/script.R"),
      arguments = Vector("--vanilla"),
      resultContract = ResultContract.ExitOnly.descriptor,
      resources = ResourceRequest
        .validate(2, 1, Some(1), Some(memory), WallTimeMinutes.from(10).toOption)
        .toOption
        .get
    )
