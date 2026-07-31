package io.github.bbuchsbaum.slurm4s.core

class SiteProfileSuite extends munit.FunSuite:
  test("site resolution records portable intent and resolved defaults") {
    val account = AccountName.unsafeFrom("research")
    val partition = PartitionName.unsafeFrom("cpu")
    val module = ModuleName.unsafeFrom("python/3.12")
    val profile = SiteProfile(
      site = SiteId.unsafeFrom("example-hpc"),
      defaultAccount = Some(account),
      defaultPartition = Some(partition),
      defaultMemory = Some(MemoryRequest.PerCpu(memory(1024))),
      defaultModules = Vector(module),
      accountRequired = true,
      allowedAccounts = Some(Set(account)),
      allowedPartitions = Some(Set(partition)),
      availableModules = Some(Set(module))
    )
    val requested = resources(tasks = 2, nodes = Some(1), memory = None)

    val result = profile.resolve(requested, SiteIntent()).toOption.get

    assertEquals(result.requestedResources, requested)
    assertEquals(result.requestedIntent, SiteIntent())
    assertEquals(result.effective.account, Some(account))
    assertEquals(result.effective.partition, Some(partition))
    assertEquals(result.effective.resources.memory, Some(MemoryRequest.PerCpu(memory(1024))))
    assertEquals(result.effective.modules, Vector(module))
    assertEquals(result.authority, PreflightAuthority.Advisory)
    assert(result.advisories.exists(_.code == "controller-authoritative"))
  }

  test("site preflight accumulates account, topology, memory, and native-option violations") {
    val allowed = AccountName.unsafeFrom("allowed")
    val denied = AccountName.unsafeFrom("denied")
    val profile = SiteProfile(
      site = SiteId.unsafeFrom("strict-hpc"),
      accountRequired = true,
      allowedAccounts = Some(Set(allowed)),
      allowedMemoryModes = Set(MemoryMode.PerCpu),
      maximumPerNodeMemory = Some(memory(4096)),
      allowedNativeOptions = Set("licenses"),
      limits = SiteLimits(maximumNodes = Some(positive("maximumNodes", 2)))
    )
    val native = NativeOption.from("exclusive", "user").toOption.get
    val request = resources(
      tasks = 1,
      nodes = Some(4),
      memory = Some(MemoryRequest.PerNode(memory(8192)))
    )

    val failures = profile
      .resolve(request, SiteIntent(account = Some(denied), nativeOptions = Vector(native)))
      .swap
      .toOption
      .get
      .toChain
      .toVector
      .map(_.code)
      .toSet

    assertEquals(
      failures,
      Set(
        "account-not-allowed",
        "native-option-not-allowed",
        "invalid-task-topology",
        "memory-mode-not-allowed",
        "per-node-memory-limit-exceeded",
        "node-limit-exceeded"
      )
    )
  }

  test("disabled site features reject modules, containers, and unknown accelerators") {
    val profile = SiteProfile(
      site = SiteId.unsafeFrom("minimal-hpc"),
      features = SiteFeatures(modules = false, containers = false)
    )
    val intent = SiteIntent(
      modules = Vector(ModuleName.unsafeFrom("R/4.5")),
      container = Some(ContainerImage.unsafeFrom("analysis.sif")),
      accelerators =
        Vector(AcceleratorRequest(AcceleratorKind.unsafeFrom("gpu"), positive("gpus", 1)))
    )

    val failures = profile
      .resolve(resources(1, Some(1), None), intent)
      .swap
      .toOption
      .get
      .toChain
      .toVector
      .map(_.code)
      .toSet

    assertEquals(
      failures,
      Set("modules-disabled", "containers-disabled", "accelerator-not-available")
    )
  }

  test(
    "array policy rejects disabled arrays and configured size limits without controller claims"
  ) {
    val request =
      JobArrayRequest.contiguous(positive("size", 4), Some(positive("parallel", 3))).toOption.get
    val profile = SiteProfile(
      site = SiteId.unsafeFrom("array-limited"),
      limits = SiteLimits(
        maximumArraySize = Some(positive("maximumArraySize", 2)),
        maximumArrayConcurrency = Some(positive("maximumArrayConcurrency", 1))
      ),
      features = SiteFeatures(arrays = false)
    )

    val failures =
      profile.validateArray(request).swap.toOption.get.toChain.toVector.map(_.code).toSet

    assertEquals(
      failures,
      Set(
        "arrays-disabled",
        "array-size-limit-exceeded",
        "array-concurrency-limit-exceeded"
      )
    )
  }

  private def memory(value: Long): Mebibytes = Mebibytes.from(value).toOption.get

  private def positive(field: String, value: Int): PositiveInt =
    PositiveInt.from(field, value).toOption.get

  private def resources(
      tasks: Int,
      nodes: Option[Int],
      memory: Option[MemoryRequest]
  ): ResourceRequest =
    ResourceRequest
      .validate(
        cpusPerTask = 2,
        tasks = tasks,
        nodes = nodes,
        memory = memory,
        wallTime = WallTimeMinutes.from(30).toOption
      )
      .toOption
      .get
