package io.github.bbuchsbaum.scalaslurm.core

class RequestSuite extends munit.FunSuite:
  test("resource validation accumulates independent problems") {
    val result = ResourceRequest.validate(
      cpusPerTask = 0,
      tasks = -2,
      nodes = Some(0),
      memory = None,
      wallTime = None
    )

    assertEquals(result.swap.toOption.get.length, 3L)
  }

  test("memory alternatives remain explicit rather than collapsing to a number") {
    val amount = Mebibytes.from(8192).toOption.get
    val perNode: MemoryRequest = MemoryRequest.PerNode(amount)
    val perCpu: MemoryRequest = MemoryRequest.PerCpu(amount)
    val allNode: MemoryRequest = MemoryRequest.AllNodeMemory

    assertNotEquals(perNode, perCpu)
    assertNotEquals(perNode, allNode)
  }
