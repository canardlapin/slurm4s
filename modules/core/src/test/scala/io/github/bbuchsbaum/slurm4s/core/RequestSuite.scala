package io.github.bbuchsbaum.slurm4s.core

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

  test("retry safety defaults conservatively and attempt epochs advance without overflow") {
    assertEquals(RetrySafety.Unknown, RetrySafety.Unknown)
    assertEquals(AttemptEpoch.initial.next.map(_.value), Right(2L))
    assert(AttemptEpoch.from(Long.MaxValue).toOption.get.next.isLeft)
  }

  test("termination notice lead time covers exactly Slurm's supported range") {
    assertEquals(SignalLeadSeconds.from(0).map(_.toInt), Right(0))
    assertEquals(SignalLeadSeconds.from(65535).map(_.toInt), Right(65535))
    assert(SignalLeadSeconds.from(-1).isLeft)
    assert(SignalLeadSeconds.from(65536).isLeft)
  }

  test("termination notice signals are catchable names, never KILL or STOP") {
    val names = TerminationNoticeSignal.values.map(_.slurmName).toSet
    assert(names.contains("USR1"))
    assert(names.contains("TERM"))
    assert(!names.contains("KILL"))
    assert(!names.contains("STOP"))
  }

  test("environment names exclude Slurm export syntax and reserved tokens") {
    Vector(
      "",
      "9INVALID",
      "NOT VALID",
      "FOO,BAR",
      "FOO=BAR",
      "FOO\nBAR",
      "ALL",
      "none",
      "NIL"
    ).foreach(raw => assert(EnvName.from(raw).isLeft, clues(raw)))

    assertEquals(EnvName.from("_VALID_42").map(_.value), Right("_VALID_42"))
  }
