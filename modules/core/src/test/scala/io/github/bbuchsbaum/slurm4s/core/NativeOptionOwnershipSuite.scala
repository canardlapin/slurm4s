package io.github.bbuchsbaum.slurm4s.core

/** P8.A6: a native option may not impersonate one the library renders itself. */
class NativeOptionOwnershipSuite extends munit.FunSuite:

  test("renderer-owned option names are rejected at construction") {
    val reserved = Vector(
      "output",
      "error",
      "job-name",
      "parsable",
      "array",
      "export",
      "account",
      "partition",
      "qos",
      "cpus-per-task",
      "mem",
      "time",
      "gres"
    )
    val admitted = reserved.filter(name => NativeOption.from(name, "value").isRight)

    assertEquals(
      admitted,
      Vector.empty[String],
      s"these would silently override the rendered option: $admitted"
    )
  }

  test("an ordinary native option is still admitted") {
    assert(NativeOption.from("mail-type", "END").isRight)
    assert(NativeOption.from("nice", "100").isRight)
  }

  test("rejection is independent of the site allowlist") {
    // The allowlist can admit a name; it can never reserve one. Construction must fail first.
    assert(
      NativeOption.from("output", "/tmp/hijacked.log").isLeft,
      "an operator who allowlists 'output' must still not be able to build it"
    )
  }
