package io.github.bbuchsbaum.slurm4s.cli

import io.github.bbuchsbaum.slurm4s.core.*

/** P8.A2: base states SchedMD documents must not fall through to `Unknown`.
  *
  * An unrecognized state yields `InterruptionClass.Unknown`, is not terminal to the managed
  * coordinator, and produces no failure cause — so a job that is finished at the site stays
  * invisible to every classifier and the awaiter polls it for its full 24-hour default.
  */
class SlurmStateModelSuite extends munit.FunSuite:

  private val documentedBaseStates = Vector("BOOT_FAIL", "DEADLINE", "SUSPENDED")

  test("documented base states parse to a recognized state rather than Unknown") {
    val unrecognized = documentedBaseStates.filter { token =>
      SlurmStateParser.parse(token) match
        case SlurmState.Unknown(_) => true
        case _                     => false
    }
    assertEquals(
      unrecognized,
      Vector.empty[String],
      s"these documented base states fall through to Unknown: $unrecognized"
    )
  }

  test("terminal base states are classified as terminal") {
    val terminal = Vector("BOOT_FAIL", "DEADLINE", "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT")
    val misclassified = terminal.filter { token =>
      Terminality.of(SlurmStateParser.parse(token)) != Terminality.Terminal
    }
    assertEquals(misclassified, Vector.empty[String], s"not terminal: $misclassified")
  }

  test("a suspended job is active, not terminal") {
    assertEquals(Terminality.of(SlurmStateParser.parse("SUSPENDED")), Terminality.Active)
  }

  test("an unrecognized state is indeterminate, never assumed active or terminal") {
    assertEquals(Terminality.of(SlurmStateParser.parse("FUTURE_STATE")), Terminality.Indeterminate)
  }

  test("a truncated state is recorded as truncated rather than invented as a flag") {
    val report = SlurmStateParser.report("CANCELLED+")

    assertEquals(report.state, SlurmState.Cancelled)
    assertEquals(report.flags, Vector.empty[SlurmStateFlag])
    assert(report.truncated, "the trailing plus must be recorded, not silently dropped")
  }

  test("sacct free text after the state is not mistaken for a flag") {
    val report = SlurmStateParser.report("CANCELLED by 1000")

    assertEquals(report.state, SlurmState.Cancelled)
    assertEquals(report.flags, Vector.empty[SlurmStateFlag])
  }

  test("every element of an array-valued job state is retained") {
    val report = SlurmStateParser.reportOf(Vector("RUNNING", "POWER_UP_NODE"))

    assertEquals(report.state, SlurmState.Running)
    assertEquals(report.flags, Vector(SlurmStateFlag.PowerUpNode))
  }

  test("an unknown flag is preserved verbatim rather than discarded") {
    val report = SlurmStateParser.reportOf(Vector("RUNNING", "FUTURE_FLAG"))

    assertEquals(report.state, SlurmState.Running)
    assertEquals(report.flags, Vector(SlurmStateFlag.Unknown("FUTURE_FLAG")))
  }

  test("a flag reported without a base state does not fabricate one") {
    val report = SlurmStateParser.report("RESIZING")

    assertEquals(report.flags, Vector(SlurmStateFlag.Resizing))
    assert(
      report.state match
        case SlurmState.Unknown(_) => true
        case _                     => false,
      s"a flag-only report must not invent a base state, got ${report.state}"
    )
    assertEquals(Terminality.of(report.state), Terminality.Indeterminate)
  }
