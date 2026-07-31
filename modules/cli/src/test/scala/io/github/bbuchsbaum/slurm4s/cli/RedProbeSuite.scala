// NOTE: this file should be renamed to SlurmStateMappingSuite.scala. It began as a temporary RED
// probe for P8.A2 and the session that wrote it could not delete or rename files.
package io.github.bbuchsbaum.slurm4s.cli

import io.github.bbuchsbaum.slurm4s.core.*

/** P8.B1 regression: the states added in P8.A2 must be mapped everywhere, not just where tests hit.
  *
  * Both matches below were already written exhaustively with no catch-all, which is why adding
  * `BootFail`, `Deadline` and `Suspended` turned them into runtime `MatchError`s rather than silent
  * fall-throughs. The suite was green; only `-Werror` exposed them.
  */
class SlurmStateMappingSuite extends munit.FunSuite:

  test("every state has an accounting outcome mapping") {
    val expectations = Vector(
      "BOOT_FAIL" -> Some(WorkloadOutcome.NodeFailure),
      "DEADLINE" -> Some(WorkloadOutcome.TimeLimitExceeded),
      "SUSPENDED" -> None
    )
    expectations.foreach { case (token, expected) =>
      assertEquals(
        SacctParsable2.outcome(SlurmStateParser.parse(token), None, ""),
        expected,
        s"$token has no accounting outcome mapping"
      )
    }
  }

  test("a terminal state does not imply the job actually started") {
    val at = SchedulerTimestamp.Absolute(java.time.Instant.parse("2026-07-30T12:00:00Z"))

    // DEADLINE commonly fires while the job is still pending, and BOOT_FAIL means the node never
    // came up, so neither may be reported as an actual start.
    assertEquals(
      SlurmTiming.classifyStart(SlurmStateParser.parse("BOOT_FAIL"), at),
      JobStart.Reported(at)
    )
    assertEquals(
      SlurmTiming.classifyStart(SlurmStateParser.parse("DEADLINE"), at),
      JobStart.Reported(at)
    )
    assertEquals(
      SlurmTiming.classifyStart(SlurmStateParser.parse("SUSPENDED"), at),
      JobStart.Actual(at)
    )
  }
