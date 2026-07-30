package io.github.bbuchsbaum.slurm4s.ssh

import io.github.bbuchsbaum.slurm4s.core.*

/** The await cadence has to stay affordable on the transport ADR 0003 actually specifies: one SSH
  * process and one remote agent start per request.
  */
class RemoteAwaitPolicySuite extends munit.FunSuite:
  private val default = RemoteAwaitPolicy.default

  test("the default cadence backs off instead of polling at a fixed high rate") {
    val intervals = (0L until 8L).toVector.map(default.intervalAfter(_).value)
    assertEquals(intervals, Vector(5000L, 10000L, 20000L, 40000L, 60000L, 60000L, 60000L, 60000L))
  }

  test("the ceiling holds for arbitrarily long waits and never overflows") {
    Vector(10L, 40L, 1000L, Long.MaxValue).foreach { attempt =>
      assertEquals(
        default.intervalAfter(attempt).value,
        default.maximumPollInterval.value,
        s"attempt $attempt must clamp to the ceiling"
      )
    }
  }

  test("accounting is checked on the first poll and then far less often than the result read") {
    val checks = (0L until 24L).toVector.filter(default.checksAccounting)
    assertEquals(checks, Vector(0L, 6L, 12L, 18L))
  }

  test("a full day of waiting costs orders of magnitude fewer requests than a one-second poll") {
    val day = 24L * 60L * 60L * 1000L
    val (polls, accountingCalls) = walk(default, day)

    // A 1 s poll that also queried sacct every time cost 86,400 result reads plus 86,400 sacct
    // queries against a shared cluster database.
    assert(polls < 1500L, s"expected well under 1500 polls for a day, got $polls")
    assert(
      accountingCalls < 300L,
      s"expected well under 300 accounting queries for a day, got $accountingCalls"
    )
  }

  test("a caller may still opt into an aggressive cadence explicitly") {
    val eager = RemoteAwaitPolicy(
      pollInterval = DurationMillis.unsafeFrom(100L),
      timeout = DurationMillis.unsafeFrom(1000L),
      maximumPollInterval = DurationMillis.unsafeFrom(100L),
      accountingEveryPolls = PositiveInt.unsafeFrom(1)
    )
    assertEquals(eager.intervalAfter(0L).value, 100L)
    assertEquals(eager.intervalAfter(9L).value, 100L)
    assert((0L until 5L).forall(eager.checksAccounting))
  }

  test("a ceiling below the initial interval never shortens the caller's interval") {
    val inverted = RemoteAwaitPolicy(
      pollInterval = DurationMillis.unsafeFrom(30_000L),
      timeout = DurationMillis.unsafeFrom(60_000L),
      maximumPollInterval = DurationMillis.unsafeFrom(1_000L)
    )
    assertEquals(inverted.intervalAfter(0L).value, 30_000L)
    assertEquals(inverted.intervalAfter(5L).value, 30_000L)
  }

  /** Replays the poll loop's own arithmetic over a wall-clock budget. */
  private def walk(policy: RemoteAwaitPolicy, budgetMillis: Long): (Long, Long) =
    var elapsed = 0L
    var attempt = 0L
    var accounting = 0L
    while elapsed < budgetMillis do
      if policy.checksAccounting(attempt) then accounting += 1L
      elapsed += policy.intervalAfter(attempt).value
      attempt += 1L
    (attempt, accounting)
