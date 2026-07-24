package io.github.bbuchsbaum.scalaslurm.core

import java.time.Instant
import java.time.LocalDateTime

class ObservationSuite extends munit.FunSuite:
  test("timing facts distinguish absolute and site-local scheduler timestamps") {
    val start = Instant.parse("2026-07-23T10:00:00Z")
    val end = LocalDateTime.parse("2026-07-23T11:30:00")
    val limit = WallTimeMinutes.from(90).toOption.get
    val timing = JobTiming(
      Some(JobStart.Actual(SchedulerTimestamp.Absolute(start))),
      Some(SchedulerTimestamp.SiteLocal(end)),
      ObservedTimeLimit.Limited(limit)
    )

    assertEquals(
      timing.start,
      Some(JobStart.Actual(SchedulerTimestamp.Absolute(start)))
    )
    assertEquals(timing.projectedEndAt, Some(SchedulerTimestamp.SiteLocal(end)))
    assertEquals(timing.timeLimit, ObservedTimeLimit.Limited(limit))
  }

  test("unknown timing does not invent a start, deadline, or time limit") {
    assertEquals(
      JobTiming.unknown,
      JobTiming(None, None, ObservedTimeLimit.Unknown(None))
    )
  }

  test("interruption classification is exhaustive and does not infer retry policy") {
    val cases = Vector(
      SlurmState.Pending -> InterruptionClass.NotInterrupted,
      SlurmState.Running -> InterruptionClass.NotInterrupted,
      SlurmState.Completing -> InterruptionClass.NotInterrupted,
      SlurmState.Completed -> InterruptionClass.NotInterrupted,
      SlurmState.Requeued -> InterruptionClass.Requeueing,
      SlurmState.RequeueHeld -> InterruptionClass.Requeueing,
      SlurmState.RequeueFederation -> InterruptionClass.Requeueing,
      SlurmState.SpecialExit -> InterruptionClass.Requeueing,
      SlurmState.NodeFailure -> InterruptionClass.InfrastructureFailure,
      SlurmState.Preempted -> InterruptionClass.SchedulerPolicy,
      SlurmState.TimedOut -> InterruptionClass.SchedulerPolicy,
      SlurmState.Cancelled -> InterruptionClass.Cancellation,
      SlurmState.Failed -> InterruptionClass.WorkloadFailure,
      SlurmState.OutOfMemory -> InterruptionClass.WorkloadFailure,
      SlurmState.Unknown("FUTURE_STATE") -> InterruptionClass.Unknown
    )

    cases.foreach { case (state, expected) =>
      assertEquals(InterruptionClass.classify(state), expected, clues(state))
    }
  }
