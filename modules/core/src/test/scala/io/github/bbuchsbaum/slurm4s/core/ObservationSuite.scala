package io.github.bbuchsbaum.slurm4s.core

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
      SlurmState.Completed -> InterruptionClass.NotInterrupted,
      SlurmState.Suspended -> InterruptionClass.NotInterrupted,
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

  test("requeueing is read from flags, because no base state can express it") {
    val requeueFlags = Vector(
      SlurmStateFlag.Requeued,
      SlurmStateFlag.RequeueHold,
      SlurmStateFlag.RequeueFederation,
      SlurmStateFlag.SpecialExit
    )
    requeueFlags.foreach { flag =>
      val report = ReportedState(SlurmState.Unknown("REQUEUED"), Vector(flag), truncated = false)
      assertEquals(InterruptionClass.classify(report), InterruptionClass.Requeueing, clues(flag))
    }
  }

  test("a requeue flag over a running base state still reads as requeueing") {
    val report =
      ReportedState(SlurmState.Running, Vector(SlurmStateFlag.Requeued), truncated = false)

    assertEquals(InterruptionClass.classify(report), InterruptionClass.Requeueing)
    assertEquals(InterruptionClass.classify(report.state), InterruptionClass.NotInterrupted)
  }
