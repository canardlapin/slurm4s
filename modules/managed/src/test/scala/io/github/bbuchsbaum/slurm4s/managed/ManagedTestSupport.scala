package io.github.bbuchsbaum.slurm4s.managed

import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant

private[managed] object ManagedTestSupport:
  val instant: Instant = Instant.parse("2026-07-22T12:00:00Z")
  val later: Instant = instant.plusSeconds(1L)
  val evidence: EvidenceBundle = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.DurableJournal, instant, Vector(1, 2, 3))
  )
  val job: JobRef = JobRef(JobId.from("7001").toOption.get, None, None)

  def request(key: String, body: String = "true"): LaunchSpec =
    LaunchSpec(
      SubmissionKey.from(key).toOption.get,
      JobName.from("managed-test").toOption.get,
      ScriptSource.Inline(
        "job.sh",
        s"#!/bin/sh\n$body\n".getBytes("UTF-8").toVector
      ),
      Vector.empty,
      ResultContract.ExitOnly.descriptor,
      ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
    )

  def intent(key: String, body: String = "true"): ManagedIntent =
    ManagedIntent.from(request(key, body), instant).toOption.get

  def accepted: SubmissionAttempt =
    SubmissionAttempt.Completed(Submission.Accepted(job, evidence))

  def applyCommand(state: ControlState, command: ControlCommand): ControlCommit =
    ControlTransition(state, command).fold(
      failure => throw new AssertionError(failure.toString),
      identity
    )
