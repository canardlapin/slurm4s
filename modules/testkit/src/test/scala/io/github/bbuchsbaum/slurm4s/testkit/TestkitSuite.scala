package io.github.bbuchsbaum.slurm4s.testkit

import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.cli.*
import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant

class TestkitSuite extends munit.CatsEffectSuite:
  private val observedAt = Instant.parse("2026-07-30T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(
      EvidenceSource.CommandStdout("test"),
      observedAt,
      Vector.empty
    )
  )
  private val job = JobRef(JobId.unsafeFrom("42"), None)
  private val submissionKey = SubmissionKey.unsafeFrom("scripted-submit")
  private val request = JobRequest(
    submissionKey,
    JobName.unsafeFrom("scripted"),
    Payload.Script(
      ScriptSource.ExistingRemote("/work/script.sh"),
      Vector.empty,
      ResultContract.ExitOnly
    ),
    ResourceRequest.validate(1, 1, None, None, None).toOption.get
  )

  test("scripted scheduler traces ordered public operations and proves exhaustion") {
    val submission = SubmissionAttempt.PreparationFailed(
      Diagnostics.one(Diagnostic("fixture", "fixture"))
    )
    val cancellation =
      CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence))

    for
      scheduler <- ScriptedScheduler.create[IO](
        Vector(
          SchedulerScriptStep.Submit(submissionKey, submission),
          SchedulerScriptStep.Cancel(job, cancellation)
        )
      )
      submitted <- scheduler.submitLowered(request)
      cancelled <- scheduler.cancel(job)
      _ <- scheduler.assertDrained
      observed <- scheduler.observed
      exhausted <- scheduler.cancel(job).attempt
    yield
      assertEquals(submitted, submission)
      assertEquals(cancelled, cancellation)
      assertEquals(
        observed,
        Vector(SchedulerOperation.Submit(submissionKey), SchedulerOperation.Cancel(job))
      )
      assert(exhausted.left.exists(_.getMessage.contains("script exhausted")))
  }

  test("scripted command expectations can require the declared policy") {
    val command = SlurmCommand(SlurmExecutable.Squeue, Vector("--json"))
    val policy = CommandPolicy(
      DurationMillis.unsafeFrom(1000L),
      ByteLimit.unsafeFrom(1024)
    )
    val wrongPolicy = policy.copy(captureLimit = ByteLimit.unsafeFrom(2048))
    val result = InvocationResult.Exited(0, evidence.primary, evidence.primary)

    for
      executor <- ScriptedCommandExecutor.create[IO](
        Vector(ExpectedCommand.exact(command, policy, result))
      )
      mismatch <- executor.execute(command, wrongPolicy).attempt
      accepted <- executor.execute(command, policy)
      observed <- executor.observed
    yield
      assert(mismatch.left.exists(_.getMessage.contains("policy did not match")))
      assertEquals(accepted, result)
      assertEquals(
        observed,
        Vector(ObservedCommand(command, wrongPolicy), ObservedCommand(command, policy))
      )
  }

  test("scripted log reader matches the full read contract and records calls") {
    val ref = LogRef(
      AttemptId.unsafeFrom("attempt"),
      AttemptEpoch.initial,
      LogStream.Stdout,
      "/work/stdout"
    )
    val limit = ByteLimit.unsafeFrom(128)
    val call = LogReadCall(ref, LogCursor.start, limit)
    val result = LogReadResult.WaitingForFile(LogCursor.start, observedAt)

    for
      reader <- ScriptedLogReader.create[IO](Vector(ExpectedLogRead(call, result)))
      actual <- reader.read(ref, LogCursor.start, limit)
      observed <- reader.observed
      remaining <- reader.remaining
    yield
      assertEquals(actual, result)
      assertEquals(observed, Vector(call))
      assertEquals(remaining, Vector.empty)
  }
