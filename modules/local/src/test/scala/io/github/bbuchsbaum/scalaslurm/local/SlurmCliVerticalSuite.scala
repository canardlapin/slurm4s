package io.github.bbuchsbaum.scalaslurm.local

import cats.data.NonEmptyVector
import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.cli.*
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.testkit.ExpectedCommand
import io.github.bbuchsbaum.scalaslurm.testkit.ScriptedCommandExecutor

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

class SlurmCliVerticalSuite extends munit.CatsEffectSuite:
  private val parser = DataParserVersion.from("v0.0.43").toOption.get
  private val settings = SlurmCliSettings(parser, LocalTestSupport.policy())

  test("local opaque script runs submit, observe, logs, accounting, and terminal diagnosis") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024 * 1024).toOption.get)
      )
      val request = LocalTestSupport.request("vertical-opaque")

      for
        prepared <- planner.prepareLocal(request).map(_.toOption.get)
        job = JobRef(
          JobId.from("1001").toOption.get,
          Some(ClusterName.from("alpha").toOption.get),
          None
        )
        jobs = NonEmptyVector.one(job)
        executor <- ScriptedCommandExecutor.create[IO](
          Vector(
            ExpectedCommand.exact(
              SlurmCommands.submit(prepared.submission),
              exited(SlurmExecutable.Sbatch, "1001;alpha\n")
            ),
            ExpectedCommand.exact(
              SlurmCommands.observe(jobs, parser),
              exited(
                SlurmExecutable.Squeue,
                """{"meta":{"data_parser":"v0.0.43"},"jobs":[{"job_id":1001,"cluster":"alpha","job_state":["PENDING"],"state_reason":"Priority"}]}"""
              )
            ),
            ExpectedCommand.exact(
              SlurmCommands.accounting(jobs),
              exited(SlurmExecutable.Sacct, "1001|OUT_OF_MEMORY|0:9|OutOfMemory\n")
            ),
            ExpectedCommand.exact(
              SlurmCommands.focused(job),
              exited(
                SlurmExecutable.Scontrol,
                "JobId=1001 JobState=OUT_OF_MEMORY StartTime=2026-07-23T10:00:00 EndTime=2026-07-23T10:15:00 TimeLimit=00:15:00 Reason=Out of memory on node alpha\n"
              )
            ),
            ExpectedCommand.exact(
              SlurmCommands.cancel(job),
              exited(SlurmExecutable.Scancel, "")
            )
          )
        )
        scheduler = SlurmCliScheduler[IO](executor, planner, settings)
        submitted <- scheduler.submit(request)
        _ = submitted match
          case SubmissionAttempt.Completed(Submission.Accepted(bound, _)) =>
            assertEquals(bound, job)
          case other => fail(s"unexpected submission: $other")
        observed <- scheduler.observe(jobs)
        _ = observed match
          case SchedulerQueryResult.Succeeded(batch) =>
            val observation = batch.results.head.asInstanceOf[ObservationResult.Observed].value
            assertEquals(observation.state, SlurmState.Pending)
          case other => fail(s"unexpected observation: $other")
        _ <- IO.blocking(
          Files.write(
            java.nio.file.Path.of(prepared.stdout.locator),
            "model started\n".getBytes(StandardCharsets.UTF_8)
          )
        )
        log <- LocalLogReader[IO](root).read(
          prepared.stdout,
          LogCursor.start,
          ByteLimit.from(1024).toOption.get
        )
        _ = log match
          case LogReadResult.Page(page) =>
            assertEquals(String(page.bytes.toArray, StandardCharsets.UTF_8), "model started\n")
          case other => fail(s"unexpected log result: $other")
        accounted <- scheduler.accounting(jobs)
        _ = accounted match
          case SchedulerQueryResult.Succeeded(batch) =>
            assertEquals(batch.records.head.outcome, Some(WorkloadOutcome.OutOfMemory))
          case other => fail(s"unexpected accounting: $other")
        focused <- scheduler.focused(job)
        _ = focused match
          case SchedulerQueryResult.Succeeded(value) =>
            assertEquals(value.fields.get("Reason"), Some("Out of memory on node alpha"))
            assertEquals(
              value.timing.timeLimit,
              ObservedTimeLimit.Limited(WallTimeMinutes.from(15).toOption.get)
            )
          case other => fail(s"unexpected focused diagnostic: $other")
        cancelled <- scheduler.cancel(job)
        _ = assert(cancelled.isInstanceOf[CancellationAttempt.Completed])
        remaining <- executor.remaining
        _ = assertEquals(remaining, Vector.empty)
      yield ()
    }
  }

  test("capability discovery preserves probe evidence and explicit parser availability") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024).toOption.get)
      )
      for
        executor <- ScriptedCommandExecutor.create[IO](
          Vector(
            ExpectedCommand.exact(
              SlurmCommands.versionProbe,
              exited(SlurmExecutable.Sbatch, "slurm 25.05.3\n")
            ),
            ExpectedCommand.exact(
              SlurmCommands.parserProbe,
              exited(SlurmExecutable.Squeue, "v0.0.42\nv0.0.43\n")
            ),
            ExpectedCommand.exact(
              SlurmCommands.accountingProbe,
              exited(SlurmExecutable.Sacct, "slurm 25.05.3\n")
            ),
            ExpectedCommand.exact(
              SlurmCommands.configProbe,
              exited(SlurmExecutable.Scontrol, "ClusterName = alpha\nMaxArraySize = 1001\n")
            )
          )
        )
        result <- SlurmCliScheduler[IO](executor, planner, settings).capabilities
        _ = result match
          case SchedulerQueryResult.Succeeded(capabilities) =>
            assertEquals(capabilities.slurmVersion, Some("25.05.3"))
            assertEquals(capabilities.cluster.map(_.value), Some("alpha"))
            assertEquals(capabilities.structuredQueue, CapabilitySupport.Supported)
            assertEquals(capabilities.accounting, CapabilitySupport.Supported)
            assertEquals(capabilities.arrays, CapabilitySupport.Supported)
            assertEquals(capabilities.rawEvidence.length, 8)
          case other => fail(s"unexpected capabilities: $other")
      yield ()
    }
  }

  test(
    "rejection, invocation failure, parse failure, empty query, stale data, and workload failure stay distinct"
  ) {
    LocalTestSupport.temporaryDirectory.use { root =>
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024 * 1024).toOption.get)
      )
      val request = LocalTestSupport.request("failure-planes")
      for
        prepared <- planner.prepareLocal(request).map(_.toOption.get)
        job = JobRef(JobId.from("1001").toOption.get, None, None)
        jobs = NonEmptyVector.one(job)
        job2 = JobRef(JobId.from("1002").toOption.get, None, None)
        multiJobs = NonEmptyVector.of(job, job2)
        spawn = InvocationResult.SpawnFailed(
          SpawnFailureKind.ExecutableMissing,
          Diagnostics.one(Diagnostic("missing", "sbatch is missing")),
          EvidenceBundle(evidence(SlurmExecutable.Sbatch, ""))
        )
        executor <- ScriptedCommandExecutor.create[IO](
          Vector(
            ExpectedCommand.exact(
              SlurmCommands.submit(prepared.submission),
              exited(SlurmExecutable.Sbatch, "", "invalid account", exitCode = 1)
            ),
            ExpectedCommand.exact(SlurmCommands.submit(prepared.submission), spawn),
            ExpectedCommand.exact(
              SlurmCommands.observe(jobs, parser),
              exited(SlurmExecutable.Squeue, "not-json")
            ),
            ExpectedCommand.exact(
              SlurmCommands.observe(jobs, parser),
              exited(SlurmExecutable.Squeue, "{\"jobs\":[]}")
            ),
            ExpectedCommand.exact(
              SlurmCommands.observe(multiJobs, parser),
              exited(
                SlurmExecutable.Squeue,
                """{"jobs":[{"job_id":1001,"job_state":"RUNNING"}]}"""
              )
            ),
            ExpectedCommand.exact(
              SlurmCommands.accounting(jobs),
              exited(SlurmExecutable.Sacct, "1001|FAILED|2:0|NonZeroExitCode\n")
            ),
            ExpectedCommand.exact(
              SlurmCommands.cancel(job),
              InvocationResult.TimedOut(
                DurationMillis.from(1000).toOption.get,
                evidence(SlurmExecutable.Scancel, ""),
                evidence(SlurmExecutable.Scancel, "", stdout = false)
              )
            )
          )
        )
        scheduler = SlurmCliScheduler[IO](executor, planner, settings)
        rejected <- scheduler.submit(request)
        invocation <- scheduler.submit(request)
        parseFailure <- scheduler.observe(jobs)
        empty <- scheduler.observe(jobs)
        partial <- scheduler.observe(multiJobs)
        workload <- scheduler.accounting(jobs)
        cancellation <- scheduler.cancel(job)
        staleAge = DurationMillis.from(5000).toOption.get
        current = JobObservation(
          job,
          SlurmState.Pending,
          Freshness.Current(Instant.EPOCH),
          None,
          Map.empty,
          EvidenceBundle(evidence(SlurmExecutable.Squeue, ""))
        )
        stale = ObservationSemantics.markStale(current, staleAge)
        _ = assert(rejected match
          case SubmissionAttempt.Completed(_: Submission.Rejected) => true
          case _                                                   => false)
        _ = assert(invocation.isInstanceOf[SubmissionAttempt.InvocationFailed])
        _ = assert(parseFailure match
          case SchedulerQueryResult.ParseFailed(_, _) => true
          case _                                      => false)
        _ = assert(empty match
          case SchedulerQueryResult.Empty(_, _) => true
          case _                                => false)
        _ = partial match
          case SchedulerQueryResult.Succeeded(batch) =>
            assertEquals(batch.results.length, 2)
            assert(batch.results.toVector.exists(_.isInstanceOf[ObservationResult.NotFound]))
          case other => fail(s"unexpected partial observation: $other")
        _ = assert(stale.freshness.isInstanceOf[Freshness.Stale])
        _ = workload match
          case SchedulerQueryResult.Succeeded(batch) =>
            assert(batch.records.head.outcome.exists(_.isInstanceOf[WorkloadOutcome.Failed]))
          case other => fail(s"unexpected workload result: $other")
        _ = cancellation match
          case CancellationAttempt.Completed(_: CancellationResult.Unknown) => ()
          case other => fail(s"unexpected cancellation result: $other")
      yield ()
    }
  }

  private def exited(
      executable: SlurmExecutable,
      stdout: String,
      stderr: String = "",
      exitCode: Int = 0
  ): InvocationResult =
    InvocationResult.Exited(
      exitCode,
      evidence(executable, stdout, stdout = true),
      evidence(executable, stderr, stdout = false)
    )

  private def evidence(
      executable: SlurmExecutable,
      text: String,
      stdout: Boolean = true
  ): BoundedEvidence =
    BoundedEvidence.capture(
      if stdout then EvidenceSource.CommandStdout(executable.fileName)
      else EvidenceSource.CommandStderr(executable.fileName),
      Instant.parse("2026-07-22T12:00:00Z"),
      text.getBytes(StandardCharsets.UTF_8).toVector
    )
