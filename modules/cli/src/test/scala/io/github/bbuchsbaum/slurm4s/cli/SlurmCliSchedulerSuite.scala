package io.github.bbuchsbaum.slurm4s.cli

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant

class SlurmCliSchedulerSuite extends munit.CatsEffectSuite:
  private val commandPolicy = CommandPolicy(
    DurationMillis.from(1000).toOption.get,
    ByteLimit.defaultEvidence
  )
  private val settings = SlurmCliSettings(
    DataParserVersion.from("v0.0.43").toOption.get,
    commandPolicy
  )
  private val evidence = BoundedEvidence.capture(
    EvidenceSource.CommandStdout("test"),
    Instant.parse("2026-07-22T12:00:00Z"),
    Vector.empty
  )

  test("a single job that has left the queue is absent, not an invocation failure") {
    // `squeue -j` exits 1 with this diagnostic once a job is no longer queued, which is how every
    // job ends. Treating that as an invocation failure would make the most ordinary poll in the
    // library look like a broken transport. With one job named, it is the one squeue rejected.
    val jobs = NonEmptyVector.one(jobRef("5001"))
    val scheduler = SlurmCliScheduler[IO](
      fixedExecutor(
        InvocationResult.Exited(
          1,
          bytes(""),
          bytes("slurm_load_jobs error: Invalid job id specified\n")
        )
      ),
      recordingPlanner(),
      settings
    )

    scheduler.observe(jobs).map {
      case SchedulerQueryResult.Succeeded(batch) =>
        assertEquals(
          batch.results.toVector.collect { case ObservationResult.NotFound(job, _, _) =>
            job.jobId.value
          },
          Vector("5001")
        )
      case other => fail(s"a departed job must be reported as absent, got $other")
    }
  }

  test("an unreadable multi-job absence response never fabricates absence") {
    // If squeue refuses the whole query, the queued jobs among those named are still running.
    // Declaring them absent would be a fabricated answer; the honest report is that the response
    // could not be interpreted.
    val jobs = NonEmptyVector.of(jobRef("5006"), jobRef("5007"))
    val scheduler = SlurmCliScheduler[IO](
      fixedExecutor(
        InvocationResult.Exited(
          1,
          bytes(""),
          bytes("slurm_load_jobs error: Invalid job id specified\n")
        )
      ),
      recordingPlanner(),
      settings
    )

    scheduler.observe(jobs).map {
      case SchedulerQueryResult.ParseFailed(diagnostics, _) =>
        assertEquals(diagnostics.toVector.map(_.code), Vector("squeue-absence-ambiguous"))
        assertEquals(diagnostics.toVector.head.fields.get("requestedJobs"), Some("2"))
      case other =>
        fail(s"an uninterpretable multi-job response must not claim absence, got $other")
    }
  }

  test("a readable multi-job response may report every named job absent") {
    // A readable response names everything still queued, so an empty list plus the diagnostic does
    // account for all of them.
    val jobs = NonEmptyVector.of(jobRef("5008"), jobRef("5009"))
    val scheduler = SlurmCliScheduler[IO](
      fixedExecutor(
        InvocationResult.Exited(
          1,
          bytes("""{"meta":{"data_parser":"v0.0.43"},"jobs":[]}"""),
          bytes("slurm_load_jobs error: Invalid job id specified\n")
        )
      ),
      recordingPlanner(),
      settings
    )

    scheduler.observe(jobs).map {
      case SchedulerQueryResult.Succeeded(batch) =>
        assertEquals(
          batch.results.toVector.collect { case ObservationResult.NotFound(job, _, _) =>
            job.jobId.value
          },
          Vector("5008", "5009")
        )
      case other => fail(s"a readable empty queue must report absence, got $other")
    }
  }

  test("an unrecognized squeue failure stays an invocation failure") {
    val jobs = NonEmptyVector.one(jobRef("5003"))
    val scheduler = SlurmCliScheduler[IO](
      fixedExecutor(
        InvocationResult.Exited(
          1,
          bytes(""),
          bytes("slurm_load_jobs error: Unable to contact slurm controller\n")
        )
      ),
      recordingPlanner(),
      settings
    )

    scheduler.observe(jobs).map { result =>
      assert(
        result.isInstanceOf[SchedulerQueryResult.InvocationFailed[?]],
        s"an unrecognized failure must not be read as absence, got $result"
      )
    }
  }

  test("jobs squeue still returned survive a partial invalid-job-id response") {
    val jobs = NonEmptyVector.of(jobRef("5004"), jobRef("5005"))
    val queued =
      """{"meta":{"data_parser":"v0.0.43"},"jobs":[{"job_id":5004,"job_state":["RUNNING"]}]}"""
    val scheduler = SlurmCliScheduler[IO](
      fixedExecutor(
        InvocationResult.Exited(
          1,
          bytes(queued),
          bytes("slurm_load_jobs error: Invalid job id specified\n")
        )
      ),
      recordingPlanner(),
      settings
    )

    scheduler.observe(jobs).map {
      case SchedulerQueryResult.Succeeded(batch) =>
        val observed = batch.results.toVector.collect { case ObservationResult.Observed(value) =>
          value.job.jobId.value -> value.state
        }
        val absent = batch.results.toVector.collect { case ObservationResult.NotFound(job, _, _) =>
          job.jobId.value
        }
        assertEquals(observed, Vector("5004" -> SlurmState.Running))
        assertEquals(absent, Vector("5005"))
      case other => fail(s"expected a mixed batch, got $other")
    }
  }

  test("site preflight rejection performs no staging or command invocation") {
    val request = jobRequest.copy(
      array = Some(
        JobArrayRequest.contiguous(PositiveInt.from("size", 2).toOption.get, None).toOption.get
      )
    )
    val profile = SiteProfile(
      site = token("disabled-array-site"),
      features = SiteFeatures(arrays = false)
    )
    for
      commands <- Ref.of[IO, Vector[SlurmCommand]](Vector.empty)
      preparations <- Ref.of[IO, Int](0)
      scheduler = SlurmCliScheduler[IO](
        recordingExecutor(commands),
        recordingPlanner(preparations),
        settings
      )
      result <- scheduler.submitAt(
        LaunchSpec.fromRequest(request).toOption.get,
        profile,
        SiteIntent()
      )
      invoked <- commands.get
      prepared <- preparations.get
      _ = assert(result.isInstanceOf[SiteSubmissionResult.PreflightRejected])
      _ = assertEquals(invoked, Vector.empty)
      _ = assertEquals(prepared, 0)
    yield ()
  }

  test("accepted site resolution is returned and applied to the sbatch argv") {
    val account = token("research")
    val profile = SiteProfile(
      site = token("profiled-site"),
      defaultAccount = Some(account),
      allowedAccounts = Some(Set(account)),
      accountRequired = true
    )
    for
      commands <- Ref.of[IO, Vector[SlurmCommand]](Vector.empty)
      preparations <- Ref.of[IO, Int](0)
      scheduler = SlurmCliScheduler[IO](
        recordingExecutor(commands),
        recordingPlanner(preparations),
        settings
      )
      result <- scheduler.submitAt(
        LaunchSpec.fromRequest(jobRequest).toOption.get,
        profile,
        SiteIntent()
      )
      invoked <- commands.get
      prepared <- preparations.get
      _ = result match
        case SiteSubmissionResult.Attempted(resolution, _) =>
          assertEquals(resolution.requestedResources, jobRequest.resources)
          assertEquals(resolution.effective.account, Some(account))
        case other => fail(s"expected attempted submission, received $other")
      _ = assertEquals(prepared, 1)
      _ = assertEquals(invoked.size, 1)
      _ = assert(invoked.head.arguments.contains("--account=research"))
      _ = assert(invoked.head.arguments.contains("--export=NONE"))
    yield ()
  }

  private val jobRequest: JobRequest[NoResult] = JobRequest(
    SubmissionKey.from("profiled-submit").toOption.get,
    JobName.from("profiled-submit").toOption.get,
    Payload.Script(
      ScriptSource.ExistingRemote("/work/job.sh"),
      Vector.empty,
      ResultContract.ExitOnly
    ),
    ResourceRequest.validate(1, 1, None, None, None).toOption.get
  )

  private def recordingPlanner(counter: Ref[IO, Int]): SubmissionPlanner[IO] =
    new SubmissionPlanner[IO]:
      def prepare(spec: LaunchSpec): IO[Either[Diagnostics, PreparedSubmission]] =
        counter
          .update(_ + 1)
          .as(
            Right(PreparedSubmission(spec, "/work/job.sh", "/work/stdout", "/work/stderr"))
          )

  private def recordingExecutor(commands: Ref[IO, Vector[SlurmCommand]]): CommandExecutor[IO] =
    new CommandExecutor[IO]:
      def execute(command: SlurmCommand, policy: CommandPolicy): IO[InvocationResult] =
        commands.update(_ :+ command).as(InvocationResult.Exited(1, evidence, evidence))

  private def token(value: String): SiteToken = SiteToken.from("test", value).toOption.get

  private def jobRef(id: String): JobRef = JobRef(JobId.from(id).toOption.get, None, None)

  private def bytes(text: String): BoundedEvidence =
    BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      Instant.parse("2026-07-25T12:00:00Z"),
      text.getBytes(java.nio.charset.StandardCharsets.UTF_8).toVector
    )

  private def recordingPlanner(): SubmissionPlanner[IO] =
    new SubmissionPlanner[IO]:
      def prepare(spec: LaunchSpec): IO[Either[Diagnostics, PreparedSubmission]] =
        IO.pure(Right(PreparedSubmission(spec, "/work/job.sh", "/work/stdout", "/work/stderr")))

  private def fixedExecutor(result: InvocationResult): CommandExecutor[IO] =
    new CommandExecutor[IO]:
      def execute(command: SlurmCommand, policy: CommandPolicy): IO[InvocationResult] =
        IO.pure(result)
