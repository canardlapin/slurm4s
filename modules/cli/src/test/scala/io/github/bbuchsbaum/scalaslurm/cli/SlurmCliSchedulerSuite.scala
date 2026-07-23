package io.github.bbuchsbaum.scalaslurm.cli

import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.scalaslurm.core.*

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
      result <- scheduler.submitAt(request, profile, SiteIntent())
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
      result <- scheduler.submitAt(jobRequest, profile, SiteIntent())
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
      def prepare[A](request: JobRequest[A]): IO[Either[Diagnostics, PreparedSubmission[A]]] =
        counter
          .update(_ + 1)
          .as(
            Right(PreparedSubmission(request, "/work/job.sh", "/work/stdout", "/work/stderr"))
          )

  private def recordingExecutor(commands: Ref[IO, Vector[SlurmCommand]]): CommandExecutor[IO] =
    new CommandExecutor[IO]:
      def execute(command: SlurmCommand, policy: CommandPolicy): IO[InvocationResult] =
        commands.update(_ :+ command).as(InvocationResult.Exited(1, evidence, evidence))

  private def token(value: String): SiteToken = SiteToken.from("test", value).toOption.get
