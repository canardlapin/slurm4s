package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.IO
import fs2.Chunk
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.agent.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*

import scodec.bits.ByteVector

import java.time.Instant

class SshAgentConformanceSuite extends munit.CatsEffectSuite:
  test("local scheduler and framed SSH agent pass the same scheduler scenarios") {
    val local = deterministicScheduler
    val service = AgentService[IO](local, deterministicLogs)
    val schedulerHandler = SchedulerRequestHandler[IO](service)
    val server = AgentStdioServer[IO](ServiceRequestHandler[IO](service, schedulerHandler))
    val runner = LoopbackSshRunner(server)
    val wire = wireClient(runner)

    for
      connected <- SshAgentApi.connect[IO](wire)
      remote = connected match
        case AgentCall.Succeeded(value) => value
        case AgentCall.Failed(failure)  => throw new AssertionError(failure.toString)
      localTrace <- localScenario(local)
      remoteTrace <- remoteScenario(remote)
      _ = assertEquals(remoteTrace, localTrace)
    yield ()
  }

  test("remote log paging resumes byte-exactly across independent SSH processes") {
    val service = AgentService[IO](deterministicScheduler, deterministicLogs)
    val server = AgentStdioServer[IO](
      ServiceRequestHandler[IO](service, SchedulerRequestHandler[IO](service))
    )
    val runner = LoopbackSshRunner(server)
    val wire = wireClient(runner)

    for
      connected <- SshAgentApi.connect[IO](wire)
      remote = connected.asInstanceOf[AgentCall.Succeeded[SshAgentApi[IO]]].value
      first <- remote.readLog(logRef, LogCursor.start, ByteLimit.from(3).toOption.get)
      firstPage = logPage(first)
      second <- remote.readLog(logRef, firstPage.next, ByteLimit.from(3).toOption.get)
      secondPage = logPage(second)
      _ = assertEquals(new String(firstPage.bytes.toArray, "UTF-8"), "abc")
      _ = assertEquals(new String(secondPage.bytes.toArray, "UTF-8"), "def")
      _ = assertEquals(secondPage.next.offset.value, 6L)
      _ = assertEquals(runner.exchangeCount, 3)
    yield ()
  }

  test("negotiated log-page maximum fits the wire and oversized requests stay local") {
    val frameLimit = ByteLimit.from(64 * 1024).toOption.get
    val largeLogs = AgentLogReader[IO] { (_, cursor, maximum) =>
      IO.pure(
        LogReadResult.Page(
          LogPage(
            ByteVector.fill(maximum.value.toLong)(0xff.toByte),
            cursor,
            endOfFile = false,
            observedAt
          )
        )
      )
    }
    val config = AgentServiceConfig.default.copy(maximumFrameBytes = frameLimit)
    val service = AgentService[IO](
      deterministicScheduler,
      largeLogs,
      config = config
    )
    val server = AgentStdioServer[IO](
      ServiceRequestHandler[IO](service, SchedulerRequestHandler[IO](service)),
      FrameLimits(frameLimit)
    )
    val runner = LoopbackSshRunner(server)
    val wire = SshAgentWireClient(
      SshCommand
        .agent(
          SshConnection(
            "/usr/bin/ssh",
            SshTarget.from("loopback-cluster").toOption.get
          )
        )
        .toOption
        .get,
      runner,
      FrameLimits(frameLimit),
      SshExchangePolicy(DurationMillis.from(5000).toOption.get)
    )

    for
      connected <- SshAgentApi.connect[IO](wire, maximumFrameBytes = frameLimit)
      remote = connected.asInstanceOf[AgentCall.Succeeded[SshAgentApi[IO]]].value
      pageLimit = remote.handshake.maximumLogPageBytes.get
      boundary <- remote.readLog(logRef, LogCursor.start, pageLimit)
      exchangesAfterBoundary = runner.exchangeCount
      oversized <- remote.readLog(
        logRef,
        LogCursor.start,
        ByteLimit.from(pageLimit.value + 1).toOption.get
      )
      _ = assertEquals(logPage(boundary).bytes.size, pageLimit.value.toLong)
      _ = assert(oversized.isInstanceOf[AgentCall.Failed[?]])
      _ = assertEquals(runner.exchangeCount, exchangesAfterBoundary)
    yield ()
  }

  private def localScenario(scheduler: Scheduler[IO]): IO[SchedulerTrace] =
    for
      capabilities <- scheduler.capabilities
      submission <- scheduler.submit(request)
      observations <- scheduler.observe(NonEmptyVector.one(job))
      accounting <- scheduler.accounting(NonEmptyVector.one(job))
      cancellation <- scheduler.cancel(job)
    yield SchedulerTrace(capabilities, submission, observations, accounting, cancellation)

  private def remoteScenario(remote: AgentApi[IO]): IO[SchedulerTrace] =
    for
      capabilities <- unwrap(remote.capabilities)
      submission <- unwrap(remote.submitOpaque(request))
      observations <- unwrap(remote.observe(NonEmptyVector.one(job)))
      accounting <- unwrap(remote.accounting(NonEmptyVector.one(job)))
      cancellation <- unwrap(remote.cancel(job))
    yield SchedulerTrace(capabilities, submission, observations, accounting, cancellation)

  private def unwrap[A](call: IO[AgentCall[A]]): IO[A] = call.flatMap {
    case AgentCall.Succeeded(value) => IO.pure(value)
    case AgentCall.Failed(failure)  => IO.raiseError(new AssertionError(failure.toString))
  }

  private def logPage(call: AgentCall[LogReadResult]): LogPage = call match
    case AgentCall.Succeeded(LogReadResult.Page(value)) => value
    case other => throw new AssertionError(s"expected log page, received $other")

  private def wireClient(runner: SshProcessRunner[IO]): SshAgentWireClient[IO] =
    val target = SshTarget.from("loopback-cluster").toOption.get
    val launch = SshCommand.agent(SshConnection("/usr/bin/ssh", target)).toOption.get
    SshAgentWireClient(
      launch,
      runner,
      FrameLimits.default,
      SshExchangePolicy(DurationMillis.from(5000).toOption.get)
    )

  private val observedAt = Instant.parse("2026-07-22T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, ByteVector(1, 2, 3))
  )
  private val job = JobRef(JobId.from("8182").toOption.get, None)
  private val freshness = Freshness.Current(observedAt)
  private val observation = JobObservation(
    job,
    SlurmState.Running,
    freshness,
    Some("None"),
    Map("State" -> "RUNNING"),
    evidence
  )
  private val accountingRecord = AccountingRecord(
    job,
    SlurmState.Completed,
    Some(ExitStatus(0, None)),
    Some(WorkloadOutcome.Completed(0)),
    freshness,
    Map("State" -> "COMPLETED"),
    evidence
  )
  private val schedulerCapabilities = SchedulerCapabilities(
    Some("24.11.5"),
    None,
    CapabilitySupport.Supported,
    CapabilitySupport.Supported,
    CapabilitySupport.Supported,
    CapabilitySupport.Supported,
    Vector(evidence.primary)
  )
  private val request = LaunchSpec(
    SubmissionKey.from("submission-ssh-1").toOption.get,
    JobName.from("opaque-remote").toOption.get,
    ScriptSource
      .unsafeInlineScript("job.sh", ByteVector.view("#!/bin/sh\ntrue\n".getBytes("UTF-8"))),
    Vector("one", "two"),
    ResultContract.ExitOnly.descriptor,
    ResourceRequest.validate(2, 1, None, None, None).toEither.toOption.get,
    Map(EnvName.unsafeFrom("LANG") -> "C")
  )

  private val deterministicScheduler: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.pure(SchedulerQueryResult.Succeeded(schedulerCapabilities))

    def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
      IO.pure(SubmissionAttempt.Completed(Submission.Accepted(job, evidence)))

    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] =
      IO.pure(
        SchedulerQueryResult.Succeeded(
          ObservationBatch(NonEmptyVector.one(ObservationResult.Observed(observation)))
        )
      )

    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] =
      IO.pure(
        SchedulerQueryResult.Succeeded(
          AccountingBatch(NonEmptyVector.one(accountingRecord), Vector.empty)
        )
      )

    def cancel(job: JobRef): IO[CancellationAttempt] =
      IO.pure(CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)))

  private val logBytes = ByteVector.view("abcdef".getBytes("UTF-8"))
  private val logIdentity = FileIdentity.from("ssh-log-file").toOption.get
  private val logRef = LogRef(
    AttemptId.from("ssh-attempt").toOption.get,
    AttemptEpoch.initial,
    LogStream.Stdout,
    "stdout.log"
  )
  private val deterministicLogs = AgentLogReader[IO] { (_, cursor, maximum) =>
    val start = cursor.offset.value.toInt
    val bytes = logBytes.slice(start, start + maximum.value)
    val nextOffset = LogOffset.from(start.toLong + bytes.size).toOption.get
    val next = LogCursor(nextOffset, Some(logIdentity))
    IO.pure(LogReadResult.Page(LogPage(bytes, next, nextOffset.value == logBytes.size, observedAt)))
  }

final private case class SchedulerTrace(
    capabilities: SchedulerQueryResult[SchedulerCapabilities],
    submission: SubmissionAttempt,
    observations: SchedulerQueryResult[ObservationBatch],
    accounting: SchedulerQueryResult[AccountingBatch],
    cancellation: CancellationAttempt
)

final private class LoopbackSshRunner(server: AgentStdioServer[IO]) extends SshProcessRunner[IO]:
  private var count = 0
  def exchangeCount: Int = count

  def exchange(
      launch: SshLaunch,
      request: ByteVector,
      policy: SshExchangePolicy
  ): IO[SshProcessOutcome] =
    count += 1
    Stream
      .chunk(Chunk.byteVector(request))
      .covary[IO]
      .chunkN(3)
      .flatMap(Stream.chunk)
      .through(server.pipe)
      .compile
      .to(ByteVector)
      .map { response =>
        val now = Instant.parse("2026-07-22T12:00:00Z")
        SshProcessOutcome.Exited(
          0,
          requestWriteCompleted = true,
          BoundedEvidence.capture(EvidenceSource.CommandStdout("ssh"), now, response),
          BoundedEvidence.capture(EvidenceSource.CommandStderr("ssh"), now, ByteVector.empty)
        )
      }
