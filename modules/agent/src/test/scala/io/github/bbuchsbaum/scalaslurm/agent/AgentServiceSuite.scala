package io.github.bbuchsbaum.scalaslurm.agent

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.AgentFailure

import java.time.Instant

class AgentServiceSuite extends munit.CatsEffectSuite:
  test("disconnect after scheduler acceptance never implies cancellation") {
    for
      cancellations <- Ref.of[IO, Int](0)
      service = AgentService[IO](scheduler(cancellations), logReader)
      client <- connected(service)
      submitted <- client.submitOpaque(request)
      _ = assert(submitted.isInstanceOf[AgentCall.Succeeded[?]])
      _ <- client.disconnect
      count <- cancellations.get
      disconnected <- client.capabilities
      _ = assertEquals(count, 0)
      _ = assert(
        disconnected == AgentCall.Failed(
          AgentFailure.TransportDisconnected(
            afterRequestWrite = false,
            diagnostic = "agent session is disconnected",
            evidence = None
          )
        )
      )
    yield ()
  }

  test("a new connection resumes logs from the caller supplied byte cursor") {
    val service = AgentService[IO](schedulerWithoutCounters, logReader)
    for
      firstClient <- connected(service)
      first <- firstClient.readLog(logRef, LogCursor.start, ByteLimit.from(3).toOption.get)
      firstPage = page(first)
      _ <- firstClient.disconnect
      secondClient <- connected(service)
      second <- secondClient.readLog(logRef, firstPage.next, ByteLimit.from(3).toOption.get)
      secondPage = page(second)
      _ = assertEquals(new String(firstPage.bytes.toArray, "UTF-8"), "abc")
      _ = assertEquals(new String(secondPage.bytes.toArray, "UTF-8"), "def")
      _ = assertEquals(secondPage.next.offset.value, 6L)
    yield ()
  }

  test("protocol mismatch is a distinct handshake failure") {
    val service = AgentService[IO](schedulerWithoutCounters, logReader)
    val incompatible = ProtocolVersion.from(2, 0).toOption.get

    InProcessAgentClient.connect[IO](service, clientProtocol = incompatible).map { result =>
      assertEquals(result, AgentCall.Failed(AgentFailure.ProtocolMismatch(2, 1)))
    }
  }

  private def connected(service: AgentService[IO]): IO[InProcessAgentClient[IO]] =
    InProcessAgentClient.connect[IO](service).flatMap {
      case AgentCall.Succeeded(client) => IO.pure(client)
      case AgentCall.Failed(failure)   => IO.raiseError(new AssertionError(failure.toString))
    }

  private def page(result: AgentCall[LogReadResult]): LogPage = result match
    case AgentCall.Succeeded(LogReadResult.Page(value)) => value
    case other => throw new AssertionError(s"expected log page, received $other")

  private val request: JobRequest[NoResult] = JobRequest(
    SubmissionKey.from("submission-1").toOption.get,
    JobName.from("opaque-script").toOption.get,
    Payload.Script(
      ScriptSource.Inline("job.sh", "#!/bin/sh\ntrue\n".getBytes("UTF-8").toVector),
      Vector.empty,
      ResultContract.ExitOnly
    ),
    ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
  )

  private val acceptedJob = JobRef(JobId.from("42").toOption.get, None, None)
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, Instant.EPOCH, Vector.empty)
  )

  private def scheduler(cancellations: Ref[IO, Int]): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.raiseError(new AssertionError("capabilities should not be called while disconnected"))

    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] =
      IO.pure(SubmissionAttempt.Completed(Submission.Accepted(acceptedJob, evidence)))

    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] =
      IO.raiseError(new AssertionError("observe not used"))

    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] =
      IO.raiseError(new AssertionError("accounting not used"))

    def cancel(job: JobRef): IO[CancellationAttempt] =
      cancellations.update(_ + 1) *>
        IO.pure(CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)))

  private val schedulerWithoutCounters: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.raiseError(new AssertionError("capabilities not used"))

    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] =
      IO.pure(SubmissionAttempt.Completed(Submission.Accepted(acceptedJob, evidence)))

    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] =
      IO.raiseError(new AssertionError("observe not used"))

    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] =
      IO.raiseError(new AssertionError("accounting not used"))

    def cancel(job: JobRef): IO[CancellationAttempt] =
      IO.pure(CancellationAttempt.Completed(CancellationResult.Acknowledged(evidence)))

  private val logRef = LogRef(
    AttemptId.from("attempt-1").toOption.get,
    AttemptEpoch.initial,
    LogStream.Stdout,
    "stdout.log"
  )
  private val logBytes = "abcdef".getBytes("UTF-8").toVector
  private val identity = FileIdentity.from("stable-file").toOption.get

  private val logReader: AgentLogReader[IO] = AgentLogReader { (_, cursor, maximum) =>
    val start = cursor.offset.value.toInt
    val bytes = logBytes.slice(start, start + maximum.value)
    val next = LogCursor(LogOffset.from(start.toLong + bytes.size).toOption.get, Some(identity))
    IO.pure(
      LogReadResult.Page(LogPage(bytes, next, next.offset.value == logBytes.size, Instant.EPOCH))
    )
  }
