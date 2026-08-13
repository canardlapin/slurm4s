package io.github.bbuchsbaum.slurm4s.agent

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFeature
import io.github.bbuchsbaum.slurm4s.protocol.AgentFrameBudget
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import io.github.bbuchsbaum.slurm4s.protocol.HandshakeRequest
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredSubmission
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchRequest
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchSubmission
import io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredTaskRequest
import io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRead
import io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRef

import scodec.bits.ByteVector

import java.time.Instant

class AgentServiceSuite extends munit.CatsEffectSuite:
  test("disconnect after scheduler acceptance never implies cancellation") {
    for
      cancellations <- Ref.of[IO, Int](0)
      service = AgentService[IO](scheduler(cancellations), logReader)
      client <- connected(service)
      submitted <- client.submitOpaque(LaunchSpec.fromRequest(request).toOption.get)
      _ = assert(submitted.isInstanceOf[AgentCall.Succeeded[?]])
      _ <- client.disconnect
      count <- cancellations.get
      disconnected <- client.capabilities
      _ = assertEquals(count, 0)
      _ = disconnected match
        case AgentCall.Failed(
              AgentFailure.TransportDisconnected(false, diagnostic, None)
            ) =>
          assertEquals(diagnostic, "agent session is disconnected")
        case other => fail(s"unexpected disconnected call: $other")
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

  test("handshake advertises and service enforces the safe raw log-page budget") {
    val frameLimit = ByteLimit.from(16 * 1024).toOption.get
    val pageLimit = AgentFrameBudget.maximumLogPageBytes(frameLimit).get
    for
      reads <- Ref.of[IO, Int](0)
      reader = AgentLogReader[IO] { (_, cursor, maximum) =>
        reads.update(_ + 1) *>
          IO.pure(
            LogReadResult.Page(
              LogPage(
                ByteVector.fill(maximum.value.toLong)(0xff.toByte),
                cursor,
                false,
                Instant.EPOCH
              )
            )
          )
      }
      service = AgentService[IO](
        schedulerWithoutCounters,
        reader,
        config = AgentServiceConfig.default.copy(maximumFrameBytes = frameLimit)
      )
      handshake <- service.handshake(
        ProtocolVersion.v1,
        HandshakeRequest(frameLimit, Set(AgentFeature.PagedLogs))
      )
      _ = assertEquals(
        handshake,
        AgentCall.Succeeded(
          io.github.bbuchsbaum.slurm4s.protocol.HandshakeResponse(
            ProtocolVersion.v1,
            frameLimit,
            Set(AgentFeature.PagedLogs),
            AgentServiceConfig.default.build,
            Some(pageLimit)
          )
        )
      )
      accepted <- service.api.readLog(logRef, LogCursor.start, pageLimit)
      oversized <- service.api.readLog(
        logRef,
        LogCursor.start,
        ByteLimit.from(pageLimit.value + 1).toOption.get
      )
      count <- reads.get
      _ = assert(accepted.isInstanceOf[AgentCall.Succeeded[?]])
      _ = assert(oversized.isInstanceOf[AgentCall.Failed[?]])
      _ = assertEquals(count, 1)
    yield ()
  }

  test("typed-result reads cannot exceed the negotiated response-frame budget") {
    val frameLimit = ByteLimit.from(256 * 1024).toOption.get
    val resultLimit = AgentFrameBudget.maximumTypedResultBytes(frameLimit).get
    val resultRef = RemoteResultRef(
      AttemptId.from("typed-frame-attempt").toOption.get,
      AttemptEpoch.initial
    )
    for
      reads <- Ref.of[IO, Int](0)
      registered = new AgentRegisteredTaskService[IO]:
        def submit(
            request: RemoteRegisteredTaskRequest
        ): IO[AgentCall[RemoteRegisteredSubmission]] =
          IO.raiseError(new AssertionError("submit is not used"))
        def submitBatch(
            request: RemoteRegisteredBatchRequest
        ): IO[AgentCall[RemoteRegisteredBatchSubmission]] =
          IO.raiseError(new AssertionError("submitBatch is not used"))
        def submitScriptBatch(
            request: io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchRequest
        ): IO[
          AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchSubmission]
        ] =
          IO.raiseError(new AssertionError("submitScriptBatch is not used"))
        def readResult(
            ref: RemoteResultRef,
            maximumBytes: ByteLimit
        ): IO[AgentCall[RemoteResultRead]] =
          reads.update(_ + 1) *>
            IO.pure(AgentCall.Succeeded(RemoteResultRead.Pending(Instant.EPOCH)))
        def readScriptExit(
            ref: io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRef
        ): IO[AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRead]] =
          IO.raiseError(new AssertionError("readScriptExit is not used"))
      service = AgentService[IO](
        schedulerWithoutCounters,
        logReader,
        Some(registered),
        AgentServiceConfig.default.copy(maximumFrameBytes = frameLimit)
      )
      handshake <- service.handshake(
        ProtocolVersion.v1,
        HandshakeRequest(
          frameLimit,
          Set(AgentFeature.RegisteredTasks, AgentFeature.TypedResults)
        )
      )
      _ = handshake match
        case AgentCall.Succeeded(value) =>
          assert(value.availableFeatures.contains(AgentFeature.RegisteredTasks))
          assert(value.availableFeatures.contains(AgentFeature.TypedResults))
        case other => fail(s"expected typed-result negotiation, received $other")
      accepted <- service.api.readResult(resultRef, resultLimit)
      oversized <- service.api.readResult(
        resultRef,
        ByteLimit.from(resultLimit.value + 1).toOption.get
      )
      count <- reads.get
    yield
      assert(accepted.isInstanceOf[AgentCall.Succeeded[?]])
      assert(oversized.isInstanceOf[AgentCall.Failed[?]])
      assertEquals(count, 1)
  }

  test("handshake advertises and service enforces the safe queue-page budget") {
    val frameLimit = ByteLimit.from(512 * 1024).toOption.get
    val pageLimit = AgentFrameBudget.maximumQueuePage(frameLimit).get
    for
      reads <- Ref.of[IO, Int](0)
      reader = new QueueReader[IO]:
        def listJobs(
            query: QueueQuery,
            page: Page
        ): IO[SchedulerQueryResult[QueuePage]] =
          reads
            .update(_ + 1)
            .as(
              SchedulerQueryResult.Empty(Instant.EPOCH, evidence)
            )
      service = AgentService[IO](
        schedulerWithoutCounters,
        logReader,
        config = AgentServiceConfig.default.copy(maximumFrameBytes = frameLimit),
        queue = Some(reader)
      )
      handshake <- service.handshake(
        ProtocolVersion.v1,
        HandshakeRequest(frameLimit, Set(AgentFeature.QueueListing))
      )
      _ = handshake match
        case AgentCall.Succeeded(value) =>
          assertEquals(value.maximumQueuePage, Some(pageLimit))
          assert(value.availableFeatures.contains(AgentFeature.QueueListing))
        case other => fail(s"expected queue-listing negotiation, received $other")
      accepted <- service.api.listJobs(QueueQuery.currentUser, pageLimit)
      oversized <- service.api.listJobs(
        QueueQuery.currentUser,
        Page.from(Page.MaximumItems).toOption.get
      )
      count <- reads.get
    yield
      assert(accepted.isInstanceOf[AgentCall.Succeeded[?]])
      assert(oversized.isInstanceOf[AgentCall.Failed[?]])
      assertEquals(count, 1)
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
      ScriptSource
        .unsafeInlineScript("job.sh", ByteVector.view("#!/bin/sh\ntrue\n".getBytes("UTF-8"))),
      Vector.empty,
      ResultContract.ExitOnly
    ),
    ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
  )

  private val acceptedJob = JobRef(JobId.from("42").toOption.get, None)
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, Instant.EPOCH, ByteVector.empty)
  )

  private def scheduler(cancellations: Ref[IO, Int]): Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.raiseError(new AssertionError("capabilities should not be called while disconnected"))

    def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
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

    def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
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
  private val logBytes = ByteVector.view("abcdef".getBytes("UTF-8"))
  private val identity = FileIdentity.from("stable-file").toOption.get

  private val logReader: AgentLogReader[IO] = AgentLogReader { (_, cursor, maximum) =>
    val start = cursor.offset.value.toInt
    val bytes = logBytes.slice(start, start + maximum.value)
    val next = LogCursor(LogOffset.from(start.toLong + bytes.size).toOption.get, Some(identity))
    IO.pure(
      LogReadResult.Page(LogPage(bytes, next, next.offset.value == logBytes.size, Instant.EPOCH))
    )
  }
