package io.github.bbuchsbaum.scalaslurm.agent

import cats.data.NonEmptyVector
import cats.effect.IO
import fs2.Stream
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.protocol.*

class AgentStdioServerSuite extends munit.CatsEffectSuite:
  test("stdio command is deliberately fixed") {
    assertEquals(AgentCommand.parse(List("serve", "--stdio")), Right(AgentCommand.ServeStdio))
    assert(AgentCommand.parse(List("serve", "--stdio", "user-value")).isLeft)
    assert(AgentCommand.parse(List("shell")).isLeft)
  }

  test("agent runtime requires a non-root private workspace") {
    assert(AgentRuntimeConfig.fromEnvironment(Map.empty).isLeft)
    assert(
      AgentRuntimeConfig
        .fromEnvironment(Map(AgentRuntimeConfig.WorkspaceEnvironment -> "/"))
        .isLeft
    )
    assert(
      AgentRuntimeConfig
        .fromEnvironment(Map(AgentRuntimeConfig.WorkspaceEnvironment -> "/tmp/scala-slurm-agent"))
        .isRight
    )
  }

  test("stdio server decodes fragmented handshake frames and emits one response frame") {
    val service = AgentService[IO](unusedScheduler, unusedLogs)
    val delegate = new AgentRequestHandler[IO]:
      def handle(request: AgentEnvelope): IO[AgentEnvelope] =
        IO.raiseError(new AssertionError("handshake must not reach delegate"))
    val handler = ServiceRequestHandler[IO](service, delegate)
    val server = AgentStdioServer[IO](handler)
    val request = AgentEnvelope(
      RequestId.from("handshake-1").toOption.get,
      ProtocolVersion.v1,
      AgentBody.Request(
        AgentMethod.Handshake,
        HandshakeJson.request(
          HandshakeRequest(
            ByteLimit.from(4096).toOption.get,
            Set(AgentFeature.PagedLogs)
          )
        )
      )
    )
    val framed = FrameCodec
      .encode(AgentMessageCodec.encode(request), FrameLimits.default)
      .toOption
      .get

    Stream
      .emits(framed)
      .covary[IO]
      .chunkN(2)
      .flatMap(Stream.chunk)
      .through(server.pipe)
      .compile
      .toVector
      .map { bytes =>
        val decodedFrame = FrameDecoder.empty().feed(bytes).toOption.get
        assertEquals(decodedFrame._2.size, 1)
        val response = AgentMessageCodec.decode(decodedFrame._2.head).toOption.get
        assertEquals(response.requestId, request.requestId)
        response.body match
          case AgentBody.Response(AgentResponseStatus.Ok, payload) =>
            assert(HandshakeJson.decodeResponse(payload).isRight)
          case other => fail(s"unexpected response: $other")
      }
  }

  private val unusedLogs: AgentLogReader[IO] =
    AgentLogReader((_, _, _) => IO.raiseError(new AssertionError("logs not used")))

  private val unusedScheduler: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] = unused
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] = unused

  private def unused[A]: IO[A] = IO.raiseError(new AssertionError("scheduler not used"))
