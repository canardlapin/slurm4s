package io.github.bbuchsbaum.slurm4s.agent

import cats.data.NonEmptyVector
import cats.effect.IO
import fs2.Chunk
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*
import scodec.bits.ByteVector

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
        .fromEnvironment(Map(AgentRuntimeConfig.WorkspaceEnvironment -> "/tmp/slurm4s-agent"))
        .isRight
    )
  }

  test("registered-task runtime configuration is all-or-none and validates environment names") {
    val base = Map(
      AgentRuntimeConfig.WorkspaceEnvironment -> "/tmp/slurm4s-agent",
      AgentRuntimeConfig.WorkerExecutableEnvironment -> "/tmp/slurm4s-worker",
      AgentRuntimeConfig.WorkerReleaseIdEnvironment -> "worker-v1",
      AgentRuntimeConfig.WorkerReleaseDigestEnvironment -> "sha256:50f78b480f466c542a8fc083c27c1143d0d7f571dbac6ede72f7d70cc3f3fec2",
      AgentRuntimeConfig.AllowedEnvironmentNames -> "LANG,OMP_NUM_THREADS"
    )

    val configured = AgentRuntimeConfig.fromEnvironment(base)
    assert(configured.exists(_.worker.nonEmpty))
    assertEquals(
      configured.toOption.get.allowedEnvironmentOverrides,
      Set("LANG", "OMP_NUM_THREADS")
    )
    assert(
      AgentRuntimeConfig
        .fromEnvironment(base.removed(AgentRuntimeConfig.WorkerReleaseDigestEnvironment))
        .isLeft
    )
    assert(
      AgentRuntimeConfig
        .fromEnvironment(base.updated(AgentRuntimeConfig.AllowedEnvironmentNames, "FOO,ALL"))
        .isLeft
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
      .chunk(Chunk.byteVector(framed))
      .covary[IO]
      .chunkN(2)
      .flatMap(Stream.chunk)
      .through(server.pipe)
      .compile
      .to(ByteVector)
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

  test("handler failure is correlated, redacted, and does not terminate later requests") {
    val secret = "account-token=do-not-leak"
    val failing = request("handler-fails")
    val following = request("handler-recovers")
    val handler = new AgentRequestHandler[IO]:
      def handle(request: AgentEnvelope): IO[AgentEnvelope] =
        if request.requestId == failing.requestId then IO.raiseError(new RuntimeException(secret))
        else
          IO.pure(
            request.withBody(
              AgentBody.Response(
                AgentResponseStatus.Ok,
                io.circe.Json.obj("continued" -> io.circe.Json.fromBoolean(true))
              )
            )
          )
    val input = frame(failing) ++ frame(following)

    Stream
      .chunk(Chunk.byteVector(input))
      .covary[IO]
      .through(AgentStdioServer[IO](handler).pipe)
      .compile
      .to(ByteVector)
      .map { bytes =>
        val responses = decodeFrames(bytes)
        assertEquals(responses.map(_.requestId), Vector(failing.requestId, following.requestId))
        responses match
          case Vector(first, second) =>
            first.body match
              case AgentBody.Response(AgentResponseStatus.InternalFailure, payload) =>
                assertEquals(
                  payload.hcursor.get[String]("code"),
                  Right("agent-handler-failed")
                )
                assertEquals(
                  payload.hcursor.get[String]("causeClass"),
                  Right("RuntimeException")
                )
                assert(!payload.noSpaces.contains(secret))
              case other => fail(s"expected contained handler failure, received $other")
            second.body match
              case AgentBody.Response(AgentResponseStatus.Ok, payload) =>
                assertEquals(payload.hcursor.get[Boolean]("continued"), Right(true))
              case other => fail(s"expected following success, received $other")
          case other => fail(s"expected two responses, received ${other.size}")
      }
  }

  test("framing corruption remains a stream failure") {
    val invalidLength = ByteVector(0x7f, 0xff.toByte, 0xff.toByte, 0xff.toByte)

    Stream
      .chunk(Chunk.byteVector(invalidLength))
      .covary[IO]
      .through(AgentStdioServer[IO](unusedHandler).pipe)
      .compile
      .drain
      .attempt
      .map {
        case Left(_: AgentWireException) => assert(true)
        case other                       => fail(s"expected framing failure, received $other")
      }
  }

  private val unusedLogs: AgentLogReader[IO] =
    AgentLogReader((_, _, _) => IO.raiseError(new AssertionError("logs not used")))

  private val unusedHandler: AgentRequestHandler[IO] = new AgentRequestHandler[IO]:
    def handle(request: AgentEnvelope): IO[AgentEnvelope] =
      IO.raiseError(new AssertionError(s"handler not used: ${request.requestId.value}"))

  private val unusedScheduler: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] = unused
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] = unused
    def observe(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[ObservationBatch]] = unused
    def accounting(jobs: NonEmptyVector[JobRef]): IO[SchedulerQueryResult[AccountingBatch]] = unused
    def cancel(job: JobRef): IO[CancellationAttempt] = unused

  private def unused[A]: IO[A] = IO.raiseError(new AssertionError("scheduler not used"))

  private def request(id: String): AgentEnvelope =
    AgentEnvelope(
      RequestId.from(id).fold(problem => fail(problem.toString), identity),
      ProtocolVersion.v1,
      AgentBody.Request(AgentMethod.Capabilities, io.circe.Json.obj())
    )

  private def frame(request: AgentEnvelope): ByteVector =
    FrameCodec
      .encode(AgentMessageCodec.encode(request), FrameLimits.default)
      .fold(problem => fail(problem.toString), identity)

  private def decodeFrames(bytes: ByteVector): Vector[AgentEnvelope] =
    FrameDecoder
      .empty()
      .feed(bytes)
      .fold(
        problem => fail(problem.toString),
        result =>
          result._2.map(frame =>
            AgentMessageCodec.decode(frame).fold(problem => fail(problem.toString), identity)
          )
      )
