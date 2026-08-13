package io.github.bbuchsbaum.slurm4s.examples

import cats.data.NonEmptyVector
import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.local.LocalLogReader
import io.github.bbuchsbaum.slurm4s.managed.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentApi
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits
import io.github.bbuchsbaum.slurm4s.ssh.*

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class PublicApiExamplesSuite extends munit.CatsEffectSuite:
  private val resources =
    JobRequests.resources(1, 1).toEither.fold(problem => fail(problem.toString), identity)
  private val pageSize = ByteLimit.from(4).fold(problem => fail(problem.toString), identity)

  test("opaque and declared-output builders preserve distinct result contracts") {
    val source = ScriptSource.unsafeInlineScript(
      "analysis.sh",
      ByteVector.view("#!/bin/sh\ntrue\n".getBytes(StandardCharsets.UTF_8))
    )
    val exitOnly = JobRequests
      .exitOnly("opaque-1", "opaque", source, Vector.empty, resources)
      .toEither
    val declared = JobRequests
      .declaredOutputs(
        "outputs-1",
        "outputs",
        source,
        Vector("--seed", "1"),
        Vector("results/model.rds"),
        resources
      )
      .toEither

    assertEquals(
      exitOnly.map(_.payload.resultContract.descriptor.mode),
      Right(ResultMode.ExitOnly)
    )
    assertEquals(
      declared.map(_.payload.resultContract.descriptor.mode),
      Right(ResultMode.DeclaredOutputs)
    )
  }

  test("validation accumulates malformed identity and output paths before submission") {
    val invalid = JobRequests.declaredOutputs(
      "bad key",
      "bad name",
      ScriptSource.ExistingRemote("/cluster/jobs/model.R"),
      Vector.empty,
      Vector("../escape", "also//invalid"),
      resources
    )

    assert(invalid.isInvalid)
    assert(invalid.fold(_.toChain.toList.size >= 4, _ => false))
  }

  test("local log pages resume from the returned cursor") {
    val root = Files.createTempDirectory("slurm4s-examples-log")
    val log = root.resolve("stdout.log")
    Files.write(log, "abcdefgh".getBytes(StandardCharsets.UTF_8))
    val reader = LocalLogReader[IO](root, pageSize)
    val ref = LogRef(
      AttemptId.from("example-attempt").fold(problem => fail(problem.toString), identity),
      AttemptEpoch.initial,
      LogStream.Stdout,
      log.toString
    )

    for
      first <- LogMonitoring.readLocal(reader, ref, LogCursor.start, pageSize)
      firstPage = first match
        case LogReadResult.Page(value) => value
        case other                     => fail(s"expected first page, received $other")
      second <- LogMonitoring.readLocal(reader, ref, firstPage.next, pageSize)
    yield second match
      case LogReadResult.Page(value) =>
        assertEquals(
          new String(firstPage.bytes.toArray ++ value.bytes.toArray, StandardCharsets.UTF_8),
          "abcdefgh"
        )
        assertEquals(value.next.offset.value, 8L)
      case other => fail(s"expected second page, received $other")
  }

  test("remote wire validation does not start ssh") {
    val target =
      SshTarget.from("cluster.example").fold(problem => fail(problem.toString), identity)
    val runner = new SshProcessRunner[IO]:
      def exchange(
          launch: SshLaunch,
          request: ByteVector,
          policy: SshExchangePolicy
      ): IO[SshProcessOutcome] =
        IO.raiseError(new AssertionError(s"unexpected exchange: $launch $request $policy"))

    val invalid = RemoteOpaque.wire(
      SshConnection("\u0000", target),
      runner,
      FrameLimits.default,
      SshExchangePolicy(
        DurationMillis.from(1000).fold(problem => fail(problem.toString), identity)
      )
    )

    assert(invalid.isLeft)
  }

  test("remote submission keeps transport failure separate from Slurm submission") {
    val request = JobRequests
      .exitOnly(
        "remote-1",
        "remote",
        ScriptSource.ExistingRemote("/cluster/jobs/model.py"),
        Vector.empty,
        resources
      )
      .toEither
      .fold(problem => fail(problem.toString), identity)
    val failure = AgentFailure.AgentUnavailable("agent missing", None)
    val api = failingAgent(failure)

    RemoteOpaque.submit(api, request).map(result => assertEquals(result, AgentCall.Failed(failure)))
  }

  test("managed restart pass is bounded and does no work for an empty journal") {
    for
      store <- InMemoryControlStore.create[IO]()
      controller = ManagedController[IO](store, inertScheduler)
      search = new AcceptanceSearch[IO]:
        def find(intent: ManagedIntent): IO[AcceptanceSearchResult] =
          IO.raiseError(new AssertionError(s"unexpected acceptance search: $intent"))
      one = PositiveInt.from("bound", 1).fold(problem => fail(problem.toString), identity)
      result <- ManagedRecovery.afterRestart(controller, search, RecoveryBounds(one, one, one))
    yield assertEquals(result, RecoveryPass(Vector.empty, Vector.empty, Vector.empty))
  }

  test("typed task construction binds operation schemas, codecs, and request result type") {
    val task = IncrementTask.create().toEither.fold(problem => fail(problem.toString), identity)
    val maximum = ByteLimit.from(1024).fold(problem => fail(problem.toString), identity)
    val request = TypedResults
      .request("typed-1", "increment", 41, task, maximum, resources)
      .toEither
      .fold(problem => fail(problem.toString), identity)

    assertEquals(task.inputCodec.decode(task.inputCodec.encode(41).toOption.get), Right(41))
    assertEquals(task.outputCodec.schemaId, task.operation.outputSchema)
    assert(request.payload.isInstanceOf[Payload.RegisteredTask[?, ?]])
    assert(TypedResults.registry(task).isRight)
  }

  private val inertScheduler: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.raiseError(new AssertionError("unexpected capabilities call"))
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
      IO.raiseError(new AssertionError(s"unexpected submit: $spec"))
    def observe(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[ObservationBatch]] =
      IO.raiseError(new AssertionError(s"unexpected observe: $jobs"))
    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[AccountingBatch]] =
      IO.raiseError(new AssertionError(s"unexpected accounting: $jobs"))
    def cancel(job: JobRef): IO[CancellationAttempt] =
      IO.raiseError(new AssertionError(s"unexpected cancel: $job"))

  private def failingAgent(
      failure: AgentFailure
  ): AgentApi[IO] =
    new AgentApi[IO]:
      def capabilities: IO[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
        IO.pure(AgentCall.Failed(failure))
      def listJobs(
          query: QueueQuery,
          page: Page
      ): IO[AgentCall[SchedulerQueryResult[QueuePage]]] =
        IO.pure(AgentCall.Failed(failure))
      def submitOpaque(spec: LaunchSpec): IO[AgentCall[SubmissionAttempt]] =
        IO.pure(AgentCall.Failed(failure))
      def submitRegistered(
          request: io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredTaskRequest
      ): IO[
        AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredSubmission]
      ] =
        IO.pure(AgentCall.Failed(failure))
      def submitBatch(
          request: io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchRequest
      ): IO[
        AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchSubmission]
      ] =
        IO.pure(AgentCall.Failed(failure))
      def submitScriptBatch(
          request: io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchRequest
      ): IO[
        AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchSubmission]
      ] =
        IO.pure(AgentCall.Failed(failure))
      def observe(
          jobs: NonEmptyVector[JobRef]
      ): IO[AgentCall[SchedulerQueryResult[ObservationBatch]]] =
        IO.pure(AgentCall.Failed(failure))
      def accounting(
          jobs: NonEmptyVector[JobRef]
      ): IO[AgentCall[SchedulerQueryResult[AccountingBatch]]] =
        IO.pure(AgentCall.Failed(failure))
      def cancel(job: JobRef): IO[AgentCall[CancellationAttempt]] =
        IO.pure(AgentCall.Failed(failure))
      def readLog(
          ref: LogRef,
          cursor: LogCursor,
          maxBytes: ByteLimit
      ): IO[AgentCall[LogReadResult]] =
        IO.pure(AgentCall.Failed(failure))
      def readResult(
          ref: io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRef,
          maximumBytes: ByteLimit
      ): IO[AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRead]] =
        IO.pure(AgentCall.Failed(failure))
      def readScriptExit(
          ref: io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRef
      ): IO[AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRead]] =
        IO.pure(AgentCall.Failed(failure))
