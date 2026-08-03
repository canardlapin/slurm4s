package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*

import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.DurationInt

/** The exchange budget must cover every remote call a script-batch element makes.
  *
  * `RemoteScriptBatchHandle.await` fans out with `elements.parTraverse(_.await)`, so any call left
  * outside the budget is multiplied by the element count. The registered-task path already routes
  * its accounting probe through the budget; this suite holds the script-batch path to the same
  * ceiling, because a probe fires on the first poll of every element at once.
  */
class RemoteScriptBatchBudgetSuite extends munit.CatsEffectSuite:

  private val observedAt = Instant.parse("2026-07-24T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, observedAt, ByteVector.empty)
  )
  private val parentJob = JobRef(JobId.from("9200").toOption.get, None)

  private val policy = RemoteAwaitPolicy(
    pollInterval = DurationMillis.from(1L).toOption.get,
    timeout = DurationMillis.from(10_000L).toOption.get
  )

  /** Peak simultaneous accounting exchanges observed while awaiting `elements` elements. */
  private def peakAccountingExchanges(elements: Int, budgetSize: Int): IO[Int] =
    for
      inFlight <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      runner = ScriptExitRunner(inFlight, peak)
      remote <- connect(runner)
      budget <- RemoteExchangeBudget.of[IO](PositiveInt.unsafeFrom(budgetSize))
      handles = (0 until elements).toVector.map(index => handle(budget, remote, index))
      _ <- handles.parTraverse(_.await)
      observed <- peak.get
    yield observed

  private def handle(
      budget: RemoteExchangeBudget[IO],
      remote: RemoteSlurm[IO],
      index: Int
  ): RemoteScriptBatchElementHandle[IO, Int] =
    val arrayIndex = ArrayIndex.from(index).toOption.get
    val exitRef = RemoteScriptExitRef(
      AttemptId.from(s"element-$index").toOption.get,
      AttemptEpoch.from(1L).toOption.get
    )
    RemoteScriptBatchElementHandle[IO, Int](
      budget,
      remote,
      SubmissionAttempt.Completed(Submission.Accepted(parentJob, evidence)),
      policy,
      // The probe therefore asks about the parent job, which the canned record names, so one
      // accounting exchange per element is terminal and the wait ends without a timeout.
      None,
      arrayIndex,
      arrayIndex,
      index,
      exitRef,
      LogRef(exitRef.attemptId, exitRef.attemptEpoch, LogStream.Stdout, s"stdout-$index"),
      LogRef(exitRef.attemptId, exitRef.attemptEpoch, LogStream.Stderr, s"stderr-$index")
    )

  private def connect(runner: SshProcessRunner[IO]): IO[RemoteSlurm[IO]] =
    val target = SshTarget.from("budget-cluster").toOption.get
    val launch = SshCommand.agent(SshConnection("/usr/bin/ssh", target)).toOption.get
    val wire = SshAgentWireClient(
      launch,
      runner,
      FrameLimits.default,
      SshExchangePolicy(DurationMillis.from(10_000L).toOption.get)
    )
    SshAgentApi.connect[IO](wire).map(RemoteSlurm(_))

  test("a script-batch accounting probe stays inside the exchange budget") {
    peakAccountingExchanges(elements = 16, budgetSize = 2).map(peak =>
      assert(
        peak <= 2,
        s"a budget of 2 admitted $peak simultaneous accounting exchanges"
      )
    )
  }

  /** Answers the exit read as pending so the element always reaches its accounting probe.
    *
    * Accounting is deliberately slow, so any probe left outside the budget accumulates and the
    * high-water mark reports the element count rather than the ceiling.
    */
  final private class ScriptExitRunner(
      inFlight: Ref[IO, Int],
      peak: Ref[IO, Int]
  ) extends SshProcessRunner[IO]:
    def exchange(
        launch: SshLaunch,
        request: ByteVector,
        policy: SshExchangePolicy
    ): IO[SshProcessOutcome] =
      requestOf(request) match
        case Some(envelope) =>
          envelope.body match
            case AgentBody.Request(AgentMethod.Handshake, _) =>
              respond(envelope, HandshakeJson.response(handshakeResponse))
            case AgentBody.Request(AgentMethod.ReadScriptExit, _) =>
              respond(
                envelope,
                AgentDomainJson.encodeRemoteScriptExitRead(
                  RemoteScriptExitRead.Pending(observedAt)
                )
              )
            case AgentBody.Request(AgentMethod.Accounting, _) =>
              val measured =
                inFlight.updateAndGet(_ + 1).flatMap(current => peak.update(_ max current)) *>
                  IO.sleep(25.millis) *>
                  inFlight.update(_ - 1)
              measured *> respond(envelope, AgentDomainJson.encodeAccounting(terminalAccounting))
            case other =>
              IO.raiseError(new IllegalStateException(s"unexpected agent request: $other"))
        case None =>
          IO.raiseError(new IllegalStateException("request was not a single agent frame"))

    private def respond(request: AgentEnvelope, payload: io.circe.Json): IO[SshProcessOutcome] =
      val response = request.withBody(AgentBody.Response(AgentResponseStatus.Ok, payload))
      FrameCodec.encode(AgentMessageCodec.encode(response), FrameLimits.default) match
        case Left(failure) => IO.raiseError(new IllegalStateException(s"framing failed: $failure"))
        case Right(frame)  =>
          IO.pure(
            SshProcessOutcome.Exited(
              0,
              requestWriteCompleted = true,
              BoundedEvidence.capture(EvidenceSource.CommandStdout("ssh"), observedAt, frame),
              BoundedEvidence.capture(
                EvidenceSource.CommandStderr("ssh"),
                observedAt,
                ByteVector.empty
              )
            )
          )

  private def requestOf(request: ByteVector): Option[AgentEnvelope] =
    FrameDecoder
      .empty(FrameLimits.default)
      .feed(request)
      .toOption
      .flatMap(_._2.headOption)
      .flatMap(AgentMessageCodec.decode(_).toOption)

  private val handshakeResponse = HandshakeResponse(
    agentProtocol = ProtocolVersion.v1,
    maximumFrameBytes = ByteLimit.maximumCommandCapture,
    availableFeatures = Set(
      AgentFeature.SchedulerQueries,
      AgentFeature.ScriptBatches,
      AgentFeature.OpaqueScripts
    ),
    agentBuild = "budget-suite",
    maximumLogPageBytes = None
  )

  /** A terminal accounting record, so each element finishes after exactly one probe. */
  private val terminalAccounting: SchedulerQueryResult[AccountingBatch] =
    SchedulerQueryResult.Succeeded(
      AccountingBatch(
        NonEmptyVector.one(
          AccountingRecord(
            job = parentJob,
            state = SlurmState.Completed,
            exitStatus = Some(ExitStatus(0, None)),
            outcome = Some(WorkloadOutcome.Completed(0)),
            freshness = Freshness.Current(observedAt),
            rawFields = Map.empty,
            evidence = evidence
          )
        ),
        missing = Vector.empty
      )
    )
