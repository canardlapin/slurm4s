package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all.*
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.agent.AgentApi
import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits
import io.github.bbuchsbaum.slurm4s.worker.SlurmTaskCall
import io.github.bbuchsbaum.slurm4s.worker.SlurmBatch

import java.nio.charset.StandardCharsets

final case class SlurmSshConfig(
    connection: SshConnection,
    frameLimits: FrameLimits,
    exchangePolicy: SshExchangePolicy
) derives CanEqual

object SlurmSshConfig:
  def default(connection: SshConnection): Either[ValidationFailure, SlurmSshConfig] =
    DurationMillis
      .from(30_000L)
      .map(timeout =>
        SlurmSshConfig(
          connection,
          FrameLimits.default,
          SshExchangePolicy(timeout)
        )
      )

final class RemoteSlurm[F[_]: Async] private[ssh] (
    val connection: AgentCall[SshAgentApi[F]]
) extends AgentApi[F]:
  /** Transport-neutral scheduler view used by durable managed control.
    *
    * A disconnect after writing `sbatch` is preserved as acceptance uncertainty, never rewritten as
    * a rejected or safely retryable submission. The same law governs cancellation: loss after the
    * cancellation request was written yields cancellation-unknown, because `scancel` may already
    * have run and a plain invocation failure would assert a non-cancellation nobody can verify.
    */
  val scheduler: Scheduler[F] = new Scheduler[F]:
    def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
      RemoteSlurm.this.capabilities.flatMap(queryResult)

    def submit(spec: LaunchSpec): F[SubmissionAttempt] =
      RemoteSlurm.this.submitOpaque(spec).flatMap {
        case AgentCall.Succeeded(value)                         => value.pure[F]
        case AgentCall.Failed(failure) if mayHaveActed(failure) =>
          failureEvidence(failure).map(evidence =>
            SubmissionAttempt.Completed(
              Submission.AcceptanceUnknown(uncertaintyOf(failure), evidence)
            )
          )
        case AgentCall.Failed(failure) =>
          invocationFailure(failure).map(SubmissionAttempt.InvocationFailed(_))
      }

    def observe(
        jobs: NonEmptyVector[JobRef]
    ): F[SchedulerQueryResult[ObservationBatch]] =
      RemoteSlurm.this.observe(jobs).flatMap(queryResult)

    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): F[SchedulerQueryResult[AccountingBatch]] =
      RemoteSlurm.this.accounting(jobs).flatMap(queryResult)

    def cancel(job: JobRef): F[CancellationAttempt] =
      RemoteSlurm.this.cancel(job).flatMap {
        case AgentCall.Succeeded(value)                         => value.pure[F]
        case AgentCall.Failed(failure) if mayHaveActed(failure) =>
          failureEvidence(failure).map(evidence =>
            CancellationAttempt.Completed(
              CancellationResult.Unknown(
                Diagnostics.one(
                  Diagnostic(
                    "cancellation-acknowledgement-unknown",
                    "transport was lost after the cancellation request was written; " +
                      "scancel may already have run"
                  )
                ),
                evidence
              )
            )
          )
        case AgentCall.Failed(failure) =>
          invocationFailure(failure).map(CancellationAttempt.InvocationFailed(_))
      }

  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
    connected(_.capabilities)

  def submitOpaque(spec: LaunchSpec): F[AgentCall[SubmissionAttempt]] =
    connected(_.submitOpaque(spec))

  def submit[I, A](
      call: SlurmTaskCall[I, A],
      options: RemoteTaskOptions
  ): F[Either[RemoteSubmitFailure, RemoteTaskHandle[F, A]]] =
    RemoteTasks.submit(this, call, options)

  def submitOrRaise[I, A](
      call: SlurmTaskCall[I, A],
      options: RemoteTaskOptions
  ): F[RemoteTaskHandle[F, A]] =
    submit(call, options).flatMap {
      case Right(handle) => handle.pure[F]
      case Left(failure) => Async[F].raiseError(RemoteSubmitException(failure))
    }

  def submitBatch[I, A](
      batch: SlurmBatch.Registered[I, A],
      execution: BatchExecutionPlan,
      options: RemoteBatchOptions
  ): F[Either[RemoteBatchSubmitFailure, RemoteBatchHandle[F, I, A]]] =
    RemoteBatches.submit(this, batch, execution, options)

  def submitBatchOrRaise[I, A](
      batch: SlurmBatch.Registered[I, A],
      execution: BatchExecutionPlan,
      options: RemoteBatchOptions
  ): F[RemoteBatchHandle[F, I, A]] =
    submitBatch(batch, execution, options).flatMap {
      case Right(handle) => handle.pure[F]
      case Left(failure) => Async[F].raiseError(RemoteBatchSubmitException(failure))
    }

  def submitBatch[I](
      batch: SlurmBatch.Script[I],
      execution: BatchExecutionPlan,
      options: RemoteScriptBatchOptions
  ): F[Either[RemoteScriptBatchSubmitFailure, RemoteScriptBatchHandle[F, I]]] =
    RemoteScriptBatches.submit(this, batch, execution, options)

  def submitBatchOrRaise[I](
      batch: SlurmBatch.Script[I],
      execution: BatchExecutionPlan,
      options: RemoteScriptBatchOptions
  ): F[RemoteScriptBatchHandle[F, I]] =
    submitBatch(batch, execution, options).flatMap {
      case Right(handle) => handle.pure[F]
      case Left(failure) =>
        Async[F].raiseError(RemoteScriptBatchSubmitException(failure))
    }

  def attach[A](
      descriptor: RemoteTaskDescriptor,
      codec: ResultCodec[A],
      policy: RemoteAwaitPolicy = RemoteAwaitPolicy.default
  ): Either[RemoteSubmitFailure, RemoteTaskHandle[F, A]] =
    RemoteTasks.attach(this, descriptor, codec, policy)

  def submitRegistered(
      request: io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredTaskRequest
  ): F[
    AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredSubmission]
  ] =
    connected(_.submitRegistered(request))

  def submitRegisteredBatch(
      request: io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchRequest
  ): F[
    AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchSubmission]
  ] =
    connected(_.submitBatch(request))

  override def submitBatch(
      request: io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchRequest
  ): F[
    AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteRegisteredBatchSubmission]
  ] =
    submitRegisteredBatch(request)

  def submitScriptBatch(
      request: io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchRequest
  ): F[
    AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptBatchSubmission]
  ] =
    connected(_.submitScriptBatch(request))

  def observe(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[ObservationBatch]]] =
    connected(_.observe(jobs))

  def accounting(
      jobs: NonEmptyVector[JobRef]
  ): F[AgentCall[SchedulerQueryResult[AccountingBatch]]] =
    connected(_.accounting(jobs))

  def cancel(job: JobRef): F[AgentCall[CancellationAttempt]] =
    connected(_.cancel(job))

  def readLog(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    connected(_.readLog(ref, cursor, maxBytes))

  def readResult(
      ref: io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRef,
      maximumBytes: ByteLimit
  ): F[AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteResultRead]] =
    connected(_.readResult(ref, maximumBytes))

  def readScriptExit(
      ref: io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRef
  ): F[AgentCall[io.github.bbuchsbaum.slurm4s.protocol.RemoteScriptExitRead]] =
    connected(_.readScriptExit(ref))

  private def connected[A](run: SshAgentApi[F] => F[AgentCall[A]]): F[AgentCall[A]] =
    connection match
      case AgentCall.Succeeded(api)  => run(api)
      case AgentCall.Failed(failure) => AgentCall.Failed(failure).pure[F]

  private def queryResult[A](call: AgentCall[SchedulerQueryResult[A]]): F[SchedulerQueryResult[A]] =
    call match
      case AgentCall.Succeeded(value) => value.pure[F]
      case AgentCall.Failed(failure)  =>
        invocationFailure(failure).map(SchedulerQueryResult.InvocationFailed(_))

  /** What a failed agent call actually establishes.
    *
    * Every variant used to collapse into `SpawnFailed(Unknown)`, which asserted three things at
    * once: that the remote program never ran, that nothing is known about why, and — for the
    * variants that arise only AFTER a successful exchange — a plain falsehood. The low-level SSH
    * client had the distinctions and the scheduler adapter destroyed them.
    */
  private enum CallFailure derives CanEqual:
    /** The remote agent was never executed. Safely repeatable.
      */
    case NotStarted(kind: SpawnFailureKind, code: String)

    /** The connection was lost. `afterWrite` decides whether the request may have been acted on.
      */
    case TransportLost(afterWrite: Boolean, code: String)

    /** The exchange completed and the far side reported a failure, so the program DID run.
      */
    case RemoteReported(code: String)

    /** The exchange completed but the response could not be trusted, so what the far side did is
      * unknown rather than known not to have happened.
      */
    case ResponseUntrusted(code: String)

  private def classify(failure: AgentFailure): CallFailure = failure match
    case AgentFailure.AgentUnavailable(_, _) =>
      CallFailure.NotStarted(SpawnFailureKind.ExecutableMissing, "ssh-agent-unavailable")
    case AgentFailure.AuthenticationFailed(_, _) =>
      CallFailure.NotStarted(SpawnFailureKind.PermissionDenied, "ssh-authentication-failed")
    case AgentFailure.TransportDisconnected(afterWrite, _, _) =>
      CallFailure.TransportLost(afterWrite, "ssh-transport-disconnected")
    case AgentFailure.RemoteCliFailure(_, _) =>
      CallFailure.RemoteReported("ssh-remote-cli-failure")
    case AgentFailure.RemoteAgentFailure(_, _) =>
      CallFailure.RemoteReported("ssh-remote-agent-failure")
    case AgentFailure.ProtocolViolation(_, _) =>
      CallFailure.ResponseUntrusted("ssh-protocol-violation")
    case AgentFailure.ProtocolMismatch(_, _) =>
      CallFailure.NotStarted(SpawnFailureKind.EnvironmentInvalid, "ssh-protocol-mismatch")

  /** True when the call may already have taken effect at the scheduler. */
  /** Why acceptance is unknown, preserving the distinction the flattening destroyed.
    *
    * An unparseable response is not a lost connection: `AcceptanceUncertainty.ResponseUnparseable`
    * exists for exactly this, the CLI backend already uses it, and reporting it as a transport
    * interruption hid the difference between "the wire broke" and "the agent answered something we
    * could not read".
    */
  private def uncertaintyOf(failure: AgentFailure): AcceptanceUncertainty =
    classify(failure) match
      case CallFailure.ResponseUntrusted(_) => AcceptanceUncertainty.ResponseUnparseable
      case CallFailure.RemoteReported(_)    => AcceptanceUncertainty.Unclassified
      case _                                => AcceptanceUncertainty.TransportInterrupted

  /** True when the call may already have taken effect at the scheduler. */
  private def mayHaveActed(failure: AgentFailure): Boolean = classify(failure) match
    case CallFailure.NotStarted(_, _)             => false
    case CallFailure.TransportLost(afterWrite, _) => afterWrite
    // The far side ran and answered; whatever it did, it did.
    case CallFailure.RemoteReported(_)    => true
    case CallFailure.ResponseUntrusted(_) => true

  private def invocationFailure(failure: AgentFailure): F[InvocationResult] =
    val classified = classify(failure)
    val code = classified match
      case CallFailure.NotStarted(_, value)     => value
      case CallFailure.TransportLost(_, value)  => value
      case CallFailure.RemoteReported(value)    => value
      case CallFailure.ResponseUntrusted(value) => value
    val kind = classified match
      case CallFailure.NotStarted(value, _) => value
      // A transport loss or an untrusted response says nothing about how the program was launched.
      case _ => SpawnFailureKind.Unknown
    failureEvidence(failure).map(evidence =>
      InvocationResult.SpawnFailed(
        kind,
        Diagnostics.one(Diagnostic(code, failure.toString)),
        evidence
      )
    )

  private def failureEvidence(failure: AgentFailure): F[EvidenceBundle] =
    val retained = failure match
      case AgentFailure.AgentUnavailable(_, evidence)         => evidence
      case AgentFailure.AuthenticationFailed(_, evidence)     => evidence
      case AgentFailure.TransportDisconnected(_, _, evidence) => evidence
      case AgentFailure.RemoteCliFailure(_, evidence)         => evidence
      case AgentFailure.RemoteAgentFailure(_, evidence)       => evidence
      case AgentFailure.ProtocolViolation(_, evidence)        => evidence
      case AgentFailure.ProtocolMismatch(_, _)                => None
    retained match
      case Some(value) => EvidenceBundle(value).pure[F]
      case None        =>
        cats.effect
          .Clock[F]
          .realTimeInstant
          .map(at =>
            EvidenceBundle(
              BoundedEvidence.capture(
                EvidenceSource.AgentProtocol,
                at,
                failure.toString.getBytes(StandardCharsets.UTF_8).toVector
              )
            )
          )

object Slurm:
  def overSsh[F[_]: Async](
      config: SlurmSshConfig
  )(using Processes[F]): Either[ValidationFailure, Resource[F, RemoteSlurm[F]]] =
    SshCommand.agent(config.connection).map { launch =>
      val wire = SshAgentWireClient(
        launch,
        SystemSshProcessRunner[F],
        config.frameLimits,
        config.exchangePolicy
      )
      Resource.eval(SshAgentApi.connect[F](wire).map(RemoteSlurm(_)))
    }
