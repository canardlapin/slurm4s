package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all.*
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.agent.AgentApi
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
    * a rejected or safely retryable submission.
    */
  val scheduler: Scheduler[F] = new Scheduler[F]:
    def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
      RemoteSlurm.this.capabilities.flatMap(queryResult)

    def submit[A](request: JobRequest[A]): F[SubmissionAttempt] =
      erase(request) match
        case Left(diagnostics) => SubmissionAttempt.PreparationFailed(diagnostics).pure[F]
        case Right(opaque)     =>
          RemoteSlurm.this.submitOpaque(opaque).flatMap {
            case AgentCall.Succeeded(value) => value.pure[F]
            case AgentCall.Failed(
                  failure @ AgentFailure.TransportDisconnected(true, _, _)
                ) =>
              failureEvidence(failure).map(evidence =>
                SubmissionAttempt.Completed(
                  Submission.AcceptanceUnknown(
                    AcceptanceUncertainty.TransportInterrupted,
                    evidence
                  )
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
        case AgentCall.Succeeded(value) => value.pure[F]
        case AgentCall.Failed(failure)  =>
          invocationFailure(failure).map(CancellationAttempt.InvocationFailed(_))
      }

  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]] =
    connected(_.capabilities)

  def submitOpaque(request: JobRequest[NoResult]): F[AgentCall[SubmissionAttempt]] =
    connected(_.submitOpaque(request))

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

  private def invocationFailure(failure: AgentFailure): F[InvocationResult] =
    failureEvidence(failure).map(evidence =>
      InvocationResult.SpawnFailed(
        SpawnFailureKind.Unknown,
        Diagnostics.one(Diagnostic(agentFailureCode(failure), failure.toString)),
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

  private def agentFailureCode(failure: AgentFailure): String =
    failure match
      case _: AgentFailure.ProtocolMismatch      => "ssh-protocol-mismatch"
      case _: AgentFailure.AgentUnavailable      => "ssh-agent-unavailable"
      case _: AgentFailure.AuthenticationFailed  => "ssh-authentication-failed"
      case _: AgentFailure.TransportDisconnected => "ssh-transport-disconnected"
      case _: AgentFailure.RemoteCliFailure      => "ssh-remote-cli-failure"
      case _: AgentFailure.RemoteAgentFailure    => "ssh-remote-agent-failure"
      case _: AgentFailure.ProtocolViolation     => "ssh-protocol-violation"

  private def erase[A](request: JobRequest[A]): Either[Diagnostics, JobRequest[NoResult]] =
    request.payload match
      case Payload.Script(source, arguments, _) =>
        Right(
          JobRequest[NoResult](
            request.submissionKey,
            request.name,
            Payload.Script(source, arguments, ResultContract.ExitOnly),
            request.resources,
            request.environment,
            request.array,
            request.retrySafety
          )
        )
      case _ =>
        Left(
          Diagnostics.one(
            Diagnostic(
              "ssh-scheduler-request",
              "the managed SSH scheduler accepts only lowered script requests"
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
