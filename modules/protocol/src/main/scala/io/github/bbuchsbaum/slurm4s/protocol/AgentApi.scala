package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*

/** The transport-neutral contract a client speaks to an agent.
  *
  * Lives in `protocol` because it is a wire contract, not a server. It previously sat in the agent
  * module, so the SSH CLIENT compile-depended on the SERVER assembly — and through it on `cli` and
  * `local` — to name the interface it implements. `AgentCall` and `AgentFailure` were already here;
  * the contract that produces them belongs beside them.
  */
trait AgentApi[F[_]]:
  def capabilities: F[AgentCall[SchedulerQueryResult[SchedulerCapabilities]]]
  def submitOpaque(spec: LaunchSpec): F[AgentCall[SubmissionAttempt]]
  def submitRegistered(
      request: RemoteRegisteredTaskRequest
  ): F[AgentCall[RemoteRegisteredSubmission]]
  def submitBatch(
      request: RemoteRegisteredBatchRequest
  ): F[AgentCall[RemoteRegisteredBatchSubmission]]
  def submitScriptBatch(
      request: RemoteScriptBatchRequest
  ): F[AgentCall[RemoteScriptBatchSubmission]]
  def observe(jobs: NonEmptyVector[JobRef]): F[AgentCall[SchedulerQueryResult[ObservationBatch]]]
  def accounting(jobs: NonEmptyVector[JobRef]): F[AgentCall[SchedulerQueryResult[AccountingBatch]]]
  def cancel(job: JobRef): F[AgentCall[CancellationAttempt]]
  def readLog(
      ref: LogRef,
      cursor: LogCursor,
      maxBytes: ByteLimit
  ): F[AgentCall[LogReadResult]]
  def readResult(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit
  ): F[AgentCall[RemoteResultRead]]
  def readScriptExit(
      ref: RemoteScriptExitRef
  ): F[AgentCall[RemoteScriptExitRead]]
