package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyChain
import cats.data.NonEmptyVector
import cats.effect.Async
import cats.effect.implicits.*
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*
import io.github.bbuchsbaum.slurm4s.worker.SlurmBatch

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import scala.concurrent.duration.*

final case class RemoteBatchOptions(
    submissionKey: SubmissionKey,
    name: JobName,
    perTask: TaskResources,
    maximumResultBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath] = Vector.empty,
    environment: Map[EnvName, String] = Map.empty,
    awaitPolicy: RemoteAwaitPolicy = RemoteAwaitPolicy.default
) derives CanEqual

enum RemoteBatchSubmitFailure derives CanEqual:
  case Plan(failures: NonEmptyChain[BatchPlanFailure])
  case Input(index: ArrayIndex, failure: ResultCodecFailure)
  case ElementKey(index: ArrayIndex, failure: ValidationFailure)
  case InvalidOptions(failure: ValidationFailure)
  case Protocol(message: String)
  case Agent(failure: AgentFailure)

final case class RemoteBatchSubmitException(failure: RemoteBatchSubmitFailure)
    extends RuntimeException("remote Slurm batch submission could not be prepared")

final case class RemoteBatchElementResult[I, +A](
    index: ArrayIndex,
    input: I,
    result: RemoteExecutionResult[A]
)

final case class RemoteBatchElementException(
    index: ArrayIndex,
    result: RemoteExecutionResult[?]
) extends RuntimeException(s"remote Slurm batch element ${index.value} did not produce a value")

final class RemoteBatchElementHandle[F[_]: Async, I, A] private[ssh] (
    remote: RemoteSlurm[F],
    val index: ArrayIndex,
    val shardIndex: ArrayIndex,
    val input: I,
    val stdout: LogRef,
    val stderr: LogRef,
    task: RemoteTaskHandle[F, A]
):
  def descriptor: RemoteTaskDescriptor = task.descriptor

  def await: F[RemoteBatchElementResult[I, A]] =
    task.await.map(RemoteBatchElementResult(index, input, _))

  def awaitValue: F[A] =
    task.await.flatMap {
      case RemoteExecutionResult.Completed(ExecutionResult.Succeeded(value, _, _)) =>
        value.pure[F]
      case result =>
        Async[F].raiseError(RemoteBatchElementException(index, result))
    }

  def readStdout(
      cursor: LogCursor,
      maximumBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    remote.readLog(stdout, cursor, maximumBytes)

  def readStderr(
      cursor: LogCursor,
      maximumBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    remote.readLog(stderr, cursor, maximumBytes)

final class RemoteBatchHandle[F[_]: Async, I, A] private[ssh] (
    val topology: BatchTopology,
    val submission: SubmissionAttempt,
    val elements: NonEmptyVector[RemoteBatchElementHandle[F, I, A]]
):
  def await: F[NonEmptyVector[RemoteBatchElementResult[I, A]]] =
    elements.parTraverse(_.await)

  def awaitValues: F[NonEmptyVector[A]] =
    elements.parTraverse(_.awaitValue)

private[ssh] object RemoteBatches:
  def submit[F[_]: Async, I, A](
      remote: RemoteSlurm[F],
      batch: SlurmBatch.Registered[I, A],
      execution: BatchExecutionPlan,
      options: RemoteBatchOptions
  ): F[Either[RemoteBatchSubmitFailure, RemoteBatchHandle[F, I, A]]] =
    submitRegistered(remote, batch, execution, options)

  private def submitRegistered[F[_]: Async, I, A](
      remote: RemoteSlurm[F],
      batch: SlurmBatch.Registered[I, A],
      execution: BatchExecutionPlan,
      options: RemoteBatchOptions
  ): F[Either[RemoteBatchSubmitFailure, RemoteBatchHandle[F, I, A]]] =
    prepare(batch, execution, options) match
      case Left(failure)   => failure.asLeft[RemoteBatchHandle[F, I, A]].pure[F]
      case Right(prepared) =>
        remote.submitRegisteredBatch(prepared.request).map {
          case AgentCall.Failed(failure) =>
            RemoteBatchSubmitFailure.Agent(failure).asLeft
          case AgentCall.Succeeded(submission) =>
            buildHandle(remote, prepared, submission, options.awaitPolicy)
        }

  final private case class Prepared[I, A](
      plan: BatchPlan[I, A],
      request: RemoteRegisteredBatchRequest,
      contract: ResultContract.Structured[A]
  )

  private def prepare[I, A](
      batch: SlurmBatch.Registered[I, A],
      execution: BatchExecutionPlan,
      options: RemoteBatchOptions
  ): Either[RemoteBatchSubmitFailure, Prepared[I, A]] =
    for
      _ <- validatePolicy(options.awaitPolicy)
      contract <- ResultContract.Structured
        .from(
          batch.task.outputCodec,
          options.maximumResultBytes,
          options.declaredOutputs
        )
        .left
        .map(RemoteBatchSubmitFailure.InvalidOptions.apply)
      plan <- BatchPlanner
        .compile(
          Batch(
            batch.grid,
            BatchTask.Registered(
              batch.task.operation,
              batch.task.inputCodec,
              batch.task.outputCodec,
              options.maximumResultBytes,
              batch.task.retrySafety
            )
          ),
          execution,
          options.perTask
        )
        .left
        .map(RemoteBatchSubmitFailure.Plan.apply)
      elements <- plan.elements.traverse { element =>
        for
          key <- BatchElementKey
            .derive(options.submissionKey, element.index)
            .left
            .map(RemoteBatchSubmitFailure.ElementKey(element.index, _))
          input <- batch.task.inputCodec
            .encode(element.input)
            .left
            .map(RemoteBatchSubmitFailure.Input(element.index, _))
        yield RemoteRegisteredBatchElement(element.index, key, input)
      }
      request = RemoteRegisteredBatchRequest(
        options.submissionKey,
        options.name,
        batch.task.operation.descriptor,
        plan.topology,
        elements,
        options.environment,
        options.maximumResultBytes,
        options.declaredOutputs,
        batch.task.retrySafety
      )
    yield Prepared(plan, request, contract)

  private def buildHandle[F[_]: Async, I, A](
      remote: RemoteSlurm[F],
      prepared: Prepared[I, A],
      submission: RemoteRegisteredBatchSubmission,
      policy: RemoteAwaitPolicy
  ): Either[RemoteBatchSubmitFailure, RemoteBatchHandle[F, I, A]] =
    for
      _ <- Either.cond(
        submission.topology == prepared.plan.topology,
        (),
        RemoteBatchSubmitFailure.Protocol(
          "the agent returned a different batch topology"
        )
      )
      submittedByIndex <- uniqueByIndex(submission.elements)
      requestedByIndex = prepared.request.elements.toVector.map(value => value.index -> value).toMap
      shardByElement = prepared.plan.topology.shards.toVector
        .flatMap(shard => shard.elements.toVector.map(_ -> shard.index))
        .toMap
      handles <- prepared.plan.elements.traverse { element =>
        for
          requested <- requestedByIndex
            .get(element.index)
            .toRight(
              RemoteBatchSubmitFailure.Protocol(
                s"request metadata is missing element ${element.index.value}"
              )
            )
          returned <- submittedByIndex
            .get(element.index)
            .toRight(
              RemoteBatchSubmitFailure.Protocol(
                s"agent response is missing element ${element.index.value}"
              )
            )
          shardIndex <- shardByElement
            .get(element.index)
            .toRight(
              RemoteBatchSubmitFailure.Protocol(
                s"topology is missing element ${element.index.value}"
              )
            )
          _ <- validateElement(prepared.request, requested, returned)
          accountingIndex = prepared.plan.topology.array.map(_ => shardIndex)
          descriptor = RemoteTaskDescriptor(
            RemoteRegisteredSubmission(
              returned.resultRef,
              returned.resultHandle,
              submission.submission
            )
          )
          task = RemoteTaskHandle(
            remote,
            descriptor,
            prepared.contract,
            policy,
            accountingIndex
          )
        yield RemoteBatchElementHandle(
          remote,
          element.index,
          shardIndex,
          element.input,
          returned.stdout,
          returned.stderr,
          task
        )
      }
    yield RemoteBatchHandle(
      prepared.plan.topology,
      submission.submission,
      handles
    )

  private def uniqueByIndex(
      elements: NonEmptyVector[RemoteRegisteredBatchElementSubmission]
  ): Either[
    RemoteBatchSubmitFailure,
    Map[ArrayIndex, RemoteRegisteredBatchElementSubmission]
  ] =
    val values = elements.toVector
    Either.cond(
      values.map(_.index).distinct.size == values.size,
      values.map(value => value.index -> value).toMap,
      RemoteBatchSubmitFailure.Protocol(
        "the agent returned duplicate batch element indices"
      )
    )

  private def validateElement(
      batch: RemoteRegisteredBatchRequest,
      request: RemoteRegisteredBatchElement,
      submission: RemoteRegisteredBatchElementSubmission
  ): Either[RemoteBatchSubmitFailure, Unit] =
    val handle = submission.resultHandle
    val expectedOperation =
      WorkloadOperation.Registered(batch.operation.id, batch.operation.version)
    val mismatches = Vector(
      Option.when(submission.index != request.index)("index"),
      Option.when(handle.submissionKey != request.submissionKey)("submissionKey"),
      Option.when(handle.operation != expectedOperation)("operation"),
      Option.when(handle.resultSchema != batch.operation.outputSchema)("resultSchema"),
      Option.when(handle.maximumResultBytes != batch.maximumResultBytes)("maximumResultBytes"),
      Option.when(handle.declaredOutputs != batch.declaredOutputs)("declaredOutputs"),
      Option.when(handle.retrySafety != batch.retrySafety)("retrySafety"),
      Option.when(handle.attemptId != submission.resultRef.attemptId)("attemptId"),
      Option.when(handle.attemptEpoch != submission.resultRef.attemptEpoch)("attemptEpoch"),
      Option.when(
        submission.stdout.attemptId != submission.resultRef.attemptId ||
          submission.stdout.epoch != submission.resultRef.attemptEpoch ||
          submission.stdout.stream != LogStream.Stdout
      )("stdout"),
      Option.when(
        submission.stderr.attemptId != submission.resultRef.attemptId ||
          submission.stderr.epoch != submission.resultRef.attemptEpoch ||
          submission.stderr.stream != LogStream.Stderr
      )("stderr")
    ).flatten
    Either.cond(
      mismatches.isEmpty,
      (),
      RemoteBatchSubmitFailure.Protocol(
        s"remote batch element does not match submitted field: ${mismatches.headOption.getOrElse("unknown")}"
      )
    )

  private def validatePolicy(
      policy: RemoteAwaitPolicy
  ): Either[RemoteBatchSubmitFailure, Unit] =
    Either.cond(
      policy.pollInterval.value > 0L && policy.timeout.value > 0L &&
        policy.maximumPollInterval.value >= policy.pollInterval.value,
      (),
      RemoteBatchSubmitFailure.InvalidOptions(
        ValidationFailure(
          "remoteAwaitPolicy",
          "poll interval and timeout must both be positive"
        )
      )
    )

final case class RemoteScriptBatchOptions(
    submissionKey: SubmissionKey,
    name: JobName,
    perTask: TaskResources,
    maximumLocalScriptBytes: ByteLimit = ByteLimit.defaultEvidence,
    environment: Map[EnvName, String] = Map.empty,
    retrySafety: RetrySafety = RetrySafety.Unknown,
    awaitPolicy: RemoteAwaitPolicy = RemoteAwaitPolicy.default
) derives CanEqual

enum RemoteScriptSourceLocation derives CanEqual:
  case Inline
  case StagedLocal(path: String)

enum RemoteScriptSourceUnavailableReason derives CanEqual:
  case NotRegular
  case ReadFailed(errorType: String)

enum RemoteScriptBatchSubmitFailure derives CanEqual:
  case Plan(failures: NonEmptyChain[BatchPlanFailure])
  case Arguments(index: ArrayIndex, failures: NonEmptyChain[ValidationFailure])
  case ElementKey(index: ArrayIndex, failure: ValidationFailure)
  case SourceUnavailable(
      path: String,
      reason: RemoteScriptSourceUnavailableReason
  )
  case SourceLimitExceeded(
      source: RemoteScriptSourceLocation,
      maximumBytes: Int
  )
  case InvalidOptions(failure: ValidationFailure)
  case Protocol(message: String)
  case Agent(failure: AgentFailure)

final case class RemoteScriptBatchSubmitException(
    failure: RemoteScriptBatchSubmitFailure
) extends RuntimeException("remote Slurm script batch submission could not be prepared")

enum RemoteScriptExecutionResult derives CanEqual:
  case Exited(exitCode: Int)
  case SubmissionFailed(attempt: SubmissionAttempt)
  case AgentUnavailable(failure: AgentFailure)
  case SchedulerUnavailable(result: SchedulerQueryResult[AccountingBatch])
  case WorkloadTerminated(outcome: WorkloadOutcome, evidence: EvidenceBundle)
  case ExitStatusInvalid(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case AwaitTimedOut(diagnostics: Diagnostics)

final case class RemoteScriptBatchElementResult[I](
    index: ArrayIndex,
    input: I,
    result: RemoteScriptExecutionResult
)

final class RemoteScriptBatchElementHandle[F[_]: Async, I] private[ssh] (
    remote: RemoteSlurm[F],
    submission: SubmissionAttempt,
    policy: RemoteAwaitPolicy,
    accountingArrayIndex: Option[ArrayIndex],
    val index: ArrayIndex,
    val shardIndex: ArrayIndex,
    val input: I,
    val exitRef: RemoteScriptExitRef,
    val stdout: LogRef,
    val stderr: LogRef
):
  def await: F[RemoteScriptBatchElementResult[I]] =
    result.map(RemoteScriptBatchElementResult(index, input, _))

  def readStdout(
      cursor: LogCursor,
      maximumBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    remote.readLog(stdout, cursor, maximumBytes)

  def readStderr(
      cursor: LogCursor,
      maximumBytes: ByteLimit
  ): F[AgentCall[LogReadResult]] =
    remote.readLog(stderr, cursor, maximumBytes)

  private def result: F[RemoteScriptExecutionResult] =
    submission match
      case failed @ SubmissionAttempt.PreparationFailed(_) =>
        RemoteScriptExecutionResult.SubmissionFailed(failed).pure[F]
      case failed @ SubmissionAttempt.InvocationFailed(_) =>
        RemoteScriptExecutionResult.SubmissionFailed(failed).pure[F]
      case SubmissionAttempt.Completed(Submission.Rejected(_, _)) =>
        RemoteScriptExecutionResult.SubmissionFailed(submission).pure[F]
      case SubmissionAttempt.Completed(Submission.Accepted(job, _)) =>
        val accountingJob =
          job.copy(arrayIndex = accountingArrayIndex.orElse(job.arrayIndex))
        awaitExit(Some(accountingJob))
      case SubmissionAttempt.Completed(Submission.AcceptanceUnknown(_, _)) =>
        awaitExit(None)

  private def awaitExit(job: Option[JobRef]): F[RemoteScriptExecutionResult] =
    Async[F].monotonic.flatMap(started => poll(started, job, attempt = 0L, failures = 0))

  private def poll(
      started: FiniteDuration,
      job: Option[JobRef],
      attempt: Long,
      failures: Int
  ): F[RemoteScriptExecutionResult] =
    remote.readScriptExit(exitRef).flatMap {
      case AgentCall.Failed(failure) =>
        // The element keeps running and will still publish its exit artifact; one lost round trip
        // is no reason to discard a wait that may have hours left.
        tolerate(
          started,
          job,
          attempt,
          failures + 1,
          RemoteScriptExecutionResult.AgentUnavailable(failure)
        )
      case AgentCall.Succeeded(RemoteScriptExitRead.Exited(exitCode, _)) =>
        RemoteScriptExecutionResult.Exited(exitCode).pure[F]
      case AgentCall.Succeeded(RemoteScriptExitRead.Failed(diagnostics, evidence, _)) =>
        RemoteScriptExecutionResult.ExitStatusInvalid(diagnostics, evidence).pure[F]
      case AgentCall.Succeeded(RemoteScriptExitRead.Pending(_)) =>
        val accounting =
          if policy.checksAccounting(attempt) then terminalAccounting(job)
          else AccountingProbe.Inconclusive.pure[F]
        accounting.flatMap {
          case AccountingProbe.Terminal(result)    => result.pure[F]
          case AccountingProbe.Unavailable(result) =>
            // The exit artifact outranks aggregate accounting (ADR-0011), so an unreadable
            // accounting probe must not end the wait for it.
            tolerate(started, job, attempt, failures + 1, result)
          case AccountingProbe.Inconclusive =>
            Async[F].monotonic.flatMap { now =>
              if now - started >= policy.timeout.value.millis then
                RemoteScriptExecutionResult
                  .AwaitTimedOut(
                    Diagnostics.one(
                      Diagnostic(
                        "remote-script-await-timeout",
                        "the script exit status did not appear before the await deadline"
                      )
                    )
                  )
                  .pure[F]
              else
                Async[F].sleep(policy.intervalAfter(attempt).value.millis) *>
                  poll(started, job, attempt + 1L, failures = 0)
            }
        }
    }

  /** Continue waiting through a transient observation failure, or surrender to a persistent one. */
  private def tolerate(
      started: FiniteDuration,
      job: Option[JobRef],
      attempt: Long,
      failures: Int,
      outcome: RemoteScriptExecutionResult
  ): F[RemoteScriptExecutionResult] =
    if failures >= policy.maximumConsecutiveObservationFailures.toInt then outcome.pure[F]
    else
      Async[F].monotonic.flatMap { now =>
        if now - started >= policy.timeout.value.millis then outcome.pure[F]
        else
          Async[F].sleep(policy.intervalAfter(attempt).value.millis) *>
            poll(started, job, attempt + 1L, failures)
      }

  private def terminalAccounting(
      job: Option[JobRef]
  ): F[AccountingProbe[RemoteScriptExecutionResult]] =
    job match
      case None        => AccountingProbe.Inconclusive.pure[F]
      case Some(value) =>
        remote.accounting(NonEmptyVector.one(value)).map {
          case AgentCall.Failed(failure) =>
            AccountingProbe.Unavailable(RemoteScriptExecutionResult.AgentUnavailable(failure))
          case AgentCall.Succeeded(result @ SchedulerQueryResult.InvocationFailed(_)) =>
            AccountingProbe.Unavailable(RemoteScriptExecutionResult.SchedulerUnavailable(result))
          case AgentCall.Succeeded(result @ SchedulerQueryResult.ParseFailed(_, _)) =>
            AccountingProbe.Unavailable(RemoteScriptExecutionResult.SchedulerUnavailable(result))
          case AgentCall.Succeeded(SchedulerQueryResult.Succeeded(batch)) =>
            batch.records.toVector
              .find(record => record.job == value)
              .flatMap(record =>
                record.outcome.map(
                  RemoteScriptExecutionResult.WorkloadTerminated(_, record.evidence)
                )
              )
              .fold[AccountingProbe[RemoteScriptExecutionResult]](AccountingProbe.Inconclusive)(
                AccountingProbe.Terminal(_)
              )
          case AgentCall.Succeeded(SchedulerQueryResult.Empty(_, _)) =>
            AccountingProbe.Inconclusive
        }

final class RemoteScriptBatchHandle[F[_]: Async, I] private[ssh] (
    val topology: BatchTopology,
    val submission: SubmissionAttempt,
    val elements: NonEmptyVector[RemoteScriptBatchElementHandle[F, I]]
):
  def await: F[NonEmptyVector[RemoteScriptBatchElementResult[I]]] =
    elements.parTraverse(_.await)

private[ssh] object RemoteScriptBatches:
  private enum LocalSourceFailure derives CanEqual:
    case NotRegular
    case LimitExceeded

  final private case class Prepared[I](
      plan: BatchPlan[I, NoResult],
      request: RemoteScriptBatchRequest
  )

  def submit[F[_]: Async, I](
      remote: RemoteSlurm[F],
      batch: SlurmBatch.Script[I],
      execution: BatchExecutionPlan,
      options: RemoteScriptBatchOptions
  ): F[Either[RemoteScriptBatchSubmitFailure, RemoteScriptBatchHandle[F, I]]] =
    prepare(batch, execution, options).flatMap {
      case Left(failure)   => failure.asLeft[RemoteScriptBatchHandle[F, I]].pure[F]
      case Right(prepared) =>
        remote.submitScriptBatch(prepared.request).map {
          case AgentCall.Failed(failure) =>
            RemoteScriptBatchSubmitFailure.Agent(failure).asLeft
          case AgentCall.Succeeded(submission) =>
            buildHandle(remote, prepared, submission, options.awaitPolicy)
        }
    }

  private def prepare[F[_]: Async, I](
      batch: SlurmBatch.Script[I],
      execution: BatchExecutionPlan,
      options: RemoteScriptBatchOptions
  ): F[Either[RemoteScriptBatchSubmitFailure, Prepared[I]]] =
    purePlan(batch, execution, options) match
      case Left(failure)           => failure.asLeft[Prepared[I]].pure[F]
      case Right((plan, elements)) =>
        materializeLocalSource[F](batch.program, options.maximumLocalScriptBytes).map {
          _.map { program =>
            Prepared(
              plan,
              RemoteScriptBatchRequest(
                options.submissionKey,
                options.name,
                program,
                plan.topology,
                elements,
                options.environment,
                options.retrySafety
              )
            )
          }
        }

  private def purePlan[I](
      batch: SlurmBatch.Script[I],
      execution: BatchExecutionPlan,
      options: RemoteScriptBatchOptions
  ): Either[
    RemoteScriptBatchSubmitFailure,
    (BatchPlan[I, NoResult], NonEmptyVector[RemoteScriptBatchElement])
  ] =
    for
      _ <- validatePolicy(options.awaitPolicy)
      plan <- BatchPlanner
        .compile(
          Batch(
            batch.grid,
            BatchTask.Script(batch.program, batch.arguments)
          ),
          execution,
          options.perTask
        )
        .left
        .map(RemoteScriptBatchSubmitFailure.Plan.apply)
      elements <- plan.elements.traverse { element =>
        for
          key <- BatchElementKey
            .derive(options.submissionKey, element.index)
            .left
            .map(RemoteScriptBatchSubmitFailure.ElementKey(element.index, _))
          arguments <- batch.arguments
            .encode(element.input)
            .left
            .map(RemoteScriptBatchSubmitFailure.Arguments(element.index, _))
        yield RemoteScriptBatchElement(element.index, key, arguments)
      }
    yield plan -> elements

  private def materializeLocalSource[F[_]: Async](
      program: ScriptProgram,
      maximumBytes: ByteLimit
  ): F[Either[RemoteScriptBatchSubmitFailure, ScriptProgram]] =
    program.source match
      case ScriptSource.Inline(_, bytes) =>
        Either
          .cond(
            bytes.size <= maximumBytes.value,
            program,
            RemoteScriptBatchSubmitFailure.SourceLimitExceeded(
              RemoteScriptSourceLocation.Inline,
              maximumBytes.value
            )
          )
          .pure[F]
      case ScriptSource.ExistingRemote(_) => program.asRight.pure[F]
      case ScriptSource.StagedLocal(raw)  =>
        Async[F]
          .blocking {
            val path = Path.of(raw)
            readLocalScript(path, maximumBytes).map(path.getFileName.toString -> _)
          }
          .attempt
          .map {
            case Left(error) =>
              RemoteScriptBatchSubmitFailure
                .SourceUnavailable(
                  raw,
                  RemoteScriptSourceUnavailableReason.ReadFailed(
                    error.getClass.getSimpleName
                  )
                )
                .asLeft
            case Right(Left(LocalSourceFailure.NotRegular)) =>
              RemoteScriptBatchSubmitFailure
                .SourceUnavailable(
                  raw,
                  RemoteScriptSourceUnavailableReason.NotRegular
                )
                .asLeft
            case Right(Left(LocalSourceFailure.LimitExceeded)) =>
              RemoteScriptBatchSubmitFailure
                .SourceLimitExceeded(
                  RemoteScriptSourceLocation.StagedLocal(raw),
                  maximumBytes.value
                )
                .asLeft
            case Right(Right((name, bytes))) =>
              ScriptProgram(
                ScriptSource.Inline(name, bytes),
                program.invocation
              ).asRight
          }

  private def readLocalScript(
      path: Path,
      maximumBytes: ByteLimit
  ): Either[LocalSourceFailure, Vector[Byte]] =
    if !Files.isRegularFile(path) then Left(LocalSourceFailure.NotRegular)
    else
      val input = Files.newInputStream(path, StandardOpenOption.READ)
      val output = ByteArrayOutputStream()
      val buffer = new Array[Byte](8192)
      try
        var done = false
        while !done && output.size() <= maximumBytes.value do
          val count = input.read(buffer)
          if count < 0 then done = true
          else output.write(buffer, 0, count)
        if output.size() > maximumBytes.value then Left(LocalSourceFailure.LimitExceeded)
        else Right(output.toByteArray.toVector)
      finally
        input.close()
        output.close()

  private def buildHandle[F[_]: Async, I](
      remote: RemoteSlurm[F],
      prepared: Prepared[I],
      submission: RemoteScriptBatchSubmission,
      policy: RemoteAwaitPolicy
  ): Either[RemoteScriptBatchSubmitFailure, RemoteScriptBatchHandle[F, I]] =
    for
      _ <- Either.cond(
        submission.topology == prepared.plan.topology,
        (),
        RemoteScriptBatchSubmitFailure.Protocol(
          "the agent returned a different script batch topology"
        )
      )
      submittedByIndex <- uniqueByIndex(submission.elements)
      shardByElement = prepared.plan.topology.shards.toVector
        .flatMap(shard => shard.elements.toVector.map(_ -> shard.index))
        .toMap
      handles <- prepared.plan.elements.traverse { element =>
        for
          returned <- submittedByIndex
            .get(element.index)
            .toRight(
              RemoteScriptBatchSubmitFailure.Protocol(
                s"agent response is missing script element ${element.index.value}"
              )
            )
          shardIndex <- shardByElement
            .get(element.index)
            .toRight(
              RemoteScriptBatchSubmitFailure.Protocol(
                s"topology is missing script element ${element.index.value}"
              )
            )
          _ <- validateElement(returned)
          accountingIndex = prepared.plan.topology.array.map(_ => shardIndex)
        yield RemoteScriptBatchElementHandle(
          remote,
          submission.submission,
          policy,
          accountingIndex,
          element.index,
          shardIndex,
          element.input,
          returned.exitRef,
          returned.stdout,
          returned.stderr
        )
      }
    yield RemoteScriptBatchHandle(
      prepared.plan.topology,
      submission.submission,
      handles
    )

  private def uniqueByIndex(
      elements: NonEmptyVector[RemoteScriptBatchElementSubmission]
  ): Either[
    RemoteScriptBatchSubmitFailure,
    Map[ArrayIndex, RemoteScriptBatchElementSubmission]
  ] =
    val values = elements.toVector
    Either.cond(
      values.map(_.index).distinct.size == values.size,
      values.map(value => value.index -> value).toMap,
      RemoteScriptBatchSubmitFailure.Protocol(
        "the agent returned duplicate script batch element indices"
      )
    )

  private def validateElement(
      element: RemoteScriptBatchElementSubmission
  ): Either[RemoteScriptBatchSubmitFailure, Unit] =
    Either.cond(
      element.stdout.attemptId == element.exitRef.attemptId &&
        element.stdout.epoch == element.exitRef.attemptEpoch &&
        element.stdout.stream == LogStream.Stdout &&
        element.stderr.attemptId == element.exitRef.attemptId &&
        element.stderr.epoch == element.exitRef.attemptEpoch &&
        element.stderr.stream == LogStream.Stderr,
      (),
      RemoteScriptBatchSubmitFailure.Protocol(
        s"script element ${element.index.value} has mismatched log and exit references"
      )
    )

  private def validatePolicy(
      policy: RemoteAwaitPolicy
  ): Either[RemoteScriptBatchSubmitFailure, Unit] =
    Either.cond(
      policy.pollInterval.value > 0L && policy.timeout.value > 0L &&
        policy.maximumPollInterval.value >= policy.pollInterval.value,
      (),
      RemoteScriptBatchSubmitFailure.InvalidOptions(
        ValidationFailure(
          "remoteAwaitPolicy",
          "poll interval and timeout must both be positive"
        )
      )
    )
