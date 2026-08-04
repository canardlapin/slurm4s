package io.github.bbuchsbaum.slurm4s.ssh

import cats.data.NonEmptyVector
import cats.effect.Async
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*
import io.github.bbuchsbaum.slurm4s.task.SlurmTaskCall

import scala.concurrent.duration.*
import scodec.bits.ByteVector

/** How often a caller may ask the target about work it has already submitted.
  *
  * Every poll costs one SSH process and one remote agent start, because ADR 0003 deliberately gives
  * each request a bounded resource lifetime instead of multiplexing. The cadence has to respect
  * that: `pollInterval` is the first delay only, doubling up to `maximumPollInterval`, so a job
  * that finishes quickly is still noticed quickly while a long job settles into a cheap rhythm.
  *
  * `accountingEveryPolls` keeps `sacct` on a slower cadence than the result read. Accounting is a
  * shared cluster database, and a client that queries it at result-read frequency is a problem for
  * every other user of the site.
  *
  * `maximumConsecutiveObservationFailures` bounds how much transient blindness a wait survives.
  * Over a long wait a lost SSH round trip is close to certain, and the work itself is unaffected by
  * it: the job keeps running and the durable result envelope remains the authority. Abandoning the
  * wait on the first failed observation would discard hours of waiting over a hiccup, so failures
  * are ridden out until they stop looking transient.
  */
final case class RemoteAwaitPolicy(
    pollInterval: DurationMillis,
    timeout: DurationMillis,
    maximumPollInterval: DurationMillis = DurationMillis.unsafeFrom(60_000L),
    accountingEveryPolls: PositiveInt = PositiveInt.unsafeFrom(6),
    maximumConsecutiveObservationFailures: PositiveInt = PositiveInt.unsafeFrom(5)
) derives CanEqual:
  /** Delay after `attempt` completed polls, doubling from `pollInterval` up to the ceiling. */
  def intervalAfter(attempt: Long): DurationMillis =
    val ceiling = math.max(maximumPollInterval.value, pollInterval.value)
    val doublings = math.min(attempt, 32L).toInt
    val grown =
      if pollInterval.value > ceiling / (1L << doublings) then ceiling
      else pollInterval.value << doublings
    DurationMillis.unsafeFrom(math.min(grown, ceiling))

  /** Accounting is checked on the first poll, then every `accountingEveryPolls` polls. */
  def checksAccounting(attempt: Long): Boolean =
    attempt % accountingEveryPolls.toInt.toLong == 0L

object RemoteAwaitPolicy:
  val default: RemoteAwaitPolicy = RemoteAwaitPolicy(
    pollInterval = DurationMillis.unsafeFrom(5_000L),
    timeout = DurationMillis.unsafeFrom(24L * 60L * 60L * 1000L),
    maximumPollInterval = DurationMillis.unsafeFrom(60_000L),
    accountingEveryPolls = PositiveInt.unsafeFrom(6),
    maximumConsecutiveObservationFailures = PositiveInt.unsafeFrom(5)
  )

/** What one accounting probe established while a wait was in progress.
  *
  * Separating `Unavailable` from `Inconclusive` is the point: "accounting says this job died" and
  * "accounting could not be read" are different facts, and collapsing them makes a broken query
  * indistinguishable from a broken job.
  */
private[ssh] enum AccountingProbe[+A] derives CanEqual:
  case Terminal(result: A)
  case Unavailable(result: A)
  case Inconclusive

final case class RemoteTaskOptions(
    submissionKey: SubmissionKey,
    name: JobName,
    resources: ResourceRequest,
    maximumResultBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath] = Vector.empty,
    environment: Map[EnvName, String] = Map.empty,
    awaitPolicy: RemoteAwaitPolicy = RemoteAwaitPolicy.default,
    terminationNotice: Option[TerminationNotice] = None
) derives CanEqual

enum RemoteSubmitFailure derives CanEqual:
  case InputCodec(failure: ResultCodecFailure)
  case InvalidOptions(failure: ValidationFailure)
  case Protocol(message: String)
  case Agent(failure: AgentFailure)

final case class RemoteSubmitException(failure: RemoteSubmitFailure)
    extends RuntimeException("remote Slurm task submission could not be prepared")

final case class RemoteTaskDescriptor(
    submission: RemoteRegisteredSubmission
) derives CanEqual

object RemoteTaskDescriptor:
  def encode(
      descriptor: RemoteTaskDescriptor,
      maximumBytes: ByteLimit = RemoteTaskWireLimits.MaximumDescriptorBytes
  ): Either[RemoteTaskDescriptorCodecFailure, ByteVector] =
    RemoteRegisteredSubmissionCodec.encode(descriptor.submission, maximumBytes)

  def decode(
      bytes: ByteVector,
      maximumBytes: ByteLimit = RemoteTaskWireLimits.MaximumDescriptorBytes
  ): Either[RemoteTaskDescriptorCodecFailure, RemoteTaskDescriptor] =
    RemoteRegisteredSubmissionCodec.decode(bytes, maximumBytes).map(RemoteTaskDescriptor.apply)

enum RemoteExecutionResult[+A]:
  case Completed(result: ExecutionResult[A])
  case SubmissionFailed(attempt: SubmissionAttempt)
  case AgentUnavailable(failure: AgentFailure)
  case SchedulerUnavailable(result: SchedulerQueryResult[AccountingBatch])
  case AwaitTimedOut(diagnostics: Diagnostics)

final case class RemoteTaskException(result: RemoteExecutionResult[?])
    extends RuntimeException("remote Slurm task did not produce a typed value")

/** What one poll established about the work being awaited. */
private[ssh] enum AwaitTick[+A] derives CanEqual:
  /** The wait is over. */
  case Done(result: A)

  /** Nothing was learned this tick; keep waiting until the deadline. */
  case Waiting

  /** Transient blindness. Ride it out, and surrender to `outcome` if it persists. */
  case Blind(outcome: A)

/** The awaiting policy, applied in one place.
  *
  * Backoff, the accounting cadence, tolerance of transient blindness, and the deadline are decided
  * here rather than at each call site. A second awaiting path that reimplemented them would drift,
  * and this codebase has already paid for that pattern twice — three hand-written terminality
  * classifiers, and two independent constructions of one durable handle.
  */
private[ssh] object RemoteAwaitDriver:
  def run[F[_]: Async, A](
      policy: RemoteAwaitPolicy,
      timedOut: => A
  )(tick: Long => F[AwaitTick[A]]): F[A] =
    Async[F].monotonic.flatMap(started => loop(policy, timedOut, tick, started, 0L, 0))

  private def loop[F[_]: Async, A](
      policy: RemoteAwaitPolicy,
      timedOut: => A,
      tick: Long => F[AwaitTick[A]],
      started: FiniteDuration,
      attempt: Long,
      failures: Int
  ): F[A] =
    tick(attempt).flatMap {
      case AwaitTick.Done(result) => result.pure[F]
      case AwaitTick.Waiting      =>
        continueOr(policy, timedOut, tick, started, attempt, failures = 0)
      case AwaitTick.Blind(outcome) =>
        // Losing sight of the work says nothing about the work, which is still running and will
        // still publish its result. Surrender only once blindness looks persistent, and report the
        // blindness itself rather than a timeout: "I stopped being able to see it" is the honest
        // account of what happened.
        if failures + 1 >= policy.maximumConsecutiveObservationFailures.toInt then outcome.pure[F]
        else continueOr(policy, outcome, tick, started, attempt, failures + 1)
    }

  private def continueOr[F[_]: Async, A](
      policy: RemoteAwaitPolicy,
      expired: => A,
      tick: Long => F[AwaitTick[A]],
      started: FiniteDuration,
      attempt: Long,
      failures: Int
  ): F[A] =
    Async[F].monotonic.flatMap { now =>
      if now - started >= policy.timeout.value.millis then expired.pure[F]
      else
        Async[F].sleep(policy.intervalAfter(attempt).value.millis) *>
          loop(policy, expired, tick, started, attempt + 1L, failures)
    }

final class RemoteTaskHandle[F[_]: Async, A] private[ssh] (
    remote: RemoteSlurm[F],
    val descriptor: RemoteTaskDescriptor,
    contract: ResultContract.Structured[A],
    policy: RemoteAwaitPolicy,
    accountingArrayIndex: Option[ArrayIndex],
    // Shared across the handles of one batch, so awaiting N elements does not open N simultaneous
    // SSH processes. A lone handle has no one to share with and passes `unbounded`.
    budget: RemoteExchangeBudget[F]
):
  def submission: SubmissionAttempt = descriptor.submission.submission

  /** Decode one batched read for this element. Shares the validation the single path uses. */
  private[ssh] def interpret(read: RemoteResultRead): Option[RemoteExecutionResult[A]] =
    read match
      case RemoteResultRead.Available(stored, bytes, observedAt) =>
        Some(
          RemoteExecutionResult.Completed(
            RemoteResultValidation.attach(resultHandle, stored, contract, bytes, observedAt)
          )
        )
      case RemoteResultRead.Failed(diagnostics, evidence, _) =>
        Some(RemoteExecutionResult.Completed(ExecutionResult.Indeterminate(diagnostics, evidence)))
      case RemoteResultRead.Pending(_) => None

  private[ssh] def awaitPolicy: RemoteAwaitPolicy = policy
  private[ssh] def envelopeBytes: ByteLimit = resultHandle.maximumEnvelopeBytes
  private[ssh] def batchResultRef: RemoteResultRef = resultRef
  def resultHandle: DurableResultHandle = descriptor.submission.resultHandle
  def resultRef: RemoteResultRef = descriptor.submission.resultRef

  def await: F[RemoteExecutionResult[A]] =
    submission match
      case failed @ SubmissionAttempt.PreparationFailed(_) =>
        RemoteExecutionResult.SubmissionFailed(failed).pure[F]
      case failed @ SubmissionAttempt.InvocationFailed(_) =>
        RemoteExecutionResult.SubmissionFailed(failed).pure[F]
      case SubmissionAttempt.Completed(Submission.Rejected(_, _)) =>
        RemoteExecutionResult.SubmissionFailed(submission).pure[F]
      case SubmissionAttempt.Completed(Submission.Accepted(job, _)) =>
        awaitResult(
          Some(job.copy(arrayIndex = accountingArrayIndex.orElse(job.arrayIndex)))
        )
      case SubmissionAttempt.Completed(Submission.AcceptanceUnknown(_, _)) =>
        awaitResult(None)

  def awaitValue: F[A] =
    await.flatMap {
      case RemoteExecutionResult.Completed(ExecutionResult.Succeeded(value, _, _)) =>
        value.pure[F]
      case other => Async[F].raiseError(RemoteTaskException(other))
    }

  private def awaitResult(job: Option[JobRef]): F[RemoteExecutionResult[A]] =
    RemoteAwaitDriver.run(policy, awaitTimedOut)(readOnce(job, _))

  private def awaitTimedOut: RemoteExecutionResult[A] =
    RemoteExecutionResult.AwaitTimedOut(
      Diagnostics.one(
        Diagnostic(
          "remote-await-timeout",
          "the remote result did not become available before the await deadline"
        )
      )
    )

  /** One poll: read the result, and consult accounting on the policy's slower cadence. */
  private def readOnce(job: Option[JobRef], attempt: Long): F[AwaitTick[RemoteExecutionResult[A]]] =
    budget.use(remote.readResult(resultRef, resultHandle.maximumEnvelopeBytes)).flatMap {
      case AgentCall.Failed(failure) =>
        AwaitTick.Blind(RemoteExecutionResult.AgentUnavailable(failure)).pure[F]
      case AgentCall.Succeeded(RemoteResultRead.Available(stored, bytes, observedAt)) =>
        AwaitTick
          .Done(
            RemoteExecutionResult.Completed(
              RemoteResultValidation.attach(resultHandle, stored, contract, bytes, observedAt)
            )
          )
          .pure[F]
      case AgentCall.Succeeded(RemoteResultRead.Failed(diagnostics, evidence, _)) =>
        AwaitTick
          .Done(
            RemoteExecutionResult.Completed(ExecutionResult.Indeterminate(diagnostics, evidence))
          )
          .pure[F]
      case AgentCall.Succeeded(RemoteResultRead.Pending(_)) =>
        if policy.checksAccounting(attempt) then
          terminalAccounting(job).map {
            case AccountingProbe.Terminal(result) => AwaitTick.Done(result)
            // Accounting is only the safety net for a job that died without publishing; the result
            // envelope remains the authority, so an unreadable probe must not end the wait.
            case AccountingProbe.Unavailable(result) => AwaitTick.Blind(result)
            case AccountingProbe.Inconclusive        => AwaitTick.Waiting
          }
        else AwaitTick.Waiting.pure[F]
    }

  private def terminalAccounting(
      job: Option[JobRef]
  ): F[AccountingProbe[RemoteExecutionResult[A]]] =
    job match
      case None        => AccountingProbe.Inconclusive.pure[F]
      case Some(value) =>
        budget.use(remote.accounting(NonEmptyVector.one(value))).map {
          case AgentCall.Failed(failure) =>
            AccountingProbe.Unavailable(RemoteExecutionResult.AgentUnavailable(failure))
          case AgentCall.Succeeded(result @ SchedulerQueryResult.InvocationFailed(_)) =>
            AccountingProbe.Unavailable(RemoteExecutionResult.SchedulerUnavailable(result))
          case AgentCall.Succeeded(result @ SchedulerQueryResult.ParseFailed(_, _)) =>
            AccountingProbe.Unavailable(RemoteExecutionResult.SchedulerUnavailable(result))
          case AgentCall.Succeeded(SchedulerQueryResult.Succeeded(batch)) =>
            batch.records.toVector
              .find(record => record.job == value)
              .flatMap(record =>
                record.outcome.map {
                  case WorkloadOutcome.Completed(_) =>
                    RemoteExecutionResult.Completed(
                      ExecutionResult.Indeterminate(
                        Diagnostics.one(
                          Diagnostic(
                            "terminal-without-result",
                            "Slurm reports successful completion but no typed result was published"
                          )
                        ),
                        record.evidence
                      )
                    )
                  case outcome =>
                    RemoteExecutionResult.Completed(
                      ExecutionResult.WorkloadFailed(outcome, record.evidence)
                    )
                }
              )
              .fold[AccountingProbe[RemoteExecutionResult[A]]](AccountingProbe.Inconclusive)(
                AccountingProbe.Terminal(_)
              )
          case AgentCall.Succeeded(SchedulerQueryResult.Empty(_, _)) =>
            AccountingProbe.Inconclusive
        }

private[ssh] object RemoteTasks:
  def submit[F[_]: Async, I, A](
      remote: RemoteSlurm[F],
      call: SlurmTaskCall[I, A],
      options: RemoteTaskOptions
  ): F[Either[RemoteSubmitFailure, RemoteTaskHandle[F, A]]] =
    validatePolicy(options.awaitPolicy).flatMap(_ =>
      ResultContract.Structured
        .from(
          call.task.outputCodec,
          options.maximumResultBytes,
          options.declaredOutputs
        )
        .left
        .map(RemoteSubmitFailure.InvalidOptions.apply)
    ) match
      case Left(failure)   => Left(failure).pure[F]
      case Right(contract) =>
        call.task.inputCodec.encode(call.input) match
          case Left(failure)     => Left(RemoteSubmitFailure.InputCodec(failure)).pure[F]
          case Right(inputBytes) =>
            val request = RemoteRegisteredTaskRequest(
              options.submissionKey,
              options.name,
              call.task.operation.descriptor,
              inputBytes,
              options.resources,
              options.environment,
              options.maximumResultBytes,
              options.declaredOutputs,
              call.task.retrySafety,
              options.terminationNotice
            )
            remote.submitRegistered(request).map {
              case AgentCall.Failed(failure)       => Left(RemoteSubmitFailure.Agent(failure))
              case AgentCall.Succeeded(submission) =>
                validateSubmission(request, submission).map { _ =>
                  RemoteTaskHandle(
                    remote,
                    RemoteTaskDescriptor(submission),
                    contract,
                    options.awaitPolicy,
                    None,
                    RemoteExchangeBudget.unbounded[F]
                  )
                }
            }

  def attach[F[_]: Async, A](
      remote: RemoteSlurm[F],
      descriptor: RemoteTaskDescriptor,
      codec: ResultCodec[A],
      policy: RemoteAwaitPolicy
  ): Either[RemoteSubmitFailure, RemoteTaskHandle[F, A]] =
    val handle = descriptor.submission.resultHandle
    val ref = descriptor.submission.resultRef
    for
      _ <- validatePolicy(policy)
      _ <- Either.cond(
        handle.attemptId == ref.attemptId && handle.attemptEpoch == ref.attemptEpoch,
        (),
        RemoteSubmitFailure.Protocol(
          "result reference and reconnectable handle identify different attempts"
        )
      )
      _ <- Either.cond(
        handle.resultSchema == codec.schemaId,
        (),
        RemoteSubmitFailure.Protocol(
          s"result codec schema ${codec.schemaId.value} does not match ${handle.resultSchema.value}"
        )
      )
      contract <- ResultContract.Structured
        .from(codec, handle.maximumResultBytes, handle.declaredOutputs)
        .left
        .map(RemoteSubmitFailure.InvalidOptions.apply)
    yield RemoteTaskHandle(
      remote,
      descriptor,
      contract,
      policy,
      None,
      RemoteExchangeBudget.unbounded[F]
    )

  private def validatePolicy(policy: RemoteAwaitPolicy): Either[RemoteSubmitFailure, Unit] =
    Either.cond(
      policy.pollInterval.value > 0L && policy.timeout.value > 0L &&
        policy.maximumPollInterval.value >= policy.pollInterval.value,
      (),
      RemoteSubmitFailure.InvalidOptions(
        ValidationFailure(
          "remoteAwaitPolicy",
          "poll interval and timeout must be positive and the maximum must not be smaller"
        )
      )
    )

  private def validateSubmission(
      request: RemoteRegisteredTaskRequest,
      submission: RemoteRegisteredSubmission
  ): Either[RemoteSubmitFailure, Unit] =
    val handle = submission.resultHandle
    val mismatches = Vector(
      Option.when(handle.submissionKey != request.submissionKey)("submissionKey"),
      Option.when(
        handle.operation !=
          WorkloadOperation.Registered(request.operation.id, request.operation.version)
      )("operation"),
      Option.when(handle.resultSchema != request.operation.outputSchema)("resultSchema"),
      Option.when(handle.maximumResultBytes != request.maximumResultBytes)("maximumResultBytes"),
      Option.when(handle.declaredOutputs != request.declaredOutputs)("declaredOutputs"),
      Option.when(handle.retrySafety != request.retrySafety)("retrySafety"),
      Option.when(handle.attemptId != submission.resultRef.attemptId)("attemptId"),
      Option.when(handle.attemptEpoch != submission.resultRef.attemptEpoch)("attemptEpoch")
    ).flatten
    Either.cond(
      mismatches.isEmpty,
      (),
      RemoteSubmitFailure.Protocol(
        s"remote result handle does not match submitted field: ${mismatches.headOption.getOrElse("unknown")}"
      )
    )

private[ssh] object RemoteResultValidation:
  def attach[A](
      expectedHandle: DurableResultHandle,
      storedHandle: DurableResultHandle,
      contract: ResultContract.Structured[A],
      envelopeBytes: ByteVector,
      observedAt: java.time.Instant
  ): ExecutionResult[A] =
    val evidence = EvidenceBundle(
      BoundedEvidence.capture(
        EvidenceSource.ResultEnvelope,
        observedAt,
        envelopeBytes,
        expectedHandle.maximumEnvelopeBytes
      )
    )
    if storedHandle.attemptEpoch != expectedHandle.attemptEpoch then
      invalid(
        "stale-result-epoch",
        "stored result metadata has a different attempt epoch",
        evidence
      )
    else if storedHandle != expectedHandle then
      invalid(
        "result-handle-mismatch",
        "stored result metadata does not match the reconnectable handle",
        evidence
      )
    else
      ResultEnvelopeCodec.decode(
        envelopeBytes,
        expectedHandle.maximumEnvelopeBytes,
        expectedHandle.maximumResultBytes
      ) match
        case Left(failure) =>
          invalid("malformed-result-envelope", failure.toString, evidence)
        case Right(envelope) =>
          val mismatches = Vector(
            Option.when(envelope.submissionKey != expectedHandle.submissionKey)("submissionKey"),
            Option.when(envelope.attemptId != expectedHandle.attemptId)("attemptId"),
            Option.when(envelope.attemptEpoch != expectedHandle.attemptEpoch)("attemptEpoch"),
            Option.when(envelope.job != expectedHandle.job)("job"),
            Option.when(envelope.operation != expectedHandle.operation)("operation"),
            Option.when(envelope.resultSchema != expectedHandle.resultSchema)("resultSchema"),
            Option.when(envelope.workerRelease != expectedHandle.workerRelease)("workerRelease")
          ).flatten
          mismatches.headOption match
            case Some(field) =>
              invalid(
                "result-envelope-binding-mismatch",
                s"result envelope differs from the handle field: $field",
                evidence
              )
            case None =>
              envelope.status match
                case ResultEnvelopeStatus.Failed(code, message) =>
                  ExecutionResult.WorkloadFailed(
                    WorkloadOutcome.Failed(
                      None,
                      Diagnostics.one(
                        Diagnostic("worker-task-failed", message, Map("workerCode" -> code))
                      )
                    ),
                    evidence
                  )
                case ResultEnvelopeStatus.Succeeded =>
                  decodeSuccess(contract, envelope, evidence)

  private def decodeSuccess[A](
      contract: ResultContract.Structured[A],
      envelope: ResultEnvelope,
      evidence: EvidenceBundle
  ): ExecutionResult[A] =
    val expectedPaths = contract.outputs.toSet
    val reportedPaths = envelope.outputs.entries.map(_.path).toSet
    if expectedPaths != reportedPaths then
      invalid(
        "output-validation-failed",
        "result output paths do not match the reconnectable result contract",
        evidence
      )
    else
      envelope.value match
        case None => invalid("result-value-missing", "successful result has no value", evidence)
        case Some(bytes) =>
          contract.codec.decode(bytes) match
            case Left(failure) => invalid(failure.code, failure.message, evidence)
            case Right(value)  => ExecutionResult.Succeeded(value, envelope.outputs, evidence)

  private def invalid[A](
      code: String,
      message: String,
      evidence: EvidenceBundle
  ): ExecutionResult[A] =
    ExecutionResult.ResultInvalid(
      Diagnostics.one(Diagnostic(code, message)),
      evidence
    )
