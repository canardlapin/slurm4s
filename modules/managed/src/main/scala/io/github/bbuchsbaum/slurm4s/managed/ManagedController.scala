package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.Clock
import cats.effect.Outcome
import cats.effect.kernel.Async
import cats.effect.kernel.Poll
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.core.*

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

final case class ManagedHandle(
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    digest: ContentDigest
) derives CanEqual

enum ManagedSubmitResult derives CanEqual:
  case Created(handle: ManagedHandle)
  case Existing(handle: ManagedHandle)
  case Conflict(failure: ControlFailure.DigestConflict)
  case Failed(failure: ControlFailure)

enum AcceptanceSearchResult derives CanEqual:
  case Unique(job: JobRef, evidence: EvidenceBundle)
  case NoMatch(evidence: EvidenceBundle)
  case Ambiguous(candidates: Vector[JobRef], evidence: EvidenceBundle)
  case Unavailable(diagnostics: Diagnostics, evidence: EvidenceBundle)

trait AcceptanceSearch[F[_]]:
  def find(intent: ManagedIntent): F[AcceptanceSearchResult]

final class ManagedController[F[_]: Async](
    store: ControlStore[F],
    scheduler: Scheduler[F],
    requestPolicy: ManagedRequestPolicy = ManagedRequestPolicy.rejectEnvironmentValues
):
  /** Read the current durable attempt projection for a submission key. */
  def inspect(submissionKey: SubmissionKey): F[Option[ManagedAttempt]] =
    store.attempt(submissionKey)

  def submit(request: JobRequest[NoResult]): F[ManagedSubmitResult] =
    Clock[F].realTimeInstant.flatMap { now =>
      ManagedIntent.from(request, now, requestPolicy) match
        case Left(problem) =>
          ManagedSubmitResult
            .Failed(ControlFailure.RequestRejected(problem.diagnostics))
            .pure[F]
        case Right(intent) =>
          store.transact(ControlCommand.RecordIntent(intent)).map {
            case Right(ControlCommit(ControlResult.IntentCreated(attempt), _, _)) =>
              ManagedSubmitResult.Created(handle(attempt))
            case Right(ControlCommit(ControlResult.IntentExisting(attempt), _, _)) =>
              ManagedSubmitResult.Existing(handle(attempt))
            case Left(conflict: ControlFailure.DigestConflict) =>
              ManagedSubmitResult.Conflict(conflict)
            case Left(failure) => ManagedSubmitResult.Failed(failure)
            case Right(other)  =>
              ManagedSubmitResult.Failed(
                ControlFailure.JournalCorrupt(s"unexpected intent result: ${other.result}")
              )
          }
    }

  /** Observe one durable attempt and record the resulting evidence before returning it.
    *
    * The managed record remains the lifecycle authority: callers receive the post-commit attempt,
    * including unavailable/stale evidence, rather than a transport-local scheduler answer.
    */
  def observeSubmission(
      submissionKey: SubmissionKey
  ): F[Either[ControlFailure, ManagedAttempt]] =
    store.attempt(submissionKey).flatMap {
      case None          => Left(ControlFailure.AttemptNotFound(submissionKey)).pure[F]
      case Some(current) =>
        current.currentJob match
          case None      => Right(current).pure[F]
          case Some(job) =>
            val requested = cats.data.NonEmptyVector.one(job)
            for
              result <- scheduler.observe(requested)
              observedAt <- Clock[F].realTimeInstant
              committed <- store.transact(
                ControlCommand.RecordObservations(requested, result, observedAt)
              )
            yield committed.flatMap {
              case ControlCommit(ControlResult.Updated(attempts), _, _) =>
                attempts
                  .find(_.intent.submissionKey == submissionKey)
                  .toRight(
                    ControlFailure.JournalCorrupt(
                      s"observation commit omitted ${submissionKey.value}"
                    )
                  )
              case ControlCommit(ControlResult.NoChange(Some(attempt)), _, _) =>
                Right(attempt)
              case other =>
                Left(
                  ControlFailure.JournalCorrupt(
                    s"unexpected observation result: ${other.result}"
                  )
                )
            }
    }

  def dispatchSubmission(
      submissionKey: SubmissionKey
  ): F[Either[ControlFailure, ManagedAttempt]] =
    Clock[F].realTimeInstant.flatMap { claimedAt =>
      Async[F].uncancelable { poll =>
        store.transact(ControlCommand.ClaimSubmission(submissionKey, claimedAt)).flatMap {
          case Left(failure) => poll(Left(failure).pure[F])
          case Right(ControlCommit(ControlResult.SubmissionClaimed(attempt, _), _, _)) =>
            invokeSubmission(attempt, poll)
          case Right(ControlCommit(ControlResult.SubmissionAlreadyClaimed(attempt), _, _)) =>
            poll(Right(attempt).pure[F])
          case Right(other) =>
            poll(
              Left(
                ControlFailure.JournalCorrupt(
                  s"unexpected submission claim result: ${other.result}"
                )
              ).pure[F]
            )
        }
      }
    }

  def dispatchPending(maximum: Int): F[Vector[Either[ControlFailure, ManagedAttempt]]] =
    store
      .pendingOutbox(math.max(0, maximum))
      .flatMap(
        _.traverse { entry =>
          entry.action match
            case OutboxAction.Submit(key, _) => dispatchSubmission(key)
            case OutboxAction.Cancel(key, _) => dispatchCancellation(key)
        }
      )

  def recoverInFlight(
      maximum: Int = 1024
  ): F[Vector[Either[ControlFailure, ManagedAttempt]]] =
    store
      .nonTerminal(math.max(0, maximum))
      .flatMap(
        _.flatMap { attempt =>
          attempt.phase match
            case _: ManagedPhase.Submitting => Some(attempt -> true)
            case _ if attempt.cancellation.isInstanceOf[ManagedCancellation.Dispatching] =>
              Some(attempt -> false)
            case _ => None
        }.traverse { case (attempt, submission) =>
          Clock[F].realTimeInstant.flatMap { now =>
            val evidence = recoveryEvidence(
              now,
              if submission then "controller restarted while scheduler submission was in flight"
              else "controller restarted while cancellation was in flight"
            )
            val command =
              if submission then
                ControlCommand.RecoverSubmissionClaim(
                  attempt.intent.submissionKey,
                  attempt.intent.epoch,
                  evidence,
                  now
                )
              else
                ControlCommand.RecoverCancellationClaim(
                  attempt.intent.submissionKey,
                  evidence,
                  now
                )
            store
              .transact(command)
              .map(commitAttempt)
          }
        }
      )

  def reconcileUnknown(
      search: AcceptanceSearch[F],
      maximum: Int
  ): F[Vector[(SubmissionKey, AcceptanceSearchResult)]] =
    store
      .nonTerminal(math.max(0, maximum))
      .flatMap(
        _.collect {
          case attempt @ ManagedAttempt(_, _: ManagedPhase.AcceptanceUnknown, _, _, _, _, _) =>
            attempt
        }.traverse { attempt =>
          search.find(attempt.intent).flatMap {
            case found @ AcceptanceSearchResult.Unique(job, evidence) =>
              Clock[F].realTimeInstant.flatMap { now =>
                store
                  .transact(
                    ControlCommand.ReconcileBinding(
                      attempt.intent.submissionKey,
                      attempt.intent.epoch,
                      job,
                      evidence,
                      now
                    )
                  )
                  .flatMap {
                    case Right(_)      => (attempt.intent.submissionKey -> found).pure[F]
                    case Left(failure) =>
                      Async[F].raiseError[(SubmissionKey, AcceptanceSearchResult)](
                        new IllegalStateException(failure.toString)
                      )
                  }
              }
            case other => (attempt.intent.submissionKey -> other).pure[F]
          }
        }
      )

  def requestCancellation(
      submissionKey: SubmissionKey
  ): F[Either[ControlFailure, ManagedAttempt]] =
    Clock[F].realTimeInstant.flatMap { now =>
      store
        .transact(ControlCommand.RequestCancellation(submissionKey, now))
        .map(commitAttempt)
    }

  def retrySubmission(
      submissionKey: SubmissionKey,
      expectedEpoch: AttemptEpoch,
      authorization: RetryAuthorization
  ): F[Either[ControlFailure, ManagedAttempt]] =
    Clock[F].realTimeInstant.flatMap { now =>
      store
        .transact(
          ControlCommand.RetrySubmission(
            submissionKey,
            expectedEpoch,
            authorization,
            now
          )
        )
        .map(commitAttempt)
    }

  def dispatchCancellation(
      submissionKey: SubmissionKey
  ): F[Either[ControlFailure, ManagedAttempt]] =
    Clock[F].realTimeInstant.flatMap { claimedAt =>
      Async[F].uncancelable { poll =>
        store.transact(ControlCommand.ClaimCancellation(submissionKey, claimedAt)).flatMap {
          case Left(failure) => poll(Left(failure).pure[F])
          case Right(commit) =>
            commit.result match
              case ControlResult.CancellationQueued(attempt, outbox)
                  if outbox.status.isInstanceOf[OutboxStatus.InFlight] =>
                attempt.currentJob match
                  case Some(job) => invokeCancellation(submissionKey, job, poll)
                  case None      =>
                    val failure =
                      ControlFailure.OutboxInvariant(submissionKey, "job binding missing")
                    recoverCancellationClaim(
                      submissionKey,
                      "cancellation claim has no durable job binding"
                    ).as(Left(failure))
              case ControlResult.NoChange(Some(attempt)) =>
                poll(Right(attempt).pure[F])
              case other =>
                poll(
                  Left(
                    ControlFailure.JournalCorrupt(
                      s"unexpected cancellation claim result: $other"
                    )
                  ).pure[F]
                )
        }
      }
    }

  def eventStream(
      after: EventCursor,
      pageSize: Int,
      pollInterval: FiniteDuration
  ): Stream[F, CommittedEvent] =
    def loop(cursor: EventCursor): Stream[F, CommittedEvent] =
      Stream.eval(store.events(cursor, math.max(1, pageSize))).flatMap { page =>
        val values = Stream.emits(page.events).covary[F]
        if page.events.nonEmpty then values ++ loop(page.next)
        else values ++ Stream.eval(Async[F].sleep(pollInterval)).drain ++ loop(page.next)
      }
    loop(after)

  private def invokeSubmission(
      attempt: ManagedAttempt,
      poll: Poll[F]
  ): F[Either[ControlFailure, ManagedAttempt]] =
    attempt.intent.request.decode match
      case Left(problem) =>
        Async[F].raiseError(new IllegalStateException(s"durable request cannot decode: $problem"))
      case Right(request) =>
        val operation = poll(scheduler.submit(request)).flatMap { result =>
          Clock[F].realTimeInstant.flatMap { now =>
            store
              .transact(
                ControlCommand.RecordSubmission(
                  attempt.intent.submissionKey,
                  attempt.intent.epoch,
                  result,
                  now
                )
              )
              .map(commitAttempt)
          }
        }
        operation.guaranteeCase {
          case Outcome.Succeeded(_) => Async[F].unit
          case _                    => recoverSubmissionClaim(attempt)
        }

  private def invokeCancellation(
      submissionKey: SubmissionKey,
      job: JobRef,
      poll: Poll[F]
  ): F[Either[ControlFailure, ManagedAttempt]] =
    val operation = poll(scheduler.cancel(job)).flatMap { result =>
      Clock[F].realTimeInstant.flatMap { now =>
        store
          .transact(ControlCommand.RecordCancellation(submissionKey, result, now))
          .map(commitAttempt)
      }
    }
    operation.guaranteeCase {
      case Outcome.Succeeded(_) => Async[F].unit
      case _                    =>
        recoverCancellationClaim(
          submissionKey,
          "cancellation effect ended without a persisted result"
        )
    }

  private def recoverSubmissionClaim(attempt: ManagedAttempt): F[Unit] =
    Clock[F].realTimeInstant.flatMap { now =>
      store
        .transact(
          ControlCommand.RecoverSubmissionClaim(
            attempt.intent.submissionKey,
            attempt.intent.epoch,
            recoveryEvidence(now, "submission effect ended without a persisted result"),
            now
          )
        )
        .void
    }

  private def recoverCancellationClaim(
      submissionKey: SubmissionKey,
      message: String
  ): F[Unit] =
    Clock[F].realTimeInstant.flatMap { now =>
      store
        .transact(
          ControlCommand.RecoverCancellationClaim(
            submissionKey,
            recoveryEvidence(now, message),
            now
          )
        )
        .void
    }

  private def commitAttempt(
      result: Either[ControlFailure, ControlCommit]
  ): Either[ControlFailure, ManagedAttempt] = result.flatMap { commit =>
    commit.result match
      case ControlResult.Updated(Vector(single))          => Right(single)
      case ControlResult.CancellationQueued(attempt, _)   => Right(attempt)
      case ControlResult.SubmissionRetried(attempt, _, _) => Right(attempt)
      case ControlResult.NoChange(Some(attempt))          => Right(attempt)
      case other => Left(ControlFailure.JournalCorrupt(s"expected one attempt, received $other"))
  }

  private def handle(attempt: ManagedAttempt): ManagedHandle =
    ManagedHandle(
      attempt.intent.submissionKey,
      attempt.intent.attemptId,
      attempt.intent.request.digest
    )

  private def recoveryEvidence(at: java.time.Instant, message: String): EvidenceBundle =
    EvidenceBundle(
      BoundedEvidence.capture(
        EvidenceSource.DurableJournal,
        at,
        message.getBytes(StandardCharsets.UTF_8).toVector
      )
    )
