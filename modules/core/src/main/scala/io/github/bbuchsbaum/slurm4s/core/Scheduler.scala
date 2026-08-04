package io.github.bbuchsbaum.slurm4s.core

import cats.data.NonEmptyVector

final case class ObservationBatch(results: NonEmptyVector[ObservationResult]) derives CanEqual
final case class AccountingBatch(records: NonEmptyVector[AccountingRecord], missing: Vector[JobRef])
    derives CanEqual

/** Transport-neutral scheduler semantics. Routine operational failures are returned as values. */
trait Scheduler[F[_]]:
  def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]]
  def submit(spec: LaunchSpec): F[SubmissionAttempt]
  def observe(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[ObservationBatch]]
  def accounting(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[AccountingBatch]]
  def cancel(job: JobRef): F[CancellationAttempt]

object Scheduler:

  extension [F[_]](scheduler: Scheduler[F])

    /** Lower a script request and submit it.
      *
      * A convenience for callers that already hold a `JobRequest`, and deliberately not a return of
      * the erased type parameter: the result is still an untyped `SubmissionAttempt`, and a payload
      * that cannot be lowered here fails as a preparation failure rather than being quietly
      * rewritten. Callers who need a typed result hold the contract themselves.
      */
    def submitLowered[A](request: JobRequest[A])(using
        applicative: cats.Applicative[F]
    ): F[SubmissionAttempt] =
      LaunchSpec.fromRequest(request) match
        case Left(diagnostics) =>
          applicative.pure(SubmissionAttempt.PreparationFailed(diagnostics))
        case Right(spec) => scheduler.submit(spec)
