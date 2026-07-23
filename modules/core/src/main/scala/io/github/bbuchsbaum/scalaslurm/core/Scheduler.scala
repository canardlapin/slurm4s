package io.github.bbuchsbaum.scalaslurm.core

import cats.data.NonEmptyVector

final case class ObservationBatch(results: NonEmptyVector[ObservationResult]) derives CanEqual
final case class AccountingBatch(records: NonEmptyVector[AccountingRecord], missing: Vector[JobRef])
    derives CanEqual

/** Transport-neutral scheduler semantics. Routine operational failures are returned as values. */
trait Scheduler[F[_]]:
  def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]]
  def submit[A](request: JobRequest[A]): F[SubmissionAttempt]
  def observe(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[ObservationBatch]]
  def accounting(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[AccountingBatch]]
  def cancel(job: JobRef): F[CancellationAttempt]
