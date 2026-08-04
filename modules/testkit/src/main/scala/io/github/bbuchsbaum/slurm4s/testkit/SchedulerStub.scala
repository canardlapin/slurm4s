package io.github.bbuchsbaum.slurm4s.testkit

import cats.ApplicativeThrow
import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

/** A reusable scheduler program for tests that need fixed values, callbacks, or effect failures. */
final case class SchedulerProgram[F[_]](
    capabilities: F[SchedulerQueryResult[SchedulerCapabilities]],
    submit: SubmissionKey => F[SubmissionAttempt],
    observe: NonEmptyVector[JobRef] => F[SchedulerQueryResult[ObservationBatch]],
    accounting: NonEmptyVector[JobRef] => F[SchedulerQueryResult[AccountingBatch]],
    cancel: JobRef => F[CancellationAttempt]
):
  def scheduler: Scheduler[F] = new Scheduler[F]:
    def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
      SchedulerProgram.this.capabilities

    def submit(spec: LaunchSpec): F[SubmissionAttempt] =
      SchedulerProgram.this.submit(spec.submissionKey)

    def observe(
        jobs: NonEmptyVector[JobRef]
    ): F[SchedulerQueryResult[ObservationBatch]] =
      SchedulerProgram.this.observe(jobs)

    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): F[SchedulerQueryResult[AccountingBatch]] =
      SchedulerProgram.this.accounting(jobs)

    def cancel(job: JobRef): F[CancellationAttempt] =
      SchedulerProgram.this.cancel(job)

object SchedulerProgram:
  def unexpected[F[_]: ApplicativeThrow](context: String): SchedulerProgram[F] =
    def fail[A](operation: String): F[A] =
      new AssertionError(s"$context: unexpected scheduler operation: $operation").raiseError[F, A]

    SchedulerProgram(
      fail("capabilities"),
      key => fail(s"submit ${key.value}"),
      jobs => fail(s"observe ${jobs.toVector}"),
      jobs => fail(s"accounting ${jobs.toVector}"),
      job => fail(s"cancel $job")
    )

enum SchedulerOperation derives CanEqual:
  case Capabilities
  case Submit(submissionKey: SubmissionKey)
  case Observe(jobs: Vector[JobRef])
  case Accounting(jobs: Vector[JobRef])
  case Cancel(job: JobRef)

enum SchedulerScriptStep:
  case Capabilities(result: SchedulerQueryResult[SchedulerCapabilities])
  case Submit(submissionKey: SubmissionKey, result: SubmissionAttempt)
  case Observe(jobs: Vector[JobRef], result: SchedulerQueryResult[ObservationBatch])
  case Accounting(jobs: Vector[JobRef], result: SchedulerQueryResult[AccountingBatch])
  case Cancel(job: JobRef, result: CancellationAttempt)

  def operation: SchedulerOperation = this match
    case Capabilities(_)     => SchedulerOperation.Capabilities
    case Submit(key, _)      => SchedulerOperation.Submit(key)
    case Observe(jobs, _)    => SchedulerOperation.Observe(jobs)
    case Accounting(jobs, _) => SchedulerOperation.Accounting(jobs)
    case Cancel(job, _)      => SchedulerOperation.Cancel(job)
