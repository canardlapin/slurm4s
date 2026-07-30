package io.github.bbuchsbaum.slurm4s.observability

import cats.Applicative
import cats.data.NonEmptyVector
import cats.effect.Clock
import cats.effect.Temporal
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant
import scala.concurrent.duration.*

enum SchedulerOperation derives CanEqual:
  case Capabilities
  case Submit
  case Observe
  case Accounting
  case Cancel

enum OperationalSubject derives CanEqual:
  case None
  case Submission(key: SubmissionKey)
  case Job(jobId: JobId, arrayIndex: Option[ArrayIndex])
  case Batch(jobCount: Int)

enum OperationalOutcome derives CanEqual:
  case Succeeded
  case Empty
  case Accepted
  case Rejected
  case AcceptanceUnknown
  case InvocationFailed
  case PreparationFailed
  case ParseFailed
  case CancellationAcknowledged
  case CancellationNotFound
  case CancellationRejected
  case CancellationUnknown
  case EffectFailed(className: String)

final case class SchedulerOperationEvent private[observability] (
    operation: SchedulerOperation,
    subject: OperationalSubject,
    completedAt: Instant,
    duration: DurationMillis,
    outcome: OperationalOutcome,
    diagnosticCodes: Vector[String]
) derives CanEqual

trait SchedulerTelemetrySink[F[_]]:
  def record(event: SchedulerOperationEvent): F[Unit]

object SchedulerTelemetrySink:
  def noop[F[_]: Applicative]: SchedulerTelemetrySink[F] =
    new SchedulerTelemetrySink[F]:
      def record(event: SchedulerOperationEvent): F[Unit] = Applicative[F].unit

final case class SchedulerTelemetryPolicy(
    sinkTimeout: DurationMillis,
    maximumDiagnosticCodes: PositiveInt
) derives CanEqual

object SchedulerTelemetryPolicy:
  val default: SchedulerTelemetryPolicy = SchedulerTelemetryPolicy(
    DurationMillis.unsafeFrom(100L),
    PositiveInt.unsafeFrom(16)
  )

final class TelemetryScheduler[F[_]: Temporal] private (
    delegate: Scheduler[F],
    sink: SchedulerTelemetrySink[F],
    policy: SchedulerTelemetryPolicy
) extends Scheduler[F]:
  def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
    measure(SchedulerOperation.Capabilities, OperationalSubject.None)(
      delegate.capabilities
    )(querySummary)

  def submit[A](request: JobRequest[A]): F[SubmissionAttempt] =
    measure(
      SchedulerOperation.Submit,
      OperationalSubject.Submission(request.submissionKey)
    )(delegate.submit(request))(submissionSummary)

  def observe(
      jobs: NonEmptyVector[JobRef]
  ): F[SchedulerQueryResult[ObservationBatch]] =
    measure(
      SchedulerOperation.Observe,
      OperationalSubject.Batch(jobs.toVector.size)
    )(delegate.observe(jobs))(querySummary)

  def accounting(
      jobs: NonEmptyVector[JobRef]
  ): F[SchedulerQueryResult[AccountingBatch]] =
    measure(
      SchedulerOperation.Accounting,
      OperationalSubject.Batch(jobs.toVector.size)
    )(delegate.accounting(jobs))(querySummary)

  def cancel(job: JobRef): F[CancellationAttempt] =
    measure(
      SchedulerOperation.Cancel,
      OperationalSubject.Job(job.jobId, job.arrayIndex)
    )(delegate.cancel(job))(cancellationSummary)

  private def measure[A](
      operation: SchedulerOperation,
      subject: OperationalSubject
  )(effect: F[A])(summarize: A => Summary): F[A] =
    for
      started <- Clock[F].monotonic
      attempted <- effect.attempt
      ended <- Clock[F].monotonic
      completedAt <- Clock[F].realTimeInstant
      summary = attempted.fold(
        error => Summary(OperationalOutcome.EffectFailed(error.getClass.getName), Vector.empty),
        summarize
      )
      event = SchedulerOperationEvent(
        operation,
        subject,
        completedAt,
        duration(ended - started),
        summary.outcome,
        summary.diagnosticCodes.take(policy.maximumDiagnosticCodes.toInt)
      )
      _ <- recordBestEffort(event)
      result <- attempted.fold(Temporal[F].raiseError, _.pure[F])
    yield result

  private def recordBestEffort(event: SchedulerOperationEvent): F[Unit] =
    Temporal[F]
      .timeoutTo(
        sink.record(event),
        policy.sinkTimeout.value.millis,
        Temporal[F].unit
      )
      .attempt
      .void

  private def duration(value: FiniteDuration): DurationMillis =
    DurationMillis.unsafeFrom(math.max(0L, value.toMillis))

  private def submissionSummary(value: SubmissionAttempt): Summary = value match
    case SubmissionAttempt.Completed(Submission.Accepted(_, _)) =>
      Summary(OperationalOutcome.Accepted, Vector.empty)
    case SubmissionAttempt.Completed(Submission.Rejected(diagnostics, _)) =>
      Summary(OperationalOutcome.Rejected, codes(diagnostics))
    case SubmissionAttempt.Completed(Submission.AcceptanceUnknown(_, _)) =>
      Summary(OperationalOutcome.AcceptanceUnknown, Vector.empty)
    case SubmissionAttempt.InvocationFailed(result) =>
      Summary(OperationalOutcome.InvocationFailed, invocationCodes(result))
    case SubmissionAttempt.PreparationFailed(diagnostics) =>
      Summary(OperationalOutcome.PreparationFailed, codes(diagnostics))

  private def querySummary[A](value: SchedulerQueryResult[A]): Summary = value match
    case SchedulerQueryResult.Succeeded(_) =>
      Summary(OperationalOutcome.Succeeded, Vector.empty)
    case SchedulerQueryResult.Empty(_, _) =>
      Summary(OperationalOutcome.Empty, Vector.empty)
    case SchedulerQueryResult.InvocationFailed(result) =>
      Summary(OperationalOutcome.InvocationFailed, invocationCodes(result))
    case SchedulerQueryResult.ParseFailed(diagnostics, _) =>
      Summary(OperationalOutcome.ParseFailed, codes(diagnostics))

  private def cancellationSummary(value: CancellationAttempt): Summary = value match
    case CancellationAttempt.Completed(CancellationResult.Acknowledged(_)) =>
      Summary(OperationalOutcome.CancellationAcknowledged, Vector.empty)
    case CancellationAttempt.Completed(CancellationResult.NotFound(_)) =>
      Summary(OperationalOutcome.CancellationNotFound, Vector.empty)
    case CancellationAttempt.Completed(CancellationResult.Rejected(diagnostics, _)) =>
      Summary(OperationalOutcome.CancellationRejected, codes(diagnostics))
    case CancellationAttempt.Completed(CancellationResult.Unknown(diagnostics, _)) =>
      Summary(OperationalOutcome.CancellationUnknown, codes(diagnostics))
    case CancellationAttempt.InvocationFailed(result) =>
      Summary(OperationalOutcome.InvocationFailed, invocationCodes(result))

  private def invocationCodes(value: InvocationResult): Vector[String] = value match
    case InvocationResult.SpawnFailed(_, diagnostics, _) => codes(diagnostics)
    case _                                               => Vector.empty

  private def codes(value: Diagnostics): Vector[String] =
    value.toVector.map(_.code).distinct.sorted

  final private case class Summary(
      outcome: OperationalOutcome,
      diagnosticCodes: Vector[String]
  )

object TelemetryScheduler:
  def apply[F[_]: Temporal](
      delegate: Scheduler[F],
      sink: SchedulerTelemetrySink[F],
      policy: SchedulerTelemetryPolicy = SchedulerTelemetryPolicy.default
  ): TelemetryScheduler[F] =
    new TelemetryScheduler(delegate, sink, policy)
