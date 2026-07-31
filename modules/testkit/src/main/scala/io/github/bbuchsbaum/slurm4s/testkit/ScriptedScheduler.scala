package io.github.bbuchsbaum.slurm4s.testkit

import cats.data.NonEmptyVector
import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

/** An ordered scheduler script. Calls are traced, and mismatch or exhaustion fails the test. */
final class ScriptedScheduler[F[_]: Sync] private (
    remainingRef: Ref[F, Vector[SchedulerScriptStep]],
    observedRef: Ref[F, Vector[SchedulerOperation]]
) extends Scheduler[F]:
  def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
    next(SchedulerOperation.Capabilities).flatMap {
      case SchedulerScriptStep.Capabilities(result) => result.pure[F]
      case step                                     => impossible(step)
    }

  def submit(spec: LaunchSpec): F[SubmissionAttempt] =
    next(SchedulerOperation.Submit(spec.submissionKey)).flatMap {
      case SchedulerScriptStep.Submit(_, result) => result.pure[F]
      case step                                  => impossible(step)
    }

  def observe(
      jobs: NonEmptyVector[JobRef]
  ): F[SchedulerQueryResult[ObservationBatch]] =
    next(SchedulerOperation.Observe(jobs.toVector)).flatMap {
      case SchedulerScriptStep.Observe(_, result) => result.pure[F]
      case step                                   => impossible(step)
    }

  def accounting(
      jobs: NonEmptyVector[JobRef]
  ): F[SchedulerQueryResult[AccountingBatch]] =
    next(SchedulerOperation.Accounting(jobs.toVector)).flatMap {
      case SchedulerScriptStep.Accounting(_, result) => result.pure[F]
      case step                                      => impossible(step)
    }

  def cancel(job: JobRef): F[CancellationAttempt] =
    next(SchedulerOperation.Cancel(job)).flatMap {
      case SchedulerScriptStep.Cancel(_, result) => result.pure[F]
      case step                                  => impossible(step)
    }

  def remaining: F[Vector[SchedulerScriptStep]] = remainingRef.get
  def observed: F[Vector[SchedulerOperation]] = observedRef.get

  def assertDrained: F[Unit] =
    remaining.flatMap {
      case Vector() => ().pure[F]
      case steps    =>
        new AssertionError(s"unconsumed scheduler steps: ${steps.map(_.operation).mkString(", ")}")
          .raiseError[F, Unit]
    }

  private def next(operation: SchedulerOperation): F[SchedulerScriptStep] =
    observedRef.update(_ :+ operation) *>
      remainingRef
        .modify {
          case head +: tail if head.operation == operation => tail -> Right(head)
          case steps @ (head +: _)                         =>
            steps -> Left(
              new AssertionError(
                s"scheduler call mismatch: expected ${head.operation}, received $operation"
              )
            )
          case empty =>
            empty -> Left(new AssertionError(s"scheduler script exhausted by $operation"))
        }
        .flatMap(_.liftTo[F])

  private def impossible[A](step: SchedulerScriptStep): F[A] =
    new AssertionError(s"internal scheduler script mismatch after ${step.operation}")
      .raiseError[F, A]

object ScriptedScheduler:
  def create[F[_]: Sync](steps: Vector[SchedulerScriptStep]): F[ScriptedScheduler[F]] =
    (
      Ref.of[F, Vector[SchedulerScriptStep]](steps),
      Ref.of[F, Vector[SchedulerOperation]](Vector.empty)
    ).mapN(new ScriptedScheduler(_, _))
