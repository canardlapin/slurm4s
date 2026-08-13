package io.github.bbuchsbaum.slurm4s.testkit

import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

final case class QueueReadCall(query: QueueQuery, page: Page) derives CanEqual

final case class ExpectedQueueRead(
    call: QueueReadCall,
    result: SchedulerQueryResult[QueuePage]
) derives CanEqual

/** An ordered queue-discovery program, separate from the known-job scheduler script. */
final class ScriptedQueueReader[F[_]: Sync] private (
    remainingRef: Ref[F, Vector[ExpectedQueueRead]],
    observedRef: Ref[F, Vector[QueueReadCall]]
) extends QueueReader[F]:
  def listJobs(
      query: QueueQuery,
      page: Page
  ): F[SchedulerQueryResult[QueuePage]] =
    val call = QueueReadCall(query, page)
    observedRef.update(_ :+ call) *>
      remainingRef
        .modify {
          case head +: tail if head.call == call => tail -> Right(head.result)
          case steps @ (head +: _)               =>
            steps -> Left(
              new AssertionError(
                s"queue call mismatch: expected ${head.call}, received $call"
              )
            )
          case empty =>
            empty -> Left(new AssertionError(s"queue script exhausted by $call"))
        }
        .flatMap(_.liftTo[F])

  def remaining: F[Vector[ExpectedQueueRead]] = remainingRef.get
  def observed: F[Vector[QueueReadCall]] = observedRef.get

  def assertDrained: F[Unit] =
    remaining.flatMap {
      case Vector() => ().pure[F]
      case steps    =>
        new AssertionError(s"unconsumed queue steps: ${steps.map(_.call).mkString(", ")}")
          .raiseError[F, Unit]
    }

object ScriptedQueueReader:
  def create[F[_]: Sync](steps: Vector[ExpectedQueueRead]): F[ScriptedQueueReader[F]] =
    (
      Ref.of[F, Vector[ExpectedQueueRead]](steps),
      Ref.of[F, Vector[QueueReadCall]](Vector.empty)
    ).mapN(new ScriptedQueueReader(_, _))
