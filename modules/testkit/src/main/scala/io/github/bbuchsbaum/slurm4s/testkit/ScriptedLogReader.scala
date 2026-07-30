package io.github.bbuchsbaum.slurm4s.testkit

import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

final case class LogReadCall(ref: LogRef, cursor: LogCursor, maximumBytes: ByteLimit)
    derives CanEqual

final case class ExpectedLogRead(call: LogReadCall, result: LogReadResult) derives CanEqual

/** An ordered log-read script that can be adapted to any interpreter's log-reader algebra. */
final class ScriptedLogReader[F[_]: Sync] private (
    remainingRef: Ref[F, Vector[ExpectedLogRead]],
    observedRef: Ref[F, Vector[LogReadCall]]
):
  def read(ref: LogRef, cursor: LogCursor, maximumBytes: ByteLimit): F[LogReadResult] =
    val call = LogReadCall(ref, cursor, maximumBytes)
    observedRef.update(_ :+ call) *>
      remainingRef
        .modify {
          case head +: tail if head.call == call => tail -> Right(head.result)
          case steps @ (head +: _)               =>
            steps -> Left(
              new AssertionError(
                s"log read mismatch: expected ${head.call}, received $call"
              )
            )
          case empty =>
            empty -> Left(new AssertionError(s"log read script exhausted by $call"))
        }
        .flatMap(_.liftTo[F])

  def remaining: F[Vector[ExpectedLogRead]] = remainingRef.get
  def observed: F[Vector[LogReadCall]] = observedRef.get

object ScriptedLogReader:
  def create[F[_]: Sync](steps: Vector[ExpectedLogRead]): F[ScriptedLogReader[F]] =
    (
      Ref.of[F, Vector[ExpectedLogRead]](steps),
      Ref.of[F, Vector[LogReadCall]](Vector.empty)
    ).mapN(new ScriptedLogReader(_, _))
