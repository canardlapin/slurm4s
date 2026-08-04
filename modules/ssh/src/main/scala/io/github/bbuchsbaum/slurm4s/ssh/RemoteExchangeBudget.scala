package io.github.bbuchsbaum.slurm4s.ssh

import cats.effect.kernel.Concurrent
import cats.effect.std.Semaphore
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.PositiveInt

/** A shared ceiling on how many remote exchanges may be in flight at once.
  *
  * ADR 0003 gives every request its own SSH process rather than multiplexing, so each poll costs
  * one process and one remote agent start. That is affordable for one handle and not for many: a
  * batch awaits with `elements.parTraverse(_.await)`, and every element runs its own polling loop,
  * so a 100-element batch opened 100 concurrent SSH processes per tick and a 4,096-element one
  * opened 4,096 — against a login node that has other users.
  *
  * Handles that share a budget queue behind it instead. The work is unchanged and no result is
  * missed; only the number of simultaneous processes is bounded. This is a ceiling, not batching:
  * genuinely batching the reads needs a protocol method that reads several results in one exchange,
  * which is a wire change and belongs with the batched-read work.
  */
final class RemoteExchangeBudget[F[_]: Concurrent] private (permits: Option[Semaphore[F]]):

  /** Run one remote exchange, waiting for a permit when the budget is exhausted. */
  def use[A](exchange: F[A]): F[A] =
    permits.fold(exchange)(_.permit.use(_ => exchange))

object RemoteExchangeBudget:

  /** Bound concurrent exchanges to `maximumConcurrent`. */
  def of[F[_]: Concurrent](maximumConcurrent: PositiveInt): F[RemoteExchangeBudget[F]] =
    Semaphore[F](maximumConcurrent.toInt.toLong).map(value =>
      new RemoteExchangeBudget[F](Some(value))
    )

  /** No ceiling. Correct for a single handle, which can only have one exchange in flight anyway. */
  def unbounded[F[_]: Concurrent]: RemoteExchangeBudget[F] =
    new RemoteExchangeBudget[F](None)
