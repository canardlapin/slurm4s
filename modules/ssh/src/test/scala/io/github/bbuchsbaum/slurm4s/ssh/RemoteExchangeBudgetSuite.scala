package io.github.bbuchsbaum.slurm4s.ssh

import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.PositiveInt

/** P8.G2: a shared ceiling on simultaneous remote exchanges.
  *
  * ADR 0003 gives every request its own SSH process, and a batch awaits its elements with
  * `parTraverse`, so a 100-element batch previously opened 100 concurrent processes per tick — and
  * a 4,096-element one opened 4,096 — against a login node shared with other users.
  */
class RemoteExchangeBudgetSuite extends munit.CatsEffectSuite:

  /** Run `count` exchanges concurrently and report the high-water mark of simultaneous ones. */
  private def peakConcurrency(
      budget: RemoteExchangeBudget[IO],
      count: Int
  ): IO[Int] =
    for
      inFlight <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      _ <- (1 to count).toList.parTraverse { _ =>
        budget.use(
          inFlight
            .updateAndGet(_ + 1)
            .flatMap(current => peak.update(_ max current)) *>
            IO.sleep(scala.concurrent.duration.DurationInt(5).millis) *>
            inFlight.update(_ - 1)
        )
      }
      observed <- peak.get
    yield observed

  test("a shared budget bounds simultaneous exchanges") {
    peakConcurrency(RemoteExchangeBudget.unbounded[IO], 1).map(peak => assertEquals(peak, 1)) *>
      RemoteExchangeBudget
        .of[IO](PositiveInt.unsafeFrom(4))
        .flatMap(peakConcurrency(_, 32))
        .map(peak => assert(peak <= 4, s"a budget of 4 admitted $peak simultaneous exchanges"))
  }

  test("every exchange still runs; the budget queues rather than drops") {
    for
      budget <- RemoteExchangeBudget.of[IO](PositiveInt.unsafeFrom(3))
      completed <- Ref.of[IO, Int](0)
      _ <- (1 to 50).toList.parTraverse(_ => budget.use(completed.update(_ + 1)))
      total <- completed.get
    yield assertEquals(total, 50)
  }

  test("an unbounded budget does not serialize a lone caller") {
    peakConcurrency(RemoteExchangeBudget.unbounded[IO], 8).map(peak =>
      assert(peak > 1, "an unbounded budget must not impose a ceiling")
    )
  }
