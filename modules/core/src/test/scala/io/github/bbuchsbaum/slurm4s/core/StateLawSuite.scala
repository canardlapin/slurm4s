package io.github.bbuchsbaum.slurm4s.core

import org.scalacheck.Prop.forAll

/** P8.B2 law family 3: evidence classification is total and consistent.
  *
  * Three classifiers read `SlurmState` for different questions. They must agree about which states
  * are finished, and none may be partial — a `MatchError` here is a crash on live scheduler output,
  * which is exactly what `-Werror` caught in P8.B1 after P8.A2 added three states.
  */
class StateLawSuite extends munit.ScalaCheckSuite:
  import Generators.*

  property("terminality is total") {
    forAll(slurmState) { state =>
      Terminality.of(state) match
        case Terminality.Terminal | Terminality.Active | Terminality.Indeterminate => true
    }
  }

  property("interruption classification is total") {
    forAll(slurmState)(state => InterruptionClass.classify(state) != null)
  }

  property("only an unrecognized state is indeterminate") {
    forAll(slurmState) { state =>
      val indeterminate = Terminality.of(state) == Terminality.Indeterminate
      val unknown = state match
        case SlurmState.Unknown(_) => true
        case _                     => false
      indeterminate == unknown
    }
  }

  property("a terminal state is never classified as uninterrupted unless it completed") {
    forAll(slurmState) { state =>
      val terminal = Terminality.of(state) == Terminality.Terminal
      val uninterrupted = InterruptionClass.classify(state) == InterruptionClass.NotInterrupted
      !terminal || !uninterrupted || state == SlurmState.Completed
    }
  }

  property("a requeueing state is never terminal") {
    forAll(slurmState) { state =>
      InterruptionClass.classify(state) != InterruptionClass.Requeueing ||
      Terminality.of(state) != Terminality.Terminal
    }
  }
