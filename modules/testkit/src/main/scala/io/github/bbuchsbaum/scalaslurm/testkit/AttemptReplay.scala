package io.github.bbuchsbaum.scalaslurm.testkit

import io.github.bbuchsbaum.scalaslurm.core.AttemptReducer
import io.github.bbuchsbaum.scalaslurm.core.AttemptState
import io.github.bbuchsbaum.scalaslurm.core.AttemptTransitionFailure
import io.github.bbuchsbaum.scalaslurm.core.VersionedAttemptEvent

object AttemptReplay:
  def replay(
      initial: AttemptState,
      events: Vector[VersionedAttemptEvent]
  ): Either[AttemptTransitionFailure, AttemptState] =
    events.foldLeft[Either[AttemptTransitionFailure, AttemptState]](Right(initial)) {
      case (state, event) => state.flatMap(AttemptReducer.reduce(_, event))
    }
