package io.github.bbuchsbaum.slurm4s.cli

import io.github.bbuchsbaum.slurm4s.core.DurationMillis
import io.github.bbuchsbaum.slurm4s.core.Freshness
import io.github.bbuchsbaum.slurm4s.core.JobObservation

object ObservationSemantics:
  def markStale(observation: JobObservation, age: DurationMillis): JobObservation =
    val observedAt = observation.freshness match
      case Freshness.Current(value)    => value
      case Freshness.Stale(value, _)   => value
      case Freshness.Unknown(value, _) => value
    observation.copy(freshness = Freshness.Stale(observedAt, age))
