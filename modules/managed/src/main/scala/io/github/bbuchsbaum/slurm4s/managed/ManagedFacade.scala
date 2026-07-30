package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.Async
import cats.effect.Resource
import io.github.bbuchsbaum.slurm4s.core.Scheduler

import java.nio.file.Path

final case class ManagedConfig(
    journalLimits: JournalLimits = JournalLimits.default,
    requestPolicy: ManagedRequestPolicy = ManagedRequestPolicy.rejectEnvironmentValues
) derives CanEqual

object Managed:
  def durable[F[_]: Async](
      journal: Path,
      scheduler: Scheduler[F],
      config: ManagedConfig = ManagedConfig()
  ): Resource[F, ManagedController[F]] =
    FileJournalControlStore
      .open[F](journal, config.journalLimits)
      .map(store => ManagedController[F](store, scheduler, config.requestPolicy))
