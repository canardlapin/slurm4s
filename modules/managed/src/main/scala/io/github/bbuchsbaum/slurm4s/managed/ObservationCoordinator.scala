package io.github.bbuchsbaum.slurm4s.managed

import cats.Applicative
import cats.data.NonEmptyVector
import cats.effect.Clock
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.kernel.Sync
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.slurm4s.core.*

import scala.concurrent.duration.*

final case class ObservationPolicy(
    maximumBatchSize: Int,
    minimumInterval: FiniteDuration,
    maximumInterval: FiniteDuration,
    jitterFraction: Double,
    focusedProbeLimit: Int
) derives CanEqual:
  require(maximumBatchSize > 0, "maximum batch size must be positive")
  require(minimumInterval > Duration.Zero, "minimum interval must be positive")
  require(maximumInterval >= minimumInterval, "maximum interval must cover minimum interval")
  require(jitterFraction >= 0.0 && jitterFraction <= 1.0, "jitter fraction must be in [0,1]")
  require(focusedProbeLimit >= 0, "focused probe limit must not be negative")

object ObservationPolicy:
  val default: ObservationPolicy = ObservationPolicy(
    maximumBatchSize = 256,
    minimumInterval = 2.seconds,
    maximumInterval = 30.seconds,
    jitterFraction = 0.2,
    focusedProbeLimit = 16
  )

trait Jitter[F[_]]:
  def nextUnit: F[Double]

object Jitter:
  def fixed[F[_]: Applicative](value: Double): Jitter[F] = new Jitter[F]:
    def nextUnit: F[Double] = math.max(0.0, math.min(1.0, value)).pure[F]

trait FocusedReconciler[F[_]]:
  def inspect(jobs: NonEmptyVector[JobRef]): F[Unit]

object FocusedReconciler:
  def noop[F[_]: Applicative]: FocusedReconciler[F] = new FocusedReconciler[F]:
    def inspect(jobs: NonEmptyVector[JobRef]): F[Unit] = ().pure[F]

final case class ObserverTick(
    site: SiteId,
    activeCandidates: Int,
    observedJobs: Int,
    accountingCandidates: Int,
    focusedCandidates: Int
) derives CanEqual

final class ObservationCoordinator[F[_]: Temporal](
    val site: SiteId,
    store: ControlStore[F],
    scheduler: Scheduler[F],
    focused: FocusedReconciler[F],
    jitter: Jitter[F],
    policy: ObservationPolicy = ObservationPolicy.default
):
  def tick: F[ObserverTick] =
    boundCandidates.flatMap {
      case Vector()   => ObserverTick(site, 0, 0, 0, 0).pure[F]
      case candidates =>
        val jobs = NonEmptyVector.fromVectorUnsafe(candidates.map(_._2))
        for
          observations <- scheduler.observe(jobs)
          observedAt <- Clock[F].realTimeInstant
          observationCommit <- store.transact(
            ControlCommand.RecordObservations(jobs, observations, observedAt)
          )
          _ <- observationCommit
            .leftMap(failure => new IllegalStateException(failure.toString))
            .liftTo[F]
          terminal = terminalCandidates(observations)
          accountingStats <- NonEmptyVector.fromVector(terminal) match
            case None         => (0, 0).pure[F]
            case Some(values) =>
              scheduler.accounting(values).flatMap { accounting =>
                val succeeded = accountingSucceeded(accounting)
                Clock[F].realTimeInstant.flatMap { accountedAt =>
                  store
                    .transact(ControlCommand.RecordAccounting(values, accounting, accountedAt))
                    .flatMap {
                      case Left(failure) =>
                        Temporal[F].raiseError[Unit](new IllegalStateException(failure.toString))
                      case Right(_) =>
                        if succeeded then ().pure[F]
                        else focusedFallback(values)
                    }
                    .as(
                      values.length ->
                        (if succeeded then 0 else math.min(values.length, policy.focusedProbeLimit))
                    )
                }
              }
        yield ObserverTick(
          site,
          activeCandidates = candidates.size,
          observedJobs = observations match
            case SchedulerQueryResult.Succeeded(batch) => batch.results.length.toInt
            case _                                     => 0,
          accountingCandidates = accountingStats._1,
          focusedCandidates = accountingStats._2
        )
    }

  def stream: Stream[F, ObserverTick] =
    Stream.repeatEval(
      tick.flatTap(result => nextDelay(result).flatMap(Temporal[F].sleep))
    )

  def uniqueStream(registry: SiteObserverRegistry[F]): Stream[F, ObserverTick] =
    Stream.resource(registry.lease(site)).flatMap(_ => stream)

  private def boundCandidates: F[Vector[(SubmissionKey, JobRef)]] =
    store
      .bound(policy.maximumBatchSize)
      .map(
        _.flatMap(attempt => attempt.currentJob.map(job => attempt.intent.submissionKey -> job))
      )

  private def terminalCandidates(
      result: SchedulerQueryResult[ObservationBatch]
  ): Vector[JobRef] = result match
    case SchedulerQueryResult.Succeeded(batch) =>
      batch.results.toVector.collect {
        case ObservationResult.Observed(value) if terminalState(value.state) => value.job
      }
    case _ => Vector.empty

  private def terminalState(state: SlurmState): Boolean = state match
    case SlurmState.Completed | SlurmState.Failed | SlurmState.Cancelled | SlurmState.OutOfMemory |
        SlurmState.TimedOut | SlurmState.NodeFailure | SlurmState.Preempted =>
      true
    case _ => false

  private def accountingSucceeded(result: SchedulerQueryResult[AccountingBatch]): Boolean =
    result.isInstanceOf[SchedulerQueryResult.Succeeded[?]]

  private def focusedFallback(jobs: NonEmptyVector[JobRef]): F[Unit] =
    NonEmptyVector
      .fromVector(jobs.toVector.take(policy.focusedProbeLimit))
      .fold(().pure[F])(focused.inspect)

  private[managed] def nextDelay(result: ObserverTick): F[FiniteDuration] =
    val base =
      if result.activeCandidates == 0 then policy.maximumInterval
      else policy.minimumInterval
    jitter.nextUnit.map { sample =>
      val centered = (math.max(0.0, math.min(1.0, sample)) * 2.0) - 1.0
      val multiplier = 1.0 + centered * policy.jitterFraction
      val nanos = math.max(1L, (base.toNanos.toDouble * multiplier).toLong)
      nanos.nanos
    }

final class SiteObserverRegistry[F[_]: Sync] private (active: Ref[F, Set[SiteId]]):
  def lease(site: SiteId): Resource[F, Unit] =
    Resource.make(
      active
        .modify { sites =>
          if sites.contains(site) then
            sites -> Left(
              new IllegalStateException(s"an observer is already active for ${site.value}")
            )
          else (sites + site) -> Right(())
        }
        .flatMap(_.liftTo[F])
    )(_ => active.update(_ - site))

object SiteObserverRegistry:
  def create[F[_]: Sync]: F[SiteObserverRegistry[F]] =
    Ref.of[F, Set[SiteId]](Set.empty).map(SiteObserverRegistry(_))
