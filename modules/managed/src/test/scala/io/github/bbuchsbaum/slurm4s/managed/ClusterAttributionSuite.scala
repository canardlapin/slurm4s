package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*

/** P8.A1: a reported cluster is evidence, not operational identity.
  *
  * `sbatch --parsable` yields a bare job id on a non-federated site, so a bound attempt carries no
  * cluster, while `squeue` JSON reports one and the parser copies it onto the observation. Matching
  * on whole-`JobRef` equality therefore misses every row, and the attempt never advances.
  */
class ClusterAttributionSuite extends munit.FunSuite:
  import ManagedTestSupport.*

  private val alpha: ClusterName = ClusterName.unsafeFrom("alpha")
  private val beta: ClusterName = ClusterName.unsafeFrom("beta")
  private val reported: JobRef = job.copy(cluster = Some(alpha))

  test("observation reported with a cluster is applied to an attempt bound without one") {
    val bound = boundState("cluster-observe")
    val current = applyCommand(
      bound,
      ControlCommand.RecordObservations(
        NonEmptyVector.one(job),
        SchedulerQueryResult.Succeeded(
          ObservationBatch(
            NonEmptyVector.one(
              ObservationResult.Observed(
                JobObservation(
                  reported,
                  SlurmState.Running,
                  Freshness.Current(later),
                  None,
                  Map.empty,
                  evidence
                )
              )
            )
          )
        ),
        later.plusSeconds(2L)
      )
    )
    val attempt = current.state.attempts.values.head

    assert(
      attempt.observation.isInstanceOf[ManagedObservation.Current],
      s"observation was dropped: ${attempt.observation}"
    )

  }

  test("accounting reported with a cluster still reaches terminal") {
    val bound = boundState("cluster-accounting")
    val record = AccountingRecord(
      reported,
      SlurmState.Completed,
      Some(ExitStatus(0, None)),
      Some(WorkloadOutcome.Completed(0)),
      Freshness.Current(later),
      Map.empty,
      evidence
    )
    val terminal = applyCommand(
      bound,
      ControlCommand.RecordAccounting(
        NonEmptyVector.one(job),
        SchedulerQueryResult.Succeeded(AccountingBatch(NonEmptyVector.one(record), Vector.empty)),
        later.plusSeconds(4L)
      )
    )
    val attempt = terminal.state.attempts.values.head

    assert(
      attempt.phase.isInstanceOf[ManagedPhase.Terminal],
      s"attempt never terminated: ${attempt.phase}"
    )
  }

  test("two known and different clusters are different jobs and never alias") {
    val bound = boundStateOn("cluster-conflict", job.copy(cluster = Some(beta)))
    val current = applyCommand(
      bound,
      ControlCommand.RecordObservations(
        NonEmptyVector.one(job.copy(cluster = Some(beta))),
        SchedulerQueryResult.Succeeded(
          ObservationBatch(
            NonEmptyVector.one(
              ObservationResult.Observed(
                JobObservation(
                  reported,
                  SlurmState.Running,
                  Freshness.Current(later),
                  None,
                  Map.empty,
                  evidence
                )
              )
            )
          )
        ),
        later.plusSeconds(2L)
      )
    )
    val attempt = current.state.attempts.values.head

    assert(
      !attempt.observation.isInstanceOf[ManagedObservation.Current],
      "a beta-cluster attempt must not adopt an alpha-cluster observation"
    )
    attempt.observation match
      case unavailable: ManagedObservation.Unavailable =>
        assert(
          unavailable.diagnostics.values.exists(_.code == "observation-cluster-conflict"),
          s"conflict must be diagnosed, not silent: ${unavailable.diagnostics}"
        )
      case other => fail(s"expected a diagnosed conflict, got $other")
  }

  private def boundState(key: String): ControlState = boundStateOn(key, job)

  private def boundStateOn(key: String, bound: JobRef): ControlState =
    val value = intent(key)
    val recorded = applyCommand(ControlState.empty, ControlCommand.RecordIntent(value))
    val claimed = applyCommand(
      recorded.state,
      ControlCommand.ClaimSubmission(value.submissionKey, later)
    )
    applyCommand(
      claimed.state,
      ControlCommand.RecordSubmission(
        value.submissionKey,
        value.epoch,
        SubmissionAttempt.Completed(Submission.Accepted(bound, evidence)),
        later.plusSeconds(1L)
      )
    ).state
