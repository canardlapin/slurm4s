package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*

/** P8.D4: a reported cluster is evidence, and cannot affect attribution at all.
  *
  * P8.A1 fixed the symptom by matching on a key that excluded cluster while `JobRef` still carried
  * one. D4 removes the field, so these tests now assert something stronger: there is no cluster in
  * operational identity for a report to disagree with.
  */
class ClusterAttributionSuite extends munit.FunSuite:
  import ManagedTestSupport.*

  private val alpha: ClusterName = ClusterName.unsafeFrom("alpha")

  test("an observation reporting a cluster is applied and keeps the cluster as evidence") {
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
                  job,
                  SlurmState.Running,
                  Freshness.Current(later),
                  None,
                  Map.empty,
                  evidence,
                  reportedCluster = Some(alpha)
                )
              )
            )
          )
        ),
        later.plusSeconds(2L)
      )
    )
    val attempt = current.state.attempts.values.head

    attempt.observation match
      case ManagedObservation.Current(ObservationResult.Observed(value)) =>
        assertEquals(value.reportedCluster, Some(alpha))
      case other => fail(s"observation was dropped: $other")
  }

  test("accounting reaches terminal regardless of what the site reports") {
    val bound = boundState("cluster-accounting")
    val record = AccountingRecord(
      job,
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

    assert(terminal.state.attempts.values.head.phase.isInstanceOf[ManagedPhase.Terminal])
  }

  private def boundState(key: String): ControlState =
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
        SubmissionAttempt.Completed(Submission.Accepted(job, evidence)),
        later.plusSeconds(1L)
      )
    ).state
