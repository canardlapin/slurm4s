package io.github.bbuchsbaum.slurm4s.core

import java.time.Instant
import java.nio.charset.StandardCharsets

class AttemptReducerSuite extends munit.FunSuite:
  private val epoch = AttemptEpoch.initial
  private val attempt = AttemptId.from("attempt-1").toOption.get
  private val job = JobRef(JobId.from("9876").toOption.get, None)
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(
      EvidenceSource.CommandStdout("sbatch"),
      Instant.EPOCH,
      "9876\n".getBytes(StandardCharsets.UTF_8).toVector
    )
  )

  private def event(cursor: Long, value: AttemptEvent): VersionedAttemptEvent =
    VersionedAttemptEvent(EventCursor.from(cursor).toOption.get, epoch, value)

  test("lost submission response becomes stable AcceptanceUnknown and can reconcile") {
    val initial = AttemptState.prepared(attempt, epoch)

    val result = for
      submitting <- AttemptReducer.reduce(initial, event(1, AttemptEvent.SubmissionStarted))
      unknown <- AttemptReducer.reduce(
        submitting,
        event(
          2,
          AttemptEvent.SubmissionResponseLost(AcceptanceUncertainty.ResponseLost, evidence)
        )
      )
      reconciled <- AttemptReducer.reduce(unknown, event(3, AttemptEvent.Reconciled(job, evidence)))
    yield (unknown, reconciled)

    val (unknown, reconciled) = result.toOption.get
    assert(unknown.phase.isInstanceOf[AttemptPhase.AcceptanceUnknown])
    assertEquals(reconciled.phase, AttemptPhase.Bound(job, evidence))
  }

  test("AcceptanceUnknown does not authorize blind resubmission") {
    val initial = AttemptState.prepared(attempt, epoch)
    val unknown = for
      submitting <- AttemptReducer.reduce(initial, event(1, AttemptEvent.SubmissionStarted))
      state <- AttemptReducer.reduce(
        submitting,
        event(
          2,
          AttemptEvent.SubmissionResponseLost(AcceptanceUncertainty.TransportInterrupted, evidence)
        )
      )
    yield state

    val result = unknown.flatMap(state =>
      AttemptReducer.reduce(state, event(3, AttemptEvent.SubmissionStarted))
    )
    assert(result.isLeft)
  }

  test("reducer rejects stale epochs and replayed cursors") {
    val initial = AttemptState.prepared(attempt, epoch)
    val submitting = AttemptReducer
      .reduce(initial, event(1, AttemptEvent.SubmissionStarted))
      .toOption
      .get
    val staleEpoch = AttemptEpoch.from(2L).toOption.get

    val replayed = AttemptReducer.reduce(submitting, event(1, AttemptEvent.SubmissionStarted))
    val fenced = AttemptReducer.reduce(
      submitting,
      VersionedAttemptEvent(
        EventCursor.from(2L).toOption.get,
        staleEpoch,
        AttemptEvent.SubmissionStarted
      )
    )

    assert(replayed.isLeft)
    assert(fenced.isLeft)
  }
