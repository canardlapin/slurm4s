package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.ProtocolGenerators

import org.scalacheck.Gen

/** Generators for the commands the control journal persists (P6f.34).
  *
  * A command that survives encoding but loses a field on the way back is worse here than on the
  * wire: the journal is the record a restarted controller replays to decide what it already did, so
  * a dropped field silently rewrites history. These generators exist to put a non-default value in
  * every field a command carries, including the defaulted ones.
  */
private[managed] object ManagedGenerators:
  export ProtocolGenerators.{
    attemptEpoch,
    cancellationAttempt,
    evidenceBundle,
    instant,
    jobRef,
    managedLaunchSpec,
    schedulerQueryResult,
    submissionAttempt,
    submissionKey,
    accountingBatch,
    observationBatch
  }

  /** Built through `ManagedIntent.from` rather than by hand, because the decoder cross-checks the
    * stored submission key and retry safety against the ones inside the canonical request. A
    * generator that set those independently would produce intents no decoder should accept, and the
    * law would be testing the cross-check instead of the round trip. Epoch and attempt id are not
    * cross-checked, so they are varied afterwards.
    */
  val managedIntent: Gen[ManagedIntent] =
    for
      spec <- managedLaunchSpec
      recordedAt <- instant
      epoch <- attemptEpoch
    yield ManagedIntent
      .from(spec, recordedAt)
      .fold(
        failure => throw new AssertionError(s"generated an uncanonicalizable request: $failure"),
        _.copy(epoch = epoch)
      )

  val retryReason: Gen[RetryReason] =
    Gen
      .oneOf("operator asked for a rerun", "transient node failure", "scheduler lost the binding")
      .map(raw => RetryReason.from(raw).toOption.get)

  val retryAuthorization: Gen[RetryAuthorization] =
    Gen.oneOf(
      retryReason.map(RetryAuthorization.Manual.apply),
      retryReason.map(RetryAuthorization.Automatic.apply)
    )

  val requestedJobs: Gen[NonEmptyVector[JobRef]] =
    Gen
      .choose(1, 3)
      .flatMap(Gen.listOfN(_, jobRef))
      .map(values => NonEmptyVector.fromVectorUnsafe(values.toVector))

  /** Every case of the command enum. A codec that forgot one is a controller that cannot replay the
    * journal it wrote, so the generator is exhaustive by construction: adding a case to
    * `ControlCommand` without adding it here leaves a hole the laws cannot see.
    */
  val controlCommand: Gen[ControlCommand] =
    Gen.oneOf(
      managedIntent.map(ControlCommand.RecordIntent.apply),
      Gen.zip(submissionKey, instant).map(ControlCommand.ClaimSubmission.apply),
      Gen
        .zip(submissionKey, attemptEpoch, submissionAttempt, instant)
        .map(ControlCommand.RecordSubmission.apply),
      Gen
        .zip(submissionKey, attemptEpoch, evidenceBundle, instant)
        .map(ControlCommand.RecoverSubmissionClaim.apply),
      Gen
        .zip(submissionKey, attemptEpoch, retryAuthorization, instant)
        .map(ControlCommand.RetrySubmission.apply),
      Gen
        .zip(submissionKey, attemptEpoch, jobRef, evidenceBundle, instant)
        .map(ControlCommand.ReconcileBinding.apply),
      Gen
        .zip(requestedJobs, schedulerQueryResult(observationBatch), instant)
        .map(ControlCommand.RecordObservations.apply),
      Gen
        .zip(requestedJobs, schedulerQueryResult(accountingBatch), instant)
        .map(ControlCommand.RecordAccounting.apply),
      Gen.zip(submissionKey, instant).map(ControlCommand.RequestCancellation.apply),
      Gen.zip(submissionKey, instant).map(ControlCommand.ClaimCancellation.apply),
      Gen
        .zip(submissionKey, evidenceBundle, instant)
        .map(ControlCommand.RecoverCancellationClaim.apply),
      Gen
        .zip(submissionKey, cancellationAttempt, instant)
        .map(ControlCommand.RecordCancellation.apply)
    )

  /** A command sequence the transition rules will largely accept.
    *
    * Independently drawn commands are almost all refused — they name a submission that was never
    * recorded — which would leave the journal nearly empty and make a replay law pass by having
    * nothing to replay. So the sequence opens with the intent that creates the attempt and the
    * claim that advances it, then draws follow-ups that reuse that attempt's key and epoch.
    * Follow-ups still land in an order the rules may refuse, which is wanted: a refused command
    * writes no record and the law then covers the mixed accepted/refused traffic a real controller
    * produces.
    */
  val controlScenario: Gen[Vector[ControlCommand]] =
    for
      intent <- managedIntent
      key = intent.submissionKey
      epoch = intent.epoch
      at = intent.recordedAt
      submitted <- submissionAttempt
      cancelled <- cancellationAttempt
      evidence <- evidenceBundle
      job <- jobRef
      authorization <- retryAuthorization
      observations <- schedulerQueryResult(observationBatch)
      accounting <- schedulerQueryResult(accountingBatch)
      followUps <- Gen.someOf(
        Vector(
          ControlCommand.RecordSubmission(key, epoch, submitted, at.plusSeconds(2L)),
          ControlCommand.RecoverSubmissionClaim(key, epoch, evidence, at.plusSeconds(3L)),
          ControlCommand.ReconcileBinding(key, epoch, job, evidence, at.plusSeconds(4L)),
          ControlCommand.RetrySubmission(key, epoch, authorization, at.plusSeconds(5L)),
          ControlCommand.RecordObservations(requested(job), observations, at.plusSeconds(6L)),
          ControlCommand.RecordAccounting(requested(job), accounting, at.plusSeconds(7L)),
          ControlCommand.RequestCancellation(key, at.plusSeconds(8L)),
          ControlCommand.ClaimCancellation(key, at.plusSeconds(9L)),
          ControlCommand.RecoverCancellationClaim(key, evidence, at.plusSeconds(10L)),
          ControlCommand.RecordCancellation(key, cancelled, at.plusSeconds(11L))
        )
      )
    yield ControlCommand.RecordIntent(intent) +:
      ControlCommand.ClaimSubmission(key, at.plusSeconds(1L)) +:
      followUps.toVector

  private def requested(job: JobRef): NonEmptyVector[JobRef] = NonEmptyVector.one(job)
