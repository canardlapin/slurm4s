package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*

import org.scalacheck.Gen

import java.time.LocalDateTime
import java.time.ZoneOffset

/** Generators for the domain values the hand-written protocol codecs carry (P6f.34).
  *
  * Built on core's `Generators` rather than duplicating it; protocol's test scope depends on core's
  * test scope for exactly that reason.
  *
  * The governing rule here is that a generator must be able to put a non-default value in every
  * defaulted field of every type it builds. A round-trip law is only as strong as the values fed to
  * it: `JobObservation.reportedCluster` defaulted to `None`, so a decoder that never read the field
  * still typechecked and still round-tripped any value that left it at the default. Generators that
  * settle for defaults reproduce that blind spot instead of closing it.
  */
object ProtocolGenerators:
  export Generators.{arrayIndex, clusterName, evidenceBundle, instant, jobId, jobRef, slurmState}

  val diagnostic: Gen[Diagnostic] =
    for
      code <- Gen.oneOf("invalid-account", "stale-result-epoch", "unknown-partition", "held-job")
      message <- Gen.oneOf("rejected by the scheduler", "not found", "quota exceeded")
      fields <- Gen
        .choose(0, 2)
        .flatMap(
          Gen.mapOfN(
            _,
            Gen.zip(Gen.oneOf("partition", "account", "qos"), Gen.oneOf("alpha", "beta"))
          )
        )
    yield Diagnostic(code, message, fields)

  val diagnostics: Gen[Diagnostics] =
    Gen
      .choose(1, 3)
      .flatMap(Gen.listOfN(_, diagnostic))
      .map(values => Diagnostics.fromVector(values.toVector).toOption.get)

  val freshness: Gen[Freshness] =
    Gen.oneOf(
      instant.map(Freshness.Current.apply),
      Gen.zip(instant, Gen.choose(0L, 600_000L)).map { (at, age) =>
        Freshness.Stale(at, DurationMillis.unsafeFrom(age))
      },
      Gen.zip(instant, diagnostics).map(Freshness.Unknown.apply)
    )

  val schedulerTimestamp: Gen[SchedulerTimestamp] =
    Gen.oneOf(
      instant.map(SchedulerTimestamp.Absolute.apply),
      instant.map(at => SchedulerTimestamp.SiteLocal(LocalDateTime.ofInstant(at, ZoneOffset.UTC)))
    )

  val jobStart: Gen[JobStart] =
    Gen.oneOf(
      schedulerTimestamp.map(JobStart.Actual.apply),
      schedulerTimestamp.map(JobStart.Expected.apply),
      schedulerTimestamp.map(JobStart.Reported.apply)
    )

  val observedTimeLimit: Gen[ObservedTimeLimit] =
    Gen.oneOf(
      Gen.choose(1L, 10_080L).map(v => ObservedTimeLimit.Limited(WallTimeMinutes.unsafeFrom(v))),
      Gen.const(ObservedTimeLimit.Unlimited),
      Gen.const(ObservedTimeLimit.PartitionDefault),
      Gen.option(Gen.oneOf("UNLIMITED", "1-00:00:00", "?")).map(ObservedTimeLimit.Unknown.apply)
    )

  val jobTiming: Gen[JobTiming] =
    for
      start <- Gen.option(jobStart)
      projectedEnd <- Gen.option(schedulerTimestamp)
      limit <- observedTimeLimit
    yield JobTiming(start, projectedEnd, limit)

  val slurmStateFlag: Gen[SlurmStateFlag] =
    Gen.oneOf(
      Gen.const(SlurmStateFlag.Completing),
      Gen.const(SlurmStateFlag.Configuring),
      Gen.const(SlurmStateFlag.PowerUpNode),
      Gen.const(SlurmStateFlag.StageOut),
      Gen.const(SlurmStateFlag.Resizing),
      Gen.const(SlurmStateFlag.Requeued),
      Gen.const(SlurmStateFlag.RequeueFederation),
      Gen.const(SlurmStateFlag.RequeueHold),
      Gen.const(SlurmStateFlag.Revoked),
      Gen.const(SlurmStateFlag.Signaling),
      Gen.const(SlurmStateFlag.SpecialExit),
      Gen.const(SlurmStateFlag.Stopped),
      Gen.const(SlurmStateFlag.ReservationDeleteHold),
      Gen.const(SlurmStateFlag.LaunchFailed),
      Gen.const(SlurmStateFlag.UpdateDb),
      Gen.alphaUpperStr.suchThat(_.nonEmpty).map(SlurmStateFlag.Unknown.apply)
    )

  val rawFields: Gen[Map[String, String]] =
    Gen
      .choose(0, 3)
      .flatMap(
        Gen.mapOfN(
          _,
          Gen.zip(
            Gen.oneOf("JobID", "State", "Partition", "Account", "Reason"),
            Gen.oneOf("alpha", "beta", "None", "")
          )
        )
      )

  /** Every defaulted field of `JobObservation` — timing, flags and reportedCluster — is populated
    * here. Leaving any of them at its default would let a decoder that never reads it pass.
    */
  val jobObservation: Gen[JobObservation] =
    for
      job <- jobRef
      state <- slurmState
      fresh <- freshness
      reason <- Gen.option(Gen.oneOf("Resources", "Priority", "DependencyNeverSatisfied"))
      fields <- rawFields
      evidence <- evidenceBundle
      timing <- jobTiming
      flags <- Gen.choose(0, 3).flatMap(Gen.listOfN(_, slurmStateFlag)).map(_.toVector)
      cluster <- Gen.option(clusterName)
    yield JobObservation(job, state, fresh, reason, fields, evidence, timing, flags, cluster)

  val observationResult: Gen[ObservationResult] =
    Gen.oneOf(
      jobObservation.map(ObservationResult.Observed.apply),
      Gen.zip(jobRef, freshness, evidenceBundle).map(ObservationResult.NotFound.apply),
      Gen
        .zip(jobRef, freshness, diagnostics, evidenceBundle)
        .map(ObservationResult.Failed.apply)
    )

  val observationBatch: Gen[ObservationBatch] =
    nonEmpty(observationResult).map(ObservationBatch.apply)

  val exitStatus: Gen[ExitStatus] =
    Gen.zip(Gen.choose(0, 255), Gen.option(Gen.choose(1, 64))).map(ExitStatus.apply)

  val workloadOutcome: Gen[WorkloadOutcome] =
    Gen.oneOf(
      Gen.choose(0, 255).map(WorkloadOutcome.Completed.apply),
      Gen.zip(Gen.option(Gen.choose(1, 255)), diagnostics).map(WorkloadOutcome.Failed.apply),
      Gen.const(WorkloadOutcome.OutOfMemory),
      Gen.const(WorkloadOutcome.TimeLimitExceeded),
      Gen.const(WorkloadOutcome.Cancelled),
      Gen.const(WorkloadOutcome.NodeFailure),
      Gen.alphaUpperStr.suchThat(_.nonEmpty).map(WorkloadOutcome.Unknown.apply)
    )

  val accountingRecord: Gen[AccountingRecord] =
    for
      job <- jobRef
      state <- slurmState
      exit <- Gen.option(exitStatus)
      outcome <- Gen.option(workloadOutcome)
      fresh <- freshness
      fields <- rawFields
      evidence <- evidenceBundle
    yield AccountingRecord(job, state, exit, outcome, fresh, fields, evidence)

  val accountingBatch: Gen[AccountingBatch] =
    for
      records <- nonEmpty(accountingRecord)
      missing <- Gen.choose(0, 2).flatMap(Gen.listOfN(_, jobRef)).map(_.toVector)
    yield AccountingBatch(records, missing)

  val spawnFailureKind: Gen[SpawnFailureKind] =
    Gen.oneOf(
      SpawnFailureKind.ExecutableMissing,
      SpawnFailureKind.PermissionDenied,
      SpawnFailureKind.WorkingDirectoryMissing,
      SpawnFailureKind.EnvironmentInvalid,
      SpawnFailureKind.ResourceUnavailable,
      SpawnFailureKind.Unknown
    )

  val invocationResult: Gen[InvocationResult] =
    val streams = Gen.zip(Generators.boundedEvidence, Generators.boundedEvidence)
    Gen.oneOf(
      Gen.zip(Gen.choose(1, 255), streams).map { (code, streams) =>
        InvocationResult.Exited(code, streams._1, streams._2)
      },
      Gen
        .zip(spawnFailureKind, diagnostics, evidenceBundle)
        .map(InvocationResult.SpawnFailed.apply),
      Gen.zip(Gen.choose(0L, 600_000L), streams).map { (after, streams) =>
        InvocationResult.TimedOut(DurationMillis.unsafeFrom(after), streams._1, streams._2)
      }
    )

  /** Every failure case as well as success: a query codec that handles only `Succeeded` is a real
    * defect, and a generator biased toward success would not report it.
    */
  def schedulerQueryResult[A](value: Gen[A]): Gen[SchedulerQueryResult[A]] =
    Gen.oneOf(
      value.map(SchedulerQueryResult.Succeeded.apply),
      Gen.zip(instant, evidenceBundle).map(SchedulerQueryResult.Empty.apply),
      invocationResult.map(SchedulerQueryResult.InvocationFailed.apply),
      Gen.zip(diagnostics, evidenceBundle).map(SchedulerQueryResult.ParseFailed.apply)
    )

  val submission: Gen[Submission] =
    Gen.oneOf(
      Gen.zip(jobRef, evidenceBundle).map(Submission.Accepted.apply),
      Gen.zip(diagnostics, evidenceBundle).map(Submission.Rejected.apply),
      Gen
        .zip(Generators.acceptanceUncertainty, evidenceBundle)
        .map(Submission.AcceptanceUnknown.apply)
    )

  val submissionAttempt: Gen[SubmissionAttempt] =
    Gen.oneOf(
      submission.map(SubmissionAttempt.Completed.apply),
      invocationResult.map(SubmissionAttempt.InvocationFailed.apply),
      diagnostics.map(SubmissionAttempt.PreparationFailed.apply)
    )

  val cancellationResult: Gen[CancellationResult] =
    Gen.oneOf(
      evidenceBundle.map(CancellationResult.Acknowledged.apply),
      evidenceBundle.map(CancellationResult.NotFound.apply),
      Gen.zip(diagnostics, evidenceBundle).map(CancellationResult.Rejected.apply),
      Gen.zip(diagnostics, evidenceBundle).map(CancellationResult.Unknown.apply)
    )

  val cancellationAttempt: Gen[CancellationAttempt] =
    Gen.oneOf(
      cancellationResult.map(CancellationAttempt.Completed.apply),
      invocationResult.map(CancellationAttempt.InvocationFailed.apply)
    )

  val capabilitySupport: Gen[CapabilitySupport] =
    Gen.oneOf(
      Gen.const(CapabilitySupport.Supported),
      Gen.const(CapabilitySupport.Unsupported),
      diagnostics.map(CapabilitySupport.Unknown.apply)
    )

  val schedulerCapabilities: Gen[SchedulerCapabilities] =
    for
      version <- Gen.option(Gen.oneOf("23.02.7", "24.05.2", "22.05.11"))
      cluster <- Gen.option(clusterName)
      structuredQueue <- capabilitySupport
      structuredAccounting <- capabilitySupport
      accounting <- capabilitySupport
      arrays <- capabilitySupport
      raw <- Gen.choose(0, 2).flatMap(Gen.listOfN(_, Generators.boundedEvidence)).map(_.toVector)
    yield SchedulerCapabilities(
      version,
      cluster,
      structuredQueue,
      structuredAccounting,
      accounting,
      arrays,
      raw
    )

  val logStream: Gen[LogStream] = Gen.oneOf(LogStream.Stdout, LogStream.Stderr)

  val fileIdentity: Gen[FileIdentity] =
    Gen.oneOf("ino-1234", "ino-9999:dev-8", "hash-abc").map(FileIdentity.unsafeFrom)

  val logCursor: Gen[LogCursor] =
    for
      offset <- Gen.choose(0L, 1_000_000L).map(LogOffset.unsafeFrom)
      identity <- Gen.option(fileIdentity)
    yield LogCursor(offset, identity)

  val logRef: Gen[LogRef] =
    for
      attempt <- Gen.oneOf("attempt-1", "attempt-2", "attempt-abc").map(AttemptId.unsafeFrom)
      epoch <- Gen.choose(1L, 64L).map(AttemptEpoch.unsafeFrom)
      stream <- logStream
      locator <- Gen.oneOf("/work/job.out", "/work/job.err", "/scratch/a b/log.txt")
    yield LogRef(attempt, epoch, stream, locator)

  val logPage: Gen[LogPage] =
    for
      bytes <- Generators.evidenceBytes
      next <- logCursor
      endOfFile <- Gen.prob(0.5)
      at <- instant
    yield LogPage(bytes, next, endOfFile, at)

  val logReadResult: Gen[LogReadResult] =
    Gen.oneOf(
      logPage.map(LogReadResult.Page.apply),
      Gen.zip(logCursor, instant).map(LogReadResult.WaitingForFile.apply),
      Gen
        .zip(logCursor, Gen.option(fileIdentity), Gen.choose(0L, 1_000_000L), instant)
        .map(LogReadResult.CursorInvalid.apply),
      Gen.zip(diagnostics, instant).map(LogReadResult.Failed.apply)
    )

  private def nonEmpty[A](value: Gen[A]): Gen[NonEmptyVector[A]] =
    for
      head <- value
      rest <- Gen.choose(0, 2).flatMap(Gen.listOfN(_, value))
    yield NonEmptyVector(head, rest.toVector)
