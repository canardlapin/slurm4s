package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson

import org.scalacheck.Gen

import scodec.bits.ByteVector

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

  // --- The persisted codecs: a dropped field here is data loss on disk, not just on the wire. ---

  val contentDigest: Gen[ContentDigest] =
    Gen
      .listOfN(64, Gen.oneOf("0123456789abcdef".toVector))
      .map(hex => ContentDigest.unsafeFrom(s"sha256:${hex.mkString}"))

  val submissionKey: Gen[SubmissionKey] =
    Gen.oneOf("key-1", "key-2", "submission-abc").map(SubmissionKey.unsafeFrom)

  val attemptId: Gen[AttemptId] =
    Gen.oneOf("attempt-1", "attempt-2", "attempt-abc").map(AttemptId.unsafeFrom)

  val attemptEpoch: Gen[AttemptEpoch] = Gen.choose(1L, 64L).map(AttemptEpoch.unsafeFrom)

  val resultSchemaId: Gen[ResultSchemaId] =
    Gen.oneOf("schema.a", "schema.b", "slurm4s.test").map(ResultSchemaId.unsafeFrom)

  val workerRelease: Gen[WorkerRelease] =
    for
      id <- Gen.oneOf("release-1", "release-2").map(WorkerReleaseId.unsafeFrom)
      digest <- contentDigest
    yield WorkerRelease(id, digest)

  val workloadOperation: Gen[WorkloadOperation] =
    Gen.oneOf(
      contentDigest.map(WorkloadOperation.Script.apply),
      Gen
        .zip(
          Gen.oneOf("op.alpha", "op.beta").map(OperationId.unsafeFrom),
          Gen.choose(1L, 9L).map(value => OperationVersion.unsafeFrom(value.toString))
        )
        .map(WorkloadOperation.Registered.apply)
    )

  val registeredOperation: Gen[RegisteredOperation] =
    for
      id <- Gen.oneOf("op.alpha", "op.beta").map(OperationId.unsafeFrom)
      version <- Gen.choose(1L, 9L).map(value => OperationVersion.unsafeFrom(value.toString))
      inputSchema <- Gen.oneOf("in.a", "in.b").map(SchemaId.unsafeFrom)
      outputSchema <- resultSchemaId
    yield RegisteredOperation(id, version, inputSchema, outputSchema)

  /** No whitespace: RelativeOutputPath rejects it, so a generator emitting it would fail
    * construction rather than exercise a codec. Non-ASCII is included because it must survive.
    */
  val relativeOutputPath: Gen[RelativeOutputPath] =
    Gen
      .oneOf("out.txt", "nested/result.json", "deep/a/b.bin", "\u00e9t\u00e9.txt")
      .map(RelativeOutputPath.unsafeFrom)

  /** Distinct paths: the manifest and the handle both reject duplicates, so a generator producing
    * them would fail construction rather than test a codec.
    */
  private val distinctOutputPaths: Gen[Vector[RelativeOutputPath]] =
    Gen.choose(0, 3).flatMap(Gen.listOfN(_, relativeOutputPath)).map(_.distinct.toVector)

  val outputEntry: Gen[OutputEntry] =
    for
      path <- relativeOutputPath
      size <- Gen.choose(0L, 1_000_000L)
      digest <- contentDigest
    yield OutputEntry.from(path, size, digest).toOption.get

  val outputManifest: Gen[OutputManifest] =
    Gen
      .choose(0, 3)
      .flatMap(Gen.listOfN(_, outputEntry))
      .map { entries =>
        OutputManifest.from(entries.toVector.distinctBy(_.path)).toOption.get
      }

  val resultEnvelopeStatus: Gen[ResultEnvelopeStatus] =
    Gen.oneOf(
      Gen.const(ResultEnvelopeStatus.Succeeded),
      Gen
        .zip(Gen.oneOf("failed", "invalid-result"), Gen.oneOf("boom", "bad input"))
        .map(ResultEnvelopeStatus.Failed.apply)
    )

  val retrySafety: Gen[RetrySafety] =
    Gen.oneOf(
      RetrySafety.Unknown,
      RetrySafety.NoAutomaticRetry,
      RetrySafety.SafeForAutomaticRetry
    )

  /** Built through the public factories, so `status` and `value` stay in the pairing the type
    * actually admits: a success carries a value, a failure carries a code and message and no value.
    * Generating those two fields independently would produce combinations no encoder can ever see.
    */
  val resultEnvelope: Gen[ResultEnvelope] =
    for
      key <- submissionKey
      attempt <- attemptId
      epoch <- attemptEpoch
      job <- Gen.option(jobRef)
      operation <- workloadOperation
      schema <- resultSchemaId
      status <- resultEnvelopeStatus
      value <- Generators.evidenceBytes
      outputs <- outputManifest
      release <- workerRelease
      at <- instant
    yield status match
      case ResultEnvelopeStatus.Succeeded =>
        ResultEnvelope
          .succeeded(key, attempt, epoch, job, operation, schema, value, outputs, release, at)
      case ResultEnvelopeStatus.Failed(code, message) =>
        ResultEnvelope
          .failed(key, attempt, epoch, job, operation, schema, code, message, outputs, release, at)

  /** `retrySafety` has a default on `from`, so it is generated explicitly. */
  val durableResultHandle: Gen[DurableResultHandle] =
    for
      key <- submissionKey
      attempt <- attemptId
      epoch <- attemptEpoch
      job <- Gen.option(jobRef)
      operation <- workloadOperation
      schema <- resultSchemaId
      outputs <- distinctOutputPaths
      release <- workerRelease
      safety <- retrySafety
    yield DurableResultHandle
      .from(
        key,
        attempt,
        epoch,
        job,
        operation,
        schema,
        ByteLimit.defaultEvidence,
        ByteLimit.maximumCommandCapture,
        outputs,
        release,
        safety
      )
      .toOption
      .get

  // --- The submitted request. ---

  val jobName: Gen[JobName] =
    Gen.oneOf("managed-test", "nightly", "fit-model").map(JobName.unsafeFrom)

  val scriptSource: Gen[ScriptSource] =
    Gen.oneOf(
      Gen
        .zip(Gen.oneOf("job.sh", "run.sh"), Generators.evidenceBytes)
        .map(ScriptSource.Inline.apply),
      Gen.oneOf("/local/job.sh", "/local/run.sh").map(ScriptSource.StagedLocal.apply),
      Gen.oneOf("/remote/job.sh", "/remote/run.sh").map(ScriptSource.ExistingRemote.apply)
    )

  val resourceRequest: Gen[ResourceRequest] =
    for
      cpus <- Gen.choose(1, 8)
      tasks <- Gen.choose(1, 4)
      nodes <- Gen.option(Gen.choose(1, 4))
      wallTime <- Gen.option(Gen.choose(1L, 1440L).map(WallTimeMinutes.unsafeFrom))
    yield ResourceRequest
      .validate(cpus, tasks, nodes, None, wallTime)
      .toEither
      .toOption
      .get

  /** Mode, schema and declared outputs are drawn together rather than independently, because `from`
    * couples them: exit-only carries neither, declared-outputs requires at least one path and no
    * schema, structured requires a schema. Drawing them separately produces combinations the type
    * refuses, so the generator would fail construction instead of exercising a codec.
    */
  val resultContractDescriptor: Gen[ResultContractDescriptor] =
    val outputs =
      Gen
        .choose(1, 3)
        .flatMap(Gen.listOfN(_, relativeOutputPath))
        .map(_.distinct.toVector)
        .suchThat(_.nonEmpty)
    Gen
      .oneOf(
        Gen.const(
          ResultContractDescriptor
            .from(ResultMode.ExitOnly, None, ByteLimit.defaultEvidence, Vector.empty)
        ),
        outputs.map(paths =>
          ResultContractDescriptor
            .from(ResultMode.DeclaredOutputs, None, ByteLimit.defaultEvidence, paths)
        ),
        Gen.zip(resultSchemaId, Gen.oneOf(Gen.const(Vector.empty), outputs)).map { (schema, paths) =>
          ResultContractDescriptor
            .from(ResultMode.Structured, Some(schema), ByteLimit.defaultEvidence, paths)
        }
      )
      .map(_.toOption.get)

  /** Concurrency is drawn from the deduplicated index count, since the type refuses a limit larger
    * than the array it applies to.
    */
  val jobArrayRequest: Gen[JobArrayRequest] =
    for
      drawn <- Gen.choose(1, 4).flatMap(Gen.listOfN(_, arrayIndex)).map(_.distinct.toVector)
      indices = if drawn.isEmpty then Vector(ArrayIndex.unsafeFrom(0)) else drawn
      concurrent <- Gen.option(Gen.choose(1, indices.size).map(PositiveInt.unsafeFrom))
    yield JobArrayRequest.from(indices, concurrent).toOption.get

  val envName: Gen[EnvName] =
    Gen.oneOf("SLURM4S_A", "PATH_EXTRA", "MODEL_DIR").map(EnvName.unsafeFrom)

  /** Populates `environment`, `array` and `retrySafety`, all three of which have defaults. */
  def launchSpec(environment: Gen[Map[EnvName, String]]): Gen[LaunchSpec] =
    for
      key <- submissionKey
      name <- jobName
      source <- scriptSource
      arguments <- Gen.choose(0, 3).flatMap(Gen.listOfN(_, Gen.oneOf("-v", "--fast", "input.csv")))
      contract <- resultContractDescriptor
      resources <- resourceRequest
      env <- environment
      array <- Gen.option(jobArrayRequest)
      safety <- retrySafety
    yield LaunchSpec(
      key,
      name,
      source,
      arguments.toVector,
      contract,
      resources,
      env,
      array,
      safety
    )

  val launchSpec: Gen[LaunchSpec] =
    launchSpec(
      Gen
        .choose(0, 2)
        .flatMap(Gen.mapOfN(_, Gen.zip(envName, Gen.oneOf("1", "/opt/models", ""))))
    )

  /** The opaque submit protocol carries only exit-only contracts, by design: it never learned to
    * describe declared outputs or a structured schema. Round-trip laws over it therefore hold on
    * exit-only specs, and the other modes get their own law asserting the encoder refuses them.
    */
  val exitOnlyLaunchSpec: Gen[LaunchSpec] =
    launchSpec.map(_.copy(resultContract = ResultContract.ExitOnly.descriptor))

  val unsupportedContractLaunchSpec: Gen[LaunchSpec] =
    for
      spec <- launchSpec
      contract <- resultContractDescriptor.suchThat(_.mode != ResultMode.ExitOnly)
    yield spec.copy(resultContract = contract)

  /** A canonicalizable request. Two constraints narrow it: the managed store's default policy
    * refuses to persist environment VALUES, and canonicalization goes through the submit encoder,
    * so the contract must be one that encoder can express.
    */
  val managedLaunchSpec: Gen[LaunchSpec] =
    launchSpec(Gen.const(Map.empty))
      .map(_.copy(resultContract = ResultContract.ExitOnly.descriptor))

  // --- The transport envelope and its framing. ---

  val requestId: Gen[RequestId] =
    Gen
      .oneOf("req-1", "req-2", "handshake-0", "observe-42")
      .map(raw => RequestId.from(raw).toOption.get)

  /** Minor version varies, major does not: the envelope decoder refuses any major but its own, so a
    * generator that moved it would be generating messages no peer should accept.
    */
  val protocolVersion: Gen[ProtocolVersion] =
    Gen
      .choose(0, 4)
      .map(minor => ProtocolVersion.from(VersionedJson.supportedMajor, minor).toOption.get)

  val agentMethod: Gen[AgentMethod] = Gen.oneOf(AgentMethod.values.toVector)

  val agentResponseStatus: Gen[AgentResponseStatus] =
    Gen.oneOf(AgentResponseStatus.values.toVector)

  val agentBody: Gen[AgentBody] =
    Gen.oneOf(
      Gen.zip(agentMethod, JsonCorpus.arbitraryJson).map(AgentBody.Request.apply),
      Gen.zip(agentResponseStatus, JsonCorpus.arbitraryJson).map(AgentBody.Response.apply)
    )

  /** Extension names avoid the reserved set, which `withExtensions` refuses by contract. */
  val agentExtensions: Gen[JsonObject] =
    Gen
      .choose(0, 3)
      .flatMap(
        Gen.listOfN(
          _,
          Gen.zip(
            Gen.oneOf("traceId", "deadlineMillis", "zzzLastAlphabetically", "aaaFirst"),
            JsonCorpus.arbitraryJson
          )
        )
      )
      .map(fields => JsonObject.fromIterable(fields))

  val agentEnvelope: Gen[AgentEnvelope] =
    for
      id <- requestId
      protocol <- protocolVersion
      body <- agentBody
      extensions <- agentExtensions
    yield AgentEnvelope
      .withExtensions(id, protocol, body, extensions)
      .fold(
        failure => throw new AssertionError(s"generated a reserved extension: $failure"),
        identity
      )

  /** Splits a byte stream into consecutive chunks of arbitrary size, including empty ones.
    *
    * This is the shape a stream decoder actually sees: a chunk boundary has nothing to do with a
    * frame boundary, so a decoder tested only on whole frames has never been tested.
    */
  def chunkings(bytes: ByteVector): Gen[Vector[ByteVector]] =
    if bytes.isEmpty then Gen.const(Vector.empty)
    else
      Gen.choose(1L, math.min(bytes.size, 8L)).flatMap { taken =>
        chunkings(bytes.drop(taken)).map(rest => bytes.take(taken) +: rest)
      }

  private def nonEmpty[A](value: Gen[A]): Gen[NonEmptyVector[A]] =
    for
      head <- value
      rest <- Gen.choose(0, 2).flatMap(Gen.listOfN(_, value))
    yield NonEmptyVector(head, rest.toVector)
