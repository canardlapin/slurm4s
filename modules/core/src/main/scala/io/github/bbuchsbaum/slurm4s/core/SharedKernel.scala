package io.github.bbuchsbaum.slurm4s.core

/** Source-compatible names for contracts now owned by `remote-exec-kernel`.
  *
  * New provider-neutral code should import `io.github.bbuchsbaum.remoteexec.kernel` directly. These
  * aliases keep slurm4s's pre-1.0 public surface source-compatible while downstream users migrate.
  */
type ValidationFailure = io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
val ValidationFailure = io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure

type SubmissionKey = io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
val SubmissionKey = io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
type AttemptId = io.github.bbuchsbaum.remoteexec.kernel.AttemptId
val AttemptId = io.github.bbuchsbaum.remoteexec.kernel.AttemptId
type OperationId = io.github.bbuchsbaum.remoteexec.kernel.OperationId
val OperationId = io.github.bbuchsbaum.remoteexec.kernel.OperationId
type OperationVersion = io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
val OperationVersion = io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
type OperationDescriptor = io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor
val OperationDescriptor = io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor
type OperationRef[I, O] = io.github.bbuchsbaum.remoteexec.kernel.OperationRef[I, O]
val OperationRef = io.github.bbuchsbaum.remoteexec.kernel.OperationRef
type RegisteredOperation = io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor
val RegisteredOperation = io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor
type SchemaId = io.github.bbuchsbaum.remoteexec.kernel.SchemaId
val SchemaId = io.github.bbuchsbaum.remoteexec.kernel.SchemaId
type ResultSchemaId = io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
val ResultSchemaId = io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
type WorkerReleaseId = io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
val WorkerReleaseId = io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
type ContentDigest = io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
val ContentDigest = io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
type AttemptEpoch = io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
val AttemptEpoch = io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
type ByteLimit = io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
val ByteLimit = io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
type DurationMillis = io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
val DurationMillis = io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
type PositiveInt = io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
val PositiveInt = io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
type WallTimeMinutes = io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes
val WallTimeMinutes = io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes

type Diagnostic = io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
val Diagnostic = io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
type Diagnostics = io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
val Diagnostics = io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
type Freshness = io.github.bbuchsbaum.remoteexec.kernel.Freshness
val Freshness = io.github.bbuchsbaum.remoteexec.kernel.Freshness
type RetrySafety = io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
val RetrySafety = io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
type ResultCodecFailure = io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
val ResultCodecFailure = io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
type ResultCodec[A] = io.github.bbuchsbaum.remoteexec.kernel.ResultCodec[A]
type InputCodec[A] = io.github.bbuchsbaum.remoteexec.kernel.InputCodec[A]
type SpawnFailureKind = io.github.bbuchsbaum.remoteexec.kernel.SpawnFailureKind
val SpawnFailureKind = io.github.bbuchsbaum.remoteexec.kernel.SpawnFailureKind
type FailureCause = io.github.bbuchsbaum.remoteexec.kernel.FailureCause
val FailureCause = io.github.bbuchsbaum.remoteexec.kernel.FailureCause
type FailureDiagnosis = io.github.bbuchsbaum.remoteexec.kernel.FailureDiagnosis
type WorkerRelease = io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
val WorkerRelease = io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
