package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*
import scodec.bits.ByteVector

final case class RemoteRegisteredBatchElement(
    index: ArrayIndex,
    submissionKey: SubmissionKey,
    inputBytes: ByteVector
) derives CanEqual

final case class RemoteRegisteredBatchRequest(
    submissionKey: SubmissionKey,
    name: JobName,
    operation: RegisteredOperation,
    topology: BatchTopology,
    elements: NonEmptyVector[RemoteRegisteredBatchElement],
    environment: Map[EnvName, String],
    maximumResultBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath],
    retrySafety: RetrySafety
) derives CanEqual

final case class RemoteRegisteredBatchElementSubmission(
    index: ArrayIndex,
    resultRef: RemoteResultRef,
    resultHandle: DurableResultHandle,
    stdout: LogRef,
    stderr: LogRef
) derives CanEqual

final case class RemoteRegisteredBatchSubmission(
    topology: BatchTopology,
    elements: NonEmptyVector[RemoteRegisteredBatchElementSubmission],
    submission: SubmissionAttempt
) derives CanEqual

final case class RemoteScriptBatchElement(
    index: ArrayIndex,
    submissionKey: SubmissionKey,
    arguments: Vector[Argument]
) derives CanEqual

final case class RemoteScriptBatchRequest(
    submissionKey: SubmissionKey,
    name: JobName,
    program: ScriptProgram,
    topology: BatchTopology,
    elements: NonEmptyVector[RemoteScriptBatchElement],
    environment: Map[EnvName, String],
    retrySafety: RetrySafety
) derives CanEqual

final case class RemoteScriptExitRef(
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch
) derives CanEqual

final case class RemoteScriptBatchElementSubmission(
    index: ArrayIndex,
    exitRef: RemoteScriptExitRef,
    stdout: LogRef,
    stderr: LogRef
) derives CanEqual

final case class RemoteScriptBatchSubmission(
    topology: BatchTopology,
    elements: NonEmptyVector[RemoteScriptBatchElementSubmission],
    submission: SubmissionAttempt
) derives CanEqual

enum RemoteScriptExitRead derives CanEqual:
  case Pending(observedAt: java.time.Instant)
  case Exited(exitCode: Int, observedAt: java.time.Instant)
  case Failed(
      diagnostics: Diagnostics,
      evidence: EvidenceBundle,
      observedAt: java.time.Instant
  )
