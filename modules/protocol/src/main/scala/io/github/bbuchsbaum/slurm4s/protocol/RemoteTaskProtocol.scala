package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.core.codec.CodecFailure
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson
import io.github.bbuchsbaum.slurm4s.core.codec.WireEnvelope

import scodec.bits.ByteVector

import java.time.Instant

/** Transport form of a registered task. It contains an operation descriptor and already-encoded
  * input bytes, never a Scala function, closure, codec implementation, or suspended effect.
  */
final case class RemoteRegisteredTaskRequest(
    submissionKey: SubmissionKey,
    name: JobName,
    operation: RegisteredOperation,
    inputBytes: ByteVector,
    resources: ResourceRequest,
    environment: Map[EnvName, String],
    maximumResultBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath],
    retrySafety: RetrySafety
) derives CanEqual

/** Durable, path-free locator interpreted relative to the agent's private worker workspace. */
final case class RemoteResultRef(
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch
) derives CanEqual

final case class RemoteRegisteredSubmission(
    resultRef: RemoteResultRef,
    resultHandle: DurableResultHandle,
    submission: SubmissionAttempt
) derives CanEqual

enum RemoteResultRead derives CanEqual:
  case Pending(observedAt: Instant)
  case Available(
      storedHandle: DurableResultHandle,
      envelopeBytes: ByteVector,
      observedAt: Instant
  )
  case Failed(
      diagnostics: Diagnostics,
      evidence: EvidenceBundle,
      observedAt: Instant
  )

object RemoteTaskWireLimits:
  val MaximumHandleBytes: ByteLimit = ByteLimit.defaultEvidence
  val MaximumDescriptorBytes: ByteLimit = ByteLimit.maximumCommandCapture

  /** Cardinality ceiling for the per-element collections a batch request or response carries.
    *
    * A byte bound on each element says nothing about how many of them arrive, so a collection needs
    * its own cap. This matches Slurm's own practical array ceiling and was already applied as a
    * bare literal at the batch-element sites; naming it keeps those from drifting apart from the
    * result-reference list, which had no cap at all.
    */
  val MaximumBatchEntries: Int = 100000

enum RemoteTaskDescriptorCodecFailure derives CanEqual:
  case TooLarge(actualBytes: Long, maximumBytes: Int)
  case Envelope(failure: CodecFailure)
  case WrongSchema(received: String)
  case Invalid(message: String)

object RemoteRegisteredSubmissionCodec:
  private val SchemaName = "slurm4s.remote-task-descriptor"

  def encode(
      value: RemoteRegisteredSubmission,
      maximumBytes: ByteLimit = RemoteTaskWireLimits.MaximumDescriptorBytes
  ): Either[RemoteTaskDescriptorCodecFailure, ByteVector] =
    for
      schema <- SchemaId
        .from(SchemaName)
        .left
        .map(problem => RemoteTaskDescriptorCodecFailure.Invalid(problem.reason))
      payload <- AgentDomainJson
        .encodeRemoteSubmission(value)
        .left
        .map(RemoteTaskDescriptorCodecFailure.Invalid.apply)
      bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, payload))
      _ <- bounded(bytes.size.toLong, maximumBytes)
    yield bytes

  def decode(
      bytes: ByteVector,
      maximumBytes: ByteLimit = RemoteTaskWireLimits.MaximumDescriptorBytes
  ): Either[RemoteTaskDescriptorCodecFailure, RemoteRegisteredSubmission] =
    for
      _ <- bounded(bytes.size.toLong, maximumBytes)
      envelope <- VersionedJson
        .decode(bytes)
        .left
        .map(RemoteTaskDescriptorCodecFailure.Envelope.apply)
      _ <- Either.cond(
        envelope.schema.value == SchemaName,
        (),
        RemoteTaskDescriptorCodecFailure.WrongSchema(envelope.schema.value)
      )
      value <- AgentDomainJson
        .decodeRemoteSubmission(envelope.payload)
        .left
        .map(RemoteTaskDescriptorCodecFailure.Invalid.apply)
    yield value

  private def bounded(
      actualBytes: Long,
      maximumBytes: ByteLimit
  ): Either[RemoteTaskDescriptorCodecFailure, Unit] =
    Either.cond(
      actualBytes <= maximumBytes.value.toLong,
      (),
      RemoteTaskDescriptorCodecFailure.TooLarge(actualBytes, maximumBytes.value)
    )
