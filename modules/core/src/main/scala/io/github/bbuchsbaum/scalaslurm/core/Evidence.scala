package io.github.bbuchsbaum.scalaslurm.core

import java.time.Instant

enum EvidenceSource derives CanEqual:
  case CommandStdout(command: String)
  case CommandStderr(command: String)
  case CommandLaunch(command: String)
  case SchedulerJson(command: String, dataParser: String)
  case SchedulerText(command: String)
  case AgentProtocol
  case DurableJournal
  case WorkerEvent
  case ResultEnvelope

final case class BoundedEvidence private (
    source: EvidenceSource,
    observedAt: Instant,
    bytes: Vector[Byte],
    originalByteCount: Long,
    truncated: Boolean
) derives CanEqual

object BoundedEvidence:
  def capture(
      source: EvidenceSource,
      observedAt: Instant,
      bytes: Vector[Byte],
      limit: ByteLimit = ByteLimit.defaultEvidence
  ): BoundedEvidence =
    val retained = bytes.take(limit.value)
    BoundedEvidence(
      source = source,
      observedAt = observedAt,
      bytes = retained,
      originalByteCount = bytes.size.toLong,
      truncated = retained.size < bytes.size
    )

  private[scalaslurm] def fromCapture(
      source: EvidenceSource,
      observedAt: Instant,
      retainedBytes: Vector[Byte],
      originalByteCount: Long
  ): BoundedEvidence =
    require(
      originalByteCount >= retainedBytes.size.toLong,
      "original byte count must cover retained bytes"
    )
    BoundedEvidence(
      source = source,
      observedAt = observedAt,
      bytes = retainedBytes,
      originalByteCount = originalByteCount,
      truncated = originalByteCount > retainedBytes.size.toLong
    )

final case class EvidenceBundle(
    primary: BoundedEvidence,
    related: Vector[BoundedEvidence] = Vector.empty
) derives CanEqual:
  def all: Vector[BoundedEvidence] = primary +: related
