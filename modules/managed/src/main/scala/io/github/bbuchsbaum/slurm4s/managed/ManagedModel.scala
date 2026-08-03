package io.github.bbuchsbaum.slurm4s.managed

import cats.Order
import cats.Show
import io.github.bbuchsbaum.remoteexec.kernel.TextIdentifier
import io.github.bbuchsbaum.remoteexec.kernel.byteVectorCanEqual
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.core.codec.CanonicalJson
import io.github.bbuchsbaum.slurm4s.protocol.AgentDomainJson

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

object StoreRevision:
  opaque type Type = Long
  val initial: Type = 0L

  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("storeRevision", "must not be negative"))

  extension (revision: Type)
    def value: Long = revision

    /** Refuses rather than wrapping; see `EventCursor.next`. */
    def next: Either[ValidationFailure, Type] =
      Either.cond(
        revision < Long.MaxValue,
        revision + 1L,
        ValidationFailure("storeRevision", "cannot advance beyond Long.MaxValue")
      )
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)

type StoreRevision = StoreRevision.Type

object OutboxId extends TextIdentifier("outboxId", 255)
type OutboxId = OutboxId.Type

object SiteId extends TextIdentifier("siteId", 255)
type SiteId = SiteId.Type

final case class CanonicalRequest private (
    bytes: ByteVector,
    digest: ContentDigest
) derives CanEqual:
  def decode: Either[String, LaunchSpec] =
    io.circe.parser
      .parse(new String(bytes.toArray, StandardCharsets.UTF_8))
      .left
      .map(_.message)
      .flatMap(AgentDomainJson.decodeSubmitRequest)

object CanonicalRequest:
  def from(
      request: LaunchSpec,
      policy: ManagedRequestPolicy = ManagedRequestPolicy.rejectEnvironmentValues
  ): Either[ManagedIntentFailure, CanonicalRequest] =
    policy
      .validate(request)
      .left
      .map(ManagedIntentFailure.RequestRejected.apply)
      .flatMap(_ => encode(request).left.map(ManagedIntentFailure.CanonicalizationFailed.apply))

  private def encode(request: LaunchSpec): Either[String, CanonicalRequest] =
    AgentDomainJson.encodeSubmitRequest(request).flatMap { json =>
      val bytes = CanonicalJson.bytes(json)
      Either.cond(
        bytes.size <= ByteLimit.maximumCommandCapture.value,
        CanonicalRequest(bytes, digest(bytes)),
        "canonical managed request exceeds the durable request limit"
      )
    }

  def validated(bytes: ByteVector, digest: ContentDigest): Either[String, CanonicalRequest] =
    val actual = CanonicalRequest.digest(bytes)
    Either
      .cond(
        bytes.size <= ByteLimit.maximumCommandCapture.value && actual == digest,
        CanonicalRequest(bytes, digest),
        "canonical request digest or size is invalid"
      )
      .flatMap { value =>
        value.decode.flatMap(request =>
          encode(request).flatMap(canonical =>
            Either.cond(
              canonical.bytes == bytes && canonical.digest == digest,
              value,
              "stored request bytes are not canonical"
            )
          )
        )
      }

  private def digest(bytes: ByteVector): ContentDigest =
    val value = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    ContentDigest.unsafeFrom(s"sha256:$value")

enum ManagedIntentFailure derives CanEqual:
  case RequestRejected(reasons: Diagnostics)
  case CanonicalizationFailed(message: String)

  def diagnostics: Diagnostics = this match
    case ManagedIntentFailure.RequestRejected(value)          => value
    case ManagedIntentFailure.CanonicalizationFailed(message) =>
      Diagnostics.one(
        Diagnostic(
          "durable-request-canonicalization-failed",
          "the managed request could not be encoded canonically",
          Map("reason" -> message)
        )
      )

object RetryReason:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("retryReason", "must not be null"))
    else
      val normalized = raw.trim
      if normalized.isEmpty then Left(ValidationFailure("retryReason", "must not be empty"))
      else if normalized.length > 4096 then
        Left(ValidationFailure("retryReason", "must contain at most 4096 characters"))
      else if raw.exists(_.isControl) then
        Left(ValidationFailure("retryReason", "must not contain control characters"))
      else Right(normalized)

  extension (reason: Type) def value: String = reason
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

type RetryReason = RetryReason.Type

enum RetryAuthorization derives CanEqual:
  case Manual(reason: RetryReason)
  case Automatic(reason: RetryReason)

  def retryReason: RetryReason = this match
    case RetryAuthorization.Manual(value)    => value
    case RetryAuthorization.Automatic(value) => value

final case class ManagedIntent(
    submissionKey: SubmissionKey,
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    request: CanonicalRequest,
    recordedAt: Instant,
    retrySafety: RetrySafety = RetrySafety.Unknown
) derives CanEqual

object ManagedIntent:
  def from(
      request: LaunchSpec,
      recordedAt: Instant,
      policy: ManagedRequestPolicy = ManagedRequestPolicy.rejectEnvironmentValues
  ): Either[ManagedIntentFailure, ManagedIntent] =
    CanonicalRequest.from(request, policy).flatMap { canonical =>
      AttemptId
        .from(s"managed-${canonical.digest.value.stripPrefix("sha256:").take(32)}")
        .left
        .map(problem => ManagedIntentFailure.CanonicalizationFailed(problem.reason))
        .map { attemptId =>
          ManagedIntent(
            request.submissionKey,
            attemptId,
            AttemptEpoch.initial,
            canonical,
            recordedAt,
            request.retrySafety
          )
        }
    }

enum ManagedPhase derives CanEqual:
  case IntentRecorded
  case Submitting(claimedAt: Instant)
  case AcceptanceUnknown(reason: AcceptanceUncertainty, evidence: EvidenceBundle)
  case Bound(job: JobRef)
  case SubmissionRejected(diagnostics: Diagnostics, evidence: Option[EvidenceBundle])
  case SubmissionUnavailable(result: SubmissionAttempt)
  case Terminal(outcome: WorkloadOutcome, evidence: EvidenceBundle)

final case class BindingRecord(
    epoch: AttemptEpoch,
    job: JobRef,
    recordedAt: Instant,
    evidence: EvidenceBundle,
    reconciled: Boolean
) derives CanEqual

enum ManagedObservation derives CanEqual:
  case Unobserved
  case Current(result: ObservationResult)
  case Unavailable(
      attemptedAt: Instant,
      diagnostics: Diagnostics,
      evidence: EvidenceBundle
  )
  case Stale(
      lastKnown: Option[ObservationResult],
      attemptedAt: Instant,
      diagnostics: Diagnostics,
      evidence: EvidenceBundle
  )

enum ManagedAccounting derives CanEqual:
  case Unobserved
  case Current(record: AccountingRecord)
  case Unavailable(attemptedAt: Instant, diagnostics: Diagnostics, evidence: EvidenceBundle)

enum ManagedCancellation derives CanEqual:
  case NotRequested
  case Requested(at: Instant)
  case Dispatching(at: Instant)
  case Acknowledged(evidence: EvidenceBundle)
  case NotFound(evidence: EvidenceBundle)
  case Rejected(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case Unknown(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case Reconciled(outcome: WorkloadOutcome, evidence: EvidenceBundle)

final case class ManagedAttempt(
    intent: ManagedIntent,
    phase: ManagedPhase,
    bindings: Vector[BindingRecord],
    observation: ManagedObservation,
    accounting: ManagedAccounting,
    cancellation: ManagedCancellation,
    updatedAt: Instant
) derives CanEqual:
  def currentBinding: Option[BindingRecord] =
    bindings.reverseIterator.find(_.epoch == intent.epoch)
  def currentJob: Option[JobRef] = currentBinding.map(_.job)
  def isTerminal: Boolean = phase.isInstanceOf[ManagedPhase.Terminal]

enum OutboxAction derives CanEqual:
  case Submit(submissionKey: SubmissionKey, epoch: AttemptEpoch)
  case Cancel(submissionKey: SubmissionKey, job: JobRef)

enum OutboxStatus derives CanEqual:
  case Pending
  case InFlight(startedAt: Instant)
  case Completed(completedAt: Instant)
  case Uncertain(at: Instant, diagnostic: String)
  case Superseded(at: Instant, diagnostic: String)

final case class OutboxEntry(
    id: OutboxId,
    action: OutboxAction,
    status: OutboxStatus,
    createdAt: Instant
) derives CanEqual

enum ManagedEvent derives CanEqual:
  case IntentRecorded(intent: ManagedIntent)
  case SubmissionClaimed(submissionKey: SubmissionKey, epoch: AttemptEpoch)
  case SubmissionResolved(
      submissionKey: SubmissionKey,
      epoch: AttemptEpoch,
      result: SubmissionAttempt
  )
  case SubmissionRecoveryRequired(submissionKey: SubmissionKey, epoch: AttemptEpoch)
  case SubmissionRetried(
      submissionKey: SubmissionKey,
      previousEpoch: AttemptEpoch,
      nextEpoch: AttemptEpoch,
      authorization: RetryAuthorization
  )
  case BindingReconciled(submissionKey: SubmissionKey, epoch: AttemptEpoch, job: JobRef)
  case ObservationRecorded(submissionKey: SubmissionKey)
  case ObservationUnavailable(submissionKey: SubmissionKey)
  case AccountingRecorded(submissionKey: SubmissionKey)
  case CancellationRequested(submissionKey: SubmissionKey)
  case CancellationClaimed(submissionKey: SubmissionKey)
  case CancellationRecoveryRequired(submissionKey: SubmissionKey)
  case CancellationResolved(submissionKey: SubmissionKey, result: CancellationAttempt)
  case TerminalReconciled(submissionKey: SubmissionKey, outcome: WorkloadOutcome)

final case class CommittedEvent(
    cursor: EventCursor,
    revision: StoreRevision,
    committedAt: Instant,
    event: ManagedEvent
) derives CanEqual

final case class ControlState(
    revision: StoreRevision,
    attempts: Map[SubmissionKey, ManagedAttempt],
    outbox: Map[OutboxId, OutboxEntry],
    events: Vector[CommittedEvent]
) derives CanEqual

object ControlState:
  val empty: ControlState = ControlState(StoreRevision.initial, Map.empty, Map.empty, Vector.empty)

enum EpochFence derives CanEqual:
  case Current
  case Stale(expected: AttemptEpoch, received: AttemptEpoch)

object EpochFence:
  def validate(attempt: ManagedAttempt, received: AttemptEpoch): EpochFence =
    if attempt.intent.epoch == received then EpochFence.Current
    else EpochFence.Stale(attempt.intent.epoch, received)
