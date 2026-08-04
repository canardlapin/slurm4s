package io.github.bbuchsbaum.slurm4s.managed

import cats.syntax.all.*
import io.circe.HCursor
import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentDomainJson

import java.time.Instant
import scala.util.Try

final private[managed] case class ControlSnapshot(
    state: ControlState,
    minimumAvailableAfter: EventCursor
)

/** Explicit durable codec for the materialized control projection.
  *
  * The command journal remains the source of transitions. A snapshot only replaces a replayed
  * prefix and therefore carries the exact reducer state plus the oldest event cursor that remains
  * pageable after compaction.
  */
private[managed] object ControlSnapshotJson:
  def encode(value: ControlSnapshot): Json =
    Json.obj(
      "revision" -> Json.fromLong(value.state.revision.value),
      "minimumAvailableAfter" -> Json.fromLong(value.minimumAvailableAfter.value),
      "attempts" -> Json.arr(
        value.state.attempts.values.toVector
          .sortBy(_.intent.submissionKey.value)
          .map(encodeAttempt)*
      ),
      "outbox" -> Json.arr(
        value.state.outbox.values.toVector.sortBy(_.id.value).map(encodeOutbox)*
      ),
      "events" -> Json.arr(value.state.events.map(encodeCommittedEvent)*)
    )

  def decode(json: Json): Either[String, ControlSnapshot] =
    for
      cursor <- objectCursor(json, "control snapshot")
      revisionRaw <- long(cursor, "revision")
      revision <- StoreRevision.from(revisionRaw).left.map(_.reason)
      minimumRaw <- long(cursor, "minimumAvailableAfter")
      minimum <- EventCursor.from(minimumRaw).left.map(_.reason)
      attemptJson <- array(cursor, "attempts")
      attempts <- attemptJson.traverse(decodeAttempt)
      attemptMap = attempts.map(value => value.intent.submissionKey -> value).toMap
      _ <- Either.cond(attemptMap.size == attempts.size, (), "snapshot has duplicate attempts")
      outboxJson <- array(cursor, "outbox")
      outbox <- outboxJson.traverse(decodeOutbox)
      outboxMap = outbox.map(value => value.id -> value).toMap
      _ <- Either.cond(outboxMap.size == outbox.size, (), "snapshot has duplicate outbox entries")
      _ <- Either.cond(
        outbox.forall(entry => attemptMap.contains(outboxSubmissionKey(entry.action))),
        (),
        "snapshot outbox references a missing attempt"
      )
      eventJson <- array(cursor, "events")
      events <- eventJson.traverse(decodeCommittedEvent)
      _ <- validateEvents(events, minimum, revision)
      _ <- Either.cond(
        events.forall(event => attemptMap.contains(eventSubmissionKey(event.event))),
        (),
        "snapshot event references a missing attempt"
      )
      _ <- Either.cond(
        revision != StoreRevision.initial ||
          (minimum == EventCursor.origin &&
            attemptMap.isEmpty &&
            outboxMap.isEmpty &&
            events.isEmpty),
        (),
        "initial snapshot must be completely empty and start at the origin"
      )
    yield ControlSnapshot(ControlState(revision, attemptMap, outboxMap, events), minimum)

  private def encodeAttempt(value: ManagedAttempt): Json =
    Json.obj(
      "intent" -> ControlCommandJson.encode(ControlCommand.RecordIntent(value.intent)),
      "phase" -> encodePhase(value.phase),
      "bindings" -> Json.arr(value.bindings.map(encodeBinding)*),
      "observation" -> encodeObservation(value.observation),
      "accounting" -> encodeAccounting(value.accounting),
      "cancellation" -> encodeCancellation(value.cancellation),
      "updatedAt" -> instant(value.updatedAt)
    )

  private def decodeAttempt(json: Json): Either[String, ManagedAttempt] =
    for
      cursor <- objectCursor(json, "managed attempt")
      intentJson <- fieldJson(cursor, "intent")
      intent <- ControlCommandJson.decode(intentJson).flatMap {
        case ControlCommand.RecordIntent(value) => Right(value)
        case _                                  => Left("snapshot intent is not record-intent")
      }
      phaseJson <- fieldJson(cursor, "phase")
      phase <- decodePhase(phaseJson)
      bindingJson <- array(cursor, "bindings")
      bindings <- bindingJson.traverse(decodeBinding)
      observationJson <- fieldJson(cursor, "observation")
      observation <- decodeObservation(observationJson)
      accountingJson <- fieldJson(cursor, "accounting")
      accounting <- decodeAccounting(accountingJson)
      cancellationJson <- fieldJson(cursor, "cancellation")
      cancellation <- decodeCancellation(cancellationJson)
      updatedAt <- instant(cursor, "updatedAt")
    yield ManagedAttempt(
      intent,
      phase,
      bindings,
      observation,
      accounting,
      cancellation,
      updatedAt
    )

  private def encodePhase(value: ManagedPhase): Json = value match
    case ManagedPhase.IntentRecorded        => tagged("intent-recorded")
    case ManagedPhase.Submitting(claimedAt) =>
      tagged("submitting", "claimedAt" -> instant(claimedAt))
    case ManagedPhase.AcceptanceUnknown(reason, evidence) =>
      tagged(
        "acceptance-unknown",
        "reason" -> Json.fromString(encodeAcceptance(reason)),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )
    case ManagedPhase.Bound(job) =>
      tagged("bound", "job" -> AgentDomainJson.encodeJobRef(job))
    case ManagedPhase.SubmissionRejected(diagnostics, evidence) =>
      tagged(
        "submission-rejected",
        "diagnostics" -> AgentDomainJson.encodeDiagnostics(diagnostics),
        "evidence" -> evidence.fold(Json.Null)(AgentDomainJson.encodeEvidence)
      )
    case ManagedPhase.SubmissionUnavailable(result) =>
      tagged("submission-unavailable", "result" -> AgentDomainJson.encodeSubmission(result))
    case ManagedPhase.Terminal(outcome, evidence) =>
      tagged(
        "terminal",
        "outcome" -> AgentDomainJson.encodeWorkloadOutcome(outcome),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )

  private def decodePhase(json: Json): Either[String, ManagedPhase] =
    for
      cursor <- objectCursor(json, "managed phase")
      kind <- string(cursor, "kind")
      phase <- kind match
        case "intent-recorded"    => Right(ManagedPhase.IntentRecorded)
        case "submitting"         => instant(cursor, "claimedAt").map(ManagedPhase.Submitting.apply)
        case "acceptance-unknown" =>
          for
            reasonText <- string(cursor, "reason")
            reason <- decodeAcceptance(reasonText)
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedPhase.AcceptanceUnknown(reason, evidence)
        case "bound"               => decodeJob(cursor, "job").map(ManagedPhase.Bound.apply)
        case "submission-rejected" =>
          for
            diagnostics <- decodeDiagnostics(cursor, "diagnostics")
            evidence <- optionalEvidence(cursor, "evidence")
          yield ManagedPhase.SubmissionRejected(diagnostics, evidence)
        case "submission-unavailable" =>
          fieldJson(cursor, "result")
            .flatMap(AgentDomainJson.decodeSubmission)
            .map(ManagedPhase.SubmissionUnavailable.apply)
        case "terminal" =>
          for
            outcomeJson <- fieldJson(cursor, "outcome")
            outcome <- AgentDomainJson.decodeWorkloadOutcome(outcomeJson)
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedPhase.Terminal(outcome, evidence)
        case other => Left(s"unknown managed phase: $other")
    yield phase

  private def encodeBinding(value: BindingRecord): Json =
    Json.obj(
      "epoch" -> Json.fromLong(value.epoch.value),
      "job" -> AgentDomainJson.encodeJobRef(value.job),
      "recordedAt" -> instant(value.recordedAt),
      "evidence" -> AgentDomainJson.encodeEvidence(value.evidence),
      "reconciled" -> Json.fromBoolean(value.reconciled)
    )

  private def decodeBinding(json: Json): Either[String, BindingRecord] =
    for
      cursor <- objectCursor(json, "binding record")
      epochRaw <- long(cursor, "epoch")
      epoch <- AttemptEpoch.from(epochRaw).left.map(_.reason)
      job <- decodeJob(cursor, "job")
      recordedAt <- instant(cursor, "recordedAt")
      evidence <- decodeEvidence(cursor, "evidence")
      reconciled <- boolean(cursor, "reconciled")
    yield BindingRecord(epoch, job, recordedAt, evidence, reconciled)

  private def encodeObservation(value: ManagedObservation): Json = value match
    case ManagedObservation.Unobserved      => tagged("unobserved")
    case ManagedObservation.Current(result) =>
      tagged("current", "result" -> AgentDomainJson.encodeObservationResult(result))
    case ManagedObservation.Unavailable(attemptedAt, diagnostics, evidence) =>
      tagged(
        "unavailable",
        "attemptedAt" -> instant(attemptedAt),
        "diagnostics" -> AgentDomainJson.encodeDiagnostics(diagnostics),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )
    case ManagedObservation.Stale(lastKnown, attemptedAt, diagnostics, evidence) =>
      tagged(
        "stale",
        "lastKnown" -> lastKnown.fold(Json.Null)(AgentDomainJson.encodeObservationResult),
        "attemptedAt" -> instant(attemptedAt),
        "diagnostics" -> AgentDomainJson.encodeDiagnostics(diagnostics),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )

  private def decodeObservation(json: Json): Either[String, ManagedObservation] =
    for
      cursor <- objectCursor(json, "managed observation")
      kind <- string(cursor, "kind")
      observation <- kind match
        case "unobserved" => Right(ManagedObservation.Unobserved)
        case "current"    =>
          fieldJson(cursor, "result")
            .flatMap(AgentDomainJson.decodeObservationResult)
            .map(ManagedObservation.Current.apply)
        case "unavailable" =>
          for
            attemptedAt <- instant(cursor, "attemptedAt")
            diagnostics <- decodeDiagnostics(cursor, "diagnostics")
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedObservation.Unavailable(attemptedAt, diagnostics, evidence)
        case "stale" =>
          for
            lastKnown <- optionalObservation(cursor, "lastKnown")
            attemptedAt <- instant(cursor, "attemptedAt")
            diagnostics <- decodeDiagnostics(cursor, "diagnostics")
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedObservation.Stale(lastKnown, attemptedAt, diagnostics, evidence)
        case other => Left(s"unknown managed observation: $other")
    yield observation

  private def encodeAccounting(value: ManagedAccounting): Json = value match
    case ManagedAccounting.Unobserved      => tagged("unobserved")
    case ManagedAccounting.Current(record) =>
      tagged("current", "record" -> AgentDomainJson.encodeAccountingRecord(record))
    case ManagedAccounting.Unavailable(attemptedAt, diagnostics, evidence) =>
      tagged(
        "unavailable",
        "attemptedAt" -> instant(attemptedAt),
        "diagnostics" -> AgentDomainJson.encodeDiagnostics(diagnostics),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )

  private def decodeAccounting(json: Json): Either[String, ManagedAccounting] =
    for
      cursor <- objectCursor(json, "managed accounting")
      kind <- string(cursor, "kind")
      accounting <- kind match
        case "unobserved" => Right(ManagedAccounting.Unobserved)
        case "current"    =>
          fieldJson(cursor, "record")
            .flatMap(AgentDomainJson.decodeAccountingRecord)
            .map(ManagedAccounting.Current.apply)
        case "unavailable" =>
          for
            attemptedAt <- instant(cursor, "attemptedAt")
            diagnostics <- decodeDiagnostics(cursor, "diagnostics")
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedAccounting.Unavailable(attemptedAt, diagnostics, evidence)
        case other => Left(s"unknown managed accounting: $other")
    yield accounting

  private def encodeCancellation(value: ManagedCancellation): Json = value match
    case ManagedCancellation.NotRequested           => tagged("not-requested")
    case ManagedCancellation.Requested(at)          => tagged("requested", "at" -> instant(at))
    case ManagedCancellation.Dispatching(at)        => tagged("dispatching", "at" -> instant(at))
    case ManagedCancellation.Acknowledged(evidence) =>
      tagged("acknowledged", "evidence" -> AgentDomainJson.encodeEvidence(evidence))
    case ManagedCancellation.NotFound(evidence) =>
      tagged("not-found", "evidence" -> AgentDomainJson.encodeEvidence(evidence))
    case ManagedCancellation.Rejected(diagnostics, evidence) =>
      tagged(
        "rejected",
        "diagnostics" -> AgentDomainJson.encodeDiagnostics(diagnostics),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )
    case ManagedCancellation.Unknown(diagnostics, evidence) =>
      tagged(
        "unknown",
        "diagnostics" -> AgentDomainJson.encodeDiagnostics(diagnostics),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )
    case ManagedCancellation.Reconciled(outcome, evidence) =>
      tagged(
        "reconciled",
        "outcome" -> AgentDomainJson.encodeWorkloadOutcome(outcome),
        "evidence" -> AgentDomainJson.encodeEvidence(evidence)
      )

  private def decodeCancellation(json: Json): Either[String, ManagedCancellation] =
    for
      cursor <- objectCursor(json, "managed cancellation")
      kind <- string(cursor, "kind")
      cancellation <- kind match
        case "not-requested" => Right(ManagedCancellation.NotRequested)
        case "requested"     => instant(cursor, "at").map(ManagedCancellation.Requested.apply)
        case "dispatching"   => instant(cursor, "at").map(ManagedCancellation.Dispatching.apply)
        case "acknowledged"  =>
          decodeEvidence(cursor, "evidence").map(ManagedCancellation.Acknowledged.apply)
        case "not-found" =>
          decodeEvidence(cursor, "evidence").map(ManagedCancellation.NotFound.apply)
        case "rejected" =>
          for
            diagnostics <- decodeDiagnostics(cursor, "diagnostics")
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedCancellation.Rejected(diagnostics, evidence)
        case "unknown" =>
          for
            diagnostics <- decodeDiagnostics(cursor, "diagnostics")
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedCancellation.Unknown(diagnostics, evidence)
        case "reconciled" =>
          for
            outcomeJson <- fieldJson(cursor, "outcome")
            outcome <- AgentDomainJson.decodeWorkloadOutcome(outcomeJson)
            evidence <- decodeEvidence(cursor, "evidence")
          yield ManagedCancellation.Reconciled(outcome, evidence)
        case other => Left(s"unknown managed cancellation: $other")
    yield cancellation

  private def encodeOutbox(value: OutboxEntry): Json =
    Json.obj(
      "id" -> Json.fromString(value.id.value),
      "action" -> encodeOutboxAction(value.action),
      "status" -> encodeOutboxStatus(value.status),
      "createdAt" -> instant(value.createdAt)
    )

  private def decodeOutbox(json: Json): Either[String, OutboxEntry] =
    for
      cursor <- objectCursor(json, "outbox entry")
      idText <- string(cursor, "id")
      id <- OutboxId.from(idText).left.map(_.reason)
      actionJson <- fieldJson(cursor, "action")
      action <- decodeOutboxAction(actionJson)
      statusJson <- fieldJson(cursor, "status")
      status <- decodeOutboxStatus(statusJson)
      createdAt <- instant(cursor, "createdAt")
    yield OutboxEntry(id, action, status, createdAt)

  private def encodeOutboxAction(value: OutboxAction): Json = value match
    case OutboxAction.Submit(key, epoch) =>
      tagged(
        "submit",
        "submissionKey" -> Json.fromString(key.value),
        "epoch" -> Json.fromLong(epoch.value)
      )
    case OutboxAction.Cancel(key, job) =>
      tagged(
        "cancel",
        "submissionKey" -> Json.fromString(key.value),
        "job" -> AgentDomainJson.encodeJobRef(job)
      )

  private def decodeOutboxAction(json: Json): Either[String, OutboxAction] =
    for
      cursor <- objectCursor(json, "outbox action")
      kind <- string(cursor, "kind")
      key <- submissionKey(cursor)
      action <- kind match
        case "submit" =>
          for
            epochRaw <- long(cursor, "epoch")
            epoch <- AttemptEpoch.from(epochRaw).left.map(_.reason)
          yield OutboxAction.Submit(key, epoch)
        case "cancel" => decodeJob(cursor, "job").map(OutboxAction.Cancel(key, _))
        case other    => Left(s"unknown outbox action: $other")
    yield action

  private def encodeOutboxStatus(value: OutboxStatus): Json = value match
    case OutboxStatus.Pending             => tagged("pending")
    case OutboxStatus.InFlight(startedAt) =>
      tagged("in-flight", "startedAt" -> instant(startedAt))
    case OutboxStatus.Completed(completedAt) =>
      tagged("completed", "completedAt" -> instant(completedAt))
    case OutboxStatus.Uncertain(at, diagnostic) =>
      tagged("uncertain", "at" -> instant(at), "diagnostic" -> Json.fromString(diagnostic))
    case OutboxStatus.Superseded(at, diagnostic) =>
      tagged("superseded", "at" -> instant(at), "diagnostic" -> Json.fromString(diagnostic))

  private def decodeOutboxStatus(json: Json): Either[String, OutboxStatus] =
    for
      cursor <- objectCursor(json, "outbox status")
      kind <- string(cursor, "kind")
      status <- kind match
        case "pending"   => Right(OutboxStatus.Pending)
        case "in-flight" => instant(cursor, "startedAt").map(OutboxStatus.InFlight.apply)
        case "completed" => instant(cursor, "completedAt").map(OutboxStatus.Completed.apply)
        case "uncertain" =>
          (instant(cursor, "at"), string(cursor, "diagnostic")).mapN(OutboxStatus.Uncertain.apply)
        case "superseded" =>
          (instant(cursor, "at"), string(cursor, "diagnostic")).mapN(OutboxStatus.Superseded.apply)
        case other => Left(s"unknown outbox status: $other")
    yield status

  private def encodeCommittedEvent(value: CommittedEvent): Json =
    Json.obj(
      "cursor" -> Json.fromLong(value.cursor.value),
      "revision" -> Json.fromLong(value.revision.value),
      "committedAt" -> instant(value.committedAt),
      "event" -> encodeEvent(value.event)
    )

  private def decodeCommittedEvent(json: Json): Either[String, CommittedEvent] =
    for
      cursor <- objectCursor(json, "committed event")
      cursorRaw <- long(cursor, "cursor")
      eventCursor <- EventCursor.from(cursorRaw).left.map(_.reason)
      revisionRaw <- long(cursor, "revision")
      revision <- StoreRevision.from(revisionRaw).left.map(_.reason)
      committedAt <- instant(cursor, "committedAt")
      eventJson <- fieldJson(cursor, "event")
      event <- decodeEvent(eventJson)
    yield CommittedEvent(eventCursor, revision, committedAt, event)

  private def encodeEvent(value: ManagedEvent): Json = value match
    case ManagedEvent.IntentRecorded(intent) =>
      tagged(
        "intent-recorded",
        "intent" -> ControlCommandJson.encode(ControlCommand.RecordIntent(intent))
      )
    case ManagedEvent.SubmissionClaimed(key, epoch) =>
      tagged(
        "submission-claimed",
        "submissionKey" -> Json.fromString(key.value),
        "epoch" -> Json.fromLong(epoch.value)
      )
    case ManagedEvent.SubmissionResolved(key, epoch, result) =>
      tagged(
        "submission-resolved",
        "submissionKey" -> Json.fromString(key.value),
        "epoch" -> Json.fromLong(epoch.value),
        "result" -> AgentDomainJson.encodeSubmission(result)
      )
    case ManagedEvent.SubmissionRecoveryRequired(key, epoch) =>
      tagged(
        "submission-recovery-required",
        "submissionKey" -> Json.fromString(key.value),
        "epoch" -> Json.fromLong(epoch.value)
      )
    case ManagedEvent.SubmissionRetried(key, previousEpoch, nextEpoch, authorization) =>
      tagged(
        "submission-retried",
        "submissionKey" -> Json.fromString(key.value),
        "previousEpoch" -> Json.fromLong(previousEpoch.value),
        "nextEpoch" -> Json.fromLong(nextEpoch.value),
        "authorization" -> ControlCommandJson.encodeRetryAuthorization(authorization)
      )
    case ManagedEvent.BindingReconciled(key, epoch, job) =>
      tagged(
        "binding-reconciled",
        "submissionKey" -> Json.fromString(key.value),
        "epoch" -> Json.fromLong(epoch.value),
        "job" -> AgentDomainJson.encodeJobRef(job)
      )
    case ManagedEvent.ObservationRecorded(key) =>
      keyed("observation-recorded", key)
    case ManagedEvent.ObservationUnavailable(key) =>
      keyed("observation-unavailable", key)
    case ManagedEvent.AccountingRecorded(key) =>
      keyed("accounting-recorded", key)
    case ManagedEvent.CancellationRequested(key) =>
      keyed("cancellation-requested", key)
    case ManagedEvent.CancellationClaimed(key) =>
      keyed("cancellation-claimed", key)
    case ManagedEvent.CancellationRecoveryRequired(key) =>
      keyed("cancellation-recovery-required", key)
    case ManagedEvent.CancellationResolved(key, result) =>
      tagged(
        "cancellation-resolved",
        "submissionKey" -> Json.fromString(key.value),
        "result" -> AgentDomainJson.encodeCancellation(result)
      )
    case ManagedEvent.TerminalReconciled(key, outcome) =>
      tagged(
        "terminal-reconciled",
        "submissionKey" -> Json.fromString(key.value),
        "outcome" -> AgentDomainJson.encodeWorkloadOutcome(outcome)
      )

  private def decodeEvent(json: Json): Either[String, ManagedEvent] =
    for
      cursor <- objectCursor(json, "managed event")
      kind <- string(cursor, "kind")
      event <- kind match
        case "intent-recorded" =>
          fieldJson(cursor, "intent").flatMap(ControlCommandJson.decode).flatMap {
            case ControlCommand.RecordIntent(intent) => Right(ManagedEvent.IntentRecorded(intent))
            case _                                   => Left("event intent is not record-intent")
          }
        case "submission-claimed" =>
          (submissionKey(cursor), epoch(cursor, "epoch")).mapN(ManagedEvent.SubmissionClaimed.apply)
        case "submission-resolved" =>
          for
            key <- submissionKey(cursor)
            valueEpoch <- epoch(cursor, "epoch")
            resultJson <- fieldJson(cursor, "result")
            result <- AgentDomainJson.decodeSubmission(resultJson)
          yield ManagedEvent.SubmissionResolved(key, valueEpoch, result)
        case "submission-recovery-required" =>
          (submissionKey(cursor), epoch(cursor, "epoch"))
            .mapN(ManagedEvent.SubmissionRecoveryRequired.apply)
        case "submission-retried" =>
          for
            key <- submissionKey(cursor)
            previous <- epoch(cursor, "previousEpoch")
            next <- epoch(cursor, "nextEpoch")
            authorizationJson <- fieldJson(cursor, "authorization")
            authorization <- ControlCommandJson.decodeRetryAuthorization(authorizationJson)
          yield ManagedEvent.SubmissionRetried(key, previous, next, authorization)
        case "binding-reconciled" =>
          for
            key <- submissionKey(cursor)
            valueEpoch <- epoch(cursor, "epoch")
            job <- decodeJob(cursor, "job")
          yield ManagedEvent.BindingReconciled(key, valueEpoch, job)
        case "observation-recorded" =>
          submissionKey(cursor).map(ManagedEvent.ObservationRecorded.apply)
        case "observation-unavailable" =>
          submissionKey(cursor).map(ManagedEvent.ObservationUnavailable.apply)
        case "accounting-recorded" =>
          submissionKey(cursor).map(ManagedEvent.AccountingRecorded.apply)
        case "cancellation-requested" =>
          submissionKey(cursor).map(ManagedEvent.CancellationRequested.apply)
        case "cancellation-claimed" =>
          submissionKey(cursor).map(ManagedEvent.CancellationClaimed.apply)
        case "cancellation-recovery-required" =>
          submissionKey(cursor).map(ManagedEvent.CancellationRecoveryRequired.apply)
        case "cancellation-resolved" =>
          for
            key <- submissionKey(cursor)
            resultJson <- fieldJson(cursor, "result")
            result <- AgentDomainJson.decodeCancellation(resultJson)
          yield ManagedEvent.CancellationResolved(key, result)
        case "terminal-reconciled" =>
          for
            key <- submissionKey(cursor)
            outcomeJson <- fieldJson(cursor, "outcome")
            outcome <- AgentDomainJson.decodeWorkloadOutcome(outcomeJson)
          yield ManagedEvent.TerminalReconciled(key, outcome)
        case other => Left(s"unknown managed event: $other")
    yield event

  private def validateEvents(
      events: Vector[CommittedEvent],
      minimum: EventCursor,
      revision: StoreRevision
  ): Either[String, Unit] =
    val ordered = events
      .sliding(2)
      .forall {
        case Vector(left, right) =>
          left.cursor.value + 1L == right.cursor.value &&
          (right.revision.value == left.revision.value ||
            right.revision.value == left.revision.value + 1L)
        case _ => true
      }
    val startsAtMinimum = events.headOption.forall(_.cursor.value == minimum.value + 1L)
    val revisionsValid =
      events.forall(event => event.revision.value > 0L && event.revision.value <= revision.value)
    val emptinessMatchesRevision = events.isEmpty == (revision == StoreRevision.initial)
    val endsAtRevision = events.lastOption.forall(_.revision == revision)
    Either.cond(
      ordered &&
        startsAtMinimum &&
        revisionsValid &&
        emptinessMatchesRevision &&
        endsAtRevision,
      (),
      "snapshot event cursors or revisions are inconsistent"
    )

  private def outboxSubmissionKey(value: OutboxAction): SubmissionKey = value match
    case OutboxAction.Submit(key, _) => key
    case OutboxAction.Cancel(key, _) => key

  private def eventSubmissionKey(value: ManagedEvent): SubmissionKey = value match
    case ManagedEvent.IntentRecorded(intent)             => intent.submissionKey
    case ManagedEvent.SubmissionClaimed(key, _)          => key
    case ManagedEvent.SubmissionResolved(key, _, _)      => key
    case ManagedEvent.SubmissionRecoveryRequired(key, _) => key
    case ManagedEvent.SubmissionRetried(key, _, _, _)    => key
    case ManagedEvent.BindingReconciled(key, _, _)       => key
    case ManagedEvent.ObservationRecorded(key)           => key
    case ManagedEvent.ObservationUnavailable(key)        => key
    case ManagedEvent.AccountingRecorded(key)            => key
    case ManagedEvent.CancellationRequested(key)         => key
    case ManagedEvent.CancellationClaimed(key)           => key
    case ManagedEvent.CancellationRecoveryRequired(key)  => key
    case ManagedEvent.CancellationResolved(key, _)       => key
    case ManagedEvent.TerminalReconciled(key, _)         => key

  private def encodeAcceptance(value: AcceptanceUncertainty): String = value match
    case AcceptanceUncertainty.ResponseLost           => "response-lost"
    case AcceptanceUncertainty.TransportInterrupted   => "transport-interrupted"
    case AcceptanceUncertainty.ResponseUnparseable    => "response-unparseable"
    case AcceptanceUncertainty.PersistenceInterrupted => "persistence-interrupted"
    case AcceptanceUncertainty.Unclassified           => "unclassified"

  private def decodeAcceptance(value: String): Either[String, AcceptanceUncertainty] = value match
    case "response-lost"           => Right(AcceptanceUncertainty.ResponseLost)
    case "transport-interrupted"   => Right(AcceptanceUncertainty.TransportInterrupted)
    case "response-unparseable"    => Right(AcceptanceUncertainty.ResponseUnparseable)
    case "persistence-interrupted" => Right(AcceptanceUncertainty.PersistenceInterrupted)
    case "unclassified"            => Right(AcceptanceUncertainty.Unclassified)
    case other                     => Left(s"unknown acceptance uncertainty: $other")

  private def tagged(kind: String, fields: (String, Json)*): Json =
    Json.obj(("kind" -> Json.fromString(kind)) +: fields*)

  private def keyed(kind: String, key: SubmissionKey): Json =
    tagged(kind, "submissionKey" -> Json.fromString(key.value))

  private def instant(value: Instant): Json = Json.fromString(value.toString)

  private def objectCursor(json: Json, label: String): Either[String, HCursor] =
    Either.cond(json.isObject, json.hcursor, s"$label must be an object")

  private def fieldJson(cursor: HCursor, name: String): Either[String, Json] =
    cursor.downField(name).focus.toRight(s"missing $name")

  private def array(cursor: HCursor, name: String): Either[String, Vector[Json]] =
    cursor.get[Vector[Json]](name).left.map(_.message)

  private def string(cursor: HCursor, name: String): Either[String, String] =
    cursor.get[String](name).left.map(_.message)

  private def long(cursor: HCursor, name: String): Either[String, Long] =
    cursor.get[Long](name).left.map(_.message)

  private def boolean(cursor: HCursor, name: String): Either[String, Boolean] =
    cursor.get[Boolean](name).left.map(_.message)

  private def instant(cursor: HCursor, name: String): Either[String, Instant] =
    string(cursor, name).flatMap(value => Try(Instant.parse(value)).toEither.left.map(_.getMessage))

  private def submissionKey(cursor: HCursor): Either[String, SubmissionKey] =
    string(cursor, "submissionKey").flatMap(SubmissionKey.from(_).left.map(_.reason))

  private def epoch(cursor: HCursor, name: String): Either[String, AttemptEpoch] =
    long(cursor, name).flatMap(AttemptEpoch.from(_).left.map(_.reason))

  private def decodeJob(cursor: HCursor, name: String): Either[String, JobRef] =
    fieldJson(cursor, name).flatMap(AgentDomainJson.decodeJobRef)

  private def decodeEvidence(cursor: HCursor, name: String): Either[String, EvidenceBundle] =
    fieldJson(cursor, name).flatMap(AgentDomainJson.decodeEvidence)

  private def optionalEvidence(
      cursor: HCursor,
      name: String
  ): Either[String, Option[EvidenceBundle]] =
    cursor.downField(name).focus match
      case None                        => Left(s"missing $name")
      case Some(value) if value.isNull => Right(None)
      case Some(value)                 => AgentDomainJson.decodeEvidence(value).map(Some(_))

  private def decodeDiagnostics(cursor: HCursor, name: String): Either[String, Diagnostics] =
    fieldJson(cursor, name).flatMap(AgentDomainJson.decodeDiagnostics)

  private def optionalObservation(
      cursor: HCursor,
      name: String
  ): Either[String, Option[ObservationResult]] =
    cursor.downField(name).focus match
      case None                        => Left(s"missing $name")
      case Some(value) if value.isNull => Right(None)
      case Some(value) => AgentDomainJson.decodeObservationResult(value).map(Some(_))
