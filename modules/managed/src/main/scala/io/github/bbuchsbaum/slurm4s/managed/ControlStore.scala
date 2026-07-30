package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.Ref
import cats.effect.kernel.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.EventCursor
import io.github.bbuchsbaum.slurm4s.core.SubmissionKey

final case class EventPage(
    events: Vector[CommittedEvent],
    next: EventCursor,
    endOfJournal: Boolean
) derives CanEqual

trait ControlStore[F[_]]:
  def transact(command: ControlCommand): F[Either[ControlFailure, ControlCommit]]
  def snapshot: F[ControlState]
  def attempt(submissionKey: SubmissionKey): F[Option[ManagedAttempt]]
  def events(after: EventCursor, maximum: Int): F[EventPage]

  def pendingOutbox(maximum: Int): F[Vector[OutboxEntry]]
  def nonTerminal(maximum: Int): F[Vector[ManagedAttempt]]
  def bound(maximum: Int): F[Vector[ManagedAttempt]]

final class InMemoryControlStore[F[_]: Sync] private (state: Ref[F, ControlState])
    extends ControlStore[F]:
  def transact(command: ControlCommand): F[Either[ControlFailure, ControlCommit]] =
    state.modify { current =>
      ControlTransition(current, command) match
        case Left(failure) => current -> Left(failure)
        case Right(commit) => commit.state -> Right(commit)
    }

  def snapshot: F[ControlState] = state.get

  def attempt(submissionKey: SubmissionKey): F[Option[ManagedAttempt]] =
    state.get.map(_.attempts.get(submissionKey))

  def events(after: EventCursor, maximum: Int): F[EventPage] =
    state.get.map(current => ControlStore.page(current, after, maximum))

  def pendingOutbox(maximum: Int): F[Vector[OutboxEntry]] =
    state.get.map(current =>
      ControlStore.boundedOldest(
        current.outbox.valuesIterator.filter(_.status == OutboxStatus.Pending),
        maximum,
        entry => entry.createdAt -> entry.id.value
      )
    )

  def nonTerminal(maximum: Int): F[Vector[ManagedAttempt]] =
    state.get.map(current =>
      ControlStore.boundedOldest(
        current.attempts.valuesIterator.filterNot(_.isTerminal),
        maximum,
        attempt => attempt.intent.recordedAt -> attempt.intent.submissionKey.value
      )
    )

  def bound(maximum: Int): F[Vector[ManagedAttempt]] =
    state.get.map(current =>
      ControlStore.boundedOldest(
        current.attempts.valuesIterator.filter(_.phase.isInstanceOf[ManagedPhase.Bound]),
        maximum,
        attempt => attempt.updatedAt -> attempt.intent.submissionKey.value
      )
    )

object InMemoryControlStore:
  def create[F[_]: Sync](initial: ControlState = ControlState.empty): F[InMemoryControlStore[F]] =
    Ref.of[F, ControlState](initial).map(InMemoryControlStore(_))

object ControlStore:
  private[managed] def boundedOldest[A](
      values: Iterator[A],
      maximum: Int,
      key: A => (java.time.Instant, String)
  ): Vector[A] =
    val limit = math.max(0, maximum)
    if limit == 0 then Vector.empty
    else values.toVector.sortBy(key).take(limit)

  private[managed] def page(
      state: ControlState,
      after: EventCursor,
      maximum: Int
  ): EventPage =
    val limit = math.max(0, maximum)
    val values = state.events.filter(_.cursor.value > after.value).take(limit)
    val next = values.lastOption.map(_.cursor).getOrElse(after)
    EventPage(
      values,
      next,
      endOfJournal = !state.events.exists(_.cursor.value > next.value)
    )
