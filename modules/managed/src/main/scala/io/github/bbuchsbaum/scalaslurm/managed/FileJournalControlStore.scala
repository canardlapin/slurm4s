package io.github.bbuchsbaum.scalaslurm.managed

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Semaphore
import cats.syntax.all.*
import io.circe.Json
import io.circe.Printer
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.EventCursor
import io.github.bbuchsbaum.scalaslurm.core.ProtocolVersion
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.codec.VersionedJson
import io.github.bbuchsbaum.scalaslurm.core.codec.WireEnvelope
import io.github.bbuchsbaum.scalaslurm.protocol.FrameCodec
import io.github.bbuchsbaum.scalaslurm.protocol.FrameLimits

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import scala.jdk.CollectionConverters.*
import scala.util.Try

final case class JournalLimits(maximumRecordBytes: ByteLimit, recentEventCacheSize: Int)
    derives CanEqual:
  require(recentEventCacheSize > 0, "recent event cache size must be positive")

object JournalLimits:
  val default: JournalLimits = JournalLimits(
    ByteLimit
      .from(32 * 1024 * 1024)
      .fold(problem => throw new IllegalStateException(problem.reason), identity),
    recentEventCacheSize = 64
  )

final case class JournalRecovery(truncatedUncommittedBytes: Long) derives CanEqual

private[managed] enum JournalCommitBoundary derives CanEqual:
  case BeforeAppend
  case AfterForce
  case AfterPublication

private[managed] trait JournalCommitProbe[F[_]]:
  def checkpoint(boundary: JournalCommitBoundary): F[Unit]

private object JournalCommitProbe:
  def noop[F[_]: Async]: JournalCommitProbe[F] = new JournalCommitProbe[F]:
    def checkpoint(boundary: JournalCommitBoundary): F[Unit] = Async[F].unit

final class FileJournalControlStore[F[_]: Async] private (
    channel: FileChannel,
    state: Ref[F, ControlState],
    semaphore: Semaphore[F],
    limits: JournalLimits,
    val recovery: JournalRecovery,
    commitProbe: JournalCommitProbe[F]
) extends ControlStore[F]:
  def transact(command: ControlCommand): F[Either[ControlFailure, ControlCommit]] =
    semaphore.permit.use { _ =>
      state.get.flatMap { current =>
        ControlTransition(current, command) match
          case Left(failure)                                   => Left(failure).pure[F]
          case Right(commit) if commit.committedEvents.isEmpty => Right(commit).pure[F]
          case Right(commit)                                   =>
            val compacted = compact(commit.state)
            Async[F].uncancelable { poll =>
              poll(commitProbe.checkpoint(JournalCommitBoundary.BeforeAppend)) *>
                Async[F]
                  .blocking(append(current.revision, commit.state.revision, command)) *>
                commitProbe.checkpoint(JournalCommitBoundary.AfterForce) *>
                state.set(compacted) *>
                poll(commitProbe.checkpoint(JournalCommitBoundary.AfterPublication)) *>
                Right(commit.copy(state = compacted)).pure[F]
            }
      }
    }

  def snapshot: F[ControlState] = state.get

  def events(after: EventCursor, maximum: Int): F[EventPage] =
    semaphore.permit.use(_ => Async[F].blocking(scanEvents(after, math.max(0, maximum))))

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

  private def append(
      priorRevision: StoreRevision,
      revision: StoreRevision,
      command: ControlCommand
  ): Unit =
    val frame = JournalCodec
      .encode(priorRevision, revision, command, limits.maximumRecordBytes)
      .fold(problem => throw JournalException(problem), identity)
    val start = channel.size()
    try
      channel.position(start)
      writeFully(channel, ByteBuffer.wrap(frame.toArray))
      channel.force(true)
    catch
      case error: Throwable =>
        channel.truncate(start)
        channel.force(true)
        throw error

  private def scanEvents(after: EventCursor, maximum: Int): EventPage =
    if maximum == 0 then EventPage(Vector.empty, after, endOfJournal = false)
    else
      var projection = ControlState.empty
      var position = 0L
      val collected = Vector.newBuilder[CommittedEvent]
      var count = 0
      var omitted = false
      val size = channel.size()
      while position < size && count < maximum do
        JournalCodec.read(channel, position, size, limits.maximumRecordBytes) match
          case JournalRead.Complete(next, record) =>
            val commit = applyRecord(projection, record)
            projection = commit.state
            commit.committedEvents.foreach { event =>
              if event.cursor.value > after.value && count < maximum then
                collected += event
                count += 1
              else if event.cursor.value > after.value then omitted = true
            }
            projection = compact(projection)
            position = next
          case JournalRead.Truncated(_, _) =>
            throw JournalException(ControlFailure.JournalCorrupt("journal has an uncommitted tail"))
      val values = collected.result()
      val next = values.lastOption.map(_.cursor).getOrElse(after)
      EventPage(values, next, endOfJournal = position >= size && !omitted)

  private def applyRecord(state: ControlState, record: JournalRecord): ControlCommit =
    if record.priorRevision != state.revision then
      throw JournalException(
        ControlFailure.JournalCorrupt(
          s"revision gap: expected ${state.revision.value}, received ${record.priorRevision.value}"
        )
      )
    ControlTransition(state, record.command) match
      case Left(failure) => throw JournalException(ControlFailure.JournalCorrupt(failure.toString))
      case Right(commit) if commit.state.revision != record.revision =>
        throw JournalException(
          ControlFailure.JournalCorrupt(
            s"revision mismatch: recorded ${record.revision.value}, replayed ${commit.state.revision.value}"
          )
        )
      case Right(commit) => commit

  private def compact(value: ControlState): ControlState =
    value.copy(events = value.events.takeRight(limits.recentEventCacheSize))

  private def writeFully(channel: FileChannel, buffer: ByteBuffer): Unit =
    while buffer.hasRemaining do
      val _ = channel.write(buffer)

object FileJournalControlStore:
  def open[F[_]: Async](
      path: Path,
      limits: JournalLimits = JournalLimits.default
  ): Resource[F, FileJournalControlStore[F]] =
    openWithProbe(path, limits, JournalCommitProbe.noop[F])

  private[managed] def openWithProbe[F[_]: Async](
      path: Path,
      limits: JournalLimits,
      commitProbe: JournalCommitProbe[F]
  ): Resource[F, FileJournalControlStore[F]] =
    Resource
      .make(acquire(path, limits))(release)
      .flatMap { opened =>
        Resource.eval(
          for
            state <- Ref.of[F, ControlState](opened.state)
            semaphore <- Semaphore[F](1L)
          yield FileJournalControlStore(
            opened.channel,
            state,
            semaphore,
            limits,
            opened.recovery,
            commitProbe
          )
        )
      }

  private def acquire[F[_]: Async](path: Path, limits: JournalLimits): F[OpenedJournal] =
    Async[F].blocking {
      preparePrivateFile(path)
      val channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
      val lock = try channel.tryLock()
      catch case _: OverlappingFileLockException => null
      if lock == null then
        channel.close()
        throw JournalException(ControlFailure.JournalLocked(path.toString))
      try
        val replayed = replay(channel, limits)
        OpenedJournal(channel, lock, replayed._1, replayed._2)
      catch
        case error: Throwable =>
          lock.release()
          channel.close()
          throw error
    }

  private def release[F[_]: Async](opened: OpenedJournal): F[Unit] =
    Async[F].blocking {
      opened.lock.release()
      opened.channel.close()
    }

  private def replay(
      channel: FileChannel,
      limits: JournalLimits
  ): (ControlState, JournalRecovery) =
    var state = ControlState.empty
    var position = 0L
    val size = channel.size()
    var truncated = 0L
    while position < size do
      JournalCodec.read(channel, position, size, limits.maximumRecordBytes) match
        case JournalRead.Complete(next, record) =>
          state = applyRecord(state, record, limits)
          position = next
        case JournalRead.Truncated(start, bytes) =>
          channel.truncate(start)
          channel.force(true)
          truncated = bytes
          position = size
    (state, JournalRecovery(truncated))

  private def applyRecord(
      state: ControlState,
      record: JournalRecord,
      limits: JournalLimits
  ): ControlState =
    if record.priorRevision != state.revision then
      throw JournalException(
        ControlFailure.JournalCorrupt(
          s"revision gap at ${record.revision.value}: expected ${state.revision.value}"
        )
      )
    ControlTransition(state, record.command) match
      case Left(failure) => throw JournalException(ControlFailure.JournalCorrupt(failure.toString))
      case Right(commit) if commit.state.revision != record.revision =>
        throw JournalException(
          ControlFailure.JournalCorrupt(
            s"replay revision mismatch at ${record.revision.value}"
          )
        )
      case Right(commit) =>
        commit.state.copy(events = commit.state.events.takeRight(limits.recentEventCacheSize))

  private def preparePrivateFile(path: Path): Unit =
    val absolute = path.toAbsolutePath.normalize()
    val parent = Option(absolute.getParent).getOrElse(
      throw JournalException(ControlFailure.JournalCorrupt("journal path must have a parent"))
    )
    val parentExisted = Files.exists(parent, LinkOption.NOFOLLOW_LINKS)
    Files.createDirectories(parent)
    if Files.isSymbolicLink(parent) then
      throw JournalException(
        ControlFailure.JournalCorrupt("journal parent must not be a symbolic link")
      )
    if Files.exists(absolute, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(absolute) then
      throw JournalException(ControlFailure.JournalCorrupt("journal file must not be a symlink"))
    if Files.exists(absolute, LinkOption.NOFOLLOW_LINKS) &&
      !Files.isRegularFile(absolute, LinkOption.NOFOLLOW_LINKS)
    then
      throw JournalException(ControlFailure.JournalCorrupt("journal path must be a regular file"))
    if parentExisted then verifyPrivateDirectory(parent)
    if !Files.exists(absolute, LinkOption.NOFOLLOW_LINKS) then
      val _ = Try(
        Files.createFile(
          absolute,
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
        )
      ).recover { case _: UnsupportedOperationException => Files.createFile(absolute) }.get
    if !parentExisted then
      val _ = Try(
        Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwx------"))
      )
    val _ = Try(
      Files.setPosixFilePermissions(absolute, PosixFilePermissions.fromString("rw-------"))
    )

  private def verifyPrivateDirectory(path: Path): Unit =
    Try(Files.getPosixFilePermissions(path)).foreach { permissions =>
      val forbidden = Set(
        PosixFilePermission.GROUP_READ,
        PosixFilePermission.GROUP_WRITE,
        PosixFilePermission.GROUP_EXECUTE,
        PosixFilePermission.OTHERS_READ,
        PosixFilePermission.OTHERS_WRITE,
        PosixFilePermission.OTHERS_EXECUTE
      )
      if permissions.asScala.exists(forbidden.contains) then
        throw JournalException(
          ControlFailure.JournalCorrupt("journal parent permissions must exclude group and others")
        )
    }

final private case class OpenedJournal(
    channel: FileChannel,
    lock: FileLock,
    state: ControlState,
    recovery: JournalRecovery
)

final private case class JournalRecord(
    priorRevision: StoreRevision,
    revision: StoreRevision,
    command: ControlCommand
)

private enum JournalRead:
  case Complete(nextPosition: Long, record: JournalRecord)
  case Truncated(startPosition: Long, byteCount: Long)

final private case class JournalException(failure: ControlFailure)
    extends RuntimeException(failure.toString)

private object JournalCodec:
  private val schema = SchemaId
    .from("scala-slurm.control-command")
    .fold(problem => throw new IllegalStateException(problem.reason), identity)
  private val printer = Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  def encode(
      priorRevision: StoreRevision,
      revision: StoreRevision,
      command: ControlCommand,
      maximumRecordBytes: ByteLimit
  ): Either[ControlFailure, Vector[Byte]] =
    val commandJson = ControlCommandJson.encode(command)
    val payload = Json.obj(
      "priorRevision" -> Json.fromLong(priorRevision.value),
      "revision" -> Json.fromLong(revision.value),
      "command" -> commandJson,
      "commandSha256" -> Json.fromString(checksum(commandJson))
    )
    val bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, payload))
    FrameCodec
      .encode(bytes, FrameLimits(maximumRecordBytes))
      .left
      .map(failure => ControlFailure.JournalCorrupt(failure.toString))

  def read(
      channel: FileChannel,
      position: Long,
      fileSize: Long,
      maximumRecordBytes: ByteLimit
  ): JournalRead =
    val remaining = fileSize - position
    if remaining < FrameCodec.HeaderBytes then JournalRead.Truncated(position, remaining)
    else
      val header = ByteBuffer.allocate(FrameCodec.HeaderBytes)
      readFully(channel, header, position)
      header.flip()
      val length = header.getInt()
      if length <= 0 || length > maximumRecordBytes.value then
        throw JournalException(
          ControlFailure.JournalCorrupt(
            s"invalid journal record length $length at byte $position"
          )
        )
      val total = FrameCodec.HeaderBytes.toLong + length.toLong
      if remaining < total then JournalRead.Truncated(position, remaining)
      else
        val payload = ByteBuffer.allocate(length)
        readFully(channel, payload, position + FrameCodec.HeaderBytes)
        payload.flip()
        val bytes = new Array[Byte](length)
        payload.get(bytes)
        val record = decode(bytes.toVector).fold(
          problem => throw JournalException(ControlFailure.JournalCorrupt(problem)),
          identity
        )
        JournalRead.Complete(position + total, record)

  private def decode(bytes: Vector[Byte]): Either[String, JournalRecord] =
    for
      envelope <- VersionedJson.decode(bytes).left.map(_.toString)
      _ <- Either.cond(envelope.schema == schema, (), "wrong journal schema")
      cursor = envelope.payload.hcursor
      priorRaw <- cursor.get[Long]("priorRevision").left.map(_.message)
      prior <- StoreRevision.from(priorRaw).left.map(_.reason)
      revisionRaw <- cursor.get[Long]("revision").left.map(_.message)
      revision <- StoreRevision.from(revisionRaw).left.map(_.reason)
      commandJson <- cursor.downField("command").focus.toRight("missing journal command")
      expected <- cursor.get[String]("commandSha256").left.map(_.message)
      _ <- Either.cond(checksum(commandJson) == expected, (), "journal command checksum mismatch")
      command <- ControlCommandJson.decode(commandJson)
    yield JournalRecord(prior, revision, command)

  private def checksum(json: Json): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(printer.print(json).getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def readFully(channel: FileChannel, buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val count = channel.read(buffer, position)
      if count < 0 then throw JournalException(ControlFailure.JournalCorrupt("unexpected EOF"))
      position += count.toLong
