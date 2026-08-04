package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Semaphore
import cats.syntax.all.*
import io.circe.Json
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.byteVectorCanEqual
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.EventCursor
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.SchemaId
import io.github.bbuchsbaum.slurm4s.core.SubmissionKey
import io.github.bbuchsbaum.slurm4s.core.codec.CanonicalJson
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson
import io.github.bbuchsbaum.slurm4s.core.codec.WireEnvelope
import io.github.bbuchsbaum.slurm4s.protocol.FrameCodec
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits

import scodec.bits.ByteVector

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

/** Durable file-store limits.
  *
  * `maximumStorageBytes` bounds the snapshot and command suffix together. When the materialized
  * live projection plus one command cannot fit, the command is refused with
  * [[ControlFailure.JournalExhausted]] rather than weakening the bound.
  */
final case class JournalLimits(
    maximumRecordBytes: ByteLimit,
    recentEventCacheSize: Int,
    maximumStorageBytes: ByteLimit = ByteLimit.unsafeFrom(256 * 1024 * 1024)
) derives CanEqual:
  require(recentEventCacheSize > 0, "recent event cache size must be positive")

object JournalLimits:
  val default: JournalLimits = JournalLimits(
    ByteLimit.unsafeFrom(32 * 1024 * 1024),
    recentEventCacheSize = 64,
    maximumStorageBytes = ByteLimit.unsafeFrom(256 * 1024 * 1024)
  )

final case class JournalRecovery(
    truncatedUncommittedBytes: Long,
    replayedRecordCount: Long = 0L,
    snapshotRevision: Option[StoreRevision] = None
) derives CanEqual

private[managed] enum JournalPersistenceBoundary derives CanEqual:
  case BeforeAppend
  case AfterForce
  case AfterPublication
  case BeforeSnapshotPublication
  case AfterSnapshotPublication
  case AfterJournalReset

private[managed] trait JournalPersistenceProbe[F[_]]:
  def at(boundary: JournalPersistenceBoundary): F[Unit]

private object JournalPersistenceProbe:
  def noop[F[_]: Async]: JournalPersistenceProbe[F] = new JournalPersistenceProbe[F]:
    def at(boundary: JournalPersistenceBoundary): F[Unit] = Async[F].unit

final class FileJournalControlStore[F[_]: Async] private (
    snapshotPath: Path,
    channel: FileChannel,
    state: Ref[F, ControlState],
    replayAnchor: Ref[F, ReplayAnchor],
    eventDiskScans: Ref[F, Long],
    semaphore: Semaphore[F],
    limits: JournalLimits,
    val recovery: JournalRecovery,
    persistenceProbe: JournalPersistenceProbe[F]
) extends ControlStore[F]:
  def transact(command: ControlCommand): F[Either[ControlFailure, ControlCommit]] =
    semaphore.permit.use { _ =>
      state.get.flatMap { current =>
        ControlTransition(current, command) match
          case Left(failure)                                   => Left(failure).pure[F]
          case Right(commit) if commit.committedEvents.isEmpty => Right(commit).pure[F]
          case Right(commit)                                   =>
            // Encoding is pure and its failures -- an oversized record above all -- are routine,
            // deterministic, and caller-fixable, so they are returned rather than raised. Only the
            // durable write itself may fail as an effect.
            JournalCodec.encode(
              current.revision,
              commit.state.revision,
              command,
              limits.maximumRecordBytes
            ) match
              case Left(failure) => Left(failure).pure[F]
              case Right(frame)  =>
                Async[F].blocking(compactionPlan(current, frame)).flatMap {
                  case Left(failure) => Left(failure).pure[F]
                  case Right(plan)   =>
                    val compacted = trimEventCache(commit.state)
                    Async[F].uncancelable { poll =>
                      poll(persistenceProbe.at(JournalPersistenceBoundary.BeforeAppend)) *>
                        plan.traverse_(publishSnapshot) *>
                        Async[F].blocking(append(frame)) *>
                        persistenceProbe.at(JournalPersistenceBoundary.AfterForce) *>
                        state.set(compacted) *>
                        poll(persistenceProbe.at(JournalPersistenceBoundary.AfterPublication)) *>
                        Right(commit.copy(state = compacted)).pure[F]
                    }
                }
      }
    }

  def snapshot: F[ControlState] = state.get

  def attempt(submissionKey: SubmissionKey): F[Option[ManagedAttempt]] =
    state.get.map(_.attempts.get(submissionKey))

  def events(after: EventCursor, maximum: Int): F[EventPage] =
    val limit = math.max(0, maximum)
    replayAnchor.get.flatMap { anchor =>
      if after.value < anchor.minimumAvailableAfter.value then
        EventPage
          .HistoryUnavailable(EventHistoryGap.known(after, anchor.minimumAvailableAfter))
          .pure[F]
      else if limit == 0 then EventPage.Available(Vector.empty, after, endOfJournal = false).pure[F]
      else
        state.get.flatMap { current =>
          if canPageFromCache(current, after) then ControlStore.page(current, after, limit).pure[F]
          else scanOlderEvents(after, limit)
        }
    }

  private[managed] def eventDiskScanCount: F[Long] = eventDiskScans.get

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

  private def compactionPlan(
      current: ControlState,
      nextFrame: ByteVector
  ): Either[ControlFailure, Option[SnapshotPublication]] =
    val snapshotSize =
      if Files.exists(snapshotPath, LinkOption.NOFOLLOW_LINKS) then Files.size(snapshotPath)
      else 0L
    val projected = snapshotSize + channel.size() + nextFrame.size
    if projected <= limits.maximumStorageBytes.value.toLong then Right(None)
    else
      val retained = trimEventCache(current)
      val minimum = FileJournalControlStore.minimumAfterOf(retained)
      JournalSnapshotCodec
        .encode(
          ControlSnapshot(retained, minimum),
          limits.maximumStorageBytes.value.toLong - nextFrame.size
        )
        .map(bytes => Some(SnapshotPublication(bytes, retained, minimum)))

  private def publishSnapshot(publication: SnapshotPublication): F[Unit] =
    persistenceProbe.at(JournalPersistenceBoundary.BeforeSnapshotPublication) *>
      Async[F].blocking {
        AtomicFiles.replaceBlocking(snapshotPath, publication.bytes) match
          case Right(())     => ()
          case Left(problem) =>
            throw JournalException(
              ControlFailure.JournalIo(s"snapshot publication failed: $problem")
            )
      } *>
      persistenceProbe.at(JournalPersistenceBoundary.AfterSnapshotPublication) *>
      replayAnchor.set(
        ReplayAnchor(publication.state, publication.minimumAvailableAfter)
      ) *>
      Async[F].blocking {
        channel.truncate(0L)
        channel.position(0L)
        channel.force(true)
      } *>
      persistenceProbe.at(JournalPersistenceBoundary.AfterJournalReset)

  /** Append one already-encoded record and force it to stable storage.
    *
    * A failed write is rolled back to the pre-append offset so the journal never retains a partial
    * record. A rollback that itself fails is attached as a suppressed cause rather than replacing
    * the original diagnosis; the torn tail it leaves behind is recognized as uncommitted and
    * truncated on the next open.
    */
  private def append(frame: ByteVector): Unit =
    val start = channel.size()
    try
      channel.position(start)
      writeFully(channel, ByteBuffer.wrap(frame.toArray))
      channel.force(true)
    catch
      case error: Throwable =>
        try
          channel.truncate(start)
          channel.force(true)
        catch case rollback: Throwable => error.addSuppressed(rollback)
        throw error

  private def canPageFromCache(current: ControlState, after: EventCursor): Boolean =
    current.events.headOption.forall(first => after.value >= first.cursor.value - 1L)

  private def scanOlderEvents(after: EventCursor, maximum: Int): F[EventPage] =
    def scan: F[Option[EventPage]] =
      eventDiskScans.update(_ + 1L) *>
        replayAnchor.get.flatMap { anchor =>
          if after.value < anchor.minimumAvailableAfter.value then
            Some(
              EventPage.HistoryUnavailable(
                EventHistoryGap.known(after, anchor.minimumAvailableAfter)
              )
            ).pure[F]
          else
            Async[F].blocking(scanEvents(anchor.state, after, maximum)).flatMap { result =>
              replayAnchor.get.map { current =>
                if current.state.revision == anchor.state.revision then result else None
              }
            }
        }

    // Disk scans share the writer semaphore. The suffix is bounded, and serialization prevents a
    // compaction truncate from turning a valid concurrent read into an unexpected EOF.
    semaphore.permit.use(_ => scan).flatMap {
      case Some(page) => page.pure[F]
      case None       =>
        Async[F].raiseError(
          JournalException(
            ControlFailure.JournalCorrupt("journal has an uncommitted tail")
          )
        )
    }

  private def scanEvents(
      base: ControlState,
      after: EventCursor,
      maximum: Int
  ): Option[EventPage] =
    var projection = base
    var position = 0L
    val collected = Vector.newBuilder[CommittedEvent]
    var count = 0
    var omitted = false
    val size = channel.size()
    base.events.foreach { event =>
      if event.cursor.value > after.value && count < maximum then
        collected += event
        count += 1
      else if event.cursor.value > after.value then omitted = true
    }
    while position < size && count < maximum do
      JournalCodec.read(channel, position, size, limits.maximumRecordBytes) match
        case JournalRead.Complete(next, record) =>
          if record.revision.value > projection.revision.value then
            val commit = applyRecord(projection, record)
            projection = commit.state
            commit.committedEvents.foreach { event =>
              if event.cursor.value > after.value && count < maximum then
                collected += event
                count += 1
              else if event.cursor.value > after.value then omitted = true
            }
            projection = trimEventCache(projection)
          position = next
        case JournalRead.Truncated(_, _) => return None
    val values = collected.result()
    val next = values.lastOption.map(_.cursor).getOrElse(after)
    Some(EventPage.Available(values, next, endOfJournal = position >= size && !omitted))

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

  private def trimEventCache(value: ControlState): ControlState =
    value.copy(events = value.events.takeRight(limits.recentEventCacheSize))

  private def writeFully(channel: FileChannel, buffer: ByteBuffer): Unit =
    while buffer.hasRemaining do
      val _ = channel.write(buffer)

object FileJournalControlStore:
  def open[F[_]: Async](
      path: Path,
      limits: JournalLimits = JournalLimits.default
  ): Resource[F, FileJournalControlStore[F]] =
    openWithProbe(path, limits, JournalPersistenceProbe.noop[F])

  private[managed] def openWithProbe[F[_]: Async](
      path: Path,
      limits: JournalLimits,
      persistenceProbe: JournalPersistenceProbe[F]
  ): Resource[F, FileJournalControlStore[F]] =
    Resource
      .make(acquire(path, limits))(release)
      .flatMap { opened =>
        Resource.eval(
          for
            state <- Ref.of[F, ControlState](opened.state)
            replayAnchor <- Ref.of[F, ReplayAnchor](
              ReplayAnchor(opened.replayBase, opened.minimumAvailableAfter)
            )
            eventDiskScans <- Ref.of[F, Long](0L)
            semaphore <- Semaphore[F](1L)
          yield FileJournalControlStore(
            opened.snapshotPath,
            opened.channel,
            state,
            replayAnchor,
            eventDiskScans,
            semaphore,
            limits,
            opened.recovery,
            persistenceProbe
          )
        )
      }

  private def publishSnapshotBlocking(
      path: Path,
      snapshot: ControlSnapshot,
      maximumBytes: ByteLimit
  ): Unit =
    val bytes = JournalSnapshotCodec
      .encode(snapshot, maximumBytes.value.toLong)
      .fold(failure => throw JournalException(failure), identity)
    AtomicFiles.replaceBlocking(path, bytes) match
      case Right(())     => ()
      case Left(problem) =>
        throw JournalException(
          ControlFailure.JournalIo(s"snapshot publication failed: $problem")
        )

  private def minimumAfterOf(value: ControlState): EventCursor =
    value.events.headOption match
      case Some(first) if first.cursor.value > 0L =>
        EventCursor.from(first.cursor.value - 1L).toOption.getOrElse(EventCursor.origin)
      case _ => EventCursor.origin

  private def acquire[F[_]: Async](path: Path, limits: JournalLimits): F[OpenedJournal] =
    Async[F].blocking {
      preparePrivateFile(path)
      val snapshotPath = snapshotPathOf(path)
      verifySnapshotPath(snapshotPath)
      val channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
      val lock = try channel.tryLock()
      catch case _: OverlappingFileLockException => null
      if lock == null then
        channel.close()
        throw JournalException(ControlFailure.JournalLocked(path.toString))
      try
        val snapshot = JournalSnapshotCodec.read(snapshotPath, limits.maximumStorageBytes)
        val base = snapshot.map(_.state).getOrElse(ControlState.empty)
        val minimum = snapshot.map(_.minimumAvailableAfter).getOrElse(EventCursor.origin)
        val replayed = replay(channel, base, limits, snapshot.map(_.state.revision))
        val compactedLegacy =
          snapshot.isEmpty && channel.size() > limits.maximumStorageBytes.value.toLong
        val effectiveSnapshot =
          if compactedLegacy then
            val recovered = ControlSnapshot(replayed._1, minimumAfterOf(replayed._1))
            publishSnapshotBlocking(snapshotPath, recovered, limits.maximumStorageBytes)
            Some(recovered)
          else snapshot
        if (compactedLegacy ||
            snapshot.nonEmpty && replayed._2.replayedRecordCount == 0L) &&
          channel.size() > 0L
        then
          channel.truncate(0L)
          channel.position(0L)
          channel.force(true)
        val effectiveBase = effectiveSnapshot.map(_.state).getOrElse(base)
        val effectiveMinimum =
          effectiveSnapshot.map(_.minimumAvailableAfter).getOrElse(minimum)
        val effectiveRecovery =
          replayed._2.copy(snapshotRevision = effectiveSnapshot.map(_.state.revision))
        OpenedJournal(
          snapshotPath,
          channel,
          lock,
          replayed._1,
          effectiveBase,
          effectiveMinimum,
          effectiveRecovery
        )
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
      base: ControlState,
      limits: JournalLimits,
      snapshotRevision: Option[StoreRevision]
  ): (ControlState, JournalRecovery) =
    var state = base
    var position = 0L
    val size = channel.size()
    var truncated = 0L
    var replayed = 0L
    while position < size do
      JournalCodec.read(channel, position, size, limits.maximumRecordBytes) match
        case JournalRead.Complete(next, record) =>
          if record.revision.value > state.revision.value then
            state = applyRecord(state, record, limits)
            replayed += 1L
          position = next
        case JournalRead.Truncated(start, bytes) =>
          channel.truncate(start)
          channel.force(true)
          truncated = bytes
          position = size
    (state, JournalRecovery(truncated, replayed, snapshotRevision))

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

  private def snapshotPathOf(path: Path): Path =
    val absolute = path.toAbsolutePath.normalize()
    absolute.resolveSibling(s"${absolute.getFileName}.snapshot")

  private def verifySnapshotPath(path: Path): Unit =
    if Files.exists(path, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(path) then
      throw JournalException(ControlFailure.JournalCorrupt("snapshot file must not be a symlink"))
    if Files.exists(path, LinkOption.NOFOLLOW_LINKS) &&
      !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
    then
      throw JournalException(
        ControlFailure.JournalCorrupt("snapshot path must be a regular file")
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
    snapshotPath: Path,
    channel: FileChannel,
    lock: FileLock,
    state: ControlState,
    replayBase: ControlState,
    minimumAvailableAfter: EventCursor,
    recovery: JournalRecovery
)

final private case class SnapshotPublication(
    bytes: ByteVector,
    state: ControlState,
    minimumAvailableAfter: EventCursor
)

final private case class ReplayAnchor(
    state: ControlState,
    minimumAvailableAfter: EventCursor
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

private object JournalSnapshotCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.control-snapshot")

  def encode(
      snapshot: ControlSnapshot,
      maximumBytes: Long
  ): Either[ControlFailure, ByteVector] =
    val state = ControlSnapshotJson.encode(snapshot)
    val payload = Json.obj(
      "state" -> state,
      "stateSha256" -> Json.fromString(checksum(state))
    )
    val bytes = VersionedJson.encode(WireEnvelope(ProtocolVersion.v1, schema, payload))
    Either.cond(
      maximumBytes > 0L && bytes.size <= maximumBytes,
      bytes,
      ControlFailure.JournalExhausted(
        s"compacted state requires ${bytes.size} bytes but only $maximumBytes remain"
      )
    )

  def read(path: Path, maximumBytes: ByteLimit): Option[ControlSnapshot] =
    if !Files.exists(path, LinkOption.NOFOLLOW_LINKS) then None
    else
      val size = Files.size(path)
      if size <= 0L || size > maximumBytes.value.toLong then
        throw JournalException(
          ControlFailure.JournalCorrupt(s"invalid snapshot size $size")
        )
      val bytes = ByteVector.view(Files.readAllBytes(path))
      val decoded = for
        envelope <- VersionedJson.decode(bytes).left.map(_.toString)
        _ <- Either.cond(
          VersionedJson.encode(envelope) == bytes,
          (),
          "snapshot bytes are not canonical"
        )
        _ <- Either.cond(envelope.schema == schema, (), "wrong snapshot schema")
        cursor = envelope.payload.hcursor
        stateJson <- cursor.downField("state").focus.toRight("missing snapshot state")
        expected <- cursor.get[String]("stateSha256").left.map(_.message)
        _ <- Either.cond(checksum(stateJson) == expected, (), "snapshot checksum mismatch")
        snapshot <- ControlSnapshotJson.decode(stateJson)
      yield snapshot
      Some(
        decoded.fold(
          problem => throw JournalException(ControlFailure.JournalCorrupt(problem)),
          identity
        )
      )

  private def checksum(json: Json): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(CanonicalJson.bytes(json).toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

private object JournalCodec:
  private val schema = SchemaId.unsafeFrom("slurm4s.control-command")

  def encode(
      priorRevision: StoreRevision,
      revision: StoreRevision,
      command: ControlCommand,
      maximumRecordBytes: ByteLimit
  ): Either[ControlFailure, ByteVector] =
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
        val record = decode(ByteVector.view(bytes)).fold(
          problem => throw JournalException(ControlFailure.JournalCorrupt(problem)),
          identity
        )
        JournalRead.Complete(position + total, record)

  private def decode(bytes: ByteVector): Either[String, JournalRecord] =
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
      .digest(CanonicalJson.print(json).getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def readFully(channel: FileChannel, buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val count = channel.read(buffer, position)
      if count < 0 then throw JournalException(ControlFailure.JournalCorrupt("unexpected EOF"))
      position += count.toLong
