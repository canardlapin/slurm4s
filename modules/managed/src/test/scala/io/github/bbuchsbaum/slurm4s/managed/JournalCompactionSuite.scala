package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.core.codec.CanonicalJson
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson
import io.github.bbuchsbaum.slurm4s.core.codec.WireEnvelope

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scodec.bits.ByteVector

class JournalCompactionSuite extends munit.CatsEffectSuite:
  import ManagedTestSupport.*

  test("compaction bounds storage, replays only its suffix, and signals retired history") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("bounded.journal")
      val legacyPath = directory.resolve("legacy.journal")
      val maximum = ByteLimit.from(48 * 1024).toOption.get
      val limits = JournalLimits(
        JournalLimits.default.maximumRecordBytes,
        recentEventCacheSize = 4,
        maximumStorageBytes = maximum
      )
      val value = intent("bounded-compaction")
      for
        legacyLive <- FileJournalControlStore
          .open[IO](
            legacyPath,
            limits.copy(maximumStorageBytes = JournalLimits.default.maximumStorageBytes)
          )
          .use { store =>
            bind(store, value) *>
              (1 to 80).toVector.traverse_(index => store.transact(observation(index))) *>
              store.snapshot
          }
        legacyReplayCount <- FileJournalControlStore
          .open[IO](
            legacyPath,
            limits.copy(maximumStorageBytes = JournalLimits.default.maximumStorageBytes)
          )
          .use(store => IO.pure(store.recovery.replayedRecordCount))
        promoted <- FileJournalControlStore.open[IO](legacyPath, limits).use { store =>
          store.snapshot
        }
        _ = assertEquals(promoted, legacyLive)
        legacySnapshotPath =
          legacyPath.resolveSibling(s"${legacyPath.getFileName}.snapshot")
        promotedStorage <- IO.blocking(Files.size(legacyPath) + Files.size(legacySnapshotPath))
        _ = assert(promotedStorage <= maximum.value.toLong)
        legacySuffixReplay <- FileJournalControlStore
          .open[IO](legacyPath, limits)
          .use(store => IO.pure(store.recovery.replayedRecordCount))
        _ = assertEquals(legacySuffixReplay, 0L)
        live <- FileJournalControlStore.open[IO](path, limits).use { store =>
          for
            _ <- bind(store, value)
            outcomes <- (1 to 80).toVector.traverse(index => store.transact(observation(index)))
            _ = assert(outcomes.forall(_.isRight))
            snapshot <- store.snapshot
          yield snapshot
        }
        snapshotPath = path.resolveSibling(s"${path.getFileName}.snapshot")
        _ <- IO.blocking(
          assert(Files.exists(snapshotPath), "compaction did not publish a snapshot")
        )
        storage <- IO.blocking(Files.size(path) + Files.size(snapshotPath))
        _ = assert(
          storage <= maximum.value.toLong,
          s"compacted storage $storage exceeded configured maximum ${maximum.value}"
        )
        _ <- FileJournalControlStore.open[IO](path, limits).use { reopened =>
          for
            replayed <- reopened.snapshot
            _ = assertEquals(replayed, live)
            _ = assert(
              reopened.recovery.replayedRecordCount * 2L < legacyReplayCount,
              s"compacted open replayed ${reopened.recovery.replayedRecordCount} records " +
                s"versus $legacyReplayCount before compaction"
            )
            gap <- reopened.events(EventCursor.origin, 10)
            minimum = gap match
              case EventPage.HistoryUnavailable(value) => value.minimumAvailableAfter
              case other => fail(s"retired event history was not signalled: $other")
            available <- reopened.events(minimum, 10)
            _ = available match
              case EventPage.Available(events, _, _) => assert(events.nonEmpty)
              case other => fail(s"minimum retained cursor remained unavailable: $other")
          yield ()
        }
      yield ()
    }
  }

  test("every compaction publication boundary reopens to the last committed state") {
    Vector(
      JournalPersistenceBoundary.BeforeSnapshotPublication,
      JournalPersistenceBoundary.AfterSnapshotPublication,
      JournalPersistenceBoundary.AfterJournalReset
    ).traverse_(exerciseInterruptedCompaction)
  }

  test("an older event scan cannot race a compaction truncate") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("scan-race.journal")
      val value = intent("scan-race")
      val seedLimits = JournalLimits(
        JournalLimits.default.maximumRecordBytes,
        recentEventCacheSize = 1
      )
      for
        _ <- FileJournalControlStore.open[IO](path, seedLimits).use { store =>
          bind(store, value) *>
            (1 to 6).toVector.traverse_(index => store.transact(observation(index)))
        }
        existingSize <- IO.blocking(Files.size(path))
        maximum = ByteLimit.from(Math.toIntExact(existingSize + 1L)).toOption.get
        limits = seedLimits.copy(maximumStorageBytes = maximum)
        reached <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        probe = new JournalPersistenceProbe[IO]:
          def at(boundary: JournalPersistenceBoundary): IO[Unit] =
            if boundary == JournalPersistenceBoundary.AfterSnapshotPublication then
              reached.complete(()).void *> release.get
            else IO.unit
        _ <- FileJournalControlStore.openWithProbe[IO](path, limits, probe).use { store =>
          for
            writer <- store.transact(observation(99)).start
            _ <- reached.get
            reader <- store.events(EventCursor.origin, 10).start
            early <- IO.race(IO.sleep(100.millis), reader.join)
            _ = assert(early.isLeft, "disk scan bypassed the compaction writer lock")
            _ <- release.complete(())
            written <- writer.joinWithNever
            _ = assert(written.isRight)
            page <- reader.joinWithNever
            _ = page match
              case _: EventPage.HistoryUnavailable => ()
              case other => fail(s"expected an explicit post-compaction gap, got $other")
          yield ()
        }
      yield ()
    }
  }

  test("the explicit snapshot codec preserves the materialized projection") {
    val value = intent("snapshot-codec")
    assert(
      EventHistoryGap.from(EventCursor.origin, EventCursor.origin).isLeft,
      "a non-advancing event-history gap was accepted"
    )
    InMemoryControlStore.create[IO]().flatMap { store =>
      for
        _ <- bind(store, value)
        _ <- (1 to 8).toVector.traverse_(index => store.transact(observation(index)))
        state <- store.snapshot
        retained = state.copy(events = state.events.takeRight(4))
        minimum = EventCursor.from(retained.events.head.cursor.value - 1L).toOption.get
        snapshot = ControlSnapshot(retained, minimum)
        _ = assertEquals(
          ControlSnapshotJson.decode(ControlSnapshotJson.encode(snapshot)),
          Right(snapshot)
        )
        invalidInitial = ControlSnapshotJson
          .encode(ControlSnapshot(ControlState.empty, EventCursor.origin))
          .mapObject(_.add("minimumAvailableAfter", Json.fromLong(1L)))
        _ = assert(ControlSnapshotJson.decode(invalidInitial).isLeft)
        invalidEventRevision = rewriteEventRevision(
          ControlSnapshotJson.encode(snapshot),
          index = 0,
          revision = 0L
        )
        _ = assert(ControlSnapshotJson.decode(invalidEventRevision).isLeft)
        skippedRevision = rewriteEventRevision(
          ControlSnapshotJson.encode(snapshot),
          index = 1,
          revision = retained.events.head.revision.value + 2L
        )
        _ = assert(ControlSnapshotJson.decode(skippedRevision).isLeft)
        staleFinalRevision = rewriteEventRevision(
          ControlSnapshotJson.encode(snapshot),
          index = retained.events.size - 1,
          revision = retained.revision.value - 1L
        )
        _ = assert(ControlSnapshotJson.decode(staleFinalRevision).isLeft)
      yield ()
    }
  }

  test("a corrupted published snapshot is rejected rather than ignored") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("corrupt-snapshot.journal")
      val limits = JournalLimits(
        JournalLimits.default.maximumRecordBytes,
        recentEventCacheSize = 4,
        maximumStorageBytes = ByteLimit.from(48 * 1024).toOption.get
      )
      val value = intent("corrupt-snapshot")
      for
        _ <- FileJournalControlStore.open[IO](path, limits).use { store =>
          bind(store, value) *>
            (1 to 80).toVector.traverse_(index => store.transact(observation(index)))
        }
        snapshotPath = path.resolveSibling(s"${path.getFileName}.snapshot")
        bytes <- IO.blocking(Files.readAllBytes(snapshotPath))
        _ <- IO.blocking {
          val index = bytes.length / 2
          bytes(index) = (bytes(index) ^ 1).toByte
          Files.write(snapshotPath, bytes)
        }
        reopened <- FileJournalControlStore.open[IO](path, limits).use(_ => IO.unit).attempt
        _ = assert(reopened.isLeft, "a corrupted snapshot was silently accepted")
      yield ()
    }
  }

  test("canonical additive snapshot fields and a newer minor version remain readable") {
    temporaryDirectory.use { directory =>
      val path = directory.resolve("additive-snapshot.journal")
      val limits = JournalLimits(
        JournalLimits.default.maximumRecordBytes,
        recentEventCacheSize = 4,
        maximumStorageBytes = ByteLimit.from(48 * 1024).toOption.get
      )
      val value = intent("additive-snapshot")
      for
        expected <- FileJournalControlStore.open[IO](path, limits).use { store =>
          bind(store, value) *>
            (1 to 80).toVector.traverse_(index => store.transact(observation(index))) *>
            store.snapshot
        }
        snapshotPath = path.resolveSibling(s"${path.getFileName}.snapshot")
        original <- IO.blocking(ByteVector.view(Files.readAllBytes(snapshotPath)))
        envelope <- IO.fromEither(
          VersionedJson
            .decode(original)
            .left
            .map(problem => new IllegalArgumentException(problem.toString))
        )
        state = envelope.payload.hcursor.downField("state").focus.get
        extendedState = state.mapObject(
          _.add("futureStateField", Json.fromString("preserve-compatible"))
        )
        extendedPayload = envelope.payload.mapObject(
          _.add("state", extendedState)
            .add("stateSha256", Json.fromString(checksum(extendedState)))
            .add("futurePayloadField", Json.fromBoolean(true))
        )
        protocol = ProtocolVersion.from(1, 1).toOption.get
        extended <- IO.fromEither(
          WireEnvelope
            .withExtensions(
              protocol,
              envelope.schema,
              extendedPayload,
              JsonObject("futureEnvelopeField" -> Json.fromInt(1))
            )
            .left
            .map(problem => new IllegalArgumentException(problem.toString))
        )
        _ <- IO.blocking(Files.write(snapshotPath, VersionedJson.encode(extended).toArray))
        reopened <- FileJournalControlStore.open[IO](path, limits).use(_.snapshot)
        _ = assertEquals(reopened, expected)
      yield ()
    }
  }

  private def exerciseInterruptedCompaction(boundary: JournalPersistenceBoundary): IO[Unit] =
    temporaryDirectory.use { directory =>
      val path = directory.resolve(s"interrupted-$boundary.journal")
      val value = intent(s"interrupted-$boundary")
      val seedLimits = JournalLimits(
        JournalLimits.default.maximumRecordBytes,
        recentEventCacheSize = 4
      )
      for
        expected <- FileJournalControlStore.open[IO](path, seedLimits).use { store =>
          bind(store, value) *>
            (1 to 6).toVector.traverse_(index => store.transact(observation(index))) *>
            store.snapshot
        }
        existingSize <- IO.blocking(Files.size(path))
        maximum = ByteLimit.from(Math.toIntExact(existingSize + 1L)).toOption.get
        limits = JournalLimits(
          JournalLimits.default.maximumRecordBytes,
          recentEventCacheSize = 4,
          maximumStorageBytes = maximum
        )
        probe = new JournalPersistenceProbe[IO]:
          def at(observed: JournalPersistenceBoundary): IO[Unit] =
            if observed == boundary then IO.raiseError(InjectedCrash(boundary))
            else IO.unit
        interrupted <- FileJournalControlStore
          .openWithProbe[IO](path, limits, probe)
          .use(_.transact(observation(99)))
          .attempt
        _ = assert(interrupted.isLeft, s"$boundary did not interrupt compaction")
        _ <- FileJournalControlStore.open[IO](path, limits).use { reopened =>
          for
            replayed <- reopened.snapshot
            _ = assertEquals(replayed, expected, s"$boundary changed committed state")
            next <- reopened.transact(observation(100))
            _ = assert(next.isRight, s"$boundary left the store unwritable: $next")
          yield ()
        }
      yield ()
    }

  private def bind(
      store: ControlStore[IO],
      value: ManagedIntent
  ): IO[Unit] =
    Vector(
      ControlCommand.RecordIntent(value),
      ControlCommand.ClaimSubmission(value.submissionKey, later),
      ControlCommand.RecordSubmission(value.submissionKey, value.epoch, accepted, later)
    ).traverse_(command =>
      store
        .transact(command)
        .flatMap(result => IO(assert(result.isRight, s"setup command failed: $result")))
    )

  private def observation(index: Int): ControlCommand =
    val at = later.plusSeconds(index.toLong)
    ControlCommand.RecordObservations(
      NonEmptyVector.one(job),
      SchedulerQueryResult.Succeeded(
        ObservationBatch(
          NonEmptyVector.one(
            ObservationResult.Observed(
              JobObservation(
                job,
                SlurmState.Running,
                Freshness.Current(at),
                None,
                Map("sequence" -> index.toString),
                evidence
              )
            )
          )
        )
      ),
      at
    )

  private def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("slurm4s-compaction-")))(directory =>
      IO.blocking {
        Files
          .walk(directory)
          .iterator()
          .asScala
          .toVector
          .sortBy(_.getNameCount)
          .reverse
          .foreach(path => Files.deleteIfExists(path))
      }.void
    )

  private def checksum(json: Json): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(CanonicalJson.bytes(json).toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def rewriteEventRevision(json: Json, index: Int, revision: Long): Json =
    json.mapObject { root =>
      val updated = root("events").flatMap(_.asArray) match
        case Some(events) if events.isDefinedAt(index) =>
          Json.fromValues(
            events.updated(
              index,
              events(index).mapObject(_.add("revision", Json.fromLong(revision)))
            )
          )
        case _ => Json.arr()
      root.add("events", updated)
    }

  final private case class InjectedCrash(boundary: JournalPersistenceBoundary)
      extends RuntimeException(boundary.toString)
