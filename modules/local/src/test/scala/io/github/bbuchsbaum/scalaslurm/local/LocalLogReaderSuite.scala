package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import scala.concurrent.duration.*

class LocalLogReaderSuite extends munit.CatsEffectSuite:
  test("missing pending log is WaitingForFile, then pages advance byte-exact cursors") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val reader = LocalLogReader[IO](root)
      val path = root.resolve("stdout.log")
      val ref = logRef(path)
      val pageSize = ByteLimit.from(3).toOption.get

      for
        missing <- reader.read(ref, LogCursor.start, pageSize)
        _ = assert(missing.isInstanceOf[LogReadResult.WaitingForFile])
        _ <- IO.blocking(Files.write(path, "abcdef".getBytes(StandardCharsets.UTF_8)))
        first <- reader.read(ref, LogCursor.start, pageSize)
        firstPage = first.asInstanceOf[LogReadResult.Page].value
        second <- reader.read(ref, firstPage.next, pageSize)
        secondPage = second.asInstanceOf[LogReadResult.Page].value
        _ = assertEquals(String(firstPage.bytes.toArray, StandardCharsets.UTF_8), "abc")
        _ = assertEquals(firstPage.next.offset.value, 3L)
        _ = assertEquals(String(secondPage.bytes.toArray, StandardCharsets.UTF_8), "def")
        _ = assert(secondPage.endOfFile)
      yield ()
    }
  }

  test("truncation invalidates an old cursor instead of silently replaying bytes") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val reader = LocalLogReader[IO](root)
      val path = root.resolve("stdout.log")
      val ref = logRef(path)
      val pageSize = ByteLimit.from(8).toOption.get

      for
        _ <- IO.blocking(Files.write(path, "abcdefgh".getBytes(StandardCharsets.UTF_8)))
        first <- reader.read(ref, LogCursor.start, pageSize)
        cursor = first.asInstanceOf[LogReadResult.Page].value.next
        _ <- IO.blocking(
          Files.write(
            path,
            "x".getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.TRUNCATE_EXISTING
          )
        )
        result <- reader.read(ref, cursor, pageSize)
        _ = assert(result.isInstanceOf[LogReadResult.CursorInvalid])
      yield ()
    }
  }

  test("follow is cancellable while waiting and releases promptly") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val reader = LocalLogReader[IO](root)
      val ref = logRef(root.resolve("pending.log"))
      for
        fiber <- reader
          .follow(
            ref,
            LogCursor.start,
            ByteLimit.from(16).toOption.get,
            DurationMillis.from(20).toOption.get
          )
          .compile
          .drain
          .start
        _ <- IO.sleep(60.millis)
        _ <- fiber.cancel
        outcome <- fiber.join
        _ = assert(outcome.isInstanceOf[Outcome.Canceled[IO, Throwable, Unit]])
      yield ()
    }
  }

  test("an intermediate symlink cannot escape the configured log root") {
    (LocalTestSupport.temporaryDirectory, LocalTestSupport.temporaryDirectory).tupled.use {
      case (root, outside) =>
        val outsideLog = outside.resolve("private.log")
        val link = root.resolve("escape")
        val ref = logRef(link.resolve("private.log"))
        for
          _ <- IO.blocking(Files.write(outsideLog, "secret".getBytes(StandardCharsets.UTF_8)))
          _ <- IO.blocking(Files.createSymbolicLink(link, outside))
          result <- LocalLogReader[IO](root).read(
            ref,
            LogCursor.start,
            ByteLimit.from(16).toOption.get
          )
          _ = assert(result.isInstanceOf[LogReadResult.Failed])
        yield ()
    }
  }

  private def logRef(path: java.nio.file.Path): LogRef =
    LogRef(
      AttemptId.from("log-test").toOption.get,
      AttemptEpoch.initial,
      LogStream.Stdout,
      path.toString
    )
