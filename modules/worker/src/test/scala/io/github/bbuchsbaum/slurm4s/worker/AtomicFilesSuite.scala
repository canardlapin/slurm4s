package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.IO
import cats.syntax.all.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

class AtomicFilesSuite extends munit.CatsEffectSuite:
  private val temporaryRoot = FunFixture[Path](
    setup = _ => Files.createTempDirectory("atomic-files-suite"),
    teardown = root =>
      val _ = Files
        .walk(root)
        .sorted(Comparator.reverseOrder())
        .forEach { path =>
          val _ = Files.deleteIfExists(path)
        }
  )

  private def bytes(text: String): Vector[Byte] =
    text.getBytes(StandardCharsets.UTF_8).toVector

  /** writeNew deliberately leaves parent-directory creation to callers; tests provide it. */
  private def writeInto(target: Path, content: Vector[Byte]): IO[Unit] =
    IO.blocking(Option(target.getParent).foreach(parent => Files.createDirectories(parent))) *>
      AtomicFiles
        .writeNew[IO](target, content)
        .map(outcome => assertEquals(outcome, Right(())))

  temporaryRoot.test("writeNew writes fresh targets and rejects existing ones") { root =>
    val target = root.resolve("value.bin")
    for
      first <- AtomicFiles.writeNew[IO](target, bytes("payload"))
      second <- AtomicFiles.writeNew[IO](target, bytes("other"))
    yield
      assertEquals(first, Right(()))
      assertEquals(Files.readAllBytes(target).toVector, bytes("payload"))
      assertEquals(
        second,
        Left(AtomicFiles.WriteFailure.TargetExists(target.toString))
      )
  }

  temporaryRoot.test("writeStable is idempotent for identical bytes, conflicts on drift") { root =>
    val target = root.resolve("artifact.sh")
    for
      first <- AtomicFiles.writeStable[IO](target, bytes("#!/bin/sh\n"), executable = true)
      again <- AtomicFiles.writeStable[IO](target, bytes("#!/bin/sh\n"), executable = true)
      drift <- AtomicFiles.writeStable[IO](target, bytes("#!/bin/bash\n"), executable = true)
    yield
      assertEquals(first, Right(()))
      assertEquals(again, Right(()))
      assert(drift.left.exists {
        case AtomicFiles.WriteFailure.TargetConflict(_, _) => true
        case _                                             => false
      })
  }

  temporaryRoot.test("replace atomically swaps content and creates absent targets") { root =>
    val target = root.resolve("heartbeat.json")
    for
      created <- AtomicFiles.replace[IO](target, bytes("beat-1"))
      replaced <- AtomicFiles.replace[IO](target, bytes("beat-2"))
    yield
      assertEquals(created, Right(()))
      assertEquals(replaced, Right(()))
      assertEquals(Files.readAllBytes(target).toVector, bytes("beat-2"))
  }

  temporaryRoot.test("publishOnce publishes exactly once and reports the sha256 digest") { root =>
    val target = root.resolve("results").resolve("result.json")
    for
      first <- AtomicFiles.publishOnce[IO](target, bytes("envelope"))
      second <- AtomicFiles.publishOnce[IO](target, bytes("envelope"))
    yield
      assertEquals(first.map(_.value.startsWith("sha256:")), Right(true))
      assertEquals(first, Right(AtomicFiles.digestOf(bytes("envelope"))))
      assert(second.left.exists {
        case AtomicFiles.WriteFailure.TargetExists(_) => true
        case _                                        => false
      })
  }

  temporaryRoot.test("claim moves the source into the claim directory") { root =>
    val source = root.resolve("pending").resolve("work.inv")
    val destination = root.resolve("claimed").resolve("pilot-1").resolve("work.inv")
    for
      _ <- writeInto(source, bytes("invocation"))
      outcome <- AtomicFiles.claim[IO](source, destination)
    yield
      assertEquals(outcome, Right(destination))
      assert(!Files.exists(source))
      assertEquals(Files.readAllBytes(destination).toVector, bytes("invocation"))
  }

  temporaryRoot.test("exactly one of many concurrent contenders wins a claim") { root =>
    val source = root.resolve("pending").resolve("contended.inv")
    val contenders = (1 to 16).toVector
    for
      _ <- writeInto(source, bytes("contended"))
      outcomes <- contenders.parTraverse { index =>
        AtomicFiles.claim[IO](
          source,
          root.resolve("claimed").resolve(s"pilot-$index").resolve("contended.inv")
        )
      }
    yield
      val winners = outcomes.collect { case Right(path) => path }
      val losers = outcomes.collect { case Left(AtomicFiles.ClaimFailure.SourceMissing(_)) => () }
      assertEquals(winners.size, 1)
      assertEquals(losers.size, contenders.size - 1)
      assertEquals(Files.readAllBytes(winners.head).toVector, bytes("contended"))
  }

  temporaryRoot.test("claiming a missing source is SourceMissing") { root =>
    AtomicFiles
      .claim[IO](root.resolve("pending").resolve("absent.inv"), root.resolve("claimed/absent.inv"))
      .map(outcome =>
        assert(outcome.left.exists {
          case AtomicFiles.ClaimFailure.SourceMissing(_) => true
          case _                                         => false
        })
      )
  }

  temporaryRoot.test("claiming onto an existing destination is AlreadyClaimed (restart-safe)") {
    root =>
      val source = root.resolve("pending").resolve("again.inv")
      val destination = root.resolve("claimed").resolve("pilot-1").resolve("again.inv")
      for
        _ <- writeInto(destination, bytes("previously claimed"))
        _ <- writeInto(source, bytes("republished"))
        outcome <- AtomicFiles.claim[IO](source, destination)
      yield
        assertEquals(
          outcome,
          Left(AtomicFiles.ClaimFailure.AlreadyClaimed(destination.toString))
        )
        assert(Files.exists(source), "a refused claim must leave the source in place")
  }

  temporaryRoot.test("a symlink source is refused as SourceNotRegular") { root =>
    val real = root.resolve("real.inv")
    val link = root.resolve("pending").resolve("link.inv")
    for
      _ <- writeInto(real, bytes("real"))
      _ <- IO.blocking {
        Files.createDirectories(link.getParent)
        Files.createSymbolicLink(link, real)
      }
      outcome <- AtomicFiles.claim[IO](link, root.resolve("claimed").resolve("link.inv"))
    yield assertEquals(
      outcome,
      Left(AtomicFiles.ClaimFailure.SourceNotRegular(link.toString))
    )
  }

  temporaryRoot.test("no temporary files survive a completed write") { root =>
    val target = root.resolve("clean.bin")
    for _ <- AtomicFiles.writeNew[IO](target, bytes("payload"))
    yield
      val leftovers = Files
        .list(root)
        .filter(path => path.getFileName.toString.contains(".tmp-"))
        .count()
      assertEquals(leftovers, 0L)
  }
