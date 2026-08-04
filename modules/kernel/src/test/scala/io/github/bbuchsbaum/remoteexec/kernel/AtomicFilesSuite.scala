package io.github.bbuchsbaum.remoteexec.kernel

import cats.effect.IO
import cats.syntax.all.*
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

class AtomicFilesSuite extends munit.CatsEffectSuite:
  private val temporaryRoot = FunFixture[Path](
    setup = _ => Files.createTempDirectory("remote-exec-kernel-"),
    teardown = root =>
      val paths = Files.walk(root)
      try
        val _ = paths.sorted(Comparator.reverseOrder()).forEach { path =>
          val _ = Files.deleteIfExists(path)
        }
      finally paths.close()
  )

  private def bytes(text: String): ByteVector =
    ByteVector.view(text.getBytes(StandardCharsets.UTF_8))

  private def published(path: Path): ByteVector =
    ByteVector.view(Files.readAllBytes(path))

  temporaryRoot.test("concurrent identical stable writes are logically idempotent") { root =>
    val target = root.resolve("script.sh")
    val content = bytes("#!/bin/sh\nprintf result\n")
    (1 to 32).toVector
      .parTraverse(_ =>
        IO.blocking(AtomicFiles.writeStableBlocking(target, content, executable = true))
      )
      .map { outcomes =>
        assert(outcomes.forall(_ == Right(())), outcomes.mkString(", "))
        assertEquals(published(target), content)
      }
  }

  temporaryRoot.test("concurrent different stable writes preserve exactly one value") { root =>
    val target = root.resolve("artifact.bin")
    val first = ByteVector.fill(128L * 1024L)('a'.toByte)
    val second = ByteVector.fill(128L * 1024L)('b'.toByte)
    Vector(first, second)
      .parTraverse(value => IO.blocking(AtomicFiles.writeStableBlocking(target, value)))
      .map { outcomes =>
        assertEquals(outcomes.count(_ == Right(())), 1)
        assertEquals(
          outcomes.count(_.left.exists {
            case AtomicFiles.WriteFailure.TargetConflict(_, _) => true
            case _                                             => false
          }),
          1
        )
        val observed = published(target)
        assert(observed == first || observed == second)
      }
  }

  temporaryRoot.test("atomic replacement readers observe only complete old or new values") { root =>
    val target = root.resolve("latest.bin")
    val oldValue = ByteVector.fill(256L * 1024L)('o'.toByte)
    val newValue = ByteVector.fill(256L * 1024L)('n'.toByte)
    for
      initialized <- IO.blocking(AtomicFiles.writeStableBlocking(target, oldValue))
      _ = assertEquals(initialized, Right(()))
      observed <- (
        (0 until 64).toVector.traverse_ { index =>
          val value = if index % 2 == 0 then newValue else oldValue
          IO.blocking(AtomicFiles.replaceBlocking(target, value))
            .map(result => assertEquals(result, Right(())))
        },
        (0 until 256).toVector.traverse(_ => IO.blocking(published(target)))
      ).parTupled.map(_._2)
    yield assert(
      observed.forall(value => value == oldValue || value == newValue),
      "a reader observed bytes other than one complete published value"
    )
  }

  temporaryRoot.test("stable staging refuses a symlink target") { root =>
    val real = root.resolve("real.sh")
    val link = root.resolve("script.sh")
    for
      _ <- IO.blocking {
        val _ = Files.write(real, bytes("real").toArray)
        val _ = Files.createSymbolicLink(link, real)
      }
      outcome <- IO.blocking(
        AtomicFiles.writeStableBlocking(link, bytes("replacement"), executable = true)
      )
    yield assert(outcome.left.exists {
      case AtomicFiles.WriteFailure.TargetConflict(_, _) => true
      case _                                             => false
    })
  }

  temporaryRoot.test("publishOnce permits one publication and binds its digest") { root =>
    val target = root.resolve("results").resolve("result.json")
    val content = bytes("envelope")
    for
      first <- IO.blocking(AtomicFiles.publishOnceBlocking(target, content))
      second <- IO.blocking(AtomicFiles.publishOnceBlocking(target, content))
    yield
      assertEquals(first, Right(AtomicFiles.digestOf(content)))
      assert(second.left.exists {
        case AtomicFiles.WriteFailure.TargetExists(_) => true
        case _                                        => false
      })
  }

  temporaryRoot.test("exactly one contender atomically claims a source") { root =>
    val source = root.resolve("pending").resolve("work.inv")
    val content = bytes("invocation")
    for
      _ <- IO.blocking(Files.createDirectories(source.getParent))
      written <- IO.blocking(AtomicFiles.writeNewBlocking(source, content))
      _ = assertEquals(written, Right(()))
      outcomes <- (1 to 16).toVector.parTraverse { index =>
        IO.blocking(
          AtomicFiles.claimBlocking(
            source,
            root.resolve("claimed").resolve(s"pilot-$index").resolve("work.inv")
          )
        )
      }
    yield
      val winners = outcomes.collect { case Right(path) => path }
      val losers = outcomes.collect { case Left(AtomicFiles.ClaimFailure.SourceMissing(_)) => () }
      assertEquals(winners.size, 1)
      assertEquals(losers.size, 15)
      assertEquals(published(winners.head), content)
  }

  temporaryRoot.test("completed writes leave no private temporary artifacts") { root =>
    val target = root.resolve("clean.bin")
    for
      outcome <- IO.blocking(AtomicFiles.writeStableBlocking(target, bytes("payload")))
      _ = assertEquals(outcome, Right(()))
      leftovers <- IO.blocking {
        val entries = Files.list(root)
        try entries.filter(path => path.getFileName.toString.contains(".tmp-")).count()
        finally entries.close()
      }
    yield assertEquals(leftovers, 0L)
  }
