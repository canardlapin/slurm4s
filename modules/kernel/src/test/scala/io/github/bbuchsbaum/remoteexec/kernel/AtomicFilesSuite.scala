package io.github.bbuchsbaum.remoteexec.kernel

import cats.effect.IO
import cats.syntax.all.*

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

  private def bytes(text: String): Vector[Byte] =
    text.getBytes(StandardCharsets.UTF_8).toVector

  temporaryRoot.test("concurrent identical stable writes are logically idempotent") { root =>
    val target = root.resolve("script.sh")
    val content = bytes("#!/bin/sh\nprintf result\n")
    (1 to 32).toVector
      .parTraverse(_ => AtomicFiles.writeStable[IO](target, content, executable = true))
      .map { outcomes =>
        assert(outcomes.forall(_ == Right(())), outcomes.mkString(", "))
        assertEquals(Files.readAllBytes(target).toVector, content)
      }
  }

  temporaryRoot.test("concurrent different stable writes preserve exactly one value") { root =>
    val target = root.resolve("artifact.bin")
    val first = Vector.fill(128 * 1024)('a'.toByte)
    val second = Vector.fill(128 * 1024)('b'.toByte)
    Vector(first, second)
      .parTraverse(value => AtomicFiles.writeStable[IO](target, value))
      .map { outcomes =>
        assertEquals(outcomes.count(_ == Right(())), 1)
        assertEquals(
          outcomes.count(_.left.exists {
            case AtomicFiles.WriteFailure.TargetConflict(_, _) => true
            case _                                             => false
          }),
          1
        )
        val observed = Files.readAllBytes(target).toVector
        assert(observed == first || observed == second)
      }
  }

  temporaryRoot.test("atomic replacement readers observe only complete old or new values") { root =>
    val target = root.resolve("latest.bin")
    val oldValue = Vector.fill(256 * 1024)('o'.toByte)
    val newValue = Vector.fill(256 * 1024)('n'.toByte)
    for
      initialized <- AtomicFiles.writeStable[IO](target, oldValue)
      _ = assertEquals(initialized, Right(()))
      observed <- (
        (0 until 64).toVector.traverse_ { index =>
          val value = if index % 2 == 0 then newValue else oldValue
          AtomicFiles.replace[IO](target, value).map(result => assertEquals(result, Right(())))
        },
        (0 until 256).toVector.traverse(_ => IO.blocking(Files.readAllBytes(target).toVector))
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
      outcome <- AtomicFiles.writeStable[IO](link, bytes("replacement"), executable = true)
    yield assert(outcome.left.exists {
      case AtomicFiles.WriteFailure.TargetConflict(_, _) => true
      case _                                             => false
    })
  }

  temporaryRoot.test("publishOnce permits one publication and binds its digest") { root =>
    val target = root.resolve("results").resolve("result.json")
    val content = bytes("envelope")
    for
      first <- AtomicFiles.publishOnce[IO](target, content)
      second <- AtomicFiles.publishOnce[IO](target, content)
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
      written <- AtomicFiles.writeNew[IO](source, content)
      _ = assertEquals(written, Right(()))
      outcomes <- (1 to 16).toVector.parTraverse { index =>
        AtomicFiles.claim[IO](
          source,
          root.resolve("claimed").resolve(s"pilot-$index").resolve("work.inv")
        )
      }
    yield
      val winners = outcomes.collect { case Right(path) => path }
      val losers = outcomes.collect { case Left(AtomicFiles.ClaimFailure.SourceMissing(_)) => () }
      assertEquals(winners.size, 1)
      assertEquals(losers.size, 15)
      assertEquals(Files.readAllBytes(winners.head).toVector, content)
  }

  temporaryRoot.test("completed writes leave no private temporary artifacts") { root =>
    val target = root.resolve("clean.bin")
    for
      outcome <- AtomicFiles.writeStable[IO](target, bytes("payload"))
      _ = assertEquals(outcome, Right(()))
      leftovers <- IO.blocking {
        val entries = Files.list(root)
        try entries.filter(path => path.getFileName.toString.contains(".tmp-")).count()
        finally entries.close()
      }
    yield assertEquals(leftovers, 0L)
  }
