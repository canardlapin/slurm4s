package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import io.github.bbuchsbaum.slurm4s.core.PositiveInt

import java.nio.file.Files
import java.nio.file.Path
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class DrainNoticeSourceSuite extends munit.CatsEffectSuite:
  test("a default context exposes no drain capability or implied JVM handler") {
    temporaryDirectory.use { root =>
      FileTaskContext
        .managed(FileTaskWorkspace(root), Map.empty)
        .use(context => IO(assertEquals(context.drainNotice, None)))
    }
  }

  test("a managed task context exposes an injected one-shot drain notice") {
    temporaryDirectory.use { root =>
      for
        announced <- Deferred[IO, Unit]
        observed <- Ref.of[IO, Boolean](false)
        result <- FileTaskContext
          .managed(
            FileTaskWorkspace(root),
            Map.empty,
            drainNoticeSource = Some(DrainNoticeSource.injected(announced.get))
          )
          .use { context =>
            for
              source <- IO.fromOption(context.drainNotice)(
                new AssertionError("configured drain notice was not exposed")
              )
              waiter <- (source.await *> observed.set(true)).start
              before <- observed.get
              _ <- announced.complete(())
              _ <- waiter.joinWithNever
              after <- observed.get
            yield before -> after
          }
      yield assertEquals(result, false -> true)
    }
  }

  test("a file source observes a supervisor marker without consuming it") {
    temporaryDirectory.use { root =>
      val marker = root.resolve("drain.notice")
      val source = DrainNoticeSource.file(marker, PositiveInt.unsafeFrom(5))
      for
        waiter <- source.await.start
        _ <- IO.blocking { val _ = Files.createFile(marker) }
        _ <- waiter.joinWithNever.timeout(2.seconds)
        _ <- source.await.timeout(2.seconds)
        remains <- IO.blocking(Files.exists(marker))
      yield assert(remains, "the adapter must leave supervisor state untouched")
    }
  }

  private def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("slurm4s-drain-notice-test")))(root =>
      IO.blocking {
        val stream = Files.walk(root)
        try
          stream
            .sorted(java.util.Comparator.reverseOrder())
            .iterator()
            .asScala
            .foreach { path =>
              val _ = Files.deleteIfExists(path)
            }
        finally stream.close()
      }
    )
