package io.github.bbuchsbaum.slurm4s.local

import cats.effect.IO
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.core.Scheduler

import java.nio.file.Path

class SlurmLocalSuite extends munit.CatsEffectSuite:
  given Processes[IO] = Processes.forIO

  test("default configuration assembles a scheduler and bounded log reader") {
    val config = SlurmLocalConfig
      .default(Path.of("/tmp/slurm4s-local-facade"), Map.empty)
      .fold(problem => fail(problem.toString), identity)

    SlurmLocal.default[IO](config).use { runtime =>
      val scheduler: Scheduler[IO] = runtime
      IO(assertEquals(scheduler, runtime))
    }
  }
