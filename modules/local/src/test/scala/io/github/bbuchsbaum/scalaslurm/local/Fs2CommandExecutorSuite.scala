package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.IO
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.cli.SlurmCommand
import io.github.bbuchsbaum.scalaslurm.cli.SlurmExecutable
import io.github.bbuchsbaum.scalaslurm.core.InvocationResult

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.duration.*

class Fs2CommandExecutorSuite extends munit.CatsEffectSuite:
  given Processes[IO] = Processes.forIO

  private val settings = LocalCommandSettings(
    executablePaths = Map(SlurmExecutable.Sbatch -> "/bin/sh"),
    baseEnvironment = Map("PATH" -> "/usr/bin:/bin", "LANG" -> "C"),
    allowedEnvironmentOverrides = Set("LANG", "LD_PRELOAD")
  )

  test("stdout and stderr are drained concurrently and bounded independently") {
    val executor = Fs2CommandExecutor[IO](settings)
    val shell = "i=0; while [ $i -lt 2000 ]; do printf x; printf y >&2; i=$((i+1)); done"

    executor
      .execute(
        SlurmCommand(SlurmExecutable.Sbatch, Vector("-c", shell)),
        LocalTestSupport.policy(captureBytes = 128)
      )
      .map {
        case InvocationResult.Exited(0, stdout, stderr) =>
          assertEquals(stdout.bytes.size, 128)
          assertEquals(stderr.bytes.size, 128)
          assertEquals(stdout.originalByteCount, 2000L)
          assertEquals(stderr.originalByteCount, 2000L)
          assert(stdout.truncated && stderr.truncated)
        case other => fail(s"expected successful process, got $other")
      }
  }

  test("environment overrides are allowlisted") {
    val executor = Fs2CommandExecutor[IO](settings)
    val command = SlurmCommand(
      SlurmExecutable.Sbatch,
      Vector(
        "-c",
        "printf '%s|%s|%s' \"$LANG\" \"${SECRET-unset}\" \"${LD_PRELOAD-unset}\""
      ),
      environment = Map(
        "LANG" -> "C.UTF-8",
        "SECRET" -> "leak",
        "LD_PRELOAD" -> "/tmp/untrusted.so"
      )
    )

    executor.execute(command, LocalTestSupport.policy()).map {
      case InvocationResult.Exited(0, stdout, _) =>
        assertEquals(String(stdout.bytes.toArray, StandardCharsets.UTF_8), "C.UTF-8|unset|unset")
      case other => fail(s"expected successful process, got $other")
    }
  }

  test("deadline returns partial evidence and process resource finalization prevents late work") {
    LocalTestSupport.temporaryDirectory.use { directory =>
      val marker = directory.resolve("late-marker")
      val executor = Fs2CommandExecutor[IO](settings)
      val command = SlurmCommand(
        SlurmExecutable.Sbatch,
        Vector("-c", s"printf started; sleep 1; printf late > '${marker.toString}'")
      )

      for
        result <- executor.execute(command, LocalTestSupport.policy(timeoutMillis = 50L))
        _ = assert(result.isInstanceOf[InvocationResult.TimedOut])
        _ <- IO.sleep(1200.millis)
        exists <- IO.blocking(Files.exists(marker))
        _ = assert(!exists, "timed-out process performed work after resource release")
      yield ()
    }
  }

  test("missing executable mapping is an invocation value") {
    val executor = Fs2CommandExecutor[IO](settings.copy(executablePaths = Map.empty))
    executor
      .execute(SlurmCommand(SlurmExecutable.Sbatch, Vector("--version")), LocalTestSupport.policy())
      .map(result => assert(result.isInstanceOf[InvocationResult.SpawnFailed]))
  }
