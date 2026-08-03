package io.github.bbuchsbaum.slurm4s.local

import cats.data.NonEmptyVector
import cats.effect.IO
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.cli.SlurmExecutable
import io.github.bbuchsbaum.slurm4s.core.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions

/** Guards the boundary between how much command output is read and how much is retained.
  *
  * One structured `squeue` job runs to several kilobytes, so a capture bound sized for diagnostic
  * evidence silently truncates the JSON and turns a healthy query into a parse failure. These tests
  * drive the real process executor with the shipped default configuration.
  */
class LocalCaptureBoundSuite extends munit.CatsEffectSuite:
  given Processes[IO] = Processes.forIO

  /** Sized so the rendered response comfortably exceeds `ByteLimit.defaultEvidence`. */
  private val jobCount = 60

  test("the shipped default observes a response far larger than the evidence bound") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val jobs = NonEmptyVector.fromVectorUnsafe(
        (1 to jobCount).toVector.map(index =>
          JobRef(JobId.from((5000 + index).toString).toOption.get, None)
        )
      )
      val payload = squeueJson(jobs.toVector)

      for
        _ <- IO(
          assert(
            payload.length > ByteLimit.defaultEvidence.value,
            s"the fixture must exceed the evidence bound to be meaningful, got ${payload.length}"
          )
        )
        fake <- IO.blocking(fakeSqueue(root, payload))
        config <- IO.fromEither(
          SlurmLocalConfig
            .default(root.resolve("workspace"), Map.empty)
            .left
            .map(problem => new AssertionError(problem.reason))
        )
        observed <- SlurmLocal
          .default[IO](
            config.copy(executablePaths = Map(SlurmExecutable.Squeue -> fake.toString))
          )
          .use(_.observe(jobs))
        _ = observed match
          case SchedulerQueryResult.Succeeded(batch) =>
            val states = batch.results.toVector.collect { case ObservationResult.Observed(value) =>
              value.state
            }
            assertEquals(
              states.size,
              jobCount,
              "every job in the response must be observed, not lost to truncation"
            )
            assert(states.forall(_ == SlurmState.Pending), s"unexpected states: ${states.distinct}")
          case other =>
            fail(s"a response larger than the evidence bound must still parse, got $other")
      yield ()
    }
  }

  test("retained observation evidence stays bounded and reports the original size") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val jobs = NonEmptyVector.fromVectorUnsafe(
        (1 to jobCount).toVector.map(index =>
          JobRef(JobId.from((6000 + index).toString).toOption.get, None)
        )
      )
      val payload = squeueJson(jobs.toVector)

      for
        fake <- IO.blocking(fakeSqueue(root, payload))
        config <- IO.fromEither(
          SlurmLocalConfig
            .default(root.resolve("workspace"), Map.empty)
            .left
            .map(problem => new AssertionError(problem.reason))
        )
        observed <- SlurmLocal
          .default[IO](
            config.copy(executablePaths = Map(SlurmExecutable.Squeue -> fake.toString))
          )
          .use(_.observe(jobs))
        _ = observed match
          case SchedulerQueryResult.Succeeded(batch) =>
            val evidence = batch.results.head match
              case ObservationResult.Observed(value) => value.evidence.primary
              case other                             => fail(s"unexpected result: $other")
            assertEquals(
              evidence.bytes.size,
              ByteLimit.defaultEvidence.value.toLong,
              "retained evidence must respect the evidence bound"
            )
            assert(evidence.truncated, "narrowed evidence must be reported as truncated")
            assertEquals(
              evidence.originalByteCount,
              payload.getBytes(StandardCharsets.UTF_8).length.toLong,
              "the observed original size must survive narrowing"
            )
          case other => fail(s"unexpected observation: $other")
      yield ()
    }
  }

  /** Each entry carries a wide `state_reason` so a realistic per-job byte cost is exercised. */
  private def squeueJson(jobs: Vector[JobRef]): String =
    val padding = "x" * 1500
    val entries = jobs
      .map(job =>
        s"""{"job_id":${job.jobId.value},"job_state":["PENDING"],"state_reason":"$padding"}"""
      )
      .mkString(",")
    s"""{"meta":{"data_parser":"v0.0.43"},"jobs":[$entries]}"""

  private def fakeSqueue(root: Path, payload: String): Path =
    val response = root.resolve("squeue-response.json")
    val _ = Files.write(response, payload.getBytes(StandardCharsets.UTF_8))
    val script = root.resolve("squeue")
    val _ = Files.write(
      script,
      s"#!/bin/sh\nexec cat '${response.toString}'\n".getBytes(StandardCharsets.UTF_8)
    )
    val _ = Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"))
    script
