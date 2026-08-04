package io.github.bbuchsbaum.slurm4s.managed

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.slurm4s.core.*

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions

class ManagedFacadeSuite extends munit.CatsEffectSuite:
  test("durable owns the journal resource and assembles the controller") {
    privateDirectory.use { root =>
      Managed.durable[IO](root.resolve("control.journal"), inertScheduler).use { controller =>
        controller.dispatchPending(1).map(result => assertEquals(result, Vector.empty))
      }
    }
  }

  private val inertScheduler: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      IO.raiseError(new AssertionError("unexpected capabilities"))
    def submit(spec: LaunchSpec): IO[SubmissionAttempt] =
      IO.raiseError(new AssertionError(s"unexpected submit: $spec"))
    def observe(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[ObservationBatch]] =
      IO.raiseError(new AssertionError(s"unexpected observe: $jobs"))
    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[AccountingBatch]] =
      IO.raiseError(new AssertionError(s"unexpected accounting: $jobs"))
    def cancel(job: JobRef): IO[CancellationAttempt] =
      IO.raiseError(new AssertionError(s"unexpected cancel: $job"))

  private def privateDirectory: Resource[IO, Path] =
    Resource.make(
      IO.blocking {
        val path = Files.createTempDirectory("slurm4s-managed-facade")
        val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"))
        path
      }
    )(path =>
      IO.blocking {
        val journal = path.resolve("control.journal")
        val _ = Files.deleteIfExists(journal)
        val _ = Files.deleteIfExists(path)
      }
    )
