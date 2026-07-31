package io.github.bbuchsbaum.slurm4s.local

import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.AttemptEpoch
import io.github.bbuchsbaum.slurm4s.core.ByteLimit

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

class LocalSubmissionPlannerSuite extends munit.CatsEffectSuite:
  test("inline script is staged idempotently under a private attempt directory") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024 * 1024).toOption.get)
      )
      val request = LocalTestSupport.request("private-stage")

      for
        first <- planner.prepareLocal(request).map(_.toOption.get)
        second <- planner.prepareLocal(request).map(_.toOption.get)
        directory = java.nio.file.Path.of(first.submission.scriptPath).getParent
        directoryPermissions <- IO.blocking(Files.getPosixFilePermissions(directory))
        scriptPermissions <- IO.blocking(
          Files.getPosixFilePermissions(java.nio.file.Path.of(first.submission.scriptPath))
        )
        _ = assertEquals(first.submission.scriptPath, second.submission.scriptPath)
        _ = assert(first.submission.stdoutPath != first.submission.stderrPath)
        _ = assert(java.nio.file.Path.of(first.submission.stdoutPath).startsWith(root))
        _ = assertEquals(directoryPermissions, PosixFilePermissions.fromString("rwx------"))
        _ = assertEquals(scriptPermissions, PosixFilePermissions.fromString("rwx------"))
      yield ()
    }
  }

  test("a later attempt epoch cannot overwrite the previous attempt's logs") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024 * 1024).toOption.get)
      )
      val request = LocalTestSupport.request("epoch-fenced-logs")
      val second = AttemptEpoch.initial.next.toOption.get

      for
        first <- planner.prepareLocal(request).map(_.toOption.get)
        later <- planner.prepareLocal(request, second).map(_.toOption.get)
        firstStdout = java.nio.file.Path.of(first.submission.stdoutPath)
        laterStdout = java.nio.file.Path.of(later.submission.stdoutPath)
        _ = assertEquals(
          first.attemptId.value,
          later.attemptId.value,
          "attempt identity is stable across epochs"
        )
        _ = assertEquals(first.epoch.value, AttemptEpoch.initial.value)
        _ = assertEquals(later.epoch.value, second.value)
        _ = assertNotEquals(
          firstStdout,
          laterStdout,
          "each epoch must own its log path"
        )
        _ = assertEquals(first.stdout.epoch.value, AttemptEpoch.initial.value)
        _ = assertEquals(later.stdout.epoch.value, second.value)
        // Both attempts must remain independently readable after the resubmission.
        _ <- IO.blocking(Files.write(firstStdout, "first attempt\n".getBytes))
        _ <- IO.blocking(Files.write(laterStdout, "second attempt\n".getBytes))
        firstText <- IO.blocking(Files.readString(firstStdout))
        laterText <- IO.blocking(Files.readString(laterStdout))
        _ = assertEquals(firstText, "first attempt\n")
        _ = assertEquals(laterText, "second attempt\n")
      yield ()
    }
  }

  test("concurrent staging of the same inline script is idempotent and complete") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024 * 1024).toOption.get)
      )
      val request = LocalTestSupport.request("concurrent-private-stage")
      val expected = "#!/bin/sh\nprintf result\n".getBytes.toVector

      (1 to 32).toVector
        .parTraverse(_ => planner.prepareLocal(request))
        .map { outcomes =>
          assert(outcomes.forall(_.isRight), outcomes.mkString(", "))
          val paths = outcomes.flatMap(_.toOption).map(_.submission.scriptPath).distinct
          assertEquals(paths.size, 1)
          assertEquals(Files.readAllBytes(java.nio.file.Path.of(paths.head)).toVector, expected)
        }
    }
  }

  test("missing staged script is a preparation value") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val base = LocalTestSupport.request("missing-script")
      val request = base.copy(
        source = io.github.bbuchsbaum.slurm4s.core.ScriptSource.StagedLocal(
          root.resolve("absent.R").toString
        ),
        arguments = Vector.empty
      )
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(root, ByteLimit.from(1024 * 1024).toOption.get)
      )

      planner.prepareLocal(request).map(result => assert(result.isLeft))
    }
  }

  test("existing staged script is copied into the private content-addressed workspace") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val source = root.resolve("source.py")
      val workspace = root.resolve("workspace")
      val base = LocalTestSupport.request("staged-script")
      val request = base.copy(
        source = io.github.bbuchsbaum.slurm4s.core.ScriptSource.StagedLocal(source.toString),
        arguments = Vector("--quiet")
      )
      val planner = LocalSubmissionPlanner[IO](
        LocalWorkspaceSettings(workspace, ByteLimit.from(1024 * 1024).toOption.get)
      )

      for
        _ <- IO.blocking(Files.write(source, "print('ok')\n".getBytes))
        prepared <- planner.prepareLocal(request).map(_.toOption.get)
        staged <- IO.blocking(
          Files.readString(java.nio.file.Path.of(prepared.submission.scriptPath))
        )
        _ = assertEquals(staged, "print('ok')\n")
        _ = assert(java.nio.file.Path.of(prepared.submission.scriptPath).startsWith(workspace))
      yield ()
    }
  }
