package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit

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
        permissions <- IO.blocking(Files.getPosixFilePermissions(directory))
        _ = assertEquals(first.submission.scriptPath, second.submission.scriptPath)
        _ = assert(first.submission.stdoutPath != first.submission.stderrPath)
        _ = assert(java.nio.file.Path.of(first.submission.stdoutPath).startsWith(root))
        _ = assertEquals(permissions, PosixFilePermissions.fromString("rwx------"))
      yield ()
    }
  }

  test("missing staged script is a preparation value") {
    LocalTestSupport.temporaryDirectory.use { root =>
      val base = LocalTestSupport.request("missing-script")
      val request = base.copy(
        payload = io.github.bbuchsbaum.scalaslurm.core.Payload.Script(
          io.github.bbuchsbaum.scalaslurm.core.ScriptSource.StagedLocal(
            root.resolve("absent.R").toString
          ),
          Vector.empty,
          io.github.bbuchsbaum.scalaslurm.core.ResultContract.ExitOnly
        )
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
        payload = io.github.bbuchsbaum.scalaslurm.core.Payload.Script(
          io.github.bbuchsbaum.scalaslurm.core.ScriptSource.StagedLocal(source.toString),
          Vector("--quiet"),
          io.github.bbuchsbaum.scalaslurm.core.ResultContract.ExitOnly
        )
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
