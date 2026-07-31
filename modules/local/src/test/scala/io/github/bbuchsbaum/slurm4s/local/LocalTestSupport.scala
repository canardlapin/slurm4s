package io.github.bbuchsbaum.slurm4s.local

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.slurm4s.core.*

import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

object LocalTestSupport:
  def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("slurm4s-test-")))(deleteRecursively)

  def policy(
      timeoutMillis: Long = 2000L,
      captureBytes: Int = 65536
  ): io.github.bbuchsbaum.slurm4s.cli.CommandPolicy =
    io.github.bbuchsbaum.slurm4s.cli.CommandPolicy(
      DurationMillis.from(timeoutMillis).toOption.get,
      ByteLimit.from(captureBytes).toOption.get
    )

  def request(key: String = "local-test"): LaunchSpec =
    LaunchSpec(
      submissionKey = SubmissionKey.from(key).toOption.get,
      name = JobName.from("opaque-analysis").toOption.get,
      source = ScriptSource.Inline("analysis.sh", "#!/bin/sh\nprintf result\n".getBytes.toVector),
      arguments = Vector.empty,
      resultContract = ResultContract.ExitOnly.descriptor,
      resources = ResourceRequest.validate(1, 1, Some(1), None, None).toOption.get
    )

  private def deleteRecursively(root: Path): IO[Unit] =
    IO.blocking {
      if Files.exists(root) then
        val paths = Files.walk(root)
        try
          paths.sorted(Comparator.reverseOrder()).forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
        finally paths.close()
    }.void
