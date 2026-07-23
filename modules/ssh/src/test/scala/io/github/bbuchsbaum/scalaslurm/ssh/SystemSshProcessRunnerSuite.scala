package io.github.bbuchsbaum.scalaslurm.ssh

import cats.effect.IO
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis

class SystemSshProcessRunnerSuite extends munit.CatsEffectSuite:
  given Processes[IO] = Processes.forIO

  test("system process runner writes stdin and drains stdout without a shell") {
    val request = Vector[Byte](0, 1, 2, 3, 4)
    SystemSshProcessRunner[IO]
      .exchange(
        SshLaunch("/bin/cat", Vector.empty),
        request,
        SshExchangePolicy(DurationMillis.from(2000).toOption.get)
      )
      .map {
        case SshProcessOutcome.Exited(0, true, stdout, stderr) =>
          assertEquals(stdout.bytes, request)
          assertEquals(stderr.bytes, Vector.empty)
        case other => fail(s"unexpected process result: $other")
      }
  }
