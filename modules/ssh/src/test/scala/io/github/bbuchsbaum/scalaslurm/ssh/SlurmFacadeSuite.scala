package io.github.bbuchsbaum.scalaslurm.ssh

import cats.effect.IO
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.protocol.AgentCall
import io.github.bbuchsbaum.scalaslurm.protocol.AgentFailure

class SlurmFacadeSuite extends munit.CatsEffectSuite:
  given Processes[IO] = Processes.forIO

  test("overSsh validates configuration before creating the resource") {
    val target = SshTarget
      .from("alice@login.cluster.example")
      .fold(problem => fail(problem.toString), identity)
    val connection = SshConnection("ssh", target)
    val config =
      SlurmSshConfig.default(connection).fold(problem => fail(problem.toString), identity)

    assert(Slurm.overSsh[IO](config).isRight)
    assert(
      Slurm
        .overSsh[IO](config.copy(connection = connection.copy(executablePath = "\u0000")))
        .isLeft
    )
  }

  test("RemoteSlurm preserves a failed handshake across operations") {
    val failure = AgentFailure.AuthenticationFailed("public-key authentication failed", None)
    val remote = RemoteSlurm[IO](AgentCall.Failed(failure))

    remote.capabilities.map(result => assertEquals(result, AgentCall.Failed(failure)))
  }
