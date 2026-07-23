package io.github.bbuchsbaum.scalaslurm.ssh

import io.github.bbuchsbaum.scalaslurm.core.DurationMillis

class SshCommandSuite extends munit.FunSuite:
  test("agent launch uses no TTY and an invariant remote command") {
    val target = SshTarget.from("research-cluster").toOption.get
    val proxy = SshTarget.from("bastion").toOption.get
    val launch = SshCommand
      .agent(
        SshConnection(
          "/usr/bin/ssh",
          target,
          Vector(
            SshOption.ProxyJump(proxy),
            SshOption.ConnectTimeout(DurationMillis.from(2500).toOption.get)
          )
        )
      )
      .toOption
      .get

    assertEquals(launch.executablePath, "/usr/bin/ssh")
    assertEquals(
      launch.arguments.take(3),
      Vector("-T", "-o", "RequestTTY=no")
    )
    assertEquals(
      launch.arguments.takeRight(4),
      Vector("research-cluster", "scala-slurm-agent", "serve", "--stdio")
    )
  }

  test("target cannot be interpreted as an OpenSSH option or split into shell words") {
    assert(SshTarget.from("-oProxyCommand=bad").isLeft)
    assert(SshTarget.from("host command").isLeft)
    assert(SshTarget.from("host\ncommand").isLeft)
  }

  test("direct compatibility mode advertises its losses") {
    val capabilities = DirectSshCompatibility.capabilities
    assert(!capabilities.protocolFrames)
    assert(!capabilities.pagedLogs)
    assert(!capabilities.durableControl)
    assert(capabilities.degradationReasons.nonEmpty)
  }
