package io.github.bbuchsbaum.slurm4s.ssh

import io.github.bbuchsbaum.slurm4s.core.DurationMillis

class SshCommandSuite extends munit.FunSuite:
  test("agent launch uses no TTY, non-interactive authentication, and an invariant command") {
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
      launch.arguments.take(5),
      Vector("-T", "-o", "RequestTTY=no", "-o", "BatchMode=yes")
    )
    assertEquals(
      launch.arguments.takeRight(4),
      Vector("research-cluster", "slurm4s-agent", "serve", "--stdio")
    )
  }

  test("interactive authentication requires an explicit connection mode") {
    val target = SshTarget.from("research-cluster").toOption.get
    val launch = SshCommand
      .agent(
        SshConnection(
          "/usr/bin/ssh",
          target,
          authentication = SshAuthentication.ConfiguredInteractive
        )
      )
      .toOption
      .get

    assertEquals(
      launch.arguments.take(5),
      Vector("-T", "-o", "RequestTTY=no", "-o", "BatchMode=no")
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
