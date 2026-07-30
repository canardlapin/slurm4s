package io.github.bbuchsbaum.slurm4s.ssh

import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure

class RemoteSchedulerSuite extends munit.CatsEffectSuite:
  test("disconnect after request write remains acceptance unknown") {
    val failure =
      AgentFailure.TransportDisconnected(
        afterRequestWrite = true,
        diagnostic = "response channel closed",
        evidence = None
      )
    val remote = RemoteSlurm[IO](AgentCall.Failed(failure))

    remote.scheduler.submit(request).map {
      case SubmissionAttempt.Completed(
            Submission.AcceptanceUnknown(AcceptanceUncertainty.TransportInterrupted, _)
          ) =>
        ()
      case other => fail(s"expected transport acceptance uncertainty, observed $other")
    }
  }

  test("disconnect before a query is a typed invocation failure") {
    val failure =
      AgentFailure.TransportDisconnected(
        afterRequestWrite = false,
        diagnostic = "not connected",
        evidence = None
      )
    val remote = RemoteSlurm[IO](AgentCall.Failed(failure))

    remote.scheduler.capabilities.map {
      case SchedulerQueryResult.InvocationFailed(
            InvocationResult.SpawnFailed(SpawnFailureKind.Unknown, diagnostics, _)
          ) =>
        assertEquals(diagnostics.values.head.code, "ssh-transport-disconnected")
      case other => fail(s"expected typed invocation failure, observed $other")
    }
  }

  private val request: JobRequest[NoResult] =
    JobRequest(
      SubmissionKey.from("ssh-scheduler-test").toOption.get,
      JobName.from("ssh-scheduler-test").toOption.get,
      Payload.Script(
        ScriptSource.Inline("test.sh", "#!/bin/sh\ntrue\n".getBytes("UTF-8").toVector),
        Vector.empty,
        ResultContract.ExitOnly
      ),
      ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
    )
