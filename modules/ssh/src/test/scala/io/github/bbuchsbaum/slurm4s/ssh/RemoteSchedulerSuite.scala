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

    remote.scheduler.submitLowered(request).map {
      case SubmissionAttempt.Completed(
            Submission.AcceptanceUnknown(AcceptanceUncertainty.TransportInterrupted, _)
          ) =>
        ()
      case other => fail(s"expected transport acceptance uncertainty, observed $other")
    }
  }

  /** P8.A3: loss after writing the cancellation request can yield only cancellation-unknown.
    *
    * `scancel` may already have run, so reporting a plain invocation failure asserts a safely
    * repeatable non-cancellation that the client cannot know to be true.
    */
  test("disconnect after cancellation write is cancellation unknown, not invocation failure") {
    val failure =
      AgentFailure.TransportDisconnected(
        afterRequestWrite = true,
        diagnostic = "response channel closed",
        evidence = None
      )
    val remote = RemoteSlurm[IO](AgentCall.Failed(failure))

    remote.scheduler.cancel(job).map {
      case CancellationAttempt.Completed(CancellationResult.Unknown(diagnostics, _)) =>
        assertEquals(diagnostics.values.head.code, "cancellation-acknowledgement-unknown")
      case other => fail(s"expected cancellation uncertainty, observed $other")
    }
  }

  test("disconnect before the cancellation write stays a typed invocation failure") {
    val failure =
      AgentFailure.TransportDisconnected(
        afterRequestWrite = false,
        diagnostic = "not connected",
        evidence = None
      )
    val remote = RemoteSlurm[IO](AgentCall.Failed(failure))

    remote.scheduler.cancel(job).map {
      case CancellationAttempt.InvocationFailed(_) => ()
      case other => fail(s"expected a repeatable invocation failure, observed $other")
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

  private val job: JobRef =
    JobRef(JobId.from("9001").toOption.get, None, None)

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
