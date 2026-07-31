package io.github.bbuchsbaum.slurm4s.ssh

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** P8.B2 law family 2: submission and cancellation uncertainty.
  *
  * The governing law is that losing the transport *after* the request was written can never be
  * reported as a safely repeatable failure — the scheduler may already have acted. P8.A3 fixed the
  * cancellation half; these properties pin both halves against every failure shape rather than the
  * two the hand-written tests happen to use.
  */
class UncertaintyLawSuite extends munit.ScalaCheckSuite:

  private val diagnostic: Gen[String] =
    Gen.oneOf("response channel closed", "connection reset", "broken pipe", "eof")

  private def disconnect(afterWrite: Boolean): Gen[AgentFailure] =
    diagnostic.map(text =>
      AgentFailure.TransportDisconnected(
        afterRequestWrite = afterWrite,
        diagnostic = text,
        evidence = None
      )
    )

  private val otherFailure: Gen[AgentFailure] =
    Gen.oneOf(
      diagnostic.map(text => AgentFailure.AgentUnavailable(text, None)),
      diagnostic.map(text => AgentFailure.AuthenticationFailed(text, None)),
      diagnostic.map(text => AgentFailure.ProtocolViolation(text, None)),
      Gen.choose(2, 9).map(major => AgentFailure.ProtocolMismatch(1, major))
    )

  property("a disconnect after the request write is never a definite non-submission") {
    forAll(disconnect(true)) { failure =>
      RemoteSlurm[IO](AgentCall.Failed(failure)).scheduler
        .submitLowered(request)
        .map {
          case SubmissionAttempt.Completed(Submission.AcceptanceUnknown(_, _)) => true
          case _                                                               => false
        }
        .unsafeRunSync()
    }
  }

  property("a disconnect after the cancellation write is never a definite non-cancellation") {
    forAll(disconnect(true)) { failure =>
      RemoteSlurm[IO](AgentCall.Failed(failure)).scheduler
        .cancel(job)
        .map {
          case CancellationAttempt.Completed(CancellationResult.Unknown(_, _)) => true
          case _                                                               => false
        }
        .unsafeRunSync()
    }
  }

  property("a disconnect before the request write stays repeatable") {
    forAll(disconnect(false)) { failure =>
      val remote = RemoteSlurm[IO](AgentCall.Failed(failure))
      val submitted = remote.scheduler.submitLowered(request).unsafeRunSync()
      val cancelled = remote.scheduler.cancel(job).unsafeRunSync()
      submitted.isInstanceOf[SubmissionAttempt.InvocationFailed] &&
      cancelled.isInstanceOf[CancellationAttempt.InvocationFailed]
    }
  }

  /** Uncertainty must never be *invented* either: a failure that proves the request never reached
    * the scheduler is safely repeatable, and reporting it as unknown would block retry forever.
    */
  property("failures that prove non-delivery are not reported as uncertain") {
    forAll(otherFailure) { failure =>
      RemoteSlurm[IO](AgentCall.Failed(failure)).scheduler
        .submitLowered(request)
        .map(_.isInstanceOf[SubmissionAttempt.InvocationFailed])
        .unsafeRunSync()
    }
  }

  private val job: JobRef = JobRef(JobId.unsafeFrom("9001"), None, None)

  private val request: JobRequest[NoResult] =
    JobRequest(
      SubmissionKey.from("uncertainty-law").toOption.get,
      JobName.from("uncertainty-law").toOption.get,
      Payload.Script(
        ScriptSource.Inline("test.sh", "#!/bin/sh\ntrue\n".getBytes("UTF-8").toVector),
        Vector.empty,
        ResultContract.ExitOnly
      ),
      ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
    )
