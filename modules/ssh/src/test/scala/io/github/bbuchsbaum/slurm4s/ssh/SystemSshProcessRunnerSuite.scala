package io.github.bbuchsbaum.slurm4s.ssh

import cats.effect.IO
import cats.effect.Deferred
import fs2.Stream
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.core.DurationMillis

import java.io.IOException

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

  test("a failing stdout drain is typed and cancels the pending exit wait") {
    val runner = SystemSshProcessRunner[IO]
    for
      waitCancelled <- Deferred[IO, Unit]
      outcome <- runner.exchangeStreams(
        IO.unit,
        Stream.raiseError[IO](IOException("stdout detail")),
        Stream.empty,
        IO.never.onCancel(waitCancelled.complete(()).void),
        SshExchangePolicy(DurationMillis.from(2000).toOption.get)
      )
      cancelled <- waitCancelled.tryGet
    yield outcome match
      case SshProcessOutcome.Failed(
            SshProcessStage.StdoutDrain,
            diagnostic,
            true,
            _,
            _
          ) =>
        assert(diagnostic.startsWith("io-error"))
        assertEquals(cancelled, Some(()))
      case other => fail(s"unexpected process result: $other")
  }

  test("a failing exit wait is typed rather than raised") {
    SystemSshProcessRunner[IO]
      .exchangeStreams(
        IO.unit,
        Stream.empty,
        Stream.empty,
        IO.raiseError(IOException("wait detail")),
        SshExchangePolicy(DurationMillis.from(2000).toOption.get)
      )
      .map {
        case SshProcessOutcome.Failed(
              SshProcessStage.ExitWait,
              diagnostic,
              true,
              stdout,
              stderr
            ) =>
          assert(diagnostic.startsWith("io-error"))
          assertEquals(stdout.bytes, Vector.empty)
          assertEquals(stderr.bytes, Vector.empty)
        case other => fail(s"unexpected process result: $other")
      }
  }

  test("timeout retains partial bounded stream evidence and cancels pending work") {
    val runner = SystemSshProcessRunner[IO]
    for
      waitCancelled <- Deferred[IO, Unit]
      outcome <- runner.exchangeStreams(
        IO.unit,
        Stream.emit(7.toByte) ++ Stream.never[IO],
        Stream.empty,
        IO.never.onCancel(waitCancelled.complete(()).void),
        SshExchangePolicy(DurationMillis.from(50).toOption.get)
      )
      cancelled <- waitCancelled.tryGet
    yield outcome match
      case SshProcessOutcome.TimedOut(true, stdout, stderr) =>
        assertEquals(stdout.bytes, Vector(7.toByte))
        assertEquals(stderr.bytes, Vector.empty)
        assertEquals(cancelled, Some(()))
      case other => fail(s"unexpected process result: $other")
  }
