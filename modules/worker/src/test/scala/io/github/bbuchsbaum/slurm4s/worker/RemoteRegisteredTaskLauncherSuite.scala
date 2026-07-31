package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Comparator

class RemoteRegisteredTaskLauncherSuite extends munit.CatsEffectSuite:
  private val inputLimit = ByteLimit.from(1024).toOption.get
  private val resultLimit = ByteLimit.from(128).toOption.get
  private val envelopeLimit = ByteLimit.from(4096).toOption.get
  private val outputLimit = ByteLimit.from(8192).toOption.get
  private val release = WorkerRelease(
    WorkerReleaseId.from("remote-worker-1").toOption.get,
    ContentDigest
      .from("sha256:f2d58b10a6afdd905632763ca78e97095c341e354c582aa9f45ade25a9102813")
      .toOption
      .get
  )

  private val temporaryRoot = FunFixture[Path](
    setup = _ => Files.createTempDirectory("remote-registered-launcher"),
    teardown = root =>
      val stream = Files.walk(root)
      try
        stream
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
      finally stream.close()
  )

  temporaryRoot.test(
    "remote preparation stages only encoded invocation data and a durable handle"
  ) { root =>
    val launcher = createLauncher(root)
    for preparedEither <- launcher.prepareRemote(request)
    yield preparedEither match
      case Left(diagnostics) => fail(diagnostics.toString)
      case Right(prepared)   =>
        val invocationBytes = Files.readAllBytes(prepared.invocationPath).toVector
        val invocation = TaskInvocationCodec
          .decode(invocationBytes, ByteLimit.maximumCommandCapture, inputLimit)
          .toOption
          .get
        val handleBytes =
          Files
            .readAllBytes(prepared.invocationPath.getParent.resolve("result-handle.json"))
            .toVector

        assertEquals(invocation.operation, request.operation)
        assertEquals(invocation.inputBytes, request.inputBytes)
        assertEquals(invocation.retrySafety, request.retrySafety)
        assertEquals(
          DurableResultHandleCodec.decode(handleBytes, RemoteTaskWireLimits.MaximumHandleBytes),
          Right(prepared.resultHandle)
        )
        assert(!new String(invocationBytes.toArray, StandardCharsets.UTF_8).contains("SlurmTask"))
        prepared.schedulerRequest.payload match
          case Payload.Script(
                ScriptSource.ExistingRemote(path),
                arguments,
                ResultContract.ExitOnly
              ) =>
            assertEquals(path, prepared.launchScript.toString)
            assertEquals(arguments, Vector.empty)
          case other => fail(s"expected lowered worker script, received $other")
  }

  temporaryRoot.test("remote result reads are pending, bounded, and preserve the stored handle") {
    root =>
      val launcher = createLauncher(root)
      for
        prepared <- launcher.prepareRemote(request).map(_.toOption.get)
        pending <- launcher.readRemoteResult(prepared.resultRef, envelopeLimit)
        envelope = ResultEnvelope.succeeded(
          prepared.resultHandle.submissionKey,
          prepared.resultHandle.attemptId,
          prepared.resultHandle.attemptEpoch,
          prepared.resultHandle.job,
          prepared.resultHandle.operation,
          prepared.resultHandle.resultSchema,
          "42".getBytes(StandardCharsets.UTF_8).toVector,
          OutputManifest.empty,
          prepared.resultHandle.workerRelease,
          Instant.parse("2026-07-24T12:00:00Z")
        )
        envelopeBytes = ResultEnvelopeCodec
          .encode(envelope, envelopeLimit, resultLimit)
          .toOption
          .get
        _ <- IO.blocking {
          val _ = Files.write(prepared.resultPath, envelopeBytes.toArray)
        }
        available <- launcher.readRemoteResult(prepared.resultRef, envelopeLimit)
      yield
        assert(pending.isInstanceOf[RemoteResultRead.Pending])
        available match
          case RemoteResultRead.Available(storedHandle, bytes, _) =>
            assertEquals(storedHandle, prepared.resultHandle)
            assertEquals(bytes, envelopeBytes)
          case other => fail(s"expected an available result, received $other")
  }

  temporaryRoot.test("oversized result evidence retains a prefix and counts the whole file") {
    root =>
      val launcher = createLauncher(root)
      val original = Vector.fill(envelopeLimit.value + 37)(0x7f.toByte)
      for
        prepared <- launcher.prepareRemote(request).map(_.toOption.get)
        _ <- IO.blocking {
          val _ = Files.write(prepared.resultPath, original.toArray)
        }
        result <- launcher.readRemoteResult(prepared.resultRef, envelopeLimit)
      yield result match
        case RemoteResultRead.Failed(diagnostics, evidence, _) =>
          assertEquals(diagnostics.toVector.map(_.code), Vector("remote-result-envelope-too-large"))
          assertEquals(evidence.primary.bytes.size, envelopeLimit.value)
          assertEquals(evidence.primary.originalByteCount, original.size.toLong)
          assert(evidence.primary.truncated)
        case other => fail(s"expected bounded oversized-result failure, received $other")
  }

  temporaryRoot.test("unknown and mismatched result references stay typed failures") { root =>
    val launcher = createLauncher(root)
    val unknown = RemoteResultRef(
      AttemptId.from("unknown-remote-attempt").toOption.get,
      AttemptEpoch.initial
    )
    launcher.readRemoteResult(unknown, envelopeLimit).map {
      case RemoteResultRead.Failed(diagnostics, _, _) =>
        assertEquals(diagnostics.toVector.map(_.code), Vector("remote-result-reference-unknown"))
      case other => fail(s"expected an unknown-reference failure, received $other")
    }
  }

  private def createLauncher(root: Path): RegisteredTaskLauncher =
    val executable = root.resolve("slurm4s-worker")
    val _ = Files.writeString(executable, "#!/bin/sh\nexit 0\n")
    val _ = executable.toFile.setExecutable(true, true)
    RegisteredTaskLauncher(
      WorkerLaunchSettings(
        root.resolve("workspace"),
        executable,
        release,
        inputLimit,
        ByteLimit.maximumCommandCapture,
        envelopeLimit,
        outputLimit
      )
    )

  private val request = RemoteRegisteredTaskRequest(
    SubmissionKey.from("remote-launcher-41").toOption.get,
    JobName.from("remote-launcher").toOption.get,
    RegisteredOperation(
      OperationId.from("example.increment").toOption.get,
      OperationVersion.from("1").toOption.get,
      SchemaId.from("example.int-input.v1").toOption.get,
      ResultSchemaId.from("example.int-result.v1").toOption.get
    ),
    "41".getBytes(StandardCharsets.UTF_8).toVector,
    ResourceRequest.validate(1, 1, None, None, None).toOption.get,
    Map.empty,
    resultLimit,
    Vector.empty,
    RetrySafety.SafeForAutomaticRetry
  )
