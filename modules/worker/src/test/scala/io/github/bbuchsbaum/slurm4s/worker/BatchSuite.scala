package io.github.bbuchsbaum.slurm4s.worker

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.util.Comparator
import scala.jdk.CollectionConverters.*

class BatchSuite extends munit.CatsEffectSuite:
  private val operation = RegisteredOperation(
    OperationId.from("example.worker-batch").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("example.worker-batch-input.v1").toOption.get,
    ResultSchemaId.from("example.worker-batch-result.v1").toOption.get
  )
  private val release = WorkerRelease(
    WorkerReleaseId.from("worker-batch-suite").toOption.get,
    ContentDigest
      .from("sha256:6fdc880d985593d8acde2c52dd3c3b67dba0d08c858a4d426dab5fac98af0373")
      .toOption
      .get
  )
  private val perTask = TaskResources(
    PositiveInt.from("cpus", 1).toOption.get,
    Some(MemoryRequest.PerNode(Mebibytes.from(1024).toOption.get)),
    Some(WallTimeMinutes.from(30).toOption.get)
  )

  private val temporaryRoot = FunFixture[Path](
    setup = _ => Files.createTempDirectory("registered-batch-suite"),
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

  temporaryRoot.test("target preparation lowers all three plans without user command strings") {
    root =>
      val executable = root.resolve("slurm4s-worker")
      val _ = Files.writeString(executable, "#!/bin/sh\nexit 0\n")
      val _ = executable.toFile.setExecutable(true, true)
      val launcher = RegisteredTaskLauncher(
        WorkerLaunchSettings(
          root.resolve("workspace"),
          executable,
          release,
          ByteLimit.from(1024).toOption.get,
          ByteLimit.maximumCommandCapture,
          ByteLimit.from(4096).toOption.get,
          ByteLimit.from(8192).toOption.get
        )
      )
      val plans = Vector(
        "independent" -> BatchExecutionPlan.Independent(),
        "sharded" -> BatchExecutionPlan.Sharded(
          PositiveInt.from("shards", 3).toOption.get,
          PositiveInt.from("slots", 5).toOption.get,
          ShardAssignment.Exactly(PositiveInt.from("rows", 9).toOption.get)
        ),
        "gang" -> BatchExecutionPlan.Gang(
          PositiveInt.from("nodes", 9).toOption.get,
          PositiveInt.from("tasksPerNode", 3).toOption.get
        )
      )

      plans.traverse_ { case (name, execution) =>
        val request = remoteRequest(name, execution)
        launcher.prepareRemoteBatch(request).flatMap {
          case Left(diagnostics) =>
            IO.raiseError(
              new AssertionError(diagnostics.toVector.map(_.code).mkString(","))
            )
          case Right(prepared) =>
            IO.blocking {
              assertEquals(prepared.elements.length, 27)
              assertEquals(prepared.topology, request.topology)
              assertEquals(
                prepared.schedulerRequest.resources,
                request.topology.resources
              )
              assertEquals(
                prepared.schedulerRequest.array,
                request.topology.array
              )
              val launch = Files.readString(prepared.launchScript)
              execution match
                case BatchExecutionPlan.Independent(_) =>
                  assertEquals(count(launch, ") exec "), 27)
                  assert(launch.contains("SLURM_ARRAY_TASK_ID"))
                case BatchExecutionPlan.Sharded(_, slots, _, _) =>
                  assertEquals(count(launch, ") exec "), 3)
                  val firstShard =
                    Files.readString(prepared.launchScript.getParent.resolve("launch-shard-1.sh"))
                  assertEquals(count(firstShard, "'--invocation'"), 9)
                  assertEquals(count(firstShard, "wait -n"), 1)
                  assertEquals(slots.toInt, 5)
                case BatchExecutionPlan.Gang(nodes, tasksPerNode) =>
                  assert(launch.contains(s"'--nodes=${nodes.toInt}'"))
                  assert(launch.contains("'--ntasks=27'"))
                  assert(launch.contains(s"'--ntasks-per-node=${tasksPerNode.toInt}'"))
                  assert(launch.contains("'--exact'"))
                  val ranks =
                    Files.readString(
                      prepared.launchScript.getParent.resolve("launch-gang-rank.sh")
                    )
                  assertEquals(count(ranks, ") exec "), 27)
            }
        }
      }
  }

  temporaryRoot.test("opaque script argv is exact and exit publication is atomic") { root =>
    val executable = root.resolve("slurm4s-worker")
    val _ = Files.writeString(executable, "#!/bin/sh\nexit 0\n")
    val _ = executable.toFile.setExecutable(true, true)
    val launcher = RegisteredTaskLauncher(
      WorkerLaunchSettings(
        root.resolve("workspace"),
        executable,
        release,
        ByteLimit.from(1024).toOption.get,
        ByteLimit.maximumCommandCapture,
        ByteLimit.from(4096).toOption.get,
        ByteLimit.from(8192).toOption.get
      )
    )
    val topology = compileOne
    val base = SubmissionKey.from("worker-script-batch").toOption.get
    val script =
      "#!/bin/sh\nprintf '<%s>\\n' \"$@\"\nexit 7\n".getBytes(StandardCharsets.UTF_8).toVector
    val request = RemoteScriptBatchRequest(
      base,
      JobName.from("worker-script-batch").toOption.get,
      ScriptProgram(
        ScriptSource.Inline("argv.sh", script),
        ScriptInvocation.Direct
      ),
      topology,
      NonEmptyVector.one(
        RemoteScriptBatchElement(
          ArrayIndex.from(1).toOption.get,
          BatchElementKey
            .derive(base, ArrayIndex.from(1).toOption.get)
            .toOption
            .get,
          Vector(
            Argument.from("--label").toOption.get,
            Argument.from("space and ' quote").toOption.get,
            Argument.from("$HOME").toOption.get
          )
        )
      ),
      Map.empty,
      RetrySafety.NoAutomaticRetry
    )

    for
      preparedEither <- launcher.prepareRemoteScriptBatch(request)
      prepared = preparedEither.fold(
        diagnostics => fail(diagnostics.toVector.map(_.code).mkString(",")),
        identity
      )
      exitCode <- IO.blocking(
        new ProcessBuilder(prepared.elements.head.launchScript.toString).start().waitFor()
      )
      status <- launcher.readRemoteScriptExit(prepared.elements.head.exitRef)
      stdoutPath = Path.of(prepared.elements.head.stdout.locator)
      stdout <- IO.blocking(Files.readString(stdoutPath))
      element = prepared.elements.head
      launchText <- IO.blocking(Files.readString(element.launchScript))
      leftovers <- IO.blocking {
        val directory = stdoutPath.getParent
        val stream = Files.list(directory)
        try
          stream.iterator().asScala.map(_.getFileName.toString).filter(_.contains(".tmp")).toVector
        finally stream.close()
      }
      stdoutMode <- IO.blocking(
        PosixFilePermissions.toString(Files.getPosixFilePermissions(stdoutPath))
      )
    yield
      assertEquals(exitCode, 7)
      assertEquals(
        status,
        RemoteScriptExitRead.Exited(7, statusObservedAt(status))
      )
      assertEquals(
        stdout,
        "<--label>\n<space and ' quote>\n<$HOME>\n"
      )
      // The exit artifact is published on the same terms as AtomicFiles: a private, non-clobbering
      // temporary, forced to storage, then renamed. Nothing may survive the publication.
      assertEquals(leftovers, Vector.empty, "no temporary artifact may survive publication")
      assert(
        launchText.contains("set -C"),
        "the temporary must be created without clobbering an existing file"
      )
      assert(
        launchText.contains(".tmp.'\"$$\""),
        s"the temporary must be process-private, got:\n$launchText"
      )
      // umask must precede the redirects, otherwise the workload logs inherit the site default.
      assert(
        launchText.indexOf("umask 077") < launchText.indexOf(">'"),
        s"umask must be set before any redirect creates a file, got:\n$launchText"
      )
      assertEquals(stdoutMode, "rw-------", "workload logs must be created private")
  }

  private def remoteRequest(
      name: String,
      execution: BatchExecutionPlan
  ): RemoteRegisteredBatchRequest =
    val base = SubmissionKey.from(s"worker-batch-$name").toOption.get
    val topology = compile(execution)
    RemoteRegisteredBatchRequest(
      base,
      JobName.from(s"worker-batch-$name").toOption.get,
      operation,
      topology,
      topology.elementIndices.map { index =>
        RemoteRegisteredBatchElement(
          index,
          BatchElementKey.derive(base, index).toOption.get,
          index.value.toString.getBytes(StandardCharsets.UTF_8).toVector
        )
      },
      Map.empty,
      ByteLimit.from(256).toOption.get,
      Vector.empty,
      RetrySafety.SafeForAutomaticRetry
    )

  private def compile(execution: BatchExecutionPlan): BatchTopology =
    val grid = Grid.fromAxis(Axis.of(1, (2 to 27)*))
    val task = BatchTask.Script(
      ScriptProgram(
        ScriptSource.ExistingRemote("/unused"),
        ScriptInvocation.Direct
      ),
      ScriptArguments.positional[Int]
    )
    BatchPlanner
      .compile(Batch(grid, task), execution, perTask)
      .toOption
      .get
      .topology

  private def compileOne: BatchTopology =
    val task = BatchTask.Script(
      ScriptProgram(
        ScriptSource.ExistingRemote("/unused"),
        ScriptInvocation.Direct
      ),
      ScriptArguments.positional[Int]
    )
    BatchPlanner
      .compile(
        Batch(Grid.one(1), task),
        BatchExecutionPlan.Independent(),
        perTask
      )
      .toOption
      .get
      .topology

  private def statusObservedAt(value: RemoteScriptExitRead): java.time.Instant =
    value match
      case RemoteScriptExitRead.Pending(at)      => at
      case RemoteScriptExitRead.Exited(_, at)    => at
      case RemoteScriptExitRead.Failed(_, _, at) => at

  private def count(value: String, needle: String): Int =
    value.sliding(needle.length).count(_ == needle)
