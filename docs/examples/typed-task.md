# Registered typed task

A registered task sends an operation reference and encoded input. The worker already contains the
implementation; the caller never serializes the function or its `IO` value.

```scala
import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.worker.*

object Increment extends SlurmTask[Int, Int]:
  val operation = OperationRef[Int, Int](
    OperationId.from("example.increment").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("example.int-input.v1").toOption.get,
    ResultSchemaId.from("example.int-result.v1").toOption.get
  )

  val inputCodec = new InputCodec[Int]:
    val schemaId = operation.inputSchema
    def encode(value: Int) = Right(value.toString.getBytes("UTF-8").toVector)
    def decode(bytes: Vector[Byte]) =
      scala.util.Try(new String(bytes.toArray, "UTF-8").toInt).toEither
        .left.map(error => ResultCodecFailure("invalid-int", error.getMessage))

  val outputCodec = new ResultCodec[Int]:
    val schemaId = operation.outputSchema
    def encode(value: Int) = Right(value.toString.getBytes("UTF-8").toVector)
    def decode(bytes: Vector[Byte]) = inputCodec.decode(bytes)

  def run(input: Int, context: TaskContext[IO]): IO[Int] =
    context.progress(ProgressEvent("incrementing")) *> IO.pure(input + 1)

val registry = TaskRegistry.from(Vector(TaskRegistration(Increment)))
```

The actual invocation adds submission key, attempt ID and epoch, optional Slurm binding, byte
limits, declared output paths, and the expected `WorkerRelease`. `TaskInvocations.encode` checks
the codec schemas and bounds input before constructing that transportable value. `WorkerRuntime`
resolves the registry entry, executes it with a managed or explicitly native context, validates
outputs, and publishes one atomic result envelope.

For scheduler submission, construct a task call and lower it with typed identifiers and resources:

```scala
val request =
  Increment(41).request(
    submissionKey,
    jobName,
    resources,
    maximumResultBytes
  )
```

A target-side `RegisteredTaskLauncher` stages the versioned invocation and fixed worker command;
`RegisteredTaskSubmitter` then passes the lowered script request to `Scheduler[IO]`. This staging
must run where the worker distribution and private attempt workspace exist—locally on the HPC
host or inside the remote agent application.

For a complete compiling execution—including progress events, staged output, publication, and
failure cases—see `WorkerRuntimeSuite` in the worker module tests.
