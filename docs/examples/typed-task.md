# Registered typed task

A registered task sends an operation reference and encoded input. The worker already contains the
implementation; the caller never serializes the function or its `IO` value.

```scala
import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.ssh.*
import io.github.bbuchsbaum.slurm4s.worker.*

object Increment extends SlurmTask[Int, Int]:
  // Identifier literals are checked where you write them: an invalid one fails the build. A value
  // computed at runtime still goes through `OperationId.from`, which returns an `Either`.
  val operation = OperationRef[Int, Int](
    OperationId("example.increment"),
    OperationVersion("1"),
    SchemaId("example.int-input.v1"),
    ResultSchemaId("example.int-result.v1")
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

With registered tasks configured on the target agent, remote submission and result retrieval use
the same task call:

```scala
val options =
  RemoteTaskOptions(
    submissionKey,
    jobName,
    resources,
    maximumResultBytes
  )

val answer: IO[Int] =
  remote
    .submitOrRaise(Increment(41), options)
    .flatMap(_.awaitValue)
```

The non-throwing `remote.submit` returns a reconnectable `RemoteTaskHandle`. Its `await` result
keeps rejection, agent failure, accounting failure, timeout, workload failure, invalid result, and
typed success distinct. `awaitValue` raises `RemoteTaskException` for every non-success and leaves
the complete typed result available as `exception.result`.

Awaiting is a polling loop, and each poll costs one SSH process and one remote agent start.
`RemoteAwaitPolicy.default` therefore waits five seconds before the first check and doubles up to a
sixty-second ceiling, and consults `sacct` only once every six polls because accounting is a shared
cluster database rather than a per-user resource. Waiting a full day costs roughly 1,400 result
reads and 240 accounting queries. Override `pollInterval`, `maximumPollInterval`, and
`accountingEveryPolls` deliberately if a site sanctions a faster cadence:

```scala
val eager = RemoteAwaitPolicy(
  pollInterval = DurationMillis.unsafeFrom(1_000L),
  timeout = DurationMillis.unsafeFrom(600_000L),
  maximumPollInterval = DurationMillis.unsafeFrom(5_000L),
  accountingEveryPolls = PositiveInt.unsafeFrom(10)
)
```

A wait also survives transient blindness. Over hundreds of polls a lost SSH round trip is close to
certain, and it says nothing about the job: the work keeps running and the durable result envelope
remains the authority. Up to `maximumConsecutiveObservationFailures` consecutive failed
observations are therefore ridden out rather than ending the wait, and one successful observation
resets the count. Only a persistent run of failures surrenders, and it reports the observation
failure itself — `AgentUnavailable` or `SchedulerUnavailable` — rather than a timeout, because
losing sight of the job is the honest account of what happened. Accounting is subject to the same
tolerance: an unreadable `sacct` response is a broken query, not a dead job.

Persist the bounded bytes from `RemoteTaskDescriptor.encode(handle.descriptor)`, not a remote
filesystem path. A later process decodes the descriptor, reconnects, and continues:

```scala
val handleDescriptor =
  RemoteTaskDescriptor.decode(persistedDescriptorBytes)
    .fold(problem => throw new IllegalArgumentException(problem.toString), identity)

val attached =
  remote.attach(handleDescriptor, Increment.outputCodec)

val answer: IO[Int] =
  IO.fromEither(attached.left.map(RemoteSubmitException.apply))
    .flatMap(_.awaitValue)
```

The agent stages only the registered operation descriptor and already encoded input. The
application worker executable on the HPC must bundle `Increment` in its `TaskRegistry` and must
have the release ID and digest configured on the agent. No task closure, codec implementation, or
`IO` value crosses SSH.

For a complete compiling execution—including progress events, staged output, publication, and
failure cases—see `WorkerRuntimeSuite` in the worker module tests.
