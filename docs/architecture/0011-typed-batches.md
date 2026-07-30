# ADR-0011: Typed parameter grids and explicit batch execution

## Status

Accepted and implemented.

## Context

A parameter sweep is a collection of logical task invocations, not a file of shell commands.
Slurm can execute the same sweep in materially different ways:

- one array element per logical task;
- one array element per one-node shard, with bounded concurrency inside the shard; or
- one multi-node allocation whose ranks are launched by `srun`.

Those choices change accounting identity, resource multiplication, failure visibility, and log
placement. They must therefore be explicit domain values rather than boolean options or inferred
command-line fragments.

The workload may also have one of two honest result contracts. A registered Scala task has input
and output codecs and can publish a durable typed value. An opaque Python, R, or shell program has
exact arguments, logs, and an exit code, but no invented Scala return type.

## Decision

### Grids and arguments

`Axis[A]` and `Grid[A]` are non-empty and deterministically ordered. `Grid.cross` assigns the
resulting rows stable, one-based logical indices. For products, `ScriptArguments.derived` uses
Scala 3 mirrors and `ArgValue` instances to produce exact `--field value` arguments in constructor
order.

```scala
enum Method:
  case Ridge, Lasso, ElasticNet

given ArgValue[Method] = ArgValue.fromString(_.toString.toLowerCase)

final case class Fit(alpha: Double, method: Method, seed: Int)
given ScriptArguments[Fit] = ScriptArguments.derived

val fits =
  Grid.cross(
    Axis.of(0.1, 0.5, 1.0),
    Axis.of(Method.Ridge, Method.Lasso, Method.ElasticNet),
    Axis.of(1, 2, 3)
  )(Fit.apply)
```

`fits` contains 27 rows. The first script invocation receives:

```text
--alpha 0.1 --method ridge --seed 1
```

Arguments remain an argv vector through the public model and protocol. Users never provide a
shell command line. Target-side shell scripts are private lowering artifacts whose paths and
arguments are individually quoted.

### Workloads

`SlurmBatch.registered(grid, task)` describes a registered Scala computation. Each logical element
gets an independent durable result envelope and can be awaited as a typed value.

`SlurmBatch.script(grid, program)` describes an opaque program. `ScriptSource.StagedLocal` is read
with a byte bound and transported once; `Inline` is transported directly; `ExistingRemote` is
validated on the target. Each logical element gets independent stdout, stderr, and an atomic
exit-status artifact. It returns `RemoteScriptExecutionResult`, never a fictitious value.

### Execution plans

`BatchExecutionPlan` has three non-interchangeable cases:

- `Independent(maximumRunning)` lowers 27 rows to `--array=1-27`, optionally with `%N`.
- `Sharded(shards, slotsPerShard, assignment, maximumRunningShards)` lowers to a one-node array
  whose elements own groups of logical rows. A generated work-conserving shard launcher runs no
  more than `slotsPerShard` child processes simultaneously and starts the next row when any child
  finishes. The launcher checks for Bash 4.3's `wait -n` support before starting work and otherwise
  exits with a direct diagnostic.
- `Gang(nodes, tasksPerNode)` requests one allocation and launches exactly
  `nodes * tasksPerNode` ranks with `srun --exact`.

`ShardAssignment.Exactly(rowsPerShard)` rejects mismatched shapes. Balanced-contiguous and
round-robin assignment are explicit alternatives.

`TaskResources` describes one logical task. Planning derives the Slurm allocation request:
per-node CPU and memory are multiplied by shard concurrency; gang tasks and nodes are stated
directly. Per-CPU memory is not multiplied. Overflow and impossible shapes are typed
`BatchPlanFailure` values.

Examples for 27 rows:

```scala
val onePerNode =
  BatchExecutionPlan.Independent()

val nineNodesThreeAtATime =
  BatchExecutionPlan.Sharded(
    shards = PositiveInt.unsafeFrom(9),
    slotsPerShard = PositiveInt.unsafeFrom(3),
    assignment = ShardAssignment.Exactly(PositiveInt.unsafeFrom(3))
  )

val threeNodesAtMostFiveAtATime =
  BatchExecutionPlan.Sharded(
    shards = PositiveInt.unsafeFrom(3),
    slotsPerShard = PositiveInt.unsafeFrom(5),
    assignment = ShardAssignment.Exactly(PositiveInt.unsafeFrom(9))
  )

val nineNodeGang =
  BatchExecutionPlan.Gang(
    nodes = PositiveInt.unsafeFrom(9),
    tasksPerNode = PositiveInt.unsafeFrom(3)
  )
```

The `unsafeFrom` calls above are documentation shorthand for already-validated application
configuration; request-facing code should accumulate constructor failures.

### Remote use

A registered computation returns values in logical-grid order:

```scala
val handle =
  remote.submitBatchOrRaise(
    SlurmBatch.registered(fits, fitTask),
    threeNodesAtMostFiveAtATime,
    RemoteBatchOptions(
      submissionKey = submissionKey,
      name = jobName,
      perTask = perFitResources,
      maximumResultBytes = resultLimit
    )
  )

val values: IO[NonEmptyVector[FitResult]] =
  handle.flatMap(_.awaitValues)
```

An opaque script preserves the same grid and execution plan but returns exit outcomes:

```scala
val program = ScriptProgram(
  ScriptSource.StagedLocal("./fit.py"),
  ScriptInvocation.Via(CommandPrefix.python3)
)

val outcomes: IO[NonEmptyVector[RemoteScriptBatchElementResult[Fit]]] =
  remote
    .submitBatchOrRaise(
      SlurmBatch.script(fits, program),
      threeNodesAtMostFiveAtATime,
      RemoteScriptBatchOptions(
        submissionKey = submissionKey,
        name = jobName,
        perTask = perFitResources
      )
    )
    .flatMap(_.await)
```

Every element handle retains its logical input, logical index, shard index, stdout reference, and
stderr reference. Registered handles additionally retain a reconnectable result descriptor.

## Failure semantics

Planning, argument encoding, input-codec failure, source staging, protocol mismatch, and agent
failure remain distinct submission failures. A lost `sbatch` response remains
`AcceptanceUnknown`; durable results or atomic script exit status may still resolve the work.

For script shards and gangs, an element exit artifact has higher authority than aggregate Slurm
accounting. If an element never publishes one and its containing Slurm job becomes terminal, the
result is `WorkloadTerminated`, not a fabricated child exit code.

## Consequences

The low-level library now models logical work independently from physical placement while keeping
Slurm-specific execution explicit. Higher-level pool or site layers can choose a plan, but do not
need to reinterpret raw array indices, command files, logs, or scheduler state.
