# ADR 0005: Structured workloads and typed result authority

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY5W5C46S7TJ3846BBEMX5N3`

## Decision

Opaque scripts and registered Scala tasks share the same scheduler boundary but make different
result claims. Exit-only mode claims only process evidence. Declared-output mode additionally
requires an exact set of ordinary files. Structured mode requires one bounded, versioned result
envelope written atomically after every output is complete. Typed Scala tasks are structured
workloads whose operation implementation is resolved from a worker-local registry.

The result envelope—not stdout, exit zero, a Scala type parameter, or a client cache—is the typed
result authority. Acceptance also requires a matching durable handle, current attempt epoch,
operation, schema, worker release, and independently observed output manifest.

## Pure contracts

Core owns immutable values only:

- `RegisteredOperation` is operation ID, operation version, input schema, and result schema.
- `WorkerRelease` identifies the installed worker distribution and its content digest.
- `ResultEnvelope` binds submission key, attempt ID, epoch, optional Slurm job, operation,
  result schema, status, bounded value bytes, output manifest, worker release, and completion time.
- `OutputValidation` compares declared, reported, and independently observed paths, sizes, and
  digests. Missing, extra, duplicate, size-mismatched, and digest-mismatched outputs remain
  distinct failures.
- `WorkerEvent` carries the same attempt and release identity for start, progress, exit, failure,
  and result-publication events.

These values contain no Cats Effect or FS2 runtime objects.

## Wire protocol

`ResultEnvelopeCodec`, `WorkerEventCodec`, `DurableResultHandleCodec`, and `TaskInvocationCodec`
use canonical UTF-8 JSON under protocol major 1 and schemas `scala-slurm.result-envelope`,
`scala-slurm.worker-event`, `scala-slurm.result-handle`, and `scala-slurm.task-invocation`.
Decoders apply byte ceilings before parsing or Base64 allocation. Input, result, envelope, and
event records have independent limits. Output count is bounded, output paths are relative, and
duplicate paths are rejected.

`FileWorkerEventSink` appends one already-bounded canonical event under an OS file lock and forces
the write. It does not use a topic or unbounded queue. `FileResultPublisher` encodes first, writes
and forces a private temporary sibling, and atomically moves it into place under a per-target OS
lock. A completed target is never overwritten by another library publisher.

The Python golden fixture proves that a non-Scala producer is accepted and re-encodes to the same
bytes. The Python example uses only the standard library. The R helper uses `jsonlite` and
`openssl`, neither of which becomes a scala-slurm runtime dependency.

## Registered Scala runtime

`ScalaTask[I, O]` supplies an `OperationRef`, explicit `InputCodec[I]`, explicit
`ResultCodec[O]`, and `run(input, TaskContext[IO]): IO[O]`. `TaskInvocations.encode` turns a caller
input into bounded bytes before it becomes a durable or transported value. The worker registry
selects a task only by operation ID and version, then checks both schemas before decoding input.
It bounds output bytes before constructing an envelope.

`RegisteredTaskLauncher` is the target-side bridge to `Scheduler[IO]`. It writes the canonical
task invocation into a private attempt directory, creates a fixed worker-distribution command with
only quoted library-generated paths, and returns an ordinary script request plus durable result
handle. `RegisteredTaskSubmitter` stages that bundle before calling the same scheduler algebra.
Task input bytes remain in the invocation document and never appear in shell text. Reusing the
same submission key and operation with changed invocation bytes conflicts during staging rather
than silently replacing an artifact.

The worker distribution contains `TaskRegistration` objects at assembly time. No closure,
`Future`, arbitrary JVM graph, fiber, or `IO[A]` value is serialized. An implementation object may
use effect-polymorphic components internally; the deployed runtime selects `IO`.

Task failure, codec failure, schema mismatch, output validation failure, oversized data, unknown
operation, wrong worker release, and publication failure are separate `TaskFailure` cases. A
class name and bounded message may describe a raised exception; exception object graphs and stack
traces are not result values.

## File capabilities

Managed `TaskContext` exposes:

- logical-name input reads with a caller-supplied byte ceiling;
- relative-path staged output writes and exact manifest sealing;
- scoped scratch directories removed when their `Resource` closes;
- structured progress and a logger algebra.

It does not expose input paths or a workspace root. `NativeTaskContext` deliberately exposes
`nativeRoot` and labels itself `NativeFilesystem`; it is convenience for trusted workloads, not a
capability-isolation or retry-safety claim. Symlinked output parents are rejected, file reads and
directory inspection are bounded, and staged writes use atomic siblings.

## Durable reattachment and fencing

`DurableResultHandle` stores ordinary durable identity, including `ResultSchemaId`; it does not
store `A`. `ResultAttachment.attach` checks the supplied `ResultContract[A]` against the handle
before parsing envelope bytes. It then checks attempt identity and epoch, envelope bindings,
worker release, declared/reported/observed output equality, and finally the explicit result codec.
Wrong-schema reattachment, malformed bytes, stale epochs, missing output, size/digest mismatch,
and worker-reported failure cannot produce `ExecutionResult.Succeeded`.

Retry safety is independent. `ScalaTask.retrySafety` defaults to `Unknown`, and the runtime never
uses typing to authorize automatic retry.

## Known limits

- Worker-distribution installation and registry assembly remain application/site concerns. The
  current P2 agent endpoint still exposes opaque scheduler submission; a host that wants remote
  typed submission must assemble `RegisteredTaskSubmitter` on the target side.
- The low-level typed submitter invokes the scheduler once per call and does not infer retry
  safety or supply managed idempotency. Compose it with durable control for that policy.
- The current `ManagedController.submit` entry point persists exit-only scheduler requests. A
  future typed managed facade must persist both original typed intent and lowered launch identity;
  the P4 primitives do not erase that distinction or claim the facade already exists.
- The reference file event sink is append-only and has no compaction.
- Atomic moves require a filesystem that supports same-directory atomic move.
- Native filesystem context is explicitly lower assurance.
