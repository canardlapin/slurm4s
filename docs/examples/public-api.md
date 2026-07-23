# Compile-checked public API examples

The `scala-slurm-examples` module is executable documentation. Its main sources compile against
the published modules on Scala 3.7.4, but the examples module itself is not published. This makes
API drift a build failure without adding an examples artifact to the library surface.

The complete source is
`modules/examples/src/main/scala/io/github/bbuchsbaum/scalaslurm/examples/PublicApiExamples.scala`.
It covers the following low-level workflows.

## Opaque scripts: local and remote

Build opaque requests with `JobRequests.exitOnly`. A `ScriptSource.Inline` or
`ScriptSource.StagedLocal` request can be submitted through the runtime returned by
`LocalOpaque.runtime`; that runtime composes `Fs2CommandExecutor`, `LocalSubmissionPlanner`, and
`SlurmCliScheduler`.

For a remote HPC login node, validate an `SshConnection`, create a framed wire client with
`RemoteOpaque.systemWire`, and negotiate the agent with `RemoteOpaque.connect`. Submit with
`RemoteOpaque.submit`. A remote process failure remains an `AgentFailure`; an `sbatch` rejection,
unknown acceptance, or accepted job remains a distinct `SubmissionAttempt`.

The SSH command is a fixed argument vector that starts `scala-slurm-agent serve --stdio`. The
request is a bounded protocol frame; no user script or argument is interpolated into a shell
command.

## Declared outputs

Use `JobRequests.declaredOutputs` for an opaque Python, R, shell, or other program whose return
value is a set of files. Output paths are validated as relative paths, duplicates are rejected,
and the manifest byte limit is explicit. Declaring an output does not make untrusted script
claims authoritative: the consumer still inspects and verifies the files it accepts.

## Resumable logs

`LogMonitoring.readLocal` accepts the `LocalLogReader` from the local runtime, and
`LogMonitoring.readRemote` accepts the connected agent. Both read at most one requested
`ByteLimit`. Begin with `LogCursor.start`. For a `LogReadResult.Page`, persist `page.next` and
supply it to the next call. `WaitingForFile` is normal while a queued job has not created its log.
`CursorInvalid` means the file was replaced or truncated; it is not silently treated as an empty
page.

Remote logs are read by the agent and returned in bounded pages. They do not depend on keeping one
SSH process or FS2 stream alive.

For opaque workloads, pass each already bounded page to `CommonLogRecognizer.recognize` with an
explicit hint-count and excerpt-byte limit, and handle validation failure for an incoherent
page/cursor pair. Feed valid hints, together with scheduler, accounting, worker, and result
evidence, to `FailureDiagnosis.assess`. Log patterns can produce only `Assessment.Suspected`; they
never override confirmed controller, worker, or result evidence. Temporary unavailability or a
missing log produces `Assessment.Undetermined`, not workload failure.

## Managed restart recovery

`ManagedRecovery.durableController` holds an exclusive, append-only journal for the resource
lifetime. On process restart, `ManagedRecovery.afterRestart`:

1. converts persisted in-flight submission or cancellation claims to explicit uncertainty;
2. runs the injected evidence-backed acceptance search for unknown submissions; and
3. dispatches only outbox entries that were durably pending, with caller-supplied bounds.

An uncertain in-flight submission is never blindly issued again. No match, ambiguous candidates,
or unavailable accounting leaves it unknown.

## Typed Scala tasks and results

`IncrementTask` is a small `ScalaTask[Int, Int]`. `IncrementTask.create` validates its versioned
operation and schema identifiers. `TypedResults.request` binds the input codec and structured
result codec to a `Payload.RegisteredTask`; it does not serialize a closure or an `IO`.

On the execution host, `RegisteredTaskLauncher` lowers that request to a fixed worker command and
stages a bounded invocation. `TypedResults.submit` submits the lowered script. Register the same
task implementation in the worker with `TypedResults.registry`.

After completion, `TypedResults.attach` uses the managed attempt and durable result handle to
check submission identity, attempt epoch, operation, schema, worker release, result byte limits,
and declared output evidence before returning `ExecutionResult[Int]`. A workload failure, invalid
result, or indeterminate read remains distinct from a typed success.

The examples tests exercise validation, log resumption, remote failure separation, bounded empty
restart recovery, and typed schema binding without requiring Slurm or SSH.
