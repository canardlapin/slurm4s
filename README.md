# slurm4s

slurm4s is a Scala 3 library for truthful, low-level interaction with Slurm locally and
through a fixed agent protocol over OpenSSH. It models command invocation, scheduler acceptance,
observation freshness, workload execution, logs, and typed result validation as distinct facts.

The project has a tested foundation, local CLI vertical, bounded SSH-agent transport, durable
managed control, a structured worker runtime, advisory site profiles, typed job-array lowering,
and controller-safe coalesced observation.
[PRD.html](PRD.html) is the normative product specification; the source modules and executable
tests are the implementation authority.

## Initial modules

| Module | Boundary |
| --- | --- |
| `remote-exec-kernel` | Provider-neutral validated IDs, codecs, diagnostics/freshness, retry provenance, failure reports, and atomic filesystem mechanics |
| `slurm4s-core` | Slurm requests, scheduler evidence/outcomes, result contracts, and reducers; depends downward on the neutral kernel |
| `slurm4s-cli` | Fixed command vectors, version-scoped parsers, capability probes, truthful scheduler interpreter |
| `slurm4s-local` | Cats Effect/FS2 process execution, private script staging, cursor-based local logs |
| `slurm4s-protocol` | Bounded framing, versioned messages, handshake, typed scheduler wire codecs |
| `slurm4s-agent` | Fixed stdio server, local scheduler assembly, page-based remote log service |
| `slurm4s-ssh` | System OpenSSH argv, bounded bidirectional process lifecycle, typed remote API |
| `slurm4s-managed` | Durable intent, command journal, outbox, reconciliation, cursored events, coalesced observation |
| `slurm4s-worker` | Declared-output inspection, typed launch staging, worker events, atomic result envelopes, registered task runtime, managed I/O contexts |
| `slurm4s-observability` | Optional bounded redacted scheduler events for log, metric, and trace adapters; never control authority |
| `slurm4s-testkit` | Deterministic scheduler, command, log-read, and effect-failure programs with call traces |

The core module depends on Cats and Circe but contains no Cats Effect or FS2 runtime values.
Routine operational failures are data. An effect fails only for caller cancellation, defects, or
an interpreter failure that cannot yet be classified safely.

## Build

The minimum runtime is JDK 17. The publication and verification baseline is Scala 3.7.4.

```shell
sbt test
sbt +test
sbt scalafmtCheckAll
```

The foundation and compatibility decisions are recorded in
[`docs/architecture/0001-foundation-boundaries.md`](docs/architecture/0001-foundation-boundaries.md)
and [`docs/compatibility.md`](docs/compatibility.md).
Provider-neutral ownership and the compatibility window are recorded in
[`docs/architecture/0010-provider-neutral-kernel.md`](docs/architecture/0010-provider-neutral-kernel.md).

The local CLI implementation is described in
[`docs/architecture/0002-local-cli-vertical.md`](docs/architecture/0002-local-cli-vertical.md).
The remote authority, framing, reconnect, and failure decisions are described in
[`docs/architecture/0003-agent-over-openssh.md`](docs/architecture/0003-agent-over-openssh.md).
Durable submission, journal, recovery, observation, and cancellation semantics are described in
[`docs/architecture/0004-durable-managed-control.md`](docs/architecture/0004-durable-managed-control.md).
Declared outputs, structured envelopes, registered Scala tasks, and typed reattachment are
described in
[`docs/architecture/0005-structured-workloads.md`](docs/architecture/0005-structured-workloads.md).
Site-policy resolution, per-element array identity, and scale laws are described in
[`docs/architecture/0006-site-scale.md`](docs/architecture/0006-site-scale.md). The current
real-site evidence gap and reproducible smoke procedure are recorded in
[`docs/acceptance/p5-site-scale.md`](docs/acceptance/p5-site-scale.md).
The executable typed-task API is introduced in
[`docs/examples/typed-task.md`](docs/examples/typed-task.md).

Published assembly façades keep routine setup out of application code:

```scala
val local = SlurmLocal.default[IO](localConfig)
val remote = Slurm.overSsh[IO](sshConfig)
val durable = Managed.durable[IO](journal, scheduler)
```

Registered Scala operations can expose a compact call shape while retaining explicit schemas and
codecs in the worker definition:

```scala
val options =
  RemoteTaskOptions(submissionKey, jobName, resources, maximumResultBytes)

val answer: IO[Int] =
  remote
    .submitOrRaise(Increment(41), options)
    .flatMap(_.awaitValue)
```

`submit` is the non-throwing form and returns `Either[RemoteSubmitFailure, RemoteTaskHandle]`.
`submitOrRaise` and `awaitValue` are explicit conveniences; their exceptions retain the typed
failure or complete `RemoteExecutionResult`. Persist `handle.descriptor` to reattach from a new
process: `RemoteTaskDescriptor.encode` and `decode` provide the bounded, versioned durable form,
and `remote.attach(descriptor, Increment.outputCodec)` resumes polling.

To enable registered tasks, the target agent needs a private workspace and an application worker
executable containing the same versioned task registry:

```shell
export SLURM4S_WORKSPACE=/path/to/private/slurm4s
export SLURM4S_WORKER_EXECUTABLE=/path/to/my-registered-worker
export SLURM4S_WORKER_RELEASE_ID=my-worker-2026-07-24
export SLURM4S_WORKER_RELEASE_DIGEST=sha256:deployed-worker-content
export SLURM4S_ALLOWED_ENVIRONMENT=LANG,OMP_NUM_THREADS
slurm4s-agent serve --stdio
```

The worker distribution owns the registered Scala implementations; the protocol sends only an
operation/version descriptor and bounded encoded input. The agent command is launched by the
library over OpenSSH, and its stdout is reserved for framed protocol bytes.
