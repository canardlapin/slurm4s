# scala-slurm

scala-slurm is a Scala 3 library for truthful, low-level interaction with Slurm locally and
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
| `scala-slurm-core` | Pure IDs, evidence, requests, outcomes, result contracts, codecs, reducers |
| `scala-slurm-cli` | Fixed command vectors, version-scoped parsers, capability probes, truthful scheduler interpreter |
| `scala-slurm-local` | Cats Effect/FS2 process execution, private script staging, cursor-based local logs |
| `scala-slurm-protocol` | Bounded framing, versioned messages, handshake, typed scheduler wire codecs |
| `scala-slurm-agent` | Fixed stdio server, local scheduler assembly, page-based remote log service |
| `scala-slurm-ssh` | System OpenSSH argv, bounded bidirectional process lifecycle, typed remote API |
| `scala-slurm-managed` | Durable intent, command journal, outbox, reconciliation, cursored events, coalesced observation |
| `scala-slurm-worker` | Declared-output inspection, typed launch staging, worker events, atomic result envelopes, registered task runtime, managed I/O contexts |
| `scala-slurm-observability` | Optional bounded redacted scheduler events for log, metric, and trace adapters; never control authority |
| `scala-slurm-testkit` | Pure attempt replay plus deterministic command/fault scripts |

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
val request =
  Increment(41).request(submissionKey, jobName, resources, maximumResultBytes)
```

The current remote agent still accepts opaque scripts only. Target-side registered-task staging
and typed result reattachment remain explicit rather than being hidden behind a façade that cannot
yet honor them.

To install the agent on an HPC host, package the `agent` module as the executable
`scala-slurm-agent` and configure a private workspace:

```shell
export SCALA_SLURM_WORKSPACE=/path/to/private/scala-slurm
scala-slurm-agent serve --stdio
```

The command is intended to be launched by the library over OpenSSH. Its stdout is reserved for
framed protocol bytes.
