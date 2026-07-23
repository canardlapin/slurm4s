# Compatibility policy

## Scala and JVM

The minimum supported runtime is JDK 17. Published artifacts are compiled and tested with Scala
3.7.4. That final 3.7 patch is the sole supported compiler baseline for v1; moving to a different
minor line requires an explicit compatibility decision and full gate. Compiler, Cats, Cats
Effect, FS2, Circe, and protocol-version changes require their own tracked change and cannot
arrive as an incidental dependency refresh.

The exact compiler release is recorded by the official
[Scala 3.7.4 release](https://www.scala-lang.org/news/3.7.4/).

The project follows early SemVer before 1.0 and SemVer after 1.0. Source compatibility is a goal;
wire, persisted-state, event-journal, and fixture compatibility are explicit contracts.

## Wire and persisted JSON

- Every durable document has a schema identifier and `{major, minor}` protocol version.
- Readers reject unknown major versions before interpreting payload bytes.
- Minor releases may add fields. Generic envelopes retain unknown top-level fields.
- Required-field removal or semantic reinterpretation requires a new major version or schema.
- Canonical bytes are UTF-8 JSON with lexicographically sorted object keys, no insignificant
  whitespace, stable numeric/string representation, and exactly one trailing LF.
- Codecs are named and manually reviewed. Automatic derivation alone is not a durable contract.
- Golden fixtures are byte-exact and carry provenance as described in `docs/fixtures/README.md`.

The agent transport adds a four-byte network-order length prefix around canonical JSON. Both
peers apply a negotiated frame ceiling before payload allocation. Request IDs are correlation
bound, protocol-major mismatch is rejected, and minor additions may be retained as extensions.
The named `AgentDomainJson` codec owns the P2 opaque-script scheduler schema. Its derived internal
components are exercised through full local/agent differential round trips; changing their JSON
shape is a wire change and requires compatibility evidence, not a mechanical refactor.

The SSH client invokes the user's system OpenSSH and preserves its configured authentication,
host-key, proxy, and connection-sharing policies. The remote command and no-TTY policy are fixed.
Human-readable OpenSSH stderr is evidence, not a stable API: only conservative recognized cases
receive a narrower classification, while unknown exit-255 failures remain transport failures.

## Managed journal

The reference managed store persists bounded, length-framed canonical JSON records under schema
`scala-slurm.control-command` and protocol major 1. Each record carries contiguous prior/new store
revisions, a typed command, and a checksum of the canonical command object. Unknown major or
schema, checksum mismatch, revision gap, or an invalid replay transition is corruption. Only an
incomplete final frame is uncommitted and may be truncated during open.

Changing command JSON shape, canonical request bytes, digest semantics, reducer meaning, cursor
assignment, or revision behavior is a persistence-format change. Additive minor evolution still
requires replay fixtures and migration evidence. No in-place migration or journal compaction is
implemented yet; a future SQLite or compacted interpreter must first reproduce the same command
replay and committed-event cursor laws.

## Structured workload protocol

The worker protocol defines four canonical JSON schemas at protocol major 1:

- `scala-slurm.worker-event` for bounded start, progress, process-exit, failure, and
  result-publication records;
- `scala-slurm.result-envelope` for one atomic-last structured result.
- `scala-slurm.result-handle` for type-erased durable reattachment identity and limits.
- `scala-slurm.task-invocation` for registered operation identity and bounded encoded input.

A result envelope binds submission key, attempt ID and epoch, optional Slurm binding, script or
registered-operation identity, result schema, status, bounded value bytes, output paths, output
sizes and SHA-256 digests, worker-release identity, and completion time. A successful envelope
must have a value; a failed envelope must not. Readers reject unknown major versions, wrong
schemas, oversized envelope/value bytes, malformed Base64, duplicate output paths, and invalid
status/value combinations.

`modules/protocol/src/test/resources/fixtures/python-result-envelope-v1.json` is the byte-exact
cross-language fixture. The standard-library Python producer creates it and the Scala codec
re-encodes it byte-for-byte. Any field-name, status-tag, operation-tag, Base64, timestamp, or
output-entry change is a protocol change and requires fixture and migration review.

Registered Scala tasks are identified by operation ID and version plus explicit input and output
schemas. The durable invocation contains encoded bounded input and a worker-release identity; it
never contains a closure, `Future`, arbitrary JVM object graph, or `IO[A]`. Typed reattachment
checks the durable handle schema before decoding envelope bytes, then checks attempt epoch,
operation, worker release, and observed output size/digest before invoking `ResultCodec[A]`.

A target-side worker distribution used by `RegisteredTaskLauncher` implements the fixed argv
contract `run --invocation PATH --result PATH --events PATH`. These paths are library-generated,
quoted launch data; input values remain inside `scala-slurm.task-invocation`. Changing the command
shape requires a worker-launch protocol version rather than an unannounced script change.

## Slurm

The minimum Slurm release is intentionally not guessed from the newest upstream release. The local
vertical registers a `squeue` codec for data-parser v0.0.43 and a synthetic fixture that exercises
its selected shape. This is implementation coverage, not yet a supported-site claim. Support still
requires captured evidence from real Slurm releases and sites.

Each fixture records the command, Slurm version, cluster/plugin context, locale, capture time,
redaction history, and expected normalized result. Parsers request an explicit supported Slurm
JSON data-parser version. Unknown states, reasons, flags, fields, and warnings are preserved.
Parse failure produces failed observation evidence; it never implies workload failure.

Compatibility gates before v1 include captured evidence from at least two supported Slurm
major-release families, local and SSH-agent differential conformance, and real-site smoke
evidence. The version-scoped 25.05 and 26.05 fixtures currently in the tree are synthetic shape
tests with `supportClaim: false`; they do not satisfy that capture gate by themselves.

An additional exact capture from an actual Slurm 25.05.6 disposable controller proves that the
registered v0.0.43 codec handles the binary's compressed pending-array representation. Because its
worker did not register, it is not execution/accounting evidence and does not establish site
support. A second actual major family and a real unprivileged-site capture remain required.

SchedMD's current data-parser lifecycle says v0.0.43 was added in Slurm 25.05 and is scheduled for
removal in 27.05. Discovery checks both site advertisement and the local codec registry. The
interpreter fails as an unsupported parser rather than decoding a different version's payload.

Array identity is part of compatibility. The CLI renders and matches `jobId_arrayIndex`, parses
`array_job_id` plus `array_task_id` from structured queue output, and parses the numeric suffix in
accounting `JobIDRaw`. A response that omits an element or reports a sibling cannot satisfy that
element's observation or result contract.
