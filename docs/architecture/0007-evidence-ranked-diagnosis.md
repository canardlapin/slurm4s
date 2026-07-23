# ADR 0007: Failure diagnosis is pure, evidence-ranked, and conservative

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY6EF4KAEXK9B9SQVAFF8C87`

## Decision

`scala-slurm-core` exposes a pure `Assessment[FailureDiagnosis]` with three outcomes:
`Confirmed`, `Suspected`, and `Undetermined`. Diagnosis combines already-observed submission,
scheduler, accounting, worker, structured-result, and log evidence; it performs no effects and
owns no polling, file, process, stream, or durable state.

Authority is explicit. Current accounting has the highest scheduler authority, followed by
validated structured results, explicit worker failure/exit events, current active scheduler
state, and submission/preparation evidence. A stale terminal observation is only suspected.
Temporary unavailability, not-found, acceptance uncertainty, an absent result, or a missing log
cannot create a failure cause.

When strong planes disagree, the higher-authority cause is primary and other distinct confirmed
causes are retained as contributors. Evidence is never discarded. This ordering is deterministic,
but it is not a claim of causal proof beyond the recorded sources.

## Opaque log hints

`CommonLogRecognizer` scans an already bounded `LogPage` for a small closed set of ASCII markers:
Python tracebacks, R errors, JVM exceptions, file-not-found messages, memory pressure, and killed
processes. It:

- caps the number of returned hints with `PositiveInt`;
- caps each retained excerpt with `ByteLimit`;
- preserves the exact global byte match range and retained byte range;
- rejects an incoherent page/cursor relationship as validation data rather than throwing;
- scans bytes without requiring line allocation or whole-log decoding; and
- always returns suspected causes.

These recognizers are convenience heuristics for opaque scripts. A textual “out of memory” can
support an OOM suspicion but cannot override accounting, scheduler, worker, or validated result
evidence. Raw workload bytes remain in the explicit diagnosis value and are not copied into
operational telemetry by this component.

## Bounds and evolution

`DiagnosisInput.from` bounds the combined worker-event and log-hint count. Submission,
observation, accounting, and result slots are singular. The evidence values they reference retain
the byte limits applied at their acquisition boundaries.

Adding a recognizer is a semantic API change because it can add a suspected candidate. Adding or
reordering a confirmed authority requires focused conflict tests and release documentation.
Backend-specific DTOs and parser details do not enter this algebra.

## Executable evidence

Core tests prove:

- current accounting confirms OOM while a conflicting log hint stays suspected;
- recognizer ranges remain byte-exact across nonzero page offsets and excerpts stay bounded;
- malformed page/cursor input is rejected without throwing;
- scheduler unavailability and acceptance uncertainty remain undetermined;
- stale terminal observations remain suspected;
- strong conflicts retain a deterministic primary and contributor; and
- diagnosis input rejects evidence counts beyond its configured bound.
