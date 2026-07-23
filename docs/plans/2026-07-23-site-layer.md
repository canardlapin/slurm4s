# Plan: scala-slurm compute layer ("Site" layer)

## Context

The local app (Scala/JVM) needs to treat a remote Slurm HPC as an abstract compute
resource with remote data: batch execution (one scheduler job per task) and leased
persistent compute (pilot-job pool acquired from the queue, sub-second dispatch),
plus a reference-passing remote data plane. The full-codebase review (2026-07-23,
six-agent pass) established that the low-level layer is sound in design and close to
the Typelevel bar; the design discussion settled the high-level shape: a `Site` with
`SiteStore` (content-addressed `RemoteRef[A]`), a batch `TaskRunner`, and
`pool(spec): Resource[F, LeasedPool[F]]` with an honest `LeaseState` (deadline,
degradation, revocation). Pilot work distribution v1 is a shared-filesystem spool
with claim-by-atomic-rename. This plan covers: (0) discipline/packaging baseline,
(1) published facades, (2) the seven slurm-layer additions the Site layer needs,
(3) the new `modules/site`, (4) end-to-end verification. On approval, this plan is
copied to `docs/plans/` in the repo and executed phase by phase with verification at
each step.

## Assumptions (verify before Phase 3 lands)

The review's correctness fixes are being handled by in-flight work on a separate
branch (not yet merged). The Site layer depends on these four specifically — check
the in-flight branch actually covers them before building on top:

1. Managed journal cancellation windows (`FileJournalControlStore.transact`
   uncancelable commit; `dispatchCancellation` recovery; claim→invoke region).
2. sacct array contract (`JobIDRaw` vs `JobID`; bracket rows; real sacct fixture) —
   pilots are job arrays; lease monitoring is blind without it.
3. Agent session survivability (per-request `attempt`; LogPage base64 encoding;
   negotiated frame limits enforced).
4. `ScontrolOneliner` spaced-value parsing — Phase 2a reads `StartTime`/`TimeLimit`
   from scontrol output.

If any is not in flight, it joins Phase 2 as a work item.

## Type-discipline commitments (the yardstick)

Per the scala-type-discipline skill, these are the recorded commitments every phase
is reviewed against. Deviations must be discussed, never silent.

- **Errors are sealed ADTs as data.** No `Left(String)`, no exceptions for routine
  failure, no `raiseError(new IllegalStateException)` where a typed failure channel
  exists. `TaskOutcome` is total: `Succeeded | Failed | Interrupted | Unknown` —
  uncertainty is a case, never an exception.
- **No sentinels.** Absence is `Option`; no `-1`, `""`, `Instant.EPOCH`, magic keys.
- **Parse, don't validate.** New domain types are opaque types with smart
  constructors (`from: Either[ValidationFailure, T]`) plus `inline` literal
  constructors (compile-time `scala.compiletime.error`) so no `fold(throw)` ceremony.
- **No dumping grounds.** New states get new enum cases (`SlurmState.Requeued`), not
  stuffing into `Unknown(raw)`. No `case _ =>` over domain ADTs — exhaustive matches
  so the compiler polices every new case.
- **strictEquality on; `-Werror` in CI.** `derives CanEqual` becomes enforced, not
  decorative. `CanEqual`/`Order`/`Show` givens in identifier companions; any new
  `Eq`/`Order`/`Monoid` instance ships with a discipline law test (`checkAll`).
- **Effect discipline.** `F[_]: Async` polymorphism in the site module (no hardcoded
  `IO` outside mains); `Resource` for every lifecycle (lease, pool, agent
  connection); no `unsafeRun*` outside `IOApp`; blocking file I/O via
  `Sync[F].blocking`.
- **Every new codec ships a round-trip property test** (ScalaCheck, finally used)
  and, for wire-facing shapes, a canonical-bytes fixture (extend the Python-fixture
  pattern from `StructuredResultCodec`).
- **Typed references.** `RemoteRef[A]` carries site identity + path + `ContentDigest`
  + schema id; tasks consume/produce refs; no stringly paths in the site API.
- **Honesty preserved upward.** Lease deadline, granted-count degradation, and
  revocation are typed facts (`LeaseState`), never hidden; `AcceptanceUnknown`
  surfaces as `TaskOutcome.Unknown` with evidence.

## Phase 0 — Discipline & packaging baseline

Lock the bar in before new code lands on it.

- Adopt **sbt-typelevel** (`project/plugins.sbt`): ci-release, MiMa (makes the
  declared `early-semver` enforceable), license headers, CI-fatal warnings via
  tpolecat settings, generated GitHub workflow so CI cannot disagree with
  `crossScalaVersions` (currently CI tests 3.3.8/3.8.4 but the build declares only
  3.7.4). Add `LICENSE`, `licenses`/`developers`/`scmInfo` metadata.
- Enable `-language:strictEquality` in `build.sbt`; fix fallout; make
  `derives CanEqual` coverage uniform (review flagged `SchedulerQueryResult`,
  `ExecutionResult`, `Assessment`, `WireEnvelope`, `JobRequest`, `PreparedSubmission`).
- `core/Identifiers.scala`: introduce `TextIdentifier(field, maxLength)` abstract
  base collapsing the twenty near-identical companions; add
  `given CanEqual`/`Order`/`Show` per identifier; add `inline def literal` (or
  `unsafeFrom`) constructors. Sweep the ~10 `fold(throw ...)` sites
  (`ManagedModel.scala:117`, `ControlTransition.scala:691-699`,
  `FileJournalControlStore.scala:38-43`, `SchedulerTelemetry.scala:63-71`,
  `AgentMessageCodec.scala:17-19`, etc.) to use them.
- Replace `Vector[Byte]` with `ArraySeq.ofByte` behind a `type Bytes` alias in core
  (`Evidence`, `Logs`, `StructuredResult`, `VersionedJson`, frame codec) — do it now,
  before `modules/site` calcifies more signatures on the boxed representation.

Verification: `sbt +test scalafmtCheckAll mimaReportBinaryIssues`; CI green with
fatal warnings.

## Phase 1 — Published facades (Site's substrate)

Promote the compositions currently trapped in the unpublished examples module
(`PublicApiExamples.scala:90-115`) into the published modules:

- `local`: `SlurmLocal.default(workspace, settings): Resource[F, Scheduler[F]]`
  (composes `Fs2CommandExecutor` + `LocalSubmissionPlanner` + `SlurmCliScheduler` +
  `LocalLogReader`), config case class with defaults instead of 8 positional args.
- `ssh`: `Slurm.overSsh(target, policy): Resource[F, AgentApi[F]]`.
- `managed`: `ManagedController.durable(journalPath, scheduler, config)`.
- Rewrite `examples` to use the facades; add the missing observability example
  (`TelemetryScheduler` wiring).

Verification: examples compile against public API only; example suite green.

## Phase 2 — Slurm-layer additions

Seven items, ripple-mapped against source. Execution order: fix scontrol parser
(if not covered by in-flight work) → 2a+2b+2c as one protocol-compatible release
(all touch `AgentDomainJson` wire shapes) → 2d → 2e (batched with 2b for the
one-way journal upgrade) → 2f → 2g.

- **2a Time facts.** `JobObservation` (`core/Observation.scala:23-30`) gains
  `startedAt: Option[Instant] = None`, `timeLimit: Option[WallTimeMinutes] = None`
  (trailing defaults keep the ~7 construction sites compiling). Parse in
  `cli/SlurmParsers.scala:138` from squeue v0.0.43 `start_time`/`time_limit` —
  NO_VAL structs; reuse `noValUnsigned` (:151-171) with a variant tolerating
  `infinite` (legal for time_limit). Codecs are semiauto-derived
  (`AgentDomainJson.scala:261`) — fields flow automatically; needed `Instant`/
  `WallTimeMinutes` givens exist. Only the `slurm-25.05.6-v0.0.43-actual` fixture
  contains these fields — parser must tolerate absence; extend `expected.json`
  columns; respect provenance regeneration policy. Add a missing-field-tolerance
  test per the existing "legacy" pattern (`AgentDomainJsonSuite:32`) and an
  observation roundtrip to `AgentDomainJsonSuite` (none exists today). scontrol-
  sourced fields need new plumbing in `SlurmCliScheduler.focused` (:139-150) and
  strictly require the space-safe oneliner fix first.
- **2b `SlurmState.Requeued` + `InterruptionClass`.** Exactly four match sites
  (grep-verified): `SlurmStateParser.parse` (`SlurmParsers.scala:406-419`, add
  `REQUEUED`/`RQ`; consider `REQUEUE_HOLD`/`REQUEUE_FED`), `SacctParsable2.outcome`
  (:309-326 — no wildcard, compile-error tripwire; Requeued → non-terminal),
  `FailureDiagnosis.stateCause` (:437-444) and
  `ObservationCoordinator.terminalState` (:139-143) — both hidden behind
  catch-alls, must be edited deliberately (no compiler help). New pure
  `InterruptionClass` classifier (`Infrastructure | Voluntary | Indeterminate`) in
  core beside `FailureDiagnosis`. Wire note: new agent emitting `Requeued` breaks
  old clients' derived decoder — upgrade clients before agents; journals containing
  it are unreadable by old binaries (batch with 2e).
- **2c Graceful drain.** New validated `SignalSpec` type in core (no existing one);
  `JobRequest.terminationNotice: Option[SignalSpec] = None` (trailing default —
  16 construction sites unaffected). Edit the hand-rolled
  `encodeSubmitRequest`/`decodeSubmitRequest` (`AgentDomainJson.scala:18-57`) —
  **the field MUST be omitted when `None`** (pattern of `array`, :28):
  `CanonicalRequest.validated` (`ManagedModel.scala:89-107`) re-encodes and
  requires byte equality with journal bytes, so an unconditional field bricks
  replay of every existing journal. Lower to `--signal=B:SIG@secs` in
  `SlurmCommands.submit` (:12-33); accept/constrain in `ManagedRequestPolicy`;
  worker runtime handles the signal (finish task → publish → clean exit); legacy
  decode-tolerance test.
- **2d Key derivation.** `SubmissionKey.derive(segment)` in core — separator choice
  documented, derived keys respect the 200-char/no-whitespace invariant;
  `ControlStore.byKeyPrefix` on the trait + `InMemoryControlStore` +
  `FileJournalControlStore` (state-map scan, same shape as `nonTerminal`). No wire
  impact. Fixes the element-key-collision gap in `RegisteredTaskLauncher`.
- **2e `RetrySubmission`.** New arm in `ControlCommand` + the exhaustive dispatch
  (`ControlTransition.scala:91-111`, compile-error tripwire). Transition from
  `SubmissionUnavailable`: copy the *intent* with advanced epoch (epoch lives in
  `ManagedIntent`, `ManagedModel.scala:134-140`; every fence compares
  `intent.epoch`), reset phase to `IntentRecorded` (what `claimSubmission`
  accepts), mint fresh submit outbox (id already embeds epoch → naturally
  distinct), add a `ManagedEvent` case. Journal codec: encode + decode arms in
  `ControlCommandJson.scala` and **mandatory** addition to the ":176 every
  persisted command codec is canonical" enumeration test. `dispatchPending` picks
  up the re-armed entry generically. One-way journal upgrade — batch with 2b.
- **2f `AtomicFiles` kernel + claim.** Stays `private[worker]` (core is pure — no
  file I/O there; managed's own copies in `ResultAttachment`/journal checksum are
  explicitly out of scope). Reconcile the variance across the four copies:
  fsync-before-move (ResultPublisher forces, others don't → adopt force),
  executable-perms variant, idempotent write-if-same-bytes (`writeStable`),
  hex-vs-`ContentDigest` return. Add `claim(from, to)` returning typed
  `AlreadyClaimed` — the spool primitive. Direct `AtomicFiles` tests including
  claim-race semantics.
- **2g `RetrySafety` into `ResultEnvelope`.** Step 1: **move `RetrySafety` to
  core** (`worker/TaskRuntime.scala:8-11` → core; worker re-exports for source
  compat) — core cannot depend on worker. Step 2: field on `ResultEnvelope` +
  all three constructors (`StructuredResult.scala:63-149`); codec field
  **optional, defaulting to `Unknown`** so persisted `result.json` files stay
  readable (`StructuredResultCodec.scala:60-123`). Step 3: plumb through the
  registry — `TaskRegistration.execute` returns only bytes today; expose
  `retrySafety` alongside; pre-task failures (unknown task) → `Unknown`.
  Step 4: update the Python generator (`examples/python/structured_result.py`),
  regenerate `python-result-envelope-v1.json` + provenance.

Landmines (treat failures as signal, never loosen the test):
`FileJournalControlStoreSuite` ":176 canonical command codec" and ":71 crash-edge
replay" catch codec drift from 2b/2c/2e; the `CanonicalRequest` re-encode check is
the single most fragile constraint (2c); three of the four `SlurmState` catch-all
sites produce no warning when the enum grows — manual review required (2b).

Verification: `sbt +test`; round-trip properties for every touched codec; wire
fixtures re-pinned intentionally with provenance updates; exhaustive-match
tripwires (`SacctParsable2.outcome`, `ControlTransition.apply`) confirmed firing
during development.

## Phase 3 — `modules/site`

New sbt module `scala-slurm-site` depending on core, managed, worker, ssh, local
(+ `testkit % "test->compile"`); add to the root aggregate AND the hand-enumerated
`checkFormatting` alias in `build.sbt`; publishes (no `publish/skip`).
Scheduler-neutral vocabulary; Slurm words stay below the boundary.

- **3a Data plane.** `RemoteRef[A]` (site id, `SitePath`, `ContentDigest`, schema
  id); `SiteStore[F]`: `put`/`stage`/`resolve`/`fetch`, content-addressed, built on
  the agent log/file services + `AtomicFiles`. `TaskInput[I] = Inline | Stored`.
- **3b Batch runner.** `TaskRunner[F].submit[I, O](op, in, key): F[TaskHandle[F, O]]`
  over `ManagedController` + `RegisteredTaskLauncher`; `TaskHandle.status` is
  freshness-stamped; `TaskOutcome` total per the commitments; `submitAll` lowers to
  a typed job array.
- **3c Spool protocol.** Specify in an ADR (becomes `docs/architecture/0010-*.md`):
  spool layout, claim-by-rename, heartbeat files, envelope publication, pilot
  drain-on-signal. Pure protocol types + codecs first (with round-trip laws), then
  the pilot main: worker runtime in a claim→execute→publish loop until
  lease-deadline-minus-grace or drain signal.
- **3d Leased pool.** `pool(spec): Resource[F, LeasedPool[F]]` — acquire submits
  pilot array via managed (durable intent, so crash-safe), awaits ≥min registration
  via spool handshake; release drains + cancels. `LeaseState`: `deadline` (from 2a
  time facts), `granted`, `events: Stream[F, LeaseEvent]`, `onRevoked` (Deferred).
  Rolling renewal via 2e `RetrySubmission`; interrupted tasks auto-requeue iff
  envelope `RetrySafety` (2g) permits.
- **3e Attach.** `Site.attach(config): Resource[F, Site[F]]` reconstitutes handles
  and pools from the journal via 2d prefix queries.

Verification: unit + property suites per component; a conformance suite proving
batch and pooled execution produce identical `TaskOutcome` traces for the same
tasks (mirrors the existing local-vs-SSH loopback conformance pattern).

## Phase 4 — End-to-end verification & docs

- Acceptance run against `tools/disposable-slurm` (containerized Slurm): batch task,
  pooled tasks through a real sbatch-acquired pilot, lease expiry mid-task →
  `Interrupted` + auto-requeue, app restart → `attach` reconnects, drain signal →
  clean pilot exit. Record evidence per the existing `docs/acceptance/` convention.
- ADR-0010 (site layer) finalized; scaladoc for the site API surface; quickstart in
  README built on the facades.
- Fresh-context review pass per phase branch with `/scala-type-discipline` against
  this plan's commitments section (per user's working style: no self-approval).

## Critical files

Existing (modified): `build.sbt`, `project/plugins.sbt`, `core/Identifiers.scala`,
`core/Observation.scala`, `core/Request.scala`, `core/StructuredResult.scala`,
`cli/SlurmParsers.scala`, `cli/SlurmCommands.scala`, `protocol/AgentDomainJson.scala`,
`protocol/StructuredResultCodec.scala`, `managed/ControlTransition.scala`,
`managed/ControlStore.scala`, `managed/FileJournalControlStore.scala`,
`managed/ManagedController.scala`, `worker/TaskRuntime.scala`,
`worker/RegisteredTaskLauncher.scala`, `worker/FileTaskContext.scala`,
`worker/ResultPublisher.scala`.
New: `modules/site/**` (Site, SiteStore, RemoteRef, TaskRunner, LeasedPool,
LeaseState, spool protocol + pilot main), `docs/architecture/0010-site-layer.md`,
facades in local/ssh/managed, `LICENSE`.

## Verification summary

Each phase: `sbt +test scalafmtCheckAll` green under `-Werror`, MiMa clean from
Phase 0 on, codec round-trip properties, wire fixtures re-pinned intentionally
(never silently), discipline review against the commitments section, and Phase 4's
acceptance evidence against disposable-slurm.

## Follow-ups from kernel review (2026-07-23)

- `ContentDigest` (core) is stringly — accepts any ≤200-char token, no `algo:hex`
  shape. The site layer is its first hard consumer (DigestMismatch, fetch-verify);
  parse the shape when core reopens (fold into Phase 0/2 window).
- Spool file layout must NOT embed `SubmissionKey` verbatim in filenames — core
  keys permit `/` and uppercase. Route through a path-safe encoding (or the 2d
  `derive` scheme restricted to the token charset) at the pool/batch milestone.
- `PoolSpec` intentionally enforces only `minReady ≤ pilots`; the pool
  implementation must additionally reject `drainGrace ≥ walltime` at acquire.
