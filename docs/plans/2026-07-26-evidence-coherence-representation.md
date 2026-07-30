# Plan: evidence, coherence, and byte representation

- Date: 2026-07-26
- Source: follow-up to the 2026-07-25 full-codebase review (quality / coherence / elegance /
  correctness) whose eleven correctness findings shipped as P6f.28–P6f.39.
- Status: proposed, not started.

## Context

The review's central conclusion was that the abstractions are sound and the interpreters are not
yet validated against the platform they target. Items P6f.28–P6f.39 fixed everything findable by
reading. What remains is structural, and it splits into five pieces that interact:

1. **Evidence.** The review produced exactly two documentation-derived claims about Slurm's CLI
   contract. One was right (`squeue` exits non-zero for a departed job), one was wrong (`sacct`
   does have `--array`). A 50% hit rate on careful reading is the argument for capture over
   inference, and it implies more wrong assumptions remain.
2. **Journal read cost.** `scanEvents` replays the whole journal on every `events()` call, under
   the writer semaphore, and `ManagedController.eventStream` polls it — O(n²) over a controller's
   lifetime. The one review finding never actioned.
3. **Codec coherence.** Three parallel JSON encodings for the same domain types coexist;
   `Diagnostics` genuinely disagrees between two of them. P6f.30's fixture bought time, not a fix.
4. **Publication and compatibility.** `mimaPreviousArtifacts` is `Set()` — no tags, nothing
   published — so the CI binary-compatibility gate currently checks nothing.
5. **Byte representation.** `Vector[Byte]` boxes: ~5× memory on a 4 MiB log page, and it makes
   `FrameDecoder.feed` quadratic (`buffered ++ input` per chunk).

## Ordering, and why

The non-obvious constraints:

- **(5) must precede (3).** Item 3 hand-writes every public codec. Doing that on `Vector[Byte]`
  and then swapping representation means writing them twice.
- **(5) must follow good golden coverage.** A representation swap that silently changes wire bytes
  is the worst outcome. The safety net already exists — P6f.30's shape fixture plus the result
  envelope, task invocation, log page, observation timing, and Slurm state fixtures — so (5) is
  safe to do now and would *not* have been before P6f.30.
- **(4) splits.** Its infrastructure half (LICENSE, metadata, `-Werror`, CI lanes) is independent
  and hardens everything after it. Its *baseline release* half must come after (5), because (5) is
  the largest public-API churn in the plan; cutting 0.1.0 first would burn the baseline
  immediately.
- **(3) is blocked** by P6f.23 (typed pre-deadline signals), which is real work in its own right.
- **(1) is gated on a Docker daemon** that would not start on the review machine, so it runs in
  parallel rather than in the critical path.

Resulting sequence: **4a → 1 (parallel) → 2 → 5 → P6f.23 → 3 → 4b.**

## Phase 4a — Build and release infrastructure (P6f.18, first half)

Independent, unblocks confidence in every later phase.

- LICENSE file matching `licenses := Seq(License.Apache2)` already declared in `build.sbt`.
- Complete publishing metadata: `homepage`, `scmInfo`, developer entries, artifact naming.
- `-Werror` in CI, and confirm Scala 3.7.4 is the only lane (`build.sbt` already sets
  `crossScalaVersions := Seq(Versions.scala3)`; verify no stale lanes survive in the workflow).
- Exercise `publishLocal` and the worker artifact in a release gate; document the procedure.
- Do **not** cut a release yet. Configure `tlMimaPreviousVersions` so the gate activates the
  moment 4b tags.

**Verification.** `sbt +publishLocal` produces every artifact; CI fails on a deliberately
introduced warning; `docs/` records the release procedure.

## Phase 1 — Evidence harness (P6f.5a, P6f.16, P6f.33a)

Runs in parallel with everything; gated only on Docker.

The insight that makes this cheap: the disposable controller can produce far more than the
squeue-absence transcript it was built for. `sbatch --hold` followed by `scancel` yields **real
CANCELLED accounting rows with real exit codes** — no worker registration, no site allocation.

- Generalize `tools/disposable-slurm/capture-squeue-absence.sh` into `capture.sh <scenario>`
  sharing the build/start/provenance scaffolding.
- Scenarios, in value order:
  - `squeue-absence` (exists, unrun) — settles the mixed-query question behind
    `squeue-absence-ambiguous`.
  - `sacct-cancelled` — real accounting rows for a cancelled held array. Converts the synthetic
    `sacct-array-contract-v1` fixture into actual-binary evidence and directly serves P6f.5a.
  - `scontrol-show-job` — real spaced-value output for P6f.16, including `Reason` and `Command`
    with embedded spaces.
  - `capability-probes` — real `sbatch --version`, `squeue --json=list`, `scontrol show config`
    for `CapabilityParser`.
- Every scenario captures raw streams plus provenance with digests, and **writes no expected
  result**. Deciding what a fixture asserts stays a reviewed step.
- Feed the findings back: any contract the capture contradicts becomes its own bead.

**Verification.** Each scenario produces a provenance-complete directory; at least one existing
synthetic fixture is replaced by actual-binary evidence with `supportClaim` still honestly false.

**Risk.** Docker Desktop would not start locally. If it stays down this phase blocks; nothing else
in the plan depends on it.

## Phase 2 — Bounded journal event polling (P6f.10)

Self-contained; the last unactioned review finding.

- Serve the common recent-cursor path from the existing bounded in-memory event cache without
  taking the writer lock for a full-file replay.
- Define the fallback for cursors older than the cache, including torn-tail semantics.
- Keep cursor and truncation behaviour explicit and unchanged.

**Verification.** A scale test showing poll cost does not grow with journal length, and a
concurrency test showing writers are not held behind replay. Both must fail against today's code —
demonstrated by running them before the fix, as with P6f.28 and P6f.30.

## Phase 5 — `Vector[Byte]` → `scodec.bits.ByteVector`

121 occurrences in main, 47 in test, across 9 modules. Mechanical but wide, and wire-sensitive.

- **Dependency decision first.** `scodec-bits 1.2.4` is already on the classpath transitively via
  fs2 for local/ssh/agent/managed/worker, but kernel/core/protocol have only cats and circe.
  Admitting it there is an explicit ADR 0001 dependency decision — worth recording as a short ADR,
  with the argument that it adds no new version surface.
- Order: `kernel` → `core` → `protocol` → interpreters. Each module compiles and its suite passes
  before moving on.
- Hot paths that motivate the change, and where the benefit must be shown:
  - `FrameCodec`/`FrameDecoder.feed` — replace `buffered ++ input` accumulation.
  - `CommonLogRecognizer.recognize` — drop the full boxed `map(asciiLower)` copy.
  - `CaptureState.append` / `SshCapture.append`.
- Base64, digest, and canonical-JSON paths are the wire-risk surface; they are exactly what the
  existing golden fixtures cover.

**Verification.** Every golden fixture byte-identical before and after — that is the whole safety
argument. Plus a JMH or simple timed benchmark on `FrameDecoder.feed` over a 4 MiB frame in 64 KiB
chunks, recorded before and after, so the claimed win is measured rather than asserted.

**Abort condition.** If any fixture changes bytes and the cause is not immediately understood,
stop and treat it as a wire change requiring its own decision.

## Phase P6f.23 — Typed pre-deadline signals

Prerequisite for Phase 3. Already specified in its bead; no changes proposed here.

## Phase 3 — Own every public wire codec (P6f.12)

Unblocked once P6f.23 lands.

- Replace derived encoders with explicit, stable field names and discriminators for the ten types
  P6f.30 currently pins by fixture.
- Resolve the three-way duplication the review found:
  - `AgentDomainJson` derived instances vs `encodeRemote*` hand-written instances vs
    `StructuredJson` — a third implementation of the same shapes.
  - `Diagnostics` disagrees today: bare array (derived) vs `{"entries":[...]}` (`encodeRemote*`).
    One must win, and the change is a wire decision.
- Fold in the adjacent coherence findings from the review, which have no beads yet:
  - Canonical printer defined twice (`VersionedJson:31`, `JournalCodec:364`) — one definition for
    a named compatibility contract.
  - `IdentifierRules` duplicated byte-identically in `kernel` and `core`.
  - Hand-rolled `traverse`/`mapN` in `StructuredJson` despite cats being available.
  - String-prefix failure tagging in `SshAgentWireClient` (`"remote-cli:"` / `stripPrefix`) —
    replace with a two-case local ADT.
  - Typed `AgentFailure` flattened to `toString` on the wire in `SchedulerRequestHandler`.
- Revisit the P6f.32 decision: with owned codecs, `AccountingBatch` can carry malformed rows, so
  accounting can move from fail-the-response to partial-success-naming-the-rows.
- Correct the documentation overclaim: `docs/compatibility.md` says reattachment checks "observed
  output size/digest"; `RemoteResultValidation.decodeSuccess` compares path sets only.

**Verification.** Round-trip and canonicalization properties via ScalaCheck (present but unused for
codec laws today); the P6f.30 fixture regenerated deliberately, with the diff reviewed as a wire
change rather than mechanically blessed.

## Phase 4b — Cut the compatibility baseline (P6f.18, second half)

- Tag and publish `0.1.0` once the public API has settled after Phase 5 and Phase 3.
- Confirm `mimaReportBinaryIssues` stops being a no-op: verify `mimaPreviousArtifacts` is non-empty
  and that a deliberate binary break fails the gate.

**Verification.** Deliberately remove a public method and confirm CI fails — the same
verify-by-breaking standard used throughout P6f.28–P6f.39.

## Tracking

Existing beads: P6f.5a, P6f.10, P6f.12, P6f.16, P6f.18, P6f.23, P6f.33a.
New beads to file on approval: the Phase 5 representation swap and its ADR, the Phase 1 harness
generalization, and the untracked coherence items folded into Phase 3.

## Explicitly out of scope

- The `Site` layer (`docs/plans/2026-07-23-site-layer.md`) and anything above slurm4s.
- Protocol-level multiplexing of the SSH transport; ADR 0003's one-process-per-request decision
  stands.
- Real-site acceptance on an authenticated cluster, which remains P6f.5a's own gate.
