# ADR 0004: Durable managed control is a journaled command reducer

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY5W5BZ9YVEVK4KK79YTBKF4`

## Decision

The optional managed surface is implemented in `slurm4s-managed`. Its authority is a
`ControlStore[F]` containing canonical submission intent, attempt projection, transactional
outbox, and an append-only committed event journal. `ManagedController[F]` coordinates that store
with the existing transport-neutral `Scheduler[F]`; it does not make a fiber, SSH connection,
FS2 stream, or process lifetime authoritative.

Every state change is a pure `ControlCommand` transition. The store commits the accepted command
before exposing the new projection or cursored events. Timestamps and scheduler results are
command data, so replay is deterministic. The same reducer drives the in-memory test interpreter
and the disk journal.

## Submission and uncertainty

Managed opaque-script submission has these durable edges:

1. Canonicalize the typed request, calculate its SHA-256 content digest, and create a deterministic
   attempt identity.
2. Commit `IntentRecorded` and a pending submit outbox item in one transaction.
3. Commit `SubmissionClaimed` and mark that outbox item in flight.
4. Invoke `Scheduler.submit` once for the successful claim.
5. Commit the returned accepted, rejected, unavailable, or acceptance-unknown value.

The same `SubmissionKey` and digest returns the existing handle without a journal append. The same
key and a different digest is a conflict before outbox claim or scheduler action. Concurrent
dispatchers race on the store transition; only the winner invokes `sbatch`.

A process restart with an in-flight submit claim cannot know whether the invocation began or
whether Slurm accepted it. Recovery therefore commits `AcceptanceUnknown` and makes the outbox
uncertain. It never places another submit item in the pending set. An injected
`AcceptanceSearch[F]` may bind exactly one evidence-backed candidate; no match, ambiguity, or
search unavailability leaves the attempt unknown. Exactly-once scheduler execution is not
claimed.

Attempt epochs are checked on every submission result or reconciled binding. `EpochFence` exposes
the same check for P4 result/output acceptance, so stale workers cannot later satisfy the current
attempt merely by returning a valid payload.

## Reference journal

`FileJournalControlStore` is the initial durable reference interpreter. It deliberately uses an
exclusive, append-only journal instead of making SQLite a mandatory dependency at this stage.
Each transaction is:

- a four-byte bounded length followed by canonical versioned JSON;
- tagged with prior and resulting store revisions;
- bound to schema `slurm4s.control-command` and protocol major 1;
- protected by a SHA-256 checksum over the canonical command JSON;
- appended under one lifetime file lock and forced to stable storage before the in-memory
  projection changes.

The configured journal parent must already be private to the user or is created private. The
journal is a regular non-symlink file and is set to user read/write only. A second writer cannot
open it. A crash may leave a partial final frame; that frame was never committed, so open truncates
only that incomplete tail and reports the byte count. A complete malformed, checksummed, schema,
revision, or transition mismatch is corruption and stops open rather than being silently repaired.

The live projection retains only a bounded recent-event cache. Cursor reads replay the durable
journal and can resume from any committed cursor, including a cursor older than that cache. This
reference is consequently bounded but O(journal size) for old event pages. A SQLite interpreter
with indexed event pages and explicit migrations may be added behind `ControlStore[F]` when scale
evidence justifies it; it must satisfy the same reducer and crash laws.

## Observation and cancellation

`ObservationCoordinator` owns one coalesced loop per `SiteId`. `SiteObserverRegistry` prevents two
supervised loops for the same site in one controller. Each tick takes a bounded batch of bound
jobs, makes one active query, records current observations or explicit stale/unavailable evidence,
and sends only terminal candidates to one accounting query. A bounded focused-reconciliation
batch is used only when accounting cannot settle those candidates. Cadence is rate-limited,
adaptive, and supplied with an injectable jitter source.

Cancellation is a separate durable requested, dispatching, acknowledged/unknown/rejected, and
reconciled lifecycle. Request and cancel outbox commit atomically. Closing resources never queues
cancel. A restart with `scancel` in flight becomes cancellation-unknown and does not issue it
again. If terminal accounting races cancellation, the terminal evidence wins, cancellation is
reconciled to that outcome, and any pending/in-flight cancel outbox entry is superseded.

Event delivery is page/cursor based. The derived FS2 stream reads those pages without an
in-memory publication queue, so a slow or canceled consumer cannot block journal append,
submission recovery, cancellation, or scheduler reconciliation.

## Known limits

- P3 supplies the `AcceptanceSearch[F]` boundary and unique-candidate reducer. A real site adapter
  still needs site-supported Slurm correlation metadata and fixtures; absence of such evidence
  remains unknown.
- The file journal is a single-writer reference and prioritizes auditable crash behavior over
  indexed query throughput. P5 load evidence will determine whether SQLite becomes the default.
- Structured result and output acceptance use the epoch-fence seam but are implemented in P4.
- Retention/compaction requires an explicit checkpoint and migration protocol; this version does
  not delete committed history.

## Executable evidence

- File reopen tests cover intent-only, claimed/in-flight, accepted/bound, partial-tail, corrupted
  record, exclusive-lock, and multi-event cursor edges.
- Controller tests prove digest idempotency/conflict before scheduler action, one invocation under
  concurrent dispatch, lost-response no-retry, effect-loss recovery, restart recovery, unique
  acceptance binding, and cancellation restart behavior.
- Reducer tests cover binding history, epoch fencing, current/stale/unavailable observation, and
  cancellation/completion reconciliation.
- Coordinator tests prove one query per bounded site batch, terminal-only accounting, delayed
  accounting fallback, deterministic adaptive jitter, and the one-loop-per-site lease.
- A deliberately sleeping event consumer cannot delay the next store transaction.

