# v1 evidence matrix

- Snapshot: 2026-07-22
- Work item: `bd-01KY6ET05M2J54TGQSN9ZMH7CD`
- Compiler baseline at this snapshot: Scala 3.7.4
- Runtime floor: JDK 17
- Status: **local requirements implemented; external P5 evidence incomplete**

> **Amended 2026-08-03.** The publication baseline is now Scala 3.3.8 LTS with 3.8.4 as a
> verification-only lane, changed under P8.B4 and recorded in
> [ADR 0001](../architecture/0001-foundation-boundaries.md). The measurements below are the
> 2026-07-22 snapshot and are left as recorded; they are not restated for the current baseline.
> On 2026-08-03 the expanded gate ran 516 tests successfully on each of Scala 3.3.8 and 3.8.4,
> locally published every publishable artifact, and packaged the worker. That run used JDK 25 and
> exposed that the script did not enforce its stated JDK 17 prerequisite, so it is implementation
> evidence rather than accepted release evidence. The current gate now requires an explicit
> JDK-17 `JAVA_HOME`; a conforming rerun remains pending.
> A later current-tree verification on the same date passed 542 tests on each Scala lane, generated
> API documentation for both lanes, and passed formatting. It also used JDK 25 because no JDK 17 is
> installed on the verification host, so it strengthens implementation evidence but does not close
> the release gate.

This is a live audit of `PRD.html` sections 15–20 against the current source tree. A tracker state,
test name, synthetic fixture, or green local build is not accepted as proof by itself. Each claim
below points to executable or inspectable evidence, and gaps remain explicit.

Status vocabulary:

- **Local complete**: the contract is implemented and covered by deterministic current-tree
  evidence.
- **External pending**: the local contract exists, but its required real Slurm/site evidence is
  incomplete.
- **Local partial**: a code, test, policy, or documentation gap remains in the repository.
- **Optional not admitted**: the optional feature has not earned a production dependency or
  support claim and is not a v1 release gate.

## Functional requirements

| ID | Status | Current evidence | Gap or limitation |
| --- | --- | --- | --- |
| F-01 | Local complete | Core immutable ADTs; `IdentifiersSuite`, `RequestSuite`, `WorkloadSuite`, `VersionedJsonSuite` | Persisted/wire compatibility still requires release audit on every change. |
| F-02 | Local complete | `SlurmCommandsSuite`, `Fs2CommandExecutorSuite`, ADR 0002 | Fixed argv, deadlines, dual-stream bounded capture, and process finalization are deterministic evidence. |
| F-03 | Local complete | `SlurmCliSchedulerSuite`, `SlurmCliVerticalSuite`, `FailureModelSuite` | Accepted, rejected, preparation/invocation failure, and acceptance uncertainty remain distinct. |
| F-04 | External pending | `SlurmParsersSuite`, `SlurmCliVerticalSuite`, version-scoped fixtures, one actual 25.05.6 capture | A second actual major-family capture is still required. |
| F-05 | Local complete | `ManagedControllerSuite`, `ControlTransitionSuite`, `ObservationCoordinatorSuite` | Cancellation acknowledgement is deliberately not terminal proof. |
| F-06 | External pending | `LocalSubmissionPlannerSuite`; Python/R/shell reference producers; compile-checked examples | Opaque Python/R/shell execution has not passed the real unprivileged-site matrix. |
| F-07 | Local complete | `LocalLogReaderSuite`, `SshAgentConformanceSuite`, `FailureDiagnosisSuite` | Log availability remains site-dependent and explicit. |
| F-08 | External pending | Capability probes in `SlurmCliVerticalSuite`; `SiteProfileSuite`; agent handshake tests | Real site capability/policy discovery is still unavailable. |
| F-09 | Local complete | `FrameCodecSuite`, `AgentStdioServerSuite`, `SshFailureClassificationSuite`, `SshAgentConformanceSuite` | This proves framed system-OpenSSH semantics with deterministic transports, not site installation permission. |
| F-10 | Local complete | `FileJournalControlStoreSuite`, `JournalCompactionSuite`, `JournalReplayLawSuite`, `ManagedControllerSuite`, ADR 0004 | The bounded snapshot-plus-suffix journal is the single-writer reference interpreter. |
| F-11 | Local complete | Digest conflict, lost response, acceptance search, restart, and epoch-fence cases across managed tests | Exactly-once scheduler execution is not claimed. |
| F-12 | Local complete | `WorkloadSuite`, opaque declared-output inspection in `WorkerRuntimeSuite` | Declared files still require independent filesystem observation. |
| F-13 | Local complete | `StructuredResultCodecSuite`, Python byte-exact fixture, Python/R producers, Scala worker runtime | R is a reference example rather than a golden CI fixture. |
| F-14 | Local complete | `WorkerRuntimeSuite`, `ResultAttachmentSuite`, compile-checked `slurm4s-examples` module | Remote typed submission requires a target-side worker/launcher assembly. |
| F-15 | Local complete | Managed/native `FileTaskContext` cases in `WorkerRuntimeSuite`, ADR 0005 | Native context remains explicitly lower assurance. |
| F-16 | Local complete | `ObservationCoordinatorSuite`, 4,096-job `ObservationScaleSuite` | Deterministic call counts do not model site latency or federation. |
| F-17 | External pending | `JobArraySuite`, array parser/command tests, typed-array lowering in `WorkerRuntimeSuite` | Real Slurm array execution/accounting attribution is still required. |
| F-18 | Optional not admitted | `Scheduler[F]` remains transport-neutral; decision recorded in P5 evidence | No authenticated REST-enabled site exists for CLI/REST parity, so no REST dependency or support claim is admitted. |

## Quality requirements

| ID | Status | Current evidence | Gap or limitation |
| --- | --- | --- | --- |
| Q-01 | Local complete | Disjoint failure ADTs, managed stale/unavailable transitions, `FailureDiagnosisSuite` | Log recognizers remain suspected and cannot override stronger evidence. |
| Q-02 | Local complete | Resource/finalizer tests in local, SSH, managed journal, and worker modules; explicit `ByteLimit`/batch bounds | Native filesystem correctness still depends on OS guarantees reported by each interpreter. |
| Q-03 | Local complete | Journal reopen, torn-tail, snapshot-compaction boundary, retained-cursor gap, and controller restart tests | Multi-writer storage is intentionally absent; atomic rename durability remains filesystem-dependent. |
| Q-04 | External pending | `docs/compatibility.md`, versioned codecs/fixtures, one actual 25.05.6 capture | Supported Slurm range cannot be certified until the second family and site receipts exist. |
| Q-05 | Local complete | Bounded process/frame/log/result APIs; coalesced observation and slow-consumer tests | Real controller limits remain a site acceptance concern. |
| Q-06 | Local complete | Fixed SSH argv/system authority, private state, limited agent operations, fail-closed `ManagedRequestPolicy`, ADR 0008 | Arbitrary environment values are rejected by default; explicitly admitted public values and opaque workload content remain caller responsibility. |
| Q-07 | Local complete | Deterministic command executor, SSH runner, scheduler, store, clock/jitter, filesystem, and worker tests | Real-site behavior remains complementary evidence, not a fake replacement. |
| Q-08 | Local complete | Durable events plus bounded redacted `TelemetryScheduler` summaries and ADR 0009 | Log/metric/trace runtime adapters remain optional; telemetry cannot replace journal or scheduler evidence. |
| Q-09 | Local complete | Ordinary ADTs/algebras plus the compile-checked public examples module | P6 still requires a final public API compatibility review before release. |
| Q-10 | Local complete | Protocol-major/schema checks, canonical fixtures, journal replay checks, compatibility policy | v1 has no predecessor to migrate; future format changes require an explicit migration implementation and fixture. |

## Current local gate

Run:

```shell
tools/acceptance/v1-local-gate.sh
```

The gate requires an explicit JDK-17 `JAVA_HOME`, checks shell syntax, verifies the fail-closed
fake-Slurm acceptance harness, validates that every F-01…F-18 and Q-01…Q-10 row remains present,
runs formatting, executes the complete suite on both supported Scala lines, locally publishes
every publishable artifact, and packages the worker. `SLURM4S_CACHE_ROOT` may point sbt and Coursier
at a writable cache root.

At this snapshot, formatting passed, all **154 tests** passed, and `worker/packageBin` succeeded.
JDK 25 emitted Scala 3.7.4's terminally deprecated `sun.misc.Unsafe` warning; bytecode and Java
sources are still compiled with release 17.

This gate is local evidence only. It does not contact SSH, Slurm, or `slurmrestd`, and it cannot
close external requirements.

## Remaining v1 blockers

External P5 receipts:

1. **Slurm 26.05 actual-binary capture** with exact parser/version/provenance evidence, completing
   the second major-family gate.
2. **An authenticated unprivileged real-site capture** using the fail-closed harness, including
   actual array execution/accounting attribution, capabilities, policy assumptions, and limits.

Remaining local P6 work is the final public API and release/upgrade review after the external
receipts. No current local requirement row is marked partial.

Optional REST parity remains unadmitted and non-gating unless an authenticated site exposes it and
differential evidence justifies the dependency.
