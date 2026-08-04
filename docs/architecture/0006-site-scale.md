# ADR 0006: Site policy, array identity, and controller-safe scale

- Status: accepted; real-site evidence pending
- Date: 2026-07-22
- Mote: `bd-01KY5W5C9N1RN8D1PFKQZ1D6EE`

## Decision

Portable resource intent and Slurm placement are separate values. `SiteProfile.resolve` is a pure,
advisory preflight that returns a `SiteResolution` containing the untouched requested resources
and intent plus one effective site specification. Rejection accumulates independent policy
violations. The profile never claims to reproduce controller state, associations, submission
plugins, reservations, or policy that changed after discovery.

Job arrays are one scheduler allocation with many independently attributable workload attempts.
An array element therefore keeps its own submission key, attempt ID and epoch, stdout and stderr
references, result contract, optional durable result handle, and `JobRef(jobId, arrayIndex)`.
Parent job ID alone is never sufficient identity for observation, accounting, cancellation, logs,
or result attachment.

## Site-profile boundary

`SiteIntent` carries optional account, partition, QoS, constraints, modules, container,
accelerators, native options, and environment-export policy. A `SiteProfile` may supply defaults,
admit named values, disable resource modes and features, and impose configured limits for nodes,
tasks, CPUs, wall time, memory, arrays, and array concurrency.

Resolution rejects, among other cases:

- required or unknown accounts, partitions, and QoS values;
- unavailable constraints, modules, containers, or accelerators;
- disallowed per-node, per-CPU, or all-node memory conventions;
- node counts greater than task counts and configured resource maxima;
- arbitrary native options that the profile did not explicitly admit;
- disabled arrays or arrays above configured size/concurrency limits.

The CLI lowers an effective specification to a fixed argv vector. It never concatenates a shell
command. Explicit environment export names are sorted into `--export=NAME,...`, while values stay
in the process environment. `None` becomes `--export=NONE`; `All` is an explicit opt-in.
Site-approved native options remain individual argv values. Module initialization remains a
launcher/site integration concern because environment modules are not a portable Slurm option.
`SlurmCliScheduler.submitAt` performs preflight before staging or invoking `sbatch` and returns
either the accumulated policy rejection or the exact `SiteResolution` beside the scheduler
attempt. A target-side remote service may expose the same operation with its locally owned
profile; the generic P2 agent endpoint does not accept client-authored site policy.

## Array lowering

`JobArrayRequest` validates a non-empty unique index set and an optional concurrency throttle.
The request is part of `JobRequest`, so it survives the canonical agent wire format and reaches
the target-side CLI interpreter. Legacy agent documents with no array field still decode as a
non-array request.

`RegisteredTaskArrayRequest` groups one registered operation, input codec, result contract, and
resource shape with independently keyed element inputs. `RegisteredTaskLauncher.prepareArray`
encodes and stages every input in a separate canonical invocation. It generates a fixed `case` on
`SLURM_ARRAY_TASK_ID`; each validated numeric branch executes the worker with only quoted,
library-generated paths and redirects to distinct bounded-log locations. Input values never
appear in the dispatch script.

`JobArrayPlan` rejects duplicate indices, submission keys, or attempt IDs; incompatible result
contracts; mismatched log identities; and result handles bound to another attempt. Binding a
parent Slurm job derives exact `jobId_index` element references. `squeue` JSON parsing prefers
`array_job_id` and `array_task_id`; `sacct` parses `JobID` array suffixes; `scontrol` and
`scancel` target the element suffix. Missing-result detection compares base ID plus array index,
not base ID alone.

## Controller load and fairness

One `ObservationCoordinator` remains responsible for a site. It requests at most the configured
batch size in one scheduler call, accounts only terminal candidates, and applies a separate cap to
focused probes. Candidate selection is ordered by last update time, so a committed observation
moves a still-active job behind jobs not yet sampled. This fixes starvation caused by repeatedly
selecting the oldest recorded attempts.

`ObservationScaleSuite` constructs 4,096 running jobs and a batch size of 256. Complete fair
coverage requires exactly 16 observation calls, every call remains within the bound, and no
accounting or focused calls occur. The suite is a controller-call-count oracle, not a controller
latency benchmark.

SchedMD warns that excessive `squeue` RPCs can degrade `slurmctld`; this is why the contract is
expressed as bounded coalesced calls rather than a polling fiber per job:
<https://slurm.schedmd.com/squeue.html>.

## Compatibility and REST

CLI remains the required interpreter. The selected parser is requested explicitly and routed by
version. SchedMD documents data-parser v0.0.43 as introduced in 25.05 and retained until its
scheduled removal in 27.05: <https://slurm.schedmd.com/rest_clients.html#data_parser_lifecycle>.
Synthetic 25.05 and 26.05 fixture families exercise stable v0.0.43 array fields and unknown-state
preservation, but their provenance says `supportClaim: false`; they do not replace captures from
actual binaries and sites.

The actual Slurm 25.05.6 disposable-controller fixture strengthens this to one-family binary
evidence: array submission succeeds and the v0.0.43 compressed pending record is parsed into only
the requested elements. Its worker-registration failure is preserved as a limitation, so neither
execution/accounting nor site support is inferred from it.

The fixture shape follows the official v0.0.43 parser source: array IDs use the bounded no-value
object `{set, infinite, number}` in the default structured representation. The parser also accepts
scalar compatibility values, treats an unset array ID as absent, and matches a compressed
`array_task_string` against only the bounded set of requested elements rather than expanding a
potentially huge range.

No `slurmrestd` interpreter is admitted yet. REST must first demonstrate semantic parity for
acceptance, rejection/unknown, observation, accounting, cancellation, array identity, and bounded
evidence behind the existing `Scheduler` algebra. Endpoint availability alone is insufficient.
No native `libslurm` binding is planned for v1.

## Evidence status

The local laws, cross-version Scala gates, two synthetic major-family fixtures, typed array
staging, partial-failure attribution, and 4,096-job call-count campaign are reproducible. A real
unprivileged-site campaign is still required. The first Trillium probe reached the host but this
non-interactive environment could not complete keyboard-interactive authentication. See
`docs/acceptance/p5-site-scale.md`; P5 must not close until a sanitized successful capture is
reviewed.

## Amendment 2026-07-30 (P8.D3)

This ADR previously said `sacct` parses `JobIDRaw` array suffixes. It does not, and never did:
`docs/compatibility.md` states the opposite — `JobIDRaw` is deliberately not requested, because it
reports the underlying sequential id rather than the array identity the caller submitted — and
`SlurmParsersSuite` asserts the rendered command never contains it. The ADR described behaviour the
code forbids and a test guards against. Corrected to `JobID`.

Reported state is also no longer a single flat vocabulary. `SlurmState` now carries SchedMD base
states only; `COMPLETING`, `REQUEUED`, `REQUEUE_HOLD`, `REQUEUE_FED` and `SPECIAL_EXIT` are
`SlurmStateFlag` values, and `ReportedState` pairs an optional base state with its flags. A surface
that reports only a flag yields an unknown base state rather than a fabricated one, so terminality
is `Indeterminate` instead of a guess.
