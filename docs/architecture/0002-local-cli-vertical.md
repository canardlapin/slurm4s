# ADR 0002: Truthful local Slurm CLI vertical

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY5W5BN9EG8RM5P3PJHNWVE0`

## Decision

The v1 baseline uses standard Slurm commands. Each command is a closed `SlurmExecutable` value
and an argument vector; slurm4s never renders these values as a shell command. The local
runner maps that closed value to a configured executable path, disables inherited environment,
applies an allowlist, drains stdout and stderr concurrently, retains each stream independently up
to a byte limit capped by a local safety maximum, and kills the process through its resource
finalizer at the deadline. Log page allocation is capped independently.

The initial command contract is:

| Operation | Command contract | Meaning of success |
| --- | --- | --- |
| submit | `sbatch --parsable ... script [args]` | a strictly parsed job binding; timeout or unreadable zero-exit response is `AcceptanceUnknown` |
| active observation | `squeue --json=v0.0.43 --jobs=...` | current observations from the registered v0.0.43 codec; an empty jobs array is `Empty` |
| accounting | `sacct --noheader --parsable2 --allocations --format=JobIDRaw,State,ExitCode,Reason --jobs=...` | strict four-field terminal evidence |
| focused diagnostic | `scontrol --oneliner show job ID` | inspectable key/value evidence; parse failure is not job failure |
| cancellation | `scancel ID` | Slurm acknowledged the request; it does not prove terminal cancellation |

SchedMD documents `squeue --json=<data_parser>` and warns clients to request an explicit parser
because the default may change. Its data-parser lifecycle currently lists v0.0.43 for Slurm 25.05
through its scheduled removal in 27.05. The implementation therefore has a registry keyed by the
explicit parser version; an arbitrary configured version cannot silently reuse the v0.0.43 codec.
See the upstream [squeue manual](https://slurm.schedmd.com/squeue.html) and
[REST client/data-parser guide](https://slurm.schedmd.com/rest_clients.html).

Accounting deliberately begins with a strict `--parsable2` field set. The
[sacct manual](https://slurm.schedmd.com/sacct.html) specifies that `--parsable2` omits a trailing
delimiter and that a job filter searches from epoch by default. The implementation does not add a
broader time scan. JSON accounting can be enabled only when a versioned codec has its own fixtures
and conformance evidence.

An accounting row is not automatically terminal: pending, running, completing, and unknown states
carry no `WorkloadOutcome`. A completed row with a non-zero exit is a failure value. Partial
multi-job responses retain the missing job references rather than silently shrinking the query.

The [scancel manual](https://slurm.schedmd.com/scancel.html) describes signaling/cancellation and
its authorization failures. An exit-zero invocation is consequently an acknowledgement, not a
claim that the workload has stopped.

## Opaque scripts and logs

Inline and staged-local scripts are copied to a content-addressed file below a private `0700`
attempt directory. Existing-remote paths are explicit and are passed as one argv element. Stdout
and stderr always have separate paths in that directory.

Logs are read as bounded pages from an exact byte offset. A cursor carries file identity as well as
offset, so replacement or truncation produces `CursorInvalid` rather than silent replay. Absence is
`WaitingForFile`, which is normal while a job is pending. The FS2 `follow` stream is derived from
these pages, polls at EOF, and owns no durable truth; cancellation stops its wait promptly.

Log access is confined to a configured root, rejects symlinks, and does not become a general file
browser. The later remote agent will enforce the same contract against attempt-scoped locators.

## Executable evidence

The deterministic vertical test covers:

1. private preparation of an inline script;
2. accepted `sbatch --parsable` binding;
3. pending structured observation;
4. `WaitingForFile` and bounded log paging;
5. strict accounting diagnosis of out-of-memory;
6. focused `scontrol` evidence and asynchronous cancellation acknowledgement.

Separate tests keep rejection, invocation failure, parse failure, empty query, stale observation,
and workload failure disjoint. Process timeout tests prove bounded dual-stream capture and verify
that resource finalization prevents late work. Log-follow cancellation is an executable stream
finalization law.
