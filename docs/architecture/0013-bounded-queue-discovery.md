# ADR 0013: Bounded queue discovery is a separate Slurm capability

- Status: accepted
- Date: 2026-08-13

## Context

`Scheduler[F]` controls jobs whose `JobRef`s the caller already knows. A coding-agent product also
needs to discover the user's active jobs after restart or when another process submitted them.
That discovery is Slurm-specific, but it is not a lifecycle requirement for every scheduler
interpreter.

`squeue` offers useful server-side filters but no stable snapshot cursor. Offset pagination would
therefore suggest continuity that Slurm does not provide: page two could be drawn from a different
queue state than page one. An unbounded `squeue` result is also unsuitable for an agent protocol.

## Decision

slurm4s owns a separate capability:

```scala
trait QueueReader[F[_]]:
  def listJobs(query: QueueQuery, page: Page):
    F[SchedulerQueryResult[QueuePage]]
```

Protocol v1 fixes `QueueQuery` to the Unix user running the remote agent. Optional job-name,
partition, and active-state filters are bounded and canonicalized. The command is a fixed argv:
`squeue --json=<parser> --me --array`, followed only by validated filter arguments. It never uses a
shell and never expands the query to all cluster users.

`Page` is a positive response item ceiling, not an offset or continuation token. One invocation is
parsed, sorted deterministically by reported cluster, base job id, and array index, and narrowed to
the requested maximum. `QueuePageCompleteness.Truncated(totalMatched)` says explicitly that the
same captured response contained more matches. A caller obtains a smaller result by refining the
query; it does not pretend to continue an earlier snapshot.

Queue rows are compact. Command evidence and freshness occur once on the enclosing page rather
than once per job. Empty, invocation failure, and parse failure remain distinct query outcomes.
Grouped array expressions are refused because the command requested one row per array element.

The remote feature is additive and negotiated as `queue-listing`. The handshake also advertises a
conservative maximum `Page` derived from the negotiated frame size. Both client and agent reject a
larger request before running `squeue`. The frame encoder remains the final byte authority and
turns an unexpectedly large encoded response into a typed protocol refusal rather than a broken
SSH session.

## Consequences

- Existing `Scheduler[F]` interpreters are not forced to claim queue enumeration.
- `SlurmCliScheduler` and `SlurmLocal` implement both capabilities; `RemoteSlurm` exposes queue
  discovery as `remote.queue` after feature negotiation.
- The API cannot enumerate other users or provide cluster-administrator inventory in v1.
- Repeated listing is caller policy. slurm4s does not encourage a tight `squeue` polling loop.
- Stable snapshot pagination remains unsupported until a Slurm surface can actually provide it.

The command and filter behavior follows SchedMD's
[`squeue` documentation](https://slurm.schedmd.com/squeue.html).
