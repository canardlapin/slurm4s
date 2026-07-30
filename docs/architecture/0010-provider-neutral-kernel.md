# ADR 0010: Provider-neutral remote-execution kernel

Status: accepted (2026-07-24)

## Context

slurm4s originally defined identifiers, codecs, diagnostic evidence, retry provenance, and
atomic filesystem mechanics because it was their first consumer. Sojourn then reused those types
for scheduler-neutral APIs and imported `AtomicFiles` from the worker package. Package ownership
therefore implied Slurm or worker policy where none existed.

## Decision

`remote-exec-kernel` owns only contracts and mechanics that have the same meaning for every remote
execution provider:

- validated submission, attempt, operation, schema, release, digest, bound, and epoch values;
- input/result codec interfaces and typed codec failures;
- diagnostics, freshness, retry safety, and provider-neutral failure diagnoses;
- worker-release identity;
- atomic filesystem writes, replacement, publish-once, and claim.

The kernel depends on neither slurm4s nor Sojourn. It contains no scheduler states, resource
requests, command execution, spool layout, queues, leases, worker runtime, retry decisions, or
result-attachment policy. Both repositories depend downward on the same kernel artifact.

Atomic publication requires private `CREATE_NEW` temporary files, content force before publication,
and `ATOMIC_MOVE`; unsupported atomic rename is data and there is no copy/delete fallback.
`ContentDigest` is the single digest identity returned by atomic publication.

slurm4s temporarily exposes explicitly documented source-compatibility names in
`slurm4s.core.SharedKernel`. New provider-neutral code must import
`io.github.bbuchsbaum.remoteexec.kernel` directly. These pre-1.0 aliases carry no independent
semantics and may be removed after downstream migration; wire encodings do not include Scala
package names and therefore do not change.

## Consequences

- `sojourn-core` can compile and test without any slurm4s artifact.
- scheduler and backend modules may compose kernel values without translation or duplicate digest
  types.
- provider-specific types remain visibly provider-owned.
- compatibility is checked by existing golden wire fixtures plus cross-repository local
  publication and test gates.
