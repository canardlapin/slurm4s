# ADR 0001: Foundation and authority boundaries

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY5W5BGJV9HH39JXYRY8MJC3`

## Decision

slurm4s is published under `io.github.bbuchsbaum` with package root
`io.github.bbuchsbaum.slurm4s`. The build uses sbt 1.11.7, Scala 3.7.4 as its sole publication
and verification baseline, and JDK 17 as the minimum runtime. Changing the Scala minor line is an
explicit compatibility decision rather than an incidental current-release refresh.
sbt 2 is deliberately deferred until its plugin ecosystem has accumulated more production use;
this is a build-only choice and does not affect library semantics.

Dependencies are exact pins. Cats Core is admitted for functional data and validation. Cats
Effect and FS2 are confined to interpreter/application modules. Circe supplies the JSON AST and
parser, while slurm4s owns version negotiation and canonical bytes. MUnit and ScalaCheck are
the initial executable specification tools. sbt-scalafmt remains on the 2.5 line because 2.6 has
raised its minimum sbt version beyond this build baseline; that migration must be intentional.

## Authority table

| Concern | Authority | Non-authoritative convenience |
| --- | --- | --- |
| Allocation and scheduler facts | Bounded Slurm evidence with source and observation time | A cache, inferred log message, or lost connection |
| Managed submission intent | Durable control store and append-only journal | A fiber, SSH channel, `Ref`, `Queue`, or live stream |
| Workload meaning | Payload descriptor, launch bundle, operation identity, result contract | Generated shell text or Slurm job name |
| Typed result | Validated, bounded envelope bound to attempt epoch and schema | Exit zero, stdout, or the Scala type expected by a caller |
| Live delivery | Cursor-backed durable pages exposed as bounded FS2 streams | An unbounded topic or process pipe after disconnect |

Persisted and wire domain values are ordinary immutable data. They never contain `IO`,
`Resource`, `Fiber`, `Ref`, `Queue`, or `Stream`. A service algebra may be parameterized by
`F[_]`; concrete `IO` is selected at application assembly.

## Failure policy

Expected operational outcomes are values: spawn failure, timeout, non-zero command exit,
scheduler rejection, unknown acceptance, stale/failed observation, workload failure, and result
validation failure. Interpreter effects fail only for cancellation, defects, resource exhaustion
inside the client itself, or a failure for which returning a classified value would be dishonest.

Unknown acceptance is a stable state. It may be reconciled using durable submission identity and
scheduler evidence. It never authorizes blind resubmission.

## Compatibility consequences

- Scala 3 opaque types prevent accidental interchange of durable identifiers.
- A major protocol version is a compatibility boundary; minor versions are additive.
- Canonical JSON has sorted keys, no insignificant spaces, UTF-8 encoding, and one trailing LF.
- Unknown fields are retained by generic envelopes, and unknown Slurm values remain inspectable.
- Slurm parser DTOs will live in version-scoped interpreter packages, not in core domain types.
- No `libslurm` binding is required for v1; standard Slurm commands are the baseline interface.
