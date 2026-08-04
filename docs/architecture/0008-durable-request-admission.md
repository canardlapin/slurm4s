# ADR 0008: Durable request environment admission is fail closed

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY6F28896YKQG44R0BW9TZJV`

## Decision

Raw `Scheduler[F]` submission may supply process environment values to a local interpreter.
Durable managed submission is stricter because it canonicalizes the request into a checksummed
journal projection. `ManagedController` therefore uses
`ManagedRequestPolicy.rejectEnvironmentValues` by
default. A nonempty environment is rejected before canonical bytes, intent, outbox, or journal
state exist.

An application may explicitly construct
`ManagedRequestPolicy.publicEnvironment(Set("NAME", ...))` for values it certifies as non-secret
configuration. Names use portable process-variable syntax, the allowlist and each value are
bounded, null/NUL values are rejected, and undeclared names fail admission. Rejection diagnostics
contain names and stable reason codes but never values.

The policy object is runtime configuration and is not journal authority. Once a request is
admitted, journal reopen validates the canonical bytes, digest, schema, and request decoder
without applying a possibly changed current admission policy. Otherwise a policy deployment
change could make previously committed history unreadable.

## Scope

This boundary prevents accidental persistence of environment credentials through the managed API
by default. It does not inspect arbitrary opaque script bytes, arguments, staged files, or declared
public values for hidden secrets. Callers must not embed credentials in those workload artifacts.
SSH authentication remains owned by system OpenSSH; slurm4s does not read or journal its
private keys, agents, or authentication tokens.

Secret references and target-side secret resolution may be added as a separate typed capability.
They must never lower a resolved secret into `CanonicalRequest`, agent protocol evidence,
diagnostics, or operational telemetry.

## Executable evidence

Managed policy tests prove:

- default admission rejects a secret-like environment binding with an empty store and no scheduler
  invocation;
- diagnostics expose the rejected name but not its value;
- an explicit validated public-name policy admits only its allowlist;
- undeclared values fail without state mutation;
- canonical replay accepts bytes that were admitted by the policy active at creation; and
- invalid environment names cannot construct a public policy.
