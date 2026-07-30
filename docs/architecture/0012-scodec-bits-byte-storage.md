# ADR 0012: Use scodec-bits for owned byte storage

- Status: accepted
- Date: 2026-07-30
- Mote: `bd-01KYNA5E517YB38ZAAP970X8YY`

## Decision

Kernel, core, and protocol depend directly on `scodec-bits` 1.2.4. The representation work tracked
by `bd-01KYNA5EAA733KTPSK7M1V1JWZ` may therefore use `scodec.bits.ByteVector` for immutable byte
storage shared across these modules.

The direct dependency does not add a new library version to the build. FS2 3.13.0 already selects
`scodec-bits` 1.2.4 for local, SSH, agent, managed, and worker modules. The direct declarations make
the dependency of the lower modules explicit and keep its version under the same exact-pin policy
as Cats, Circe, and FS2.

## Alternative considered

The alternative was to retain `Vector[Byte]`. That would avoid a direct dependency in kernel,
core, and protocol, but each boxed `Byte` would remain a separate generic collection element.
Byte-oriented values would continue to allocate during conversions to arrays, buffers, and FS2
chunks. Accepting those costs would also leave each interpreter to choose its own conversion
boundary.

## Consequences

- Public byte-bearing values may migrate from `Vector[Byte]` to `ByteVector`. Each such change is
  still a source and binary compatibility change and requires its own migration review.
- Kernel, core, and protocol may use `ByteVector`, but they do not gain an FS2 dependency.
- `scodec-bits` remains an immutable storage dependency. This decision does not adopt scodec
  codecs or introduce a second serialization protocol.
- Future version changes to `scodec-bits` are explicit dependency updates and must pass the normal
  compatibility and test gates.
