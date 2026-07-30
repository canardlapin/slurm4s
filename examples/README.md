# Structured workload reference producers

`python/structured_result.py` and `R/structured_result.R` show the language-neutral v1 result
envelope. Both producers calculate output size and SHA-256 digest, serialize canonical JSON, write
to a temporary sibling, and publish the envelope with one atomic rename after output files are
complete.

These helpers are examples, not trusted result authorities. The managed Scala reader still checks
submission identity, attempt epoch, operation, schema, worker release, output presence, sizes, and
digests before returning a typed value.

`python/opaque_modes.py` and `R/opaque_modes.R` demonstrate the three opaque-workload contracts:
exit-only writes no claimed result, declared-output writes an ordinary output for independent
inspection, and structured mode publishes the v1 envelope last.

Compile-checked Scala 3.7.4 usage for local submission, framed SSH submission, declared outputs,
resumable logs, durable recovery, and typed tasks lives in
`modules/examples/src/main/scala/io/github/bbuchsbaum/slurm4s/examples/PublicApiExamples.scala`.
The accompanying guide is `docs/examples/public-api.md`.
