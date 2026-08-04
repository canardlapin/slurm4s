# Acceptance tools

Run the complete reproducible local v1 evidence gate with:

```shell
tools/acceptance/v1-local-gate.sh
```

It validates the requirement matrix, shell harness, Scala 3.3.8 formatting/tests, and worker
package. It does not contact a cluster and cannot satisfy the external P5 evidence gates. Set
`SLURM4S_CACHE_ROOT` when sbt and Coursier need a specific writable cache root. The audited
status is recorded in `docs/acceptance/v1-evidence.md`.

## Real-site capture

`real-site-smoke.sh` captures low-risk Slurm capability evidence and, only with `--submit`, runs a
minimal two-element array. It creates a new private output directory and refuses to overwrite one.

Every command records its shell-escaped argv and exit status. Submission mode captures structured
queue JSON immediately after acceptance, polls with a fixed upper bound, waits independently for
both array elements in accounting, and returns exit 75 when it cannot prove a stable terminal
capture. `manifest.tsv` records the byte count and SHA-256 digest of every captured file. Polling
defaults to 60 attempts at two-second intervals; use `--poll-attempts` and
`--poll-interval-seconds` only when the site's policy requires a different bound.

The capture is not automatically safe to commit. Review and redact actor names, home/work paths,
accounts, private cluster identifiers, and workload content. Do not erase structural evidence:
retain argv, versions, parser names, controller limits, exit codes, byte counts, digests, and the
fact that a value was redacted.

See `docs/acceptance/p5-site-scale.md` for the exact acceptance sequence.

Run the deterministic harness test before taking a site capture:

```shell
tools/acceptance/test-real-site-smoke.sh
```
