# P5 site-scale acceptance record

- Date: 2026-07-22
- Work item: `bd-01KY5W5C9N1RN8D1PFKQZ1D6EE`
- Status: **incomplete — real-site authentication required**

## Reproducible evidence

| Contract | Evidence | Result |
| --- | --- | --- |
| Site policy is advisory and accumulates failures | `SiteProfileSuite` | Pass |
| Requested and effective specifications both survive | `SiteProfileSuite` | Pass |
| Site settings lower to argv without environment-value leakage | `SlurmCommandsSuite` | Pass |
| Array elements retain attempt/log/result identity | `JobArraySuite`, `WorkerRuntimeSuite` | Pass |
| Partial failures do not alias sibling elements | `SlurmParsersSuite` | Pass |
| Array requests survive agent wire transport and legacy absence | `AgentDomainJsonSuite` | Pass |
| Element cancellation/focused probes use `jobId_index` | `SlurmCommandsSuite` | Pass |
| 4,096 active jobs use 16 calls at batch 256 | `ObservationScaleSuite` | Pass |
| Later jobs are not starved by bounded selection | `ObservationCoordinatorSuite` | Pass |
| Slurm 25.05 and 26.05 v0.0.43 parser shapes | versioned synthetic fixtures | Shape-only; not a support claim |
| Slurm 25.05.6 actual binary, parser registry, submission, and queue JSON | disposable controller capture | Pass for parser/submission; worker did not register |
| Slurm 26.05 actual binary | second disposable build | Blocked by execution-environment approval |
| Real unprivileged site, exact version and policy | Trillium | Blocked before discovery |
| CLI/REST differential parity | no authenticated REST-enabled site | Not run; optional interpreter not admitted |

The 4,096-job campaign is deterministic call-count evidence. It does not simulate controller
latency, federation behavior, site rate limits, or delayed accounting.

## Disposable actual-binary attempt

An image derived from `nathanhess/slurm:base-root` built official Slurm 25.05.6 source and reported
the v0.0.43 data-parser in its runtime registry. Its controller accepted
`--array=0-1%1 --wrap=/usr/bin/true`; exact `squeue --json=v0.0.43` bytes, version, parser list,
digests, and limitations are stored in
`modules/cli/src/test/resources/fixtures/slurm-25.05.6-v0.0.43-actual/`.

The container's worker daemon did not register, leaving the array pending for resources. The
environment also denied the requested second 26.05 image build. Accordingly this proves one-family
actual-binary parser/submission behavior only. It does not satisfy the two-major-family gate or the
real unprivileged-site gate.

On 2026-07-23 an explicitly authorized 26.05.2 build retry passed the execution-approval boundary
but never reached Docker's build engine. The client remained silent for sixteen minutes; cancelling
it reported a blocked request to the local Docker socket's `_ping` endpoint. A separate bounded
five-second `_ping` against the existing socket also timed out with zero response bytes, even though
the Docker backend processes and socket were present. No image was produced or inspected, so this
retry contributes infrastructure diagnostics only and does not advance the two-family support
claim. Docker Desktop was not restarted because doing so could disrupt unrelated user containers.

## Real-site attempt

A read-only SSH probe was attempted against the configured Trillium host. The network connection
reached the host, but authentication ended with:

```text
Permission denied (keyboard-interactive,hostbased).
```

The actor name is intentionally omitted. No Slurm command ran, no job was submitted, and no
version, cluster, account, partition, or policy fact was learned. This record must not be treated
as site acceptance or site incompatibility.

The same read-only `sbatch --version` probe was retried on 2026-07-23 with batch-mode authentication
and a bounded connection timeout. Authentication failed in the same way before the command ran. No
submission was attempted.

## Required rerun

After establishing an authenticated shell on the site, copy or invoke:

```shell
tools/acceptance/real-site-smoke.sh \
  --output "$PWD/slurm4s-site-capture" \
  --data-parser v0.0.43
```

That first mode is read-only. Review its private output. Then run the unprivileged two-element
array smoke with the account/partition/QoS arguments required by the site:

```shell
tools/acceptance/real-site-smoke.sh \
  --output "$PWD/slurm4s-site-smoke" \
  --data-parser v0.0.43 \
  --submit \
  --sbatch-arg=--account=REDACT_BEFORE_IMPORT \
  --sbatch-arg=--partition=REDACT_BEFORE_IMPORT
```

The harness refuses to overwrite an existing directory. Submission is opt-in. The smoke requests
one CPU, one task, one minute, and two serial array elements running `/usr/bin/true`. Inspect and
redact usernames, paths, accounts, and private cluster details before copying evidence into the
repository. Preserve command argv, Slurm version, parser list, `MaxArraySize`, accounting status,
exit codes, byte counts, digests, and any limitation that affected the run.

The harness captures structured queue evidence immediately after acceptance, then uses at most 60
polls at a two-second cadence and waits independently for both array elements in accounting. It
emits exact argv/status records plus `manifest.tsv` byte counts and SHA-256 digests. Exit 75 means
the bounded window ended without a stable empty queue and both accounting rows; such output is
diagnostic evidence, not a successful acceptance receipt.

## Acceptance-harness verification

`tools/acceptance/test-real-site-smoke.sh` supplies deterministic fake Slurm commands and verifies
four evidence boundaries: read-only discovery, accepted array completion, incomplete accounting,
and scheduler rejection. Each case re-hashes every manifest entry; incomplete accounting must
return exit 75 and rejection must retain the original nonzero `sbatch` status. On 2026-07-22 the
shell syntax checks and harness test passed, as did formatting, 128 tests on Scala 3.3.8, 128 tests
on Scala 3.8.4, and worker packaging on both versions.

Those dual-lane results are historical evidence from before the compiler policy changed. After the
user selected Scala 3.7, the build, compatibility policy, and PRD were moved to the final 3.7.4
patch. Formatting, all 128 tests, and worker packaging passed on Scala 3.7.4 on 2026-07-22.

P5 can close only after the sanitized capture demonstrates actual array attribution and bounded
observation on real Slurm, or after the acceptance contract is explicitly revised in Mote.
