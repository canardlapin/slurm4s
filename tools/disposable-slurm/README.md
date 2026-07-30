# Disposable Slurm binary capture

This image builds an explicit official Slurm release over a small single-node Slurm base image.
It is a reproducibility aid for actual-binary parser and submission fixtures, not a substitute for
unprivileged real-site acceptance.

Build with an explicit version:

```shell
docker build \
  --build-arg SLURM_VERSION=25.05.6 \
  --tag slurm4s-disposable:25.05.6 \
  tools/disposable-slurm
```

Record the base-image digest, derived image ID, exact Slurm version, parser registry, command argv,
raw stdout/stderr bytes, and all daemon limitations in fixture provenance. Never convert a pending
controller-only capture into an execution, accounting, or site-support claim.

## Automated captures

`capture-squeue-absence.sh` records how `squeue` reports a job the controller does not know,
including the mixed case where one named job is live and another is unknown. It builds the image,
starts the controller, holds a real array job in the queue, and writes raw streams plus a complete
`provenance.json`:

```shell
tools/disposable-slurm/capture-squeue-absence.sh 25.05.6 v0.0.43
```

It needs a controller only — no worker registers and no job runs — because an unknown job id
produces the same diagnostic either way. It captures evidence and never writes an expected result;
deciding what a fixture asserts stays a reviewed step.

This script has not yet been executed against a running daemon. Treat its first run as part of the
capture, not as a routine invocation.
