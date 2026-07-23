# Disposable Slurm binary capture

This image builds an explicit official Slurm release over a small single-node Slurm base image.
It is a reproducibility aid for actual-binary parser and submission fixtures, not a substitute for
unprivileged real-site acceptance.

Build with an explicit version:

```shell
docker build \
  --build-arg SLURM_VERSION=25.05.6 \
  --tag scala-slurm-disposable:25.05.6 \
  tools/disposable-slurm
```

Record the base-image digest, derived image ID, exact Slurm version, parser registry, command argv,
raw stdout/stderr bytes, and all daemon limitations in fixture provenance. Never convert a pending
controller-only capture into an execution, accounting, or site-support claim.
