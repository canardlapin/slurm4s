#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd "$script_directory/../.." && pwd)
matrix="$repository_root/docs/acceptance/v1-evidence.md"

[[ -f "$matrix" ]]

for prefix in F Q; do
  if [[ "$prefix" == F ]]; then
    maximum=18
  else
    maximum=10
  fi
  for number in $(seq -w 1 "$maximum"); do
    requirement="$prefix-$number"
    count=$(grep -Ec "^\| $requirement \|" "$matrix" || true)
    if [[ "$count" != 1 ]]; then
      echo "expected exactly one evidence row for $requirement, found $count" >&2
      exit 1
    fi
  done
done

required_paths=(
  modules/core/src/main/scala/io/github/bbuchsbaum/slurm4s/core/FailureDiagnosis.scala
  modules/examples/src/main/scala/io/github/bbuchsbaum/slurm4s/examples/PublicApiExamples.scala
  modules/ssh/src/test/scala/io/github/bbuchsbaum/slurm4s/ssh/SshAgentConformanceSuite.scala
  modules/managed/src/main/scala/io/github/bbuchsbaum/slurm4s/managed/ManagedRequestPolicy.scala
  modules/managed/src/test/scala/io/github/bbuchsbaum/slurm4s/managed/ObservationScaleSuite.scala
  modules/observability/src/main/scala/io/github/bbuchsbaum/slurm4s/observability/SchedulerTelemetry.scala
  modules/worker/src/test/scala/io/github/bbuchsbaum/slurm4s/worker/WorkerRuntimeSuite.scala
  modules/cli/src/test/resources/fixtures/slurm-25.05.6-v0.0.43-actual/provenance.json
  tools/acceptance/real-site-smoke.sh
)

for path in "${required_paths[@]}"; do
  [[ -e "$repository_root/$path" ]] || {
    echo "evidence path is missing: $path" >&2
    exit 1
  }
done

grep -F 'Slurm 26.05 actual-binary capture' "$matrix" >/dev/null
grep -F 'authenticated unprivileged real-site capture' "$matrix" >/dev/null
grep -F 'fail-closed `ManagedRequestPolicy`' "$matrix" >/dev/null
grep -F 'bounded redacted `TelemetryScheduler`' "$matrix" >/dev/null

echo 'v1 evidence matrix checks passed'
