#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd "$script_directory/../.." && pwd)
cache_root=${SLURM4S_CACHE_ROOT:-${TMPDIR:-/tmp}/slurm4s-v1-gate}

mkdir -p "$cache_root/coursier" "$cache_root/sbt" "$cache_root/ivy"

bash -n "$script_directory/real-site-smoke.sh"
bash -n "$script_directory/test-real-site-smoke.sh"
bash -n "$script_directory/test-v1-evidence-matrix.sh"
bash "$script_directory/test-v1-evidence-matrix.sh"
bash "$script_directory/test-real-site-smoke.sh"

cd "$repository_root"
env \
  COURSIER_CACHE="$cache_root/coursier" \
  sbt \
  -sbt-dir "$cache_root/sbt" \
  -ivy "$cache_root/ivy" \
  checkFormatting \
  test \
  worker/packageBin

echo 'slurm4s v1 local evidence gate passed'
