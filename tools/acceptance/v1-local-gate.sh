#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd "$script_directory/../.." && pwd)
cache_root=${SLURM4S_CACHE_ROOT:-${TMPDIR:-/tmp}/slurm4s-v1-gate}

fail() {
  printf 'slurm4s v1 local evidence gate: %s\n' "$1" >&2
  exit 1
}

[[ -n "${JAVA_HOME:-}" ]] ||
  fail "JAVA_HOME must point to the JDK 17 installation that sbt will use"
java_launcher="$JAVA_HOME/bin/java"
[[ -n "$java_launcher" && -x "$java_launcher" ]] || fail "no Java launcher is available"

java_specification_version=$(
  "$java_launcher" -XshowSettings:properties -version 2>&1 |
    awk -F'= ' '/^[[:space:]]*java\.specification\.version =/ { print $2; exit }'
)
[[ "$java_specification_version" == "17" ]] ||
  fail "JDK 17 is required, but $java_launcher reports specification version ${java_specification_version:-unknown}"

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
  +test \
  +publishLocal \
  worker/packageBin

echo 'slurm4s v1 local evidence gate passed'
