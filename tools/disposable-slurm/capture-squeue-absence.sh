#!/usr/bin/env bash
# Capture how squeue reports a job it does not know about.
#
# slurm4s treats `slurm_load_jobs error: Invalid job id specified` as absence rather than as a
# broken query, because leaving the queue is how every job ends. Two things about that contract are
# established only from documentation today:
#
#   1. the exit code and exact stderr for a job id the controller does not know; and
#   2. whether a query naming both a live job and an unknown one still returns the live job's
#      queue state, or is refused wholesale.
#
# (2) is the one that matters. SlurmCliScheduler is deliberately correct under either answer -- it
# claims absence only for jobs the response accounts for -- but "correct under either answer" is a
# weaker statement than knowing which answer is true.
#
# This needs a controller only. No worker has to register and no job has to run, because an
# unknown job id produces the same diagnostic either way. That makes it reproducible on a laptop
# rather than something that waits for a site allocation.
#
# Usage:
#   tools/disposable-slurm/capture-squeue-absence.sh [SLURM_VERSION] [DATA_PARSER]

set -euo pipefail

SLURM_VERSION="${1:-25.05.6}"
DATA_PARSER="${2:-v0.0.43}"
IMAGE="slurm4s-disposable:${SLURM_VERSION}"
CONTAINER="slurm4s-absence-capture"
FIXTURE_ID="actual-slurm-${SLURM_VERSION}-${DATA_PARSER}-squeue-absence"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${REPO_ROOT}/modules/cli/src/test/resources/fixtures/${FIXTURE_ID}"
UNKNOWN_JOB_ID=999999

fail() { printf 'capture aborted: %s\n' "$1" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail "docker is not on PATH"
docker info >/dev/null 2>&1 || fail "the docker daemon is not reachable"

cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

printf 'building %s (this compiles Slurm from source and is slow the first time)\n' "$IMAGE"
docker build \
  --build-arg "SLURM_VERSION=${SLURM_VERSION}" \
  --tag "$IMAGE" \
  "${REPO_ROOT}/tools/disposable-slurm"

cleanup
docker run --detach --name "$CONTAINER" "$IMAGE" >/dev/null

printf 'waiting for slurmctld\n'
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" scontrol ping >/dev/null 2>&1; then break; fi
  sleep 2
done
docker exec "$CONTAINER" scontrol ping >/dev/null 2>&1 \
  || fail "slurmctld never became reachable"

# A held array keeps a real job in the queue without needing a worker to register, which is what
# makes the mixed-query case observable at all.
QUEUED_JOB_ID="$(docker exec "$CONTAINER" bash -lc \
  "sbatch --parsable --hold --array=0-1 --job-name=slurm4s-absence --wrap=/usr/bin/true" \
  | tr -d '[:space:]')"
[ -n "$QUEUED_JOB_ID" ] || fail "sbatch did not return a job id"
printf 'queued job %s is held in the queue\n' "$QUEUED_JOB_ID"

mkdir -p "$OUT"

# Capture one command's three observable outputs without interpreting any of them.
capture() {
  local label="$1"; shift
  local status=0
  docker exec "$CONTAINER" "$@" \
    >"${OUT}/${label}.stdout" 2>"${OUT}/${label}.stderr" || status=$?
  printf '%s' "$status" >"${OUT}/${label}.exit"
  printf '  %-16s exit=%s stdout=%sB stderr=%sB\n' \
    "$label" "$status" \
    "$(wc -c <"${OUT}/${label}.stdout" | tr -d ' ')" \
    "$(wc -c <"${OUT}/${label}.stderr" | tr -d ' ')"
}

printf 'capturing\n'
capture unknown-only squeue "--json=${DATA_PARSER}" "--jobs=${UNKNOWN_JOB_ID}"
capture mixed squeue "--json=${DATA_PARSER}" "--jobs=${QUEUED_JOB_ID},${UNKNOWN_JOB_ID}"
capture queued-only squeue "--json=${DATA_PARSER}" "--jobs=${QUEUED_JOB_ID}"

digest() { shasum -a 256 "$1" | cut -d' ' -f1; }
size() { wc -c <"$1" | tr -d ' '; }

stream_json() {
  local label="$1"
  cat <<JSON
    "${label}": {
      "exitCode": $(cat "${OUT}/${label}.exit"),
      "stdout": { "bytes": $(size "${OUT}/${label}.stdout"), "sha256": "$(digest "${OUT}/${label}.stdout")" },
      "stderr": { "bytes": $(size "${OUT}/${label}.stderr"), "sha256": "$(digest "${OUT}/${label}.stderr")" }
    }
JSON
}

cat >"${OUT}/provenance.json" <<JSON
{
  "fixtureSchemaVersion": 1,
  "fixtureId": "${FIXTURE_ID}",
  "source": "disposable-cluster-actual-binary",
  "capturedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "generator": "tools/disposable-slurm/capture-squeue-absence.sh",
  "image": {
    "tag": "${IMAGE}",
    "derivedImageId": "$(docker image inspect --format '{{.Id}}' "$IMAGE")"
  },
  "slurmVersion": "$(docker exec "$CONTAINER" sbatch --version | tr -d '\r')",
  "dataParser": "${DATA_PARSER}",
  "queuedJobId": "${QUEUED_JOB_ID}",
  "unknownJobId": "${UNKNOWN_JOB_ID}",
  "commands": {
    "unknown-only": ["squeue", "--json=${DATA_PARSER}", "--jobs=${UNKNOWN_JOB_ID}"],
    "mixed": ["squeue", "--json=${DATA_PARSER}", "--jobs=${QUEUED_JOB_ID},${UNKNOWN_JOB_ID}"],
    "queued-only": ["squeue", "--json=${DATA_PARSER}", "--jobs=${QUEUED_JOB_ID}"]
  },
  "streams": {
$(stream_json unknown-only),
$(stream_json mixed),
$(stream_json queued-only)
  },
  "locale": "container-default",
  "redactions": [],
  "observed": {
    "controller": "running",
    "worker": "not-registered",
    "queuedJob": "held-and-pending"
  },
  "limitation": "Actual Slurm binaries and controller output for the absence diagnostic only. No worker registered and no job ran, so this is not execution, accounting, or real-site acceptance evidence.",
  "parserCompatibilityClaim": true,
  "siteSupportClaim": false,
  "supportClaim": false
}
JSON

printf '\ncaptured into %s\n' "$OUT"
printf 'the answer that matters is in mixed.stdout:\n'
printf '  non-empty  -> squeue returns surviving queue state alongside the diagnostic\n'
printf '  empty      -> squeue refuses the whole query, and only single-job absence is knowable\n'
