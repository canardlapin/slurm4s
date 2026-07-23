#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: real-site-smoke.sh --output DIR [--data-parser VERSION] [--submit] [--sbatch-arg ARG ...] [--poll-attempts N] [--poll-interval-seconds N]" >&2
  exit 64
}

output_directory=""
data_parser="v0.0.43"
submit_job=false
poll_attempts=60
poll_interval_seconds=2
sbatch_arguments=()

while (($# > 0)); do
  case "$1" in
    --output)
      (($# >= 2)) || usage
      output_directory=$2
      shift 2
      ;;
    --data-parser)
      (($# >= 2)) || usage
      data_parser=$2
      shift 2
      ;;
    --submit)
      submit_job=true
      shift
      ;;
    --sbatch-arg)
      (($# >= 2)) || usage
      sbatch_arguments+=("$2")
      shift 2
      ;;
    --sbatch-arg=*)
      sbatch_arguments+=("${1#--sbatch-arg=}")
      shift
      ;;
    --poll-attempts)
      (($# >= 2)) || usage
      poll_attempts=$2
      shift 2
      ;;
    --poll-interval-seconds)
      (($# >= 2)) || usage
      poll_interval_seconds=$2
      shift 2
      ;;
    *) usage ;;
  esac
done

[[ -n "$output_directory" ]] || usage
[[ "$poll_attempts" =~ ^[1-9][0-9]*$ ]] || usage
[[ "$poll_interval_seconds" =~ ^[0-9]+$ ]] || usage
[[ ! -e "$output_directory" ]] || {
  echo "refusing to overwrite existing output: $output_directory" >&2
  exit 73
}

if command -v sha256sum >/dev/null 2>&1; then
  hash_tool=sha256sum
elif command -v shasum >/dev/null 2>&1; then
  hash_tool=shasum
elif command -v openssl >/dev/null 2>&1; then
  hash_tool=openssl
else
  echo "one of sha256sum, shasum, or openssl is required for exact evidence manifests" >&2
  exit 69
fi

export LC_ALL=C
umask 077
mkdir -p "$output_directory"
: >"$output_directory/commands.argv.tsv"

record_argv() {
  local label=$1
  shift
  printf '%s' "$label" >>"$output_directory/commands.argv.tsv"
  for argument in "$@"; do
    printf '\t%q' "$argument" >>"$output_directory/commands.argv.tsv"
  done
  printf '\n' >>"$output_directory/commands.argv.tsv"
}

capture_command() {
  local label=$1
  shift
  local stdout_path="$output_directory/$label.stdout"
  local stderr_path="$output_directory/$label.stderr"
  local status_path="$output_directory/$label.exit-code"
  local command_status

  record_argv "$label" "$@"
  if "$@" >"$stdout_path" 2>"$stderr_path"; then
    command_status=0
  else
    command_status=$?
  fi
  printf '%s\n' "$command_status" >"$status_path"
  return "$command_status"
}

sha256_file() {
  local path=$1
  local digest_line
  case "$hash_tool" in
    sha256sum)
      digest_line=$(sha256sum "$path")
      printf '%s\n' "${digest_line%% *}"
      ;;
    shasum)
      digest_line=$(shasum -a 256 "$path")
      printf '%s\n' "${digest_line%% *}"
      ;;
    openssl)
      digest_line=$(openssl dgst -sha256 "$path")
      printf '%s\n' "${digest_line##* }"
      ;;
  esac
}

finalize_capture() {
  local manifest_path="$output_directory/manifest.tsv"
  local temporary_manifest="$output_directory/.manifest.tsv.tmp"
  local path
  local name
  local bytes
  local digest

  printf 'path\tbytes\tsha256\n' >"$temporary_manifest"
  for path in "$output_directory"/*; do
    [[ -f "$path" ]] || continue
    name=${path##*/}
    [[ "$name" != "manifest.tsv" ]] || continue
    bytes=$(wc -c <"$path")
    bytes=${bytes//[[:space:]]/}
    digest=$(sha256_file "$path")
    printf '%s\t%s\t%s\n' "$name" "$bytes" "$digest" >>"$temporary_manifest"
  done
  mv "$temporary_manifest" "$manifest_path"
}

date -u +%Y-%m-%dT%H:%M:%SZ >"$output_directory/captured-at.txt"
hostname >"$output_directory/host.txt"
printf 'LC_ALL=%s\n' "$LC_ALL" >"$output_directory/environment.txt"
printf '%s\n' "$hash_tool" >"$output_directory/hash-tool.txt"

for command_name in sbatch squeue sacct scontrol scancel slurmrestd; do
  if command -v "$command_name" >/dev/null 2>&1; then
    printf '%s=present\n' "$command_name"
  else
    printf '%s=absent\n' "$command_name"
  fi
done >"$output_directory/commands.txt"

if ! capture_command slurm-version sbatch --version; then
  command_status=$(<"$output_directory/slurm-version.exit-code")
  finalize_capture
  exit "$command_status"
fi
capture_command data-parsers squeue --json=list || true

config_pattern='^[[:space:]]*(ClusterName|MaxArraySize|AccountingStorageType|SlurmctldParameters|AuthAltTypes)[[:space:]]*='
record_argv config-scontrol scontrol show config
record_argv config-filter grep -E "$config_pattern"
set +e
scontrol show config 2>"$output_directory/config.stderr" \
  | grep -E "$config_pattern" >"$output_directory/config.stdout"
config_pipeline_status=("${PIPESTATUS[@]}")
set -e
printf 'scontrol\t%s\ngrep\t%s\n' \
  "${config_pipeline_status[0]}" \
  "${config_pipeline_status[1]}" >"$output_directory/config.exit-codes.tsv"

if capture_command accounting-probe \
  sacct --noheader --starttime=now --endtime=now --format=JobIDRaw; then
  accounting_available=true
  echo success >"$output_directory/accounting.status"
else
  accounting_available=false
  echo failed >"$output_directory/accounting.status"
fi

if [[ "$submit_job" != true ]]; then
  finalize_capture
  echo "capture complete: inspect and redact the private output directory before importing evidence"
  exit 0
fi

stdout_template="$output_directory/smoke-%A_%a.stdout"
stderr_template="$output_directory/smoke-%A_%a.stderr"
submission_argv=(sbatch)
if ((${#sbatch_arguments[@]} > 0)); then
  submission_argv+=("${sbatch_arguments[@]}")
fi
submission_argv+=(
  --parsable
  --job-name=scala-slurm-smoke
  "--output=$stdout_template"
  "--error=$stderr_template"
  --time=1
  --ntasks=1
  --cpus-per-task=1
  --array=0-1%1
  --wrap=/usr/bin/true
)
record_argv submission "${submission_argv[@]}"
if submission=$("${submission_argv[@]}" 2>"$output_directory/submission.stderr"); then
  submission_status=0
else
  submission_status=$?
fi
printf '%s\n' "$submission_status" >"$output_directory/submission.exit-code"
if ((submission_status != 0)); then
  echo rejected >"$output_directory/submission.status"
  finalize_capture
  exit "$submission_status"
fi

printf '%s\n' "$submission" >"$output_directory/submission.stdout"
echo accepted >"$output_directory/submission.status"
job_id=${submission%%;*}
if [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
  echo malformed-binding >"$output_directory/submission.status"
  finalize_capture
  exit 65
fi

capture_command initial-squeue \
  squeue "--json=$data_parser" "--jobs=$job_id" || true

: >"$output_directory/poll-squeue.stdout"
: >"$output_directory/poll-squeue.stderr"
: >"$output_directory/poll-squeue.status.tsv"
record_argv poll-squeue squeue --noheader '--format=%i|%T|%R' "--jobs=$job_id"
queue_empty=false
attempt=1
while ((attempt <= poll_attempts)); do
  if poll_output=$(
    squeue --noheader '--format=%i|%T|%R' "--jobs=$job_id" \
      2>>"$output_directory/poll-squeue.stderr"
  ); then
    poll_status=0
  else
    poll_status=$?
  fi
  if [[ -n "$poll_output" ]]; then
    printf -- '--- attempt %s ---\n%s\n' "$attempt" "$poll_output" \
      >>"$output_directory/poll-squeue.stdout"
    poll_result=active
  elif ((poll_status == 0)); then
    poll_result=empty
    queue_empty=true
  else
    poll_result=unavailable
  fi
  printf '%s\t%s\t%s\n' "$attempt" "$poll_status" "$poll_result" \
    >>"$output_directory/poll-squeue.status.tsv"
  [[ "$queue_empty" == true ]] && break
  ((attempt == poll_attempts)) || sleep "$poll_interval_seconds"
  attempt=$((attempt + 1))
done

if [[ "$queue_empty" == true ]]; then
  echo empty >"$output_directory/queue-completion.status"
else
  echo limit-reached >"$output_directory/queue-completion.status"
fi

capture_command final-squeue \
  squeue "--json=$data_parser" "--jobs=$job_id" || true

accounting_complete=false
if [[ "$accounting_available" == true ]]; then
  : >"$output_directory/final-sacct.status.tsv"
  record_argv final-sacct \
    sacct --noheader --parsable2 --allocations \
    --format=JobIDRaw,State,ExitCode,Reason "--jobs=$job_id"
  attempt=1
  while ((attempt <= poll_attempts)); do
    if sacct --noheader --parsable2 --allocations \
      --format=JobIDRaw,State,ExitCode,Reason "--jobs=$job_id" \
      >"$output_directory/final-sacct.stdout" \
      2>"$output_directory/final-sacct.stderr"; then
      accounting_status=0
    else
      accounting_status=$?
    fi
    printf '%s\t%s\n' "$attempt" "$accounting_status" \
      >>"$output_directory/final-sacct.status.tsv"
    printf '%s\n' "$accounting_status" >"$output_directory/final-sacct.exit-code"
    if ((accounting_status == 0)) \
      && grep -Eq "^[[:space:]]*${job_id}_0\\|" "$output_directory/final-sacct.stdout" \
      && grep -Eq "^[[:space:]]*${job_id}_1\\|" "$output_directory/final-sacct.stdout"; then
      accounting_complete=true
      break
    fi
    ((attempt == poll_attempts)) || sleep "$poll_interval_seconds"
    attempt=$((attempt + 1))
  done
else
  : >"$output_directory/final-sacct.stdout"
  : >"$output_directory/final-sacct.stderr"
  printf 'unavailable\n' >"$output_directory/final-sacct.exit-code"
fi

if [[ "$accounting_complete" == true ]]; then
  echo complete >"$output_directory/accounting-completion.status"
else
  echo incomplete >"$output_directory/accounting-completion.status"
fi

finalize_capture
if [[ "$queue_empty" != true || "$accounting_complete" != true ]]; then
  echo "capture incomplete: job remained active or accounting did not expose both array elements" >&2
  exit 75
fi

echo "capture complete: inspect and redact the private output directory before importing evidence"
