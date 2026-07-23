#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
harness="$script_directory/real-site-smoke.sh"
test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"

cat >"$fake_bin/fake-slurm" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail

command_name=${0##*/}
state_directory=${FAKE_SLURM_STATE:?}
mkdir -p "$state_directory"

has_argument() {
  local expected=$1
  shift
  local argument
  for argument in "$@"; do
    [[ "$argument" == "$expected" ]] && return 0
  done
  return 1
}

case "$command_name" in
  sbatch)
    if has_argument --version "$@"; then
      echo 'slurm 25.05.6'
    elif [[ "${FAKE_SBATCH_REJECT:-false}" == true ]]; then
      echo 'sbatch: error: invalid account' >&2
      exit 1
    else
      echo '42;cluster'
    fi
    ;;
  squeue)
    if has_argument --json=list "$@"; then
      printf 'Possible data_parser plugins:\ndata_parser/v0.0.43\n'
    elif has_argument --noheader "$@"; then
      counter_path="$state_directory/poll-count"
      poll_count=0
      [[ ! -f "$counter_path" ]] || poll_count=$(<"$counter_path")
      poll_count=$((poll_count + 1))
      printf '%s\n' "$poll_count" >"$counter_path"
      if ((poll_count == 1)); then
        echo '42_[0-1%1]|PENDING|Resources'
      fi
    else
      counter_path="$state_directory/json-count"
      json_count=0
      [[ ! -f "$counter_path" ]] || json_count=$(<"$counter_path")
      json_count=$((json_count + 1))
      printf '%s\n' "$json_count" >"$counter_path"
      if ((json_count == 1)); then
        printf '%s\n' '{"jobs":[{"job_id":42,"array_job_id":{"set":true,"infinite":false,"number":42},"array_task_id":{"set":false,"infinite":false,"number":0},"array_task_string":"0-1%1","job_state":["PENDING"],"state_reason":"Resources"}],"meta":{"slurm":{"release":"25.05.6","cluster":"cluster"}}}'
      else
        printf '%s\n' '{"jobs":[],"meta":{"slurm":{"release":"25.05.6","cluster":"cluster"}}}'
      fi
    fi
    ;;
  sacct)
    if has_argument --jobs=42 "$@"; then
      if [[ "${FAKE_SACCT_INCOMPLETE:-false}" == true ]]; then
        printf '42_0|COMPLETED|0:0|None\n'
      else
        printf '42_0|COMPLETED|0:0|None\n42_1|COMPLETED|0:0|None\n'
      fi
    fi
    ;;
  scontrol)
    printf 'ClusterName = cluster\nMaxArraySize = 1001\nAccountingStorageType = accounting_storage/slurmdbd\n'
    ;;
  scancel)
    ;;
  *)
    echo "unexpected fake command: $command_name" >&2
    exit 70
    ;;
esac
FAKE
chmod +x "$fake_bin/fake-slurm"
for command_name in sbatch squeue sacct scontrol scancel; do
  ln -s fake-slurm "$fake_bin/$command_name"
done

sha256_file() {
  local path=$1
  local digest_line
  if command -v sha256sum >/dev/null 2>&1; then
    digest_line=$(sha256sum "$path")
    printf '%s\n' "${digest_line%% *}"
  elif command -v shasum >/dev/null 2>&1; then
    digest_line=$(shasum -a 256 "$path")
    printf '%s\n' "${digest_line%% *}"
  else
    digest_line=$(openssl dgst -sha256 "$path")
    printf '%s\n' "${digest_line##* }"
  fi
}

verify_manifest() {
  local directory=$1
  local name
  local expected_bytes
  local expected_digest
  local actual_bytes
  local actual_digest
  while IFS=$'\t' read -r name expected_bytes expected_digest; do
    [[ "$name" != path ]] || continue
    [[ -f "$directory/$name" ]]
    actual_bytes=$(wc -c <"$directory/$name")
    actual_bytes=${actual_bytes//[[:space:]]/}
    actual_digest=$(sha256_file "$directory/$name")
    [[ "$actual_bytes" == "$expected_bytes" ]]
    [[ "$actual_digest" == "$expected_digest" ]]
  done <"$directory/manifest.tsv"
}

read_only_capture="$test_root/read-only"
PATH="$fake_bin:$PATH" FAKE_SLURM_STATE="$test_root/read-only-state" \
  "$harness" --output "$read_only_capture"
grep -Fx 'slurm 25.05.6' "$read_only_capture/slurm-version.stdout" >/dev/null
grep -Fx 'data_parser/v0.0.43' "$read_only_capture/data-parsers.stdout" >/dev/null
grep -Fx $'accounting-probe\tsacct\t--noheader\t--starttime=now\t--endtime=now\t--format=JobIDRaw' \
  "$read_only_capture/commands.argv.tsv" >/dev/null
[[ ! -e "$read_only_capture/submission.status" ]]
verify_manifest "$read_only_capture"

submitted_capture="$test_root/submitted"
PATH="$fake_bin:$PATH" FAKE_SLURM_STATE="$test_root/submitted-state" \
  "$harness" \
  --output "$submitted_capture" \
  --submit \
  --poll-attempts 3 \
  --poll-interval-seconds 0 \
  --sbatch-arg=--account=test-account
grep -Fx accepted "$submitted_capture/submission.status" >/dev/null
grep -F '"array_task_string":"0-1%1"' "$submitted_capture/initial-squeue.stdout" >/dev/null
grep -F '"jobs":[]' "$submitted_capture/final-squeue.stdout" >/dev/null
grep -Fx '42_0|COMPLETED|0:0|None' "$submitted_capture/final-sacct.stdout" >/dev/null
grep -Fx '42_1|COMPLETED|0:0|None' "$submitted_capture/final-sacct.stdout" >/dev/null
grep -Fx empty "$submitted_capture/queue-completion.status" >/dev/null
grep -Fx complete "$submitted_capture/accounting-completion.status" >/dev/null
grep -F -- '--account=test-account' "$submitted_capture/commands.argv.tsv" >/dev/null
verify_manifest "$submitted_capture"

incomplete_capture="$test_root/incomplete"
if PATH="$fake_bin:$PATH" \
  FAKE_SLURM_STATE="$test_root/incomplete-state" \
  FAKE_SACCT_INCOMPLETE=true \
  "$harness" \
  --output "$incomplete_capture" \
  --submit \
  --poll-attempts 2 \
  --poll-interval-seconds 0; then
  echo 'expected incomplete accounting evidence to return nonzero' >&2
  exit 1
else
  incomplete_status=$?
fi
[[ "$incomplete_status" == 75 ]]
grep -Fx empty "$incomplete_capture/queue-completion.status" >/dev/null
grep -Fx incomplete "$incomplete_capture/accounting-completion.status" >/dev/null
verify_manifest "$incomplete_capture"

rejected_capture="$test_root/rejected"
if PATH="$fake_bin:$PATH" \
  FAKE_SLURM_STATE="$test_root/rejected-state" \
  FAKE_SBATCH_REJECT=true \
  "$harness" \
  --output "$rejected_capture" \
  --submit \
  --poll-attempts 1 \
  --poll-interval-seconds 0; then
  echo 'expected rejected submission to return nonzero' >&2
  exit 1
fi
grep -Fx rejected "$rejected_capture/submission.status" >/dev/null
grep -Fx 1 "$rejected_capture/submission.exit-code" >/dev/null
[[ ! -e "$rejected_capture/initial-squeue.stdout" ]]
verify_manifest "$rejected_capture"

echo 'real-site-smoke acceptance harness tests passed'
