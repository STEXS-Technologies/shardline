#!/usr/bin/env bash
set -euo pipefail

duration_seconds="${SHARDLINE_SOAK_DURATION_SECONDS:-600}"
seed="${SHARDLINE_SOAK_INITIAL_SEED:-2436552524}"
output_root="${1:-reliability-soak-results}"

case "$duration_seconds" in
  ''|*[!0-9]*)
    echo "SHARDLINE_SOAK_DURATION_SECONDS must be a positive decimal integer" >&2
    exit 2
    ;;
esac
case "$seed" in
  ''|*[!0-9]*)
    echo "SHARDLINE_SOAK_INITIAL_SEED must be a non-negative decimal integer" >&2
    exit 2
    ;;
esac
if [ "$duration_seconds" -eq 0 ]; then
  echo "SHARDLINE_SOAK_DURATION_SECONDS must be greater than zero" >&2
  exit 2
fi

mkdir -p "$output_root"
started_epoch="$(date +%s)"
deadline_epoch="$((started_epoch + duration_seconds))"
commit="$(git rev-parse HEAD)"
results_tsv="$output_root/results.tsv"
summary_md="$output_root/summary.md"

process_tree_pids() {
  local pending="$1"
  local observed=""
  local next
  local process_id
  local children
  while [ -n "$pending" ]; do
    next=""
    for process_id in $pending; do
      if [ ! -d "/proc/$process_id" ]; then
        continue
      fi
      observed="$observed $process_id"
      children=""
      if [ -r "/proc/$process_id/task/$process_id/children" ]; then
        read -r children < "/proc/$process_id/task/$process_id/children" || true
      fi
      next="$next $children"
    done
    pending="$next"
  done
  echo "$observed"
}

sample_process_tree() {
  local root_process_id="$1"
  local open_fds=0
  local os_tasks=0
  local process_id
  local count
  for process_id in $(process_tree_pids "$root_process_id"); do
    if [ -d "/proc/$process_id/fd" ]; then
      count="$(find "/proc/$process_id/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
      open_fds="$((open_fds + count))"
    fi
    if [ -d "/proc/$process_id/task" ]; then
      count="$(find "/proc/$process_id/task" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
      os_tasks="$((os_tasks + count))"
    fi
  done
  printf '%s\t%s\n' "$open_fds" "$os_tasks"
}

monitor_process_tree() {
  local root_process_id="$1"
  local resource_log="$2"
  local peak_file="$3"
  local peak_open_fds=0
  local peak_os_tasks=0
  local open_fds
  local os_tasks
  printf 'epoch_millis\topen_fds\tos_tasks\n' > "$resource_log"
  while kill -0 "$root_process_id" 2>/dev/null; do
    IFS=$'\t' read -r open_fds os_tasks < <(sample_process_tree "$root_process_id")
    printf '%s\t%s\t%s\n' "$(date +%s%3N)" "$open_fds" "$os_tasks" >> "$resource_log"
    if [ "$open_fds" -gt "$peak_open_fds" ]; then
      peak_open_fds="$open_fds"
    fi
    if [ "$os_tasks" -gt "$peak_os_tasks" ]; then
      peak_os_tasks="$os_tasks"
    fi
    sleep 0.1
  done
  printf '%s\t%s\n' "$peak_open_fds" "$peak_os_tasks" > "$peak_file"
}

printf 'iteration\tseed\telapsed_seconds\tmax_rss_kib\tpeak_open_fds\tpeak_os_tasks\tstatus\n' > "$results_tsv"
iteration=0

while [ "$(date +%s)" -lt "$deadline_epoch" ]; do
  iteration="$((iteration + 1))"
  run_log="$output_root/run-${iteration}-seed-${seed}.log"
  time_log="$output_root/run-${iteration}-seed-${seed}.time"
  resource_log="$output_root/run-${iteration}-seed-${seed}.resources.tsv"
  resource_peak="$output_root/run-${iteration}-seed-${seed}.resources.peak"
  run_started="$(date +%s)"

  set +e
  /usr/bin/time -v -o "$time_log" \
    env SHARDLINE_CHAOS_SEED="$seed" \
    cargo test --locked -p shardline-server --test chaos_runner -- --nocapture \
    > "$run_log" 2>&1 &
  run_process_id=$!
  monitor_process_tree "$run_process_id" "$resource_log" "$resource_peak" &
  monitor_process_id=$!
  wait "$run_process_id"
  status=$?
  wait "$monitor_process_id" || true
  set -e

  elapsed="$(( $(date +%s) - run_started ))"
  max_rss="$(awk -F ': ' '/Maximum resident set size/ { print $2 }' "$time_log")"
  max_rss="${max_rss:-unknown}"
  peak_open_fds=0
  peak_os_tasks=0
  if [ -f "$resource_peak" ]; then
    IFS=$'\t' read -r peak_open_fds peak_os_tasks < "$resource_peak" || true
  fi
  if [ "$status" -eq 0 ]; then
    outcome="pass"
  else
    outcome="fail"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$iteration" "$seed" "$elapsed" "$max_rss" "$peak_open_fds" "$peak_os_tasks" "$outcome" >> "$results_tsv"

  if [ "$status" -ne 0 ]; then
    break
  fi
  if [ "$seed" -eq 9223372036854775807 ]; then
    seed=0
  else
    seed="$((seed + 1))"
  fi
done

finished_epoch="$(date +%s)"
passed="$(awk -F '\t' 'NR > 1 && $7 == "pass" { count += 1 } END { print count + 0 }' "$results_tsv")"
failed="$(awk -F '\t' 'NR > 1 && $7 == "fail" { count += 1 } END { print count + 0 }' "$results_tsv")"
peak_rss="$(awk -F '\t' 'NR > 1 && $4 ~ /^[0-9]+$/ && $4 > peak { peak = $4 } END { print peak + 0 }' "$results_tsv")"
peak_open_fds="$(awk -F '\t' 'NR > 1 && $5 > peak { peak = $5 } END { print peak + 0 }' "$results_tsv")"
peak_os_tasks="$(awk -F '\t' 'NR > 1 && $6 > peak { peak = $6 } END { print peak + 0 }' "$results_tsv")"
first_open_fds="$(awk -F '\t' 'NR == 2 { print $5 + 0 }' "$results_tsv")"
last_open_fds="$(awk -F '\t' 'END { if (NR > 1) print $5 + 0; else print 0 }' "$results_tsv")"
first_os_tasks="$(awk -F '\t' 'NR == 2 { print $6 + 0 }' "$results_tsv")"
last_os_tasks="$(awk -F '\t' 'END { if (NR > 1) print $6 + 0; else print 0 }' "$results_tsv")"

{
  echo "# Shardline reliability soak"
  echo
  echo "- commit: \`$commit\`"
  echo "- started_epoch: \`$started_epoch\`"
  echo "- finished_epoch: \`$finished_epoch\`"
  echo "- requested_duration_seconds: \`$duration_seconds\`"
  echo "- completed_seed_runs: \`$iteration\`"
  echo "- passed_seed_runs: \`$passed\`"
  echo "- failed_seed_runs: \`$failed\`"
  echo "- peak_max_rss_kib: \`$peak_rss\`"
  echo "- peak_open_fds: \`$peak_open_fds\`"
  echo "- peak_os_tasks: \`$peak_os_tasks\`"
  echo "- first_to_last_peak_open_fds: \`$first_open_fds -> $last_open_fds\`"
  echo "- first_to_last_peak_os_tasks: \`$first_os_tasks -> $last_os_tasks\`"
  echo
  echo '```text'
  cat "$results_tsv"
  echo '```'
} > "$summary_md"

cat "$summary_md"
if [ "$failed" -ne 0 ]; then
  exit 1
fi
