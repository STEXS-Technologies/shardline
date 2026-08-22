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

printf 'iteration\tseed\telapsed_seconds\tmax_rss_kib\tstatus\n' > "$results_tsv"
iteration=0

while [ "$(date +%s)" -lt "$deadline_epoch" ]; do
  iteration="$((iteration + 1))"
  run_log="$output_root/run-${iteration}-seed-${seed}.log"
  time_log="$output_root/run-${iteration}-seed-${seed}.time"
  run_started="$(date +%s)"

  set +e
  /usr/bin/time -v -o "$time_log" \
    env SHARDLINE_CHAOS_SEED="$seed" \
    cargo test -p shardline-server --test chaos_runner -- --nocapture \
    > "$run_log" 2>&1
  status=$?
  set -e

  elapsed="$(( $(date +%s) - run_started ))"
  max_rss="$(awk -F ': ' '/Maximum resident set size/ { print $2 }' "$time_log")"
  max_rss="${max_rss:-unknown}"
  if [ "$status" -eq 0 ]; then
    outcome="pass"
  else
    outcome="fail"
  fi
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$iteration" "$seed" "$elapsed" "$max_rss" "$outcome" >> "$results_tsv"

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
passed="$(awk -F '\t' 'NR > 1 && $5 == "pass" { count += 1 } END { print count + 0 }' "$results_tsv")"
failed="$(awk -F '\t' 'NR > 1 && $5 == "fail" { count += 1 } END { print count + 0 }' "$results_tsv")"
peak_rss="$(awk -F '\t' 'NR > 1 && $4 ~ /^[0-9]+$/ && $4 > peak { peak = $4 } END { print peak + 0 }' "$results_tsv")"

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
  echo
  echo '```text'
  cat "$results_tsv"
  echo '```'
} > "$summary_md"

cat "$summary_md"
if [ "$failed" -ne 0 ]; then
  exit 1
fi
