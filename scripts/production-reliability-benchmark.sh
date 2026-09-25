#!/usr/bin/env bash
set -euo pipefail

# Runs production-shaped incident drills as isolated nextest processes and
# records end-to-end incident/recovery duration, peak RSS, and outcome. The
# scenarios are exact test names; this intentionally does not use substring
# selection so a new drill cannot silently enter the release report.

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
output_root="${1:-$repo_root/benchmark-results}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
commit="$(git -C "$repo_root" rev-parse --short=12 HEAD)"
run_dir="$output_root/incident-${timestamp}-${commit}"

if [[ -e "$run_dir" ]]; then
  echo "refusing to overwrite benchmark run: $run_dir" >&2
  exit 1
fi
mkdir -p "$run_dir/logs" "$run_dir/results"

scenario_names=(
  postgres_mid_upload
  backup_destroy_restore
  postgres_mid_lfs_patch
  postgres_mid_oci_upload
  postgres_mid_s3_multipart
  repeated_kill_cycles
)
scenario_packages=(
  shardline-server
  shardline-server
  shardline-server
  shardline-server
  shardline-server
  shardline-server
)
scenario_targets=(
  fault_drills
  fault_drills
  deployment_chaos
  deployment_chaos
  deployment_chaos
  fault_drills_extreme
)
scenario_tests=(
  drill3_postgres_kill_mid_upload_recovery
  drill9_backup_destroy_restore_fsck_and_download
  drill_deploy_postgres_kill_mid_lfs_patch
  drill_deploy_postgres_kill_mid_oci_blob_upload
  drill_deploy_postgres_kill_mid_s3_multipart
  drill_extreme_5_repeated_kill_cycles
)

printf 'name\tpackage\ttarget\ttest\tstatus\texit_code\twall_seconds\tpeak_rss_kib\n' \
  >"$run_dir/results/raw.tsv"

run_count="${#scenario_names[@]}"
for ((index = 0; index < run_count; index++)); do
  name="${scenario_names[$index]}"
  package="${scenario_packages[$index]}"
  target="${scenario_targets[$index]}"
  test_name="${scenario_tests[$index]}"
  stdout_log="$run_dir/logs/${name}.stdout.log"
  stderr_log="$run_dir/logs/${name}.stderr.log"
  timing_log="$run_dir/results/${name}.time.txt"

  echo "[incident] ${name} (${test_name})"
  set +e
  /usr/bin/time -f '%e\t%M\t%x' -o "$timing_log" \
    cargo nextest run \
      -p "$package" \
      --test "$target" \
      --all-features \
      --no-fail-fast \
      -E "test(${test_name})" \
      >"$stdout_log" 2>"$stderr_log"
  command_status=$?
  set -e

  read -r wall_seconds peak_rss_kib timed_exit <"$timing_log"
  status="passed"
  if [[ "$command_status" -ne 0 || "$timed_exit" -ne 0 ]]; then
    status="failed"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$package" "$target" "$test_name" "$status" "$command_status" \
    "$wall_seconds" "$peak_rss_kib" >>"$run_dir/results/raw.tsv"
done

python3 - "$run_dir" "$commit" "$timestamp" <<'PY'
import json
import pathlib
import platform
import sys

run_dir = pathlib.Path(sys.argv[1])
commit = sys.argv[2]
timestamp = sys.argv[3]
rows = []
with (run_dir / "results" / "raw.tsv").open(encoding="utf-8") as source:
    headers = source.readline().rstrip("\n").split("\t")
    for line in source:
        values = line.rstrip("\n").split("\t")
        if values:
            row = dict(zip(headers, values, strict=True))
            row["wall_seconds"] = float(row["wall_seconds"])
            row["peak_rss_kib"] = int(row["peak_rss_kib"])
            row["exit_code"] = int(row["exit_code"])
            rows.append(row)

metadata = {
    "schema_version": 1,
    "commit": commit,
    "timestamp_utc": timestamp,
    "platform": platform.platform(),
    "logical_cpus": __import__("os").cpu_count(),
    "scenarios": rows,
}
(run_dir / "results" / "incident-recovery.json").write_text(
    json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

summary = [
    "# Shardline production incident and recovery benchmark",
    "",
    f"- source: `{commit}`",
    f"- measured at: `{timestamp}`",
    f"- host: {metadata['platform']} ({metadata['logical_cpus']} logical CPUs)",
    "",
    "| scenario | status | wall seconds | peak RSS MiB |",
    "| --- | --- | ---: | ---: |",
]
for row in rows:
    summary.append(
        f"| `{row['name']}` | {row['status']} | {row['wall_seconds']:.3f} "
        f"| {row['peak_rss_kib'] / 1024:.1f} |"
    )
summary += [
    "",
    "Wall time measures the complete isolated incident drill, including fault injection, "
    "restart, reconciliation, and successful verification. It is an engineering baseline, "
    "not an SLA or cross-machine product claim.",
]
(run_dir / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

if any(row["status"] != "passed" for row in rows):
    raise SystemExit(1)
PY

echo "$run_dir"
