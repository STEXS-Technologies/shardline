#!/usr/bin/env bash
# Existing benchmark matrix plus ordinary maintenance and released-format reads.
# Usage: bash scripts/shardline/verify_maintenance_benchmark_compatibility.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_benchmark_compatibility.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in jq sqlite3 sha256sum cmp; do command -v "$tool" >/dev/null; done
test -x /usr/bin/time
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-benchmark-compatibility.XXXXXX")
echo "diagnostics: $fixture"
trap 'printf "FAIL: benchmark compatibility at line %s; diagnostics: %s\n" "$LINENO" "$fixture" >&2' ERR
touch "$fixture/local.toml"
sha256sum "$current" "$released" > "$fixture/binaries.sha256"
released_version=$("$released" --version)
# v1.12.1 already journals ordinary fsck; the older supported predecessor did not.
case "$released_version" in
    'shardline 1.11.3') expected_fsck_completions=1 ;;
    'shardline 1.12.1') expected_fsck_completions=2 ;;
    *) echo "unsupported benchmark compatibility predecessor: $released_version" >&2; exit 2 ;;
esac
uname -a > "$fixture/host.txt"
for concurrency in 1 32 128; do
    for version in released current; do
        case "$version" in released) binary="$released";; current) binary="$current";; esac
        report="$fixture/$version-$concurrency.json"
        /usr/bin/time -f 'wall_seconds=%e\npeak_rss_kib=%M\nexit_code=%x' \
            -o "$fixture/$version-$concurrency.time" \
            env -i "$binary" --config "$fixture/local.toml" bench \
            --deployment-target isolated-local --scenario full \
            --storage-dir "$fixture/$version-$concurrency" --iterations 3 \
            --concurrency "$concurrency" --chunk-size-bytes 65536 \
            --base-bytes 1048576 --mutated-bytes 65536 --json \
            > "$report" 2> "$fixture/$version-$concurrency.stderr"
        jq -e --argjson concurrency "$concurrency" '
            .deployment_target == "isolated-local" and .scenario == "full" and
            .concurrency == $concurrency and .iterations == 3 and
            (.iterations_detail | length) == 3 and .totals.cache_hit_iterations == 3
        ' "$report" > "$fixture/$version-$concurrency.validated"
        jq -S '[paths(scalars) | map(tostring) | join(".")] | sort' "$report" \
            > "$fixture/$version-$concurrency.schema.json"
        # Compare every non-performance counter and setting. Only private
        # filesystem locations and measured timing/throughput are excluded.
        jq -S '
            del(.storage_dir, .latency, .timing, .throughput) |
            .totals |= del(.concurrent_latest_download_scaling_per_mille,
                          .concurrent_upload_scaling_per_mille) |
            .iterations_detail |= map(del(.storage_dir, .latency, .timing))
        ' "$report" > "$fixture/$version-$concurrency.stable.json"
    done
    cmp "$fixture/released-$concurrency.stable.json" "$fixture/current-$concurrency.stable.json"
    cmp "$fixture/released-$concurrency.schema.json" "$fixture/current-$concurrency.schema.json"
    jq -r '.iterations_detail[].storage_dir' "$fixture/current-$concurrency.json" \
        > "$fixture/roots-$concurrency.txt"
    iteration=0
    while IFS= read -r root; do
        iteration=$((iteration + 1))
        for version in released current; do
            case "$version" in released) binary="$released";; current) binary="$current";; esac
            env -i "$binary" --config "$fixture/local.toml" fsck --root "$root" \
                > "$fixture/$version-fsck-$concurrency-$iteration.stdout" \
                2> "$fixture/$version-fsck-$concurrency-$iteration.stderr"
        done
        for stream in stdout stderr; do
            cmp "$fixture/released-fsck-$concurrency-$iteration.$stream" \
                "$fixture/current-fsck-$concurrency-$iteration.$stream"
        done
        test ! -e "$root/chunks/_shardline_tasks"
        test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/records/%' AND json_type(CAST(payload AS TEXT), '$.event.Completed') IS NOT NULL;")" = "$expected_fsck_completions"
    done < "$fixture/roots-$concurrency.txt"
    jq -r '.iterations_detail[].storage_dir' "$fixture/released-$concurrency.json" > "$fixture/released-roots-$concurrency.txt"
    iteration=0
    while IFS= read -r root; do
        iteration=$((iteration + 1))
        stem="$fixture/upgrade-$concurrency-$iteration"
        sqlite3 "$root/metadata.sqlite3" 'SELECT * FROM shardline_local_schema_migrations ORDER BY version' > "$stem.core-before"
        env -i "$released" --config "$fixture/local.toml" fsck --root "$root" > "$stem.before" 2> "$stem.before.stderr"
        env -i "$current" --config "$fixture/local.toml" db migrate local-up --root "$root" > "$stem.migrate" 2> "$stem.migrate.stderr"
        sqlite3 "$root/metadata.sqlite3" 'SELECT * FROM shardline_local_schema_migrations ORDER BY version' > "$stem.core-after"
        cmp "$stem.core-before" "$stem.core-after"
        test "$(sqlite3 "$root/metadata.sqlite3" 'SELECT count(*)=7 AND count(DISTINCT name)=7 FROM shardline_frontend_schema_v1')" = 1
        for version in current released; do
            case "$version" in current) binary="$current";; released) binary="$released";; esac
            env -i "$binary" --config "$fixture/local.toml" fsck --root "$root" > "$stem.$version" 2> "$stem.$version.stderr"
            cmp "$stem.before" "$stem.$version"
            cmp "$stem.before.stderr" "$stem.$version.stderr"
        done
    done < "$fixture/released-roots-$concurrency.txt"
    echo "PASS: benchmark counters, released reads and ordinary fsck at concurrency $concurrency"
done
echo "PASS: existing 1/32/128 benchmark matrix preserves workload results and maintenance output; measurements are fixture-specific, not deployment SLOs; diagnostics: $fixture"
