#!/usr/bin/env bash
# Owned synthetic inventory: normal GC preserves large diagnostic exports.
# Usage: bash scripts/shardline/verify_maintenance_large_diagnostics.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_large_diagnostics.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in jq sqlite3 sha256sum find sort xargs stat; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-large-diagnostics.XXXXXX")
echo "diagnostics: $fixture"
trap 'printf "FAIL: large diagnostic drill at line %s; diagnostics: %s\n" "$LINENO" "$fixture" >&2' ERR
root="$fixture/root"
touch "$fixture/local.toml"
mkdir -p "$root/chunks/00"
count=4096
# These are synthetic orphan identities for an inventory/export test, not
# cryptographically verified chunk contents or reclamation evidence.
for index in $(seq 1 "$count"); do
    printf -v hash '%064x' "$index"
    printf '%s' 'owned synthetic orphan payload' > "$root/chunks/00/$hash"
done
payload_inventory() {
    (cd "$root/chunks/00"; find . -type f -print0 | sort -z | xargs -0 sha256sum)
}
payload_inventory > "$fixture/payload-before.sha256"
arguments=(--config "$fixture/local.toml" gc --mark --root "$root" --retention-seconds 86400
    --retention-report "$fixture/retention.json" --orphan-inventory "$fixture/orphans.json")
env -i "$released" "${arguments[@]}" > "$fixture/bootstrap.stdout" 2> "$fixture/bootstrap.stderr"
for version in released current; do
    case "$version" in released) binary="$released";; current) binary="$current";; esac
    status=0
    env -i "$binary" "${arguments[@]}" > "$fixture/$version.stdout" 2> "$fixture/$version.stderr" || status=$?
    printf '%s\n' "$status" > "$fixture/$version.status"
    test "$status" = 0
    cp "$fixture/retention.json" "$fixture/$version-retention.json"
    cp "$fixture/orphans.json" "$fixture/$version-orphans.json"
    test "$(stat -c %s "$fixture/retention.json")" -gt 1048576
    test "$(stat -c %s "$fixture/orphans.json")" -gt 1048576
    jq -e --argjson count "$count" 'length == $count and all(.[]; .expired == false)' \
        "$fixture/retention.json" > "$fixture/$version-retention-count.log"
    jq -e --argjson count "$count" 'length == $count and all(.[]; .quarantine_state == "quarantined")' \
        "$fixture/orphans.json" > "$fixture/$version-orphan-count.log"
    # Countdown changes with wall time; all persisted identities, lengths,
    # first-seen times and expiry times must remain byte-for-byte comparable.
    jq 'map(del(.seconds_until_delete))' "$fixture/retention.json" > "$fixture/$version-retention-stable.json"
    (cd "$root/chunks"; find . -type f | sort) > "$fixture/$version-inventory-keys.txt"
done
cmp "$fixture/released-inventory-keys.txt" "$fixture/current-inventory-keys.txt"
for artifact in stdout stderr status retention-stable.json orphans.json; do
    case "$artifact" in
        stdout|stderr|status) cmp "$fixture/released.$artifact" "$fixture/current.$artifact";;
        *) cmp "$fixture/released-$artifact" "$fixture/current-$artifact";;
    esac
done
payload_inventory > "$fixture/payload-after.sha256"
cmp "$fixture/payload-before.sha256" "$fixture/payload-after.sha256"
test ! -e "$root/chunks/_shardline_tasks"
test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/records/%' AND json_type(CAST(payload AS TEXT), '$.event.Completed') IS NOT NULL;")" = 1
echo "PASS: normal GC preserves released output, status, payload bytes and 4096-entry diagnostic exports larger than 1 MiB while recording its own completion; diagnostics: $fixture"
