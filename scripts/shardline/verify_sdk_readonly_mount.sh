#!/usr/bin/env bash
# Usage: verify_sdk_readonly_mount.sh RELEASED_CLI RELEASED_PROBE CURRENT_PROBE
# Probes are the compiled mounted_sdk_upgrade integration-test executables.
set -euo pipefail
test "$#" = 3 || { echo 'usage: verify_sdk_readonly_mount.sh RELEASED_CLI RELEASED_PROBE CURRENT_PROBE' >&2; exit 2; }
released=$(realpath "$1")
released_probe=$(realpath "$2")
current_probe=$(realpath "$3")
command -v sqlite3 >/dev/null
docker image inspect debian:13-slim >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-sdk-readonly-mount.XXXXXX")
echo "diagnostics: $fixture"
keeper=''
cleanup() {
    exec 9>&- || true
    if test -n "$keeper"; then
        kill "$keeper" 2>/dev/null || true
        wait "$keeper" 2>/dev/null || true
    fi
}
trap cleanup EXIT
for version in released current; do
    root="$fixture/$version"
    "$released" index rebuild --root "$root" > "$fixture/$version-bootstrap.log" 2>&1
    mkdir -p "$root/chunks"
    # This owned fixture represents an SDK deployment predating the barrier file.
    rm -f "$root/.gc-write-barrier.lock"
    mkfifo "$fixture/$version-input"
    sqlite3 "$root/metadata.sqlite3" < "$fixture/$version-input" > "$fixture/$version-keeper.log" 2>&1 &
    keeper=$!
    exec 9> "$fixture/$version-input"
    printf 'PRAGMA journal_mode=WAL;\nSELECT count(*) FROM sqlite_master;\n.shell touch %s/%s-ready\n' "$fixture" "$version" >&9
    for attempt in $(seq 1 60); do
        test ! -e "$fixture/$version-ready" || break
        sleep 0.1
    done
    test -e "$fixture/$version-ready"
    test -f "$root/metadata.sqlite3-wal" && test -f "$root/metadata.sqlite3-shm"
    case "$version" in released) probe="$released_probe";; current) probe="$current_probe";; esac
    status=0
    docker run --rm --network none --user "$(id -u):$(id -g)" \
        --mount "type=bind,source=$root,target=/state,readonly" \
        --mount "type=bind,source=$root/chunks,target=/state/chunks" \
        --mount "type=bind,source=$root/metadata.sqlite3,target=/state/metadata.sqlite3" \
        --mount "type=bind,source=$root/metadata.sqlite3-wal,target=/state/metadata.sqlite3-wal" \
        --mount "type=bind,source=$root/metadata.sqlite3-shm,target=/state/metadata.sqlite3-shm" \
        --mount "type=bind,source=$probe,target=/probe,readonly" \
        --env SHARDLINE_MOUNT_FIXTURE=/state \
        debian:13-slim /probe --ignored --exact mounted_sdk_upgrade --nocapture \
        > "$fixture/$version-probe.log" 2>&1 || status=$?
    cat "$fixture/$version-probe.log"
    printf '%s\n' "$status" > "$fixture/$version-status"
    cleanup
    keeper=''
    test "$status" = 0
    rg -q '1 passed; 0 failed' "$fixture/$version-probe.log"
    test ! -e "$root/.gc-write-barrier.lock"
    echo "PASS: $version SDK uploads through a read-only root with writable existing metadata and chunks"
done
