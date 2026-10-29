#!/usr/bin/env bash
# Usage: verify_sdk_readonly_mount.sh RELEASED_CLI RELEASED_PROBE CURRENT_CLI CURRENT_PROBE
# Probes are the compiled mounted_sdk_upgrade integration-test executables.
set -euo pipefail
test "$#" = 4 || { echo 'usage: verify_sdk_readonly_mount.sh RELEASED_CLI RELEASED_PROBE CURRENT_CLI CURRENT_PROBE' >&2; exit 2; }
released=$(realpath "$1")
released_probe=$(realpath "$2")
current=$(realpath "$3")
current_probe=$(realpath "$4")
test -x "$current"
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
for fixture_id in absent-released absent-current present-released present-current; do
    version=${fixture_id##*-}
    layout=${fixture_id%-*}
    root="$fixture/$fixture_id"
    "$released" index rebuild --root "$root" > "$fixture/$fixture_id-bootstrap.log" 2>&1
    if test "$version" = current; then
        # Runtime SDK construction checks schema compatibility without DDL.
        # Apply the documented owner migration before making the root read-only.
        "$current" db migrate local-up --root "$root" > "$fixture/$fixture_id-migration.log" 2>&1
    fi
    mkdir -p "$root/chunks"
    # This owned fixture represents an SDK deployment predating the barrier file.
    case "$layout" in
        absent) rm -f "$root/.gc-write-barrier.lock";;
        present)
            printf 'existing legacy ownership inode\n' > "$root/.gc-write-barrier.lock"
            sha256sum "$root/.gc-write-barrier.lock" > "$fixture/$fixture_id-barrier-before.sha256"
            ;;
    esac
    mkfifo "$fixture/$fixture_id-input"
    sqlite3 "$root/metadata.sqlite3" < "$fixture/$fixture_id-input" > "$fixture/$fixture_id-keeper.log" 2>&1 &
    keeper=$!
    exec 9> "$fixture/$fixture_id-input"
    printf 'PRAGMA journal_mode=WAL;\nSELECT count(*) FROM sqlite_master;\n.shell touch %s/%s-ready\n' "$fixture" "$fixture_id" >&9
    for attempt in $(seq 1 60); do
        test ! -e "$fixture/$fixture_id-ready" || break
        sleep 0.1
    done
    test -e "$fixture/$fixture_id-ready"
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
        --env "SHARDLINE_MOUNT_BARRIER=$layout" \
        debian:13-slim /probe --ignored --exact mounted_sdk_upgrade --nocapture \
        > "$fixture/$fixture_id-probe.log" 2>&1 || status=$?
    cat "$fixture/$fixture_id-probe.log"
    printf '%s\n' "$status" > "$fixture/$fixture_id-status"
    cleanup
    keeper=''
    test "$status" = 0
    rg -q '1 passed; 0 failed' "$fixture/$fixture_id-probe.log"
    case "$layout" in
        absent) test ! -e "$root/.gc-write-barrier.lock";;
        present)
            sha256sum "$root/.gc-write-barrier.lock" > "$fixture/$fixture_id-barrier-after.sha256"
            cmp "$fixture/$fixture_id-barrier-before.sha256" "$fixture/$fixture_id-barrier-after.sha256"
            ;;
    esac
    echo "PASS: $version SDK uploads through a read-only root with $layout legacy barrier and writable existing metadata and chunks"
done
