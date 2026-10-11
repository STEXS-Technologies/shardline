#!/usr/bin/env bash
# Compare existing-file maintenance under a non-writable deployment root.
# Usage: bash scripts/shardline/verify_maintenance_local_permissions.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_local_permissions.sh CURRENT RELEASED' >&2; exit 2; }
test "$(id -u)" != 0 || { echo 'run as a normal user; root bypasses permissions' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
command -v sqlite3 >/dev/null
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-local-permissions.XXXXXX")
echo "Validation artifacts: $fixture"
root="$fixture/root"
keeper=''
cleanup() {
    chmod 755 "$root" 2>/dev/null || true
    if test -n "$keeper"; then kill "$keeper" 2>/dev/null || true; wait "$keeper" 2>/dev/null || true; fi
    rm -f "$fixture/keeper-input"
}
trap cleanup EXIT
env -i "$released" index rebuild --root "$root" > "$fixture/bootstrap.log" 2>&1
mkfifo "$fixture/keeper-input"
sqlite3 "$root/metadata.sqlite3" < "$fixture/keeper-input" > "$fixture/keeper.log" 2>&1 &
keeper=$!
exec 9> "$fixture/keeper-input"
printf 'PRAGMA journal_mode=WAL;\nSELECT COUNT(*) FROM sqlite_master;\n.shell touch %s/ready\n' "$fixture" >&9
for attempt in $(seq 1 30); do test ! -e "$fixture/ready" || break; sleep 0.1; done
test -e "$fixture/ready"
test -f "$root/metadata.sqlite3-wal" && test -f "$root/metadata.sqlite3-shm"
chmod 555 "$root"
if mkdir "$root/permission-probe" 2> "$fixture/permission-probe.log"; then
    echo 'fixture unexpectedly permits new root entries' >&2
    exit 1
fi
for operation in fsck rebuild repair gc; do
    case "$operation" in
        fsck) arguments=(fsck);;
        rebuild) arguments=(index rebuild);;
        repair) arguments=(repair lifecycle);;
        gc) arguments=(gc --mark);;
    esac
    for version in released current; do
        case "$version" in released) executable="$released";; current) executable="$current";; esac
        status=0
        (cd "$fixture"; env -i "$executable" "${arguments[@]}" --root "$root") > "$fixture/$operation-$version.stdout" 2> "$fixture/$operation-$version.stderr" || status=$?
        printf '%s\n' "$status" > "$fixture/$operation-$version.status"
        test "$status" = 0
        test ! -e "$root/.shardline-maintenance"
        test "$(stat -c '%a' "$root")" = 555
    done
    for artifact in stdout stderr status; do
        cmp "$fixture/$operation-released.$artifact" "$fixture/$operation-current.$artifact"
    done
    echo "PASS: $operation preserves released output and status without new root entries or permission changes"
done
