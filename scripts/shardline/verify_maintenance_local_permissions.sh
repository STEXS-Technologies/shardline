#!/usr/bin/env bash
# Released/current comparison with an existing writable database under a 0555 root.
# Usage: bash scripts/shardline/verify_maintenance_local_permissions.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_local_permissions.sh CURRENT RELEASED' >&2; exit 2; }
test "$(id -u)" != 0 || { echo 'run as an unprivileged user to test directory permissions' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
command -v sqlite3 >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-local-permissions.XXXXXX")
echo "diagnostics: $fixture"
root="$fixture/root"
keeper=''
cleanup() {
    exec 9>&- || true
    chmod 755 "$root" 2>/dev/null || true
    chmod 600 "$root"/metadata.sqlite3* 2>/dev/null || true
    if test -n "$keeper"; then
        kill "$keeper" 2>/dev/null || true
        wait "$keeper" 2>/dev/null || true
    fi
    rm -f "$fixture/keeper-input"
}
trap cleanup EXIT
env -i "$released" index rebuild --root "$root" > "$fixture/bootstrap.log" 2>&1
mkfifo "$fixture/keeper-input"
sqlite3 "$root/metadata.sqlite3" < "$fixture/keeper-input" > "$fixture/keeper.log" 2>&1 &
keeper=$!
exec 9> "$fixture/keeper-input"
# Keep existing WAL/SHM files open, matching a root whose directory is not writable.
printf 'PRAGMA journal_mode=WAL;\nSELECT COUNT(*) FROM sqlite_master;\n.shell touch %s/ready\n' "$fixture" >&9
for attempt in $(seq 1 60); do
    test ! -e "$fixture/ready" || break
    sleep 0.1
done
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
test "$(stat -c '%a' "$root")" = 555
test ! -e "$root/chunks/_shardline_tasks"
test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/manifests/*';")" = 4
echo "PASS: four released/current commands preserve output and save metadata-backed history under a 0555 root; diagnostics: $fixture"

# Optional recording must not turn read-only fsck into a failed operation.
# Keep WAL/SHM present but deny new writable opens of all SQLite files.
chmod 444 "$root"/metadata.sqlite3*
if (: >> "$root/metadata.sqlite3") 2> "$fixture/metadata-write-probe.log"; then
    echo 'fixture unexpectedly permits metadata writes' >&2
    exit 1
fi
sha256sum "$root/metadata.sqlite3" "$root/metadata.sqlite3-wal" > "$fixture/readonly-before.sha256"
for version in released current; do
    case "$version" in released) executable="$released";; current) executable="$current";; esac
    status=0
    (cd "$fixture"; env -i "$executable" fsck --root "$root") > "$fixture/readonly-fsck-$version.stdout" 2> "$fixture/readonly-fsck-$version.stderr" || status=$?
    printf '%s\n' "$status" > "$fixture/readonly-fsck-$version.status"
    test "$status" = 0
done
cmp "$fixture/readonly-fsck-released.stdout" "$fixture/readonly-fsck-current.stdout"
cmp "$fixture/readonly-fsck-released.status" "$fixture/readonly-fsck-current.status"
rg -qF 'maintenance tracking unavailable' "$fixture/readonly-fsck-current.stderr"
rg -qF 'the command will still run, but its result will not be confirmed in history' "$fixture/readonly-fsck-current.stderr"
rg -qF 'Check command output and logs before retrying' "$fixture/readonly-fsck-current.stderr"
sha256sum "$root/metadata.sqlite3" "$root/metadata.sqlite3-wal" > "$fixture/readonly-after.sha256"
cmp "$fixture/readonly-before.sha256" "$fixture/readonly-after.sha256"
test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/manifests/*';")" = 4
test ! -e "$root/.shardline-maintenance"
test ! -e "$root/chunks/_shardline_tasks"
echo 'PASS: fully read-only SQLite fsck preserves released output/status and existing evidence; optional history warns without recording completion'
