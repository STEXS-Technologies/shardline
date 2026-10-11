#!/usr/bin/env bash
# Real machine identities, shared evidence, partition and replacement.
# Usage: bash scripts/shardline/verify_maintenance_cross_host.sh CURRENT IMAGE [connection-loss]
set -euo pipefail
test "$#" -ge 2 && test "$#" -le 3 || { echo 'usage: verify_maintenance_cross_host.sh CURRENT IMAGE [connection-loss]' >&2; exit 2; }
mode=${3:-node-loss}
test "$mode" = node-loss || test "$mode" = connection-loss
current=$(realpath "$1")
image=$(realpath "$2")
repository=$(cd "$(dirname "$0")/../.." && pwd)
test -x "$current" && test -f "$image"
for tool in docker qemu-system-x86_64 qemu-img genisoimage ssh ssh-keygen scp curl jq socat; do
    command -v "$tool" >/dev/null
done
test -r /dev/kvm && test -w /dev/kvm
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-cross-host.XXXXXX")
echo "Validation artifacts: $fixture"
project=$(basename "$fixture" | tr '[:upper:].' '[:lower:]-')
vm_pids=('' '')
blocker_pid=''
ssh_ports=()
http_ports=()
export SHARDLINE_CHAOS_PG_PORT=0 SHARDLINE_CHAOS_MINIO_PORT=0 SHARDLINE_CHAOS_MINIO_CONSOLE_PORT=0 SHARDLINE_CHAOS_REDIS_PORT=0
compose() { docker compose -f "$repository/docker-compose.chaos.yml" -p "$project" "$@"; }
cleanup() {
    if test -n "$blocker_pid"; then kill "$blocker_pid" 2>/dev/null || true; wait "$blocker_pid" 2>/dev/null || true; fi
    for pid in "${vm_pids[@]}"; do
        if test -n "$pid"; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
    done
    compose down --volumes --remove-orphans > "$fixture/cleanup.log" 2>&1 || true
    for node in 0 1; do rm -f "$fixture/$node/id_ed25519" "$fixture/$node/seed.iso" "$fixture/$node/disk.qcow2"; done
}
trap cleanup EXIT
compose up -d chaos-postgres chaos-minio chaos-minio-init > "$fixture/backend.log" 2>&1
for attempt in $(seq 1 40); do
    if compose exec -T chaos-postgres pg_isready -U shardline -d shardline >/dev/null 2>&1; then break; fi
    sleep 1
done
compose exec -T chaos-postgres pg_isready -U shardline -d shardline
test "$(docker wait "$(compose ps -aq chaos-minio-init)")" = 0
pg_port=$(compose port chaos-postgres 5432 | head -n 1 | awk -F: '{print $NF}')
s3_port=$(compose port chaos-minio 9000 | head -n 1 | awk -F: '{print $NF}')
"$current" db migrate up --database-url "postgres://shardline:shardline-dev-password@127.0.0.1:$pg_port/shardline" > "$fixture/migrations.log" 2>&1
for node in 0 1; do
    directory="$fixture/$node"
    mkdir "$directory"
    ssh_ports[$node]=$(shuf -i 20000-39999 -n 1)
    http_ports[$node]=$(shuf -i 40000-59999 -n 1)
    ssh-keygen -q -t ed25519 -N '' -f "$directory/id_ed25519"
    cat > "$directory/user-data" <<EOF
#cloud-config
users:
  - name: fixture
    groups: [sudo]
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $(cat "$directory/id_ed25519.pub")
ssh_pwauth: false
disable_root: true
EOF
    printf 'instance-id: shardline-cross-host-%s\nlocal-hostname: shardline-cross-host-%s\n' "$node" "$node" > "$directory/meta-data"
    genisoimage -quiet -output "$directory/seed.iso" -volid cidata -joliet -rock "$directory/user-data" "$directory/meta-data"
    qemu-img create -f qcow2 -F qcow2 -b "$image" "$directory/disk.qcow2" 8G
    qemu-system-x86_64 -enable-kvm -machine q35 -cpu host -smp 2 -m 2048 \
        -drive "file=$directory/disk.qcow2,if=virtio,format=qcow2" \
        -drive "file=$directory/seed.iso,media=cdrom" \
        -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:${ssh_ports[$node]}-:22,hostfwd=tcp:127.0.0.1:${http_ports[$node]}-:8080" \
        -device virtio-net-pci,netdev=net0,id=nic0 -display none \
        -serial "file:$directory/serial.log" -monitor "unix:$directory/monitor.sock,server=on,wait=off" > "$directory/qemu.log" 2>&1 &
    vm_pids[$node]=$!
done
vm_ssh() {
    local node=$1
    shift
    ssh -i "$fixture/$node/id_ed25519" -p "${ssh_ports[$node]}" \
        -o "UserKnownHostsFile=$fixture/$node/known_hosts" -o StrictHostKeyChecking=accept-new \
        -o ConnectTimeout=3 -o BatchMode=yes fixture@127.0.0.1 "$@"
}
history() {
    curl --fail --silent --show-error --max-time 5 -H 'Authorization: Bearer maintenance-cross-host-token' \
        "http://127.0.0.1:${http_ports[1]}/api/v1/tasks/maintenance"
}
shared_hashes() {
    local id=$1
    local sequences=${2:-'0000000000 0000000001'}
    compose run --rm --no-deps --entrypoint /bin/sh chaos-minio-init -c \
        "set -e; mc alias set local http://chaos-minio:9000 shardline shardline-dev-password >/dev/null; for sequence in $sequences; do mc cat local/shardline/_shardline_tasks/v1/runs/$id/records/\$sequence > /tmp/record; test -s /tmp/record; sha256sum /tmp/record; done"
}
sql() { compose exec -T chaos-postgres psql -U shardline -d shardline -At -v ON_ERROR_STOP=1 -c "$1"; }
barrier_filter="locktype = 'advisory' AND objsubid = 1 AND classid::bigint = (x'5348415244474301'::bit(64)::bigint >> 32) AND objid::bigint = (x'5348415244474301'::bit(64)::bigint & 4294967295)"
contended_barrier() {
    # Require one owner and one queued contender, not just two admitted tasks.
    sql "SELECT COUNT(*) FILTER (WHERE granted)=1 AND COUNT(*) FILTER (WHERE NOT granted)=1
         FROM pg_locks WHERE $barrier_filter"
}
for node in 0 1; do
    for attempt in $(seq 1 60); do
        kill -0 "${vm_pids[$node]}" || { cat "$fixture/$node/qemu.log" >&2; exit 1; }
        if vm_ssh "$node" 'cat /etc/machine-id; cat /proc/sys/kernel/random/boot_id' > "$fixture/$node/identity" 2>/dev/null; then break; fi
        sleep 1
    done
    test "$(wc -l < "$fixture/$node/identity")" -eq 2
    scp -i "$fixture/$node/id_ed25519" -P "${ssh_ports[$node]}" -o "UserKnownHostsFile=$fixture/$node/known_hosts" \
        -o StrictHostKeyChecking=yes "$current" fixture@127.0.0.1:/tmp/shardline
    cat > "$fixture/$node/shardline.env" <<EOF
SHARDLINE_ROOT_DIR=/var/lib/shardline
SHARDLINE_BIND_ADDR=0.0.0.0:8080
SHARDLINE_PUBLIC_BASE_URL=http://localhost:8080
SHARDLINE_AUTH_PROVIDER=local
SHARDLINE_TOKEN_SIGNING_KEY=0123456789abcdef0123456789abcdef
SHARDLINE_ADMIN_READ_TOKEN=maintenance-cross-host-token
SHARDLINE_SERVER_FRONTENDS=xet
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3
SHARDLINE_S3_BUCKET=shardline
SHARDLINE_S3_REGION=us-east-1
SHARDLINE_S3_ENDPOINT=http://10.0.2.2:$s3_port
SHARDLINE_S3_ACCESS_KEY_ID=shardline
SHARDLINE_S3_SECRET_ACCESS_KEY=shardline-dev-password
SHARDLINE_S3_ALLOW_HTTP=true
SHARDLINE_INDEX_POSTGRES_URL=postgres://shardline:shardline-dev-password@10.0.2.2:$pg_port/shardline
EOF
    scp -i "$fixture/$node/id_ed25519" -P "${ssh_ports[$node]}" -o "UserKnownHostsFile=$fixture/$node/known_hosts" \
        -o StrictHostKeyChecking=yes "$fixture/$node/shardline.env" fixture@127.0.0.1:/tmp/shardline.env
    vm_ssh "$node" 'sudo install -m 0755 /tmp/shardline /usr/local/bin/shardline; sudo install -d -m 0755 /etc/shardline; sudo install -m 0600 /tmp/shardline.env /etc/shardline/shardline.env; sudo install -d -m 0755 -o fixture -g fixture /var/lib/shardline'
    vm_ssh "$node" "sudo sed 's/SHARDLINE_SERVER_FRONTENDS=xet/SHARDLINE_SERVER_FRONTENDS=lfs/' /etc/shardline/shardline.env | sudo tee /etc/shardline/shardline-lfs.env >/dev/null; sudo chmod 0600 /etc/shardline/shardline-lfs.env"
done
test "$(head -n 1 "$fixture/0/identity")" != "$(head -n 1 "$fixture/1/identity")"
test "$(tail -n 1 "$fixture/0/identity")" != "$(tail -n 1 "$fixture/1/identity")"
vm_ssh 1 'sudo systemd-run --unit shardline-validation-observer -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve'
for attempt in $(seq 1 20); do
    if history > "$fixture/initial.json"; then break; fi
    sleep 1
done
jq -e '(.tasks | length) == 0' "$fixture/initial.json"
vm_ssh 0 'bash -s' <<'GUEST'
set -euo pipefail
sudo install -d -m 0755 -o fixture -g fixture /var/lib/shardline/tmp/lfs-patch
sudo systemd-run --unit shardline-validation-session-lock flock --exclusive /var/lib/shardline/tmp/lfs-patch/.sessions.lock sleep infinity
for attempt in $(seq 1 20); do
    if sudo lslocks -n -o COMMAND,TYPE,MODE,PATH | grep -E '^flock +FLOCK +WRITE +/var/lib/shardline/tmp/lfs-patch/.sessions.lock$'; then break; fi
    sleep 1
done
sudo lslocks -n -o COMMAND,TYPE,MODE,PATH | grep -E '^flock +FLOCK +WRITE +/var/lib/shardline/tmp/lfs-patch/.sessions.lock$'
sudo chown fixture:fixture /var/lib/shardline/tmp/lfs-patch/.sessions.lock
sudo systemd-run --unit shardline-validation-startup -p EnvironmentFile=/etc/shardline/shardline-lfs.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve
GUEST
for attempt in $(seq 1 30); do
    if history > "$fixture/started.json" && jq -e '(.tasks | length) == 1 and .tasks[0].state == "running_or_interrupted"' "$fixture/started.json" >/dev/null; then break; fi
    sleep 1
done
id=$(jq -er '.tasks[0] | select(.state == "running_or_interrupted") | .id' "$fixture/started.json")
[[ "$id" =~ ^[a-f0-9]{64}$ ]]
source_pid=$(vm_ssh 0 'sudo systemctl show shardline-validation-startup -p MainPID --value')
jq -e --arg host "$(head -n 1 "$fixture/0/identity")" --argjson pid "$source_pid" '.tasks[0].owner.host == $host and .tasks[0].owner.pid == $pid' "$fixture/started.json"
shared_hashes "$id" > "$fixture/records-before"
# A normal source CLI owns the existing global barrier while its version scan
# waits on a fixture table lock. The other machine admits its own distinct run.
sql "INSERT INTO shardline_file_records (record_key, record_kind, scope_key, file_id, content_hash, record) VALUES ('6:latest8:6:global9:stale.bin', 'latest', '6:global', 'stale.bin', repeat('0',64), jsonb_build_object('file_id','stale.bin','content_hash',repeat('0',64),'total_bytes',0,'chunk_size',128,'storage_repr','fixed_chunk_v1','repository_scope',NULL,'chunks','[]'::jsonb))" > "$fixture/stale-fixture.log"
test "$(sql "SELECT COUNT(*) FROM shardline_file_records WHERE file_id='stale.bin'")" = 1
compose exec -T chaos-postgres psql -U shardline -d shardline -v ON_ERROR_STOP=1 -c \
    "SET application_name='shardline-vm-scan-blocker'; BEGIN; LOCK TABLE shardline_file_records IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(600);" > "$fixture/scan-blocker.log" 2>&1 &
blocker_pid=$!
for attempt in $(seq 1 20); do
    if test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name='shardline-vm-scan-blocker' AND wait_event='PgSleep')")" = t; then break; fi
    sleep 1
done
test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name='shardline-vm-scan-blocker' AND wait_event='PgSleep')")" = t
vm_ssh 0 'sudo systemd-run --unit shardline-validation-source-rebuild -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline index rebuild'
for attempt in $(seq 1 30); do
    if test "$(sql "SELECT COUNT(*) FILTER (WHERE granted)=1 FROM pg_locks WHERE $barrier_filter")" = t \
        && test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event='relation' AND query LIKE '%shardline_file_records%')")" = t; then break; fi
    sleep 1
done
test "$(sql "SELECT COUNT(*) FILTER (WHERE granted)=1 FROM pg_locks WHERE $barrier_filter")" = t
test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event='relation' AND query LIKE '%shardline_file_records%')")" = t
history > "$fixture/source-rebuild.json"
source_host=$(head -n 1 "$fixture/0/identity")
source_run=$(jq -er --arg host "$source_host" '.tasks[] | select(.execution == "inline" and .owner.host == $host) | .id' "$fixture/source-rebuild.json")
[[ "$source_run" =~ ^[a-f0-9]{32}$ ]]
source_cli_pid=$(vm_ssh 0 'sudo systemctl show shardline-validation-source-rebuild -p MainPID --value')
jq -e --arg id "$source_run" --argjson pid "$source_cli_pid" 'any(.tasks[]; .id == $id and .owner.pid == $pid and .state == "running_or_interrupted")' "$fixture/source-rebuild.json"
shared_hashes "$source_run" '0000000000 0000000001 0000000002' > "$fixture/source-cli-records-before"
vm_ssh 1 'sudo systemd-run --unit shardline-validation-waiter-rebuild -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline index rebuild'
for attempt in $(seq 1 30); do
    if test "$(contended_barrier)" = t; then break; fi
    sleep 1
done
test "$(contended_barrier)" = t
source_coordinator=$(sql "SELECT pid FROM pg_locks WHERE $barrier_filter AND granted")
history > "$fixture/contended.json"
waiter_run=$(jq -er --arg host "$(head -n 1 "$fixture/1/identity")" '.tasks[] | select(.execution == "inline" and .owner.host == $host and .state == "running") | .id' "$fixture/contended.json")
[[ "$waiter_run" =~ ^[a-f0-9]{32}$ ]]
test "$waiter_run" != "$source_run"
if test "$mode" = connection-loss; then
    # Terminate only the source's lock-owning connection, keeping its VM alive.
    # Losing ownership must fail the original command, not reconnect its scan.
    coordinator=$(sql "SELECT pid FROM pg_locks WHERE $barrier_filter AND granted")
    [[ "$coordinator" =~ ^[0-9]+$ ]]
    test "$(sql "SELECT pg_terminate_backend($coordinator)")" = t
    for attempt in $(seq 1 15); do
        replacement_coordinator=$(sql "SELECT pid FROM pg_locks WHERE $barrier_filter AND granted")
        if [[ "$replacement_coordinator" =~ ^[0-9]+$ ]] && test "$replacement_coordinator" != "$coordinator"; then break; fi
        sleep 1
    done
    [[ "$replacement_coordinator" =~ ^[0-9]+$ ]]
    test "$replacement_coordinator" != "$coordinator"
    sql "SELECT pid, granted FROM pg_locks WHERE $barrier_filter ORDER BY pid" > "$fixture/connection-loss-locks.log"
    for attempt in $(seq 1 15); do
        if history > "$fixture/connection-lost.json" && jq -e --arg id "$source_run" 'any(.tasks[]; .id == $id and .state == "failed")' "$fixture/connection-lost.json" >/dev/null; then break; fi
        sleep 1
    done
    if ! jq -e --arg id "$source_run" 'any(.tasks[]; .id == $id and .state == "failed")' "$fixture/connection-lost.json"; then
        echo 'lost coordinator did not stop the still-live source command' >&2
        vm_ssh 0 'sudo systemctl show shardline-validation-source-rebuild -p MainPID -p ActiveState' >&2
        exit 1
    fi
    test "$(sql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='shardline-vm-scan-blocker'")" = t
    wait "$blocker_pid" 2>/dev/null || true
    blocker_pid=''
    for attempt in $(seq 1 30); do
        if history > "$fixture/connection-waiter.json" && jq -e --arg id "$waiter_run" 'any(.tasks[]; .id == $id and .state == "completed")' "$fixture/connection-waiter.json" >/dev/null; then break; fi
        sleep 1
    done
    jq -e --arg id "$waiter_run" --arg source "$source_run" 'any(.tasks[]; .id == $id and .state == "completed") and any(.tasks[]; .id == $source and .state == "failed")' "$fixture/connection-waiter.json"
    vm_ssh 1 'sudo journalctl -u shardline-validation-waiter-rebuild -o cat --no-pager' > "$fixture/connection-waiter-report.log"
    grep -Fx 'removed_stale_latest_records: 1' "$fixture/connection-waiter-report.log"
    test "$(sql "SELECT COUNT(*) FROM shardline_file_records WHERE file_id='stale.bin'")" = 0
    echo 'PASS: coordinator loss stops the original command; the separately admitted waiter completes'
    exit 0
fi
# Disconnect the source machine while its kernel and original process remain
# alive. The observer cannot promote a foreign PID observation into a lease.
printf 'set_link nic0 off\n' | socat - "UNIX-CONNECT:$fixture/0/monitor.sock" > "$fixture/partition.log"
kill -0 "${vm_pids[0]}"
if vm_ssh 0 true > "$fixture/source-unreachable.log" 2>&1; then
    echo 'source remained reachable after network partition' >&2
    exit 1
fi
kill -0 "${vm_pids[0]}"
history > "$fixture/partitioned.json"
jq -e --arg id "$id" --arg source "$source_run" --arg waiter "$waiter_run" '(.tasks | length) == 3 and all(.tasks[]; if .id == $waiter then .state == "running" else (.id == $id or .id == $source) and .state == "running_or_interrupted" end)' "$fixture/partitioned.json"
sql "SELECT pid, granted FROM pg_locks WHERE $barrier_filter ORDER BY pid" > "$fixture/partition-locks.log"
sql "SELECT pid, state, wait_event, query FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid" > "$fixture/partition-activity.log"
test "$(contended_barrier)" = t
kill -KILL "${vm_pids[0]}"
wait "${vm_pids[0]}" 2>/dev/null || true
vm_pids[0]=''
# Backend session death releases the source's barrier. The already admitted
# waiter now owns it; no task history or local PID observation grants ownership.
for attempt in $(seq 1 30); do
    replacement_coordinator=$(sql "SELECT pid FROM pg_locks WHERE $barrier_filter AND granted")
    if [[ "$replacement_coordinator" =~ ^[0-9]+$ ]] && test "$replacement_coordinator" != "$source_coordinator"; then break; fi
    sleep 1
done
sql "SELECT pid, granted FROM pg_locks WHERE $barrier_filter ORDER BY pid" > "$fixture/node-loss-locks.log"
sql "SELECT pid, state, wait_event, query FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid" > "$fixture/node-loss-activity.log"
test "$(sql "SELECT COUNT(*) FILTER (WHERE granted)=1 AND COUNT(*) FILTER (WHERE NOT granted)=0 FROM pg_locks WHERE $barrier_filter")" = t
test "$replacement_coordinator" != "$source_coordinator"
test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event='relation' AND query LIKE '%shardline_file_records%')")" = t
test "$(sql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='shardline-vm-scan-blocker'")" = t
wait "$blocker_pid" 2>/dev/null || true
blocker_pid=''
for attempt in $(seq 1 30); do
    if history > "$fixture/waiter-finished.json" && jq -e --arg id "$waiter_run" 'any(.tasks[]; .id == $id and .state == "completed")' "$fixture/waiter-finished.json" >/dev/null; then break; fi
    sleep 1
done
jq -e --arg id "$waiter_run" --arg source "$source_run" 'any(.tasks[]; .id == $id and .state == "completed") and any(.tasks[]; .id == $source and .state == "running_or_interrupted")' "$fixture/waiter-finished.json"
vm_ssh 1 'sudo journalctl -u shardline-validation-waiter-rebuild -o cat --no-pager' > "$fixture/waiter-report.log"
grep -Fx 'removed_stale_latest_records: 1' "$fixture/waiter-report.log"
test "$(sql "SELECT COUNT(*) FROM shardline_file_records WHERE file_id='stale.bin'")" = 0
vm_ssh 1 'sudo systemctl stop shardline-validation-observer; sudo systemd-run --unit shardline-validation-replacement -p EnvironmentFile=/etc/shardline/shardline-lfs.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve'
for attempt in $(seq 1 30); do
    if history > "$fixture/replaced.json" && jq -e '(.tasks | length) == 4' "$fixture/replaced.json" >/dev/null; then break; fi
    sleep 1
done
jq -e --arg id "$id" --arg source "$source_run" --arg waiter "$waiter_run" --arg host "$(head -n 1 "$fixture/1/identity")" '(.tasks | length) == 4 and ([.tasks[] | select((.id == $id or .id == $source) and .state == "running_or_interrupted")] | length) == 2 and ([.tasks[] | select(.state == "completed" and .owner.host == $host)] | length) == 2 and any(.tasks[]; .id == $waiter and .state == "completed")' "$fixture/replaced.json"
curl --fail --silent --show-error --max-time 5 -H 'Authorization: Bearer maintenance-cross-host-token' \
    "http://127.0.0.1:${http_ports[1]}/api/v1/status" > "$fixture/status.json"
jq -e '.state == "ready"' "$fixture/status.json"
vm_ssh 1 'test ! -d /var/lib/shardline/.shardline-maintenance; test "$(pgrep -xc shardline)" -eq 1'
shared_hashes "$id" > "$fixture/records-after"
cmp "$fixture/records-before" "$fixture/records-after"
shared_hashes "$source_run" '0000000000 0000000001 0000000002' > "$fixture/source-cli-records-after"
cmp "$fixture/source-cli-records-before" "$fixture/source-cli-records-after"
echo 'PASS: cross-machine partition preserves ownership; backend session death releases a waiting ordinary rebuild; lost runs stay unresolved'
