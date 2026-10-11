#!/usr/bin/env bash
# Real machine identities, shared evidence, partition and replacement.
# Usage: bash scripts/shardline/verify_maintenance_cross_host.sh CURRENT IMAGE
set -euo pipefail
test "$#" -eq 2 || { echo 'usage: verify_maintenance_cross_host.sh CURRENT IMAGE' >&2; exit 2; }
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
ssh_ports=()
http_ports=()
export SHARDLINE_CHAOS_PG_PORT=0 SHARDLINE_CHAOS_MINIO_PORT=0 SHARDLINE_CHAOS_MINIO_CONSOLE_PORT=0 SHARDLINE_CHAOS_REDIS_PORT=0
compose() { docker compose -f "$repository/docker-compose.chaos.yml" -p "$project" "$@"; }
cleanup() {
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
    compose run --rm --no-deps --entrypoint /bin/sh chaos-minio-init -c \
        "set -e; mc alias set local http://chaos-minio:9000 shardline shardline-dev-password >/dev/null; for sequence in 0000000000 0000000001; do mc cat local/shardline/_shardline_tasks/v1/runs/$id/records/\$sequence > /tmp/record; test -s /tmp/record; sha256sum /tmp/record; done"
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
jq -e --arg id "$id" '(.tasks | length) == 1 and .tasks[0].id == $id and .tasks[0].state == "running_or_interrupted"' "$fixture/partitioned.json"
kill -KILL "${vm_pids[0]}"
wait "${vm_pids[0]}" 2>/dev/null || true
vm_pids[0]=''
vm_ssh 1 'sudo systemctl stop shardline-validation-observer; sudo systemd-run --unit shardline-validation-replacement -p EnvironmentFile=/etc/shardline/shardline-lfs.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve'
for attempt in $(seq 1 30); do
    if history > "$fixture/replaced.json" && jq -e '(.tasks | length) == 2' "$fixture/replaced.json" >/dev/null; then break; fi
    sleep 1
done
jq -e --arg id "$id" --arg host "$(head -n 1 "$fixture/1/identity")" '(.tasks | length) == 2 and ([.tasks[] | select(.id == $id and .state == "running_or_interrupted")] | length) == 1 and ([.tasks[] | select(.id != $id and .state == "completed" and .owner.host == $host)] | length) == 1' "$fixture/replaced.json"
curl --fail --silent --show-error --max-time 5 -H 'Authorization: Bearer maintenance-cross-host-token' \
    "http://127.0.0.1:${http_ports[1]}/api/v1/status" > "$fixture/status.json"
jq -e '.state == "ready"' "$fixture/status.json"
vm_ssh 1 'test ! -d /var/lib/shardline/.shardline-maintenance; test "$(pgrep -xc shardline)" -eq 1'
shared_hashes "$id" > "$fixture/records-after"
cmp "$fixture/records-before" "$fixture/records-after"
echo 'PASS: distinct machines share verified history; partitioned and dead foreign owners remain unresolved; replacement uses one server process'
