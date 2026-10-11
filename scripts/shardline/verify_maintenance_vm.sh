#!/usr/bin/env bash
# Isolated acceptance drill. Requires a verified Debian 13 cloud image.
# Usage: bash scripts/shardline/verify_maintenance_vm.sh CURRENT RELEASED IMAGE
set -euo pipefail
test "$#" -eq 3 || { echo 'usage: verify_maintenance_vm.sh CURRENT RELEASED IMAGE' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
image=$(realpath "$3")
test -x "$current" && test -x "$released" && test -f "$image"
for tool in qemu-system-x86_64 qemu-img genisoimage ssh ssh-keygen scp curl jq socat; do
    command -v "$tool" >/dev/null
done
test -r /dev/kvm && test -w /dev/kvm
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-maintenance-vm.XXXXXX")
echo "Validation artifacts: $fixture"
vm_pid=''
cleanup() {
    if test -n "$vm_pid"; then
        kill "$vm_pid" 2>/dev/null || true
        wait "$vm_pid" 2>/dev/null || true
    fi
    rm -f "$fixture/id_ed25519" "$fixture/seed.iso" "$fixture/disk.qcow2"
}
trap cleanup EXIT
ssh_port=$(shuf -i 20000-39999 -n 1)
http_port=$(shuf -i 40000-59999 -n 1)
ssh-keygen -q -t ed25519 -N '' -f "$fixture/id_ed25519"
cat > "$fixture/user-data" <<EOF
#cloud-config
users:
  - name: fixture
    groups: [sudo]
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $(cat "$fixture/id_ed25519.pub")
ssh_pwauth: false
disable_root: true
EOF
printf 'instance-id: shardline-maintenance-validation\nlocal-hostname: shardline-maintenance-validation\n' > "$fixture/meta-data"
genisoimage -quiet -output "$fixture/seed.iso" -volid cidata -joliet -rock "$fixture/user-data" "$fixture/meta-data"
qemu-img create -f qcow2 -F qcow2 -b "$image" "$fixture/disk.qcow2" 8G
qemu-system-x86_64 -enable-kvm -machine q35 -cpu host -smp 2 -m 2048 \
    -drive "file=$fixture/disk.qcow2,if=virtio,format=qcow2" \
    -drive "file=$fixture/seed.iso,media=cdrom" \
    -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:$ssh_port-:22,hostfwd=tcp:127.0.0.1:$http_port-:8080" \
    -device virtio-net-pci,netdev=net0 -display none \
    -serial "file:$fixture/serial.log" \
    -monitor "unix:$fixture/monitor.sock,server=on,wait=off" > "$fixture/qemu.log" 2>&1 &
vm_pid=$!
vm_ssh() {
    ssh -i "$fixture/id_ed25519" -p "$ssh_port" -o "UserKnownHostsFile=$fixture/known_hosts" \
        -o StrictHostKeyChecking=accept-new -o ConnectTimeout=3 -o BatchMode=yes fixture@127.0.0.1 "$@"
}
wait_for_guest() {
    for attempt in $(seq 1 60); do
        kill -0 "$vm_pid" || { cat "$fixture/qemu.log" >&2; return 1; }
        if vm_ssh 'cat /proc/sys/kernel/random/boot_id; cat /etc/machine-id' > "$1" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}
copy_to_guest() {
    scp -i "$fixture/id_ed25519" -P "$ssh_port" -o "UserKnownHostsFile=$fixture/known_hosts" \
        -o StrictHostKeyChecking=yes "$1" "fixture@127.0.0.1:$2"
}
history() {
    curl --fail --silent --show-error --max-time 5 -H 'Authorization: Bearer maintenance-vm-validation-token' \
        "http://127.0.0.1:$http_port/api/v1/tasks/maintenance"
}
wait_for_guest "$fixture/boot-before"
copy_to_guest "$current" /tmp/shardline-new
copy_to_guest "$released" /tmp/shardline-old
vm_ssh 'bash -s' > "$fixture/systemd.log" 2>&1 <<'GUEST'
set -euo pipefail
test "$(hostname)" = shardline-maintenance-validation
sudo apt-get update -qq
sudo apt-get install -y -qq sqlite3
sudo install -d -m 0755 /etc/shardline
sudo install -d -m 0755 -o fixture -g fixture /var/lib/shardline
sudo tee /etc/shardline/shardline.env >/dev/null <<'ENV'
SHARDLINE_ROOT_DIR=/var/lib/shardline
SHARDLINE_BIND_ADDR=0.0.0.0:8080
SHARDLINE_PUBLIC_BASE_URL=http://localhost:8080
SHARDLINE_AUTH_PROVIDER=local
SHARDLINE_TOKEN_SIGNING_KEY=0123456789abcdef0123456789abcdef
SHARDLINE_ADMIN_READ_TOKEN=maintenance-vm-validation-token
SHARDLINE_SERVER_FRONTENDS=xet
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory
ENV
sudo chmod 0600 /etc/shardline/shardline.env
sudo install -m 0755 /tmp/shardline-old /usr/local/bin/shardline
sudo /usr/local/bin/shardline gc schedule install --env-file /etc/shardline/shardline.env --user fixture --group fixture
sudo systemctl daemon-reload
sudo sha256sum /etc/systemd/system/shardline-gc.service > /tmp/released-unit.sha256
sudo systemctl start shardline-gc.service
test "$(sudo systemctl show shardline-gc.service -p ExecMainStatus --value)" = 0
sudo install -m 0755 /tmp/shardline-new /usr/local/bin/shardline
sudo sha256sum -c /tmp/released-unit.sha256
for run in 1 2; do
    sudo systemctl start shardline-gc.service
    test "$(sudo systemctl show shardline-gc.service -p ExecMainStatus --value)" = 0
done
sudo systemctl show shardline-gc.service -p ProtectSystem -p ProtectHome -p NoNewPrivileges -p PrivateTmp -p ReadWritePaths -p ExecMainStatus -p Result
sudo systemd-run --unit shardline-validation-server -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve
GUEST
for attempt in $(seq 1 20); do
    if history > "$fixture/initial.json"; then break; fi
    sleep 1
done
jq -e '(.tasks | length) == 2 and all(.tasks[]; .state == "completed")' "$fixture/initial.json"
vm_ssh 'bash -s' <<'GUEST'
set -euo pipefail
sudo systemd-run --unit shardline-validation-barrier flock --exclusive /var/lib/shardline/.gc-write-barrier.lock sleep infinity
for attempt in $(seq 1 20); do
    if sudo lslocks -n -o COMMAND,TYPE,MODE,PATH | grep -E '^flock +FLOCK +WRITE +/var/lib/shardline/.gc-write-barrier.lock$'; then break; fi
    sleep 1
done
sudo lslocks -n -o COMMAND,TYPE,MODE,PATH | grep -E '^flock +FLOCK +WRITE +/var/lib/shardline/.gc-write-barrier.lock$'
sudo systemctl start --no-block shardline-gc.service
GUEST
for attempt in $(seq 1 30); do
    if history > "$fixture/before.json" && jq -e '(.tasks | length) == 3 and ([.tasks[] | select(.state == "running")] | length) == 1' "$fixture/before.json" >/dev/null; then break; fi
    sleep 1
done
run_id=$(jq -er '.tasks[] | select(.state == "running") | .id' "$fixture/before.json")
[[ "$run_id" =~ ^[a-f0-9]{32}$ ]]
vm_ssh "sudo find /var/lib/shardline/chunks/_shardline_tasks/v1/runs/$run_id/records -type f -exec sha256sum {} \\; | sort; sync" > "$fixture/records-before"
test "$(wc -l < "$fixture/records-before")" -eq 3
printf 'system_reset\n' | socat - "UNIX-CONNECT:$fixture/monitor.sock" > "$fixture/reset.log"
wait_for_guest "$fixture/boot-after"
test "$(head -n 1 "$fixture/boot-before")" != "$(head -n 1 "$fixture/boot-after")"
test "$(tail -n 1 "$fixture/boot-before")" = "$(tail -n 1 "$fixture/boot-after")"
vm_ssh 'bash -s' > "$fixture/recovery.log" 2>&1 <<'GUEST'
set -euo pipefail
sudo mkdir -p /var/lib/shardline/chunks/ee
printf orphan-after-reboot | sudo tee /var/lib/shardline/chunks/ee/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee >/dev/null
sudo chown -R fixture:fixture /var/lib/shardline/chunks/ee
sudo systemd-run --wait --pipe --unit shardline-validation-fsck -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline fsck
sudo systemd-run --unit shardline-validation-server -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve
test "$(cat /var/lib/shardline/chunks/ee/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)" = orphan-after-reboot
test "$(sudo sqlite3 -readonly /var/lib/shardline/metadata.sqlite3 'SELECT count(*) FROM shardline_quarantine_candidates')" -eq 0
for file in /var/lib/shardline/.shardline-maintenance/*/tasks.sqlite; do
    test "$(sudo sqlite3 -readonly "$file" 'SELECT count(*) FROM penelope_records_v1')" -eq 3
done
GUEST
for attempt in $(seq 1 20); do
    if history > "$fixture/after.json"; then break; fi
    sleep 1
done
jq -e --arg id "$run_id" '(.tasks | length) == 4 and ([.tasks[] | select(.id == $id and .state == "needs_reconciliation")] | length) == 1 and all(.tasks[]; if .id == $id then .state == "needs_reconciliation" else .state == "completed" end)' "$fixture/after.json"
vm_ssh "sudo find /var/lib/shardline/chunks/_shardline_tasks/v1/runs/$run_id/records -type f -exec sha256sum {} \\; | sort" > "$fixture/records-after"
cmp "$fixture/records-before" "$fixture/records-after"
# Positive control: the orphan really is discoverable by GC. Only an explicit
# new invocation may mark it; the old run must remain unresolved afterward.
vm_ssh 'sudo systemd-run --wait --pipe --unit shardline-validation-fresh-gc -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline gc --mark' > "$fixture/fresh-gc.log" 2>&1
grep -Fx 'new_quarantine_candidates: 1' "$fixture/fresh-gc.log"
history > "$fixture/final.json"
jq -e --arg id "$run_id" '(.tasks | length) == 5 and ([.tasks[] | select(.id == $id and .state == "needs_reconciliation")] | length) == 1 and all(.tasks[]; if .id == $id then .state == "needs_reconciliation" else .state == "completed" end)' "$fixture/final.json"
# Hold only completion arbitration, allowing the actual GC mutation and report.
vm_ssh 'bash -s' > "$fixture/report-admission.log" 2>&1 <<'GUEST'
set -euo pipefail
sudo mkdir -p /var/lib/shardline/chunks/aa
printf orphan-before-report-crash | sudo tee /var/lib/shardline/chunks/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa >/dev/null
sudo chown -R fixture:fixture /var/lib/shardline/chunks/aa
sudo systemd-run --unit shardline-validation-barrier flock --exclusive /var/lib/shardline/.gc-write-barrier.lock sleep infinity
for attempt in $(seq 1 20); do
    if sudo lslocks -n -o COMMAND,TYPE,MODE,PATH | grep -E '^flock +FLOCK +WRITE +/var/lib/shardline/.gc-write-barrier.lock$'; then break; fi
    sleep 1
done
sudo lslocks -n -o COMMAND,TYPE,MODE,PATH | grep -E '^flock +FLOCK +WRITE +/var/lib/shardline/.gc-write-barrier.lock$'
sudo systemctl start --no-block shardline-gc.service
GUEST
for attempt in $(seq 1 30); do
    if history > "$fixture/report-before.json" && jq -e '(.tasks | length) == 6 and ([.tasks[] | select(.state == "running")] | length) == 1' "$fixture/report-before.json" >/dev/null; then break; fi
    sleep 1
done
report_id=$(jq -er '.tasks[] | select(.state == "running") | .id' "$fixture/report-before.json")
[[ "$report_id" =~ ^[a-f0-9]{32}$ ]]
report_pid=$(jq -er '.tasks[] | select(.state == "running") | .owner.pid' "$fixture/report-before.json")
[[ "$report_pid" =~ ^[0-9]+$ ]]
vm_ssh "bash -s -- $report_id $report_pid" > "$fixture/mutation-and-report.log" 2>&1 <<'GUEST'
set -euo pipefail
id=$1
pid=$2
test "$(sudo systemctl show shardline-gc.service -p MainPID --value)" = "$pid"
directory=''
for identity in /var/lib/shardline/.shardline-maintenance/*/identity.json; do
    if sudo grep -Fq "\"id\":\"$id\"" "$identity"; then directory=$(dirname "$identity"); break; fi
done
test -n "$directory"
test "$(sudo sqlite3 -readonly "$directory/tasks.sqlite" 'SELECT count(*) FROM penelope_records_v1')" -eq 3
sudo tee /tmp/block-completion.sql >/dev/null <<'SQL'
.bail on
.timeout 5000
BEGIN IMMEDIATE;
.shell touch /run/shardline-completion-blocked
.shell sleep infinity
SQL
sudo systemd-run --unit shardline-validation-completion-writer bash -c "exec sqlite3 '$directory/tasks.sqlite' < /tmp/block-completion.sql"
for attempt in $(seq 1 100); do
    if test -f /run/shardline-completion-blocked; then break; fi
    sleep 0.05
done
test -f /run/shardline-completion-blocked
sudo systemctl stop shardline-validation-barrier
body=''
for attempt in $(seq 1 80); do
    body=$(sudo grep -lFx 'maintenance.success: true' "$directory"/artifacts/* || true)
    if test -n "$body"; then
        # Pause this owned process before its five-second SQLite busy timeout;
        # the reboot, rather than a graceful exit, must interrupt completion.
        sudo kill -STOP "$pid"
        break
    fi
    sleep 0.025
done
test -n "$body"
test "$(ps -o stat= -p "$pid" | cut -c1)" = T
sudo journalctl -u shardline-gc.service -o cat --no-pager | grep -Fx 'new_quarantine_candidates: 1'
sudo sqlite3 -readonly -json /var/lib/shardline/metadata.sqlite3 "SELECT * FROM shardline_quarantine_candidates WHERE object_key='aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'" > /var/tmp/report-candidate-before.json
test -s /var/tmp/report-candidate-before.json
sudo cat "$body"
sudo sha256sum "$body" > /var/tmp/report-body-before.sha256
sudo find "/var/lib/shardline/chunks/_shardline_tasks/v1/runs/$id/records" -type f -exec sha256sum {} \; | sort > /var/tmp/report-records-before.sha256
test "$(wc -l < /var/tmp/report-records-before.sha256)" -eq 3
printf '%s\n' "$directory" > /var/tmp/report-directory
sync
GUEST
printf 'system_reset\n' | socat - "UNIX-CONNECT:$fixture/monitor.sock" > "$fixture/report-reset.log"
wait_for_guest "$fixture/report-boot-after"
test "$(head -n 1 "$fixture/boot-after")" != "$(head -n 1 "$fixture/report-boot-after")"
test "$(tail -n 1 "$fixture/boot-after")" = "$(tail -n 1 "$fixture/report-boot-after")"
vm_ssh "bash -s -- $report_id" > "$fixture/report-recovery.log" 2>&1 <<'GUEST'
set -euo pipefail
id=$1
sudo mkdir -p /var/lib/shardline/chunks/bb
printf orphan-after-report-reboot | sudo tee /var/lib/shardline/chunks/bb/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb >/dev/null
sudo chown -R fixture:fixture /var/lib/shardline/chunks/bb
sudo systemd-run --wait --pipe --unit shardline-validation-fsck -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline fsck
sudo systemd-run --unit shardline-validation-server -p EnvironmentFile=/etc/shardline/shardline.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve
sudo sqlite3 -readonly -json /var/lib/shardline/metadata.sqlite3 "SELECT * FROM shardline_quarantine_candidates WHERE object_key='aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'" > /var/tmp/report-candidate-after.json
cmp /var/tmp/report-candidate-before.json /var/tmp/report-candidate-after.json
test "$(sudo sqlite3 -readonly /var/lib/shardline/metadata.sqlite3 "SELECT count(*) FROM shardline_quarantine_candidates WHERE object_key='bb/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'")" -eq 0
test "$(cat /var/lib/shardline/chunks/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)" = orphan-before-report-crash
test "$(cat /var/lib/shardline/chunks/bb/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)" = orphan-after-report-reboot
directory=$(cat /var/tmp/report-directory)
test "$(sudo sqlite3 -readonly "$directory/tasks.sqlite" 'SELECT count(*) FROM penelope_records_v1')" -eq 3
sudo sha256sum -c /var/tmp/report-body-before.sha256
sudo find "/var/lib/shardline/chunks/_shardline_tasks/v1/runs/$id/records" -type f -exec sha256sum {} \; | sort > /var/tmp/report-records-after.sha256
cmp /var/tmp/report-records-before.sha256 /var/tmp/report-records-after.sha256
GUEST
for attempt in $(seq 1 20); do
    if history > "$fixture/report-after.json"; then break; fi
    sleep 1
done
jq -e --arg id "$report_id" --arg old "$run_id" '(.tasks | length) == 7 and ([.tasks[] | select((.id == $id or .id == $old) and .state == "needs_reconciliation")] | length) == 2 and all(.tasks[]; if .id == $id or .id == $old then .state == "needs_reconciliation" else .state == "completed" end)' "$fixture/report-after.json"
echo 'PASS: unchanged released systemd sandbox and real guest reboots before mutation and after mutation/report'
