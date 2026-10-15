#!/usr/bin/env bash
# Sourced by verify_maintenance_cross_host.sh after isolated VMs are provisioned.
# All traffic uses each guest's Shardline HTTP frontend, never MinIO directly.
set -Eeuo pipefail
trap 'echo "FAIL: concurrent VM writes at line $LINENO; diagnostics: $fixture" >&2' ERR
sha256sum "$current" > "$fixture/binary.sha256"
printf '%s\n' 0123456789abcdef0123456789abcdef > "$fixture/signing.key"
token=$("$current" admin token --issuer local --subject concurrent-vm-drill --scope write \
    --provider generic --owner team --repo concurrent --revision main --key-file "$fixture/signing.key")
rm "$fixture/signing.key"
objects_per_node=24
mkdir "$fixture/payloads"
: > "$fixture/expected.tsv"
for node in "${nodes[@]}"; do
    vm_ssh "$node" "sudo sed 's/SHARDLINE_SERVER_FRONTENDS=xet/SHARDLINE_SERVER_FRONTENDS=s3/' /etc/shardline/shardline.env | sudo tee /etc/shardline/shardline-s3.env >/dev/null; sudo chmod 0600 /etc/shardline/shardline-s3.env; sudo systemd-run --unit shardline-validation-writer -p EnvironmentFile=/etc/shardline/shardline-s3.env -p User=fixture -p Group=fixture /usr/local/bin/shardline serve"
    for ((object=0; object<objects_per_node; object++)); do
        key="node-$node-object-$object"
        dd if=/dev/urandom of="$fixture/payloads/$key" bs=1048576 count=1 status=none
        printf '%s\t%s\n' "$key" "$(sha256sum "$fixture/payloads/$key" | cut -d ' ' -f 1)" >> "$fixture/expected.tsv"
    done
done
ready_all() {
    local node attempt
    for node in "${nodes[@]}"; do
        for attempt in $(seq 1 30); do
            if curl -fsS --max-time 5 -H 'Authorization: Bearer maintenance-cross-host-token' \
                "http://127.0.0.1:${http_ports[$node]}/api/v1/status" > "$fixture/$node/status.json" \
                && jq -e '.state == "ready"' "$fixture/$node/status.json" >/dev/null; then break; fi
            sleep 1
        done
        jq -e '.state == "ready"' "$fixture/$node/status.json" >/dev/null
    done
}
request_put() {
    local node=$1 key=$2 payload=$3 phase=$4 status started finished
    started=$(date +%s%N)
    status=$(curl -sS --max-time 60 --limit-rate 512K -o "$fixture/$node/put-response" -w '%{http_code}' \
        -H "Authorization: Bearer $token" -H 'Content-Type: application/octet-stream' \
        --upload-file "$payload" "http://127.0.0.1:${http_ports[$node]}/team.concurrent/$key")
    finished=$(date +%s%N)
    printf '%s\t%s\t%s\t%s\t%s\n' "$phase" "$key" "$started" "$finished" "$status" >> "$fixture/$node/writes.tsv"
    test "$status" = 200 || { cat "$fixture/$node/put-response" >&2; return 1; }
}
wait_clients() {
    local pid failed=0
    for pid in "${client_pids[@]}"; do wait "$pid" || failed=1; done
    client_pids=()
    test "$failed" = 0
}
assert_overlap() {
    local phase=$1
    # A common intersection of the first request intervals proves every guest
    # was handling an in-flight upload at the same time. HTTP 200 is mandatory.
    awk -F '\t' -v phase="$phase" -v count="$node_count" '
        $1 == phase && !seen[FILENAME]++ {
            if ($3 > latest_start) latest_start=$3;
            if (!earliest_end || $4 < earliest_end) earliest_end=$4;
            n++
        }
        END { if (n != count || latest_start >= earliest_end) exit 1;
              printf "PASS: %d VM upload intervals overlap in %s\n", n, phase }
    ' "$fixture"/*/writes.tsv
}
ready_all
for node in "${nodes[@]}"; do
    (
        for ((object=0; object<objects_per_node; object++)); do
            key="node-$node-object-$object"
            request_put "$node" "$key" "$fixture/payloads/$key" unique
        done
    ) > "$fixture/$node/writer.log" 2>&1 &
    client_pids+=("$!")
done
wait_clients
assert_overlap unique
for phase in identical competing; do
    for node in "${nodes[@]}"; do
        payload="$fixture/payloads/node-$node-object-0"
        if test "$phase" = identical; then payload="$fixture/payloads/node-0-object-0"; fi
        request_put "$node" "shared-$phase" "$payload" "$phase" > "$fixture/$node/$phase.log" 2>&1 &
        client_pids+=("$!")
    done
    wait_clients
    assert_overlap "$phase"
done
verify_all() {
    local phase=$1 node
    for node in "${nodes[@]}"; do
        (
            checked=0
            while IFS=$'\t' read -r key expected; do
                curl -fsS --max-time 30 -H "Authorization: Bearer $token" \
                    "http://127.0.0.1:${http_ports[$node]}/team.concurrent/$key" -o "$fixture/$node/readback"
                test "$(sha256sum "$fixture/$node/readback" | cut -d ' ' -f 1)" = "$expected"
                checked=$((checked+1))
            done < "$fixture/expected.tsv"
            for kind in identical competing; do
                curl -fsS --max-time 30 -H "Authorization: Bearer $token" \
                    "http://127.0.0.1:${http_ports[$node]}/team.concurrent/shared-$kind" -o "$fixture/$node/readback"
                sha256sum "$fixture/$node/readback" | cut -d ' ' -f 1 > "$fixture/$node/$phase-$kind.sha256"
            done
            echo "PASS: VM $node verified $checked distinct objects in $phase"
        ) > "$fixture/$node/verify-$phase.log" 2>&1 &
        client_pids+=("$!")
    done
    wait_clients
    for node in "${nodes[@]}"; do
        cmp "$fixture/0/$phase-competing.sha256" "$fixture/$node/$phase-competing.sha256"
        test "$(cat "$fixture/$node/$phase-identical.sha256")" = "$(sha256sum "$fixture/payloads/node-0-object-0" | cut -d ' ' -f 1)"
        cat "$fixture/$node/verify-$phase.log"
    done
    winner=$(cat "$fixture/0/$phase-competing.sha256")
    matched=0
    for node in "${nodes[@]}"; do
        if test "$winner" = "$(sha256sum "$fixture/payloads/node-$node-object-0" | cut -d ' ' -f 1)"; then matched=$((matched+1)); fi
    done
    test "$matched" = 1
}
verify_all before-restart
test "$(sql 'SELECT count(*) > 0 FROM shardline_file_records')" = t
sql 'SELECT record_kind, count(*) FROM shardline_file_records GROUP BY record_kind ORDER BY record_kind' > "$fixture/database-records.tsv"
assert_no_payload_history
test -s "$fixture/payload-inventory.log"
for node in "${nodes[@]}"; do vm_ssh "$node" 'sudo systemctl restart shardline-validation-writer'; done
ready_all
verify_all after-restart
cmp "$fixture/0/before-restart-competing.sha256" "$fixture/0/after-restart-competing.sha256"
test "$(cat "$fixture"/*/writes.tsv | wc -l)" -eq "$((node_count * (objects_per_node + 2)))"
echo "PASS: $((node_count * (objects_per_node + 2))) acknowledged uploads through $node_count VMs; all payload hashes match through every VM before and after restart"
rm -rf "$fixture/payloads"
