#!/usr/bin/env bash
# Deploy the scaled Kubernetes profile to a disposable kind cluster and run its smoke test.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
namespace="shardline-kind-smoke"
image="shardline-kind-smoke:latest"
cluster_suffix="${GITHUB_RUN_ID:-$(date +%s)}-${RANDOM}"
cluster_name="${SHARDLINE_KIND_CLUSTER_NAME:-shardline-smoke-${cluster_suffix}}"
log_dir="${SHARDLINE_KIND_LOG_DIR:-$(mktemp -d -t shardline-kind-smoke.XXXXXX)}"
api_port=""
transfer_port=""
api_port_forward_pid=""
transfer_port_forward_pid=""

required_commands=(docker kind kubectl cargo python3)
for command in "${required_commands[@]}"; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 1
    }
done

if kind get clusters | grep -Fxq "$cluster_name"; then
    echo "refusing to reuse existing kind cluster: $cluster_name" >&2
    exit 1
fi
if docker image inspect "$image" >/dev/null 2>&1; then
    echo "refusing to replace existing smoke image: $image" >&2
    exit 1
fi

reserve_port() {
    python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_:
    socket_.bind(("127.0.0.1", 0))
    print(socket_.getsockname()[1])
PY
}

collect_diagnostics() {
    mkdir -p "$log_dir"
    kubectl --context "kind-${cluster_name}" get all -A >"$log_dir/resources.txt" 2>&1 || true
    kubectl --context "kind-${cluster_name}" -n "$namespace" describe pods >"$log_dir/pods.txt" 2>&1 || true
    kubectl --context "kind-${cluster_name}" -n "$namespace" logs deployment/shardline-api --all-containers=true \
        >"$log_dir/shardline-api.log" 2>&1 || true
    kubectl --context "kind-${cluster_name}" -n "$namespace" logs deployment/shardline-transfer --all-containers=true \
        >"$log_dir/shardline-transfer.log" 2>&1 || true
    kind export logs "$log_dir/kind" --name "$cluster_name" >/dev/null 2>&1 || true
}

cleanup() {
    local exit_code=$?
    trap - EXIT
    set +e

    for pid in "$api_port_forward_pid" "$transfer_port_forward_pid"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [[ "$exit_code" -ne 0 ]]; then
        collect_diagnostics
        echo "kind smoke diagnostics retained in $log_dir" >&2
    else
        rm -rf "$log_dir"
    fi
    kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
    docker image rm -f "$image" >/dev/null 2>&1 || true

    if kind get clusters | grep -Fxq "$cluster_name"; then
        echo "failed to delete kind cluster: $cluster_name" >&2
        exit 1
    fi
    exit "$exit_code"
}
trap cleanup EXIT

cd "$repo_root"
echo "Building $image"
docker build --tag "$image" .

echo "Creating kind cluster $cluster_name"
kind create cluster --name "$cluster_name" --wait 3m
kind load docker-image "$image" --name "$cluster_name"

context="kind-${cluster_name}"
echo "Deploying in-cluster dependencies"
kubectl --context "$context" apply -f tests/k8s/kind/dependencies.yaml
for deployment in postgres redis minio; do
    kubectl --context "$context" -n "$namespace" rollout status "deployment/${deployment}" --timeout=180s
done
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/minio-create-bucket --timeout=180s
kubectl --context "$context" apply -f tests/k8s/kind/migration-job.yaml
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/shardline-db-migrate --timeout=180s

echo "Deploying the scaled Shardline profile"
kubectl --context "$context" apply -k tests/k8s/kind
for deployment in shardline-api shardline-transfer; do
    kubectl --context "$context" -n "$namespace" rollout status "deployment/${deployment}" --timeout=240s
done

api_port="$(reserve_port)"
transfer_port="$(reserve_port)"
kubectl --context "$context" -n "$namespace" port-forward "service/shardline-api" "${api_port}:8080" \
    >"$log_dir/api-port-forward.log" 2>&1 &
api_port_forward_pid=$!
kubectl --context "$context" -n "$namespace" port-forward "service/shardline-transfer" "${transfer_port}:8080" \
    >"$log_dir/transfer-port-forward.log" 2>&1 &
transfer_port_forward_pid=$!

SHARDLINE_KIND_SMOKE_API_URL="http://127.0.0.1:${api_port}" \
SHARDLINE_KIND_SMOKE_TRANSFER_URL="http://127.0.0.1:${transfer_port}" \
cargo nextest run --manifest-path e2e/Cargo.toml --test kind_deployment_smoke --run-ignored ignored-only

echo "kind deployment smoke test passed; deleting $cluster_name"
