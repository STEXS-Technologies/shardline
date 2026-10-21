#!/usr/bin/env bash
# Qualify the documented Git LFS and OCI endpoints with upstream clients.
set -euo pipefail
cd "$(dirname "$0")/.."
git lfs version

if [[ -z "${OCI_CONFORMANCE_BIN:-}" ]]; then
  command -v go >/dev/null
  protocol_workdir="$(mktemp -d "${TMPDIR:-/tmp}/shardline-oci-conformance.XXXXXXXX")"
  trap 'rm -rf -- "$protocol_workdir"' EXIT
  # OCI Distribution v1.1.1, pinned to the commit rather than a movable tag.
  protocol_revision=a139cc423184af6078077b9b7ee336eddbd03f8f
  git -C "$protocol_workdir" init --quiet
  git -C "$protocol_workdir" fetch --quiet --depth=1 \
    https://github.com/opencontainers/distribution-spec.git "$protocol_revision"
  git -C "$protocol_workdir" checkout --quiet --detach FETCH_HEAD
  [[ "$(git -C "$protocol_workdir" rev-parse HEAD)" == "$protocol_revision" ]]
  (cd "$protocol_workdir/conformance" && go test -c -o "$protocol_workdir/conformance.test")
  export OCI_CONFORMANCE_BIN="$protocol_workdir/conformance.test"
fi
[[ -x "$OCI_CONFORMANCE_BIN" ]]

# These tests start an isolated HTTP server and use disposable local metadata.
cargo test --locked -p shardline-server --test protocol_completion -- \
  --include-ignored --skip postgres --nocapture

if [[ -n "${DATABASE_URL:-}" ]]; then
  cargo test --locked -p shardline-server --test protocol_completion postgres -- --ignored
else
  echo "PostgreSQL qualification is separate: set DATABASE_URL to a disposable database."
fi
