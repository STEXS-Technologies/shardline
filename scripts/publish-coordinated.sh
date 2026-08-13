#!/usr/bin/env bash
# Publish every publishable crate in the Shardline workspace to crates.io in a
# coordinated bottom-up (dependencies-first) release to v1.4.0.
#
# WHY BOTTOM-UP (hard requirement):
#   * The workspace rewrites every path dep to a crates.io requirement ("^1.4.0").
#   * A crate's tarball can only be verified/compiled once every sibling it depends
#     on is already on crates.io at ^1.4.0.
#   * CONCRETE BLOCKER (verified): `sdx` depends on `shardline-xet-adapter`; the
#     published 1.3.0 adapter lacks the M5a route constants
#     (XET_TREE_ROUTE/XET_PATH_ROUTE/XET_REVISIONS_ROUTE/XET_REVISION_ROUTE) that sdx
#     imports. The adapter MUST be published (at 1.4.0, with the constants) BEFORE sdx,
#     or the sdx tarball fails to compile.
#
# EXCLUDED (do not publish):
#   shardline-fuzz      (publish = false)
#   shardline-loom-tests(publish = false)
#   shardline-bench     (bench/load-test crate; not part of the coordinated release)
#
# USAGE
#   ./scripts/publish-coordinated.sh            # dry-run every crate in order (no upload)
#   ./scripts/publish-coordinated.sh --dry-run  # same as above
#   ./scripts/publish-coordinated.sh --go       # ACTUALLY publish each crate, in order
#
# The script NEVER publishes unless --go is given. Each crate is published with
# `--allow-dirty` (the workspace always has uncommitted Cargo.lock/version-bump churn).
set -euo pipefail

# Verified bottom-up publish order (dependency first). Derive-verified against
# `cargo metadata` for v1.4.0; do not reorder without re-deriving the graph.
PUBLISH_ORDER=(
  shardline-metrics
  shardline-protocol
  shardline-test-support
  shardline-validation
  shardline-xet-core
  shardline-auth
  shardline-cache
  shardline-storage
  shardline-vcs
  shardline-index
  shardline-cas
  shardline-server-core
  shardline-protocol-adapters   # MUST land before shardline-hub-api (hub-api depends on it)
  shardline-hub-api
  shardline-oci-adapter
  shardline-xet-adapter         # MUST land before sdx (M5a route constants)
  shardline-fsck
  shardline-gc
  shardline-provider-events
  shardline-rebuild
  shardline-server              # MUST land before sdx (sdx dev-depends on it for its test suite)
  sdx                           # MUST be after shardline-xet-adapter AND shardline-server
  shardline                     # CLI bin; depends on sdx, so last
)

MODE="dry-run"
if [[ "${1:-}" == "--go" ]]; then
  MODE="go"
elif [[ -n "${1:-}" ]] && [[ "${1:-}" != "--dry-run" ]]; then
  echo "unknown argument: ${1}" >&2
  echo "usage: $0 [--dry-run | --go]" >&2
  exit 2
fi

echo "== Shardline coordinated crates.io release (v1.4.0) =="
echo "== mode: ${MODE} =="
echo "== order (bottom-up, ${#PUBLISH_ORDER[@]} crates) =="
for i in "${!PUBLISH_ORDER[@]}"; do
  n=$((i + 1))
  crate="${PUBLISH_ORDER[$i]}"
  echo "   ${n}. ${crate}"
done
echo

if [[ "${MODE}" == "dry-run" ]]; then
  echo ">> DRY RUN: nothing will be uploaded."
  echo ">> Each crate below is packaged and verified locally; upload is aborted."
  echo
fi

for i in "${!PUBLISH_ORDER[@]}"; do
  n=$((i + 1))
  crate="${PUBLISH_ORDER[$i]}"
  echo "======================================================================"
  echo "[${n}/${#PUBLISH_ORDER[@]}] ${crate} (${MODE})"
  echo "======================================================================"
  if [[ "${MODE}" == "go" ]]; then
    cargo publish -p "${crate}" --allow-dirty
  else
    cargo publish -p "${crate}" --dry-run --allow-dirty
  fi
  echo
done

if [[ "${MODE}" == "dry-run" ]]; then
  cat <<'EOF'
======================================================================
DRY-RUN SUMMARY
======================================================================
Expected results (verified at v1.4.0):
  * shardline-metrics / protocol / test-support / validation / xet-core
    -> PASS (no shardline-* normal deps; package + verify locally).
  * Every other crate (incl. sdx) -> BLOCKED-BY until its shardline-*
    dependencies are actually on crates.io at ^1.4.0. Re-run the dry-run
    for a crate AFTER its dependencies publish; it will then package and
    verify successfully.

To actually release: run `./scripts/publish-coordinated.sh --go`.
Between publishes, cargo verifies each tarball; if one fails, fix and
re-run from that crate (dependencies already published stay put).
EOF
fi
