#!/usr/bin/env bash
# Publish every publishable crate in the Shardline workspace to crates.io in a
# coordinated bottom-up (dependencies-first) release.
#
# WHY BOTTOM-UP (hard requirement):
#   * The workspace rewrites every path dep to a crates.io requirement ("^<version>").
#   * A crate's tarball can only be verified/compiled once every sibling it depends
#     on is already on crates.io at the new version.
#   * DEV-DEPENDENCIES COUNT TOO (v1.5 lesson): `sdx` has a dev-dependency on
#     `shardline-server` for its test suite, so `shardline-server` MUST land before
#     `sdx` exactly like a normal dependency.
#
# The publish order is NOT maintained by hand. scripts/publish-order.py derives it
# from the `cargo metadata` dependency graph (all dependency kinds -- normal, build,
# dev -- restricted to workspace-internal path deps) and excludes `publish = false`
# crates automatically (shardline-fuzz, shardline-loom-tests). The generated
# sequence emits `publish <crate>@<version>` followed by `wait-for-index
# <crate>@<version>`; this driver executes the wait between publishes (via
# `python3 scripts/publish-order.py --wait`) so the crates.io sparse index has the
# new version before the next crate tries to resolve it.
#
# USAGE
#   ./scripts/publish-coordinated.sh            # dry-run every crate in order (no upload)
#   ./scripts/publish-coordinated.sh --dry-run  # same as above
#   ./scripts/publish-coordinated.sh --go       # ACTUALLY publish each crate, in order
#
# The script NEVER publishes unless --go is given. Each crate is published with
# `--allow-dirty` (the workspace always has uncommitted Cargo.lock/version-bump churn).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORDER_FILE="$(mktemp)"
trap 'rm -f "${ORDER_FILE}"' EXIT

# Derive the publish order from cargo metadata (read-only; emits
# "publish <crate>@<version>" + "wait-for-index <crate>@<version>" lines).
(cd "${REPO_ROOT}" && python3 scripts/publish-order.py) > "${ORDER_FILE}"

# Collect the crate@version items from the "publish " lines, in order.
declare -a PUBLISH_ITEMS=()
while IFS= read -r line; do
  case "${line}" in
    publish\ *) PUBLISH_ITEMS+=("${line#publish }") ;;
  esac
done < "${ORDER_FILE}"

MODE="dry-run"
if [[ "${1:-}" == "--go" ]]; then
  MODE="go"
elif [[ -n "${1:-}" ]] && [[ "${1:-}" != "--dry-run" ]]; then
  echo "unknown argument: ${1}" >&2
  echo "usage: $0 [--dry-run | --go]" >&2
  exit 2
fi

echo "== Shardline coordinated crates.io release =="
echo "== mode: ${MODE} =="
echo "== order (bottom-up, ${#PUBLISH_ITEMS[@]} crates, derived from cargo metadata) =="
for i in "${!PUBLISH_ITEMS[@]}"; do
  n=$((i + 1))
  echo "   ${n}. ${PUBLISH_ITEMS[$i]%@*}"
done
echo

if [[ "${MODE}" == "dry-run" ]]; then
  echo ">> DRY RUN: nothing will be uploaded."
  echo ">> Each crate below is packaged and verified locally; upload is aborted."
  echo
fi

for i in "${!PUBLISH_ITEMS[@]}"; do
  n=$((i + 1))
  item="${PUBLISH_ITEMS[$i]}"
  crate="${item%@*}"
  version="${item#*@}"
  echo "======================================================================"
  echo "[${n}/${#PUBLISH_ITEMS[@]}] ${crate}@${version} (${MODE})"
  echo "======================================================================"
  if [[ "${MODE}" == "go" ]]; then
    cargo publish -p "${crate}" --allow-dirty
    echo ">> waiting for ${crate}@${version} to appear in the crates.io index..."
    python3 "${REPO_ROOT}/scripts/publish-order.py" --wait "${crate}" "${version}"
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
Expected results:
  * Crates with no shardline-* deps (e.g. shardline-metrics, shardline-protocol)
    -> PASS (package + verify locally).
  * Every other crate -> BLOCKED-BY until its shardline-* dependencies are
    actually on crates.io at the new version. Re-run the dry-run for a crate
    AFTER its dependencies publish; it will then package and verify
    successfully.

To actually release: run `./scripts/publish-coordinated.sh --go`.
Between publishes, the driver waits for each version in the crates.io sparse
index; cargo also verifies each tarball, so if one fails, fix and re-run from
that crate (dependencies already published stay put).
EOF
fi
