#!/usr/bin/env bash
# Fetch the immutable executable used by upgrade and distributed-integrity gates.
set -euo pipefail
test "$#" -eq 1 || { echo 'usage: fetch_compatibility_baseline.sh OUTPUT_DIRECTORY' >&2; exit 2; }
test "$(uname -s)" = Linux && test "$(uname -m)" = x86_64
umask 077
mkdir -p "$1"
output=$(realpath "$1")
# Historical layout-transition contracts default to 1.12.1. Adjacent-release
# campaigns must explicitly select their predecessor; never silently substitute
# a release with different feature or identity semantics.
version=${SHARDLINE_COMPATIBILITY_BASELINE_VERSION:-1.12.1}
case "$version" in
    1.13.0) checksum=204483cbcfe6cd3947d1eab91d6ee9091432c60454f98efd0d74109e9f2c3214 ;;
    1.12.1) checksum=12e82289b9a3152a35a6dacb4aef1b06e2fd76971066cab2ebfcbfb096dff12f ;;
    1.11.3) checksum=c16b8e281bd08f51f24ee482b4808be809e30052a4c5b3df071b665e497209b1 ;;
    *) echo "Unsupported compatibility baseline: $version" >&2; exit 2 ;;
esac
archive="shardline-$version-x86_64-unknown-linux-gnu.tar.gz"
# Keep a stalled provider or retry sequence from hanging release qualification.
command -v timeout >/dev/null || { echo 'Missing required tool: timeout' >&2; exit 1; }
timeout --kill-after=15s 600s curl --fail --silent --show-error --location --retry 3 \
    --connect-timeout 15 --max-time 300 \
    "https://github.com/STEXS-Technologies/shardline/releases/download/v$version/$archive" \
    --output "$output/$archive"
printf '%s  %s\n' "$checksum" \
    "$output/$archive" | sha256sum --check > "$output/released-checksum.log"
tar -xzf "$output/$archive" -C "$output"
released="$output/shardline-$version-x86_64-unknown-linux-gnu/shardline"
test -x "$released"
# The release executable is also mounted into non-root compatibility containers.
chmod 0755 "$released"
printf '%s\n' "$released"
