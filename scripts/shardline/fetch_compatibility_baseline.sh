#!/usr/bin/env bash
# Fetch the immutable executable used by upgrade and distributed-integrity gates.
set -euo pipefail
test "$#" -eq 1 || { echo 'usage: fetch_compatibility_baseline.sh OUTPUT_DIRECTORY' >&2; exit 2; }
test "$(uname -s)" = Linux && test "$(uname -m)" = x86_64
umask 077
mkdir -p "$1"
output=$(realpath "$1")
archive=shardline-1.11.3-x86_64-unknown-linux-gnu.tar.gz
curl --fail --silent --show-error --location --retry 3 \
    "https://github.com/STEXS-Technologies/shardline/releases/download/v1.11.3/$archive" \
    --output "$output/$archive"
printf '%s  %s\n' c16b8e281bd08f51f24ee482b4808be809e30052a4c5b3df071b665e497209b1 \
    "$output/$archive" | sha256sum --check > "$output/released-checksum.log"
tar -xzf "$output/$archive" -C "$output"
released="$output/shardline-1.11.3-x86_64-unknown-linux-gnu/shardline"
test -x "$released"
printf '%s\n' "$released"
