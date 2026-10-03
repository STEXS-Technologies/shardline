#!/usr/bin/env bash
set -euo pipefail

release_smoke_dir="$(mktemp -d)"
trap 'rm -rf -- "${release_smoke_dir}"' EXIT

# Let Cargo select the built executable, including configured target directories
# and runners, instead of accidentally checking a stale binary in target/.
cargo run --locked --release -p shardline -- --help >/dev/null
cargo run --locked --release -p shardline -- manpage --output "${release_smoke_dir}/shardline.1"
cargo run --locked --release -p shardline -- completion bash --output "${release_smoke_dir}/shardline.bash"
