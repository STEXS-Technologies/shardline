#!/usr/bin/env bash
set -euo pipefail

cargo build --locked --release -p shardline

binary_path="target/release/shardline"
if [[ ! -x "${binary_path}" ]]; then
  echo "expected release binary at ${binary_path}" >&2
  exit 1
fi

"${binary_path}" --help >/dev/null
"${binary_path}" manpage --output /tmp/shardline.1
"${binary_path}" completion bash --output /tmp/shardline.bash
