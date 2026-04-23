#!/usr/bin/env bash
set -euo pipefail

target="${1:-$(rustc -vV | sed -n 's/^host: //p')}"
out_dir="${2:-dist}"
version="$(cargo pkgid -p shardline | awk -F'[#@]' '{print $NF}')"
archive_root="shardline-${version}-${target}"
binary_name="shardline"

case "${target}" in
  *windows*)
    binary_name="shardline.exe"
    ;;
esac

cargo build --locked --release -p shardline --target "${target}"

mkdir -p "${out_dir}/${archive_root}"
cp "target/${target}/release/${binary_name}" "${out_dir}/${archive_root}/"
cp README.md LICENSE-MIT LICENSE-APACHE "${out_dir}/${archive_root}/"

if [[ "${target}" == *windows* ]]; then
  (
    cd "${out_dir}"
    zip -rq "${archive_root}.zip" "${archive_root}"
  )
  sha256sum "${out_dir}/${archive_root}.zip" > "${out_dir}/${archive_root}.zip.sha256"
else
  tar -C "${out_dir}" -czf "${out_dir}/${archive_root}.tar.gz" "${archive_root}"
  sha256sum "${out_dir}/${archive_root}.tar.gz" > "${out_dir}/${archive_root}.tar.gz.sha256"
fi
