#!/bin/sh
set -eu

root_dir="${SHARDLINE_ROOT_DIR:-/var/lib/shardline}"
local_secret_dir="${root_dir}/secrets"
local_signing_key_file="${local_secret_dir}/token-signing-key"

ensure_local_signing_key() {
  if [ -n "${SHARDLINE_TOKEN_SIGNING_KEY:-}" ] || [ -n "${SHARDLINE_TOKEN_SIGNING_KEY_FILE:-}" ]; then
    return
  fi

  mkdir -p "${local_secret_dir}"
  chmod 700 "${local_secret_dir}"

  if [ ! -s "${local_signing_key_file}" ]; then
    umask 077
    dd if=/dev/urandom bs=32 count=1 status=none | base64 > "${local_signing_key_file}"
  fi

  export SHARDLINE_TOKEN_SIGNING_KEY_FILE="${local_signing_key_file}"
}

if [ "$(id -u)" = "0" ]; then
  mkdir -p "${root_dir}"
  ensure_local_signing_key
  chown -R shardline:shardline "${root_dir}"
  exec setpriv --reuid=shardline --regid=shardline --init-groups -- shardline "$@"
fi

ensure_local_signing_key
exec shardline "$@"
