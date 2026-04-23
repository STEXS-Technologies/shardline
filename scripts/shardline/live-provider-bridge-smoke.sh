#!/usr/bin/env bash

set -euo pipefail

require_command() {
  local command_name
  command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$command_name" >&2
    exit 1
  fi
}

require_command gh
require_command tea
require_command git
require_command git-lfs
require_command git-xet
require_command cargo

REPO_NAME="${SHARDLINE_LIVE_BRIDGE_REPO_NAME:-shardline-bridge-live-$(date +%Y%m%d%H%M%S)}"
GITHUB_OWNER="${SHARDLINE_LIVE_GITHUB_OWNER:-Lythaeon}"
GITEA_OWNER="${SHARDLINE_LIVE_GITEA_OWNER:-stexs}"
GITEA_BASE_URL="${SHARDLINE_LIVE_GITEA_BASE_URL:-https://gitea.stexs.net}"
GITEA_LOGIN_NAME="${SHARDLINE_LIVE_GITEA_LOGIN_NAME:-Celestial}"

GITHUB_TOKEN="${SHARDLINE_LIVE_GITHUB_TOKEN:-$(gh auth token)}"
GITEA_TOKEN="${SHARDLINE_LIVE_GITEA_TOKEN:-$(sed -n 's/^      token: //p' "$HOME/.config/tea/config.yml" | head -n 1)}"

if [[ -z "$GITHUB_TOKEN" ]]; then
  printf 'could not resolve GitHub token from gh auth or SHARDLINE_LIVE_GITHUB_TOKEN\n' >&2
  exit 1
fi

if [[ -z "$GITEA_TOKEN" ]]; then
  printf 'could not resolve Gitea token from tea config or SHARDLINE_LIVE_GITEA_TOKEN\n' >&2
  exit 1
fi

gh repo create "${GITHUB_OWNER}/${REPO_NAME}" --private --disable-issues --disable-wiki >/dev/null
tea repos create \
  --name "$REPO_NAME" \
  --owner "$GITEA_OWNER" \
  --private \
  --login "$GITEA_LOGIN_NAME" >/dev/null

printf 'github_repo=%s/%s\n' "$GITHUB_OWNER" "$REPO_NAME"
printf 'gitea_repo=%s/%s\n' "$GITEA_OWNER" "$REPO_NAME"

SHARDLINE_LIVE_GITHUB_OWNER="$GITHUB_OWNER" \
SHARDLINE_LIVE_GITHUB_REPO="$REPO_NAME" \
SHARDLINE_LIVE_GITHUB_TOKEN="$GITHUB_TOKEN" \
SHARDLINE_LIVE_GITEA_BASE_URL="$GITEA_BASE_URL" \
SHARDLINE_LIVE_GITEA_OWNER="$GITEA_OWNER" \
SHARDLINE_LIVE_GITEA_REPO="$REPO_NAME" \
SHARDLINE_LIVE_GITEA_TOKEN="$GITEA_TOKEN" \
cargo test -p shardline-server --test live_provider_bridge_e2e -- --nocapture
