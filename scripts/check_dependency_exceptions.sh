#!/usr/bin/env bash
set -euo pipefail

readonly EXCEPTIONS_FILE="${1:-deny.toml}"
readonly TODAY="$(date --iso-8601)"

awk -v today="${TODAY}" '
  /^    # Owner: / {
    owner = $3
    sub(/\.$/, "", owner)
    expiry = $5
    sub(/\.$/, "", expiry)
    next
  }
  /^    \{ id = "RUSTSEC-/ {
    match($0, /id = "([^"]+)"/, advisory)
    if (owner == "" || expiry == "") {
      printf "dependency exception %s is missing Owner/Expiry metadata\n", advisory[1] > "/dev/stderr"
      failed = 1
    } else if (expiry <= today) {
      printf "dependency exception %s expired on %s (owner: %s)\n", advisory[1], expiry, owner > "/dev/stderr"
      failed = 1
    }
    owner = ""
    expiry = ""
  }
  END { exit failed }
' "${EXCEPTIONS_FILE}"
