#!/usr/bin/env bash
# Compare ordinary command failures using owned configuration and root fixtures.
# Usage: bash scripts/shardline/verify_maintenance_invalid_configuration.sh CURRENT RELEASED
set -euo pipefail
test "$#" -eq 2 || { echo 'usage: verify_maintenance_invalid_configuration.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current"
test -x "$released"
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-invalid-config.XXXXXX")
echo "diagnostics: $fixture"
cp -- "$current" "$fixture/current-shardline"
cp -- "$released" "$fixture/released-shardline"
current=$fixture/current-shardline
released=$fixture/released-shardline
printf '[server\ninvalid' > "$fixture/invalid.toml"
printf '' > "$fixture/valid.toml"
printf 'original root bytes' > "$fixture/root-file"
for problem in invalid-config missing-config missing-env-file root-file; do
  for operation in fsck rebuild repair repair-lifecycle gc; do
    case "$operation" in
      fsck) arguments=(fsck);;
      rebuild) arguments=(index rebuild);;
      repair) arguments=(repair);;
      repair-lifecycle) arguments=(repair lifecycle);;
      gc) arguments=(gc --mark);;
    esac
    case "$problem" in
      invalid-config) arguments+=(--config "$fixture/invalid.toml" --root "$fixture/absent-root");;
      missing-config) arguments+=(--config "$fixture/missing.toml" --root "$fixture/absent-root");;
      missing-env-file) arguments+=(--env-file "$fixture/missing.env" --config "$fixture/valid.toml" --root "$fixture/absent-root");;
      root-file) arguments+=(--config "$fixture/valid.toml" --root "$fixture/root-file");;
    esac
    for revision in released current; do
      binary=$released
      if test "$revision" = current; then binary=$current; fi
      status=0
      env -i "$binary" "${arguments[@]}" > "$fixture/$problem-$operation-$revision.stdout" 2> "$fixture/$problem-$operation-$revision.stderr" || status=$?
      test "$status" -ne 0
      printf '%s\n' "$status" > "$fixture/$problem-$operation-$revision.status"
    done
    for stream in stdout stderr status; do
      cmp "$fixture/$problem-$operation-released.$stream" "$fixture/$problem-$operation-current.$stream"
    done
    test ! -e "$fixture/absent-root"
    test "$(cat "$fixture/root-file")" = 'original root bytes'
    echo "PASS: $problem $operation"
  done
done
