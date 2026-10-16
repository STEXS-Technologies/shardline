#!/usr/bin/env bash
# Compare the existing storage-copy object-size boundary using disposable roots.
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_storage_migration_size_compatibility.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-storage-size.XXXXXX")
echo "diagnostics: $fixture"
for size in 64 65; do
    source="$fixture/source-$size"
    mkdir -p "$source/chunks/size-boundary"
    truncate -s "$((size * 1024 * 1024))" "$source/chunks/size-boundary/body"
    for version in released current; do
        case "$version" in released) executable="$released";; current) executable="$current";; esac
        destination="$fixture/destination-$size-$version"
        status=0
        (cd "$fixture"; env -i "$executable" storage migrate --from local \
            --from-root "$source" --to local --to-root "$destination") \
            > "$fixture/$size-$version.stdout" 2> "$fixture/$size-$version.stderr" || status=$?
        printf '%s\n' "$status" > "$fixture/$size-$version.status"
        if test "$size" = 64; then
            test "$status" = 0
            cmp "$source/chunks/size-boundary/body" "$destination/chunks/size-boundary/body"
            rg -qF 'inserted_objects: 1' "$fixture/$size-$version.stdout"
            rg -qF 'copied_bytes: 67108864' "$fixture/$size-$version.stdout"
        else
            test "$status" != 0
            test ! -e "$destination/chunks/size-boundary/body"
            (cd "$fixture"; env -i "$executable" storage migrate --from local \
                --from-root "$source" --to local --to-root "$destination" --dry-run) \
                > "$fixture/$size-$version-dry.stdout" 2> "$fixture/$size-$version-dry.stderr"
            rg -qF 'scanned_bytes: 68157440' "$fixture/$size-$version-dry.stdout"
            rg -qF 'inserted_objects: 0' "$fixture/$size-$version-dry.stdout"
            test ! -e "$destination/chunks/size-boundary/body"
        fi
    done
    if test "$size" = 65; then
        cmp "$fixture/$size-released-dry.stdout" "$fixture/$size-current-dry.stdout"
        cmp "$fixture/$size-released-dry.stderr" "$fixture/$size-current-dry.stderr"
    fi
    for artifact in stdout stderr status; do
        cmp "$fixture/$size-released.$artifact" "$fixture/$size-current.$artifact"
    done
    echo "PASS: $size MiB object preserves released output, status and payload outcome"
done
echo 'PASS: storage-copy size boundary is unchanged; this is not a throughput benchmark'
