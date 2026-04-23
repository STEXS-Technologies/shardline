#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
FUZZ_DIR="${ROOT_DIR}/crates/fuzz"
DEFAULT_RUNS="${SHARDLINE_FUZZ_RUNS:-20000}"

default_fuzz_target() {
    local host
    host="$(rustc +nightly -vV | sed -n 's/^host: //p')"

    case "${host}" in
        x86_64-unknown-linux-musl)
            printf '%s\n' x86_64-unknown-linux-gnu
            ;;
        *)
            printf '%s\n' "${host}"
            ;;
    esac
}

FUZZ_TARGET="${SHARDLINE_FUZZ_TARGET:-$(default_fuzz_target)}"

list_targets() {
    cargo +nightly fuzz list --fuzz-dir "${FUZZ_DIR}"
}

run_target() {
    if [ "$#" -lt 1 ]; then
        printf 'usage: %s run <target> [-- <libfuzzer args>]\n' "${0##*/}" >&2
        exit 2
    fi

    exec cargo +nightly fuzz run --fuzz-dir "${FUZZ_DIR}" --target "${FUZZ_TARGET}" "$@"
}

run_smoke() {
    mapfile -t targets < <(list_targets)

    if [ "${#targets[@]}" -eq 0 ]; then
        printf 'no shardline fuzz targets found under %s\n' "${FUZZ_DIR}" >&2
        exit 1
    fi

    for target in "${targets[@]}"; do
        printf '==> %s\n' "${target}"
        cargo +nightly fuzz run --fuzz-dir "${FUZZ_DIR}" --target "${FUZZ_TARGET}" "${target}" -- "-runs=${DEFAULT_RUNS}"
    done
}

main() {
    local command="${1:-smoke}"

    case "${command}" in
        list)
            list_targets
            ;;
        run)
            shift
            run_target "$@"
            ;;
        smoke)
            shift
            run_smoke "$@"
            ;;
        *)
            printf 'unknown command: %s\n' "${command}" >&2
            printf 'usage: %s [list|run|smoke]\n' "${0##*/}" >&2
            exit 2
            ;;
    esac
}

main "$@"
