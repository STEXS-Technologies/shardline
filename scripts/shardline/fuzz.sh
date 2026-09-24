#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
FUZZ_DIR="${ROOT_DIR}/crates/fuzz"
DEFAULT_RUNS="${SHARDLINE_FUZZ_RUNS:-20000}"
DEFAULT_RELIABILITY_DURATION_SECONDS="${SHARDLINE_FUZZ_DURATION_SECONDS:-3600}"
DEFAULT_RELIABILITY_RSS_LIMIT_MB="${SHARDLINE_FUZZ_RSS_LIMIT_MB:-2048}"

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

list_reliability_targets() {
    list_targets | awk '/^shardline_reliability_/ || /^shardline_resumable_(state_digest|session_state)$/'
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

run_regression() {
    mapfile -t targets < <(list_targets)

    if [ "${#targets[@]}" -eq 0 ]; then
        printf 'no shardline fuzz targets found under %s\n' "${FUZZ_DIR}" >&2
        exit 1
    fi

    for target in "${targets[@]}"; do
        corpus_dir="${FUZZ_DIR}/corpus/${target}"
        printf '==> %s\n' "${target}"
        if [ -d "${corpus_dir}" ]; then
            # `-runs=0` makes libFuzzer replay the supplied corpus without
            # starting an unbounded mutation campaign.
            cargo +nightly fuzz run --fuzz-dir "${FUZZ_DIR}" --target "${FUZZ_TARGET}" \
                "${target}" "${corpus_dir}" -- "-runs=0"
        else
            # A target without a seed corpus still receives one bounded input
            # so a new harness cannot silently stop compiling in CI.
            cargo +nightly fuzz run --fuzz-dir "${FUZZ_DIR}" --target "${FUZZ_TARGET}" \
                "${target}" -- "-runs=1"
        fi
    done
}

run_reliability() {
    if [ "$#" -gt 0 ] && [ "$1" = "--" ]; then
        shift
    fi

    local duration_seconds="${SHARDLINE_FUZZ_DURATION_SECONDS:-${DEFAULT_RELIABILITY_DURATION_SECONDS}}"
    local rss_limit_mb="${SHARDLINE_FUZZ_RSS_LIMIT_MB:-${DEFAULT_RELIABILITY_RSS_LIMIT_MB}}"
    if [[ ! "${duration_seconds}" =~ ^[1-9][0-9]*$ ]]; then
        printf 'SHARDLINE_FUZZ_DURATION_SECONDS must be a positive decimal integer\n' >&2
        exit 2
    fi
    if [[ ! "${rss_limit_mb}" =~ ^[1-9][0-9]*$ ]]; then
        printf 'SHARDLINE_FUZZ_RSS_LIMIT_MB must be a positive decimal integer\n' >&2
        exit 2
    fi

    mapfile -t reliability_targets < <(list_reliability_targets)
    if [ "${#reliability_targets[@]}" -eq 0 ]; then
        printf 'no reliability fuzz targets found under %s\n' "${FUZZ_DIR}" >&2
        exit 1
    fi

    local target=""
    if [ "$#" -gt 1 ]; then
        printf 'usage: %s reliability [target]\n' "${0##*/}" >&2
        exit 2
    elif [ "$#" -eq 1 ]; then
        target="$1"
        local target_found=false
        local candidate=""
        for candidate in "${reliability_targets[@]}"; do
            if [ "${candidate}" = "${target}" ]; then
                target_found=true
                break
            fi
        done
        if [ "${target_found}" != true ]; then
            printf 'unknown reliability fuzz target: %s\n' "${target}" >&2
            exit 2
        fi
        reliability_targets=("${target}")
    fi

    for target in "${reliability_targets[@]}"; do
        printf 'building %s\n' "${target}"
        cargo +nightly fuzz build --fuzz-dir "${FUZZ_DIR}" --target "${FUZZ_TARGET}" "${target}"
    done

    local -a pids=()
    local -a running_targets=()
    for target in "${reliability_targets[@]}"; do
        local fuzz_binary="${ROOT_DIR}/target/${FUZZ_TARGET}/release/${target}"
        if [ ! -x "${fuzz_binary}" ]; then
            printf 'built reliability fuzz binary is missing or not executable: %s\n' "${fuzz_binary}" >&2
            return 1
        fi
        printf '==> %s (%ss) [isolated process]\n' "${target}" "${duration_seconds}"
        (
            "${fuzz_binary}" \
                "-artifact_prefix=${FUZZ_DIR}/artifacts/${target}/" \
                "-max_total_time=${duration_seconds}" "-timeout=20" \
                "-rss_limit_mb=${rss_limit_mb}" "-verbosity=0" \
                "${FUZZ_DIR}/corpus/${target}"
        ) > >(sed "s/^/[${target}] /") 2> >(sed "s/^/[${target}] /" >&2) &
        pids+=("$!")
        running_targets+=("${target}")
    done

    local campaign_status=0
    local index=0
    for pid in "${pids[@]}"; do
        if ! wait "${pid}"; then
            printf 'reliability fuzz target failed: %s\n' "${running_targets[${index}]}" >&2
            campaign_status=1
        fi
        index=$((index + 1))
    done
    return "${campaign_status}"
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
        regression)
            shift
            run_regression "$@"
            ;;
        reliability-list)
            list_reliability_targets
            ;;
        reliability)
            shift
            run_reliability "$@"
            ;;
        *)
            printf 'unknown command: %s\n' "${command}" >&2
            printf 'usage: %s [list|reliability-list|run|smoke|regression|reliability]\n' "${0##*/}" >&2
            exit 2
            ;;
    esac
}

main "$@"
