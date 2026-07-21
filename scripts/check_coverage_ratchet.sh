#!/usr/bin/env bash
set -euo pipefail

REPORT_PATH="${1:-target/coverage.json}"
BASELINE_PATH="${2:-ci/coverage-baseline.json}"

if ! command -v jq >/dev/null 2>&1; then
    printf 'jq is required to validate the coverage report\n' >&2
    exit 1
fi

if [ ! -f "${REPORT_PATH}" ]; then
    printf 'coverage report not found: %s\n' "${REPORT_PATH}" >&2
    exit 1
fi

if [ ! -f "${BASELINE_PATH}" ]; then
    printf 'coverage baseline not found: %s\n' "${BASELINE_PATH}" >&2
    exit 1
fi

baseline="$(jq -er '.line_coverage_percent | numbers' "${BASELINE_PATH}")"
actual="$(jq -er '
    [
        .data[]?.totals.lines?
        | select(.count != null and .count > 0)
    ] as $totals
    | if ($totals | length) == 0 then
        error("no line coverage totals in report")
      else
        ([ $totals[] | .covered ] | add) / ([ $totals[] | .count ] | add) * 100
      end
' "${REPORT_PATH}")"

printf 'Line coverage: %.4f%% (ratchet: %.4f%%)\n' "${actual}" "${baseline}"
if ! awk -v actual="${actual}" -v baseline="${baseline}" 'BEGIN { exit actual + 0.0001 < baseline }'; then
    printf 'coverage regression: %.4f%% is below %.4f%%\n' "${actual}" "${baseline}" >&2
    exit 1
fi
