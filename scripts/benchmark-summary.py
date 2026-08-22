#!/usr/bin/env python3
"""Render a stable Markdown summary from one benchmark-matrix run."""

from __future__ import annotations

import json
import pathlib
import sys


def mib_per_second(value: int) -> str:
    return f"{value / (1024 * 1024):.1f}"


def percent_reused(uploaded: int, newly_stored: int) -> str:
    if uploaded <= 0:
        return "n/a"
    reused = max(0, uploaded - newly_stored)
    return f"{100 * reused / uploaded:.1f}%"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: benchmark-summary.py RUN_DIR", file=sys.stderr)
        return 2
    run_dir = pathlib.Path(sys.argv[1])
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    reports = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (run_dir / "results").glob("concurrency-*.json")
    ]
    reports.sort(key=lambda report: report["concurrency"])

    print("# Shardline benchmark matrix")
    print()
    source = metadata["commit"] + (" (dirty)" if metadata.get("dirty") else "")
    print(f"- source: `{source}`")
    print(f"- measured at: `{metadata['timestamp_utc']}`")
    print(f"- target: `{metadata['deployment_target']}`")
    print(f"- host: {metadata['platform']} ({metadata['logical_cpus']} logical CPUs)")
    print(f"- toolchain: `{metadata['rustc']}`")
    print(
        f"- fixture: {metadata['base_bytes']} bytes, "
        f"{metadata['mutated_bytes']} mutated bytes, "
        f"{metadata['chunk_size_bytes']}-byte chunks, "
        f"{metadata['iterations']} iterations"
    )
    print()
    print(
        "| clients | initial upload MiB/s | sparse upload MiB/s | latest read MiB/s "
        "| concurrent read MiB/s | concurrent upload MiB/s | upload reuse "
        "| cache cold µs | cache hot µs | CPU cores |"
    )
    print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for report in reports:
        throughput = report["throughput"]
        totals = report["totals"]
        latency = report["latency"]
        timing = report["timing"]
        print(
            f"| {report['concurrency']} "
            f"| {mib_per_second(throughput['average_initial_upload_bytes_per_second'])} "
            f"| {mib_per_second(throughput['average_sparse_update_upload_bytes_per_second'])} "
            f"| {mib_per_second(throughput['average_latest_download_bytes_per_second'])} "
            f"| {mib_per_second(throughput['average_concurrent_latest_download_bytes_per_second'])} "
            f"| {mib_per_second(throughput['average_concurrent_upload_bytes_per_second'])} "
            f"| {percent_reused(totals['total_concurrent_uploaded_bytes'], totals['total_concurrent_newly_stored_bytes'])} "
            f"| {latency['cached_latest_reconstruction_cold_micros']} "
            f"| {latency['cached_latest_reconstruction_hot_micros']} "
            f"| {timing['process_cpu_cores_per_mille'] / 1000:.2f} |"
        )
    print()
    print(
        "These are reproducible engineering measurements, not cross-machine product claims. "
        "Use the JSON reports and `/usr/bin/time` output for regression analysis."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
