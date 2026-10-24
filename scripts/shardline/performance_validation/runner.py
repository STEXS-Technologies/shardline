"""Run the P0 matrix and P1 maintenance-tail/direct-S3 comparisons locally."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from benchmark_gc_contention import command
from .database import Sampler, query_mix, snapshot, top_queries
from .fixtures import Deployment
from .measurements import RequestTrace, scale_efficiency, utc_now
from .workload import ConcurrentGc, run_direct, run_http, verify_all


def measured_phase(deployment, name, operation, logical_bytes):
    start = deployment.trace.barrier()
    before_queries = query_mix(deployment, name)
    before_database = snapshot(deployment, name)
    sampler = Sampler(deployment, name)
    try:
        result = operation()
    finally:
        observations = sampler.finish()
    after_queries = query_mix(deployment, name)
    after_database = snapshot(deployment, name)
    result.update(backend_s3=deployment.trace.snapshot(start, logical_bytes),
                  query_delta={key: after_queries[key] - before_queries[key] for key in after_queries},
                  database_before=before_database, database_after=after_database,
                  observations=observations)
    return result


def lane_result(deployment, name, replicas, dataset, repeat, args):
    lane = deployment.lane(name, replicas)
    logical_bytes = args.objects * args.object_bytes
    try:
        deployment.sql(name, "SELECT pg_stat_statements_reset()")
        result = {"replicas": replicas, "dataset": dataset, "repeat": repeat,
                  "objects": args.objects, "object_bytes": args.object_bytes,
                  "initial_logical_bytes": logical_bytes, "clients": args.clients,
                  "label": name, "started": utc_now()}
        result["ingest"] = measured_phase(deployment, name, lambda: run_http(
            deployment.ports, deployment.token, dataset, args.objects, args.object_bytes,
            args.clients, "ingest", upload_only=True), logical_bytes)
        result["after_packing"] = deployment.inventory(name)
        start = deployment.trace.barrier()
        result["settling_gc_seconds"] = [deployment.gc(lane / f"settling-gc-{i}.log") for i in range(2)]
        result["settling_gc_s3"] = deployment.trace.snapshot(start, logical_bytes)
        result["after_initial_gc"] = deployment.inventory(name)
        result["initial_stored_to_logical_ratio"] = (
            result["after_initial_gc"]["stored_payload_bytes"] / logical_bytes)
        modes = ("baseline", "maintenance") if repeat % 2 == 0 else ("maintenance", "baseline")
        for mode in modes:
            def mixed():
                collector = ConcurrentGc(deployment, lane)
                start = time.monotonic()
                if mode == "maintenance":
                    collector.thread.start()
                try:
                    measured = run_http(deployment.ports, deployment.token, dataset,
                                        args.objects, args.object_bytes, args.clients, mode)
                finally:
                    end = time.monotonic()
                    if mode == "maintenance":
                        runs = collector.finish(start, end)
                if mode == "maintenance":
                    measured["gc_runs"] = runs
                return measured
            result[mode] = measured_phase(deployment, name, mixed, logical_bytes)
        result["final_gc_seconds"] = deployment.gc(lane / "final-gc.log")
        result["post_gc_verified_responses"] = verify_all(
            deployment, dataset, args.objects, args.object_bytes)
        result["final_inventory"] = deployment.inventory(name)
        result["trace_validation"] = deployment.trace.validate()
        result["final_logical_bytes"] = 3 * logical_bytes
        result["top_queries"] = top_queries(deployment, name)
        (lane / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        deployment.stop_lane(lane)


def write_summary(output, results, direct):
    lines = ["# Local performance validation", "",
             "Shared-host, CPU-limited S3 API replicas; not a production scale-out claim.", "",
             "Throughput counts fully consumed PUT/GET responses. Initial ingest is PUT-only.", "",
             "| Data | Replicas | Repeat | Ingest MiB/s | Mixed MiB/s | Mixed + GC MiB/s | GC PUT p99 ms | Stored/logical after initial GC |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in results:
        lines.append(f'| {row["dataset"]} | {row["replicas"]} | {row["repeat"]} | '
                     f'{row["ingest"]["aggregate_mib_per_second"]:.2f} | '
                     f'{row["baseline"]["aggregate_mib_per_second"]:.2f} | '
                     f'{row["maintenance"]["aggregate_mib_per_second"]:.2f} | '
                     f'{row["maintenance"]["latency"]["put"]["p99_ms"]:.2f} | '
                     f'{row["initial_stored_to_logical_ratio"]:.3f} |')
    lines += ["", "## Scale-out efficiency", "",
              "70% is an engineering target, not a measured production guarantee.", "",
              "| Data | Replicas | Repeat | Initial ingest efficiency | Mixed efficiency | GC mixed efficiency |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in results:
        baseline = next(r for r in results if r["dataset"] == row["dataset"]
                        and r["repeat"] == row["repeat"] and r["replicas"] == 1)
        row["scaling_efficiency"] = {
            mode: scale_efficiency(row[mode]["aggregate_mib_per_second"],
                                   baseline[mode]["aggregate_mib_per_second"], row["replicas"])
            for mode in ("ingest", "baseline", "maintenance")}
        values = row["scaling_efficiency"]
        lines.append(f'| {row["dataset"]} | {row["replicas"]} | {row["repeat"]} | '
                     f'{values["ingest"]:.1%} | {values["baseline"]:.1%} | {values["maintenance"]:.1%} |')
    lines += ["", "## Backend cost and database pressure", "",
              "Requests include all initial-upload backend attempts, including read-back and dedupe probes.", "",
              "| Data | Replicas | Repeat | Ingest S3 requests/GB | Payload MiB after packing | Payload MiB after initial GC | Initial query calls | Peak connections | GC peak lock waiters |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in results:
        lines.append(f'| {row["dataset"]} | {row["replicas"]} | {row["repeat"]} | '
                     f'{row["ingest"]["backend_s3"]["requests_per_decimal_gb"]:.0f} | '
                     f'{row["after_packing"]["stored_payload_bytes"] / 1024**2:.2f} | '
                     f'{row["after_initial_gc"]["stored_payload_bytes"] / 1024**2:.2f} | '
                     f'{row["ingest"]["query_delta"]["calls"]:.0f} | '
                     f'{max(row[mode]["observations"]["postgres_peak"]["connections"] for mode in ("ingest", "baseline", "maintenance"))} | '
                     f'{row["maintenance"]["observations"]["postgres_peak"]["lock_waiting"]} |')
    lines += ["", "## Direct S3 reference", "",
              "| Data | Repeat | Mixed MiB/s | PUT p99 ms | GET p99 ms |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for row in direct:
        lines.append(f'| {row["dataset"]} | {row["repeat"]} | '
                     f'{row["aggregate_mib_per_second"]:.2f} | '
                     f'{row["latency"]["put"]["p99_ms"]:.2f} | {row["latency"]["get"]["p99_ms"]:.2f} |')
    lines += ["", "## Boundaries", "",
              "- PostgreSQL, MinIO and clients share this host. Client CPU, local disk and shared backends can limit scaling.",
              "- MinIO server tracing counts actual requests, including failures and dedupe probes. Trace bytes include protocol overhead; they are not billable egress.",
              "- Stored payload bytes include live objects and reliability evidence, but exclude MinIO filesystem overhead, PostgreSQL bytes, replication and versions. PostgreSQL size is reported separately.",
              "- Container CPU includes all PostgreSQL workers. Container peak memory is the lifetime cgroup high-water mark, including page cache; client RSS is measured separately.",
              "- Database lock/connection peaks are sampled every 0.5 seconds and can miss shorter spikes. Query calls and execution time are cumulative server counters.",
              "- The runner requests zero retention on disposable data, but the CLI enforces at least one hour. Short-run storage includes quarantined objects and does not establish the footprint after retention expires. A completed collector must overlap each maintenance workload.",
              "- GC overlap refers to CLI process lifetime, including startup and ownership waits; it does not prove every request overlapped an active scan.",
              "- These are fixed-size bursts, not a sustained-load test. With 32 objects per lane, p99 is the slowest sample; use larger samples for tail-latency claims.",
              "- Initial ingestion uses one logical namespace. Mixed lanes then add two identical copies to compare warm deduplication with/without GC; their final logical bytes are reported separately.",
              "- No forced cold cache, node termination, load balancer, Redis, cloud pricing, native LFS/Xet performance, or 100 TB–1 PB cardinality result is claimed.",
              "- Direct S3 uses boto3 and identical payload/client counts; it is a storage-service reference, not a complete Shardline protocol comparison.", ""]
    (output / "summary.md").write_text("\n".join(lines))
    (output / "results.json").write_text(json.dumps({"lanes": results, "direct_s3": direct}, indent=2) + "\n")


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path, help="new directory outside the checkout")
    parser.add_argument("--binary-source-revision", help="source revision of the supplied executable, when known; recorded separately from the harness checkout")
    parser.add_argument("--replicas", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--datasets", nargs="+", choices=["unique", "repeated", "compressible"],
                        default=["unique", "repeated", "compressible"])
    parser.add_argument("--objects", type=int, default=128)
    parser.add_argument("--object-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--replica-cpus", type=float, default=1)
    parser.add_argument("--replica-memory", default="1g")
    args = parser.parse_args()
    if min(*args.replicas, args.objects, args.object_bytes, args.clients, args.repeats, args.replica_cpus) <= 0:
        parser.error("counts, sizes and CPU limits must be positive")
    if 1 not in args.replicas or len(set(args.replicas)) != len(args.replicas):
        parser.error("replicas must include 1 and must not contain duplicates")
    if len(set(args.datasets)) != len(args.datasets):
        parser.error("datasets must not contain duplicates")
    if "repeated" in args.datasets and (args.objects < 4 or args.objects % 4):
        parser.error("the repeated fixture needs a multiple of four objects")
    args.binary = args.binary.resolve(strict=True)
    args.output = args.output.resolve()
    checkout = Path(command("git", "rev-parse", "--show-toplevel")).resolve()
    if args.output.is_relative_to(checkout):
        parser.error("measurement output must stay outside the checkout")
    return args


def main():
    args = arguments()
    args.output.mkdir(parents=True, exist_ok=False)
    with args.binary.open("rb") as binary:
        binary_hash = hashlib.file_digest(binary, "sha256").hexdigest()
    metadata = {"harness_commit": command("git", "rev-parse", "HEAD"),
                "binary_source_revision": args.binary_source_revision, "started": utc_now(),
                "binary_sha256": binary_hash, "binary_version": command(str(args.binary), "--version"),
                "host": platform.platform(), "logical_cpus": os.cpu_count(),
                "load_average_at_start": os.getloadavg(),
                "python_version": platform.python_version(),
                "server_role": "api", "frontend": "s3",
                "boto3_version": command("python3", "-c", "import boto3; print(boto3.__version__)"),
                "mc_version": command("mc", "--version"),
                "arguments": {key: str(value) if isinstance(value, Path) else value
                              for key, value in vars(args).items()}}
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    deployment = Deployment(args.output, str(args.binary), args.replica_cpus, args.replica_memory)
    results, direct = [], []
    try:
        (args.output / "services.json").write_text(command("docker", "inspect", deployment.pg, deployment.s3) + "\n")
        for repeat in range(args.repeats):
            for dataset in args.datasets:
                bucket = f"direct-{dataset}-{repeat}"
                deployment.client.create_bucket(Bucket=bucket)
                env = dict(os.environ, MC_HOST_performance=deployment.endpoint.replace(
                    "http://", "http://performance:owned_performance_password@"))
                trace = RequestTrace(subprocess.Popen(
                    ["mc", "admin", "trace", "--json", "--verbose", "--call", "s3", "performance"],
                    env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True),
                    deployment.client, bucket, deployment.endpoint)
                try:
                    start = trace.barrier()
                    reference = run_direct(deployment.client, bucket, dataset, args.objects,
                                           args.object_bytes, args.clients)
                    reference.update(dataset=dataset, repeat=repeat,
                                     backend_s3=trace.snapshot(start, args.objects * args.object_bytes))
                    if reference["backend_s3"]["requests"] != 2 * args.objects:
                        raise RuntimeError("direct S3 trace lost or added requests")
                    reference["inventory"] = deployment.inventory(bucket)
                    reference["trace_validation"] = trace.validate()
                    direct.append(reference)
                finally:
                    trace.close()
                order = sorted(args.replicas, reverse=bool(repeat % 2))
                for replicas in order:
                    name = f"p-{dataset}-{replicas}-{repeat}"
                    print(f"START {name}", flush=True)
                    results.append(lane_result(deployment, name, replicas, dataset, repeat, args))
                    print(f"PASS {name}", flush=True)
                    (args.output / "partial-results.json").write_text(json.dumps(results, indent=2) + "\n")
        write_summary(args.output, results, direct)
        print(f"PASS: measurements and scope in {args.output / 'summary.md'}", flush=True)
    finally:
        deployment.close()
