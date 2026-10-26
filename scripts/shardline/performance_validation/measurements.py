"""Measurement arithmetic and sanitized MinIO request tracing."""

from collections import Counter
from datetime import datetime
import json
import math
import os
import re
import signal
import subprocess
import threading
import time
import uuid
import urllib.request


def percentiles(milliseconds):
    if not milliseconds:
        raise ValueError("a latency sample must not be empty")
    values = sorted(milliseconds)
    return {f"p{p}_ms": values[math.ceil(len(values) * p / 100) - 1]
            for p in (50, 95, 99)}


def object_class(key):
    if not key:
        return "bucket_operations"
    if key.startswith("xorbs/"):
        return "xorbs"
    if key.startswith("shards/"):
        return "shards"
    if len(key) == 67 and key[2] == "/" and all(c in "0123456789abcdef" for c in key.replace("/", "")):
        return "individual_chunks"
    return "other"


def inventory_summary(objects):
    groups = {}
    for item in objects:
        group = groups.setdefault(object_class(item["Key"]), {"objects": 0, "bytes": 0})
        group["objects"] += 1
        group["bytes"] += item["Size"]
    return {"objects": sum(g["objects"] for g in groups.values()),
            "stored_payload_bytes": sum(g["bytes"] for g in groups.values()), "by_class": groups}


def request_summary(events, logical_bytes):
    counts = Counter(f'{e["api"]}:{e["status"]}' for e in events)
    by_class = Counter(f'{e["method"]}:{object_class(e["key"])}' for e in events)
    return {"requests": len(events), "by_api_status": dict(sorted(counts.items())),
            "by_method_object_class": dict(sorted(by_class.items())),
            "requests_per_decimal_gb": len(events) * 1_000_000_000 / logical_bytes,
            "trace_received_bytes": sum(e["rx"] for e in events),
            "trace_sent_bytes": sum(e["tx"] for e in events)}


def scale_efficiency(throughput, single_replica_throughput, replicas):
    if min(throughput, single_replica_throughput, replicas) <= 0:
        raise ValueError("throughput and replica counts must be positive")
    return throughput / (single_replica_throughput * replicas)


def server_request_counts(endpoint):
    with urllib.request.urlopen(endpoint + "/minio/metrics/v3/api/requests", timeout=10) as response:
        metrics = response.read().decode()
    result = Counter()
    for line in metrics.splitlines():
        if line.startswith("minio_api_requests_total{"):
            name = re.search(r'(?:\{|,)name="([^"]+)"', line)
            if not name:
                raise RuntimeError("MinIO request counter has no API name")
            result["s3." + name[1]] += int(float(line.rsplit(" ", 1)[1]))
    if not result:
        raise RuntimeError("MinIO v3 request counters are absent")
    return result


class RequestTrace:
    """Trace S3 at the backend; keep neither headers nor request bodies.

    A unique HEAD probe establishes subscription readiness and flushes each
    snapshot. Unlike MinIO's cached Prometheus counters, this observes short runs.
    An inactive or malformed trace makes the benchmark fail.
    """

    def __init__(self, process, client, bucket, endpoint, *, owns_process_group=False):
        self.process = process
        self.owns_process_group = owns_process_group
        self.client = client
        self.bucket = bucket
        self.endpoint = endpoint
        self.events = []
        self.probes = set()
        self.error = None
        self.probe_attempts = 0
        self.condition = threading.Condition()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()
        try:
            self.barrier()
            self.server_before = server_request_counts(endpoint)
            self.observed_before = Counter(e["api"] for e in self.events)
            self.probes_before = self.probe_attempts
        except BaseException:
            self.close()
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                row = json.loads(line)
                if row.get("type") != "S3":
                    raise RuntimeError(f"unexpected trace event: {row.get('type')}")
                request = row["request"]
                path = request["path"].split("?", 1)[0]
                root = f"/{self.bucket}"
                if path != root and not path.startswith(root + "/"):
                    continue
                key = path[len(root):].lstrip("/")
                with self.condition:
                    if key.startswith("_benchmark_probe/"):
                        self.probes.add(key)
                    else:
                        self.events.append({"api": row["api"], "method": request["method"],
                                            "status": row["response"]["statusCode"], "key": key,
                                            "rx": row["callStats"]["rx"],
                                            "tx": row["callStats"]["tx"]})
                    self.condition.notify_all()
            if self.process.poll() is not None:
                raise RuntimeError("MinIO request trace exited unexpectedly")
        except Exception as error:
            with self.condition:
                self.error = str(error)
                self.condition.notify_all()

    def barrier(self):
        # A subscription can take time to attach. A new probe on each retry avoids
        # accepting a delayed event as evidence that a later snapshot was flushed.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            key = f"_benchmark_probe/{uuid.uuid4().hex}"
            self.probe_attempts += 1
            try:
                self.client.head_object(Bucket=self.bucket, Key=key)
            except self.client.exceptions.ClientError as error:
                if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                    raise
            with self.condition:
                self.condition.wait_for(lambda: key in self.probes or self.error, timeout=0.25)
                if self.error:
                    raise RuntimeError(self.error)
                if key in self.probes:
                    return len(self.events)
        raise RuntimeError("MinIO request trace did not acknowledge its probe")

    def snapshot(self, start, logical_bytes):
        end = self.barrier()
        return request_summary(self.events[start:end], logical_bytes)

    def validate(self):
        # Counter reads and the trace are independent channels. Wait briefly for
        # the final server-side event, then reject any loss rather than estimate it.
        for _ in range(20):
            self.barrier()
            server_after = server_request_counts(self.endpoint)
            if any(server_after[api] < value for api, value in self.server_before.items()):
                raise RuntimeError("MinIO request counters reset during measurement")
            observed = Counter(e["api"] for e in self.events) - self.observed_before
            expected = server_after.copy()
            expected.subtract(self.server_before)
            expected["s3.HeadObject"] -= self.probe_attempts - self.probes_before
            expected = +expected
            if observed == expected:
                return {"matched_server_api_counts": dict(sorted(expected.items()))}
            time.sleep(0.1)
        raise RuntimeError(f"MinIO trace/counter mismatch: observed={observed}, expected={expected}")

    def close(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        finally:
            # A trace wrapper can exit while a descendant retains its stdout
            # pipe. Stop only the explicitly owned session before joining the
            # reader; closing a pipe with a blocked reader can itself hang.
            if self.owns_process_group and os.name == "posix":
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("MinIO trace reader did not stop within its deadline")
        self.process.stdout.close()


def utc_now():
    return datetime.now().astimezone().isoformat()
