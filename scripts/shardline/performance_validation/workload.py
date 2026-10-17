"""Bounded clients, deterministic fixtures and complete-response measurements."""

import concurrent.futures
import hashlib
import http.client
import threading
import time

from benchmark_gc_contention import request
from .measurements import percentiles


def payload(dataset, index, size):
    if dataset == "compressible":
        block = (f"artifact-{index:016d}\n".encode() * 4096)
        return (block * (size // len(block) + 1))[:size]
    identity = index if dataset == "unique" else index // 4
    # The repeated lane has four identical copies per unique object. Bytes are
    # otherwise incompressible; compression and cross-object reuse stay separate.
    return hashlib.shake_256(f"fixture-v1:{identity}".encode()).digest(size)


def run_http(ports, token, dataset, objects, size, clients, prefix, upload_only=False):
    samples = {"put": [], "get": []}

    def worker(worker_id):
        result = {"put": [], "get": []}
        connections = [http.client.HTTPConnection("127.0.0.1", port, timeout=120) for port in ports]
        try:
            for index in range(worker_id, objects, clients):
                body = payload(dataset, index, size)
                path = f"/team.bench/{prefix}-{index}"
                writer = index % len(ports)
                reader = (writer + 1) % len(ports)
                start = time.monotonic()
                request(connections[writer], "PUT", path, token, body)
                result["put"].append((time.monotonic() - start) * 1000)
                if upload_only:
                    continue
                start = time.monotonic()
                received = request(connections[reader], "GET", path, token)
                result["get"].append((time.monotonic() - start) * 1000)
                if received != body:
                    raise RuntimeError(f"cross-replica byte mismatch: {path}")
        finally:
            for connection in connections:
                connection.close()
        return result

    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
        results = list(pool.map(worker, range(clients)))
    elapsed = time.monotonic() - start
    for row in results:
        for method in samples:
            samples[method].extend(row[method])
    multiplier = 1 if upload_only else 2
    return {"seconds": elapsed, "completed_requests": objects * multiplier,
            "uploaded_bytes": objects * size, "downloaded_bytes": 0 if upload_only else objects * size,
            "aggregate_mib_per_second": objects * size * multiplier / elapsed / (1024 ** 2),
            "latency": {method: percentiles(values) for method, values in samples.items() if values}}


def run_direct(client, bucket, dataset, objects, size, clients):
    def operation(index):
        body = payload(dataset, index, size)
        start = time.monotonic()
        client.put_object(Bucket=bucket, Key=f"direct-{index}", Body=body)
        put_ms = (time.monotonic() - start) * 1000
        start = time.monotonic()
        response = client.get_object(Bucket=bucket, Key=f"direct-{index}")["Body"]
        try:
            received = response.read()
        finally:
            response.close()
        get_ms = (time.monotonic() - start) * 1000
        if received != body:
            raise RuntimeError("direct S3 byte mismatch")
        return put_ms, get_ms

    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
        values = list(pool.map(operation, range(objects)))
    elapsed = time.monotonic() - start
    return {"seconds": elapsed, "completed_requests": objects * 2,
            "aggregate_mib_per_second": objects * size * 2 / elapsed / (1024 ** 2),
            "latency": {"put": percentiles([v[0] for v in values]),
                        "get": percentiles([v[1] for v in values])}}


def verify_all(deployment, dataset, objects, size):
    verified = 0
    for port in deployment.ports:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
        try:
            for prefix in ("ingest", "baseline", "maintenance"):
                for index in range(objects):
                    body = request(connection, "GET", f"/team.bench/{prefix}-{index}", deployment.token)
                    if body != payload(dataset, index, size):
                        raise RuntimeError("post-GC byte mismatch")
                    verified += 1
        finally:
            connection.close()
    return verified


class ConcurrentGc:
    def __init__(self, deployment, lane):
        self.deployment = deployment
        self.lane = lane
        self.stop = threading.Event()
        self.runs = []
        self.error = None
        self.thread = threading.Thread(target=self._run)

    def _run(self):
        try:
            while not self.stop.is_set():
                start = time.monotonic()
                seconds = self.deployment.gc(self.lane / f"concurrent-gc-{len(self.runs)}.log")
                self.runs.append({"start": start, "end": time.monotonic(), "seconds": seconds})
                self.stop.wait(0.5)
        except Exception as error:
            self.error = error

    def finish(self, request_start, request_end):
        self.stop.set()
        self.thread.join(timeout=190)
        if self.thread.is_alive():
            raise RuntimeError("concurrent GC did not stop")
        if self.error:
            raise self.error
        if not any(run["start"] < request_end and run["end"] > request_start for run in self.runs):
            raise RuntimeError("no completed GC overlapped the request workload")
        return self.runs
