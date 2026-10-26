#!/usr/bin/env python3
"""Compare complete S3 PUT/GET requests while real PostgreSQL GC runs.

Usage: benchmark_gc_contention.py CURRENT RELEASED OUTPUT_DIRECTORY
Requires Docker and the same cached service images as the deployment drills.
All services and data are owned by this run; measurements stay outside Git.
"""

import concurrent.futures
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import socket
import signal
import subprocess
import sys
import tempfile
import threading
import time

MINIO = "ghcr.io/golithus/minio@sha256:8793e960474071520bdb91bb9a6d1793eb229fdbc9dcb7ec75ad836b69bbab40"
MC = "ghcr.io/golithus/mc@sha256:dde55ccc0bc9e65dc582c8e652d652eea1438c6e60d774524f4da15d7b26a900"


def command(*args, **kwargs):
    """Bound fixture commands without publishing their arguments or diagnostics."""
    timeout = kwargs.pop("timeout", 120)
    # File capture cannot fill a pipe while the parent waits for its deadline.
    # Callers may merge stderr when collecting explicitly private fixture logs.
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        output = kwargs.pop("stdout", stdout)
        error_output = kwargs.pop("stderr", stderr)
        process = subprocess.Popen(args, stdout=output, stderr=error_output,
                                   start_new_session=os.name == "posix", **kwargs)
        try:
            try:
                status = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise TimeoutError("fixture command exceeded deadline") from None
        finally:
            # A completed CLI may leave recursive clients alive. Only signal
            # the process group created above, never the runner's own group.
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.poll() is None:
                process.kill()
            process.wait()
        if status:
            # Preserve the failure type used by readiness retries, but never
            # put argv, stdout or stderr into an exception printed by CI.
            raise subprocess.CalledProcessError(status, "fixture command")
        if output is not stdout:
            return ""
        stdout.seek(0)
        return stdout.read().decode().strip()


def port(container, container_port):
    info = json.loads(command("docker", "inspect", container))[0]
    return int(info["NetworkSettings"]["Ports"][f"{container_port}/tcp"][0]["HostPort"])


def ready(check):
    for _ in range(120):
        try:
            if check():
                return
        except (OSError, subprocess.CalledProcessError, http.client.HTTPException):
            pass
        time.sleep(0.25)
    raise RuntimeError("fixture did not become ready")


def request(connection, method, path, token="", payload=None):
    connection.request(method, path, body=payload,
                       headers={"Authorization": f"Bearer {token}"})
    response = connection.getresponse()
    body = response.read()
    if not 200 <= response.status < 300:
        raise RuntimeError(f"{method} {path}: HTTP {response.status}: {body[:300]!r}")
    return body


def summary(values):
    ordered = sorted(values)
    return {f"p{p}_ms": ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]
            for p in (50, 95, 99)}


def run_lane(binary, released, output, pg, s3_port, label, concurrency, operations):
    lane = output / label
    lane.mkdir()
    root = lane / "root"
    root.mkdir()
    scratch = lane / "scratch"
    scratch.mkdir()
    config = lane / "empty.toml"
    config.touch()
    command("docker", "exec", pg, "createdb", "-h", "127.0.0.1", "-U", "postgres", label)
    database = f"postgres://postgres:owned_contention_fixture@127.0.0.1:{port(pg, 5432)}/{label}"
    endpoint = f"http://127.0.0.1:{s3_port}"
    command("docker", "run", "--rm", "--network", "host", "-e",
            f"MC_HOST_fixture=http://contention:owned_contention_password@127.0.0.1:{s3_port}",
            MC, "mb", f"fixture/{label}")
    env = {"PATH": os.environ["PATH"], "RUST_LOG": "error", "TMPDIR": str(scratch),
           "SHARDLINE_ROOT_DIR": str(root), "SHARDLINE_INDEX_POSTGRES_URL": database,
           "SHARDLINE_OBJECT_STORAGE_ADAPTER": "s3", "SHARDLINE_S3_BUCKET": label,
           "SHARDLINE_S3_REGION": "us-east-1", "SHARDLINE_S3_ENDPOINT": endpoint,
           "SHARDLINE_S3_ALLOW_HTTP": "true", "SHARDLINE_S3_ACCESS_KEY_ID": "contention",
           "SHARDLINE_S3_SECRET_ACCESS_KEY": "owned_contention_password",
           "SHARDLINE_AUTH_PROVIDER": "local",
           "SHARDLINE_TOKEN_SIGNING_KEY": "0123456789abcdef0123456789abcdef"}
    with (lane / "migration.log").open("w") as log:
        for executable in dict.fromkeys((released, binary)):
            command(executable, "--config", str(config), "db", "migrate", "up",
                    "--database-url", database, env=env, stdout=log,
                    stderr=subprocess.STDOUT, timeout=120)
    token = command(released, "--config", str(config), "admin", "token", "--issuer", "fixture",
                    "--subject", "fixture", "--scope", "write", "--provider", "generic",
                    "--owner", "team", "--repo", "bench", "--key-env",
                    "SHARDLINE_TOKEN_SIGNING_KEY", env=env)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        listen_port = sock.getsockname()[1]
    env.update(SHARDLINE_BIND_ADDR=f"127.0.0.1:{listen_port}",
               SHARDLINE_PUBLIC_BASE_URL=f"http://127.0.0.1:{listen_port}")
    stop = threading.Event()
    gc_results = []
    errors = []
    ticks = []
    memory = []
    gc_thread = None
    monitor_thread = None
    with (lane / "server.log").open("w") as log:
        server = subprocess.Popen([binary, "--config", str(config), "serve", "--frontend", "s3"],
                                  env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            def server_ready():
                if server.poll() is not None:
                    raise RuntimeError(f"server exited: {server.returncode}")
                conn = http.client.HTTPConnection("127.0.0.1", listen_port, timeout=2)
                try:
                    request(conn, "GET", "/readyz")
                    return True
                finally:
                    conn.close()

            ready(server_ready)

            def monitor():
                while not stop.is_set():
                    try:
                        stat = Path(f"/proc/{server.pid}/stat").read_text().split(") ", 1)[1].split()
                        ticks.append(int(stat[11]) + int(stat[12]))
                        status = Path(f"/proc/{server.pid}/status").read_text().splitlines()
                        memory.append(int(next(line.split()[1] for line in status
                                               if line.startswith("VmHWM:"))))
                    except (OSError, StopIteration):
                        return
                    stop.wait(0.05)

            def collect():
                try:
                    while not stop.is_set():
                        index = len(gc_results)
                        start = time.monotonic()
                        with (lane / f"gc-{index}.log").open("w") as gc_log:
                            command("/usr/bin/time", "-f", "%U %S %M", "-o",
                                    str(lane / f"gc-{index}.time"), binary,
                                    "--config", str(config), "gc", "--root", str(root),
                                    "--mark", "--sweep", "--retention-seconds", "86400",
                                    env=env, stdout=gc_log, stderr=subprocess.STDOUT,
                                    timeout=120)
                        user, system, rss = (lane / f"gc-{index}.time").read_text().split()
                        gc_results.append({"seconds": time.monotonic() - start,
                                           "cpu_seconds": float(user) + float(system),
                                           "peak_rss_kib": int(rss)})
                        stop.wait(0.5)
                except Exception as error:
                    errors.append(str(error))

            monitor_thread = threading.Thread(target=monitor)
            gc_thread = threading.Thread(target=collect)
            monitor_thread.start()
            gc_thread.start()

            def worker(worker_id):
                latencies = {"put": [], "get": []}
                conn = http.client.HTTPConnection("127.0.0.1", listen_port, timeout=60)
                try:
                    for iteration in range(operations):
                        payload = hashlib.shake_256(f"{worker_id}:{iteration}".encode()).digest(65536)
                        path = f"/team.bench/worker-{worker_id}-{iteration}"
                        for method in ("PUT", "GET"):
                            start = time.monotonic()
                            body = request(conn, method, path, token,
                                           payload if method == "PUT" else None)
                            latencies[method.lower()].append((time.monotonic() - start) * 1000)
                            if method == "GET" and body != payload:
                                raise RuntimeError(f"data mismatch: {path}")
                finally:
                    conn.close()
                return latencies

            start = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                results = list(pool.map(worker, range(concurrency)))
            elapsed = time.monotonic() - start
            stop.set()
            gc_thread.join()
            monitor_thread.join()
            if errors or not gc_results:
                raise RuntimeError(f"GC did not complete successfully: {errors}")
            # A separate read pass checks every acknowledged object after the final GC.
            conn = http.client.HTTPConnection("127.0.0.1", listen_port, timeout=60)
            try:
                for worker_id in range(concurrency):
                    for iteration in range(operations):
                        payload = hashlib.shake_256(f"{worker_id}:{iteration}".encode()).digest(65536)
                        if request(conn, "GET", f"/team.bench/worker-{worker_id}-{iteration}", token) != payload:
                            raise RuntimeError("post-GC data mismatch")
            finally:
                conn.close()
            with open(binary, "rb") as executable:
                digest = hashlib.file_digest(executable, "sha256").hexdigest()
            result = {"label": label, "binary_sha256": digest,
                      "concurrency": concurrency, "objects": concurrency * operations,
                      "payload_bytes": 65536, "seconds": elapsed,
                      "requests_per_second": concurrency * operations * 2 / elapsed,
                      "server_cpu_seconds": (ticks[-1] - ticks[0]) / os.sysconf("SC_CLK_TCK"),
                      "server_peak_rss_kib": max(memory), "gc": gc_results,
                      "post_gc_verified_objects": concurrency * operations,
                      "latency": {kind: summary([value for row in results for value in row[kind]])
                                  for kind in ("put", "get")}}
            (lane / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result), flush=True)
            return result
        finally:
            stop.set()
            for thread in (gc_thread, monitor_thread):
                if thread is not None:
                    thread.join()
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


def main():
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    current, released = (str(Path(arg).resolve(strict=True)) for arg in sys.argv[1:3])
    output = Path(sys.argv[3]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    containers = []
    try:
        pg = command("docker", "run", "-d", "--rm", "-e",
                     "POSTGRES_PASSWORD=owned_contention_fixture", "-p", "127.0.0.1::5432", "postgres:16-alpine")
        containers.append(pg)
        s3 = command("docker", "run", "-d", "--rm", "-e", "MINIO_ROOT_USER=contention", "-e",
                     "MINIO_ROOT_PASSWORD=owned_contention_password", "-p", "127.0.0.1::9000", MINIO, "server", "/data")
        containers.append(s3)
        ready(lambda: command("docker", "exec", pg, "pg_isready", "-h", "127.0.0.1",
                              "-U", "postgres", timeout=2) is not None)
        def minio_ready():
            conn = http.client.HTTPConnection("127.0.0.1", port(s3, 9000), timeout=2)
            try:
                request(conn, "GET", "/minio/health/ready")
                return True
            finally:
                conn.close()
        ready(minio_ready)
        concurrency = int(os.environ.get("BENCH_CONCURRENCY", "16"))
        operations = int(os.environ.get("BENCH_OPERATIONS", "128"))
        repeats = int(os.environ.get("BENCH_REPEATS", "3"))
        if min(concurrency, operations, repeats) < 1:
            raise ValueError("concurrency, operations and repeats must be positive")
        (output / "services.json").write_text(command("docker", "inspect", pg, s3) + "\n")
        results = []
        for repeat in range(repeats):
            order = (("released", released), ("current", current))
            if repeat % 2:
                order = tuple(reversed(order))
            for version, binary in order:
                results.append(run_lane(binary, released, output, pg, port(s3, 9000),
                                        f"{version}-{repeat}", concurrency, operations))
        (output / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")
    finally:
        for container in reversed(containers):
            command("docker", "rm", "-f", container)


if __name__ == "__main__":
    main()
