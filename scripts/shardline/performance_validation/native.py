"""Cold-client native ingestion: backend attempts, bytes and database work.

Every operation uses a new child process and cache. Diagnostics never retain
client output, credentials or signed URLs. This is a shared-host measurement,
not a production scaling or cloud-price estimate.
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import urllib.request
import urllib.parse

from benchmark_gc_contention import command
from .fixtures import Deployment
from .database import process_sample
from .runner import measured_phase


CLIENT = r'''
import hashlib, importlib.metadata, json, resource, sys, time
from pathlib import Path

def peak_rss_kib():
    # Linux getrusage can retain a pre-exec fork high-water mark from the
    # fixture generator. /proc reports this executable's own memory peak.
    if sys.platform.startswith("linux"):
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
        raise RuntimeError("native client memory high-water mark unavailable")
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1024 if sys.platform == "darwin" else peak

try:
    request = json.load(sys.stdin)
    import hf_xet
    from huggingface_hub import HfApi, hf_hub_download
    api = HfApi(endpoint=request["endpoint"], token=request["token"])
    started = time.monotonic()
    if request["action"] == "upload":
        api.upload_file(path_or_fileobj=request["path"], path_in_repo=request["name"],
                        repo_id="team/bench", commit_message="native cost qualification")
    elif request["action"] == "download":
        path = hf_hub_download(repo_id="team/bench", filename=request["name"],
                               endpoint=request["endpoint"], token=request["token"])
        with open(path, "rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != request["sha256"]:
            raise ValueError("download digest mismatch")
    else:
        raise ValueError("unknown operation")
    usage = resource.getrusage(resource.RUSAGE_SELF)
    print(json.dumps({"seconds": time.monotonic()-started,
                      "client_peak_rss_kib": peak_rss_kib(),
                      "client_cpu_seconds": usage.ru_utime+usage.ru_stime,
                      "hub_version": importlib.metadata.version("huggingface_hub"),
                      "xet_version": importlib.metadata.version("hf_xet")}))
except BaseException:
    print("native client operation failed", file=sys.stderr)
    sys.exit(1)
'''


def client_operation(python, deployment, action, path, name, digest):
    with tempfile.TemporaryDirectory(prefix="native-cost-client-") as home:
        env = dict(os.environ, HF_HOME=home, HF_XET_CACHE=str(Path(home) / "xet"),
                   HF_HUB_DISABLE_XET="0", HF_HUB_DISABLE_PROGRESS_BARS="1",
                   HF_HUB_DISABLE_TELEMETRY="1", HF_HUB_VERBOSITY="error")
        for field in ("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "HF_HUB_CACHE",
                      "HUGGINGFACE_HUB_CACHE", "HF_ENDPOINT"):
            env.pop(field, None)
        # Keep credentials off argv and use file-backed stdin, so waiting for
        # process completion cannot block on a pipe before its deadline starts.
        with tempfile.TemporaryFile(mode="w+") as request:
            json.dump({"endpoint": f"http://127.0.0.1:{deployment.ports[0]}",
                       "token": deployment.token, "action": action,
                       "path": str(path), "name": name, "sha256": digest}, request)
            request.seek(0)
            try:
                result = command(str(python), "-c", CLIENT, env=env,
                                 stdin=request, timeout=300)
            except TimeoutError:
                raise RuntimeError("native client exceeded its five-minute deadline") from None
            except subprocess.CalledProcessError:
                raise RuntimeError("native client failed; raw diagnostics withheld") from None
        return json.loads(result)


def counters(deployment):
    request = urllib.request.Request(
        f"http://127.0.0.1:{deployment.ports[0]}/metrics",
        headers={"Authorization": f"Bearer {deployment.token}"})
    with urllib.request.urlopen(request, timeout=10) as response:
        lines = response.read().decode().splitlines()
    names = ("shardline_xet_xorb_upload_bytes_total", "shardline_xet_dedupe_shard_hits_total")
    return {name: sum(int(float(line.rsplit(" ", 1)[1])) for line in lines
                      if line.startswith(name + " ") or line.startswith(name + "{"))
            for name in names}


class ShardlineRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, origin):
        self.origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urlsplit(newurl)
        if (target.scheme, target.netloc) != self.origin or target.username or target.password:
            raise RuntimeError("download attempted a redirect outside Shardline")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def slow_readers(deployment, digest, length, readers, cancel, delay=0.001):
    """Bounded buffers and explicit completion checks through Shardline only."""
    barrier = threading.Barrier(readers, timeout=30)

    def read(_index):
        origin = ("http", f"127.0.0.1:{deployment.ports[0]}")
        opener = urllib.request.build_opener(ShardlineRedirect(origin))
        request = urllib.request.Request(
            f"http://{origin[1]}/team/bench/resolve/main/overlap.bin",
            headers={"Authorization": f"Bearer {deployment.token}"})
        barrier.wait()
        started = time.monotonic()
        with opener.open(request, timeout=120) as response:
            if response.status != 200 or int(response.headers["Content-Length"]) != length:
                raise RuntimeError("plain native download returned incorrect metadata")
            observed = hashlib.sha256()
            received = 0
            while True:
                if time.monotonic() - started > 180:
                    raise RuntimeError("slow native reader exceeded its total deadline")
                block = response.read(32 * 1024)
                if not block:
                    break
                observed.update(block)
                received += len(block)
                if cancel:
                    if received >= length:
                        raise RuntimeError("cancellation fixture consumed the complete file")
                    return
                if delay:
                    time.sleep(delay)
            if received != length or observed.hexdigest() != digest:
                raise RuntimeError("slow native download failed length or SHA-256 verification")

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=readers) as pool:
        list(pool.map(read, range(readers)))
    pid = int(command("docker", "inspect", "--format", "{{.State.Pid}}", deployment.servers[0]))
    return {"seconds": time.monotonic() - started, "readers": readers,
            "completed_verified_downloads": 0 if cancel else readers,
            "abandoned_responses": readers if cancel else 0, "reader_delay_ms": delay * 1000,
            "server_process": process_sample(pid)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("python", type=Path, help="Python environment with stock Hub and hf_xet")
    parser.add_argument("output", type=Path, help="new private directory outside the checkout")
    parser.add_argument("--file-mib", type=int, default=36)
    parser.add_argument("--binary-source-revision", help="known source revision of the supplied executable")
    parser.add_argument("--replica-cpus", type=float, default=1)
    parser.add_argument("--replica-memory", default="1g")
    parser.add_argument("--slow-readers", type=int, default=0,
                        help="optional concurrent plain-HTTP readers, then response cancellations (0-64)")
    parser.add_argument("--reader-delay-ms", type=float, default=1,
                        help="delay after each 32 KiB client read; 0 measures unpaced readers")
    args = parser.parse_args()
    if args.file_mib <= 8 or args.replica_cpus <= 0:
        parser.error("file must exceed the 8 MiB native threshold; CPUs must be positive")
    if not 0 <= args.slow_readers <= 64:
        parser.error("slow readers must be between 0 and 64")
    if not 0 <= args.reader_delay_ms <= 1000:
        parser.error("reader delay must be between 0 and 1000 milliseconds")
    args.binary = args.binary.resolve(strict=True)
    args.python = args.python.absolute()
    if not args.python.is_file():
        parser.error("client Python does not exist")
    args.output = args.output.resolve()
    checkout = Path(command("git", "rev-parse", "--show-toplevel")).resolve()
    if args.output.is_relative_to(checkout):
        parser.error("measurement output must stay outside the checkout")
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {"harness_commit": command("git", "rev-parse", "HEAD"),
                "binary_source_revision": args.binary_source_revision,
                "binary_version": command(str(args.binary), "--version"),
                "shared_host": True, "replicas": 1, "role": "all", "frontend": "hub",
                "replica_cpus": args.replica_cpus, "replica_memory": args.replica_memory,
                "reader_delay_ms": args.reader_delay_ms,
                "file_bytes": args.file_mib * 1024**2}
    with args.binary.open("rb") as source:
        metadata["binary_sha256"] = hashlib.file_digest(source, "sha256").hexdigest()
    deployment = Deployment(args.output, str(args.binary), args.replica_cpus, args.replica_memory)
    lane = None
    try:
        name = "nativecost"
        lane = deployment.lane(name, 1, frontend="hub")
        request = urllib.request.Request(
            f"http://127.0.0.1:{deployment.ports[0]}/api/repos/create",
            data=json.dumps({"name": "team/bench", "type": "model", "private": True}).encode(),
            headers={"Authorization": f"Bearer {deployment.token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
        deployment.sql(name, "SELECT pg_stat_statements_reset()")
        results = {"metadata": metadata, "phases": {}}
        path = args.output / "payload.bin"
        # Deterministic incompressible bytes, then a 256 KiB tail edit. Each
        # client is cold; the server/backend remain warm between versions.
        content = bytearray(hashlib.shake_256(b"native-cost-v1").digest(metadata["file_bytes"]))
        for version in ("unique", "overlap"):
            if version == "overlap":
                for index in range(len(content) - 256*1024, len(content)):
                    content[index] ^= 0x5a
            path.write_bytes(content)
            digest = hashlib.sha256(content).hexdigest()
            before = counters(deployment)
            result = measured_phase(deployment, name, lambda: client_operation(
                args.python, deployment, "upload", path, version + ".bin", digest), len(content))
            after = counters(deployment)
            result["native_counter_delta"] = {key: after[key] - before[key] for key in after}
            if result["native_counter_delta"]["shardline_xet_xorb_upload_bytes_total"] <= 0:
                raise RuntimeError("upload did not establish the native Xorb path")
            if version == "overlap" and result["native_counter_delta"]["shardline_xet_dedupe_shard_hits_total"] <= 0:
                raise RuntimeError("cold overlapping upload did not use native dedup")
            result["inventory"] = deployment.inventory(name)
            results["phases"][version] = result
            results["phases"][version + "_download"] = measured_phase(
                deployment, name, lambda: client_operation(
                    args.python, deployment, "download", path, version + ".bin", digest), len(content))
        if args.slow_readers:
            results["pressure"] = {}
            for phase, cancel in (("slow_downloads", False), ("abandoned_downloads", True)):
                # Abandoned reads generate cost but no successful logical bytes;
                # do not present their request rate as cost per successful TB.
                logical = len(content) * args.slow_readers if not cancel else 1
                measured = measured_phase(deployment, name, lambda: slow_readers(
                    deployment, digest, len(content), args.slow_readers, cancel,
                    args.reader_delay_ms / 1000), logical)
                if cancel:
                    measured["backend_s3"].pop("requests_per_decimal_gb")
                if measured["server_process"]["peak_rss_kib"] >= 384 * 1024:
                    raise RuntimeError("native pressure exceeded the 384 MiB server RSS qualification ceiling")
                results["pressure"][phase] = measured
            results["pressure"]["plain_download_after_cancellation"] = measured_phase(
                deployment, name, lambda: slow_readers(
                    deployment, digest, len(content), 1, False, args.reader_delay_ms / 1000), len(content))
            results["pressure"]["cold_download_after_cancellation"] = measured_phase(
                deployment, name, lambda: client_operation(
                    args.python, deployment, "download", path, "overlap.bin", digest), len(content))
        results["trace_validation"] = deployment.trace.validate()
        results["limitations"] = [
            "One shared-host replica, sequential operations, warm server and cold clients.",
            "Immediate retained bytes include compatibility copies; no retention-expired GC claim.",
            "Trace bytes include protocol overhead; SQL WAL is not physical disk wear.",
            "No cloud price, production scaling, tail-latency or power-loss claim."]
        (args.output / "result.json").write_text(json.dumps(results, indent=2) + "\n")
        print("Native cost qualification passed; sanitized result.json written")
    finally:
        try:
            if lane is not None:
                deployment.stop_lane(lane)
        finally:
            deployment.close()
