"""Cold-client native ingestion: backend attempts, bytes and database work.

Every operation uses a new child process and cache. Diagnostics never retain
client output, credentials or signed URLs. This is a shared-host measurement,
not a production scaling or cloud-price estimate.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.request

from benchmark_gc_contention import command
from .fixtures import Deployment
from .runner import measured_phase


CLIENT = r'''
import hashlib, importlib.metadata, json, resource, sys, time
from pathlib import Path
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
                      "client_peak_rss_kib": usage.ru_maxrss,
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
        try:
            result = subprocess.run(
                [str(python), "-c", CLIENT], env=env, capture_output=True, text=True,
                input=json.dumps({"endpoint": f"http://127.0.0.1:{deployment.ports[0]}",
                                  "token": deployment.token, "action": action,
                                  "path": str(path), "name": name, "sha256": digest}),
                timeout=300, check=False)
        except subprocess.TimeoutExpired:
            raise RuntimeError("native client exceeded its five-minute deadline") from None
        if result.returncode:
            raise RuntimeError("native client failed; raw diagnostics withheld")
        return json.loads(result.stdout)


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("python", type=Path, help="Python environment with stock Hub and hf_xet")
    parser.add_argument("output", type=Path, help="new private directory outside the checkout")
    parser.add_argument("--file-mib", type=int, default=36)
    parser.add_argument("--binary-source-revision", help="known source revision of the supplied executable")
    parser.add_argument("--replica-cpus", type=float, default=1)
    parser.add_argument("--replica-memory", default="1g")
    args = parser.parse_args()
    if args.file_mib <= 8 or args.replica_cpus <= 0:
        parser.error("file must exceed the 8 MiB native threshold; CPUs must be positive")
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
        results["trace_validation"] = deployment.trace.validate()
        results["limitations"] = [
            "One shared-host replica, sequential operations, warm server and cold clients.",
            "Immediate retained bytes include compatibility copies; no retention-expired GC claim.",
            "Trace bytes include protocol overhead; SQL WAL is not physical disk wear.",
            "No cloud price, production scaling, tail-latency or power-loss claim."]
        (args.output / "result.json").write_text(json.dumps(results, indent=2) + "\n")
        print("Native cost qualification passed; sanitized result.json written")
    finally:
        if lane is not None:
            deployment.stop_lane(lane)
        deployment.close()
