#!/usr/bin/env python3
"""Compare retained local physical object bytes for 32 related versions over three protocols.

Usage: benchmark_protocol_dedup.py BINARY NEW_OUTPUT_DIRECTORY
Requires hf_xet==1.7.0. Uses disposable loopback servers, fresh local metadata /
payload roots per protocol, and the same 64 KiB CDC target. Native upload uses
one warm XetSession across commits; post-GC native reads use a separate process
and empty cache. S3 / basic LFS use their HTTP APIs with bearer authentication.
This is a storage-reuse qualification, not a throughput or multi-replica test.
"""
import hashlib
import http.client
import importlib.metadata
import json
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import sys
import time

from benchmark_gc_contention import command, ready, request

SIZE = 4 * 1024 * 1024
VERSIONS = 32


def corpus(output):
    base = random.Random(20261114).randbytes(SIZE)
    files = []
    for version in range(VERSIONS):
        data = bytearray(base)
        offset = version * (SIZE // VERSIONS)
        data[offset:offset + 4096] = hashlib.shake_256(f"edit-{version}".encode()).digest(4096)
        path = output / f"version-{version:02}.bin"
        path.write_bytes(data)
        files.append((path, hashlib.sha256(data).hexdigest()))
    return files


def inventory(root):
    families = {}
    for path in (root / "chunks").rglob("*"):
        if path.is_file():
            family = path.relative_to(root / "chunks").parts[0]
            families[family] = families.get(family, 0) + path.stat().st_size
    return {"physical_object_bytes": sum(families.values()), "container_bytes": families.get("xorbs", 0), "families": families}


class Server:
    def __init__(self, binary, lane, frontend):
        self.binary, self.lane = binary, lane
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.config = lane / "empty.toml"
        self.config.touch()
        self.env = {"PATH": os.environ["PATH"], "RUST_LOG": "error",
                    "SHARDLINE_ROOT_DIR": str(lane / "root"),
                    "SHARDLINE_BIND_ADDR": f"127.0.0.1:{self.port}",
                    "SHARDLINE_PUBLIC_BASE_URL": self.base,
                    "SHARDLINE_SERVER_ROLE": "all", "SHARDLINE_SERVER_FRONTENDS": frontend,
                    "SHARDLINE_OBJECT_STORAGE_ADAPTER": "local",
                    "SHARDLINE_CHUNK_SIZE_BYTES": "65536", "SHARDLINE_AUTH_PROVIDER": "local",
                    "SHARDLINE_TOKEN_SIGNING_KEY": "owned-dedup-fixture-key-0123456789"}
        self.token = command(binary, "--config", str(self.config), "admin", "token",
                             "--issuer", "fixture", "--subject", "fixture", "--scope", "write",
                             "--provider", "generic", "--owner", "team", "--repo", "bench",
                             "--key-env", "SHARDLINE_TOKEN_SIGNING_KEY", env=self.env)
        self.process = None
        self.log = (lane / "server.log").open("a")

    def start(self):
        self.process = subprocess.Popen([self.binary, "--config", str(self.config), "serve"],
                                        env=self.env, stdout=self.log, stderr=self.log)
        def healthy():
            if self.process.poll() is not None:
                raise RuntimeError("owned benchmark server exited; inspect its server.log")
            connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
            try:
                request(connection, "GET", "/healthz")
                return True
            finally:
                connection.close()
        ready(healthy)

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise RuntimeError("owned benchmark server failed clean shutdown") from None
        self.process = None

    def collect(self):
        self.stop()
        with (self.lane / "gc.log").open("w") as log:
            command(self.binary, "--config", str(self.config), "gc", "--mark", "--sweep",
                    "--retention-seconds", "0", env=self.env, stdout=log,
                    stderr=subprocess.STDOUT)
        return inventory(self.lane / "root")

    def close(self):
        self.stop()
        self.log.close()


NATIVE_VERIFY = r'''
import hashlib, json, os, pathlib, time
from hf_xet import XetSession, XetFileInfo
report = json.loads(pathlib.Path(os.environ["DEDUP_NATIVE_MANIFEST"]).read_text())
session = XetSession()
auth = dict(endpoint=os.environ["DEDUP_ENDPOINT"], token=os.environ["DEDUP_TOKEN"],
            token_expiry_unix_secs=int(time.time()) + 3600)
for index, value in enumerate(report):
    target = pathlib.Path(os.environ["HF_HOME"]) / f"verified-{index}.bin"
    group = session.new_file_download_group(**auth)
    group.start_download_file(XetFileInfo(value["hash"], value["size"]), str(target))
    group.wait_to_finish()
    assert hashlib.sha256(target.read_bytes()).hexdigest() == value["sha256"]
    target.unlink()
'''


def run(binary, output):
    if importlib.metadata.version("hf-xet") != "1.7.0":
        raise RuntimeError("qualification requires pinned hf_xet 1.7.0")
    output.mkdir(parents=True, exist_ok=False)
    files_dir = output / "corpus"
    files_dir.mkdir()
    files = corpus(files_dir)
    os.environ["HF_HOME"] = str(output / "native-upload-cache")
    os.environ["HF_XET_CACHE"] = str(output / "native-upload-cache" / "xet")
    from hf_xet import XetSession
    report = {"versions": VERSIONS, "file_bytes": SIZE, "edit_bytes": 4096,
              "cdc_target_bytes": 65536, "hf_xet": "1.7.0", "seed": 20261114,
              "logical_bytes": SIZE * VERSIONS, "lanes": {}}
    for frontend in ("xet", "s3", "lfs"):
        lane = output / frontend
        lane.mkdir()
        server = Server(binary, lane, frontend)
        connection = None
        try:
            server.start()
            connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=120)
            native = XetSession() if frontend == "xet" else None
            infos = []
            for version, (path, digest) in enumerate(files):
                if native is not None:
                    with native.new_upload_commit(endpoint=server.base, token=server.token,
                            token_expiry_unix_secs=int(time.time()) + 3600) as commit:
                        upload = commit.start_upload_file(str(path))
                    info = upload.result().xet_info
                    infos.append({"hash": info.hash, "size": info.file_size, "sha256": digest})
                else:
                    target = f"/team.bench/version-{version}" if frontend == "s3" else f"/v1/lfs/objects/{digest}"
                    if frontend == "lfs":
                        body = json.dumps({"operation": "upload", "transfers": ["basic"],
                                           "objects": [{"oid": digest, "size": SIZE}]}).encode()
                        connection.request("POST", "/v1/lfs/objects/batch", body,
                                           {"Authorization": f"Bearer {server.token}",
                                            "Content-Type": "application/vnd.git-lfs+json"})
                        response = connection.getresponse()
                        batch = json.loads(response.read())
                        assert response.status == 200 and batch["transfer"] == "basic", batch
                    request(connection, "PUT", target, server.token, path.read_bytes())
            before = inventory(lane / "root")
            connection.close()
            connection = None
            after = server.collect()
            server.start()
            if frontend == "xet":
                manifest = lane / "native-files.json"
                manifest.write_text(json.dumps(infos))
                home = lane / "empty-download-cache"
                home.mkdir()
                env = dict(os.environ, HF_HOME=str(home), HF_XET_CACHE=str(home / "xet"),
                           DEDUP_NATIVE_MANIFEST=str(manifest), DEDUP_ENDPOINT=server.base,
                           DEDUP_TOKEN=server.token, NO_PROXY="127.0.0.1,localhost")
                with (lane / "native-verify.log").open("w") as log:
                    command(sys.executable, "-c", NATIVE_VERIFY, env=env, stdout=log,
                            stderr=subprocess.STDOUT, timeout=180)
            else:
                connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=120)
                for version, (_, digest) in enumerate(files):
                    target = f"/team.bench/version-{version}" if frontend == "s3" else f"/v1/lfs/objects/{digest}"
                    assert hashlib.sha256(request(connection, "GET", target, server.token)).hexdigest() == digest
            remaining = None
            if frontend != "xet":
                for version, (_, digest) in enumerate(files[:-1]):
                    target = f"/team.bench/version-{version}" if frontend == "s3" else f"/v1/lfs/objects/{digest}"
                    request(connection, "DELETE", target, server.token)
                connection.close()
                connection = None
                remaining = server.collect()
                server.start()
                connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=120)
                digest = files[-1][1]
                target = f"/team.bench/version-{VERSIONS - 1}" if frontend == "s3" else f"/v1/lfs/objects/{digest}"
                assert hashlib.sha256(request(connection, "GET", target, server.token)).hexdigest() == digest
            report["lanes"][frontend] = {"after_source_deletion_gc": remaining,"before_gc": before, "after_gc": after,
                                           "verified_downloads_after_gc": VERSIONS}
            (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(frontend, after["physical_object_bytes"], flush=True)
        finally:
            if connection:
                connection.close()
            server.close()
    return report


if __name__ == "__main__":
    run(str(Path(sys.argv[1]).resolve(strict=True)), Path(sys.argv[2]).resolve())
