#!/usr/bin/env python3
"""Qualify S3 and stock Xet recovery on a bounded, real ext4 filesystem.

Linux only. Requires Docker with privileged containers, mkfs.ext4, boto3, and
a Python environment providing hf_xet.XetSession. Evidence must be outside Git.
The host filesystem is never filled: only the owned 512 MiB image is exhausted.
"""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

from benchmark_gc_contention import command as bounded_command

MINIO = "ghcr.io/golithus/minio@sha256:8793e960474071520bdb91bb9a6d1793eb229fdbc9dcb7ec75ad836b69bbab40"
DEBIAN = "debian@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f"
POSTGRES = "postgres@sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d"
KEY = "owned_pressure_key"
SECRET = "owned_pressure_secret"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def native_child(operation):
    from importlib.metadata import version
    from hf_xet import XetFileInfo, XetSession, hash_files

    if sys.flags.optimize:
        raise RuntimeError("native qualification requires Python assertions enabled")
    source = Path(os.environ["PRESSURE_NATIVE_SOURCE"])
    info = hash_files([str(source)])[0]
    result = Path(os.environ["PRESSURE_NATIVE_RESULT"])
    result.write_text(json.dumps({"hash": info.hash, "size": info.file_size,
                                 "client_version": version("hf_xet")}))
    if operation == "hash":
        return
    auth = dict(endpoint=os.environ["PRESSURE_NATIVE_ENDPOINT"],
                token=os.environ["PRESSURE_NATIVE_TOKEN"],
                token_expiry_unix_secs=int(time.time()) + 3600)
    session = XetSession()
    if operation == "upload":
        with session.new_upload_commit(**auth) as commit:
            upload = commit.start_upload_file(str(source))
        uploaded = upload.result().xet_info
        assert (uploaded.hash, uploaded.file_size) == (info.hash, info.file_size)
    elif operation == "download":
        destination = Path(os.environ["PRESSURE_NATIVE_DESTINATION"])
        group = session.new_file_download_group(**auth)
        group.start_download_file(XetFileInfo(info.hash, info.file_size), str(destination))
        group.wait_to_finish()
        assert destination.stat().st_size == source.stat().st_size
        assert digest(destination) == digest(source)
    else:
        raise ValueError("unknown native operation")


def request(port, method, path, token="", payload=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        connection.request(method, path, body=payload,
                           headers={"Authorization": "Bearer " + token})
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def wait_for(check):
    for _ in range(120):
        try:
            if check():
                return
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(0.5)
    raise RuntimeError("owned fixture readiness deadline exceeded")


def postgres_ready(name, log):
    try:
        bounded_command("docker", "exec", name, "pg_isready", "-h", "127.0.0.1",
                        "-U", "postgres", "-d", "pressure",
                        stdout=log, stderr=log, timeout=2)
        return True
    except (subprocess.CalledProcessError, TimeoutError):
        return False


def qualify(args, root, log):
    import boto3
    from botocore.config import Config

    binary = Path(args.binary).resolve(strict=True)
    # Resolving a venv's interpreter symlink would silently select the base
    # environment and lose the caller's pinned client dependencies.
    native_python = Path(args.native_python).expanduser().absolute()
    if not native_python.is_file() or not os.access(native_python, os.X_OK):
        raise RuntimeError("native Python interpreter is not executable")
    subprocess.run([str(native_python), "-c", "from hf_xet import XetFileInfo, XetSession, hash_files"],
                   stdout=log, stderr=log, check=True, timeout=30)
    names = []
    server = None
    prefix = "shardline-pressure-" + uuid.uuid4().hex[:12]

    def command(*arguments):
        return subprocess.check_output(arguments, text=True, stderr=log, timeout=120).strip()

    def run(*arguments):
        subprocess.run(arguments, stdout=log, stderr=log, check=True, timeout=120)

    def docker_port(name, port):
        state = json.loads(command("docker", "inspect", name))[0]
        return int(state["NetworkSettings"]["Ports"][f"{port}/tcp"][0]["HostPort"])

    def stop_server():
        nonlocal server
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
            server = None

    try:
        source = prefix + "-binary"
        names.append(source)
        command("docker", "create", "--name", source, MINIO)
        run("docker", "cp", f"{source}:/minio", str(root / "minio"))
        with (root / "data.ext4").open("xb") as image:
            image.truncate(512 * 1024 * 1024)
        run("mkfs.ext4", "-q", "-F", "-m", "0", str(root / "data.ext4"))
        # Allocate and release only this image's loop device. The mount is
        # private to this container and is never propagated to the host.
        shell = """set -eu
mkdir /data
# Docker may expose only the loop nodes that existed when it created /dev.
# These nodes are private to this container; allocation still uses the kernel's
# free-device lookup, so concurrent fixtures never reuse an occupied device.
for index in $(seq 0 63); do
    test -b /dev/loop$index || mknod /dev/loop$index b 7 $index
done
device=$(losetup --find --show /fixture/data.ext4)
printf '%s' "$device" > /fixture/owned-loop-device
mounted=0
child=''
cleanup() {
    if [ -n "$child" ]; then kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi
    if [ "$mounted" = 1 ]; then umount /data; fi
    losetup -d "$device"
}
trap cleanup EXIT
trap 'exit 0' TERM INT
mount "$device" /data
mounted=1
/fixture/minio server /data --console-address :9001 &
child=$!
wait "$child"
"""
        minio = prefix + "-minio"
        names.append(minio)
        command("docker", "run", "-d", "--privileged", "--name", minio,
                "-v", f"{root}:/fixture:Z", "-p", "127.0.0.1::9000",
                "-e", f"MINIO_ROOT_USER={KEY}", "-e", f"MINIO_ROOT_PASSWORD={SECRET}",
                DEBIAN, "/bin/sh", "-c", shell)
        port = docker_port(minio, 9000)
        wait_for(lambda: request(port, "GET", "/minio/health/ready")[0] == 200)
        client = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                              aws_access_key_id=KEY, aws_secret_access_key=SECRET,
                              region_name="us-east-1",
                              config=Config(connect_timeout=5, read_timeout=5,
                                            retries={"max_attempts": 0}))
        client.create_bucket(Bucket="pressure")
        config = root / "empty.toml"
        config.write_text("")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            api_port = sock.getsockname()[1]
        # Do not accidentally inherit a developer's production backend settings.
        env = {key: value for key, value in os.environ.items() if not key.startswith("SHARDLINE_")}
        env.update(SHARDLINE_ROOT_DIR=str(root / "metadata"),
                   SHARDLINE_OBJECT_STORAGE_ADAPTER="s3", SHARDLINE_S3_BUCKET="pressure",
                   SHARDLINE_S3_REGION="us-east-1", SHARDLINE_S3_ENDPOINT=f"http://127.0.0.1:{port}",
                   SHARDLINE_S3_ALLOW_HTTP="true", SHARDLINE_S3_ACCESS_KEY_ID=KEY,
                   SHARDLINE_S3_SECRET_ACCESS_KEY=SECRET, SHARDLINE_AUTH_PROVIDER="local",
                   SHARDLINE_TOKEN_SIGNING_KEY="0123456789abcdef0123456789abcdef",
                   SHARDLINE_BIND_ADDR=f"127.0.0.1:{api_port}",
                   SHARDLINE_PUBLIC_BASE_URL=f"http://127.0.0.1:{api_port}")
        if args.metadata == "postgresql":
            postgres = prefix + "-postgres"
            names.append(postgres)
            command("docker", "run", "-d", "--name", postgres, "-p", "127.0.0.1::5432",
                    "-e", f"POSTGRES_PASSWORD={SECRET}", "-e", "POSTGRES_DB=pressure", POSTGRES)

            wait_for(lambda: postgres_ready(postgres, log))
            database = f"postgres://postgres:{SECRET}@127.0.0.1:{docker_port(postgres, 5432)}/pressure"
            env["SHARDLINE_INDEX_POSTGRES_URL"] = database
            subprocess.run([str(binary), "--config", str(config), "db", "migrate", "up",
                            "--database-url", database], env=env, stdout=log, stderr=log,
                           check=True, timeout=120)
        token = bounded_command(str(binary), "--config", str(config), "admin", "token",
                                "--issuer", "fixture", "--subject", "fixture", "--scope", "write",
                                "--provider", "generic", "--owner", "team", "--repo", "bench",
                                "--key-env", "SHARDLINE_TOKEN_SIGNING_KEY",
                                env=env, stderr=log, timeout=30)

        def start_server():
            nonlocal server
            server = subprocess.Popen([str(binary), "--config", str(config), "serve", "--role", "all",
                                       "--frontend", "s3,xet"], env=env, stdout=log, stderr=log)
            wait_for(lambda: request(api_port, "GET", "/readyz")[0] == 200)

        sequence = 0

        def native(operation, source):
            nonlocal sequence
            sequence += 1
            home = root / f"native-{sequence}"
            home.mkdir()
            child_env = dict(env, PRESSURE_NATIVE_SOURCE=str(source),
                             PRESSURE_NATIVE_RESULT=str(home / "result.json"),
                             PRESSURE_NATIVE_DESTINATION=str(home / "download.bin"),
                             PRESSURE_NATIVE_ENDPOINT=f"http://127.0.0.1:{api_port}",
                             PRESSURE_NATIVE_TOKEN=token, HF_HOME=str(home), HF_XET_CACHE=str(home / "xet"),
                             NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
            with (home / "client.log").open("wb") as output:
                completed = subprocess.run([str(native_python), str(Path(__file__).resolve()),
                                            "--native-child", operation], env=child_env,
                                           stdout=output, stderr=output, timeout=600)
            return completed.returncode, json.loads((home / "result.json").read_text())

        start_server()
        acknowledged = hashlib.shake_256(b"acknowledged-before-exhaustion").digest(1024 * 1024)
        retry = hashlib.shake_256(b"failed-then-recovered").digest(1024 * 1024)
        native_old = root / "native-acknowledged.bin"
        native_new = root / "native-retry.bin"
        native_old.write_bytes(hashlib.shake_256(b"native-acknowledged").digest(9 * 1024 * 1024))
        native_new.write_bytes(hashlib.shake_256(b"native-retry").digest(9 * 1024 * 1024))
        assert 200 <= request(api_port, "PUT", "/team.bench/acknowledged", token, acknowledged)[0] < 300
        assert native("upload", native_old)[0] == 0
        assert native("download", native_old)[0] == 0
        hash_status, info = native("hash", native_new)
        assert hash_status == 0
        full = subprocess.run(["docker", "exec", minio, "dd", "if=/dev/zero",
                               "of=/data/owned-pressure-fill", "bs=1048576", "count=512"],
                              stdout=log, stderr=subprocess.PIPE, timeout=60)
        assert full.returncode != 0 and b"No space left on device" in full.stderr
        provider_error = None

        def backend_reports_full():
            nonlocal provider_error
            try:
                client.put_object(Bucket="pressure", Key="must-not-persist", Body=retry)
            except client.exceptions.ClientError as error:
                provider_error = error.response["Error"]["Code"]
                # A real ENOSPC can initially surface as an aborted request
                # body. Require the explicit capacity response before testing
                # Shardline; never count that preliminary error as qualification.
                if provider_error in {"IncompleteBody", "InternalError"}:
                    log.write(
                        f"backend exhaustion probe: {provider_error}; awaiting capacity response\n".encode()
                    )
                    log.flush()
                    return False
                return True
            return True

        capacity_deadline = time.monotonic() + 30
        while not backend_reports_full() and time.monotonic() < capacity_deadline:
            time.sleep(0.5)
        assert provider_error == "XMinioStorageFull", (
            "backend exhaustion prerequisite failed: expected XMinioStorageFull, "
            f"observed {provider_error!r}"
        )
        failed_status, _ = request(api_port, "PUT", "/team.bench/retry", token, retry)
        assert not 200 <= failed_status < 300
        assert request(api_port, "GET", "/team.bench/retry", token)[0] == 404
        assert request(api_port, "GET", "/team.bench/acknowledged", token) == (200, acknowledged)
        assert native("upload", native_new)[0] != 0
        assert request(api_port, "GET", "/v1/reconstructions/" + info["hash"], token)[0] == 404
        assert native("download", native_old)[0] == 0
        run("docker", "exec", minio, "rm", "/data/owned-pressure-fill")
        wait_for(lambda: 200 <= request(api_port, "PUT", "/team.bench/retry", token, retry)[0] < 300)
        assert native("upload", native_new)[0] == 0
        assert native("download", native_new)[0] == 0
        stop_server()
        start_server()
        assert request(api_port, "GET", "/team.bench/acknowledged", token) == (200, acknowledged)
        assert request(api_port, "GET", "/team.bench/retry", token) == (200, retry)
        assert native("download", native_old)[0] == 0
        assert native("download", native_new)[0] == 0
        result = dict(binary_sha256=digest(binary), metadata_backend=args.metadata,
                      native_hf_xet_version=info["client_version"],
                      actual_enospc=True, provider_error=provider_error,
                      failed_upload_status=failed_status, acknowledged_bytes_preserved=True,
                      failed_files_unpublished=True, identical_retries_recovered=True,
                      byte_exact_after_restart=True)
    finally:
        stop_server()
        cleanup_failed = False
        for name in reversed(names):
            try:
                subprocess.run(["docker", "stop", "--time", "20", name], stdout=log, stderr=log, timeout=30)
                if name.endswith("-minio"):
                    state = json.loads(command("docker", "inspect", name))[0]["State"]
                    cleanup_failed |= state["Running"] or state["ExitCode"] != 0
            except Exception:
                cleanup_failed = True
            finally:
                # An unsuccessful fixture must still remove all its containers.
                try:
                    removed = subprocess.run(["docker", "rm", "-f", "-v", name], stdout=log, stderr=log, timeout=30)
                    cleanup_failed |= removed.returncode != 0
                except Exception:
                    cleanup_failed = True
        assert not cleanup_failed, "owned fixture cleanup failed"
    (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("output", help="new private directory outside the checkout")
    parser.add_argument("--metadata", choices=["sqlite", "postgresql"], required=True)
    parser.add_argument("--native-python", required=True, help="Python with hf_xet.XetSession")
    args = parser.parse_args()
    if sys.platform != "linux" or sys.flags.optimize:
        parser.error("requires Linux and Python assertions enabled")
    root = Path(args.output).resolve()
    if root.is_relative_to(Path(__file__).resolve().parents[2]):
        parser.error("evidence must be outside the checkout")
    os.umask(0o077)
    root.mkdir(parents=True, mode=0o700)
    with (root / "qualification.log").open("wb") as log:
        try:
            qualify(args, root, log)
        except Exception:
            # Tracebacks and upstream errors may include credentials or signed
            # URLs. Keep them private and never echo their text into CI output.
            import traceback
            log.write(traceback.format_exc().encode())
            print(f"FAIL: inspect private evidence in {root}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--native-child":
        native_child(sys.argv[2])
    else:
        sys.exit(main())
