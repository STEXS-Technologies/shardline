"""Owned PostgreSQL/MinIO services and resource-limited S3 API replicas."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import time

from benchmark_gc_contention import MINIO, command, port, ready
from .measurements import RequestTrace, inventory_summary


def dispose(container):
    subprocess.run(["docker", "rm", "-f", container], check=True, stdout=subprocess.DEVNULL)


class Deployment:
    def __init__(self, output, binary, cpus, memory):
        import boto3
        from botocore.config import Config

        self.output = output
        self.binary = binary
        self.cpus = cpus
        self.memory = memory
        self.ca_bundle = Path(ssl.get_default_verify_paths().cafile or "").resolve(strict=True)
        if not self.ca_bundle.is_file():
            raise RuntimeError("a host CA certificate bundle is required for the container HTTP client")
        self.stack = ExitStack()
        self.owned = set()
        self.servers = []
        try:
            self.pg = self.container("-e", "POSTGRES_PASSWORD=owned_performance_password",
                                     "-p", "127.0.0.1::5432", "postgres:16-alpine", "-c",
                                     "shared_preload_libraries=pg_stat_statements", "-c",
                                     "pg_stat_statements.track=all", "-c", "max_connections=300")
            self.s3 = self.container("-e", "MINIO_ROOT_USER=performance", "-e",
                                     "MINIO_ROOT_PASSWORD=owned_performance_password",
                                     "-e", "MINIO_PROMETHEUS_AUTH_TYPE=public",
                                     "-p", "127.0.0.1::9000", MINIO, "server", "/data")
            ready(lambda: subprocess.run(["docker", "exec", self.pg, "pg_isready", "-U", "postgres"],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0)
            self.endpoint = f"http://127.0.0.1:{port(self.s3, 9000)}"
            self.client = boto3.client("s3", endpoint_url=self.endpoint,
                                       aws_access_key_id="performance",
                                       aws_secret_access_key="owned_performance_password",
                                       region_name="us-east-1",
                                       config=Config(retries={"total_max_attempts": 1},
                                                     max_pool_connections=128))
            from botocore.exceptions import BotoCoreError, ClientError
            def available():
                try:
                    return self.client.list_buckets() is not None
                except (BotoCoreError, ClientError):
                    return False
            ready(available)
        except BaseException:
            self.stack.close()
            raise

    def container(self, *arguments):
        container = command("docker", "run", "-d", *arguments)
        self.owned.add(container)
        self.stack.callback(self.remove, container)
        return container

    def remove(self, container):
        if container in self.owned:
            dispose(container)
            self.owned.remove(container)

    def sql(self, database, query):
        return command("docker", "exec", self.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                       "-U", "postgres", "-d", database, "-c", query.strip())

    def lane(self, name, replicas):
        lane = self.output / name
        lane.mkdir()
        (lane / "empty.toml").touch()
        command("docker", "exec", self.pg, "createdb", "-U", "postgres", name)
        self.sql(name, "CREATE EXTENSION pg_stat_statements")
        database = f"postgres://postgres:owned_performance_password@127.0.0.1:{port(self.pg, 5432)}/{name}"
        self.client.create_bucket(Bucket=name)
        self.env = {"PATH": os.environ["PATH"], "RUST_LOG": "error",
                    "SHARDLINE_ROOT_DIR": str(lane / "maintenance-root"),
                    "SHARDLINE_INDEX_POSTGRES_URL": database,
                    "SHARDLINE_OBJECT_STORAGE_ADAPTER": "s3", "SHARDLINE_S3_BUCKET": name,
                    "SHARDLINE_S3_REGION": "us-east-1", "SHARDLINE_S3_ENDPOINT": self.endpoint,
                    "SHARDLINE_S3_ALLOW_HTTP": "true", "SHARDLINE_S3_ACCESS_KEY_ID": "performance",
                    "SHARDLINE_S3_SECRET_ACCESS_KEY": "owned_performance_password",
                    "SHARDLINE_AUTH_PROVIDER": "local",
                    "SHARDLINE_TOKEN_SIGNING_KEY": "0123456789abcdef0123456789abcdef"}
        self.config = str(lane / "empty.toml")
        with (lane / "migration.log").open("w") as log:
            subprocess.run([self.binary, "--config", self.config, "db", "migrate", "up",
                            "--database-url", database], env=self.env, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=120)
        self.token = command(self.binary, "--config", self.config, "admin", "token", "--issuer", "fixture",
                             "--subject", "fixture", "--scope", "write", "--provider", "generic",
                             "--owner", "team", "--repo", "bench", "--key-env",
                             "SHARDLINE_TOKEN_SIGNING_KEY", env=self.env)
        self.servers = []
        self.ports = []
        for replica in range(replicas):
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                listen = sock.getsockname()[1]
            env = dict(self.env, SHARDLINE_ROOT_DIR=f"/fixture/root-{replica}",
                       SSL_CERT_FILE="/ca-certificates.crt",
                       SHARDLINE_BIND_ADDR=f"127.0.0.1:{listen}",
                       SHARDLINE_PUBLIC_BASE_URL=f"http://127.0.0.1:{listen}")
            arguments = []
            for key, value in env.items():
                if key != "PATH":
                    arguments.extend(("-e", f"{key}={value}"))
            container = self.container("--network", "host", "--cpus", str(self.cpus),
                                       "--memory", self.memory, "-v", f"{self.binary}:/shardline:ro,z",
                                       "-v", f"{self.ca_bundle}:/ca-certificates.crt:ro",
                                       "-v", f"{lane}:/fixture:z", *arguments,
                                       "debian:13-slim", "/shardline", "--config", "/fixture/empty.toml",
                                       "serve", "--role", "api", "--frontend", "s3")
            self.servers.append(container)
            self.ports.append(listen)
        from benchmark_gc_contention import request
        import http.client
        for container, listen in zip(self.servers, self.ports):
            def healthy():
                state = json.loads(command("docker", "inspect", container))[0]["State"]
                if not state["Running"]:
                    raise RuntimeError(command("docker", "logs", container, stderr=subprocess.STDOUT))
                connection = http.client.HTTPConnection("127.0.0.1", listen, timeout=2)
                try:
                    request(connection, "GET", "/readyz")
                    return True
                finally:
                    connection.close()
            ready(healthy)
        mc_env = dict(os.environ, MC_HOST_performance=self.endpoint.replace(
            "http://", "http://performance:owned_performance_password@"))
        self.trace = RequestTrace(subprocess.Popen(
            ["mc", "admin", "trace", "--json", "--verbose", "--call", "s3", "performance"],
            env=mc_env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True),
            self.client, name, self.endpoint)
        return lane

    def inventory(self, name):
        objects = self.client.get_paginator("list_objects_v2").paginate(Bucket=name)
        return inventory_summary(item for page in objects for item in page.get("Contents", []))

    def gc(self, log):
        with log.open("w") as output:
            start = time.monotonic()
            subprocess.run([self.binary, "--config", self.config, "gc", "--mark", "--sweep",
                            "--retention-seconds", "0"], env=self.env, stdout=output,
                           stderr=subprocess.STDOUT, check=True, timeout=180)
            return time.monotonic() - start

    def stop_lane(self, lane):
        if hasattr(self, "trace"):
            self.trace.close()
            del self.trace
        for index, server in enumerate(self.servers):
            (lane / f"server-{index}.log").write_text(command("docker", "logs", server, stderr=subprocess.STDOUT))
            self.remove(server)
        self.servers = []

    def close(self):
        if hasattr(self, "trace"):
            self.trace.close()
        self.stack.close()
