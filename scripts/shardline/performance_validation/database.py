"""Observed database saturation, query mix and container CPU/memory."""

import json
from pathlib import Path
import os
import threading
import time

from benchmark_gc_contention import command


def snapshot(deployment, name):
    return json.loads(deployment.sql(name, """
      /* performance-observer */ SELECT json_build_object(
        'database_bytes', pg_database_size(current_database()),
        'transactions', xact_commit + xact_rollback,
        'blocks_read', blks_read, 'blocks_hit', blks_hit,
        'deadlocks', deadlocks, 'temp_bytes', temp_bytes)
      FROM pg_stat_database WHERE datname = current_database()
    """))


def query_mix(deployment, name):
    return json.loads(deployment.sql(name, """
      SELECT json_build_object('calls', coalesce(sum(calls),0),
        'execution_ms', coalesce(sum(total_exec_time),0),
        'shared_blocks_read', coalesce(sum(shared_blks_read),0),
        'shared_blocks_hit', coalesce(sum(shared_blks_hit),0),
        'statement_wal_bytes', coalesce(sum(wal_bytes),0),
        'statement_wal_records', coalesce(sum(wal_records),0),
        'statement_wal_full_page_images', coalesce(sum(wal_fpi),0),
        'temp_blocks_written', coalesce(sum(temp_blks_written),0))
      FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname=current_database())
        AND query NOT LIKE '/* performance-observer */%'
    """.replace("SELECT json_build_object", "/* performance-observer */ SELECT json_build_object", 1)))


def top_queries(deployment, name):
    return json.loads(deployment.sql(name, """
      /* performance-observer */ SELECT coalesce(json_agg(t),'[]') FROM (
        SELECT left(query,300) AS query, calls, total_exec_time AS execution_ms,
          rows, shared_blks_read, shared_blks_hit, temp_blks_written,
          wal_bytes AS statement_wal_bytes, wal_records AS statement_wal_records,
          wal_fpi AS statement_wal_full_page_images
        FROM pg_stat_statements
        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND query NOT LIKE '/* performance-observer */%'
        ORDER BY total_exec_time DESC LIMIT 12) t
    """))


def process_sample(pid):
    stat = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
    status = Path(f"/proc/{pid}/status").read_text().splitlines()
    memory = int(next(line.split()[1] for line in status if line.startswith("VmHWM:")))
    return {"cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
            "peak_rss_kib": memory}


def container_sample(pid):
    group = next(line[3:] for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines()
                 if line.startswith("0::"))
    root = Path("/sys/fs/cgroup") / group.lstrip("/")
    cpu = dict(line.split() for line in (root / "cpu.stat").read_text().splitlines())
    return {"cpu_seconds": int(cpu["usage_usec"]) / 1_000_000,
            "peak_memory_bytes": int((root / "memory.peak").read_text())}


class Sampler:
    def __init__(self, deployment, name):
        self.deployment = deployment
        self.name = name
        containers = deployment.servers + [deployment.pg, deployment.s3]
        pids = [int(command("docker", "inspect", "--format", "{{.State.Pid}}", c)) for c in containers]
        # cgroup totals include PostgreSQL workers and maintenance helper processes.
        # Memory is the lifetime cgroup high-water mark, including page cache.
        self.pids = dict(zip([f"replica_{n}" for n in range(len(deployment.servers))]
                             + ["postgres", "minio"], pids))
        self.pids["load_generator"] = os.getpid()
        self.start = {key: self.sample(key, pid) for key, pid in self.pids.items()}
        self.resources = dict(self.start)
        self.samples = []
        self.error = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    @staticmethod
    def sample(key, pid):
        return process_sample(pid) if key == "load_generator" else container_sample(pid)

    def _run(self):
        try:
            while not self.stop.is_set():
                row = json.loads(self.deployment.sql(self.name, """
                  /* performance-observer */ SELECT json_build_object(
                    'connections', count(*),
                    'active', count(*) FILTER (WHERE state='active'),
                    'lock_waiting', count(*) FILTER (WHERE wait_event_type='Lock'),
                    'blocked', count(*) FILTER (WHERE cardinality(pg_blocking_pids(pid))>0))
                  FROM pg_stat_activity WHERE datname=current_database()
                    AND pid<>pg_backend_pid() AND backend_type='client backend'
                """))
                self.samples.append(dict(seconds=time.monotonic(), **row))
                for key, pid in self.pids.items():
                    self.resources[key] = self.sample(key, pid)
                self.stop.wait(0.5)
        except Exception as error:
            self.error = error

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=15)
        if self.thread.is_alive():
            raise RuntimeError("database sampler did not stop")
        if self.error:
            raise self.error
        if not self.samples:
            raise RuntimeError("database sampler collected no observations")
        self.resources = {key: self.sample(key, pid) for key, pid in self.pids.items()}
        return {"postgres_peak": {key: max(row[key] for row in self.samples)
                                  for key in ("connections", "active", "lock_waiting", "blocked")},
                "resources": {key: dict(value, cpu_seconds=value["cpu_seconds"] - self.start[key]["cpu_seconds"])
                              for key, value in self.resources.items()},
                "samples": self.samples}
