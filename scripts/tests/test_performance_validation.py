"""Guard benchmark math and data shape against misleading results."""

from pathlib import Path
from collections import Counter
import os
import selectors
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shardline"))
from performance_validation.measurements import (  # noqa: E402
    RequestTrace, inventory_summary, percentiles, request_summary, scale_efficiency,
)
from performance_validation.workload import payload  # noqa: E402
from performance_validation.database import resource_delta  # noqa: E402


class PerformanceValidationTest(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "process groups require POSIX")
    def test_trace_shutdown_stops_descendants_holding_the_reader_pipe(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "escaped-trace"
            child = ("import time; from pathlib import Path; "
                     "print('ready', flush=True); time.sleep(1); "
                     f"Path({str(marker)!r}).touch(); time.sleep(10)")
            parent = ("import subprocess,sys; "
                      f"subprocess.Popen([sys.executable,'-c',{child!r}]).wait()")
            process = subprocess.Popen([sys.executable, "-c", parent],
                                       stdout=subprocess.PIPE, text=True,
                                       start_new_session=True)
            trace = object.__new__(RequestTrace)
            trace.process = process
            trace.owns_process_group = True
            trace.thread = threading.Thread(target=lambda: process.stdout.read(), daemon=True)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(timeout=3), "trace child did not start")
                self.assertEqual(process.stdout.readline().strip(), "ready")
                trace.thread.start()
                started = time.monotonic()
                trace.close()
                self.assertLess(time.monotonic() - started, 2)
                self.assertFalse(trace.thread.is_alive())
                time.sleep(1.2)
                self.assertFalse(marker.exists())
            finally:
                # Own only this test's session, including on assertion failure.
                import signal
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=3)

    def test_block_io_deltas_preserve_devices_and_include_newly_active_devices(self):
        before = {"cpu_seconds": 4, "block_io_by_device": {
            "259:0": {"rbytes": 100, "wbytes": 200}}}
        after = {"cpu_seconds": 7, "peak_memory_bytes": 512, "block_io_by_device": {
            "259:0": {"rbytes": 150, "wbytes": 240},
            "253:0": {"rbytes": 50, "wbytes": 40}}}
        result = resource_delta(before, after)
        self.assertEqual(result["cpu_seconds"], 3)
        self.assertEqual(result["peak_memory_bytes"], 512)
        self.assertEqual(result["block_io_by_device"], {
            "259:0": {"rbytes": 50, "wbytes": 40},
            "253:0": {"rbytes": 50, "wbytes": 40}})
        self.assertEqual(before["block_io_by_device"]["259:0"]["rbytes"], 100)

    def test_reset_or_disappearing_device_invalidates_block_io_measurement(self):
        before = {"cpu_seconds": 0, "block_io_by_device": {"259:0": {"wbytes": 200}}}
        for devices in ({"259:0": {"wbytes": 100}}, {}, {"259:0": {}}):
            with self.assertRaisesRegex(RuntimeError, "block I/O"):
                resource_delta(before, {"cpu_seconds": 1, "block_io_by_device": devices})

    def test_inventory_counts_both_packed_and_fallback_bytes(self):
        result = inventory_summary([
            {"Key": "aa/" + "a" * 64, "Size": 100},
            {"Key": "xorbs/default/aa/" + "a" * 64 + ".xorb", "Size": 110},
            {"Key": "_shardline_tasks/evidence", "Size": 7},
        ])
        self.assertEqual(result["stored_payload_bytes"], 217)
        self.assertEqual(result["objects"], 3)
        self.assertEqual(result["by_class"]["individual_chunks"]["bytes"], 100)
        self.assertEqual(result["by_class"]["xorbs"]["bytes"], 110)

    def test_tail_latency_uses_nearest_rank(self):
        self.assertEqual(percentiles(list(reversed(range(1, 101)))),
                         {"p50_ms": 50, "p95_ms": 95, "p99_ms": 99})
        self.assertEqual(percentiles([42]), {"p50_ms": 42, "p95_ms": 42, "p99_ms": 42})
        with self.assertRaises(ValueError):
            percentiles([])

    def test_request_cost_includes_failed_dedupe_probes(self):
        events = [{"api": "s3.HeadObject", "method": "HEAD", "status": 404,
                   "key": "aa/" + "a" * 64, "rx": 100, "tx": 0},
                  {"api": "s3.PutObject", "method": "PUT", "status": 412,
                   "key": "aa/" + "a" * 64, "rx": 200, "tx": 0}]
        result = request_summary(events, 1_000_000)
        self.assertEqual(result["requests"], 2)
        self.assertEqual(result["requests_per_decimal_gb"], 2000)
        self.assertEqual(result["by_api_status"]["s3.PutObject:412"], 1)

    def test_scale_efficiency_divides_by_added_replicas(self):
        self.assertAlmostEqual(scale_efficiency(560, 100, 8), 0.7)
        self.assertAlmostEqual(scale_efficiency(338, 100, 8), 0.4225)
        with self.assertRaises(ValueError):
            scale_efficiency(100, 0, 8)

    def trace(self):
        trace = object.__new__(RequestTrace)
        trace.endpoint = "http://fixture"
        trace.server_before = Counter({"s3.PutObject": 10, "s3.HeadObject": 1})
        trace.observed_before = Counter()
        trace.probes_before = 1
        trace.probe_attempts = 2
        trace.events = [{"api": "s3.PutObject"}]
        trace.barrier = lambda: None
        return trace

    def test_trace_reconciles_attempts_and_excludes_its_own_probes(self):
        trace = self.trace()
        with patch("performance_validation.measurements.server_request_counts",
                   return_value=Counter({"s3.PutObject": 11, "s3.HeadObject": 2})):
            self.assertEqual(trace.validate()["matched_server_api_counts"], {"s3.PutObject": 1})

    def test_missing_trace_event_fails_instead_of_underreporting_cost(self):
        trace = self.trace()
        with patch("performance_validation.measurements.server_request_counts",
                   return_value=Counter({"s3.PutObject": 12, "s3.HeadObject": 2})), \
                patch("performance_validation.measurements.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "trace/counter mismatch"):
                trace.validate()

    def test_server_counter_reset_invalidates_a_run(self):
        trace = self.trace()
        with patch("performance_validation.measurements.server_request_counts",
                   return_value=Counter({"s3.PutObject": 1, "s3.HeadObject": 2})):
            with self.assertRaisesRegex(RuntimeError, "counters reset"):
                trace.validate()

    def test_repeated_fixture_is_four_copies_of_otherwise_unique_content(self):
        copies = [payload("repeated", i, 4096) for i in range(8)]
        self.assertEqual(len(set(copies)), 2)
        self.assertTrue(all(value == copies[0] for value in copies[:4]))
        self.assertEqual(len({payload("unique", i, 4096) for i in range(8)}), 8)

    def test_cli_rejects_missing_single_replica_baseline_before_docker(self):
        script = Path(__file__).resolve().parents[1] / "shardline" / "validate_performance.py"
        result = subprocess.run([sys.executable, "-B", str(script), sys.executable,
                                 "/tmp/unused-benchmark-output", "--replicas", "2", "4"],
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("replicas must include 1", result.stderr)


if __name__ == "__main__":
    unittest.main()
