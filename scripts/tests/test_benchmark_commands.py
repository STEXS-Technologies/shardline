"""Real process controls for bounded, credential-safe fixture commands."""

from pathlib import Path
import os
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shardline"))
from benchmark_gc_contention import command  # noqa: E402


class BenchmarkCommandTest(unittest.TestCase):
    def test_success_preserves_output(self):
        self.assertEqual(command(sys.executable, "-c", "print('fixture-result')"),
                         "fixture-result")

    def test_private_log_collection_can_merge_stderr(self):
        output = command(sys.executable, "-c",
                         "import sys; print('result', flush=True); "
                         "print('diagnostic', file=sys.stderr)",
                         stderr=subprocess.STDOUT)
        self.assertEqual(output.splitlines(), ["result", "diagnostic"])

    def test_large_output_does_not_deadlock_deadline_wait(self):
        output = command(sys.executable, "-c", "print('x' * 1048576)", timeout=5)
        self.assertEqual(len(output), 1048576)

    def test_failed_command_never_echoes_arguments_or_diagnostics(self):
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            command(sys.executable, "-c",
                    "import sys; print('private-output'); "
                    "print('private-diagnostic', file=sys.stderr); sys.exit(7)",
                    "private-argument")
        error = caught.exception
        self.assertEqual(error.returncode, 7)
        self.assertNotIn("private-", str(error))
        self.assertIsNone(error.output)
        self.assertIsNone(error.stderr)

    @unittest.skipUnless(os.name == "posix", "process groups require POSIX")
    def test_successful_parent_does_not_leave_background_descendants(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "escaped"
            command("sh", "-c", '(sleep 1; printf escaped > "$1") & exit 0',
                    "fixture-control", str(marker), timeout=5)
            time.sleep(1.2)
            self.assertFalse(marker.exists())

    @unittest.skipUnless(os.name == "posix", "process groups require POSIX")
    def test_deadline_stops_descendants(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "escaped"
            started = time.monotonic()
            with self.assertRaises(TimeoutError) as caught:
                command("sh", "-c", '(sleep 1; printf escaped > "$1") & wait',
                        "fixture-control", str(marker), timeout=0.1)
            self.assertLess(time.monotonic() - started, 2)
            self.assertNotIn(str(marker), str(caught.exception))
            time.sleep(1.2)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
