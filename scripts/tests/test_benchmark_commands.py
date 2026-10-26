"""Real process controls for bounded, credential-safe fixture commands."""

from pathlib import Path
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shardline"))
from benchmark_gc_contention import command  # noqa: E402
from performance_validation.fixtures import Deployment, require_running_server  # noqa: E402
from verify_disk_pressure import postgres_ready  # noqa: E402


class BenchmarkCommandTest(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "owned executable fixture requires POSIX")
    def test_disk_pressure_readiness_rejects_stalled_and_unready_commands(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as log:
            executable = Path(directory) / "docker"
            executable.write_text(
                f"#!{sys.executable}\n"
                "import sys,time\n"
                "name=sys.argv[2]\n"
                "if name=='stall': time.sleep(60)\n"
                "raise SystemExit(0 if name=='ready' else 1)\n"
            )
            executable.chmod(0o700)
            with patch.dict(os.environ, {"PATH": directory + os.pathsep + os.environ.get("PATH", "")}):
                self.assertTrue(postgres_ready("ready", log))
                self.assertFalse(postgres_ready("unready", log))
                started = time.monotonic()
                self.assertFalse(postgres_ready("stall", log))
                self.assertLess(time.monotonic() - started, 5)

    def test_trace_cleanup_failure_still_removes_owned_services(self):
        deployment = object.__new__(Deployment)
        deployment.trace = Mock()
        deployment.trace.close.side_effect = RuntimeError("fixture trace failed")
        deployment.stack = Mock()
        with self.assertRaisesRegex(RuntimeError, "fixture trace failed"):
            deployment.close()
        deployment.stack.close.assert_called_once_with()

    def test_exited_server_never_embeds_raw_logs_in_startup_failure(self):
        def inspect_only(*args, **kwargs):
            self.assertEqual(args, ("docker", "inspect", "owned-fixture"))
            return '[{"State":{"Running":false}}]'

        with patch("performance_validation.fixtures.command", side_effect=inspect_only):
            with self.assertRaisesRegex(RuntimeError, "raw diagnostics withheld"):
                require_running_server("owned-fixture")

    def test_running_server_passes_startup_state_check(self):
        with patch("performance_validation.fixtures.command",
                   return_value='[{"State":{"Running":true}}]'):
            require_running_server("owned-fixture")

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

    def test_private_file_capture_preserves_logs_without_returning_them(self):
        with tempfile.TemporaryFile(mode="w+") as log:
            result = command(sys.executable, "-c",
                             "import sys; print('private-output'); "
                             "print('private-diagnostic', file=sys.stderr)",
                             stdout=log, stderr=subprocess.STDOUT)
            self.assertEqual(result, "")
            log.seek(0)
            self.assertEqual(set(log.read().splitlines()),
                             {"private-output", "private-diagnostic"})

    def test_failed_file_capture_keeps_diagnostics_out_of_exception(self):
        with tempfile.TemporaryFile(mode="w+") as log:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                command(sys.executable, "-c",
                        "print('private-diagnostic'); raise SystemExit(7)",
                        stdout=log, stderr=subprocess.STDOUT)
            self.assertNotIn("private-", str(caught.exception))
            log.seek(0)
            self.assertIn("private-diagnostic", log.read())

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
