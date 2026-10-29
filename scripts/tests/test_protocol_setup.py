"""Exercise the real conformance entry point with owned stalled tool processes."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "verify-protocol-conformance.sh"
TOOL = r'''
import json, os, pathlib, subprocess, sys, time
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ["PROTOCOL_CONTROL_ROOT"])
stage = ("signer" if name == "go" and "build" in args else
         "conformance" if name == "go" else
         "fetch" if name == "git" and "fetch" in args else "")
if stage and stage == os.environ["PROTOCOL_CONTROL_STALL"]:
    child = subprocess.Popen(["/bin/sleep", "60"])
    (root / "child.json").write_text(json.dumps(child.pid))
    time.sleep(60)
if name == "git" and "rev-parse" in args:
    print("a139cc423184af6078077b9b7ee336eddbd03f8f")
if name == "git" and "checkout" in args:
    (pathlib.Path(args[1]) / "conformance").mkdir()
if name == "go":
    output = pathlib.Path(args[args.index("-o") + 1])
    output.write_text("#!/bin/sh\nexit 0\n")
    output.chmod(0o700)
if name == "cargo":
    (root / "cargo-ran").touch()
'''


@unittest.skipUnless(sys.platform == "linux" and shutil.which("timeout"),
                     "release setup uses Linux process groups and GNU timeout")
class ProtocolSetupTest(unittest.TestCase):
    def exercise(self, stall, seconds="1"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "bin"
            tools.mkdir()
            scratch = root / "scratch"
            scratch.mkdir()
            for name in ("git", "go", "aws", "python3", "cargo"):
                tool = tools / name
                tool.write_text(f"#!{sys.executable}\n{TOOL}")
                tool.chmod(0o700)
            env = os.environ.copy()
            for key in ("MINIO_SIGNER_BIN", "OCI_CONFORMANCE_BIN", "DATABASE_URL",
                        "TEST_S3_BUCKET", "TEST_S3_ENDPOINT", "TEST_S3_ACCESS_KEY",
                        "TEST_S3_SECRET_KEY", "BOTOCORE_PYTHON"):
                env.pop(key, None)
            env.update(PATH=str(tools) + os.pathsep + os.defpath,
                       TMPDIR=str(scratch), PROTOCOL_CONTROL_ROOT=str(root),
                       PROTOCOL_CONTROL_STALL=stall,
                       SHARDLINE_PROTOCOL_SETUP_TIMEOUT_SECONDS=seconds)
            try:
                result = subprocess.run(["bash", str(SCRIPT)], env=env,
                                        capture_output=True, text=True, timeout=10)
                child_file = root / "child.json"
                if child_file.exists():
                    pid = json.loads(child_file.read_text())
                    status = Path(f"/proc/{pid}/stat")
                    if status.exists():
                        self.assertEqual(status.read_text().split(")", 1)[1].split()[0], "Z",
                                         "timed-out preparation left a live descendant")
                self.assertEqual(list(scratch.iterdir()), [], "owned setup paths leaked")
                return result, (root / "cargo-ran").exists()
            finally:
                child_file = root / "child.json"
                if child_file.exists():
                    try:
                        os.kill(json.loads(child_file.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_stalled_preparation_fails_before_tests_and_cleans_children(self):
        for stage in ("signer", "fetch", "conformance"):
            with self.subTest(stage=stage):
                result, cargo_ran = self.exercise(stage)
                self.assertEqual(result.returncode, 124, result.stderr)
                self.assertFalse(cargo_ran)

    def test_successful_preparation_reaches_client_tests_and_cleans_paths(self):
        result, cargo_ran = self.exercise("")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(cargo_ran)

    def test_invalid_deadline_is_rejected_before_tests(self):
        for value in ("0", "-1", "1s", "100000"):
            with self.subTest(value=value):
                result, cargo_ran = self.exercise("", value)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(cargo_ran)
