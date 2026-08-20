"""Unit tests for scripts/publish-order.py (stdlib unittest).

Run with: python3 -m unittest discover -s scripts/tests -v
"""

import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
FIXTURE = SCRIPTS_DIR / "fixtures" / "metadata-sample.json"

# scripts/publish-order.py is a hyphenated filename, so load it explicitly.
def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publish_order = _load_module("publish_order", SCRIPTS_DIR / "publish-order.py")

# Deterministic alphabetical tie-break across zero-indegree candidates, with
# the v1.5 dev edge honored: sdx after shardline-server AND shardline-xet-adapter.
EXPECTED_ORDER = [
    "shardline-metrics",
    "shardline-server-core",
    "shardline-server",
    "shardline-xet-adapter",
    "sdx",
    "shardline",
]


class TestPublishOrder(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.metadata = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_expected_order(self):
        order = publish_order.compute_publish_order(self.metadata)
        self.assertEqual(order, EXPECTED_ORDER)

    def test_deterministic_across_runs(self):
        self.assertEqual(
            publish_order.compute_publish_order(self.metadata),
            publish_order.compute_publish_order(self.metadata),
        )

    def test_publish_false_crate_excluded(self):
        order = publish_order.compute_publish_order(self.metadata)
        self.assertNotIn("shardline-fuzz", order)

    def test_every_crate_precedes_its_dependents(self):
        order = publish_order.compute_publish_order(self.metadata)
        pos = {name: i for i, name in enumerate(order)}
        # normal-dep chain: shardline-metrics -> shardline-xet-adapter -> sdx -> shardline
        self.assertLess(pos["shardline-metrics"], pos["shardline-xet-adapter"])
        self.assertLess(pos["shardline-xet-adapter"], pos["sdx"])
        self.assertLess(pos["sdx"], pos["shardline"])
        # v1.5 lesson: sdx has a DEV dependency on shardline-server
        self.assertLess(pos["shardline-server"], pos["sdx"])
        # normal-dep chain: shardline-server-core -> shardline-server
        self.assertLess(pos["shardline-server-core"], pos["shardline-server"])

    def test_cli_emits_sequence_and_check_passes(self):
        out = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "publish-order.py"), str(FIXTURE)],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = out.stdout.splitlines()
        self.assertEqual(len(lines), 2 * len(EXPECTED_ORDER))
        self.assertTrue(lines[0].startswith("publish shardline-metrics@1.5.0"))
        self.assertTrue(lines[1].startswith("wait-for-index shardline-metrics@1.5.0"))
        self.assertFalse(any("shardline-fuzz" in line for line in lines))

        chk = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "publish-order.py"), str(FIXTURE), "--check", "-"],
            input=out.stdout,
            capture_output=True,
            text=True,
        )
        self.assertEqual(chk.returncode, 0, chk.stderr)
        self.assertIn("OK", chk.stdout)

    def test_check_rejects_violated_order(self):
        # Swap shardline-server and sdx: both the dev edge and the normal edge
        # from shardline-xet-adapter to sdx are now violated.
        bad = list(EXPECTED_ORDER)
        i, j = bad.index("shardline-server"), bad.index("sdx")
        bad[i], bad[j] = bad[j], bad[i]
        chk = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "publish-order.py"), str(FIXTURE), "--check", "-"],
            input="\n".join(bad),
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(chk.returncode, 0)
        self.assertIn("shardline-server must precede sdx", chk.stderr)
        self.assertIn("shardline-xet-adapter must precede sdx", chk.stderr)

    def test_version_override_appears_in_output(self):
        out = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "publish-order.py"),
                str(FIXTURE),
                "--version",
                "1.6.0",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn("publish sdx@1.6.0", out.stdout)
        self.assertIn("wait-for-index sdx@1.6.0", out.stdout)


if __name__ == "__main__":
    unittest.main()
