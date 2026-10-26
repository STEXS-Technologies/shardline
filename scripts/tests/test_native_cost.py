"""Guard credential-safe failure reporting and genuinely cold native clients."""

from pathlib import Path
import ast
import json
import os
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shardline"))
from performance_validation.native import CLIENT, ShardlineRedirect, client_operation  # noqa: E402
import urllib.request


class NativeCostTest(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux fork memory accounting")
    def test_client_peak_excludes_inherited_generator_high_water_mark(self):
        function = next(node for node in ast.parse(CLIENT).body
                        if isinstance(node, ast.FunctionDef) and node.name == "peak_rss_kib")
        measured = ast.unparse(function)
        # Allocate in an intermediate parent, then execute the actual client
        # measurement in a tiny child. This reproduces the inflated getrusage
        # observation without importing optional native clients.
        child = ("import json,resource,sys\nfrom pathlib import Path\n" + measured
                 + "\nprint(json.dumps({'peak':peak_rss_kib(),"
                 + "'inherited':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}))")
        parent = ("import subprocess,sys\nallocation=bytearray(128*1024*1024)\n"
                  + "subprocess.run([sys.executable,'-c'," + repr(child)
                  + "],input='{}',text=True,check=True)\n")
        result = subprocess.run([sys.executable, "-c", parent], capture_output=True,
                                text=True, check=True, timeout=30)
        observation = json.loads(result.stdout)
        self.assertGreater(observation["peak"], 0)
        self.assertLess(observation["peak"], 64 * 1024)

    def test_backend_redirect_is_rejected_before_forwarding_credentials(self):
        handler = ShardlineRedirect(("http", "127.0.0.1:1234"))
        request = urllib.request.Request("http://127.0.0.1:1234/resolve/file",
                                         headers={"Authorization": "Bearer private-token"})
        for target in ("http://127.0.0.1:9000/backend", "https://127.0.0.1:1234/object",
                       "http://private-token@127.0.0.1:1234/object"):
            with self.assertRaisesRegex(RuntimeError, "outside Shardline"):
                handler.redirect_request(request, None, 307, "redirect", {}, target)
        redirected = handler.redirect_request(request, None, 307, "redirect", {},
                                               "http://127.0.0.1:1234/lfs/objects/hash")
        self.assertEqual(redirected.get_header("Authorization"), "Bearer private-token")

    def operation(self):
        return client_operation(Path("/fixture/python"),
                                SimpleNamespace(ports=[1234], token="private-token"),
                                "upload", Path("/fixture/input"), "model.bin", "digest")

    def test_native_failure_never_reprints_signed_urls_or_credentials(self):
        failure = subprocess.CompletedProcess([], 1, "private-token", "https://private/signed-url")
        with patch("performance_validation.native.subprocess.run", return_value=failure):
            with self.assertRaisesRegex(RuntimeError, "raw diagnostics withheld") as error:
                self.operation()
        self.assertNotIn("private-token", str(error.exception))
        self.assertNotIn("signed-url", str(error.exception))

    def test_timeout_does_not_expose_captured_client_diagnostics(self):
        failure = subprocess.TimeoutExpired("native", 300, output="private-token")
        with patch("performance_validation.native.subprocess.run", side_effect=failure):
            with self.assertRaisesRegex(RuntimeError, "deadline") as error:
                self.operation()
        self.assertNotIn("private-token", str(error.exception))

    def test_each_operation_uses_an_independent_empty_client_cache(self):
        homes = []

        def execute(command, **kwargs):
            env = kwargs["env"]
            home = Path(env["HF_HOME"])
            self.assertTrue(home.is_dir())
            self.assertEqual(list(home.iterdir()), [])
            self.assertNotIn("HF_TOKEN", env)
            self.assertNotIn("HF_HUB_CACHE", env)
            self.assertNotIn("HF_ENDPOINT", env)
            self.assertEqual(env["HF_HUB_DISABLE_XET"], "0")
            self.assertNotIn("private-token", command)
            self.assertEqual(json.loads(kwargs["input"])["token"], "private-token")
            homes.append(home)
            return subprocess.CompletedProcess(command, 0, '{"seconds":1}', "")

        with patch.dict(os.environ, HF_TOKEN="ambient", HF_HUB_CACHE="ambient", HF_ENDPOINT="ambient"):
            with patch("performance_validation.native.subprocess.run", side_effect=execute):
                self.operation()
                self.operation()
        self.assertNotEqual(homes[0], homes[1])
        self.assertTrue(all(not home.exists() for home in homes))


if __name__ == "__main__":
    unittest.main()
