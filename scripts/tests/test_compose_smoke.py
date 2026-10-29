"""Execute the documented smoke entry point against owned HTTP peers."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "shardline" / "compose_smoke.py"
TOKEN = "private-compose-smoke-fixture-token"


@contextmanager
def peer(mode):
    stop = threading.Event()
    writes = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            try:
                if mode == "silent":
                    stop.wait(8)
                    return
                if mode in ("headers", "body"):
                    self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                    if mode == "body":
                        self.wfile.write(b"Content-Length: 1000000\r\n\r\n")
                    while not stop.wait(0.04):
                        self.wfile.write(b"X-Progress: yes\r\n" if mode == "headers" else b"x")
                        self.wfile.flush()
                        writes.append(1)
                    return
                if self.path == "/v1/stats" and not self.headers.get("Authorization"):
                    self.send_response(401)
                    self.end_headers()
                    return
                if self.path not in ("/healthz", "/readyz") and self.headers.get(
                        "Authorization") != f"Bearer {TOKEN}":
                    self.send_response(403)
                    self.end_headers()
                    return
                if self.path.startswith("/lfs/objects/"):
                    data = self.server.payload
                else:
                    data = b"{}"
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_PUT(self):
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                self.send_response(403)
                self.end_headers()
                return
            self.server.payload = self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", writes
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@unittest.skipUnless(os.name == "posix", "owned executable fixtures require POSIX")
class ComposeSmokeTest(unittest.TestCase):
    def invoke(self, endpoint, timeout=1, docker_source=None):
        with tempfile.TemporaryDirectory() as directory:
            docker = Path(directory) / "docker"
            docker.write_text(f"#!{sys.executable}\n" + (
                docker_source if docker_source is not None else f"print({TOKEN!r})\n"))
            docker.chmod(0o700)
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ.get("PATH", ""))
            return subprocess.run(
                [sys.executable, str(SCRIPT), "--endpoint", endpoint,
                 "--timeout", str(timeout)],
                env=env, capture_output=True, text=True, timeout=8,
            )

    def test_silent_and_slow_drip_responses_hit_whole_campaign_deadline(self):
        for mode in ("silent", "headers", "body"):
            with self.subTest(mode=mode), peer(mode) as (endpoint, writes):
                started = time.monotonic()
                result = self.invoke(endpoint)
                self.assertLess(time.monotonic() - started, 4)
                self.assertEqual(result.returncode, 1)
                self.assertIn("TimeoutError", result.stderr)
                self.assertNotIn(endpoint, result.stderr)
                self.assertNotIn(TOKEN, result.stdout + result.stderr)
                if mode != "silent":
                    self.assertGreater(len(writes), 5, "peer must make real socket progress")

    def test_success_executes_authenticated_upload_and_exact_download(self):
        with peer("success") as (endpoint, _writes):
            result = self.invoke(endpoint, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("authenticated upload/download, exact bytes", result.stdout)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)

    def test_invalid_deadline_is_rejected(self):
        for value in ("0", "-1", "100000", "not-a-number"):
            result = self.invoke("http://127.0.0.1:1", timeout=value)
            self.assertEqual(result.returncode, 2)

    def test_campaign_deadline_stops_token_mint_descendants(self):
        with tempfile.TemporaryDirectory() as directory, peer("success") as (endpoint, _writes):
            marker = Path(directory) / "escaped"
            started = Path(directory) / "started"
            child = f"import pathlib,time;time.sleep(2);pathlib.Path({str(marker)!r}).touch()"
            docker_source = (
                "import pathlib,subprocess,sys,time\n"
                f"subprocess.Popen([sys.executable, '-c', {child!r}])\n"
                f"pathlib.Path({str(started)!r}).touch()\n"
                "time.sleep(60)\n"
            )
            result = self.invoke(endpoint, docker_source=docker_source)
            self.assertEqual(result.returncode, 1)
            self.assertIn("TimeoutError", result.stderr)
            self.assertTrue(started.exists(), "token mint must actually spawn its child")
            time.sleep(2.2)
            self.assertFalse(marker.exists(), "token mint left a live descendant")


if __name__ == "__main__":
    unittest.main()
