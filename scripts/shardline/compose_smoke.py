"""Check a running development Compose deployment without exposing its token."""

import argparse
import hashlib
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:18080")
    parser.add_argument("--project", help="Compose project name, if overridden")
    parser.add_argument("--compose-file", action="append", default=[])
    args = parser.parse_args()
    files = args.compose_file or [
        str(Path(__file__).resolve().parents[2] / "docker-compose.yml")
    ]
    compose = ["docker", "compose"]
    if args.project:
        compose += ["--project-name", args.project]
    for file in files:
        compose += ["--file", file]
    opener = urllib.request.build_opener(NoRedirect())

    def request(path, token=None, body=None, limit=65536):
        headers = {"Authorization": "Bearer " + token} if token else {}
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        req = urllib.request.Request(
            args.endpoint.rstrip("/") + path,
            data=body,
            headers=headers,
            method="PUT" if body is not None else "GET",
        )
        with opener.open(req, timeout=30) as response:
            if response.status != 200 or response.headers.get("Location"):
                raise ValueError("unexpected response or redirect")
            data = response.read(limit + 1)
            if len(data) > limit:
                raise ValueError("response exceeds smoke-test bound")
            return data

    request("/healthz")
    request("/readyz")
    try:
        request("/v1/stats")
    except urllib.error.HTTPError as error:
        if error.code != 401:
            raise ValueError("missing credentials did not return 401") from None
    else:
        raise ValueError("missing credentials were accepted")

    result = subprocess.run(
        compose + [
            "exec", "-T", "shardline", "shardline", "admin", "token",
            "--issuer", "local", "--subject", "compose-smoke", "--scope", "write",
            "--provider", "generic", "--owner", "team", "--repo", "assets",
            "--revision", "main", "--key-env", "SHARDLINE_TOKEN_SIGNING_KEY",
        ],
        capture_output=True, timeout=30, check=True,
    )
    token = result.stdout.decode().strip()
    if not token:
        raise ValueError("container token mint returned no token")
    request("/v1/stats", token)
    body = bytes(range(251)) * 4178
    path = "/lfs/objects/" + hashlib.sha256(body).hexdigest()
    request(path, token, body)
    if request(path, token, limit=len(body)) != body:
        raise ValueError("downloaded bytes differ from uploaded bytes")
    print("PASS: readiness, missing-token 401, authenticated upload/download, exact bytes")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        # URLs, tokens and captured container output can contain credentials.
        print(f"Compose smoke failed ({type(error).__name__}); inspect service health and configuration.", file=sys.stderr)
        sys.exit(1)
