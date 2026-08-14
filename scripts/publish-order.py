#!/usr/bin/env python3
"""Derive the crates.io publish order for the Shardline workspace.

The publish order is the workspace dependency graph in topological order
(dependencies first). ALL dependency kinds count -- "normal", "build" and "dev"
-- because every in-workspace path dependency becomes a crates.io requirement
when the crate is published. The v1.5.0 release failed because `sdx` (which has
a DEV dependency on `shardline-server` for its test suite) was published before
`shardline-server`; dev edges are therefore included, not skipped.

Only workspace-internal edges are considered: a dependency edge is kept when
the dependency carries a `path` that resolves inside the workspace root.
Crates that declare `publish = false` (or `publish = []`) are excluded along
with their edges. The result is a deterministic, dependency-first sequence.

Stdlib only (argparse, json, subprocess, urllib, pathlib; tomllib on 3.11+).

Examples:
  python3 scripts/publish-order.py                      # from workspace root
  python3 scripts/publish-order.py meta.json            # from a saved metadata file
  python3 scripts/publish-order.py --check order.txt    # validate an order file
  python3 scripts/publish-order.py --wait sdx 1.6.0     # poll the sparse index
"""

from __future__ import annotations

import argparse
import heapq
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

try:  # Python 3.11+ only
    import tomllib
except ImportError:  # pragma: no cover - exercised only on <3.11
    tomllib = None


# ---------------------------------------------------------------------------
# Metadata loading
# ---------------------------------------------------------------------------

def load_metadata(path: str | None) -> dict:
    """Return a parsed cargo-metadata dict (runs `cargo metadata` if no path)."""
    if path:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


# ---------------------------------------------------------------------------
# publish = false detection
# ---------------------------------------------------------------------------

_PUBLISH_FALSE_RE = re.compile(r"^\s*publish\s*=\s*(?:false|\[\])\s*$", re.MULTILINE)


def _manifest_declares_publish_false(manifest_path: str) -> bool:
    """Parse `publish = false` / `publish = []` out of a Cargo.toml [package] section.

    Used only as a fallback when the metadata dict lacks the authoritative
    `publish` field. A missing/unreadable manifest is treated as publishable.
    """
    try:
        text = Path(manifest_path).read_text(encoding="utf-8")
    except OSError:
        return False
    if tomllib is not None:
        try:
            data = tomllib.loads(text)
            package = data.get("package", {})
            if "publish" in package:
                pub = package["publish"]
                return pub is False or pub == []
        except (tomllib.TOMLDecodeError, ValueError, TypeError):
            pass
    section = re.search(r"(?ms)^\[package\](.*?)(?=^\[)", text)
    if section is None:
        return False
    return _PUBLISH_FALSE_RE.search(section.group(1)) is not None


def is_publishable(pkg: dict) -> bool:
    """A package is publishable unless it declares `publish = false` / `publish = []`.

    Prefers the authoritative `publish` field already present in real cargo
    metadata (null = publishable, [] = not publishable, ["reg"] = a specific
    registry). Falls back to parsing the manifest for the key if absent.
    """
    if "publish" in pkg:
        pub = pkg["publish"]
        if isinstance(pub, list):
            return len(pub) > 0
        return True
    return not _manifest_declares_publish_false(pkg.get("manifest_path", ""))


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def _is_inside(path: str, root: str | None) -> bool:
    if not root:
        return True
    try:
        return Path(path).resolve().is_relative_to(Path(root).resolve())
    except ValueError:
        return False


def build_graph(metadata: dict) -> tuple[list[str], dict[str, set[str]], dict[str, str]]:
    """Return (nodes, edges, versions) for the publishable workspace subgraph.

    nodes: sorted names of publishable workspace packages.
    edges: {dependent: set of workspace-internal dependency names} for all
           dependency kinds (normal/build/dev).
    versions: {name: version} for every publishable package.
    """
    root = metadata.get("workspace_root") or None
    packages = metadata.get("packages", [])

    nodes: set[str] = set()
    versions: dict[str, str] = {}
    raw_edges: dict[str, set[str]] = {}
    for pkg in packages:
        name = pkg["name"]
        if not _is_inside(pkg.get("manifest_path", ""), root):
            continue  # not a workspace member
        if not is_publishable(pkg):
            continue
        nodes.add(name)
        versions[name] = pkg.get("version", "")
        deps: set[str] = set()
        for dep in pkg.get("dependencies", []):
            dep_path = dep.get("path")
            if not dep_path:
                continue
            if not _is_inside(dep_path, root):
                continue
            deps.add(dep["name"])
        raw_edges[name] = deps

    # Restrict edges to publishable nodes (deps on excluded crates drop out).
    edges: dict[str, set[str]] = {
        n: {d for d in ds if d in nodes} for n, ds in raw_edges.items()
    }
    return sorted(nodes), edges, versions


# ---------------------------------------------------------------------------
# Topological sort (Kahn's algorithm, deterministic)
# ---------------------------------------------------------------------------

def topological_order(nodes: list[str], edges: dict[str, set[str]]) -> list[str]:
    """Dependency-first order; zero-indegree candidates pop in alphabetical order.

    Raises ValueError with a cycle path (e.g. "a -> b -> a") on a cycle.
    """
    dependents: dict[str, set[str]] = {n: set() for n in nodes}
    indegree: dict[str, int] = {n: 0 for n in nodes}
    for dependent, deps in edges.items():
        for dep in deps:
            dependents[dep].add(dependent)
            indegree[dependent] += 1

    ready = [n for n in nodes if indegree[n] == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        name = heapq.heappop(ready)
        order.append(name)
        for dependent in sorted(dependents[name]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                heapq.heappush(ready, dependent)

    if len(order) != len(nodes):
        remaining = set(nodes) - set(order)
        # Walk prerequisite edges among the remaining nodes to exhibit a cycle.
        path: list[str] = []
        seen: dict[str, int] = {}
        node = sorted(remaining)[0]
        while node not in seen:
            seen[node] = len(path)
            path.append(node)
            prereqs = sorted(edges[node] & remaining)
            if not prereqs:
                break  # unreachable: Kahn guarantees a cycle here
            node = prereqs[0]
        start = seen.get(node, 0)
        cycle = path[start:] + ([node] if node in seen else [])
        raise ValueError("dependency cycle: " + " -> ".join(cycle))
    return order


def compute_publish_order(metadata: dict) -> list[str]:
    """Convenience wrapper: graph + topological sort for the given metadata."""
    nodes, edges, _versions = build_graph(metadata)
    return topological_order(nodes, edges)


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------

def publish_entries(
    order: list[str],
    versions: dict[str, str],
    version_override: str | None = None,
) -> list[tuple[str, str]]:
    """Return [(crate, version), ...] for the emitted sequence."""
    return [(name, version_override or versions[name]) for name in order]


def emit_order(order: list[str], versions: dict[str, str], version_override: str | None = None) -> None:
    for crate, version in publish_entries(order, versions, version_override):
        print(f"publish {crate}@{version}")
        print(f"wait-for-index {crate}@{version}")


# ---------------------------------------------------------------------------
# --check: validate an order file against the graph
# ---------------------------------------------------------------------------

def parse_order_lines(lines: list[str]) -> list[str]:
    """Parse crate names out of generated (`publish x@v` / `wait-for-index x@v`)
    or plain (`x`) lines, skipping blanks and comments. A `wait-for-index` line
    is the continuation of the preceding `publish` step for the same crate, so
    it is not counted as a separate order entry (piping the generator's full
    output back into --check must validate cleanly)."""
    names: list[str] = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("wait-for-index "):
            continue  # same step as the preceding publish line
        if line.startswith("publish "):
            line = line[len("publish "):]
        if "@" in line:
            line = line.split("@", 1)[0]
        names.append(line)
    return names


def validate_order(nodes: list[str], edges: dict[str, set[str]], order_file: str) -> tuple[bool, list[str]]:
    """Return (ok, violations). Every publishable crate must appear exactly once,
    and every crate must precede ALL its workspace-internal dependents."""
    if order_file == "-":
        lines = sys.stdin.read().splitlines()
    else:
        with open(order_file, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()

    given = parse_order_lines(lines)
    node_set = set(nodes)
    violations: list[str] = []

    seen: set[str] = set()
    for name in given:
        if name in seen:
            violations.append(f"{name} appears more than once in the order")
        seen.add(name)
        if name not in node_set:
            violations.append(f"{name} is not a publishable workspace crate")

    for name in nodes:
        if name not in seen:
            violations.append(f"{name} is missing from the order")

    positions = {name: i for i, name in enumerate(given)}
    for dependent, deps in edges.items():
        for dep in deps:
            if positions.get(dep, -1) > positions.get(dependent, -1):
                violations.append(f"{dep} must precede {dependent}")

    return len(violations) == 0, violations


# ---------------------------------------------------------------------------
# --wait: poll the crates.io sparse index until CRATE@VERSION is visible
# ---------------------------------------------------------------------------

def index_url(crate: str) -> str:
    """Sparse-index URL: 1-char prefix for 1-char names, 2 for 2-char, 3 for 3+."""
    name = crate.lower()
    if len(name) == 1:
        prefix = "1"
    elif len(name) == 2:
        prefix = "2"
    else:
        prefix = name[:3]
    return f"https://index.crates.io/{prefix}/{name}"


def wait_for_index(crate: str, version: str, timeout: int = 300, sleep: float = 5.0) -> int:
    """Poll until CRATE@VERSION appears in the sparse index; 0 on success, 1 on timeout."""
    url = index_url(crate)
    needle_name = crate.lower()
    deadline = time.monotonic() + timeout
    while True:
        found = False
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                body = resp.read().decode("utf-8")
            for line in body.splitlines():
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    entry.get("name", "").lower() == needle_name
                    and entry.get("vers") == version
                ):
                    found = True
                    break
            if found:
                print(f"ok: {crate}@{version} is in the crates.io index ({url})")
                return 0
            print(f"retry: {crate}@{version} not in index yet ({url})", file=sys.stderr)
        except (urllib.error.URLError, OSError) as exc:
            print(f"retry: index fetch failed ({exc}); sleeping {sleep:g}s", file=sys.stderr)
        if time.monotonic() >= deadline:
            print(
                f"timeout: {crate}@{version} not in index after {timeout}s ({url})",
                file=sys.stderr,
            )
            return 1
        time.sleep(sleep)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Derive the crates.io publish order for the Shardline workspace from "
            "the cargo metadata dependency graph (normal + build + dev edges)."
        ),
    )
    parser.add_argument(
        "metadata_json",
        nargs="?",
        default=None,
        help="path to a cargo-metadata JSON file (default: run `cargo metadata --no-deps`)",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="override the version printed in the emitted sequence (output text only)",
    )
    parser.add_argument(
        "--check",
        metavar="ORDER_FILE",
        default=None,
        help="validate an order file against the graph ('-' reads stdin)",
    )
    parser.add_argument(
        "--wait",
        nargs=2,
        metavar=("CRATE", "VERSION"),
        help="poll the crates.io sparse index until CRATE@VERSION is visible",
    )
    parser.add_argument("--timeout", type=int, default=300, help="max seconds to poll in --wait mode")
    parser.add_argument("--sleep", type=float, default=5.0, help="seconds between polls in --wait mode")
    parser.add_argument(
        "--emit",
        action="store_true",
        help="print only the ordered crate names, one per line (plain CRATE_PACKAGES form)",
    )
    args = parser.parse_args(argv)

    if args.wait:
        return wait_for_index(args.wait[0], args.wait[1], timeout=args.timeout, sleep=args.sleep)

    try:
        metadata = load_metadata(args.metadata_json)
        nodes, edges, versions = build_graph(metadata)
        order = topological_order(nodes, edges)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.check:
        ok, violations = validate_order(nodes, edges, args.check)
        if ok:
            print("OK: order is a valid topological order (deps first; normal/build/dev edges included)")
            return 0
        print("NOT OK: the given order violates the dependency graph:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1

    if args.emit:
        for crate in order:
            print(crate)
        return 0

    emit_order(order, versions, version_override=args.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
