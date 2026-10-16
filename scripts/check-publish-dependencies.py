#!/usr/bin/env python3
"""Reject unreviewed workspace overrides before crates.io publication."""

import argparse
import sys
import tomllib
from pathlib import Path


def check_manifest(path: Path) -> list[str]:
    with path.open("rb") as source:
        manifest = tomllib.load(source)
    overrides = [
        f"patch.{registry}.{package}"
        for registry, packages in manifest.get("patch", {}).items()
        for package in packages
    ]
    overrides.extend(f"replace.{package}" for package in manifest.get("replace", {}))
    return sorted(overrides)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path,
                        default=Path(__file__).resolve().parent.parent / "Cargo.toml")
    parser.add_argument("--allow-reviewed-hyper-patch", action="store_true",
                        help="acknowledge that registry consumers do not inherit the pinned Hyper fix")
    args = parser.parse_args()
    manifest = args.manifest
    overrides = check_manifest(manifest)
    if args.allow_reviewed_hyper_patch:
        with manifest.open("rb") as source:
            patch = tomllib.load(source).get("patch", {}).get("crates-io", {}).get("hyper")
        if patch == {
            "git": "https://github.com/hyperium/hyper",
            "rev": "97fff93e53f9e6a993f2d6a25d50883b7b8752a7",
        }:
            overrides.remove("patch.crates-io.hyper")
            print(
                "WARNING: reviewed Hyper override applies only to workspace-built binaries. "
                "Crates.io consumers use released Hyper without the HTTP/1 shutdown fix "
                "from https://github.com/hyperium/hyper/pull/4222. "
                "See docs/RELIABILITY_ARCHITECTURE.md#published-crates-and-hyper.",
                file=sys.stderr,
            )
    if overrides:
        print(
            "Cannot publish: workspace dependency overrides do not propagate to "
            "crates.io consumers: " + ", ".join(overrides),
            file=sys.stderr,
        )
        print(
            "Use released dependencies containing the required fixes, remove the "
            "overrides, and verify an independent consumer before publishing.",
            file=sys.stderr,
        )
        return 1
    print("PASS: no unreviewed workspace-only dependency overrides")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
