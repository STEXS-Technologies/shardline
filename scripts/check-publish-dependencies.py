#!/usr/bin/env python3
"""Reject workspace-only dependency overrides before crates.io publication."""

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
    manifest = Path(__file__).resolve().parent.parent / "Cargo.toml"
    if len(sys.argv) > 1:
        manifest = Path(sys.argv[1])
    overrides = check_manifest(manifest)
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
    print("PASS: no workspace-only dependency overrides")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
