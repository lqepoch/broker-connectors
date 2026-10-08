#!/usr/bin/env python3
"""Assert broker-execution keeps its public dependency graph domain-only."""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path


ALLOWED_PACKAGES = {"broker-execution", "domain", "exact-decimal"}
ROOT = Path(__file__).resolve().parents[1]


def toolchain() -> str:
    config = tomllib.loads((ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"))
    return config["toolchain"]["channel"]


def main() -> None:
    result = subprocess.run(
        [
            "cargo",
            f"+{toolchain()}",
            "tree",
            "--locked",
            "--offline",
            "-p",
            "broker-execution",
            "--all-features",
            "--edges",
            "normal",
            "--prefix",
            "none",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    found = set(re.findall(r"^([A-Za-z0-9_-]+) v", result.stdout, re.MULTILINE))
    if not found:
        raise ValueError("cargo tree returned no packages")
    unexpected = found - ALLOWED_PACKAGES
    missing = ALLOWED_PACKAGES - found
    if unexpected or missing:
        raise ValueError(
            f"dependency firewall mismatch: unexpected={sorted(unexpected)}, "
            f"missing={sorted(missing)}"
        )
    print("broker-execution dependency firewall passed: " + ", ".join(sorted(found)))


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"broker-execution dependency firewall failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
