#!/usr/bin/env python3
"""Refresh adapted-target hashes without access to the private source repo."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "SOURCE-MANIFEST.json"


def main() -> None:
    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for entry in document["source_files"]:
        target = ROOT / entry["target_path"]
        entry["adapted_target_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    for entry in document["new_project_code"]:
        target = ROOT / entry["target_path"]
        entry["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    for upstream in document.get("vendored_upstreams", []):
        for entry in upstream.get("source_files", []):
            target = ROOT / entry["target_path"]
            entry["adapted_target_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
        patch = ROOT / upstream["local_patch_path"]
        upstream["local_patch_sha256"] = hashlib.sha256(patch.read_bytes()).hexdigest()
    for entry in document.get("reviewed_dependency_licenses", []):
        target = ROOT / entry["target_license_path"]
        entry["target_license_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    MANIFEST.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print("updated adapted target hashes in SOURCE-MANIFEST.json")


if __name__ == "__main__":
    main()
