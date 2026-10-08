#!/usr/bin/env python3
"""Generate a Cargo-lock-backed SPDX 2.3 package SBOM."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "SBOM.spdx.json"
def run_metadata() -> dict:
    toolchain = tomllib.loads(
        (ROOT / "rust-toolchain.toml").read_text(encoding="utf-8")
    )["toolchain"]["channel"]
    result = subprocess.run(
        ["cargo", f"+{toolchain}", "metadata", "--locked", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def vendored_alpaca_pin() -> tuple[str, dict[str, str]]:
    document = json.loads(
        (ROOT / "SOURCE-MANIFEST.json").read_text(encoding="utf-8")
    )
    upstreams = document.get("vendored_upstreams", [])
    if len(upstreams) != 1:
        raise ValueError("expected one pinned vendored Alpaca Rust upstream")
    upstream = upstreams[0]
    return upstream["source_commit"], {
        package["name"]: package["source_path"] for package in upstream["packages"]
    }


def reviewed_dependency_licenses() -> dict[tuple[str, str], dict[str, str]]:
    document = json.loads(
        (ROOT / "SOURCE-MANIFEST.json").read_text(encoding="utf-8")
    )
    return {
        (entry["crate_name"], entry["version"]): entry
        for entry in document.get("reviewed_dependency_licenses", [])
    }


def spdx_id(index: int, name: str, version: str) -> str:
    label = re.sub(r"[^A-Za-z0-9.-]", "-", f"{name}-{version}")
    return f"SPDXRef-Package-{index}-{label}"


def cargo_purl(name: str, version: str) -> str:
    return f"pkg:cargo/{quote(name, safe='._-')}@{quote(version, safe='._-+')}"


def main() -> None:
    metadata = run_metadata()
    alpaca_revision, alpaca_package_paths = vendored_alpaca_pin()
    lock_path = ROOT / "Cargo.lock"
    lock_digest = hashlib.sha256(lock_path.read_bytes()).hexdigest()
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))

    lock_packages: dict[tuple[str, str, str | None], dict] = {}
    for package in lock["package"]:
        key = (package["name"], package["version"], package.get("source"))
        lock_packages[key] = package

    packages = metadata["packages"]
    license_exceptions = reviewed_dependency_licenses()
    ids = {package["id"]: spdx_id(i, package["name"], package["version"])
           for i, package in enumerate(packages, start=1)}
    root_ids = set(metadata["workspace_members"])
    relationships: set[tuple[str, str, str]] = set()
    for package_id in root_ids:
        relationships.add(("SPDXRef-DOCUMENT", "DESCRIBES", ids[package_id]))
    for node in (metadata.get("resolve") or {}).get("nodes", []):
        parent = ids[node["id"]]
        for dependency in node["deps"]:
            child = ids[dependency["pkg"]]
            relationships.add((parent, "DEPENDS_ON", child))

    package_items = []
    for package in packages:
        name = package["name"]
        version = package["version"]
        source = package.get("source")
        key = (name, version, source)
        locked = lock_packages.get(key, {})
        if source and source.startswith("registry+"):
            download = f"https://crates.io/crates/{quote(name, safe='._-')}/{quote(version, safe='._-+')}"
        elif source and source.startswith("git+"):
            download = "NOASSERTION"
        elif name in alpaca_package_paths:
            upstream_path = alpaca_package_paths[name]
            download = (
                "https://github.com/wmzhai/alpaca-rust/tree/"
                f"{alpaca_revision}/{upstream_path}"
            )
        elif package["id"] in root_ids:
            download = "https://github.com/lqepoch/broker-connectors"
        else:
            download = "NOASSERTION"

        item = {
            "name": name,
            "SPDXID": ids[package["id"]],
            "versionInfo": version,
            "downloadLocation": download,
            "filesAnalyzed": False,
            "licenseConcluded": "NOASSERTION",
            "licenseDeclared": package.get("license") or "NOASSERTION",
            "copyrightText": "NOASSERTION",
        }
        checksum = locked.get("checksum")
        if checksum:
            item["checksums"] = [{"algorithm": "SHA256", "checksumValue": checksum}]
        if source and source.startswith("registry+"):
            item["externalRefs"] = [{
                "referenceCategory": "PACKAGE-MANAGER",
                "referenceType": "purl",
                "referenceLocator": cargo_purl(name, version),
            }]
        elif name in alpaca_package_paths:
            item["sourceInfo"] = (
                "Vendored from wmzhai/alpaca-rust at commit "
                f"{alpaca_revision}; narrowly patched source and exclusions are "
                "documented in vendor/alpaca-rust/UPSTREAM.md and SOURCE-MANIFEST.json."
            )
        reviewed_license = license_exceptions.get((name, version))
        if reviewed_license:
            item["licenseComments"] = (
                "The exact package license text is retained at "
                f"{reviewed_license['target_license_path']} with SHA-256 "
                f"{reviewed_license['target_license_sha256']}; package checksum and "
                "dependency path are recorded in SOURCE-MANIFEST.json."
            )
            item["sourceInfo"] = (
                f"{reviewed_license['source_url']}; "
                f"dependency path: {reviewed_license['dependency_path']}. "
                "The root-certificate data does not establish broker authority, "
                "provider entitlement, or market-data source."
            )
        package_items.append(item)

    document_namespace = (
        "https://spdx.org/spdxdocs/broker-connectors-" + lock_digest
    )
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "broker-connectors Cargo workspace",
        "documentNamespace": document_namespace,
        "creationInfo": {
            "creators": ["Tool: scripts/generate_spdx_sbom.py"],
            "created": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
        "packages": package_items,
        "relationships": [
            {"spdxElementId": left, "relationshipType": relation, "relatedSpdxElement": right}
            for left, relation, right in sorted(relationships)
        ],
    }
    OUTPUT.write_text(json.dumps(sbom, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(ROOT)} with {len(package_items)} Cargo packages")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"SBOM generation failed: {type(error).__name__}", file=sys.stderr)
        raise SystemExit(1) from error
