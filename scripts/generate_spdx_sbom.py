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
from typing import Any
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


def vendored_package_sources() -> dict[str, dict[str, Any]]:
    document = json.loads(
        (ROOT / "SOURCE-MANIFEST.json").read_text(encoding="utf-8")
    )
    upstreams = document.get("vendored_upstreams", [])
    if not upstreams:
        raise ValueError("no pinned vendored upstreams are recorded")
    sources = {}
    for upstream in upstreams:
        formatting_config = next(
            (
                entry
                for entry in upstream["source_files"]
                if entry["source_path"] == "rustfmt.toml"
            ),
            None,
        )
        for package in upstream["packages"]:
            name = package["name"]
            if name in sources:
                raise ValueError(f"vendored package has multiple source pins: {name}")
            sources[name] = {
                "repository": upstream["source_repository"],
                "commit": upstream["source_commit"],
                "source_path": package["source_path"],
                "target_root": upstream.get("target_root", "vendor/alpaca-rust"),
                "source_archive_sha256": upstream.get("source_archive_sha256"),
                "formatting_config": formatting_config,
                "local_change_record_path": upstream.get("local_change_record_path"),
                "local_change_record_sha256": upstream.get("local_change_record_sha256"),
            }
    return sources


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
    vendored_sources = vendored_package_sources()
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
        elif name in vendored_sources:
            origin = vendored_sources[name]
            source_path = origin["source_path"]
            path = "" if source_path == "." else source_path.lstrip("./")
            download = (
                f"https://github.com/{origin['repository']}/tree/{origin['commit']}"
                f"/{path}" if path else
                f"https://github.com/{origin['repository']}/tree/{origin['commit']}"
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
        elif name in vendored_sources:
            origin = vendored_sources[name]
            source_info = (
                f"Vendored from {origin['repository']}@{origin['commit']}; "
                f"package source path: {'repository root' if source_path == '.' else source_path}. "
                "Local adaptations and "
                f"source-file hashes are documented in {origin['target_root']}/UPSTREAM.md "
                "and SOURCE-MANIFEST.json."
            )
            archive_sha256 = origin.get("source_archive_sha256")
            if archive_sha256:
                source_info += f" Pinned source archive SHA-256: {archive_sha256}."
            change_record_path = origin.get("local_change_record_path")
            change_record_hash = origin.get("local_change_record_sha256")
            if change_record_path and change_record_hash:
                source_info += (
                    f" Local source-change record: {change_record_path}; "
                    f"SHA-256 {change_record_hash}."
                )
            formatting_config = origin.get("formatting_config")
            if formatting_config:
                source_info += (
                    " Vendored formatter configuration is copied from upstream path "
                    f"{formatting_config['source_path']} to {formatting_config['target_path']} "
                    f"with SHA-256 {formatting_config['adapted_target_sha256']} and Git blob "
                    f"{formatting_config['source_git_blob_sha1']}; it scopes formatting to "
                    "the vendored source."
                )
            item["sourceInfo"] = source_info
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
