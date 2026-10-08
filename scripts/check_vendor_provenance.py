#!/usr/bin/env python3
"""Check vendored upstream pin, license, and adapted-file hashes offline."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "SOURCE-MANIFEST.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_path(relative: str) -> Path:
    candidate = ROOT / relative
    if candidate.is_symlink():
        raise ValueError(f"manifest path must not be a symlink: {relative}")
    path = candidate.resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError(f"manifest path escapes repository: {relative}")
    return path


def main() -> None:
    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for entry in document.get("source_files", []):
        target_name = entry["target_path"]
        target = checked_path(target_name)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"project source is missing or not a regular file: {target_name}")
        if sha256(target) != entry.get("adapted_target_sha256"):
            raise ValueError(f"adapted project source hash mismatch: {target_name}")
        if not re.fullmatch(r"[0-9a-f]{64}", entry["source_blob_sha256"]):
            raise ValueError(f"invalid source repository hash: {entry['source_path']}")

    for entry in document.get("new_project_code", []):
        target_name = entry["target_path"]
        target = checked_path(target_name)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"project file is missing or not a regular file: {target_name}")
        if sha256(target) != entry.get("sha256"):
            raise ValueError(f"project file hash mismatch: {target_name}")

    upstreams = document.get("vendored_upstreams", [])
    if len(upstreams) != 1:
        raise ValueError("expected one pinned vendored upstream")

    upstream = upstreams[0]
    if not re.fullmatch(r"[0-9a-f]{40}", upstream["source_commit"]):
        raise ValueError("upstream commit must be a full Git SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", upstream["source_git_tree"]):
        raise ValueError("upstream tree must be a full Git tree SHA")
    release = upstream.get("upstream_release")
    if release:
        package_versions = {package["version"] for package in upstream["packages"]}
        if (
            len(package_versions) != 1
            or release.get("tag") != f"v{next(iter(package_versions))}"
            or release.get("commit") != upstream["source_commit"]
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", release.get("verified_on", ""))
        ):
            raise ValueError("upstream release evidence does not match the pinned package")

    seen_targets: set[str] = set()
    for entry in upstream["source_files"]:
        target_name = entry["target_path"]
        if target_name in seen_targets:
            raise ValueError(f"duplicate vendored target path: {target_name}")
        seen_targets.add(target_name)
        target = checked_path(target_name)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"vendored source is missing or not a regular file: {target_name}")
        actual = sha256(target)
        if actual != entry.get("adapted_target_sha256"):
            raise ValueError(f"adapted source hash mismatch: {target_name}")
        if not re.fullmatch(r"[0-9a-f]{64}", entry["source_file_sha256"]):
            raise ValueError(f"invalid upstream source hash: {entry['source_path']}")

    for entry in upstream["removed_source_files"]:
        if not re.fullmatch(r"[0-9a-f]{64}", entry["source_file_sha256"]):
            raise ValueError(f"invalid removed upstream source hash: {entry['source_path']}")
        omitted_path = checked_path(
            f"vendor/alpaca-rust/{entry['source_path']}"
        )
        if omitted_path.exists():
            raise ValueError(f"excluded live API test is still vendored: {entry['source_path']}")

    patch_path = checked_path(upstream["local_patch_path"])
    if sha256(patch_path) != upstream.get("local_patch_sha256"):
        raise ValueError("local upstream patch hash mismatch")

    for license_file in upstream["license_files"]:
        target_name = license_file["target_path"]
        target = checked_path(target_name)
        if sha256(target) != license_file["sha256"]:
            raise ValueError(f"license hash mismatch: {target_name}")

    package_manifest = tomllib.loads(
        checked_path("vendor/alpaca-rust/Cargo.toml").read_text(encoding="utf-8")
    )
    expected_packages = {"alpaca-core", "alpaca-data", "alpaca-rest-http"}
    if {package["name"] for package in upstream["packages"]} != expected_packages:
        raise ValueError("vendored workspace package scope changed")
    if set(package_manifest["workspace"]["members"]) != {
        "crates/alpaca-core",
        "crates/alpaca-data",
        "crates/alpaca-http",
    }:
        raise ValueError("vendored workspace must remain read-only data scoped")
    for package in upstream["packages"]:
        manifest_path = checked_path(package["target_manifest"])
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["package"]["name"] != package["name"]:
            raise ValueError(f"package name mismatch: {package['name']}")
        if package["license"] != upstream["license"]:
            raise ValueError(f"package license mismatch: {package['name']}")
        if manifest["package"]["version"] != {"workspace": True}:
            raise ValueError(f"vendored package version must follow scoped workspace: {package['name']}")
        if package_manifest["workspace"]["package"]["version"] != package["version"]:
            raise ValueError(f"vendored package version mismatch: {package['name']}")
        if package_manifest["workspace"]["package"]["license"] != "MIT OR Apache-2.0":
            raise ValueError("vendored license expression changed")

    lock = tomllib.loads(checked_path("Cargo.lock").read_text(encoding="utf-8"))
    lock_packages = {
        (package["name"], package["version"]): package
        for package in lock["package"]
    }
    deny = tomllib.loads(checked_path("deny.toml").read_text(encoding="utf-8"))
    license_policy = deny["licenses"]
    sbom = json.loads(checked_path("SBOM.spdx.json").read_text(encoding="utf-8"))
    sbom_packages = {
        (package["name"], package["versionInfo"]): package
        for package in sbom["packages"]
    }
    notice = checked_path("NOTICE").read_text(encoding="utf-8")
    for entry in document.get("reviewed_dependency_licenses", []):
        target_name = entry["target_license_path"]
        target = checked_path(target_name)
        if not target.is_file() or sha256(target) != entry["target_license_sha256"]:
            raise ValueError(f"reviewed dependency license hash mismatch: {target_name}")
        if entry["source_license_sha256"] != entry["target_license_sha256"]:
            raise ValueError(f"copied license text differs from package source: {target_name}")

        package_key = (entry["crate_name"], entry["version"])
        locked_package = lock_packages.get(package_key)
        if (
            locked_package is None
            or locked_package.get("checksum") != entry["registry_checksum"]
            or not locked_package.get("source", "").startswith("registry+")
        ):
            raise ValueError(f"reviewed dependency is not locked to its recorded crate: {package_key}")

        exact_spec = f"{entry['crate_name']}@{entry['version']}"
        if entry["license"] in license_policy.get("allow", []):
            raise ValueError(f"reviewed license must not be globally allowed: {entry['license']}")
        if not any(
            exception.get("crate") == exact_spec
            and exception.get("allow") == [entry["license"]]
            for exception in license_policy.get("exceptions", [])
        ):
            raise ValueError(f"exact cargo-deny license exception missing: {exact_spec}")

        sbom_package = sbom_packages.get(package_key)
        if (
            sbom_package is None
            or sbom_package.get("licenseDeclared") != entry["license"]
            or entry["target_license_sha256"] not in sbom_package.get("licenseComments", "")
            or entry["dependency_path"] not in sbom_package.get("sourceInfo", "")
        ):
            raise ValueError(f"reviewed license metadata missing from SBOM: {package_key}")
        if target_name not in notice or entry["target_license_sha256"] not in notice:
            raise ValueError(f"reviewed license missing from NOTICE: {target_name}")

    read_source = "\n".join(
        checked_path(f"crates/alpaca-rest-read/src/{name}").read_text(encoding="utf-8")
        for name in ("client.rs", "error.rs", "lib.rs", "model.rs", "request.rs")
    )
    if re.search(r"std::env|\.from_env\s*\(|dotenv", read_source):
        raise ValueError("alpaca-rest-read must use explicit credential injection")
    if re.search(r"\.\w*_all\s*\(", read_source):
        raise ValueError("alpaca-rest-read must keep pagination finite")

    print(
        "vendor provenance passed: "
        f"{upstream['source_repository']}@{upstream['source_commit']} "
        f"({len(upstream['source_files'])} source files, {len(upstream['packages'])} packages)"
    )


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"vendor provenance check failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
