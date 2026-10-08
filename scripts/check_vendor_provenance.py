#!/usr/bin/env python3
"""Check all vendored upstream pins, licenses, source lists, and adapters offline."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "SOURCE-MANIFEST.json"
SHA256_RE = re.compile(r"[0-9a-f]{64}")
GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")


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


def require_hash(value: str, description: str) -> None:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ValueError(f"invalid SHA-256 for {description}")


def require_git_sha(value: str, description: str) -> None:
    if not isinstance(value, str) or not GIT_SHA_RE.fullmatch(value):
        raise ValueError(f"invalid full Git SHA for {description}")


def package_layout(upstream: dict) -> str:
    # The initial Alpaca record predates the explicit layout field.
    return upstream.get("package_layout", "workspace")


def check_vendor(upstream: dict, project_paths: set[str]) -> None:
    repository = upstream["source_repository"]
    target_root = upstream.get("target_root", "vendor/alpaca-rust")
    layout = package_layout(upstream)
    if layout not in {"workspace", "single-package"}:
        raise ValueError(f"unsupported package layout for {repository}: {layout}")

    require_git_sha(upstream["source_commit"], f"{repository} source commit")
    require_git_sha(upstream["source_git_tree"], f"{repository} source tree")
    archive_hash = upstream.get("source_archive_sha256")
    if archive_hash is not None:
        require_hash(archive_hash, f"{repository} source archive")
        if not upstream.get("source_archive_paths"):
            raise ValueError(f"source archive paths missing for {repository}")

    release = upstream.get("upstream_release")
    if release:
        versions = {package["version"] for package in upstream["packages"]}
        if (
            len(versions) != 1
            or release.get("tag") != f"v{next(iter(versions))}"
            or release.get("commit") != upstream["source_commit"]
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", release.get("verified_on", ""))
        ):
            raise ValueError(f"release evidence does not match the pinned source for {repository}")

    tag_reference = upstream.get("upstream_tag_reference")
    if tag_reference:
        require_git_sha(tag_reference["commit"], f"{repository} tag commit")
        versions = {package["version"] for package in upstream["packages"]}
        if (
            len(versions) != 1
            or tag_reference.get("tag") != f"v{next(iter(versions))}"
            or tag_reference.get("commit") == upstream["source_commit"]
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", tag_reference.get("verified_on", ""))
            or not tag_reference.get("note")
        ):
            raise ValueError(f"tag reference must distinguish the source pin for {repository}")

    listed_targets: set[str] = set()
    for entry in upstream["source_files"]:
        target_name = entry["target_path"]
        if not target_name.startswith(f"{target_root}/"):
            raise ValueError(f"vendored source target escapes {target_root}: {target_name}")
        if target_name in listed_targets:
            raise ValueError(f"duplicate vendored target path: {target_name}")
        listed_targets.add(target_name)
        target = checked_path(target_name)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"vendored source is missing or not a regular file: {target_name}")
        if sha256(target) != entry.get("adapted_target_sha256"):
            raise ValueError(f"adapted source hash mismatch: {target_name}")
        require_hash(entry.get("source_file_sha256"), f"{repository} source {entry['source_path']}")

    for entry in upstream.get("removed_source_files", []):
        require_hash(entry.get("source_file_sha256"), f"{repository} omitted {entry['source_path']}")
        omitted_path = checked_path(f"{target_root}/{entry['source_path']}")
        if omitted_path.exists():
            raise ValueError(f"excluded upstream source is still vendored: {entry['source_path']}")

    change_name = upstream.get("local_change_record_path", upstream.get("local_patch_path"))
    change_hash = upstream.get("local_change_record_sha256", upstream.get("local_patch_sha256"))
    if not change_name.startswith(f"{target_root}/"):
        raise ValueError(f"local change record path escapes {target_root}: {change_name}")
    change_file = checked_path(change_name)
    if not change_file.is_file() or sha256(change_file) != change_hash:
        raise ValueError(f"local change record hash mismatch for {repository}")
    listed_targets.add(change_name)
    if "local_change_record_path" in upstream:
        change_record = json.loads(change_file.read_text(encoding="utf-8"))
        if (
            change_record.get("format_version") != 1
            or change_record.get("source_repository") != repository
            or change_record.get("source_commit") != upstream["source_commit"]
            or change_record.get("operation") != "remove trailing horizontal whitespace only"
        ):
            raise ValueError(f"local change record identity mismatch for {repository}")
        sources_by_path = {entry["source_path"]: entry for entry in upstream["source_files"]}
        seen_edits = set()
        for edit in change_record.get("edits", []):
            target_name = edit["target_path"]
            source_path = edit["source_path"]
            if target_name in seen_edits or source_path not in sources_by_path:
                raise ValueError(f"duplicate or unregistered local change: {target_name}")
            seen_edits.add(target_name)
            source_entry = sources_by_path[source_path]
            if (
                target_name != source_entry["target_path"]
                or edit["source_sha256"] != source_entry["source_file_sha256"]
                or edit["target_sha256"] != source_entry["adapted_target_sha256"]
            ):
                raise ValueError(f"local change hashes do not match source manifest: {source_path}")
            target = checked_path(target_name)
            lines = target.read_text(encoding="utf-8").splitlines()
            line_numbers = edit["source_line_numbers"]
            if not line_numbers or any(
                not isinstance(line, int) or line < 1 or line > len(lines)
                or lines[line - 1].rstrip(" \t") != lines[line - 1]
                for line in line_numbers
            ):
                raise ValueError(f"local whitespace normalization record is invalid: {source_path}")
        if not seen_edits:
            raise ValueError(f"empty local change record for {repository}")

    for license_file in upstream["license_files"]:
        target_name = license_file["target_path"]
        if not target_name.startswith(f"{target_root}/"):
            raise ValueError(f"license path escapes {target_root}: {target_name}")
        target = checked_path(target_name)
        if not target.is_file() or sha256(target) != license_file["sha256"]:
            raise ValueError(f"license hash mismatch: {target_name}")
        require_hash(license_file.get("source_sha256", license_file["sha256"]),
                     f"{repository} source license {license_file['source_path']}")
        if license_file.get("source_sha256", license_file["sha256"]) != license_file["sha256"]:
            raise ValueError(f"copied upstream license differs from its source: {target_name}")
        listed_targets.add(target_name)

    manifest_path = upstream.get("manifest_target", f"{target_root}/Cargo.toml")
    root_manifest = tomllib.loads(checked_path(manifest_path).read_text(encoding="utf-8"))
    packages = upstream["packages"]
    if not packages:
        raise ValueError(f"no package metadata recorded for {repository}")

    if layout == "workspace":
        expected_members = {package["source_path"] for package in packages}
        actual_members = set(root_manifest.get("workspace", {}).get("members", []))
        if actual_members != expected_members:
            raise ValueError(f"vendored workspace package scope changed for {repository}")
        package_manifest = root_manifest
        workspace_package = package_manifest.get("workspace", {}).get("package", {})
        for package in packages:
            manifest_path = checked_path(package["target_manifest"])
            manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
            metadata = manifest.get("package", {})
            if metadata.get("name") != package["name"]:
                raise ValueError(f"package name mismatch: {package['name']}")
            if package["license"] != upstream["license"]:
                raise ValueError(f"package license mismatch: {package['name']}")
            if metadata.get("version") != {"workspace": True}:
                raise ValueError(f"vendored package version must follow workspace: {package['name']}")
            if workspace_package.get("version") != package["version"]:
                raise ValueError(f"vendored package version mismatch: {package['name']}")
            if workspace_package.get("license") != "MIT OR Apache-2.0":
                raise ValueError(f"vendored workspace license changed: {repository}")
    else:
        if len(packages) != 1:
            raise ValueError(f"single-package vendor must declare one package: {repository}")
        package = packages[0]
        metadata = root_manifest.get("package", {})
        if (
            metadata.get("name") != package["name"]
            or metadata.get("version") != package["version"]
            or metadata.get("license") != package["license"]
            or metadata.get("edition") != upstream["package_edition"]
            or metadata.get("rust-version") != upstream["package_msrv"]
        ):
            raise ValueError(f"single-package metadata changed for {repository}")
        if root_manifest.get("dev-dependencies") or root_manifest.get("example"):
            raise ValueError(f"vendored SDK packaging/dev targets must be removed: {repository}")

    # Every file beneath a vendor root must be either immutable upstream source
    # or explicitly listed project-owned packaging/patch documentation.
    expected_files = listed_targets | {
        path for path in project_paths if path.startswith(f"{target_root}/")
    }
    root_dir = checked_path(target_root)
    actual_files = set()
    for candidate in root_dir.rglob("*"):
        if candidate.is_symlink():
            raise ValueError(f"vendor tree contains a symlink: {candidate.relative_to(ROOT)}")
        if candidate.is_file():
            actual_files.add(candidate.relative_to(ROOT).as_posix())
    unrecorded = actual_files - expected_files
    missing = expected_files - actual_files
    if unrecorded or missing:
        raise ValueError(
            f"vendor file inventory mismatch for {repository}: "
            f"unrecorded={sorted(unrecorded)[:5]} missing={sorted(missing)[:5]}"
        )


def check_reviewed_dependency_licenses(document: dict) -> None:
    lock = tomllib.loads(checked_path("Cargo.lock").read_text(encoding="utf-8"))
    lock_packages = {(package["name"], package["version"]): package for package in lock["package"]}
    deny = tomllib.loads(checked_path("deny.toml").read_text(encoding="utf-8"))
    license_policy = deny["licenses"]
    sbom = json.loads(checked_path("SBOM.spdx.json").read_text(encoding="utf-8"))
    sbom_packages = {
        (package["name"], package["versionInfo"]): package for package in sbom["packages"]
    }
    notice = checked_path("NOTICE").read_text(encoding="utf-8")
    for entry in document.get("reviewed_dependency_licenses", []):
        target_name = entry["target_license_path"]
        target = checked_path(target_name)
        if not target.is_file() or sha256(target) != entry.get("target_license_sha256"):
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


def check_ibkr_adapter_boundary() -> None:
    source_dir = checked_path("crates/ibkr-read/src")
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(source_dir.rglob("*.rs"))
    )
    forbidden = (
        r"pub\s+use\s+ibapi",
        r"pub\s+type\s+\w+\s*=\s*ibapi::",
        r"pub\s+(?:async\s+)?fn\s+\w+\s*\([^)]*ibapi::",
        r"\.\s*(?:place_order|cancel_order|exercise_options)\s*\(",
    )
    if any(re.search(pattern, source) for pattern in forbidden):
        raise ValueError("ibkr-read exposes SDK client types or write operations")
    if "pub struct IbkrCatalogAdapter" not in source or "client: ibapi::Client" not in source:
        raise ValueError("IBKR SDK client must remain a private adapter field")


def main() -> None:
    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for entry in document.get("source_files", []):
        target_name = entry["target_path"]
        target = checked_path(target_name)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"project source is missing or not a regular file: {target_name}")
        if sha256(target) != entry.get("adapted_target_sha256"):
            raise ValueError(f"adapted project source hash mismatch: {target_name}")
        require_hash(entry.get("source_blob_sha256"), f"source repository {entry['source_path']}")

    project_paths = set()
    for entry in document.get("new_project_code", []):
        target_name = entry["target_path"]
        target = checked_path(target_name)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"project file is missing or not a regular file: {target_name}")
        if sha256(target) != entry.get("sha256"):
            raise ValueError(f"project file hash mismatch: {target_name}")
        project_paths.add(target_name)

    upstreams = document.get("vendored_upstreams", [])
    if not upstreams:
        raise ValueError("no vendored upstream records are registered")
    repositories = [upstream["source_repository"] for upstream in upstreams]
    if len(repositories) != len(set(repositories)):
        raise ValueError("duplicate vendored upstream repository record")
    target_owners: dict[str, str] = {}
    package_owners: dict[str, str] = {}
    for upstream in upstreams:
        for entry in upstream["source_files"]:
            target = entry["target_path"]
            owner = target_owners.setdefault(target, upstream["source_repository"])
            if owner != upstream["source_repository"]:
                raise ValueError(f"vendored target belongs to multiple upstreams: {target}")
        for package in upstream["packages"]:
            owner = package_owners.setdefault(package["name"], upstream["source_repository"])
            if owner != upstream["source_repository"]:
                raise ValueError(f"vendored Cargo package has multiple sources: {package['name']}")
        check_vendor(upstream, project_paths)

    lock = tomllib.loads(checked_path("Cargo.lock").read_text(encoding="utf-8"))
    locked_packages = {(package["name"], package["version"]): package for package in lock["package"]}
    sbom = json.loads(checked_path("SBOM.spdx.json").read_text(encoding="utf-8"))
    sbom_packages = {
        (package["name"], package["versionInfo"]): package for package in sbom["packages"]
    }
    for upstream in upstreams:
        for package in upstream["packages"]:
            key = (package["name"], package["version"])
            if key not in locked_packages or key not in sbom_packages:
                raise ValueError(f"vendored package missing from lock/SBOM: {key}")
            sbom_item = sbom_packages[key]
            expected_origin = f"{upstream['source_repository']}@{upstream['source_commit']}"
            expected_download = (
                f"https://github.com/{upstream['source_repository']}/tree/"
                f"{upstream['source_commit']}"
            )
            if (
                sbom_item.get("licenseDeclared") != package["license"]
                or expected_origin not in sbom_item.get("sourceInfo", "")
                or expected_download not in sbom_item.get("downloadLocation", "")
            ):
                raise ValueError(f"vendored package SBOM provenance mismatch: {key}")

    check_reviewed_dependency_licenses(document)

    alpaca_source = "\n".join(
        checked_path(f"crates/alpaca-rest-read/src/{name}").read_text(encoding="utf-8")
        for name in ("client.rs", "error.rs", "lib.rs", "model.rs", "request.rs")
    )
    if re.search(r"std::env|\.from_env\s*\(|dotenv", alpaca_source):
        raise ValueError("alpaca-rest-read must use explicit credential injection")
    if re.search(r"\.\w*_all\s*\(", alpaca_source):
        raise ValueError("alpaca-rest-read must keep pagination finite")
    check_ibkr_adapter_boundary()

    summary = ", ".join(
        f"{upstream['source_repository']}@{upstream['source_commit']} "
        f"({len(upstream['source_files'])} source files, {len(upstream['packages'])} packages)"
        for upstream in upstreams
    )
    print(f"vendor provenance passed: {summary}")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"vendor provenance check failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
