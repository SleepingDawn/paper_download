#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Iterable, Tuple


TOP_LEVEL_COPY_CANDIDATES = ("Local State", "First Run", "Last Version")
IGNORE_PATTERNS = (
    "Singleton*",
    "LOCK",
    "lockfile",
    "Crashpad",
    "BrowserMetrics",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "ShaderCache",
    "DawnCache",
    "optimization_guide_model_store",
    "component_crx_cache",
    "segmentation_platform",
)


def _derive_bundle_root_name(output_path: Path) -> str:
    name = output_path.name
    if name.endswith(".tar.gz"):
        return name[: -len(".tar.gz")]
    if name.endswith(".tgz"):
        return name[: -len(".tgz")]
    if name.endswith(".tar"):
        return name[: -len(".tar")]
    raise ValueError("output path must end with .tar, .tar.gz, or .tgz")


def _archive_mode(output_path: Path) -> str:
    name = output_path.name
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "w:gz"
    if name.endswith(".tar"):
        return "w"
    raise ValueError("output path must end with .tar, .tar.gz, or .tgz")


def _count_tree(path: Path) -> Tuple[int, int]:
    total_files = 0
    total_bytes = 0
    for child in path.rglob("*"):
        if child.is_file():
            total_files += 1
            total_bytes += child.stat().st_size
    return total_files, total_bytes


def _copy_profile_root(source_root: Path, bundle_root: Path, profile_name: str) -> Tuple[list[str], Path]:
    copied_top_level: list[str] = []
    source_profile_dir = source_root / profile_name
    if not source_profile_dir.is_dir():
        raise FileNotFoundError(f"profile directory is missing: {source_profile_dir}")

    bundle_root.mkdir(parents=True, exist_ok=True)
    for name in TOP_LEVEL_COPY_CANDIDATES:
        src = source_root / name
        if src.is_file():
            shutil.copy2(src, bundle_root / name)
            copied_top_level.append(name)

    target_profile_dir = bundle_root / profile_name
    shutil.copytree(
        source_profile_dir,
        target_profile_dir,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(*IGNORE_PATTERNS),
    )
    return copied_top_level, target_profile_dir


def _write_metadata(bundle_root: Path, profile_name: str, source_root: Path, copied_top_level: Iterable[str]) -> None:
    marker_path = bundle_root / ".codex_profile_seed_ready"
    marker_path.write_text(
        json.dumps(
            {
                "profile_name": profile_name,
                "source_root": str(source_root),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    manifest_path = bundle_root / "codex_profile_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "profile_name": profile_name,
                "source_root": str(source_root),
                "copied_top_level_files": list(copied_top_level),
                "ignore_patterns": list(IGNORE_PATTERNS),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_archive(bundle_root: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_path, _archive_mode(output_path)) as tf:
        tf.add(bundle_root, arcname=bundle_root.name)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Package a Linux Chrome user-data-dir into a clean seeded-profile archive."
    )
    parser.add_argument("--source-root", required=True, help="Chrome user-data-dir root created on Linux")
    parser.add_argument("--output", required=True, help="Output archive path (.tar, .tar.gz, or .tgz)")
    parser.add_argument("--profile-name", default="Default", help="Chrome profile directory name")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output archive if it already exists",
    )
    args = parser.parse_args()

    source_root = Path(args.source_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    profile_name = str(args.profile_name or "Default").strip() or "Default"

    if not source_root.is_dir():
        raise SystemExit(f"source root does not exist or is not a directory: {source_root}")
    if output_path.exists() and not args.overwrite:
        raise SystemExit(f"output already exists: {output_path} (use --overwrite to replace it)")

    bundle_root_name = _derive_bundle_root_name(output_path)
    with tempfile.TemporaryDirectory(prefix="chrome_seed_bundle_") as temp_dir:
        staging_root = Path(temp_dir) / bundle_root_name
        copied_top_level, target_profile_dir = _copy_profile_root(source_root, staging_root, profile_name)
        _write_metadata(staging_root, profile_name, source_root, copied_top_level)
        file_count, total_bytes = _count_tree(staging_root)
        _build_archive(staging_root, output_path)

        report = {
            "source_root": str(source_root),
            "output_archive": str(output_path),
            "bundle_root": bundle_root_name,
            "profile_name": profile_name,
            "copied_top_level_files": copied_top_level,
            "staged_profile_dir": str(target_profile_dir),
            "staged_file_count": file_count,
            "staged_total_bytes": total_bytes,
        }
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
