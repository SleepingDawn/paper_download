#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List


TOP_LEVEL_PATHS = (
    "Local State",
    "First Run",
    "Last Version",
    ".codex_profile_seed_ready",
    "codex_profile_manifest.json",
)

PROFILE_REQUIRED_PATHS = ("Preferences",)

PROFILE_SIGNAL_PATH_GROUPS = {
    "cookies": ("Network/Cookies", "Cookies"),
    "local_storage": ("Local Storage",),
    "session_storage": ("Session Storage",),
    "indexed_db": ("IndexedDB",),
    "history": ("History",),
    "extensions": ("Extensions",),
}


def _existing_paths(base: Path, relative_candidates: tuple[str, ...]) -> List[str]:
    hits: List[str] = []
    for rel in relative_candidates:
        if (base / rel).exists():
            hits.append(rel)
    return hits


def inspect_profile_root(profile_root: Path, profile_name: str) -> Dict[str, object]:
    resolved_root = profile_root.expanduser().resolve()
    profile_dir = resolved_root / profile_name

    top_level = {name: (resolved_root / name).exists() for name in TOP_LEVEL_PATHS}
    required_profile = {name: (profile_dir / name).exists() for name in PROFILE_REQUIRED_PATHS}
    signal_paths = {
        key: _existing_paths(profile_dir, candidates)
        for key, candidates in PROFILE_SIGNAL_PATH_GROUPS.items()
    }

    warnings: List[str] = []
    if not resolved_root.exists():
        warnings.append("profile root does not exist")
    if resolved_root.exists() and not resolved_root.is_dir():
        warnings.append("profile root is not a directory")
    if resolved_root.is_dir() and not profile_dir.is_dir():
        warnings.append(f"profile directory '{profile_name}' is missing under the root")
    if profile_dir.is_dir() and not required_profile["Preferences"]:
        warnings.append("Preferences file is missing in the selected profile")
    if profile_dir.is_dir() and not signal_paths["cookies"]:
        warnings.append("cookie database is missing; warm login state may not be preserved")
    if profile_dir.is_dir() and not signal_paths["local_storage"] and not signal_paths["indexed_db"]:
        warnings.append("web storage directories are missing; publisher session state may be weak")
    if resolved_root.is_dir() and not top_level["Local State"]:
        warnings.append("Local State is missing at the profile root")

    result = {
        "profile_root": str(resolved_root),
        "profile_name": profile_name,
        "root_exists": resolved_root.exists(),
        "root_is_dir": resolved_root.is_dir(),
        "profile_dir_exists": profile_dir.is_dir(),
        "required_ok": resolved_root.is_dir() and profile_dir.is_dir() and all(required_profile.values()),
        "top_level_paths": top_level,
        "required_profile_paths": required_profile,
        "signal_paths": signal_paths,
        "warnings": warnings,
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check whether a Linux Chrome user-data-dir is usable as a seeded profile root."
    )
    parser.add_argument("--profile-root", required=True, help="Chrome user-data-dir root to inspect")
    parser.add_argument("--profile-name", default="Default", help="Chrome profile directory name")
    args = parser.parse_args()

    report = inspect_profile_root(Path(args.profile_root), str(args.profile_name or "Default").strip() or "Default")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["required_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
