import argparse
import json
import os
import sys


PROFILE_READY_MARKER = ".codex_profile_seed_ready"


def inspect_profile_root(profile_root: str, profile_name: str) -> dict:
    root = os.path.abspath(str(profile_root or "").strip())
    profile = str(profile_name or "Default").strip() or "Default"
    profile_dir = os.path.join(root, profile)
    preferences_path = os.path.join(profile_dir, "Preferences")
    local_state_path = os.path.join(root, "Local State")
    cookie_paths = [
        os.path.join(profile_dir, "Network", "Cookies"),
        os.path.join(profile_dir, "Cookies"),
    ]
    storage_dirs = [
        os.path.join(profile_dir, "Local Storage"),
        os.path.join(profile_dir, "IndexedDB"),
    ]

    warnings = []
    if not os.path.isfile(local_state_path):
        warnings.append("Local State not found")
    if not any(os.path.isfile(path) for path in cookie_paths):
        warnings.append("Cookies DB not found")
    if not any(os.path.isdir(path) for path in storage_dirs):
        warnings.append("Local Storage/IndexedDB not found")

    return {
        "profile_root": root,
        "profile_name": profile,
        "root_exists": os.path.isdir(root),
        "profile_dir": profile_dir,
        "profile_dir_exists": os.path.isdir(profile_dir),
        "preferences_path": preferences_path,
        "preferences_exists": os.path.isfile(preferences_path),
        "local_state_path": local_state_path,
        "local_state_exists": os.path.isfile(local_state_path),
        "cookie_paths": cookie_paths,
        "cookie_exists": any(os.path.isfile(path) for path in cookie_paths),
        "storage_dirs": storage_dirs,
        "storage_exists": any(os.path.isdir(path) for path in storage_dirs),
        "marker_path": os.path.join(root, PROFILE_READY_MARKER),
        "marker_exists": os.path.isfile(os.path.join(root, PROFILE_READY_MARKER)),
        "warnings": warnings,
        "ok": os.path.isdir(profile_dir) and os.path.isfile(preferences_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check a Linux Chrome seeded profile root.")
    parser.add_argument("--profile-root", required=True, help="Chrome user-data-dir root")
    parser.add_argument("--profile-name", default="Default", help="Profile directory name")
    args = parser.parse_args()

    inspection = inspect_profile_root(args.profile_root, args.profile_name)
    print(json.dumps(inspection, ensure_ascii=False, indent=2))
    return 0 if inspection["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
