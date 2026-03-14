import argparse
import json
import os
import shutil
import tarfile
import tempfile


PROFILE_READY_MARKER = ".codex_profile_seed_ready"
TOP_LEVEL_FILES = ("Local State", "First Run", "Last Version")
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


def _bundle_root_name(output_path: str) -> str:
    filename = os.path.basename(output_path)
    if filename.endswith(".tar.gz"):
        return filename[:-7]
    if filename.endswith(".tgz"):
        return filename[:-4]
    stem, _ = os.path.splitext(filename)
    return stem or "linux_chrome_user_data_seed"


def _inspect(profile_root: str, profile_name: str) -> dict:
    root = os.path.abspath(str(profile_root or "").strip())
    profile_dir = os.path.join(root, profile_name)
    return {
        "root": root,
        "profile_dir": profile_dir,
        "root_exists": os.path.isdir(root),
        "profile_exists": os.path.isdir(profile_dir),
        "preferences_exists": os.path.isfile(os.path.join(profile_dir, "Preferences")),
    }


def package_profile(source_root: str, output_path: str, profile_name: str) -> str:
    source_root = os.path.abspath(str(source_root or "").strip())
    output_path = os.path.abspath(str(output_path or "").strip())
    profile_name = str(profile_name or "Default").strip() or "Default"
    inspection = _inspect(source_root, profile_name)
    if not inspection["root_exists"] or not inspection["profile_exists"] or not inspection["preferences_exists"]:
        raise RuntimeError(
            "Invalid Linux seed profile root. Expected a user-data-dir root containing "
            f"{profile_name}/Preferences: {source_root}"
        )

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    bundle_root_name = _bundle_root_name(output_path)

    with tempfile.TemporaryDirectory(prefix="linux_seed_bundle_") as tmp_dir:
        staged_root = os.path.join(tmp_dir, bundle_root_name)
        staged_profile_dir = os.path.join(staged_root, profile_name)
        os.makedirs(staged_root, exist_ok=True)

        for top_name in TOP_LEVEL_FILES:
            src_path = os.path.join(source_root, top_name)
            if os.path.isfile(src_path):
                shutil.copy2(src_path, os.path.join(staged_root, top_name))

        shutil.copytree(
            os.path.join(source_root, profile_name),
            staged_profile_dir,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns(*IGNORE_PATTERNS),
        )

        with open(os.path.join(staged_root, PROFILE_READY_MARKER), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "profile_name": profile_name,
                    "source_root": source_root,
                },
                f,
                ensure_ascii=False,
            )

        with tarfile.open(output_path, "w:gz") as tar:
            tar.add(staged_root, arcname=os.path.basename(staged_root))

    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Package a Linux Chrome seeded profile into a tar.gz bundle.")
    parser.add_argument("--source-root", required=True, help="Chrome user-data-dir root to package")
    parser.add_argument("--output", required=True, help="Output .tar.gz path")
    parser.add_argument("--profile-name", default="Default", help="Profile directory name")
    args = parser.parse_args()

    output_path = package_profile(args.source_root, args.output, args.profile_name)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
