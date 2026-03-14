import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def iso_from_timestamp(ts: float | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def iter_files(base: Path) -> Iterable[Path]:
    for root, dirnames, filenames in os.walk(base):
        dirnames.sort()
        filenames.sort()
        root_path = Path(root)
        for name in filenames:
            yield root_path / name


def snapshot_file(path: Path) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "path": str(path),
        "exists": path.is_file(),
        "kind": "file",
    }
    if not path.is_file():
        payload.update(
            {
                "size": 0,
                "mtime_epoch": None,
                "mtime_iso": "",
                "sha256": "",
            }
        )
        return payload
    stat = path.stat()
    payload.update(
        {
            "size": int(stat.st_size),
            "mtime_epoch": float(stat.st_mtime),
            "mtime_iso": iso_from_timestamp(stat.st_mtime),
            "sha256": sha256_file(path),
        }
    )
    return payload


def snapshot_tree(path: Path) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "path": str(path),
        "exists": path.is_dir(),
        "kind": "directory",
    }
    if not path.is_dir():
        payload.update(
            {
                "file_count": 0,
                "total_size": 0,
                "latest_mtime_epoch": None,
                "latest_mtime_iso": "",
                "tree_sha256": "",
            }
        )
        return payload

    digest = hashlib.sha256()
    file_count = 0
    total_size = 0
    latest_mtime = 0.0
    for file_path in iter_files(path):
        stat = file_path.stat()
        rel = file_path.relative_to(path).as_posix()
        file_hash = sha256_file(file_path)
        latest_mtime = max(latest_mtime, float(stat.st_mtime))
        file_count += 1
        total_size += int(stat.st_size)
        digest.update(
            json.dumps(
                {
                    "relpath": rel,
                    "size": int(stat.st_size),
                    "mtime_ns": int(stat.st_mtime_ns),
                    "sha256": file_hash,
                },
                sort_keys=True,
            ).encode("utf-8")
        )

    payload.update(
        {
            "file_count": file_count,
            "total_size": total_size,
            "latest_mtime_epoch": latest_mtime if latest_mtime else None,
            "latest_mtime_iso": iso_from_timestamp(latest_mtime) if latest_mtime else "",
            "tree_sha256": digest.hexdigest(),
        }
    )
    return payload


def select_cookie_path(profile_dir: Path) -> Dict[str, Any]:
    candidates = [
        profile_dir / "Network" / "Cookies",
        profile_dir / "Cookies",
    ]
    selected = next((path for path in candidates if path.is_file()), candidates[0])
    payload = snapshot_file(selected)
    payload["candidates"] = [str(path) for path in candidates]
    return payload


def build_snapshot(profile_root: Path, profile_name: str, label: str) -> Dict[str, Any]:
    profile_dir = profile_root / profile_name
    return {
        "captured_at": utc_now_iso(),
        "label": label,
        "profile_root": str(profile_root),
        "profile_name": profile_name,
        "profile_dir": str(profile_dir),
        "targets": {
            "local_state": snapshot_file(profile_root / "Local State"),
            "cookies": select_cookie_path(profile_dir),
            "local_storage": snapshot_tree(profile_dir / "Local Storage"),
            "indexeddb": snapshot_tree(profile_dir / "IndexedDB"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture a Chrome profile state snapshot for before/after diagnostics.")
    parser.add_argument("--profile-root", required=True, help="Chrome user-data-dir root")
    parser.add_argument("--profile-name", default="Default", help="Profile directory name")
    parser.add_argument("--label", default="", help="Human-readable label for this snapshot")
    parser.add_argument("--output", required=True, help="Output JSON path")
    args = parser.parse_args()

    profile_root = Path(str(args.profile_root)).expanduser().resolve()
    profile_name = str(args.profile_name or "Default").strip() or "Default"
    output_path = Path(str(args.output)).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = build_snapshot(profile_root=profile_root, profile_name=profile_name, label=str(args.label or "").strip())
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
