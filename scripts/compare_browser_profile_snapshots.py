import argparse
import json
from pathlib import Path
from typing import Any, Dict


def load_snapshot(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def compare_target(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(after.get("kind") or before.get("kind") or "")
    if kind == "file":
        keys = ("exists", "size", "mtime_iso", "sha256")
    else:
        keys = ("exists", "file_count", "total_size", "latest_mtime_iso", "tree_sha256")
    changed_fields = {
        key: {
            "before": before.get(key),
            "after": after.get(key),
        }
        for key in keys
        if before.get(key) != after.get(key)
    }
    return {
        "kind": kind,
        "path_before": before.get("path"),
        "path_after": after.get("path"),
        "changed": bool(changed_fields),
        "changed_fields": changed_fields,
    }


def build_diff(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    target_names = sorted(set((before.get("targets") or {}).keys()) | set((after.get("targets") or {}).keys()))
    targets = {
        name: compare_target(
            (before.get("targets") or {}).get(name, {}),
            (after.get("targets") or {}).get(name, {}),
        )
        for name in target_names
    }
    changed_targets = [name for name, payload in targets.items() if payload.get("changed")]
    return {
        "before_snapshot": {
            "path": before.get("_snapshot_path", ""),
            "label": before.get("label", ""),
            "captured_at": before.get("captured_at", ""),
            "profile_root": before.get("profile_root", ""),
            "profile_name": before.get("profile_name", ""),
        },
        "after_snapshot": {
            "path": after.get("_snapshot_path", ""),
            "label": after.get("label", ""),
            "captured_at": after.get("captured_at", ""),
            "profile_root": after.get("profile_root", ""),
            "profile_name": after.get("profile_name", ""),
        },
        "changed_targets": changed_targets,
        "unchanged_targets": [name for name in target_names if name not in changed_targets],
        "targets": targets,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two Chrome profile state snapshots.")
    parser.add_argument("--before", required=True, help="Before snapshot JSON")
    parser.add_argument("--after", required=True, help="After snapshot JSON")
    parser.add_argument("--output", default="", help="Optional output JSON path")
    args = parser.parse_args()

    before = load_snapshot(args.before)
    after = load_snapshot(args.after)
    before["_snapshot_path"] = str(Path(args.before).expanduser().resolve())
    after["_snapshot_path"] = str(Path(args.after).expanduser().resolve())
    payload = build_diff(before=before, after=after)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if str(args.output or "").strip():
        output_path = Path(str(args.output)).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(str(output_path))
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
