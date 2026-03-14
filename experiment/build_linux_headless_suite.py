from __future__ import annotations

import argparse
from pathlib import Path

from linux_headless_suite_lib import (
    FULL_TARGETS,
    PILOT_TARGETS,
    build_suite_selection,
    default_input_csv,
    default_suite_dir,
    load_source_rows,
    manifest_payload,
    relative_to_repo,
    write_csv,
    write_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build publisher-stratified DOI suites for Linux headless landing/download experiments."
    )
    parser.add_argument("--input", type=Path, default=default_input_csv())
    parser.add_argument("--output-dir", type=Path, default=default_suite_dir())
    args = parser.parse_args()

    source_csv = args.input.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_source_rows(source_csv)
    pilot_suite = build_suite_selection(rows, suite_name="pilot", targets=PILOT_TARGETS)
    full_suite = build_suite_selection(rows, suite_name="full", targets=FULL_TARGETS)

    pilot_csv = output_dir / "pilot_sample.csv"
    full_csv = output_dir / "full_sample.csv"
    manifest_json = output_dir / "suite_manifest.json"

    write_csv(pilot_csv, pilot_suite["selected_rows"])
    write_csv(full_csv, full_suite["selected_rows"])
    write_json(manifest_json, manifest_payload(source_csv, rows, [pilot_suite, full_suite]))

    print(f"source_csv={relative_to_repo(source_csv)}")
    print(f"pilot_csv={relative_to_repo(pilot_csv)} total={len(pilot_suite['selected_rows'])}")
    print(f"full_csv={relative_to_repo(full_csv)} total={len(full_suite['selected_rows'])}")
    print(f"manifest_json={relative_to_repo(manifest_json)}")

    if pilot_suite["gaps"] or full_suite["gaps"]:
        print("gaps_detected=1")
        for gap in pilot_suite["gaps"] + full_suite["gaps"]:
            print(
                "gap "
                f"suite={gap['suite']} publisher_group={gap['publisher_group']} "
                f"requested={gap['requested']} selected={gap['selected']} available={gap['available']}"
            )
    else:
        print("gaps_detected=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
