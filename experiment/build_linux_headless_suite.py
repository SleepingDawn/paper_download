from __future__ import annotations

import argparse
from pathlib import Path

from linux_headless_suite_lib import (
    FULL_TARGETS,
    PILOT_TARGETS,
    build_attempt_index,
    build_suite_selection,
    default_attempt_ledger_path,
    default_input_csv,
    default_suite_dir,
    load_attempt_ledger,
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
    parser.add_argument("--attempt-ledger", type=Path, default=default_attempt_ledger_path())
    parser.add_argument("--ignore-attempt-ledger", action="store_true")
    args = parser.parse_args()

    source_csv = args.input.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    attempt_ledger_path = None if args.ignore_attempt_ledger else args.attempt_ledger.resolve()

    rows = load_source_rows(source_csv)
    attempt_ledger_entries = load_attempt_ledger(attempt_ledger_path)
    attempt_index = build_attempt_index(attempt_ledger_entries)
    pilot_suite = build_suite_selection(rows, suite_name="pilot", targets=PILOT_TARGETS, attempt_index=attempt_index)
    full_suite = build_suite_selection(rows, suite_name="full", targets=FULL_TARGETS, attempt_index=attempt_index)

    pilot_csv = output_dir / "pilot_sample.csv"
    full_csv = output_dir / "full_sample.csv"
    manifest_json = output_dir / "suite_manifest.json"

    write_csv(pilot_csv, pilot_suite["selected_rows"])
    write_csv(full_csv, full_suite["selected_rows"])
    write_json(
        manifest_json,
        manifest_payload(
            source_csv,
            rows,
            [pilot_suite, full_suite],
            attempt_ledger_path=attempt_ledger_path,
            attempt_ledger_entries=attempt_ledger_entries,
        ),
    )

    print(f"source_csv={relative_to_repo(source_csv)}")
    print(f"pilot_csv={relative_to_repo(pilot_csv)} total={len(pilot_suite['selected_rows'])}")
    print(f"full_csv={relative_to_repo(full_csv)} total={len(full_suite['selected_rows'])}")
    print(f"manifest_json={relative_to_repo(manifest_json)}")
    if attempt_ledger_path is not None:
        print(
            "attempt_ledger="
            f"{relative_to_repo(attempt_ledger_path)} "
            f"entries={len(attempt_ledger_entries)} unique_dois={len(attempt_index)}"
        )

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
