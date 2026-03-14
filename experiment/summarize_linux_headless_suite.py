from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

from linux_headless_suite_lib import GROUP_DISPLAY_NAMES, GROUP_ORDER, load_csv_rows, parse_bool, write_json


LANDING_BUCKET_ORDER = [
    "landing_success",
    "challenge_or_interstitial",
    "blank_or_incomplete",
    "timeout_or_error",
    "environment_or_config_failure",
    "access_rights",
    "doi_not_found",
    "missing",
    "other_non_success",
]

DOWNLOAD_BUCKET_ORDER = [
    "download_success",
    "landing_success_no_download",
    "challenge_or_interstitial",
    "blank_or_incomplete",
    "timeout_or_error",
    "environment_or_config_failure",
    "access_rights",
    "doi_not_found",
    "missing",
    "other_non_success",
]


def normalize_doi(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return raw.replace("https://doi.org/", "").replace("http://doi.org/", "").strip()


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def contains_environment_marker(values: Iterable[Any]) -> bool:
    blob = " ".join(str(item or "") for item in values).lower()
    return any(
        marker in blob
        for marker in (
            "browser_executable_not_found",
            "browser_init_failed",
            "chrome_smoke_failed",
            "persistent_profile_dir_required",
            "persistent_profile_dir",
            "linux_cli_seeded",
            "profile seed",
            "profile_root",
        )
    )


def landing_bucket_from_record(record: Dict[str, Any]) -> str:
    state = str(record.get("classifier_state") or "").strip().lower()
    reason_codes = list(record.get("reason_codes") or [])
    if state in {"success_landing", "direct_pdf_handoff"}:
        return "landing_success"
    if contains_environment_marker(reason_codes):
        return "environment_or_config_failure"
    if state in {"challenge_detected", "consent_or_interstitial_block"}:
        return "challenge_or_interstitial"
    if state in {"blank_or_incomplete", "broken_js_shell"}:
        return "blank_or_incomplete"
    if state in {"timeout", "network_error"}:
        return "timeout_or_error"
    if state == "doi_not_found":
        return "doi_not_found"
    if "institution" in " ".join(str(code or "") for code in reason_codes).lower():
        return "access_rights"
    return "other_non_success"


def combined_bucket(landing_bucket: str, download_bucket: str) -> str:
    if download_bucket == "download_success":
        return "download_success"
    if download_bucket == "missing":
        if landing_bucket in {
            "challenge_or_interstitial",
            "blank_or_incomplete",
            "timeout_or_error",
            "environment_or_config_failure",
            "access_rights",
            "doi_not_found",
        }:
            return landing_bucket
        if landing_bucket == "landing_success":
            return "missing"
        if landing_bucket == "missing":
            return "missing"
        return "other_non_success"
    if download_bucket == "landing_success_no_download" or landing_bucket == "landing_success":
        return "landed_no_download"
    for bucket in (
        "challenge_or_interstitial",
        "blank_or_incomplete",
        "timeout_or_error",
        "environment_or_config_failure",
        "access_rights",
        "doi_not_found",
    ):
        if download_bucket == bucket or landing_bucket == bucket:
            return bucket
    return "other_non_success"


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def markdown_report(summary: Dict[str, Any]) -> str:
    lines = [
        "# Linux Headless Experiment Summary",
        "",
        f"- suite: `{summary['suite']}`",
        f"- sample_total: `{summary['sample_total']}`",
        f"- landing_probe_records: `{summary['landing_probe_records']}`",
        f"- download_records: `{summary['download_records']}`",
        "",
        "## Overall Buckets",
        "",
        "### Landing Probe",
    ]
    for bucket in LANDING_BUCKET_ORDER:
        lines.append(f"- {bucket}: {summary['landing_probe_bucket_counts'].get(bucket, 0)}")
    lines.extend(["", "### Download", ""])
    for bucket in DOWNLOAD_BUCKET_ORDER:
        lines.append(f"- {bucket}: {summary['download_bucket_counts'].get(bucket, 0)}")
    lines.extend(["", "## Combined", ""])
    for bucket, count in summary["combined_bucket_counts"].items():
        lines.append(f"- {bucket}: {count}")
    lines.extend(["", "## Publisher Breakdown", ""])
    for row in summary["publisher_breakdown"]:
        lines.append(
            "- "
            f"{row['publisher_display_name']} ({row['publisher_group']}): "
            f"sample={row['sample_total']}, "
            f"landing_success={row['landing_success']}, "
            f"download_success={row['download_success']}, "
            f"landed_no_download={row['landed_no_download']}, "
            f"env_fail={row['environment_or_config_failure']}"
        )
    blocked_items = list(summary.get("blocked_items") or [])
    if blocked_items:
        lines.extend(["", "## Blocked", ""])
        for item in blocked_items:
            lines.append(f"- [blocked] {item}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge Linux landing/download experiment outputs into a diagnostic summary.")
    parser.add_argument("--suite", required=True)
    parser.add_argument("--sample-csv", type=Path, required=True)
    parser.add_argument("--landing-jsonl", type=Path, required=False)
    parser.add_argument("--landing-report", type=Path, required=False)
    parser.add_argument("--download-results-csv", type=Path, required=False)
    parser.add_argument("--download-summary-json", type=Path, required=False)
    parser.add_argument("--merged-csv", type=Path, required=True)
    parser.add_argument("--publisher-summary-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--summary-md", type=Path, required=True)
    args = parser.parse_args()

    sample_rows = load_csv_rows(args.sample_csv.resolve())
    sample_by_doi = {normalize_doi(row.get("doi")): row for row in sample_rows}

    landing_rows: List[Dict[str, Any]] = []
    if args.landing_jsonl and args.landing_jsonl.exists():
        landing_rows = read_jsonl(args.landing_jsonl.resolve())
    landing_by_doi = {normalize_doi(row.get("doi")): row for row in landing_rows}

    download_rows: List[Dict[str, Any]] = []
    if args.download_results_csv and args.download_results_csv.exists():
        download_rows = load_csv_rows(args.download_results_csv.resolve())
    download_by_doi = {normalize_doi(row.get("doi")): row for row in download_rows}

    landing_probe_bucket_counts = Counter()
    download_bucket_counts = Counter()
    combined_bucket_counts = Counter()
    merged_rows: List[Dict[str, Any]] = []
    blocked_items: List[str] = []

    if not landing_rows:
        blocked_items.append("landing_access_repro 결과 JSONL이 없어 landing-only 판단이 부분적입니다.")
    if not download_rows:
        blocked_items.append("parallel_download 결과 CSV가 없어 실다운로드 판단이 부분적입니다.")

    for doi, sample in sample_by_doi.items():
        landing = landing_by_doi.get(doi, {})
        download = download_by_doi.get(doi, {})
        landing_bucket = landing_bucket_from_record(landing) if landing else "missing"
        download_bucket = str(download.get("experiment_download_bucket") or "").strip() or "missing"
        combined = combined_bucket(landing_bucket, download_bucket)

        landing_probe_bucket_counts[landing_bucket] += 1
        download_bucket_counts[download_bucket] += 1
        combined_bucket_counts[combined] += 1

        merged_rows.append(
            {
                "suite_name": sample.get("suite_name", args.suite),
                "experiment_publisher_group": sample.get("experiment_publisher_group", ""),
                "publisher_display_name": sample.get("publisher_display_name", ""),
                "selection_bucket": sample.get("selection_bucket", ""),
                "suite_slot_bucket": sample.get("suite_slot_bucket", ""),
                "source_open_access": sample.get("source_open_access", ""),
                "source_has_pdf_url": sample.get("source_has_pdf_url", ""),
                "doi": doi,
                "title": sample.get("title", ""),
                "publisher": sample.get("publisher", ""),
                "landing_probe_bucket": landing_bucket,
                "landing_probe_state": landing.get("classifier_state", ""),
                "landing_probe_outcome": landing.get("outcome", ""),
                "landing_probe_reason_codes": json.dumps(list(landing.get("reason_codes") or []), ensure_ascii=False),
                "landing_probe_url": landing.get("resolved_url", ""),
                "landing_probe_session_source": landing.get("browser_session_source", ""),
                "download_status": download.get("download_status", ""),
                "download_result_reason": download.get("download_result_reason", ""),
                "download_result_stage": download.get("download_result_stage", ""),
                "download_result_domain": download.get("download_result_domain", ""),
                "download_landing_bucket": download.get("experiment_landing_bucket", ""),
                "download_bucket": download_bucket,
                "download_session_source": download.get("browser_session_source", ""),
                "download_session_mode": download.get("browser_session_mode", ""),
                "combined_bucket": combined,
            }
        )

    publisher_rollup: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "publisher_group": "",
            "publisher_display_name": "",
            "sample_total": 0,
            "landing_success": 0,
            "challenge_or_interstitial": 0,
            "blank_or_incomplete": 0,
            "timeout_or_error": 0,
            "environment_or_config_failure": 0,
            "download_success": 0,
            "landed_no_download": 0,
            "missing_records": 0,
        }
    )

    for row in merged_rows:
        group = str(row.get("experiment_publisher_group") or "other")
        bucket = publisher_rollup[group]
        bucket["publisher_group"] = group
        bucket["publisher_display_name"] = row.get("publisher_display_name") or GROUP_DISPLAY_NAMES.get(group, group)
        bucket["sample_total"] += 1
        combined = str(row.get("combined_bucket") or "")
        if combined == "download_success":
            bucket["download_success"] += 1
        elif combined == "landed_no_download":
            bucket["landed_no_download"] += 1
        elif combined == "missing":
            bucket["missing_records"] += 1
        elif combined in {
            "challenge_or_interstitial",
            "blank_or_incomplete",
            "timeout_or_error",
            "environment_or_config_failure",
        }:
            bucket[combined] += 1
        if str(row.get("landing_probe_bucket") or "") == "landing_success":
            bucket["landing_success"] += 1

    publisher_rows = sorted(
        publisher_rollup.values(),
        key=lambda row: (
            GROUP_ORDER.index(row["publisher_group"]) if row["publisher_group"] in GROUP_ORDER else len(GROUP_ORDER),
            row["publisher_display_name"],
        ),
    )

    landing_report = read_json(args.landing_report.resolve()) if args.landing_report and args.landing_report.exists() else {}
    download_summary = (
        read_json(args.download_summary_json.resolve())
        if args.download_summary_json and args.download_summary_json.exists()
        else {}
    )

    summary = {
        "suite": args.suite,
        "sample_total": len(sample_rows),
        "landing_probe_records": len(landing_rows),
        "download_records": len(download_rows),
        "landing_probe_bucket_counts": {
            bucket: int(landing_probe_bucket_counts.get(bucket, 0)) for bucket in LANDING_BUCKET_ORDER
        },
        "download_bucket_counts": {
            bucket: int(download_bucket_counts.get(bucket, 0)) for bucket in DOWNLOAD_BUCKET_ORDER
        },
        "combined_bucket_counts": dict(sorted((key, int(value)) for key, value in combined_bucket_counts.items())),
        "publisher_breakdown": publisher_rows,
        "landing_report_summary": landing_report.get("summary", {}),
        "download_report_summary": download_summary.get("experiment_outcomes", {}),
        "blocked_items": blocked_items,
    }

    merged_fields = [
        "suite_name",
        "experiment_publisher_group",
        "publisher_display_name",
        "selection_bucket",
        "suite_slot_bucket",
        "source_open_access",
        "source_has_pdf_url",
        "doi",
        "title",
        "publisher",
        "landing_probe_bucket",
        "landing_probe_state",
        "landing_probe_outcome",
        "landing_probe_reason_codes",
        "landing_probe_url",
        "landing_probe_session_source",
        "download_status",
        "download_result_reason",
        "download_result_stage",
        "download_result_domain",
        "download_landing_bucket",
        "download_bucket",
        "download_session_source",
        "download_session_mode",
        "combined_bucket",
    ]
    publisher_fields = [
        "publisher_group",
        "publisher_display_name",
        "sample_total",
        "landing_success",
        "challenge_or_interstitial",
        "blank_or_incomplete",
        "timeout_or_error",
        "environment_or_config_failure",
        "download_success",
        "landed_no_download",
        "missing_records",
    ]

    write_csv(args.merged_csv.resolve(), merged_rows, merged_fields)
    write_csv(args.publisher_summary_csv.resolve(), publisher_rows, publisher_fields)
    write_json(args.summary_json.resolve(), summary)
    args.summary_md.resolve().parent.mkdir(parents=True, exist_ok=True)
    args.summary_md.resolve().write_text(markdown_report(summary), encoding="utf-8")

    print(f"merged_csv={args.merged_csv.resolve()}")
    print(f"publisher_summary_csv={args.publisher_summary_csv.resolve()}")
    print(f"summary_json={args.summary_json.resolve()}")
    print(f"summary_md={args.summary_md.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
