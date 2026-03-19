from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

from linux_headless_suite_lib import (
    GROUP_DISPLAY_NAMES,
    GROUP_ORDER,
    RECENT_YEAR_FLOOR,
    load_csv_rows,
    write_json,
)


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
    "publisher_native_download",
    "scihub_assisted_download",
    "download_success_unknown",
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
SUCCESS_DOWNLOAD_BUCKETS = {
    "publisher_native_download",
    "scihub_assisted_download",
    "download_success_unknown",
}
FAILURE_BUCKETS = {
    "challenge_or_interstitial",
    "blank_or_incomplete",
    "timeout_or_error",
    "environment_or_config_failure",
    "access_rights",
    "doi_not_found",
}


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


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def parse_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def resolve_publisher_group(sample: Dict[str, Any]) -> str:
    for key in ("experiment_publisher_group", "benchmark_group", "scheduler_publisher"):
        value = str(sample.get(key) or "").strip().lower()
        if value:
            return value
    publisher = str(sample.get("publisher") or "").strip().lower()
    if "elsevier" in publisher:
        return "elsevier"
    if "american institute of physics" in publisher or publisher == "aip":
        return "aip"
    if "electrical and electronics engineers" in publisher or publisher == "ieee":
        return "ieee"
    if "iop" in publisher:
        return "iop"
    return "other"


def resolve_publisher_display_name(sample: Dict[str, Any], group: str) -> str:
    value = str(sample.get("publisher_display_name") or "").strip()
    if value:
        return value
    publisher = str(sample.get("publisher") or "").strip()
    if publisher:
        return publisher
    return GROUP_DISPLAY_NAMES.get(group, group)


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


def landing_record_from_download_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if not row:
        return {}
    return {
        "doi": row.get("doi", ""),
        "classifier_state": str(row.get("landing_state") or ""),
        "outcome": str(row.get("landing_state") or ""),
        "reason_codes": parse_json_list(row.get("download_evidence")),
        "resolved_url": str(row.get("landing_url") or ""),
        "browser_session_source": str(row.get("browser_session_source") or ""),
        "browser_session_decision_reason": str(row.get("browser_session_decision_reason") or ""),
        "browser_user_data_dir": str(row.get("browser_user_data_dir") or ""),
        "browser_effective_user_data_dir": str(row.get("browser_effective_user_data_dir") or ""),
        "browser_debug_address": str(row.get("browser_debug_address") or ""),
        "aip_first_contact_policy": str(row.get("landing_aip_first_contact_policy") or ""),
        "aip_low_pressure_first_contact": parse_bool(row.get("landing_aip_low_pressure_first_contact")),
        "aip_direct_doi_path_used": parse_bool(row.get("landing_aip_direct_doi_path_used")),
        "aip_alternate_pages_opened": parse_bool(row.get("landing_aip_alternate_pages_opened")),
        "probe_page_mode_requested": str(row.get("landing_probe_page_mode_requested") or ""),
        "probe_page_mode_effective": str(row.get("landing_probe_page_mode_effective") or ""),
        "probe_open_attempts": parse_int(row.get("landing_probe_open_attempts")),
        "post_open_tab_trim_closed_count": parse_int(row.get("landing_post_open_tab_trim_closed_count")),
        "probe_open_succeeded": parse_bool(row.get("landing_probe_open_succeeded")),
        "probe_attach_restart_reason": str(row.get("landing_probe_attach_restart_reason") or ""),
        "controller_page_reused": parse_bool(row.get("landing_controller_page_reused")),
        "controller_reuse_allowed": parse_bool(row.get("landing_controller_reuse_allowed")),
        "controller_restart_reason": str(row.get("landing_controller_restart_reason") or ""),
        "controller_restart_count": parse_int(row.get("landing_controller_restart_count")),
        "controller_create_attempts": parse_int(row.get("landing_controller_create_attempts")),
        "startup_tab_cleanup_applied": parse_bool(row.get("landing_startup_tab_cleanup_applied")),
        "startup_tab_cleanup_before_count": parse_int(row.get("landing_startup_tab_cleanup_before_count")),
        "startup_tab_cleanup_after_count": parse_int(row.get("landing_startup_tab_cleanup_after_count")),
        "startup_tab_cleanup_closed_count": parse_int(row.get("landing_startup_tab_cleanup_closed_count")),
        "startup_page_reset_to_blank": parse_bool(row.get("landing_startup_page_reset_to_blank")),
        "browser_process_alive": parse_bool(row.get("landing_probe_browser_process_alive")),
        "page_access_ok": parse_bool(row.get("landing_probe_page_access_ok")),
        "page_probe_error": str(row.get("landing_probe_page_probe_error") or ""),
        "final_active_tab_id": str(row.get("landing_final_active_tab_id") or ""),
        "final_total_tab_count": parse_int(row.get("landing_final_total_tab_count")),
        "peak_tab_count_observed": parse_int(row.get("landing_peak_tab_count_observed")),
        "reduced_tab_path_used": parse_bool(row.get("landing_reduced_tab_path_used")),
        "tab_lifecycle_sequence": parse_json_list(row.get("landing_tab_lifecycle_sequence")),
        "page_disconnect_observed": parse_bool(row.get("landing_page_disconnect_observed")),
        "page_disconnect_stage": str(row.get("landing_page_disconnect_stage") or ""),
        "network_listener_started": parse_bool(row.get("landing_network_listener_started")),
        "network_listener_error": str(row.get("landing_network_listener_error") or ""),
        "runtime_probe_installed": parse_bool(row.get("landing_runtime_probe_installed")),
        "runtime_probe_error": str(row.get("landing_runtime_probe_error") or ""),
        "challenge_detected": parse_bool(row.get("landing_challenge_detected")),
        "entry_strategy": str(row.get("landing_entry_strategy") or ""),
        "entry_strategy_variant": str(row.get("landing_entry_strategy_variant") or ""),
        "entry_redirect_probe_mode": str(row.get("landing_entry_redirect_probe_mode") or ""),
        "entry_prebrowser_request_count": parse_int(row.get("landing_entry_prebrowser_request_count")),
        "entry_preanalysis_ran": parse_bool(row.get("landing_entry_preanalysis_ran")),
        "entry_url": str(row.get("landing_entry_url") or ""),
        "entry_resolved_url": str(row.get("landing_entry_resolved_url") or ""),
        "entry_browser_url": str(row.get("landing_entry_browser_url") or ""),
        "entry_browser_kind": str(row.get("landing_entry_browser_kind") or ""),
        "entry_handoff_url": str(row.get("landing_entry_handoff_url") or ""),
        "entry_handoff_used": bool(str(row.get("landing_entry_handoff_url") or "").strip()),
        "entry_context_url": str(row.get("landing_entry_context_url") or ""),
        "entry_context_kind": str(row.get("landing_entry_context_kind") or ""),
        "entry_context_bootstrap_mode": str(row.get("landing_entry_context_bootstrap_mode") or ""),
        "entry_context_bootstrap_attempted": parse_bool(row.get("landing_entry_context_bootstrap_attempted")),
        "entry_context_bootstrap_outcome": str(row.get("landing_entry_context_bootstrap_outcome") or ""),
        "entry_context_bootstrap_final_url": str(row.get("landing_entry_context_bootstrap_final_url") or ""),
        "entry_context_bootstrap_final_title": str(row.get("landing_entry_context_bootstrap_final_title") or ""),
        "entry_context_bootstrap_cache_hit": parse_bool(row.get("landing_entry_context_bootstrap_cache_hit")),
        "entry_context_bootstrap_cache_state": str(row.get("landing_entry_context_bootstrap_cache_state") or ""),
        "entry_navigation_route": str(row.get("landing_entry_navigation_route") or ""),
        "entry_preflight_url": str(row.get("landing_entry_preflight_url") or ""),
        "entry_redirect_chain_summary": parse_json_list(row.get("landing_entry_redirect_chain_summary")),
        "entry_fallback_used": parse_bool(row.get("landing_entry_fallback_used")),
        "entry_fallback_reason": str(row.get("landing_entry_fallback_reason") or ""),
        "entry_preflight_issue": str(row.get("landing_entry_preflight_issue") or ""),
        "entry_preflight_issue_overridden": parse_bool(row.get("landing_entry_preflight_issue_overridden")),
        "entry_browser_open_skipped": parse_bool(row.get("landing_entry_browser_open_skipped")),
        "landing_recovery_attempted": parse_bool(row.get("landing_recovery_attempted")),
        "landing_recovery_strategy": str(row.get("landing_recovery_strategy") or ""),
        "landing_recovery_outcome": str(row.get("landing_recovery_outcome") or ""),
        "tab_transition_count": parse_int(row.get("landing_tab_transition_count")),
        "js_runtime_probe_ok": parse_bool(row.get("landing_js_runtime_probe_ok")),
        "js_probe_error": str(row.get("landing_js_probe_error") or ""),
        "navigator_cookie_enabled": row.get("landing_navigator_cookie_enabled"),
        "document_cookie_len": parse_int(row.get("landing_document_cookie_len")),
        "challenge_script_present": parse_bool(row.get("landing_challenge_script_present")),
        "cf_chl_opt_present": parse_bool(row.get("landing_cf_chl_opt_present")),
        "noscript_cookie_hint_present": parse_bool(row.get("landing_noscript_cookie_hint_present")),
        "cookie_jar_probe_ok": parse_bool(row.get("landing_cookie_jar_probe_ok")),
        "cookie_jar_count": parse_int(row.get("landing_cookie_jar_count")),
        "aip_cookie_count": parse_int(row.get("landing_aip_cookie_count")),
        "cloudflare_cookie_count": parse_int(row.get("landing_cloudflare_cookie_count")),
        "profile_cookie_db_exists": parse_bool(row.get("landing_profile_cookie_db_exists")),
        "profile_cookie_db_writable": parse_bool(row.get("landing_profile_cookie_db_writable")),
        "profile_storage_exists": parse_bool(row.get("landing_profile_storage_exists")),
        "profile_preferences_exists": parse_bool(row.get("landing_profile_preferences_exists")),
        "reclassified_after_detector_fix": parse_bool(row.get("landing_reclassified_after_detector_fix")),
        "reclassification_reason": str(row.get("landing_reclassification_reason") or ""),
    }


def download_succeeded(record: Dict[str, Any]) -> bool:
    status = str(record.get("download_status") or "").strip().lower()
    return status.startswith("success")


def download_source_bucket_from_record(record: Dict[str, Any]) -> str:
    if not record or not download_succeeded(record):
        return ""
    source = str(record.get("download_source_category") or "").strip().lower()
    method = str(record.get("download_method") or "").strip().lower()
    stage = str(record.get("download_result_stage") or "").strip().lower()
    status = str(record.get("download_status") or "").strip().lower()

    if source == "publisher_native" or method in {"drission", "direct_oa", "api"}:
        return "publisher_native_download"
    if source == "scihub_assisted" or "scihub" in method or stage == "scihub" or "scihub" in status:
        return "scihub_assisted_download"
    return "download_success_unknown"


def download_bucket_from_record(record: Dict[str, Any]) -> str:
    if not record:
        return "missing"
    success_bucket = download_source_bucket_from_record(record)
    if success_bucket:
        return success_bucket
    legacy_bucket = str(record.get("experiment_download_bucket") or "").strip()
    if legacy_bucket == "download_success":
        return "download_success_unknown"
    if legacy_bucket:
        return legacy_bucket
    return "missing"


def combined_bucket(landing_bucket: str, download_bucket: str) -> str:
    if download_bucket in SUCCESS_DOWNLOAD_BUCKETS:
        return download_bucket
    if download_bucket == "missing":
        if landing_bucket in FAILURE_BUCKETS:
            return landing_bucket
        if landing_bucket == "landing_success":
            return "missing"
        if landing_bucket == "missing":
            return "missing"
        return "other_non_success"
    for bucket in FAILURE_BUCKETS:
        if download_bucket == bucket or landing_bucket == bucket:
            return bucket
    if download_bucket == "landing_success_no_download" or landing_bucket == "landing_success":
        return "landing_success_no_download"
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
        f"- recent_year_floor: `{summary['recent_year_floor']}`",
        f"- recent_primary_total: `{summary['validation_cohort_counts'].get('recent_primary', 0)}`",
        f"- legacy_fallback_total: `{summary['validation_cohort_counts'].get('legacy_fallback', 0)}`",
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
    lines.extend(["", "## Validation Cohorts", ""])
    for cohort, count in summary["validation_cohort_counts"].items():
        lines.append(f"- {cohort}: {count}")
    lines.extend(["", "## Retry Protection", ""])
    for action, count in summary["retry_action_counts"].items():
        lines.append(f"- {action}: {count}")
    lines.extend(["", "## Combined", ""])
    for bucket, count in summary["combined_bucket_counts"].items():
        lines.append(f"- {bucket}: {count}")
    lines.extend(["", "## Cohort Breakdown", ""])
    for cohort, counts in summary["validation_cohort_combined_bucket_counts"].items():
        lines.append(f"- {cohort}: {json.dumps(counts, ensure_ascii=False)}")
    lines.extend(["", "## Publisher Breakdown", ""])
    for row in summary["publisher_breakdown"]:
        lines.append(
            "- "
            f"{row['publisher_display_name']} ({row['publisher_group']}): "
            f"sample={row['sample_total']}, "
            f"landing_success={row['landing_success']}, "
            f"publisher_native={row['publisher_native_download']}, "
            f"scihub={row['scihub_assisted_download']}, "
            f"unknown_success={row['download_success_unknown']}, "
            f"landing_success_no_download={row['landing_success_no_download']}, "
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

    download_rows: List[Dict[str, Any]] = []
    if args.download_results_csv and args.download_results_csv.exists():
        download_rows = load_csv_rows(args.download_results_csv.resolve())
    if not landing_rows and download_rows:
        landing_rows = [landing_record_from_download_row(row) for row in download_rows]
    landing_by_doi = {normalize_doi(row.get("doi")): row for row in landing_rows}
    download_by_doi = {normalize_doi(row.get("doi")): row for row in download_rows}

    landing_probe_bucket_counts = Counter()
    download_bucket_counts = Counter()
    combined_bucket_counts = Counter()
    validation_cohort_counts = Counter()
    validation_cohort_combined_bucket_counts: Dict[str, Counter] = defaultdict(Counter)
    retry_action_counts = Counter()
    merged_rows: List[Dict[str, Any]] = []
    blocked_items: List[str] = []

    if not landing_rows and not download_rows:
        blocked_items.append("landing_access_repro 결과 JSONL이 없어 landing-only 판단이 부분적입니다.")
    if not download_rows:
        blocked_items.append("parallel_download 결과 CSV가 없어 실다운로드 판단이 부분적입니다.")

    for doi, sample in sample_by_doi.items():
        landing = landing_by_doi.get(doi, {})
        download = download_by_doi.get(doi, {})
        experiment_publisher_group = resolve_publisher_group(sample)
        publisher_display_name = resolve_publisher_display_name(sample, experiment_publisher_group)
        landing_bucket = landing_bucket_from_record(landing) if landing else "missing"
        download_bucket = download_bucket_from_record(download)
        combined = combined_bucket(landing_bucket, download_bucket)
        validation_cohort = str(sample.get("validation_cohort") or "")
        retry_action = str(sample.get("retry_protection_action") or "")

        landing_probe_bucket_counts[landing_bucket] += 1
        download_bucket_counts[download_bucket] += 1
        combined_bucket_counts[combined] += 1
        validation_cohort_counts[validation_cohort or "unknown"] += 1
        validation_cohort_combined_bucket_counts[validation_cohort or "unknown"][combined] += 1
        retry_action_counts[retry_action or "unspecified"] += 1

        merged_rows.append(
            {
                "suite_name": sample.get("suite_name", args.suite),
                "experiment_publisher_group": experiment_publisher_group,
                "publisher_display_name": publisher_display_name,
                "selection_reason": sample.get("selection_reason", ""),
                "selection_bucket": sample.get("selection_bucket", ""),
                "suite_slot_bucket": sample.get("suite_slot_bucket", ""),
                "validation_cohort": sample.get("validation_cohort", ""),
                "scihub_confound_risk": sample.get("scihub_confound_risk", ""),
                "prior_attempt_state": sample.get("prior_attempt_state", ""),
                "prior_attempt_count": sample.get("prior_attempt_count", ""),
                "prior_success_count": sample.get("prior_success_count", ""),
                "prior_hard_block_count": sample.get("prior_hard_block_count", ""),
                "prior_last_combined_bucket": sample.get("prior_last_combined_bucket", ""),
                "retry_protection_action": sample.get("retry_protection_action", ""),
                "retry_protection_reason": sample.get("retry_protection_reason", ""),
                "source_open_access": sample.get("source_open_access", ""),
                "source_has_pdf_url": sample.get("source_has_pdf_url", ""),
                "doi": doi,
                "title": sample.get("title", ""),
                "publisher": sample.get("publisher", ""),
                "publication_year": sample.get("publication_year", ""),
                "landing_probe_bucket": landing_bucket,
                "landing_probe_state": landing.get("classifier_state", ""),
                "landing_probe_outcome": landing.get("outcome", ""),
                "landing_probe_reason_codes": json.dumps(list(landing.get("reason_codes") or []), ensure_ascii=False),
                "landing_probe_url": landing.get("resolved_url", ""),
                "landing_probe_session_source": landing.get("browser_session_source", ""),
                "landing_probe_session_reason": landing.get("browser_session_decision_reason", ""),
                "landing_probe_browser_user_data_dir": landing.get("browser_user_data_dir", ""),
                "landing_probe_browser_effective_user_data_dir": landing.get("browser_effective_user_data_dir", ""),
                "landing_probe_browser_debug_address": landing.get("browser_debug_address", ""),
                "landing_aip_first_contact_policy": landing.get("aip_first_contact_policy", ""),
                "landing_aip_low_pressure_first_contact": landing.get("aip_low_pressure_first_contact", ""),
                "landing_aip_direct_doi_path_used": landing.get("aip_direct_doi_path_used", ""),
                "landing_aip_alternate_pages_opened": landing.get("aip_alternate_pages_opened", ""),
                "landing_probe_page_mode_requested": landing.get("probe_page_mode_requested", ""),
                "landing_probe_page_mode_effective": landing.get("probe_page_mode_effective", ""),
                "landing_probe_open_attempts": landing.get("probe_open_attempts", ""),
                "landing_post_open_tab_trim_closed_count": landing.get("post_open_tab_trim_closed_count", ""),
                "landing_probe_open_succeeded": landing.get("probe_open_succeeded", ""),
                "landing_probe_attach_restart_reason": landing.get("probe_attach_restart_reason", ""),
                "landing_controller_page_reused": landing.get("controller_page_reused", ""),
                "landing_controller_reuse_allowed": landing.get("controller_reuse_allowed", ""),
                "landing_controller_restart_reason": landing.get("controller_restart_reason", ""),
                "landing_controller_restart_count": landing.get("controller_restart_count", ""),
                "landing_controller_create_attempts": landing.get("controller_create_attempts", ""),
                "landing_startup_tab_cleanup_applied": landing.get("startup_tab_cleanup_applied", ""),
                "landing_startup_tab_cleanup_before_count": landing.get("startup_tab_cleanup_before_count", ""),
                "landing_startup_tab_cleanup_after_count": landing.get("startup_tab_cleanup_after_count", ""),
                "landing_startup_tab_cleanup_closed_count": landing.get("startup_tab_cleanup_closed_count", ""),
                "landing_startup_page_reset_to_blank": landing.get("startup_page_reset_to_blank", ""),
                "landing_probe_browser_process_alive": landing.get("browser_process_alive", ""),
                "landing_probe_page_access_ok": landing.get("page_access_ok", ""),
                "landing_probe_page_probe_error": landing.get("page_probe_error", ""),
                "landing_final_active_tab_id": landing.get("final_active_tab_id", ""),
                "landing_final_total_tab_count": landing.get("final_total_tab_count", ""),
                "landing_peak_tab_count_observed": landing.get("peak_tab_count_observed", ""),
                "landing_reduced_tab_path_used": landing.get("reduced_tab_path_used", ""),
                "landing_tab_lifecycle_sequence": json.dumps(
                    list(landing.get("tab_lifecycle_sequence") or []), ensure_ascii=False
                ),
                "landing_page_disconnect_observed": landing.get("page_disconnect_observed", ""),
                "landing_page_disconnect_stage": landing.get("page_disconnect_stage", ""),
                "landing_network_listener_started": landing.get("network_listener_started", ""),
                "landing_network_listener_error": landing.get("network_listener_error", ""),
                "landing_runtime_probe_installed": landing.get("runtime_probe_installed", ""),
                "landing_runtime_probe_error": landing.get("runtime_probe_error", ""),
                "landing_challenge_detected": landing.get("challenge_detected", ""),
                "landing_entry_strategy": landing.get("entry_strategy", ""),
                "landing_entry_strategy_variant": landing.get("entry_strategy_variant", ""),
                "landing_entry_redirect_probe_mode": landing.get("entry_redirect_probe_mode", ""),
                "landing_entry_prebrowser_request_count": landing.get("entry_prebrowser_request_count", ""),
                "landing_entry_preanalysis_ran": landing.get("entry_preanalysis_ran", ""),
                "landing_entry_url": landing.get("entry_url", ""),
                "landing_entry_resolved_url": landing.get("entry_resolved_url", ""),
                "landing_entry_browser_url": landing.get("entry_browser_url", ""),
                "landing_entry_browser_kind": landing.get("entry_browser_kind", ""),
                "landing_entry_url_preference": landing.get("entry_url_preference", ""),
                "landing_entry_handoff_url": landing.get("entry_handoff_url", ""),
                "landing_entry_handoff_used": landing.get("entry_handoff_used", ""),
                "landing_entry_context_url": landing.get("entry_context_url", ""),
                "landing_entry_context_kind": landing.get("entry_context_kind", ""),
                "landing_entry_context_bootstrap_mode": landing.get("entry_context_bootstrap_mode", ""),
                "landing_entry_context_bootstrap_attempted": landing.get("entry_context_bootstrap_attempted", ""),
                "landing_entry_context_bootstrap_outcome": landing.get("entry_context_bootstrap_outcome", ""),
                "landing_entry_context_bootstrap_final_url": landing.get("entry_context_bootstrap_final_url", ""),
                "landing_entry_context_bootstrap_final_title": landing.get("entry_context_bootstrap_final_title", ""),
                "landing_entry_context_bootstrap_cache_hit": landing.get("entry_context_bootstrap_cache_hit", ""),
                "landing_entry_context_bootstrap_cache_state": landing.get("entry_context_bootstrap_cache_state", ""),
                "landing_entry_navigation_route": landing.get("entry_navigation_route", ""),
                "landing_entry_preflight_url": landing.get("entry_preflight_url", ""),
                "landing_entry_redirect_chain_summary": json.dumps(
                    list(landing.get("entry_redirect_chain_summary") or []), ensure_ascii=False
                ),
                "landing_entry_fallback_used": landing.get("entry_fallback_used", ""),
                "landing_entry_fallback_reason": landing.get("entry_fallback_reason", ""),
                "landing_entry_preflight_issue": landing.get("entry_preflight_issue", ""),
                "landing_entry_preflight_issue_overridden": landing.get("entry_preflight_issue_overridden", ""),
                "landing_entry_browser_open_skipped": landing.get("entry_browser_open_skipped", ""),
                "landing_initial_landing_type": landing.get("initial_landing_type", ""),
                "landing_recovery_attempted": landing.get("landing_recovery_attempted", ""),
                "landing_recovery_strategy": landing.get("landing_recovery_strategy", ""),
                "landing_recovery_outcome": landing.get("landing_recovery_outcome", ""),
                "landing_shell_recovery_attempted": landing.get("shell_recovery_attempted", ""),
                "landing_shell_recovery_strategy": landing.get("shell_recovery_strategy", ""),
                "landing_shell_recovery_outcome": landing.get("shell_recovery_outcome", ""),
                "landing_tab_transition_count": landing.get("tab_transition_count", ""),
                "landing_js_runtime_probe_ok": landing.get("js_runtime_probe_ok", ""),
                "landing_js_probe_error": landing.get("js_probe_error", ""),
                "landing_navigator_cookie_enabled": landing.get("navigator_cookie_enabled", ""),
                "landing_document_cookie_len": landing.get("document_cookie_len", ""),
                "landing_challenge_script_present": landing.get("challenge_script_present", ""),
                "landing_cf_chl_opt_present": landing.get("cf_chl_opt_present", ""),
                "landing_noscript_cookie_hint_present": landing.get("noscript_cookie_hint_present", ""),
                "landing_cookie_jar_probe_ok": landing.get("cookie_jar_probe_ok", ""),
                "landing_cookie_jar_count": landing.get("cookie_jar_count", ""),
                "landing_aip_cookie_count": landing.get("aip_cookie_count", ""),
                "landing_cloudflare_cookie_count": landing.get("cloudflare_cookie_count", ""),
                "landing_profile_cookie_db_exists": landing.get("profile_cookie_db_exists", ""),
                "landing_profile_cookie_db_writable": landing.get("profile_cookie_db_writable", ""),
                "landing_profile_storage_exists": landing.get("profile_storage_exists", ""),
                "landing_profile_preferences_exists": landing.get("profile_preferences_exists", ""),
                "landing_reclassified_after_detector_fix": landing.get("reclassified_after_detector_fix", ""),
                "landing_reclassification_reason": landing.get("reclassification_reason", ""),
                "download_status": download.get("download_status", ""),
                "download_method": download.get("download_method", ""),
                "download_source_category": download.get("download_source_category", ""),
                "download_result_reason": download.get("download_result_reason", ""),
                "download_result_stage": download.get("download_result_stage", ""),
                "download_result_domain": download.get("download_result_domain", ""),
                "download_landing_bucket": download.get("experiment_landing_bucket", ""),
                "download_bucket": download_bucket,
                "download_session_source": download.get("browser_session_source", ""),
                "download_session_mode": download.get("browser_session_mode", ""),
                "download_session_reason": download.get("browser_session_decision_reason", ""),
                "download_challenge_detected": download.get("landing_challenge_detected", ""),
                "download_default_page_detected": download.get("landing_default_page_detected", ""),
                "download_default_page_kind": download.get("landing_default_page_kind", ""),
                "download_tab_transition_count": download.get("landing_tab_transition_count", ""),
                "download_final_active_tab_id": download.get("landing_final_active_tab_id", ""),
                "download_final_total_tab_count": download.get("landing_final_total_tab_count", ""),
                "download_final_screenshot_path": download.get("landing_final_screenshot_path", ""),
                "download_final_html_path": download.get("landing_final_html_path", ""),
                "download_entry_strategy": download.get("landing_entry_strategy", ""),
                "download_entry_strategy_variant": download.get("landing_entry_strategy_variant", ""),
                "download_entry_redirect_probe_mode": download.get("landing_entry_redirect_probe_mode", ""),
                "download_entry_prebrowser_request_count": download.get("landing_entry_prebrowser_request_count", ""),
                "download_entry_url": download.get("landing_entry_url", ""),
                "download_entry_resolved_url": download.get("landing_entry_resolved_url", ""),
                "download_entry_browser_url": download.get("landing_entry_browser_url", ""),
                "download_entry_browser_kind": download.get("landing_entry_browser_kind", ""),
                "download_entry_context_url": download.get("landing_entry_context_url", ""),
                "download_entry_context_kind": download.get("landing_entry_context_kind", ""),
                "download_entry_context_bootstrap_mode": download.get("landing_entry_context_bootstrap_mode", ""),
                "download_entry_context_bootstrap_attempted": download.get("landing_entry_context_bootstrap_attempted", ""),
                "download_entry_context_bootstrap_outcome": download.get("landing_entry_context_bootstrap_outcome", ""),
                "download_entry_context_bootstrap_final_url": download.get("landing_entry_context_bootstrap_final_url", ""),
                "download_entry_context_bootstrap_final_title": download.get("landing_entry_context_bootstrap_final_title", ""),
                "download_entry_context_bootstrap_cache_hit": download.get("landing_entry_context_bootstrap_cache_hit", ""),
                "download_entry_context_bootstrap_cache_state": download.get("landing_entry_context_bootstrap_cache_state", ""),
                "download_entry_navigation_route": download.get("landing_entry_navigation_route", ""),
                "download_entry_preflight_url": download.get("landing_entry_preflight_url", ""),
                "download_entry_redirect_chain_summary": download.get("landing_entry_redirect_chain_summary", ""),
                "download_entry_fallback_used": download.get("landing_entry_fallback_used", ""),
                "download_entry_fallback_reason": download.get("landing_entry_fallback_reason", ""),
                "download_entry_preflight_issue": download.get("landing_entry_preflight_issue", ""),
                "download_entry_preflight_issue_overridden": download.get("landing_entry_preflight_issue_overridden", ""),
                "download_entry_browser_open_skipped": download.get("landing_entry_browser_open_skipped", ""),
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
            "publisher_native_download": 0,
            "scihub_assisted_download": 0,
            "download_success_unknown": 0,
            "landing_success_no_download": 0,
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
        if combined in SUCCESS_DOWNLOAD_BUCKETS:
            bucket[combined] += 1
        elif combined == "landing_success_no_download":
            bucket["landing_success_no_download"] += 1
        elif combined == "missing":
            bucket["missing_records"] += 1
        elif combined in FAILURE_BUCKETS:
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
    landing_report_summary = landing_report.get("summary", {})
    if not landing_report_summary and download_summary:
        landing_report_summary = dict(download_summary.get("integrated_landing", {}) or {})

    summary = {
        "suite": args.suite,
        "recent_year_floor": RECENT_YEAR_FLOOR,
        "sample_total": len(sample_rows),
        "landing_probe_records": len(landing_rows),
        "download_records": len(download_rows),
        "validation_cohort_counts": dict(sorted((k, int(v)) for k, v in validation_cohort_counts.items())),
        "retry_action_counts": dict(sorted((k, int(v)) for k, v in retry_action_counts.items())),
        "validation_cohort_combined_bucket_counts": {
            cohort: dict(sorted((bucket, int(count)) for bucket, count in counts.items()))
            for cohort, counts in sorted(validation_cohort_combined_bucket_counts.items())
        },
        "landing_probe_bucket_counts": {
            bucket: int(landing_probe_bucket_counts.get(bucket, 0)) for bucket in LANDING_BUCKET_ORDER
        },
        "download_bucket_counts": {
            bucket: int(download_bucket_counts.get(bucket, 0)) for bucket in DOWNLOAD_BUCKET_ORDER
        },
        "combined_bucket_counts": dict(sorted((key, int(value)) for key, value in combined_bucket_counts.items())),
        "publisher_breakdown": publisher_rows,
        "landing_report_summary": landing_report_summary,
        "download_report_summary": download_summary.get("experiment_outcomes", {}),
        "blocked_items": blocked_items,
    }

    merged_fields = [
        "suite_name",
        "experiment_publisher_group",
        "publisher_display_name",
        "selection_reason",
        "selection_bucket",
        "suite_slot_bucket",
        "validation_cohort",
        "scihub_confound_risk",
        "prior_attempt_state",
        "prior_attempt_count",
        "prior_success_count",
        "prior_hard_block_count",
        "prior_last_combined_bucket",
        "retry_protection_action",
        "retry_protection_reason",
        "source_open_access",
        "source_has_pdf_url",
        "doi",
        "title",
        "publisher",
        "publication_year",
        "landing_probe_bucket",
        "landing_probe_state",
        "landing_probe_outcome",
        "landing_probe_reason_codes",
        "landing_probe_url",
        "landing_probe_session_source",
        "landing_probe_session_reason",
        "landing_probe_browser_user_data_dir",
        "landing_probe_browser_effective_user_data_dir",
        "landing_probe_browser_debug_address",
        "landing_aip_first_contact_policy",
        "landing_aip_low_pressure_first_contact",
        "landing_aip_direct_doi_path_used",
        "landing_aip_alternate_pages_opened",
        "landing_probe_page_mode_requested",
        "landing_probe_page_mode_effective",
        "landing_probe_open_attempts",
        "landing_post_open_tab_trim_closed_count",
        "landing_probe_open_succeeded",
        "landing_probe_attach_restart_reason",
        "landing_controller_page_reused",
        "landing_controller_reuse_allowed",
        "landing_controller_restart_reason",
        "landing_controller_restart_count",
        "landing_controller_create_attempts",
        "landing_startup_tab_cleanup_applied",
        "landing_startup_tab_cleanup_before_count",
        "landing_startup_tab_cleanup_after_count",
        "landing_startup_tab_cleanup_closed_count",
        "landing_startup_page_reset_to_blank",
        "landing_probe_browser_process_alive",
        "landing_probe_page_access_ok",
        "landing_probe_page_probe_error",
        "landing_final_active_tab_id",
        "landing_final_total_tab_count",
        "landing_peak_tab_count_observed",
        "landing_reduced_tab_path_used",
        "landing_tab_lifecycle_sequence",
        "landing_page_disconnect_observed",
        "landing_page_disconnect_stage",
        "landing_network_listener_started",
        "landing_network_listener_error",
        "landing_runtime_probe_installed",
        "landing_runtime_probe_error",
        "landing_challenge_detected",
        "landing_entry_strategy",
        "landing_entry_strategy_variant",
        "landing_entry_redirect_probe_mode",
        "landing_entry_prebrowser_request_count",
        "landing_entry_preanalysis_ran",
        "landing_entry_url",
        "landing_entry_resolved_url",
        "landing_entry_browser_url",
        "landing_entry_browser_kind",
        "landing_entry_url_preference",
        "landing_entry_handoff_url",
        "landing_entry_handoff_used",
        "landing_entry_context_url",
        "landing_entry_context_kind",
        "landing_entry_context_bootstrap_mode",
        "landing_entry_context_bootstrap_attempted",
        "landing_entry_context_bootstrap_outcome",
        "landing_entry_context_bootstrap_final_url",
        "landing_entry_context_bootstrap_final_title",
        "landing_entry_context_bootstrap_cache_hit",
        "landing_entry_context_bootstrap_cache_state",
        "landing_entry_navigation_route",
        "landing_entry_preflight_url",
        "landing_entry_redirect_chain_summary",
        "landing_entry_fallback_used",
        "landing_entry_fallback_reason",
        "landing_entry_preflight_issue",
        "landing_entry_preflight_issue_overridden",
        "landing_entry_browser_open_skipped",
        "landing_initial_landing_type",
        "landing_recovery_attempted",
        "landing_recovery_strategy",
        "landing_recovery_outcome",
        "landing_shell_recovery_attempted",
        "landing_shell_recovery_strategy",
        "landing_shell_recovery_outcome",
        "landing_tab_transition_count",
        "landing_js_runtime_probe_ok",
        "landing_js_probe_error",
        "landing_navigator_cookie_enabled",
        "landing_document_cookie_len",
        "landing_challenge_script_present",
        "landing_cf_chl_opt_present",
        "landing_noscript_cookie_hint_present",
        "landing_cookie_jar_probe_ok",
        "landing_cookie_jar_count",
        "landing_aip_cookie_count",
        "landing_cloudflare_cookie_count",
        "landing_profile_cookie_db_exists",
        "landing_profile_cookie_db_writable",
        "landing_profile_storage_exists",
        "landing_profile_preferences_exists",
        "landing_reclassified_after_detector_fix",
        "landing_reclassification_reason",
        "download_status",
        "download_method",
        "download_source_category",
        "download_result_reason",
        "download_result_stage",
        "download_result_domain",
        "download_landing_bucket",
        "download_bucket",
        "download_session_source",
        "download_session_mode",
        "download_session_reason",
        "download_challenge_detected",
        "download_default_page_detected",
        "download_default_page_kind",
        "download_tab_transition_count",
        "download_final_active_tab_id",
        "download_final_total_tab_count",
        "download_final_screenshot_path",
        "download_final_html_path",
        "download_entry_strategy",
        "download_entry_strategy_variant",
        "download_entry_redirect_probe_mode",
        "download_entry_prebrowser_request_count",
        "download_entry_url",
        "download_entry_resolved_url",
        "download_entry_browser_url",
        "download_entry_browser_kind",
        "download_entry_context_url",
        "download_entry_context_kind",
        "download_entry_context_bootstrap_mode",
        "download_entry_context_bootstrap_attempted",
        "download_entry_context_bootstrap_outcome",
        "download_entry_context_bootstrap_final_url",
        "download_entry_context_bootstrap_final_title",
        "download_entry_context_bootstrap_cache_hit",
        "download_entry_context_bootstrap_cache_state",
        "download_entry_navigation_route",
        "download_entry_preflight_url",
        "download_entry_redirect_chain_summary",
        "download_entry_fallback_used",
        "download_entry_fallback_reason",
        "download_entry_preflight_issue",
        "download_entry_preflight_issue_overridden",
        "download_entry_browser_open_skipped",
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
        "publisher_native_download",
        "scihub_assisted_download",
        "download_success_unknown",
        "landing_success_no_download",
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
