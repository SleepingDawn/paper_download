from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse


DOI_RE = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$")

GROUP_ORDER = [
    "acs",
    "elsevier",
    "cell",
    "wiley",
    "aip",
    "nature",
    "springer",
    "rsc",
    "iop",
    "mdpi",
    "ieee",
    "aps",
    "taylor_and_francis",
    "other",
]

GROUP_DISPLAY_NAMES = {
    "acs": "ACS",
    "elsevier": "Elsevier",
    "cell": "Cell-family",
    "wiley": "Wiley",
    "aip": "AIP/AVS",
    "nature": "Nature Portfolio",
    "springer": "Springer Link",
    "rsc": "RSC",
    "iop": "IOP",
    "mdpi": "MDPI",
    "ieee": "IEEE",
    "aps": "APS",
    "taylor_and_francis": "Taylor & Francis",
    "other": "Other",
}

SELECTION_BUCKET_ORDER = [
    "landing_closed",
    "landing_oa",
    "direct_pdf_oa",
    "direct_pdf_closed",
]

DEFAULT_BUCKET_SLOTS = [
    "landing_closed",
    "landing_oa",
    "direct_pdf_oa",
    "direct_pdf_closed",
]

RECENT_YEAR_FLOOR = 2024
VALIDATION_COHORT_RECENT = "recent_primary"
VALIDATION_COHORT_LEGACY = "legacy_fallback"
SCIHUB_CONFOUND_RISK_LOWER = "lower_recent"
SCIHUB_CONFOUND_RISK_HIGHER = "higher_legacy"
PRIOR_ATTEMPT_STATE_FRESH = "fresh"
PRIOR_ATTEMPT_STATE_REUSED = "reused"
RETRY_ACTION_FIRST_ATTEMPT = "first_attempt"
RETRY_ACTION_REPEATED_ATTEMPT = "repeated_attempt"
RETRY_ACTION_CONTROLLED_RETRY = "controlled_retry"
RETRY_ACTION_SKIPPED = "skipped_due_to_retry_protection"
ATTEMPT_SUCCESS_BUCKETS = {
    "publisher_native_download",
    "scihub_assisted_download",
    "download_success_unknown",
}
ATTEMPT_HARD_BLOCK_BUCKETS = {
    "challenge_or_interstitial",
    "access_rights",
}

PILOT_TARGETS = {
    "acs": 1,
    "elsevier": 1,
    "cell": 2,
    "wiley": 1,
    "aip": 1,
    "nature": 1,
    "springer": 1,
    "rsc": 2,
    "iop": 1,
    "mdpi": 1,
    "ieee": 1,
}

FULL_TARGETS = {
    "acs": 3,
    "elsevier": 3,
    "cell": 3,
    "wiley": 3,
    "aip": 3,
    "nature": 3,
    "springer": 2,
    "rsc": 3,
    "iop": 2,
    "mdpi": 2,
    "ieee": 2,
    "aps": 1,
    "taylor_and_francis": 1,
}

GROUP_BUCKET_SLOTS = {
    "elsevier": ["landing_closed", "landing_oa", "landing_closed", "direct_pdf_oa"],
    "cell": ["landing_oa", "direct_pdf_oa", "direct_pdf_oa", "landing_closed"],
    "mdpi": ["direct_pdf_oa", "landing_oa", "direct_pdf_oa", "landing_closed"],
    "rsc": ["landing_closed", "landing_oa", "direct_pdf_oa", "direct_pdf_closed"],
    "nature": ["landing_closed", "direct_pdf_oa", "landing_oa", "direct_pdf_closed"],
    "springer": ["landing_closed", "direct_pdf_oa", "landing_oa", "direct_pdf_closed"],
}

DOI_PREFIX_GROUPS = {
    "10.1016": "elsevier",
    "10.1021": "acs",
    "10.1038": "nature",
    "10.1039": "rsc",
    "10.1063": "aip",
    "10.1088": "iop",
    "10.1103": "aps",
    "10.1109": "ieee",
    "10.1002": "wiley",
    "10.1111": "wiley",
    "10.1116": "aip",
    "10.3390": "mdpi",
    "10.1080": "taylor_and_francis",
}

CELL_DOI_HINTS = (
    "j.cell",
    "j.cels",
    "j.cellrep",
    "j.stem",
    "j.medj",
    "j.molcel",
    "j.neuron",
    "j.immuni",
    "j.cmet",
    "j.ajhg",
    "j.chom",
    "j.xcrm",
    "j.xcrp",
    "j.isci",
    "j.crmethen",
    "j.joule",
    "j.chempr",
    "j.matt",
    "j.mattod",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_input_csv() -> Path:
    return repo_root() / "ready_to_download.csv"


def default_suite_dir() -> Path:
    return repo_root() / "experiment" / "linux_headless_suite"


def default_attempt_ledger_path() -> Path:
    return repo_root() / "outputs" / "linux_headless_suite_attempt_ledger.jsonl"


def parse_bool(value: Any) -> bool:
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "t"}


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except Exception:
        return default


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_doi(value: Any) -> str:
    raw = clean_text(value).lower()
    raw = raw.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return raw.strip()


def extract_domain(url: Any) -> str:
    try:
        return (urlparse(clean_text(url)).netloc or "").lower()
    except Exception:
        return ""


def has_pdf_url(value: Any) -> bool:
    raw = clean_text(value).lower()
    if not raw or raw in {"none", "nan"}:
        return False
    if raw.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")):
        return False
    return (
        raw.endswith(".pdf")
        or "/pdf" in raw
        or "pdf?" in raw
        or "pdfdirect" in raw
        or "articlepdf" in raw
        or "content/pdf" in raw
        or "doi/pdf" in raw
    )


def doi_prefix(doi: str) -> str:
    norm = normalize_doi(doi)
    if "/" not in norm:
        return ""
    return norm.split("/", 1)[0]


def is_cell_family_row(row: Dict[str, Any]) -> bool:
    doi = normalize_doi(row.get("doi"))
    publisher = clean_text(row.get("publisher")).lower()
    pdf_domain = extract_domain(row.get("pdf_url"))
    title = clean_text(row.get("title")).lower()
    if "cell press" in publisher:
        return True
    if any(token in doi for token in CELL_DOI_HINTS):
        return True
    if pdf_domain.endswith("cell.com") and any(token in title for token in ("cell", "joule", "chempr", "matter")):
        return True
    return False


def experiment_publisher_group(row: Dict[str, Any]) -> str:
    doi = normalize_doi(row.get("doi"))
    publisher = clean_text(row.get("publisher")).lower()
    pdf_domain = extract_domain(row.get("pdf_url"))
    prefix = doi_prefix(doi)

    if is_cell_family_row(row):
        return "cell"
    if "royal society of chemistry" in publisher or publisher == "rsc":
        return "rsc"
    if "american chemical society" in publisher or publisher == "acs" or re.search(r"\bacs\b", publisher):
        return "acs"
    if (
        "american institute of physics" in publisher
        or publisher == "aip"
        or prefix in {"10.1063", "10.1116"}
        or pdf_domain.endswith("aip.org")
        or pdf_domain.endswith("scitation.org")
    ):
        return "aip"
    if (
        "institute of physics" in publisher
        or "iop publishing" in publisher
        or publisher == "iop"
        or prefix == "10.1088"
        or pdf_domain.endswith("iop.org")
    ):
        return "iop"
    if (
        "institute of electrical and electronics engineers" in publisher
        or publisher == "ieee"
        or prefix == "10.1109"
        or pdf_domain.endswith("ieee.org")
    ):
        return "ieee"
    if (
        "american physical society" in publisher
        or publisher == "aps"
        or prefix == "10.1103"
        or pdf_domain.endswith("journals.aps.org")
    ):
        return "aps"
    if (
        "taylor & francis" in publisher
        or "taylor and francis" in publisher
        or prefix == "10.1080"
        or pdf_domain.endswith("tandfonline.com")
    ):
        return "taylor_and_francis"
    if (
        "multidisciplinary digital publishing institute" in publisher
        or publisher == "mdpi"
        or prefix == "10.3390"
        or pdf_domain.endswith("mdpi.com")
    ):
        return "mdpi"
    if (
        "advanced materials" in publisher
        or "wiley" in publisher
        or prefix in {"10.1002", "10.1111"}
        or pdf_domain.endswith("wiley.com")
    ):
        return "wiley"
    if pdf_domain.endswith("link.springer.com") or "springer science" in publisher:
        return "springer"
    if (
        "nature portfolio" in publisher
        or publisher == "nature"
        or "springer nature" in publisher
        or prefix == "10.1038"
        or pdf_domain.endswith("nature.com")
    ):
        return "nature"
    if (
        "elsevier" in publisher
        or prefix == "10.1016"
        or pdf_domain.endswith("sciencedirect.com")
        or pdf_domain.endswith("elsevier.com")
    ):
        return "elsevier"
    if prefix in DOI_PREFIX_GROUPS:
        return DOI_PREFIX_GROUPS[prefix]
    return "other"


def selection_bucket(row: Dict[str, Any]) -> str:
    direct_pdf = bool(row.get("source_has_pdf_url"))
    open_access = bool(row.get("source_open_access"))
    if direct_pdf and open_access:
        return "direct_pdf_oa"
    if direct_pdf and not open_access:
        return "direct_pdf_closed"
    if open_access:
        return "landing_oa"
    return "landing_closed"


def validation_cohort_for_year(year: int) -> str:
    if int(year or 0) >= RECENT_YEAR_FLOOR:
        return VALIDATION_COHORT_RECENT
    return VALIDATION_COHORT_LEGACY


def scihub_confound_risk_for_year(year: int) -> str:
    if int(year or 0) >= RECENT_YEAR_FLOOR:
        return SCIHUB_CONFOUND_RISK_LOWER
    return SCIHUB_CONFOUND_RISK_HIGHER


def parse_timestamp(value: Any) -> datetime | None:
    raw = clean_text(value)
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def attempt_root_cause_family(combined_bucket: Any) -> str:
    bucket = clean_text(combined_bucket)
    if bucket in ATTEMPT_SUCCESS_BUCKETS:
        return "success"
    if bucket == "landing_success_no_download":
        return "post_landing_download_failure"
    if bucket == "challenge_or_interstitial":
        return "challenge_or_interstitial"
    if bucket == "blank_or_incomplete":
        return "blank_or_incomplete"
    if bucket == "timeout_or_error":
        return "timeout_or_error"
    if bucket == "environment_or_config_failure":
        return "environment_or_config_failure"
    if bucket == "access_rights":
        return "access_rights"
    if bucket == "doi_not_found":
        return "doi_not_found"
    if bucket == "missing":
        return "missing"
    return "other_non_success"


def load_attempt_ledger(path: Path | None) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def build_attempt_index(entries: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        doi = normalize_doi(entry.get("doi"))
        if not doi:
            continue
        bucket = clean_text(entry.get("combined_bucket"))
        attempted_at = parse_timestamp(entry.get("attempted_at"))
        aggregate = index.setdefault(
            doi,
            {
                "doi": doi,
                "attempt_count": 0,
                "success_count": 0,
                "hard_block_count": 0,
                "publisher_group": clean_text(entry.get("experiment_publisher_group")),
                "publication_year": parse_int(entry.get("publication_year")),
                "last_attempted_at": "",
                "last_combined_bucket": "",
                "last_root_cause_family": "",
                "last_run_id": "",
            },
        )
        aggregate["attempt_count"] += 1
        if bucket in ATTEMPT_SUCCESS_BUCKETS:
            aggregate["success_count"] += 1
        if bucket in ATTEMPT_HARD_BLOCK_BUCKETS:
            aggregate["hard_block_count"] += 1
        root_cause_family = attempt_root_cause_family(bucket)
        last_attempted_at = parse_timestamp(aggregate.get("last_attempted_at"))
        if attempted_at and (last_attempted_at is None or attempted_at >= last_attempted_at):
            aggregate["last_attempted_at"] = attempted_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            aggregate["last_combined_bucket"] = bucket
            aggregate["last_root_cause_family"] = root_cause_family
            aggregate["last_run_id"] = clean_text(entry.get("run_id"))
    return index


def prior_attempt_summary(row: Dict[str, Any], attempt_index: Dict[str, Dict[str, Any]] | None) -> Dict[str, Any]:
    prior = dict((attempt_index or {}).get(normalize_doi(row.get("doi")), {}))
    attempt_count = parse_int(prior.get("attempt_count"))
    success_count = parse_int(prior.get("success_count"))
    hard_block_count = parse_int(prior.get("hard_block_count"))
    return {
        "prior_attempt_state": PRIOR_ATTEMPT_STATE_FRESH if attempt_count <= 0 else PRIOR_ATTEMPT_STATE_REUSED,
        "prior_attempt_count": attempt_count,
        "prior_success_count": success_count,
        "prior_hard_block_count": hard_block_count,
        "prior_last_combined_bucket": clean_text(prior.get("last_combined_bucket")),
        "prior_last_root_cause_family": clean_text(prior.get("last_root_cause_family")),
        "prior_last_attempted_at": clean_text(prior.get("last_attempted_at")),
        "prior_last_run_id": clean_text(prior.get("last_run_id")),
    }


def annotate_row_with_attempt_history(
    row: Dict[str, Any],
    attempt_index: Dict[str, Dict[str, Any]] | None,
    *,
    retry_action: str = "",
    retry_reason: str = "",
) -> Dict[str, Any]:
    enriched = dict(row)
    enriched.update(prior_attempt_summary(enriched, attempt_index))
    enriched["retry_protection_action"] = clean_text(retry_action)
    enriched["retry_protection_reason"] = clean_text(retry_reason)
    return enriched


def candidate_sort_key(
    row: Dict[str, Any],
    attempt_index: Dict[str, Dict[str, Any]] | None = None,
) -> tuple[Any, ...]:
    bucket = clean_text(row.get("selection_bucket"))
    try:
        bucket_rank = SELECTION_BUCKET_ORDER.index(bucket)
    except ValueError:
        bucket_rank = len(SELECTION_BUCKET_ORDER)
    publication_year = parse_int(row.get("publication_year"))
    recent_rank = 0 if publication_year >= RECENT_YEAR_FLOOR else 1
    prior = prior_attempt_summary(row, attempt_index)
    return (
        parse_int(prior.get("prior_attempt_count")),
        1 if parse_int(prior.get("prior_hard_block_count")) > 0 else 0,
        1 if parse_int(prior.get("prior_success_count")) > 0 else 0,
        recent_rank,
        bucket_rank,
        -publication_year,
        -parse_int(row.get("cited_by_count")),
        normalize_doi(row.get("doi")),
    )


def load_source_rows(csv_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            doi = normalize_doi(raw_row.get("doi"))
            if not doi or doi in seen or not DOI_RE.match(doi):
                continue
            seen.add(doi)
            row = dict(raw_row)
            row["doi"] = doi
            row["source_open_access"] = parse_bool(raw_row.get("open_access"))
            row["source_has_pdf_url"] = has_pdf_url(raw_row.get("pdf_url"))
            row["experiment_publisher_group"] = experiment_publisher_group(raw_row)
            row["selection_bucket"] = selection_bucket(row)
            publication_year = parse_int(raw_row.get("publication_year"))
            row["validation_cohort"] = validation_cohort_for_year(publication_year)
            row["scihub_confound_risk"] = scihub_confound_risk_for_year(publication_year)
            row["publisher_display_name"] = GROUP_DISPLAY_NAMES.get(
                row["experiment_publisher_group"], row["experiment_publisher_group"]
            )
            rows.append(row)
    return rows


def summarize_group_distribution(rows: Sequence[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    counter = Counter(clean_text(row.get(key)) or "(blank)" for row in rows)
    return [
        {"key": name, "count": int(count)}
        for name, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def preferred_bucket_slots(group: str, target_count: int) -> List[str]:
    base = list(GROUP_BUCKET_SLOTS.get(group, DEFAULT_BUCKET_SLOTS))
    while len(base) < target_count:
        base.extend(DEFAULT_BUCKET_SLOTS)
    return base[:target_count]


def _find_candidate(
    candidates: Sequence[Dict[str, Any]],
    chosen_dois: set[str],
    *,
    preferred_bucket: str | None,
    validation_cohort: str | None,
) -> Dict[str, Any] | None:
    for candidate in candidates:
        doi = normalize_doi(candidate.get("doi"))
        if doi in chosen_dois:
            continue
        if preferred_bucket and clean_text(candidate.get("selection_bucket")) != preferred_bucket:
            continue
        if validation_cohort and clean_text(candidate.get("validation_cohort")) != validation_cohort:
            continue
        return candidate
    return None


def _selection_reason(
    candidate: Dict[str, Any],
    *,
    preferred_bucket: str,
    matched_bucket: bool,
    matched_recent: bool,
) -> str:
    selected_bucket = clean_text(candidate.get("selection_bucket"))
    if matched_recent and matched_bucket:
        return f"preferred_recent:{preferred_bucket}"
    if matched_recent:
        return f"fill_recent:{selected_bucket}"
    if matched_bucket:
        return f"fallback_legacy:{preferred_bucket}"
    return f"fill_legacy:{selected_bucket}"


def build_suite_selection(
    rows: Sequence[Dict[str, Any]],
    suite_name: str,
    targets: Dict[str, int],
    *,
    attempt_index: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("experiment_publisher_group") or "other")].append(dict(row))

    for group_rows in grouped.values():
        group_rows.sort(key=lambda row: candidate_sort_key(row, attempt_index=attempt_index))

    selected: List[Dict[str, Any]] = []
    gaps: List[Dict[str, Any]] = []

    for group in GROUP_ORDER:
        target_count = int(targets.get(group, 0) or 0)
        if target_count <= 0:
            continue
        candidates = list(grouped.get(group, []))
        preferred_slots = preferred_bucket_slots(group, target_count)
        chosen: List[Dict[str, Any]] = []
        chosen_dois = set()

        for slot_idx, bucket_name in enumerate(preferred_slots, start=1):
            candidate = (
                _find_candidate(
                    candidates,
                    chosen_dois,
                    preferred_bucket=bucket_name,
                    validation_cohort=VALIDATION_COHORT_RECENT,
                )
                or _find_candidate(
                    candidates,
                    chosen_dois,
                    preferred_bucket=None,
                    validation_cohort=VALIDATION_COHORT_RECENT,
                )
                or _find_candidate(
                    candidates,
                    chosen_dois,
                    preferred_bucket=bucket_name,
                    validation_cohort=None,
                )
                or _find_candidate(
                    candidates,
                    chosen_dois,
                    preferred_bucket=None,
                    validation_cohort=None,
                )
            )
            if not candidate:
                continue
            doi = normalize_doi(candidate.get("doi"))
            enriched = annotate_row_with_attempt_history(candidate, attempt_index)
            enriched["suite_name"] = suite_name
            enriched["suite_slot_index"] = slot_idx
            enriched["suite_slot_bucket"] = bucket_name
            enriched["selection_reason"] = _selection_reason(
                candidate,
                preferred_bucket=bucket_name,
                matched_bucket=clean_text(candidate.get("selection_bucket")) == bucket_name,
                matched_recent=clean_text(candidate.get("validation_cohort")) == VALIDATION_COHORT_RECENT,
            )
            chosen.append(enriched)
            chosen_dois.add(doi)

        if len(chosen) < target_count:
            for cohort in (VALIDATION_COHORT_RECENT, VALIDATION_COHORT_LEGACY):
                for candidate in candidates:
                    doi = normalize_doi(candidate.get("doi"))
                    if doi in chosen_dois:
                        continue
                    if clean_text(candidate.get("validation_cohort")) != cohort:
                        continue
                    enriched = annotate_row_with_attempt_history(candidate, attempt_index)
                    enriched["suite_name"] = suite_name
                    enriched["suite_slot_index"] = len(chosen) + 1
                    enriched["suite_slot_bucket"] = clean_text(candidate.get("selection_bucket"))
                    if cohort == VALIDATION_COHORT_RECENT:
                        enriched["selection_reason"] = f"fill_recent:{clean_text(candidate.get('selection_bucket'))}"
                    else:
                        enriched["selection_reason"] = f"fill_legacy:{clean_text(candidate.get('selection_bucket'))}"
                    chosen.append(enriched)
                    chosen_dois.add(doi)
                    if len(chosen) >= target_count:
                        break
                if len(chosen) >= target_count:
                    break

        selected.extend(chosen)
        if len(chosen) < target_count:
            gaps.append(
                {
                    "suite": suite_name,
                    "publisher_group": group,
                    "publisher_display_name": GROUP_DISPLAY_NAMES.get(group, group),
                    "requested": target_count,
                    "selected": len(chosen),
                    "available": len(candidates),
                }
            )

    selected.sort(
        key=lambda row: (
            GROUP_ORDER.index(str(row.get("experiment_publisher_group") or "other"))
            if str(row.get("experiment_publisher_group") or "other") in GROUP_ORDER
            else len(GROUP_ORDER),
            int(row.get("suite_slot_index", 0) or 0),
            normalize_doi(row.get("doi")),
        )
    )
    return {
        "suite_name": suite_name,
        "targets": dict(targets),
        "selected_rows": selected,
        "gaps": gaps,
        "selected_counts": summarize_group_distribution(selected, "experiment_publisher_group"),
        "validation_cohort_counts": summarize_group_distribution(selected, "validation_cohort"),
        "prior_attempt_state_counts": summarize_group_distribution(selected, "prior_attempt_state"),
    }


def manifest_payload(
    source_csv: Path,
    rows: Sequence[Dict[str, Any]],
    suites: Sequence[Dict[str, Any]],
    *,
    attempt_ledger_path: Path | None = None,
    attempt_ledger_entries: Sequence[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    suites_by_name = {str(item.get("suite_name")): item for item in suites}
    ledger_entries = list(attempt_ledger_entries or [])
    ledger_unique_dois = len({normalize_doi(entry.get("doi")) for entry in ledger_entries if normalize_doi(entry.get("doi"))})
    return {
        "source_csv": str(source_csv),
        "source_total": int(len(rows)),
        "linux_runtime_defaults": {
            "runtime_preset": "linux_cli_seeded",
            "execution_env": "linux_server",
            "headless": True,
        },
        "recent_year_floor": RECENT_YEAR_FLOOR,
        "attempt_ledger": {
            "path": str(attempt_ledger_path) if attempt_ledger_path is not None else "",
            "entry_total": len(ledger_entries),
            "unique_doi_total": ledger_unique_dois,
        },
        "publisher_distribution_raw": summarize_group_distribution(rows, "publisher"),
        "publisher_distribution_grouped": summarize_group_distribution(rows, "experiment_publisher_group"),
        "validation_cohort_distribution": summarize_group_distribution(rows, "validation_cohort"),
        "selection_bucket_distribution": summarize_group_distribution(rows, "selection_bucket"),
        "suites": {
            name: {
                "targets": suite.get("targets", {}),
                "selected_total": len(suite.get("selected_rows", [])),
                "selected_counts": suite.get("selected_counts", []),
                "validation_cohort_counts": suite.get("validation_cohort_counts", []),
                "prior_attempt_state_counts": suite.get("prior_attempt_state_counts", []),
                "gaps": suite.get("gaps", []),
                "explicit_coverage": ["rsc", "cell"],
            }
            for name, suite in suites_by_name.items()
        },
    }


def sample_csv_fieldnames() -> List[str]:
    return [
        "suite_name",
        "experiment_publisher_group",
        "publisher_display_name",
        "suite_slot_index",
        "suite_slot_bucket",
        "selection_reason",
        "selection_bucket",
        "validation_cohort",
        "scihub_confound_risk",
        "prior_attempt_state",
        "prior_attempt_count",
        "prior_success_count",
        "prior_hard_block_count",
        "prior_last_combined_bucket",
        "prior_last_root_cause_family",
        "prior_last_attempted_at",
        "prior_last_run_id",
        "retry_protection_action",
        "retry_protection_reason",
        "source_open_access",
        "source_has_pdf_url",
        "doi",
        "title",
        "publisher",
        "publication_year",
        "cited_by_count",
        "pdf_url",
        "open_access",
        "download_status",
    ]


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sample_csv_fieldnames()
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def relative_to_repo(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root()))
    except Exception:
        return str(path.resolve())


def apply_retry_protection(
    rows: Sequence[Dict[str, Any]],
    attempt_index: Dict[str, Dict[str, Any]] | None,
    *,
    max_attempts_per_doi: int,
    cooldown_hours: int,
    allow_success_reruns: bool,
    allow_hard_block_reruns: bool,
    allow_repeated_attempts: bool,
) -> Dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    allowed_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    action_counts = Counter()
    skip_reason_counts = Counter()

    for row in rows:
        prior = prior_attempt_summary(row, attempt_index)
        prior_attempt_count = parse_int(prior.get("prior_attempt_count"))
        prior_success_count = parse_int(prior.get("prior_success_count"))
        prior_hard_block_count = parse_int(prior.get("prior_hard_block_count"))
        last_attempted_at = parse_timestamp(prior.get("prior_last_attempted_at"))
        cooldown_active = False
        if cooldown_hours > 0 and last_attempted_at is not None:
            cooldown_active = (now_utc - last_attempted_at.astimezone(timezone.utc)) < timedelta(hours=cooldown_hours)

        controlled_retry = False
        controlled_reason = ""
        if prior_success_count > 0 and allow_success_reruns:
            controlled_retry = True
            controlled_reason = "allow_success_reruns"
        elif prior_hard_block_count > 0 and allow_hard_block_reruns:
            controlled_retry = True
            controlled_reason = "allow_hard_block_reruns"
        elif prior_attempt_count > 0 and allow_repeated_attempts:
            controlled_retry = True
            controlled_reason = "allow_repeated_attempts"

        action = RETRY_ACTION_FIRST_ATTEMPT
        reason = "unseen_doi"
        keep_row = True
        if prior_attempt_count > 0:
            action = RETRY_ACTION_REPEATED_ATTEMPT
            reason = "below_retry_threshold"
            if prior_success_count > 0 and not allow_success_reruns:
                action = RETRY_ACTION_SKIPPED
                reason = "prior_success_exists"
                keep_row = False
            elif prior_hard_block_count > 0 and not allow_hard_block_reruns:
                action = RETRY_ACTION_SKIPPED
                reason = "prior_hard_block_exists"
                keep_row = False
            elif max_attempts_per_doi > 0 and prior_attempt_count >= max_attempts_per_doi and not controlled_retry:
                action = RETRY_ACTION_SKIPPED
                reason = "max_attempts_reached"
                keep_row = False
            elif cooldown_active and not controlled_retry:
                action = RETRY_ACTION_SKIPPED
                reason = "cooldown_active"
                keep_row = False
            elif controlled_retry:
                action = RETRY_ACTION_CONTROLLED_RETRY
                reason = controlled_reason

        enriched = annotate_row_with_attempt_history(
            row,
            attempt_index,
            retry_action=action,
            retry_reason=reason,
        )
        action_counts[action] += 1
        if action == RETRY_ACTION_SKIPPED:
            skip_reason_counts[reason] += 1
            skipped_rows.append(enriched)
        elif keep_row:
            allowed_rows.append(enriched)
        else:
            skipped_rows.append(enriched)

    return {
        "allowed_rows": allowed_rows,
        "skipped_rows": skipped_rows,
        "action_counts": dict(sorted((key, int(value)) for key, value in action_counts.items())),
        "skip_reason_counts": dict(sorted((key, int(value)) for key, value in skip_reason_counts.items())),
    }


def append_attempt_ledger(
    path: Path | None,
    merged_rows: Sequence[Dict[str, Any]],
    *,
    run_id: str,
    suite_name: str,
    attempted_at: str,
) -> Dict[str, Any]:
    if path is None:
        return {"ok": False, "reason": "attempt_ledger_disabled"}
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_keys = set()
    if path.exists():
        with path.open("r", encoding="utf-8") as existing_handle:
            for line in existing_handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                existing_keys.add((clean_text(payload.get("run_id")), normalize_doi(payload.get("doi"))))

    appended = 0
    duplicate_skips = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in merged_rows:
            doi = normalize_doi(row.get("doi"))
            if not doi:
                continue
            dedup_key = (run_id, doi)
            if dedup_key in existing_keys:
                duplicate_skips += 1
                continue
            existing_keys.add(dedup_key)
            combined_bucket = clean_text(row.get("combined_bucket"))
            payload = {
                "run_id": run_id,
                "suite_name": suite_name,
                "attempted_at": attempted_at,
                "doi": doi,
                "experiment_publisher_group": clean_text(row.get("experiment_publisher_group")),
                "publisher_display_name": clean_text(row.get("publisher_display_name")),
                "publication_year": parse_int(row.get("publication_year")),
                "validation_cohort": clean_text(row.get("validation_cohort")),
                "selection_reason": clean_text(row.get("selection_reason")),
                "prior_attempt_count": parse_int(row.get("prior_attempt_count")),
                "retry_protection_action": clean_text(row.get("retry_protection_action")),
                "retry_protection_reason": clean_text(row.get("retry_protection_reason")),
                "landing_probe_bucket": clean_text(row.get("landing_probe_bucket")),
                "download_bucket": clean_text(row.get("download_bucket")),
                "combined_bucket": combined_bucket,
                "download_method": clean_text(row.get("download_method")),
                "download_source_category": clean_text(row.get("download_source_category")),
                "landing_probe_session_reason": clean_text(row.get("landing_probe_session_reason")),
                "download_session_reason": clean_text(row.get("download_session_reason")),
                "root_cause_family": attempt_root_cause_family(combined_bucket),
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            appended += 1
    return {
        "ok": True,
        "path": str(path),
        "appended_rows": appended,
        "duplicate_skips": duplicate_skips,
    }
