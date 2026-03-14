from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
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


def candidate_sort_key(row: Dict[str, Any]) -> tuple[Any, ...]:
    bucket = clean_text(row.get("selection_bucket"))
    try:
        bucket_rank = SELECTION_BUCKET_ORDER.index(bucket)
    except ValueError:
        bucket_rank = len(SELECTION_BUCKET_ORDER)
    publication_year = parse_int(row.get("publication_year"))
    recent_rank = 0 if publication_year >= RECENT_YEAR_FLOOR else 1
    return (
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
) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("experiment_publisher_group") or "other")].append(dict(row))

    for group_rows in grouped.values():
        group_rows.sort(key=candidate_sort_key)

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
            enriched = dict(candidate)
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
                    enriched = dict(candidate)
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
    }


def manifest_payload(
    source_csv: Path,
    rows: Sequence[Dict[str, Any]],
    suites: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    suites_by_name = {str(item.get("suite_name")): item for item in suites}
    return {
        "source_csv": str(source_csv),
        "source_total": int(len(rows)),
        "linux_runtime_defaults": {
            "runtime_preset": "linux_cli_seeded",
            "execution_env": "linux_server",
            "headless": True,
        },
        "recent_year_floor": RECENT_YEAR_FLOOR,
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
                "gaps": suite.get("gaps", []),
                "explicit_coverage": ["rsc", "cell"],
            }
            for name, suite in suites_by_name.items()
        },
    }


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "suite_name",
        "experiment_publisher_group",
        "publisher_display_name",
        "suite_slot_index",
        "suite_slot_bucket",
        "selection_reason",
        "selection_bucket",
        "validation_cohort",
        "scihub_confound_risk",
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
