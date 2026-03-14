import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Iterable, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from landing_classifier import estimate_publisher_key
from tools_exp import normalize_publisher_label


DEFAULT_PUBLISHERS = [
    "elsevier",
    "acs",
    "wiley",
    "aip",
    "iop",
    "ieee",
    "spie",
    "springer",
    "nature",
    "rsc",
    "mdpi",
    "aps",
]

PUBLISHER_CASE_HINTS: Dict[str, str] = {
    "elsevier": "landing_challenge",
    "acs": "landing_challenge",
    "aip": "landing_challenge",
    "iop": "landing_challenge",
    "ieee": "viewer_wrapper",
    "spie": "asset_gate",
    "wiley": "cookie_or_viewer_gate",
    "springer": "direct_pdf_control",
    "nature": "direct_pdf_control",
    "rsc": "direct_pdf_control",
    "mdpi": "direct_pdf_control",
    "aps": "direct_pdf_control",
}

PREFIX_PUBLISHER_MAP = {
    "10.1016": "elsevier",
    "10.1021": "acs",
    "10.1002": "wiley",
    "10.1063": "aip",
    "10.1116": "aip",
    "10.1088": "iop",
    "10.1149": "iop",
    "10.7567": "iop",
    "10.1109": "ieee",
    "10.1117": "spie",
    "10.1007": "springer",
    "10.1038": "nature",
    "10.1039": "rsc",
    "10.3390": "mdpi",
    "10.1103": "aps",
}

PUBLISHER_PREFIX_ALLOWLIST = {
    "elsevier": ("10.1016",),
    "acs": ("10.1021",),
    "wiley": ("10.1002",),
    "aip": ("10.1063", "10.1116"),
    "iop": ("10.1088", "10.1149", "10.7567"),
    "ieee": ("10.1109",),
    "spie": ("10.1117",),
    "springer": ("10.1007",),
    "nature": ("10.1038",),
    "rsc": ("10.1039",),
    "mdpi": ("10.3390",),
    "aps": ("10.1103",),
}


def _parse_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _parse_int(value: str) -> int:
    try:
        return int(float(str(value or "").strip()))
    except Exception:
        return 0


def _row_score(row: Dict[str, str]) -> tuple:
    return (
        _parse_int(row.get("publication_year", "")),
        _parse_int(row.get("cited_by_count", "")),
        1 if str(row.get("pdf_url", "") or "").strip() else 0,
        1 if str(row.get("title", "") or "").strip() else 0,
    )


def _benchmark_publisher_key(row: Dict[str, str]) -> str:
    doi = str(row.get("doi", "") or "").strip()
    raw_publisher = str(row.get("publisher", "") or "").strip()
    pdf_url = str(row.get("pdf_url", "") or "").strip()
    for prefix, key in PREFIX_PUBLISHER_MAP.items():
        if doi.lower().startswith(prefix):
            return key
    normalized = normalize_publisher_label(raw_publisher, prefix=doi)
    if normalized:
        norm_key = str(normalized).strip().lower()
        if norm_key == "elsevier":
            return "elsevier"
        if norm_key == "springer":
            return "springer"
        if norm_key == "nature":
            return "nature"
        if norm_key == "wiley":
            return "wiley"
        if norm_key == "acs":
            return "acs"
        if norm_key == "aip":
            return "aip"
        if norm_key == "iop":
            return "iop"
        if norm_key == "ieee":
            return "ieee"
        if norm_key == "spie":
            return "spie"
        if norm_key == "rsc":
            return "rsc"
        if norm_key == "mdpi":
            return "mdpi"
    estimated = estimate_publisher_key(doi, input_publisher=raw_publisher, pdf_url=pdf_url)
    return str(estimated or "").strip().lower()


def _publisher_key_allowed_for_doi(publisher_key: str, doi: str) -> bool:
    key = str(publisher_key or "").strip().lower()
    doi_norm = str(doi or "").strip().lower()
    allowlist = PUBLISHER_PREFIX_ALLOWLIST.get(key)
    if not allowlist:
        return True
    return any(doi_norm.startswith(prefix) for prefix in allowlist)


def _dedupe_rows(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    deduped: List[Dict[str, str]] = []
    for row in rows:
        doi = str(row.get("doi", "") or "").strip().lower()
        if not doi or doi in seen:
            continue
        seen.add(doi)
        deduped.append(row)
    return deduped


def _select_group_rows(rows: List[Dict[str, str]], per_publisher: int) -> List[Dict[str, str]]:
    ordered = sorted(rows, key=_row_score, reverse=True)
    selected: List[Dict[str, str]] = []
    chosen = set()

    def take_first(predicate) -> None:
        for row in ordered:
            doi = str(row.get("doi", "") or "").strip().lower()
            if not doi or doi in chosen:
                continue
            if predicate(row):
                selected.append(row)
                chosen.add(doi)
                return

    take_first(lambda row: not _parse_bool(row.get("open_access", "")))
    take_first(lambda row: _parse_bool(row.get("open_access", "")) and bool(str(row.get("pdf_url", "") or "").strip()))
    take_first(lambda row: _parse_bool(row.get("open_access", "")) and not bool(str(row.get("pdf_url", "") or "").strip()))

    for row in ordered:
        doi = str(row.get("doi", "") or "").strip().lower()
        if not doi or doi in chosen:
            continue
        selected.append(row)
        chosen.add(doi)
        if len(selected) >= per_publisher:
            break

    return selected[:per_publisher]


def build_benchmark_rows(
    source_rows: List[Dict[str, str]],
    publishers: List[str],
    per_publisher: int,
) -> List[Dict[str, str]]:
    grouped: Dict[str, List[Dict[str, str]]] = {publisher: [] for publisher in publishers}
    for raw_row in source_rows:
        row = dict(raw_row)
        doi = str(row.get("doi", "") or "").strip()
        if not doi:
            continue
        scheduler_publisher = _benchmark_publisher_key(row)
        if not _publisher_key_allowed_for_doi(scheduler_publisher, doi):
            continue
        row["scheduler_publisher"] = scheduler_publisher
        if scheduler_publisher in grouped:
            grouped[scheduler_publisher].append(row)

    selected_rows: List[Dict[str, str]] = []
    for publisher in publishers:
        picked = _select_group_rows(_dedupe_rows(grouped.get(publisher, [])), per_publisher=per_publisher)
        for idx, row in enumerate(picked, start=1):
            enriched = dict(row)
            enriched["benchmark_group"] = publisher
            enriched["benchmark_case_hint"] = PUBLISHER_CASE_HINTS.get(publisher, "mixed")
            enriched["benchmark_rank_within_publisher"] = str(idx)
            selected_rows.append(enriched)

    return selected_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a balanced publisher triage benchmark CSV.")
    parser.add_argument("--input", default="ready_to_download.csv")
    parser.add_argument("--output", default="experiment/publisher_triage_benchmark_20260313.csv")
    parser.add_argument("--per-publisher", type=int, default=3)
    parser.add_argument("--publishers", default=",".join(DEFAULT_PUBLISHERS))
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    publishers = [item.strip().lower() for item in str(args.publishers or "").split(",") if item.strip()]
    per_publisher = max(1, int(args.per_publisher))

    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    benchmark_rows = build_benchmark_rows(rows, publishers=publishers, per_publisher=per_publisher)
    if not benchmark_rows:
        raise SystemExit("no benchmark rows selected")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(benchmark_rows[0].keys())
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(benchmark_rows)

    print(f"wrote {len(benchmark_rows)} rows to {output_path}")
    for publisher in publishers:
        count = sum(1 for row in benchmark_rows if row.get("benchmark_group") == publisher)
        print(f"{publisher}: {count}")


if __name__ == "__main__":
    main()
