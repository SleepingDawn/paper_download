from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from openalex_search import OPENALEX_MAILTO, extract_row, iter_openalex_works


SELECT_FIELDS = [
    "id",
    "ids",
    "doi",
    "title",
    "type",
    "publication_date",
    "publication_year",
    "cited_by_count",
    "primary_location",
    "locations",
    "authorships",
    "citation_normalized_percentile",
    "open_access",
]


def _build_filter(query: str, year: int) -> str:
    return f"type:article,publication_year:{int(year)},title_and_abstract.search:({query})"


def _ordered_columns(df: pd.DataFrame) -> List[str]:
    preferred = [
        "doi",
        "publisher",
        "pdf_url",
        "open_access",
        "title",
        "cited_by_count",
        "publication_year",
        "publication_date",
        "openalex_id",
        "benchmark_query",
        "benchmark_year",
        "benchmark_rank",
        "benchmark_source",
        "doi_resolution_method",
        "doi_resolution_confidence",
        "resolved_from_ssrn",
        "resolved_from_repository",
        "resolved_from_arxiv",
        "original_source_type",
        "original_openalex_id",
        "resolved_target_openalex_id",
    ]
    existing = [col for col in preferred if col in df.columns]
    tail = [col for col in df.columns if col not in existing]
    return existing + tail


def build_query_benchmark(query: str, year: int, limit: int, output_csv: Path) -> Path:
    filter_str = _build_filter(query=query, year=year)
    rows: List[Dict[str, Any]] = []
    for work in iter_openalex_works(
        filter_str=filter_str,
        sort="cited_by_count:desc",
        select_fields=SELECT_FIELDS,
        mailto=OPENALEX_MAILTO,
        max_records=max(int(limit), 1) * 3,
    ):
        row = extract_row(work)
        row["benchmark_query"] = query
        row["benchmark_year"] = int(year)
        row["benchmark_source"] = "openalex_title_and_abstract_cited_by_count_desc"
        rows.append(row)

    if not rows:
        raise RuntimeError(f"no OpenAlex results for query={query!r}, year={year}")

    df = pd.DataFrame(rows)
    if "doi" not in df.columns:
        raise RuntimeError("OpenAlex result rows do not contain a doi column")

    df = df.dropna(subset=["doi"]).copy()
    df["doi"] = df["doi"].astype(str).str.strip().str.lower()
    df = df[df["doi"] != ""].copy()
    df = df.drop_duplicates(subset=["doi"], keep="first").copy()
    df = df.sort_values(by=["cited_by_count", "doi"], ascending=[False, True], na_position="last").copy()
    df = df.head(int(limit)).copy()
    df["benchmark_rank"] = range(1, len(df) + 1)
    df = df[_ordered_columns(df)]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return output_csv


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an OpenAlex query benchmark CSV sorted by citation count.")
    parser.add_argument("--query", required=True, help="OpenAlex title_and_abstract query string")
    parser.add_argument("--year", type=int, required=True, help="Publication year filter")
    parser.add_argument("--limit", type=int, default=200, help="Maximum number of rows to keep")
    parser.add_argument("--output-csv", type=Path, required=True, help="Output CSV path")
    args = parser.parse_args()

    out = build_query_benchmark(
        query=str(args.query),
        year=int(args.year),
        limit=max(1, int(args.limit)),
        output_csv=args.output_csv.resolve(),
    )
    df = pd.read_csv(out)
    print(f"output_csv={out}")
    print(f"rows={len(df)}")
    print(f"query={args.query}")
    print(f"year={args.year}")
    if "cited_by_count" in df.columns and len(df):
        print(f"top_cited={int(df['cited_by_count'].fillna(0).max())}")
        print(f"bottom_cited={int(df['cited_by_count'].fillna(0).min())}")
    for _, row in df.head(10).iterrows():
        print(
            f"{int(row['benchmark_rank'])}. "
            f"{row.get('doi', '')} | "
            f"{int(row.get('cited_by_count', 0) or 0)} | "
            f"{row.get('publisher', '')} | "
            f"{row.get('title', '')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
