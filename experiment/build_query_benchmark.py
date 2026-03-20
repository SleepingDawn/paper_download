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


def _build_filter(query: str, year: int | None = None, min_year: int | None = None, max_year: int | None = None) -> str:
    filters = ["type:article", f"title_and_abstract.search:({query})"]
    if year is not None:
        filters.append(f"publication_year:{int(year)}")
    else:
        if min_year is not None:
            filters.append(f"from_publication_date:{int(min_year)}-01-01")
        if max_year is not None:
            filters.append(f"to_publication_date:{int(max_year)}-12-31")
    return ",".join(filters)


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
        "benchmark_min_year",
        "benchmark_max_year",
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


def _resolve_output_csv(output_csv: Path | None, benchmark_name: str | None) -> Path:
    if output_csv is not None:
        return output_csv
    if benchmark_name:
        return REPO_ROOT / "experiment" / f"{benchmark_name}.csv"
    raise ValueError("either output_csv or benchmark_name is required")


def _normalize_year_inputs(year: int | None, min_year: int | None, max_year: int | None) -> tuple[int | None, int | None, int | None]:
    if year is not None and (min_year is not None or max_year is not None):
        raise ValueError("--year cannot be combined with --min-year or --max-year")
    if year is None and min_year is None and max_year is None:
        raise ValueError("one of --year, --min-year, or --max-year is required")
    if min_year is not None and max_year is not None and int(min_year) > int(max_year):
        raise ValueError("--min-year cannot be greater than --max-year")
    return year, min_year, max_year


def build_query_benchmark(
    query: str,
    year: int | None,
    min_year: int | None,
    max_year: int | None,
    limit: int,
    output_csv: Path,
    sort: str,
    citation_percentile_min: float | None = None,
) -> Path:
    filter_str = _build_filter(query=query, year=year, min_year=min_year, max_year=max_year)
    rows: List[Dict[str, Any]] = []
    for work in iter_openalex_works(
        filter_str=filter_str,
        sort=sort,
        select_fields=SELECT_FIELDS,
        mailto=OPENALEX_MAILTO,
        max_records=max(int(limit), 1) * 3,
    ):
        row = extract_row(work)
        row["benchmark_query"] = query
        row["benchmark_year"] = int(year) if year is not None else ""
        row["benchmark_min_year"] = int(min_year) if min_year is not None else ""
        row["benchmark_max_year"] = int(max_year) if max_year is not None else ""
        row["benchmark_source"] = f"openalex_title_and_abstract_{sort.replace(':', '_')}"
        percentile = row.get("citation_normalized_percentile")
        prioritized = (
            citation_percentile_min is not None
            and percentile is not None
            and float(percentile) >= float(citation_percentile_min)
        )
        row["benchmark_citation_percentile_min"] = (
            float(citation_percentile_min) if citation_percentile_min is not None else ""
        )
        row["benchmark_priority"] = 1 if prioritized else 2
        row["benchmark_priority_reason"] = (
            f"citation_normalized_percentile>={float(citation_percentile_min):.4f}"
            if prioritized
            else "overall_top_citation"
        )
        rows.append(row)

    if not rows:
        raise RuntimeError(
            f"no OpenAlex results for query={query!r}, year={year}, min_year={min_year}, max_year={max_year}"
        )

    df = pd.DataFrame(rows)
    if "doi" not in df.columns:
        raise RuntimeError("OpenAlex result rows do not contain a doi column")

    df = df.dropna(subset=["doi"]).copy()
    df["doi"] = df["doi"].astype(str).str.strip().str.lower()
    df = df[df["doi"] != ""].copy()
    df = df.drop_duplicates(subset=["doi"], keep="first").copy()
    sort_columns = ["cited_by_count", "doi"]
    sort_orders = [False, True]
    if citation_percentile_min is not None:
        sort_columns = ["benchmark_priority", "cited_by_count", "doi"]
        sort_orders = [True, False, True]
    df = df.sort_values(by=sort_columns, ascending=sort_orders, na_position="last").copy()
    df = df.head(int(limit)).copy()
    df["benchmark_rank"] = range(1, len(df) + 1)
    df = df[_ordered_columns(df)]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return output_csv


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an OpenAlex query benchmark CSV sorted by citation count.")
    parser.add_argument("--query", required=True, help="OpenAlex title_and_abstract query string")
    parser.add_argument("--year", type=int, default=None, help="Exact publication year filter")
    parser.add_argument("--min-year", type=int, default=None, help="Minimum publication year (inclusive)")
    parser.add_argument("--max-year", type=int, default=None, help="Maximum publication year (inclusive)")
    parser.add_argument("--limit", type=int, default=200, help="Maximum number of rows to keep")
    parser.add_argument("--top-k", type=int, default=None, help="Alias of --limit")
    parser.add_argument("--sort", default="cited_by_count:desc", help="OpenAlex sort expression")
    parser.add_argument(
        "--citation-percentile-min",
        type=float,
        default=None,
        help=(
            "If set, prioritize works whose citation_normalized_percentile.value is at least this threshold "
            "before falling back to overall cited_by_count ordering."
        ),
    )
    parser.add_argument("--benchmark-name", type=str, default=None, help="Benchmark basename; used when --output-csv is omitted")
    parser.add_argument("--output-csv", type=Path, default=None, help="Output CSV path")
    args = parser.parse_args()

    year, min_year, max_year = _normalize_year_inputs(
        year=None if args.year is None else int(args.year),
        min_year=None if args.min_year is None else int(args.min_year),
        max_year=None if args.max_year is None else int(args.max_year),
    )
    limit = max(1, int(args.top_k if args.top_k is not None else args.limit))
    output_csv = _resolve_output_csv(
        output_csv=args.output_csv.resolve() if args.output_csv is not None else None,
        benchmark_name=str(args.benchmark_name).strip() if args.benchmark_name else None,
    )

    out = build_query_benchmark(
        query=str(args.query),
        year=year,
        min_year=min_year,
        max_year=max_year,
        limit=limit,
        output_csv=output_csv.resolve(),
        sort=str(args.sort),
        citation_percentile_min=(
            None if args.citation_percentile_min is None else float(args.citation_percentile_min)
        ),
    )
    df = pd.read_csv(out)
    print(f"output_csv={out}")
    print(f"rows={len(df)}")
    print(f"query={args.query}")
    if year is not None:
        print(f"year={year}")
    else:
        print(f"min_year={'' if min_year is None else min_year}")
        print(f"max_year={'' if max_year is None else max_year}")
    print(f"top_k={limit}")
    print(f"sort={args.sort}")
    print(
        "citation_percentile_min="
        f"{'' if args.citation_percentile_min is None else float(args.citation_percentile_min)}"
    )
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
