import argparse
import json
import os
from typing import Any, Dict, List, Tuple


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _parse_candidate(spec: str) -> Tuple[str, str, str]:
    label, rest = str(spec or "").split("=", 1)
    report_path, results_path = rest.split(":", 1)
    return label.strip(), os.path.abspath(report_path.strip()), os.path.abspath(results_path.strip())


def _counts(report: Dict[str, Any]) -> Dict[str, int]:
    return dict((report.get("summary") or {}).get("classifier_counts") or {})


def _rows_by_doi(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(row.get("doi") or ""): row for row in rows if str(row.get("doi") or "").strip()}


def _compare_against_baseline(
    baseline_label: str,
    baseline_report: Dict[str, Any],
    baseline_rows: List[Dict[str, Any]],
    candidate_label: str,
    candidate_report: Dict[str, Any],
    candidate_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    baseline_by_doi = _rows_by_doi(baseline_rows)
    candidate_by_doi = _rows_by_doi(candidate_rows)
    changed_rows: List[Dict[str, Any]] = []
    improved: List[Dict[str, Any]] = []
    regressed: List[Dict[str, Any]] = []

    for doi in sorted(set(baseline_by_doi) | set(candidate_by_doi)):
        base = baseline_by_doi.get(doi, {})
        cand = candidate_by_doi.get(doi, {})
        base_state = str(base.get("classifier_state") or "")
        cand_state = str(cand.get("classifier_state") or "")
        if base_state != cand_state:
            row = {
                "doi": doi,
                "publisher": cand.get("input_publisher") or cand.get("scheduler_publisher") or base.get("input_publisher") or base.get("scheduler_publisher") or "",
                "baseline_state": base_state,
                "candidate_state": cand_state,
                "baseline_url": base.get("resolved_url", ""),
                "candidate_url": cand.get("resolved_url", ""),
            }
            changed_rows.append(row)
            if base_state != "success_landing" and cand_state == "success_landing":
                improved.append(row)
            if base_state == "success_landing" and cand_state != "success_landing":
                regressed.append(row)

    return {
        "baseline_label": baseline_label,
        "candidate_label": candidate_label,
        "baseline_counts": _counts(baseline_report),
        "candidate_counts": _counts(candidate_report),
        "baseline_sample_size": int(baseline_report.get("sample_size", 0) or 0),
        "candidate_sample_size": int(candidate_report.get("sample_size", 0) or 0),
        "baseline_p50_elapsed_ms": (baseline_report.get("summary") or {}).get("p50_elapsed_ms", 0),
        "candidate_p50_elapsed_ms": (candidate_report.get("summary") or {}).get("p50_elapsed_ms", 0),
        "baseline_p90_elapsed_ms": (baseline_report.get("summary") or {}).get("p90_elapsed_ms", 0),
        "candidate_p90_elapsed_ms": (candidate_report.get("summary") or {}).get("p90_elapsed_ms", 0),
        "improved": improved,
        "regressed": regressed,
        "changed_rows": changed_rows,
        "remaining_weak_spots": list(candidate_report.get("remaining_weak_spots") or []),
    }


def _render_markdown(baseline_label: str, baseline_report: Dict[str, Any], comparisons: List[Dict[str, Any]]) -> str:
    summary = baseline_report.get("summary") or {}
    lines = [
        "# Landing Experiment Comparison",
        "",
        "## Baseline",
        f"- Label: {baseline_label}",
        f"- Sample size: {baseline_report.get('sample_size', 0)}",
        f"- Classifier counts: {json.dumps(dict(summary.get('classifier_counts') or {}), ensure_ascii=False, sort_keys=True)}",
        f"- p50 elapsed ms: {summary.get('p50_elapsed_ms', 0)}",
        f"- p90 elapsed ms: {summary.get('p90_elapsed_ms', 0)}",
    ]

    for comp in comparisons:
        lines.extend(
            [
                "",
                f"## {comp['candidate_label']}",
                f"- Sample size: {comp['candidate_sample_size']}",
                f"- Classifier counts: {json.dumps(comp['candidate_counts'], ensure_ascii=False, sort_keys=True)}",
                f"- p50 elapsed ms: {comp['candidate_p50_elapsed_ms']}",
                f"- p90 elapsed ms: {comp['candidate_p90_elapsed_ms']}",
                f"- Improved DOIs: {len(comp['improved'])}",
                f"- Regressed DOIs: {len(comp['regressed'])}",
            ]
        )
        if comp["changed_rows"]:
            lines.append("- State changes:")
            for row in comp["changed_rows"]:
                lines.append(
                    f"- State change | {row['publisher']} | {row['doi']} | {row['baseline_state']} -> {row['candidate_state']}"
                )
        else:
            lines.append("- State changes: none")
        if comp["remaining_weak_spots"]:
            for item in comp["remaining_weak_spots"]:
                lines.append(f"- Remaining weak spot | {item}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare landing experiment outputs against a baseline run.")
    parser.add_argument("--baseline-label", type=str, default="baseline")
    parser.add_argument("--baseline-report", type=str, required=True)
    parser.add_argument("--baseline-results", type=str, required=True)
    parser.add_argument("--candidate", action="append", default=[], help="label=report.json:results.jsonl")
    parser.add_argument("--output-json", type=str, default="")
    parser.add_argument("--output-md", type=str, default="")
    args = parser.parse_args()

    baseline_report = _load_json(os.path.abspath(args.baseline_report))
    baseline_rows = _load_jsonl(os.path.abspath(args.baseline_results))
    comparisons = []
    for spec in args.candidate:
        label, report_path, results_path = _parse_candidate(spec)
        comparisons.append(
            _compare_against_baseline(
                baseline_label=str(args.baseline_label or "baseline"),
                baseline_report=baseline_report,
                baseline_rows=baseline_rows,
                candidate_label=label,
                candidate_report=_load_json(report_path),
                candidate_rows=_load_jsonl(results_path),
            )
        )

    payload = {
        "baseline_label": str(args.baseline_label or "baseline"),
        "baseline_report": os.path.abspath(args.baseline_report),
        "baseline_results": os.path.abspath(args.baseline_results),
        "comparisons": comparisons,
    }
    if args.output_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.output_json)) or ".", exist_ok=True)
        with open(os.path.abspath(args.output_json), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    if args.output_md:
        os.makedirs(os.path.dirname(os.path.abspath(args.output_md)) or ".", exist_ok=True)
        with open(os.path.abspath(args.output_md), "w", encoding="utf-8") as f:
            f.write(_render_markdown(str(args.baseline_label or "baseline"), baseline_report, comparisons))

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
