import json
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib import error as urllib_error
from urllib.parse import urlparse

from pdf_pipeline import DownloadAttempt, download_pdf, summarize_metrics

OUTPUTS_DIR = Path("outputs")
BASELINE_PATH = OUTPUTS_DIR / "baseline_metrics.json"
REPORT_PATH = OUTPUTS_DIR / "benchmark_report.json"

PDF_BYTES = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]/Contents 4 0 R>>endobj\n"
    b"4 0 obj<</Length 44>>stream\nBT /F1 12 Tf 72 120 Td (Hello PDF) Tj ET\nendstream endobj\n"
    b"xref\n0 5\n0000000000 65535 f\n"
    b"0000000010 00000 n\n0000000060 00000 n\n0000000117 00000 n\n0000000205 00000 n\n"
    b"trailer<</Root 1 0 R/Size 5>>\nstartxref\n300\n%%EOF\n"
)
SMALL_PDF = b"%PDF-1.4\n%%EOF\n"


def _fake_fetcher(url: str, timeout: int, headers: Dict, cookies=None):
    parsed = urlparse(url)
    path = parsed.path
    domain = parsed.netloc

    if path == "/pdf_ok":
        return {
            "status_code": 200,
            "headers": {"Content-Type": "application/pdf", "Content-Length": str(len(PDF_BYTES))},
            "content": PDF_BYTES,
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/pdf_slow":
        time.sleep(0.15)
        return {
            "status_code": 200,
            "headers": {"Content-Type": "application/pdf", "Content-Length": str(len(PDF_BYTES))},
            "content": PDF_BYTES,
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/viewer":
        html = (
            f"<html><head><meta name='citation_pdf_url' content='https://{domain}/pdf_ok' /></head>"
            f"<body><h1>PDF Viewer</h1><a href='https://{domain}/pdf_ok'>Download PDF</a></body></html>"
        ).encode("utf-8")
        return {
            "status_code": 200,
            "headers": {"Content-Type": "text/html; charset=utf-8", "Content-Length": str(len(html))},
            "content": html,
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/wrong_mime":
        body = b"<html><body>plain html page</body></html>"
        return {
            "status_code": 200,
            "headers": {"Content-Type": "text/html", "Content-Length": str(len(body))},
            "content": body,
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/bad_magic":
        body = b"<html><body>pretending pdf</body></html>"
        return {
            "status_code": 200,
            "headers": {"Content-Type": "application/pdf", "Content-Length": str(len(body))},
            "content": body,
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/tiny_pdf":
        return {
            "status_code": 200,
            "headers": {"Content-Type": "application/pdf", "Content-Length": str(len(SMALL_PDF))},
            "content": SMALL_PDF,
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/http_403":
        return {
            "status_code": 403,
            "headers": {"Content-Type": "text/plain", "Content-Length": "9"},
            "content": b"forbidden",
            "url": url,
            "redirect_chain": [url],
        }

    if path == "/loop_a":
        raise urllib_error.URLError("redirect error that would lead to an infinite loop")

    return {
        "status_code": 404,
        "headers": {"Content-Type": "text/plain"},
        "content": b"not found",
        "url": url,
        "redirect_chain": [url],
    }


def _run_case_set(mode: str, tmp_dir: str) -> Tuple[List[DownloadAttempt], Dict]:
    urls = (
        ["https://publisher-a.test/pdf_ok"] * 8
        + ["https://publisher-b.test/viewer"] * 4
        + ["https://publisher-a.test/pdf_slow"] * 2
        + ["https://publisher-c.test/wrong_mime"] * 2
        + ["https://publisher-d.test/http_403"] * 2
        + ["https://publisher-e.test/tiny_pdf"]
        + ["https://publisher-f.test/bad_magic"]
        + ["https://publisher-g.test/loop_a"]
    )

    records: List[DownloadAttempt] = []
    for i, url in enumerate(urls):
        out = os.path.join(tmp_dir, f"{mode}_{i}.pdf")
        attempt = download_pdf(
            url,
            out,
            strategy_mode=mode,
            timeout=10,
            min_size=128,
            strategy_name=f"bench_{mode}",
            fetcher=_fake_fetcher,
        )

        # Hard guard: 성공 파일은 반드시 PDF magic
        if attempt.success:
            with open(out, "rb") as f:
                if not f.read(4).startswith(b"%PDF"):
                    attempt.success = False
                    attempt.reason = "FAIL_PDF_MAGIC"

        records.append(attempt)

    summary = summarize_metrics(records)
    return records, summary


def _gate(baseline: Dict, candidate: Dict) -> Dict:
    succ_b = baseline["success_rate"]
    succ_c = candidate["success_rate"]
    med_b = baseline["median_latency_ms"] or 1
    med_c = candidate["median_latency_ms"]

    cond_a = (succ_c - succ_b) >= 0.02
    cond_b = (succ_c >= succ_b) and ((med_b - med_c) / med_b >= 0.20)
    cond_c = candidate["false_positive"] == 0

    return {
        "A_success_rate_improved": cond_a,
        "B_latency_improved_with_non_worse_success": cond_b,
        "C_false_positive_zero": cond_c,
        "passed": (cond_a or cond_b) and cond_c,
    }


def main():
    OUTPUTS_DIR.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="pdf_bench_") as tmp:
        _, baseline_summary = _run_case_set("baseline", tmp)
        with open(BASELINE_PATH, "w", encoding="utf-8") as f:
            json.dump(baseline_summary, f, ensure_ascii=False, indent=2)

        _, candidate_summary = _run_case_set("candidate", tmp)
        gate = _gate(baseline_summary, candidate_summary)

        report = {
            "generated_at": int(time.time()),
            "dataset": {
                "cases": {
                    "pdf_ok": 8,
                    "viewer": 4,
                    "pdf_slow": 2,
                    "wrong_mime": 2,
                    "http_403": 2,
                    "tiny_pdf": 1,
                    "bad_magic": 1,
                    "redirect_loop": 1,
                },
                "total": 21,
            },
            "pipeline_structure": {
                "baseline": ["direct_fetch", "pdf_magic_and_size_validation"],
                "candidate": [
                    "direct_fetch",
                    "viewer_html_detection",
                    "pdf_candidate_expansion(meta/a/iframe/embed)",
                    "redirect_loop_detection",
                    "pdf_magic_and_size_validation",
                ],
            },
            "hypotheses": [
                {
                    "change": "viewer_html candidate expansion",
                    "why_better": "landing page가 viewer HTML인 경우 실제 PDF 링크로 재시도해 성공률을 올린다",
                    "expected": "success rate +2%p 이상",
                },
                {
                    "change": "redirect loop early classification",
                    "why_better": "redirect 무한 루프를 즉시 FAIL_REDIRECT_LOOP로 분류해 지연을 줄인다",
                    "expected": "latency tail 감소",
                },
            ],
            "baseline": baseline_summary,
            "candidate": candidate_summary,
            "gate": gate,
        }

        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
