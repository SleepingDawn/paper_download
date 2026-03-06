import argparse
import csv
import hashlib
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple
from urllib.parse import urlparse

from DrissionPage import ChromiumOptions, ChromiumPage
from rapidocr_onnxruntime import RapidOCR

from tools_exp import _apply_best_browser_profile, detect_access_issue

OUT_SUCCESS_VISUAL = "SUCCESS_VISUAL"
OUT_BLOCKED_VISUAL = "BLOCKED_VISUAL"
OUT_FAIL_NETWORK = "FAIL_NETWORK"

VISUAL_BLOCK_KEYWORDS = [
    "just a moment",
    "잠시만",
    "verify you are human",
    "are you human",
    "are you a robot",
    "i am not a robot",
    "captcha",
    "recaptcha",
    "turnstile",
    "validate user",
    "security check",
    "attention required",
    "access denied",
    "forbidden",
    "too many requests",
    "request blocked",
    "robot check",
    "unusual traffic",
]

_OCR_ENGINE = None


def _get_ocr_engine() -> RapidOCR:
    global _OCR_ENGINE
    if _OCR_ENGINE is None:
        _OCR_ENGINE = RapidOCR()
    return _OCR_ENGINE


def _valid_doi(doi: str) -> bool:
    return bool(re.match(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$", doi))


def load_dois(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    seen = set()
    out = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        doi_col = None
        for c in (reader.fieldnames or []):
            if c and c.strip().lower() == "doi":
                doi_col = c
                break
        if doi_col is None:
            raise ValueError("CSV DOI column not found")

        for row in reader:
            raw = str(row.get(doi_col, "")).strip().lower()
            if not raw:
                continue
            doi = raw.replace("https://doi.org/", "").replace("http://doi.org/", "").strip()
            if doi in seen:
                continue
            if not _valid_doi(doi):
                continue
            seen.add(doi)
            out.append(doi)
    return out


def _browser_for_worker(chrome_path: str) -> ChromiumPage:
    co = ChromiumOptions()
    if chrome_path and os.path.exists(chrome_path):
        co.set_browser_path(chrome_path)
    co.auto_port()
    _apply_best_browser_profile(co)
    return ChromiumPage(co)


def _safe_image_name(idx: int, doi: str) -> str:
    base = re.sub(r"[^A-Za-z0-9._-]", "_", doi)[:80].strip("_") or "doi"
    digest = hashlib.sha1(doi.encode("utf-8")).hexdigest()[:10]
    return f"{idx:04d}_{base}_{digest}.png"


def _ocr_text(image_path: str) -> str:
    try:
        ocr = _get_ocr_engine()
        result, _ = ocr(image_path)
        if not result:
            return ""
        texts = []
        for item in result:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            text = item[1]
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())
        return " ".join(texts)
    except Exception:
        return ""


def _classify_visual(title: str, ocr_text: str) -> Tuple[str, List[str]]:
    blob = f"{title}\n{ocr_text}".lower()
    hits = [kw for kw in VISUAL_BLOCK_KEYWORDS if kw in blob]
    if hits:
        return OUT_BLOCKED_VISUAL, hits
    return OUT_SUCCESS_VISUAL, []


def _probe_one(
    page: ChromiumPage,
    doi: str,
    idx: int,
    timeout_sec: float,
    settle_sec: float,
    screenshot_dir: str,
) -> Dict:
    started = time.perf_counter()
    doi_url = f"https://doi.org/{doi}"
    final_url = ""
    domain = ""
    title = ""
    html_issue = None
    html_evidence = []
    screenshot_path = ""
    ocr_text = ""
    visual_outcome = OUT_FAIL_NETWORK
    visual_evidence: List[str] = []

    try:
        page.get(doi_url, retry=1, interval=1, timeout=timeout_sec)
        if settle_sec > 0:
            time.sleep(settle_sec)

        final_url = page.url or doi_url
        domain = (urlparse(final_url).netloc or "").lower()
        title = page.title or ""
        html = page.html or ""
        html_issue, html_evidence = detect_access_issue(title=title, html=html)

        image_name = _safe_image_name(idx=idx, doi=doi)
        screenshot_path = os.path.join(screenshot_dir, image_name)
        page.get_screenshot(path=screenshot_dir, name=image_name, full_page=False)
        ocr_text = _ocr_text(screenshot_path)
        visual_outcome, visual_evidence = _classify_visual(title=title, ocr_text=ocr_text)
    except Exception as e:
        visual_outcome = OUT_FAIL_NETWORK
        visual_evidence = [str(e)[:300]]
        final_url = final_url or doi_url
        domain = domain or (urlparse(final_url).netloc or "").lower()

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return {
        "doi": doi,
        "doi_url": doi_url,
        "resolved_url": final_url,
        "domain": domain,
        "title": title[:300],
        "visual_outcome": visual_outcome,
        "visual_evidence": visual_evidence,
        "ocr_text_excerpt": ocr_text[:500],
        "html_issue": html_issue or "",
        "html_evidence": html_evidence,
        "screenshot_path": os.path.abspath(screenshot_path) if screenshot_path else "",
        "elapsed_ms": elapsed_ms,
        "timestamp_ms": int(time.time() * 1000),
    }


def _worker_run(
    worker_idx: int,
    records: List[Tuple[int, str]],
    out_jsonl: str,
    chrome_path: str,
    timeout_sec: float,
    settle_sec: float,
    screenshot_dir: str,
    progress_every: int,
) -> Dict:
    page = _browser_for_worker(chrome_path=chrome_path)
    out_records = []
    done = 0
    success = 0
    blocked = 0
    try:
        with open(out_jsonl, "w", encoding="utf-8") as f:
            for idx, doi in records:
                rec = _probe_one(
                    page=page,
                    doi=doi,
                    idx=idx,
                    timeout_sec=timeout_sec,
                    settle_sec=settle_sec,
                    screenshot_dir=screenshot_dir,
                )
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                out_records.append(rec)

                done += 1
                if rec["visual_outcome"] == OUT_SUCCESS_VISUAL:
                    success += 1
                elif rec["visual_outcome"] == OUT_BLOCKED_VISUAL:
                    blocked += 1

                if progress_every > 0 and (done % progress_every == 0 or done == len(records)):
                    print(
                        f"[visual_audit|worker={worker_idx}] progress {done}/{len(records)} "
                        f"(success={success}, blocked={blocked})",
                        flush=True,
                    )
    finally:
        try:
            page.quit()
        except Exception:
            pass
    return {"worker": worker_idx, "done": done, "records": out_records}


def _summarize(records: List[Dict]) -> Dict:
    total = len(records)
    counts = Counter(r.get("visual_outcome") for r in records)
    success = counts.get(OUT_SUCCESS_VISUAL, 0)
    blocked = counts.get(OUT_BLOCKED_VISUAL, 0)
    network = counts.get(OUT_FAIL_NETWORK, 0)

    by_domain = {}
    for r in records:
        domain = r.get("domain") or ""
        m = by_domain.setdefault(
            domain,
            {"n": 0, "success": 0, "blocked": 0, "network": 0},
        )
        m["n"] += 1
        if r.get("visual_outcome") == OUT_SUCCESS_VISUAL:
            m["success"] += 1
        elif r.get("visual_outcome") == OUT_BLOCKED_VISUAL:
            m["blocked"] += 1
        else:
            m["network"] += 1

    domain_rows = []
    for domain, m in by_domain.items():
        n = max(1, m["n"])
        domain_rows.append(
            {
                "domain": domain,
                "n": m["n"],
                "success_rate": round(m["success"] / n, 4),
                "blocked_rate": round(m["blocked"] / n, 4),
                "network_rate": round(m["network"] / n, 4),
            }
        )
    domain_rows.sort(key=lambda x: x["n"], reverse=True)

    return {
        "total_valid": total,
        "success_ratio_visual": round(success / total, 4) if total else 0.0,
        "blocked_ratio_visual": round(blocked / total, 4) if total else 0.0,
        "network_ratio": round(network / total, 4) if total else 0.0,
        "outcome_counts": dict(counts),
        "domain_breakdown": domain_rows,
    }


def main() -> None:
    p = argparse.ArgumentParser(
        description="Access top-N DOI landing pages, capture screenshots, and classify blocked/success from image OCR."
    )
    p.add_argument("--input", type=str, default="ready_to_download.csv")
    p.add_argument("--top-n", type=int, default=100)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--timeout-sec", type=float, default=20.0)
    p.add_argument("--settle-sec", type=float, default=1.5)
    p.add_argument("--progress-every", type=int, default=10)
    p.add_argument("--screenshot-dir", type=str, default="outputs/visual_access_audit/screenshots")
    p.add_argument("--output-jsonl", type=str, default="outputs/visual_access_audit/results.jsonl")
    p.add_argument("--report", type=str, default="outputs/visual_access_audit/report.json")
    args = p.parse_args()

    all_dois = load_dois(args.input)
    limit = max(1, int(args.top_n))
    dois = all_dois[:limit]
    indexed = list(enumerate(dois, start=1))

    os.makedirs(args.screenshot_dir, exist_ok=True)
    os.makedirs(os.path.dirname(args.output_jsonl), exist_ok=True)
    os.makedirs(os.path.dirname(args.report), exist_ok=True)

    workers = max(1, int(args.workers))
    chunks: List[List[Tuple[int, str]]] = [[] for _ in range(workers)]
    for i, pair in enumerate(indexed):
        chunks[i % workers].append(pair)

    worker_files = [f"{args.output_jsonl}.worker{i}.jsonl" for i in range(workers)]
    for wf in worker_files:
        try:
            os.remove(wf)
        except FileNotFoundError:
            pass

    chrome_path = os.environ.get("CHROME_PATH", "/home/yongyong0206/chrome-linux64/chrome")
    all_records = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = []
        for i, chunk in enumerate(chunks):
            futs.append(
                ex.submit(
                    _worker_run,
                    i,
                    chunk,
                    worker_files[i],
                    chrome_path,
                    float(args.timeout_sec),
                    float(args.settle_sec),
                    args.screenshot_dir,
                    int(args.progress_every),
                )
            )
        for fut in as_completed(futs):
            res = fut.result()
            all_records.extend(res["records"])
            print(f"[visual_audit|worker={res['worker']}] done {res['done']}", flush=True)

    with open(args.output_jsonl, "w", encoding="utf-8") as out_f:
        for wf in worker_files:
            if not os.path.exists(wf):
                continue
            with open(wf, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    out_f.write(line)

    all_records.sort(key=lambda r: r.get("doi", ""))
    summary = _summarize(all_records)
    report = {
        "generated_at": int(time.time()),
        "input_csv": os.path.abspath(args.input),
        "top_n": limit,
        "workers": workers,
        "timeout_sec": float(args.timeout_sec),
        "settle_sec": float(args.settle_sec),
        "criteria": "BLOCKED_VISUAL if screenshot OCR/title contains challenge keywords; otherwise SUCCESS_VISUAL",
        "summary": summary,
        "output_jsonl": os.path.abspath(args.output_jsonl),
        "screenshot_dir": os.path.abspath(args.screenshot_dir),
    }
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(
        json.dumps(
            {
                "top_n": limit,
                "workers": workers,
                "success_ratio_visual": summary["success_ratio_visual"],
                "blocked_ratio_visual": summary["blocked_ratio_visual"],
                "network_ratio": summary["network_ratio"],
                "report": os.path.abspath(args.report),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
