import argparse
import csv
import json
import os
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from statistics import median
from typing import Dict, List
from urllib.parse import urlparse

from DrissionPage import ChromiumOptions, ChromiumPage

from tools_exp import _apply_best_browser_profile, detect_access_issue

OUT_SUCCESS_ACCESS = "SUCCESS_ACCESS"
OUT_FAIL_CAPTCHA = "FAIL_CAPTCHA"
OUT_FAIL_BLOCK = "FAIL_BLOCK"
OUT_FAIL_NETWORK = "FAIL_NETWORK"


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


def _probe_one(page: ChromiumPage, doi: str, timeout_sec: float) -> Dict:
    started = time.perf_counter()
    doi_url = f"https://doi.org/{doi}"
    final_url = ""
    domain = ""
    title = ""
    issue = None
    evidence = []

    try:
        page.get(doi_url, retry=1, interval=1, timeout=timeout_sec)
        final_url = page.url or doi_url
        domain = (urlparse(final_url).netloc or "").lower()
        title = page.title or ""
        html = page.html or ""
        issue, evidence = detect_access_issue(title=title, html=html)
        if issue == OUT_FAIL_CAPTCHA:
            outcome = OUT_FAIL_CAPTCHA
        elif issue == OUT_FAIL_BLOCK:
            outcome = OUT_FAIL_BLOCK
        else:
            outcome = OUT_SUCCESS_ACCESS
    except Exception as e:
        outcome = OUT_FAIL_NETWORK
        evidence = [str(e)[:300]]
        final_url = final_url or doi_url
        domain = domain or (urlparse(final_url).netloc or "").lower()

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return {
        "doi": doi,
        "doi_url": doi_url,
        "resolved_url": final_url,
        "domain": domain,
        "title": title[:240],
        "outcome": outcome,
        "issue": issue or "",
        "evidence": evidence,
        "elapsed_ms": elapsed_ms,
        "timestamp_ms": int(time.time() * 1000),
    }


def _worker_run(worker_idx: int, dois: List[str], out_jsonl: str, chrome_path: str, timeout_sec: float, progress_every: int) -> Dict:
    page = _browser_for_worker(chrome_path=chrome_path)
    records = []
    done = 0
    success = 0
    try:
        with open(out_jsonl, "w", encoding="utf-8") as f:
            for doi in dois:
                rec = _probe_one(page, doi, timeout_sec=timeout_sec)
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.append(rec)
                done += 1
                if rec["outcome"] == OUT_SUCCESS_ACCESS:
                    success += 1
                if progress_every > 0 and (done % progress_every == 0 or done == len(dois)):
                    print(
                        f"[landing_repro|worker={worker_idx}] progress {done}/{len(dois)} (success={success})",
                        flush=True,
                    )
    finally:
        try:
            page.quit()
        except Exception:
            pass
    return {"worker": worker_idx, "done": done, "success": success, "records": records}


def _summarize(records: List[Dict]) -> Dict:
    total = len(records)
    counts = Counter(r.get("outcome") for r in records)
    elapsed = sorted(int(r.get("elapsed_ms", 0)) for r in records)

    def pct(v: float) -> float:
        if not elapsed:
            return 0.0
        if len(elapsed) == 1:
            return float(elapsed[0])
        idx = (len(elapsed) - 1) * v
        lo = int(idx)
        hi = min(lo + 1, len(elapsed) - 1)
        frac = idx - lo
        return float(elapsed[lo] * (1 - frac) + elapsed[hi] * frac)

    by_domain = defaultdict(lambda: {"n": 0, "success": 0, "captcha": 0, "block": 0, "network": 0, "elapsed": []})
    for r in records:
        d = r.get("domain") or ""
        o = r.get("outcome")
        m = by_domain[d]
        m["n"] += 1
        if o == OUT_SUCCESS_ACCESS:
            m["success"] += 1
        elif o == OUT_FAIL_CAPTCHA:
            m["captcha"] += 1
        elif o == OUT_FAIL_BLOCK:
            m["block"] += 1
        else:
            m["network"] += 1
        m["elapsed"].append(int(r.get("elapsed_ms", 0)))

    domain_rows = []
    for d, m in by_domain.items():
        n = max(1, m["n"])
        vals = sorted(m["elapsed"])
        domain_rows.append(
            {
                "domain": d,
                "n": m["n"],
                "success_rate": round(m["success"] / n, 4),
                "captcha_rate": round(m["captcha"] / n, 4),
                "block_rate": round(m["block"] / n, 4),
                "network_rate": round(m["network"] / n, 4),
                "p50_elapsed_ms": round(float(median(vals)), 1) if vals else 0.0,
                "p90_elapsed_ms": round(float(vals[int((len(vals) - 1) * 0.9)]), 1) if vals else 0.0,
            }
        )
    domain_rows.sort(key=lambda x: (x["captcha_rate"] + x["block_rate"], x["n"]), reverse=True)

    success = counts.get(OUT_SUCCESS_ACCESS, 0)
    blocked = counts.get(OUT_FAIL_CAPTCHA, 0) + counts.get(OUT_FAIL_BLOCK, 0)
    return {
        "total_valid": total,
        "success_ratio": round(success / total, 4) if total else 0.0,
        "block_captcha_rate": round(blocked / total, 4) if total else 0.0,
        "outcome_counts": dict(counts),
        "p50_elapsed_ms": round(pct(0.5), 1),
        "p90_elapsed_ms": round(pct(0.9), 1),
        "domain_breakdown_top20": domain_rows[:20],
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Reproduce landing-page access success using current download browser settings")
    p.add_argument("--input", type=str, default="ready_to_download.csv")
    p.add_argument("--max-dois", type=int, default=0)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--timeout-sec", type=float, default=20.0)
    p.add_argument("--progress-every", type=int, default=50)
    p.add_argument("--output-jsonl", type=str, default="outputs/landing_access_repro.jsonl")
    p.add_argument("--report", type=str, default="outputs/landing_access_repro_report.json")
    args = p.parse_args()

    dois = load_dois(args.input)
    if args.max_dois and args.max_dois > 0:
        dois = dois[: args.max_dois]
    if not dois:
        raise RuntimeError("No valid DOI found.")

    workers = max(1, int(args.workers))
    chunks = [[] for _ in range(workers)]
    for i, doi in enumerate(dois):
        chunks[i % workers].append(doi)

    os.makedirs(os.path.dirname(args.output_jsonl), exist_ok=True)
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
                    int(args.progress_every),
                )
            )
        for fut in as_completed(futs):
            res = fut.result()
            all_records.extend(res["records"])
            print(
                f"[landing_repro|worker={res['worker']}] done {res['done']} success={res['success']}",
                flush=True,
            )

    with open(args.output_jsonl, "w", encoding="utf-8") as out_f:
        for wf in worker_files:
            if not os.path.exists(wf):
                continue
            with open(wf, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    out_f.write(line)

    summary = _summarize(all_records)
    report = {
        "generated_at": int(time.time()),
        "input_csv": os.path.abspath(args.input),
        "workers": workers,
        "timeout_sec": float(args.timeout_sec),
        "total_valid": len(dois),
        "criteria": "SUCCESS_ACCESS if page loaded and detect_access_issue is not FAIL_CAPTCHA/FAIL_BLOCK",
        "summary": summary,
        "output_jsonl": os.path.abspath(args.output_jsonl),
    }

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(
        json.dumps(
            {
                "total_valid": len(dois),
                "workers": workers,
                "success_ratio": summary["success_ratio"],
                "block_captcha_rate": summary["block_captcha_rate"],
                "report": os.path.abspath(args.report),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
