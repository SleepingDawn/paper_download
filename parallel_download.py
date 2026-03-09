import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from config import get_config
from openalex_search import main_search
from tools_exp import (
    _sanitize_doi_to_filename,
    download_using_api,
    download_with_cffi,
    download_with_drission,
    normalize_publisher_label,
    resolve_browser_executable,
    setup_logger,
    try_manual_scihub,
)

REASON_SUCCESS = "SUCCESS"
REASON_FAIL_CAPTCHA = "FAIL_CAPTCHA"
REASON_FAIL_BLOCK = "FAIL_BLOCK"
REASON_FAIL_ACCESS_RIGHTS = "FAIL_ACCESS_RIGHTS"
REASON_FAIL_WRONG_MIME = "FAIL_WRONG_MIME"
REASON_FAIL_VIEWER_HTML = "FAIL_VIEWER_HTML"
REASON_FAIL_HTTP_STATUS = "FAIL_HTTP_STATUS"
REASON_FAIL_TIMEOUT_NETWORK = "FAIL_TIMEOUT/NETWORK"
REASON_FAIL_PDF_MAGIC = "FAIL_PDF_MAGIC"
REASON_FAIL_TOO_SMALL = "FAIL_TOO_SMALL"
REASON_FAIL_NO_CANDIDATE = "FAIL_NO_CANDIDATE"
REASON_FAIL_REDIRECT_LOOP = "FAIL_REDIRECT_LOOP"
REASON_FAIL_UNKNOWN = "FAIL_UNKNOWN"
SAFE_MAX_WORKERS = 5
LANDING_SUCCESS_OUTCOME = "SUCCESS_ACCESS"
LANDING_ACCESS_RIGHTS_OUTCOME = "FAIL_ACCESS_RIGHTS"

FAILURE_REASON_ORDER = [
    REASON_FAIL_CAPTCHA,
    REASON_FAIL_BLOCK,
    REASON_FAIL_ACCESS_RIGHTS,
    REASON_FAIL_WRONG_MIME,
    REASON_FAIL_VIEWER_HTML,
    REASON_FAIL_HTTP_STATUS,
    REASON_FAIL_TIMEOUT_NETWORK,
    REASON_FAIL_PDF_MAGIC,
    REASON_FAIL_TOO_SMALL,
    REASON_FAIL_NO_CANDIDATE,
    REASON_FAIL_REDIRECT_LOOP,
    REASON_FAIL_UNKNOWN,
]


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse

        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _result_template(doi: str, attempt: int, mode: str) -> Dict[str, Any]:
    return {
        "doi": doi,
        "attempt": attempt,
        "mode": mode,
        "status": "Failed",
        "reason": REASON_FAIL_UNKNOWN,
        "method": None,
        "evidence": [],
        "stage": "init",
        "domain": "",
        "http_status": None,
        "success": False,
    }


def _status_text(result: Dict[str, Any]) -> str:
    if result.get("success"):
        method = result.get("method") or "unknown"
        return f"Success ({method})"
    return result.get("reason", REASON_FAIL_UNKNOWN)


def _normalize_reason(reason: Optional[str], http_status: Optional[int] = None) -> str:
    if not reason:
        return REASON_FAIL_UNKNOWN
    if reason == "FAIL_NETWORK":
        return REASON_FAIL_TIMEOUT_NETWORK
    if reason == "FAIL_PARSE":
        return REASON_FAIL_NO_CANDIDATE
    if reason == "FAIL_ACCESS_RIGHTS":
        return REASON_FAIL_ACCESS_RIGHTS
    if reason == "FAIL_BLOCK":
        return REASON_FAIL_HTTP_STATUS if http_status else REASON_FAIL_BLOCK
    return reason


def _append_failed_jsonl(path: str, record: Dict[str, Any], dedupe_keys: set) -> None:
    key = (
        str(record.get("doi")),
        int(record.get("attempt", 0)),
        str(record.get("reason")),
        str(record.get("stage")),
        str(record.get("domain")),
    )
    if key in dedupe_keys:
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    dedupe_keys.add(key)


def _load_failed_dedupe_keys(path: str) -> set:
    keys = set()
    if not os.path.exists(path):
        return keys

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = (
                str(rec.get("doi")),
                int(rec.get("attempt", 0)),
                str(rec.get("reason")),
                str(rec.get("stage")),
                str(rec.get("domain")),
            )
            keys.add(key)
    return keys


def _summarize_failures(results: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {k: 0 for k in FAILURE_REASON_ORDER}
    for r in results:
        if r.get("success"):
            continue
        reason = _normalize_reason(r.get("reason"), r.get("http_status"))
        summary[reason] = summary.get(reason, 0) + 1
    return summary


def _backoff_sleep(base: int, attempt_idx: int) -> None:
    time.sleep(base * (2 ** attempt_idx))


def _single_download_attempt(
    row_data: Dict[str, Any],
    save_dir: str,
    attempt: int,
    mode: str,
) -> Dict[str, Any]:
    doi = str(row_data.get("doi", "")).strip()
    result = _result_template(doi=doi, attempt=attempt, mode=mode)

    if not doi or doi.lower() == "none" or doi.lower() == "nan":
        result["reason"] = REASON_FAIL_NO_CANDIDATE
        result["stage"] = "input"
        result["evidence"] = ["missing_doi"]
        return result

    publisher = normalize_publisher_label(str(row_data.get("publisher", "")))
    pdf_url_oa = str(row_data.get("pdf_url", "")).strip()
    filename = _sanitize_doi_to_filename(doi)
    full_path = os.path.join(save_dir, filename)

    logger = setup_logger(save_dir, filename)
    attempt_trace: List[Dict[str, Any]] = []

    if publisher == "arxiv" or "arxiv.org" in pdf_url_oa.lower() or doi.lower().startswith("10.1149/ma"):
        return {
            **result,
            "status": "Skipped",
            "reason": REASON_SUCCESS,
            "method": "skip",
            "success": True,
            "stage": "skip",
        }

    # 사용자 요청: Sci-Hub를 항상 최우선(1순위)으로 시도.
    try:
        scihub_budget = int(os.getenv("SCIHUB_MAX_TOTAL_S", "20"))
        if try_manual_scihub(doi, save_dir, logger, max_total_s=scihub_budget):
            return {
                **result,
                "status": "Success",
                "reason": REASON_SUCCESS,
                "method": "scihub",
                "success": True,
                "stage": "scihub",
            }
    except Exception as e:
        logger.warning(f"   Sci-Hub 다운로드 에러: {e}")
        attempt_trace.append({"strategy": "scihub", "reason": REASON_FAIL_TIMEOUT_NETWORK, "evidence": [str(e)]})

    if pdf_url_oa and pdf_url_oa.lower() not in ("none", "nan") and len(pdf_url_oa) > 10:
        cffi_timeout = int(os.getenv("DIRECT_OA_CFFI_TIMEOUT_S", "12"))
        cffi = download_with_cffi(
            pdf_url_oa,
            full_path,
            logger=logger,
            return_detail=True,
            timeout=cffi_timeout,
        )
        if cffi.get("ok"):
            return {
                **result,
                "status": "Success",
                "reason": REASON_SUCCESS,
                "method": "direct_oa",
                "success": True,
                "stage": "direct_oa",
                "domain": _domain_from_url(pdf_url_oa),
            }
        attempt_trace.append(
            {
                "strategy": "direct_oa_cffi",
                "reason": _normalize_reason(cffi.get("reason"), cffi.get("http_status")),
                "http_status": cffi.get("http_status"),
                "evidence": cffi.get("evidence", []),
            }
        )
        if cffi.get("reason") in (REASON_FAIL_CAPTCHA, REASON_FAIL_BLOCK, REASON_FAIL_ACCESS_RIGHTS):
            return {
                **result,
                "reason": _normalize_reason(cffi.get("reason"), cffi.get("http_status")),
                "stage": "direct_oa",
                "evidence": cffi.get("evidence", []) + [json.dumps({"trace": attempt_trace}, ensure_ascii=False)],
                "domain": _domain_from_url(pdf_url_oa),
                "http_status": cffi.get("http_status"),
            }

    def _run_drission_result() -> Dict[str, Any]:
        chrome_path = resolve_browser_executable(os.environ.get("CHROME_PATH", ""), logger=logger)
        dr = download_with_drission(
            f"https://doi.org/{doi}",
            save_dir,
            filename,
            chrome_path,
            max_attempts=2 if mode == "deep" else 1,
            logger=logger,
            mode=mode,
            return_detail=True,
        )
        if dr.get("ok"):
            return {
                **result,
                "status": "Success",
                "reason": REASON_SUCCESS,
                "method": "drission",
                "success": True,
                "stage": dr.get("stage", "drission"),
                "domain": dr.get("domain", ""),
            }
        return {
            **result,
            "reason": _normalize_reason(dr.get("reason"), dr.get("http_status")),
            "stage": dr.get("stage", "drission"),
            "evidence": dr.get("evidence", ["download_failed"]) + [json.dumps({"trace": attempt_trace}, ensure_ascii=False)],
            "domain": dr.get("domain", ""),
            "http_status": dr.get("http_status"),
        }

    publisher_key = (publisher or "").lower()
    # Elsevier API 경로는 실효성이 낮고 브라우저 경로와 중복 비용이 커서 생략.
    skip_api = publisher_key in {"elsevier"}
    if not skip_api:
        try:
            if download_using_api(doi, save_dir, publisher, logger):
                return {
                    **result,
                    "status": "Success",
                    "reason": REASON_SUCCESS,
                    "method": "api",
                    "success": True,
                    "stage": "api",
                }
        except Exception as e:
            logger.warning(f"   API 다운로드 에러: {e}")
            attempt_trace.append({"strategy": "api", "reason": REASON_FAIL_TIMEOUT_NETWORK, "evidence": [str(e)]})
    else:
        attempt_trace.append({"strategy": "api", "reason": REASON_FAIL_NO_CANDIDATE, "evidence": ["skipped_elsevier_api"]})

    return _run_drission_result()


def download_process_worker(row_data, final_save_path, attempt=1, mode="first"):
    network_retry_limit = 0 if mode == "first" else 2
    base_backoff = 2 if mode == "first" else 5

    last_result = None
    for network_try in range(network_retry_limit + 1):
        last_result = _single_download_attempt(row_data, final_save_path, attempt=attempt, mode=mode)

        if last_result.get("success"):
            return last_result

        reason = last_result.get("reason")
        if reason in (REASON_FAIL_CAPTCHA, REASON_FAIL_BLOCK, REASON_FAIL_ACCESS_RIGHTS):
            return last_result

        if reason == REASON_FAIL_TIMEOUT_NETWORK and network_try < network_retry_limit:
            retry_after_sec = None
            for ev in last_result.get("evidence", []):
                if str(ev).startswith("retry_after="):
                    try:
                        retry_after_sec = int(str(ev).split("=", 1)[1])
                    except Exception:
                        retry_after_sec = None
                    break

            if retry_after_sec is not None:
                time.sleep(max(1, retry_after_sec))
            else:
                _backoff_sleep(base_backoff, network_try)
            continue

        return last_result

    return last_result


def _first_pass(df: pd.DataFrame, oa_dir: str, ca_dir: str, max_workers: int) -> List[Dict[str, Any]]:
    rows = [row for _, row in df.iterrows()]
    results: List[Dict[str, Any]] = [None] * len(rows)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(
                download_process_worker,
                row,
                oa_dir if row["open_access"] else ca_dir,
                1,
                "first",
            ): i
            for i, row in enumerate(rows)
        }

        for future in tqdm(as_completed(future_to_index), total=len(rows), desc="First Pass"):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                doi = str(rows[idx].get("doi", ""))
                results[idx] = {
                    **_result_template(doi=doi, attempt=1, mode="first"),
                    "reason": REASON_FAIL_TIMEOUT_NETWORK,
                    "stage": "worker",
                    "evidence": [str(e)],
                }

    return results


def _deep_retry(
    df: pd.DataFrame,
    first_pass_results: List[Dict[str, Any]],
    oa_dir: str,
    ca_dir: str,
) -> List[Dict[str, Any]]:
    failed_indices = [i for i, r in enumerate(first_pass_results) if not r.get("success")]
    deep_results: List[Dict[str, Any]] = []

    if not failed_indices:
        return deep_results

    print("\n" + "=" * 60)
    print(f"Deep retry 시작: 실패 {len(failed_indices)}건 (동시성=1, 보수적 딜레이)")
    print("=" * 60)

    for idx in tqdm(failed_indices, desc="Deep Retry"):
        row = df.iloc[idx]
        save_dir = oa_dir if row["open_access"] else ca_dir
        result = download_process_worker(row, save_dir, attempt=2, mode="deep")
        deep_results.append({"index": idx, **result})

        if result.get("reason") in (REASON_FAIL_HTTP_STATUS, REASON_FAIL_BLOCK) and result.get("http_status") == 429:
            retry_after = None
            for ev in result.get("evidence", []):
                if str(ev).startswith("retry_after="):
                    try:
                        retry_after = int(str(ev).split("=", 1)[1])
                    except Exception:
                        retry_after = None
                    break
            time.sleep(max(2, retry_after or 10))
        else:
            time.sleep(5)

    return deep_results


def _resolve_decision(non_interactive: bool, after_first_pass: str, failed_count: int) -> str:
    if failed_count == 0:
        return "stop"
    if non_interactive:
        return after_first_pass

    print("\n1차 패스 실패 요약 확인 후 진행 방식을 선택하세요.")
    print("  - stop: 지금 종료 + 요약 저장")
    print("  - deep: 실패 논문 deep retry 진행")
    user_in = input(f"선택 [stop/deep] (기본: {after_first_pass}): ").strip().lower()
    if user_in not in ("stop", "deep"):
        return after_first_pass
    return user_in


def _summarize_live_attempt_metrics(attempts_jsonl_path: str, out_path: str) -> Dict[str, Any]:
    records = []
    if os.path.exists(attempts_jsonl_path):
        with open(attempts_jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    def _med(vals):
        if not vals:
            return 0
        vals = sorted(vals)
        n = len(vals)
        return vals[n // 2] if n % 2 else int((vals[n // 2 - 1] + vals[n // 2]) / 2)

    reason_dist: Dict[str, int] = {}
    by_strategy: Dict[str, Dict[str, Any]] = {}
    by_domain: Dict[str, Dict[str, Any]] = {}

    for r in records:
        reason = r.get("reason", REASON_FAIL_UNKNOWN)
        reason_dist[reason] = reason_dist.get(reason, 0) + 1

    for key, target in (("strategy", by_strategy), ("domain", by_domain)):
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            gk = str(r.get(key, "unknown"))
            groups.setdefault(gk, []).append(r)
        for gk, vals in groups.items():
            lat = [int(v.get("elapsed_ms", 0) or 0) for v in vals]
            succ = sum(1 for v in vals if bool(v.get("success")))
            target[gk] = {
                "count": len(vals),
                "success_rate": round(succ / len(vals), 4) if vals else 0,
                "median_latency_ms": _med(lat),
            }

    lat_all = [int(r.get("elapsed_ms", 0) or 0) for r in records]
    payload = {
        "count": len(records),
        "success_rate": round(sum(1 for r in records if bool(r.get("success"))) / len(records), 4) if records else 0.0,
        "median_latency_ms": _med(lat_all),
        "reason_distribution": reason_dist,
        "by_strategy": by_strategy,
        "by_domain": by_domain,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def _run_landing_precheck(
    df: pd.DataFrame,
    final_save_path: str,
    max_workers: int,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    precheck_dir = os.path.join(final_save_path, "landing_precheck")
    os.makedirs(precheck_dir, exist_ok=True)

    landing_input_csv = os.path.join(precheck_dir, "landing_input.csv")
    landing_output_jsonl = os.path.join(precheck_dir, "landing_results.jsonl")
    landing_report_json = os.path.join(precheck_dir, "landing_report.json")
    landing_report_md = os.path.join(precheck_dir, "landing_report.md")
    landing_artifact_dir = os.path.join(precheck_dir, "artifacts")

    df.to_csv(landing_input_csv, index=False, encoding="utf-8-sig")

    cmd = [
        sys.executable,
        "-u",
        "landing_access_repro.py",
        "--input",
        landing_input_csv,
        "--workers",
        str(max(1, min(int(max_workers), 2))),
        "--headless",
        "1" if os.environ.get("PDF_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes") else "0",
        "--progress-every",
        "100",
        "--capture-fail-artifacts",
        "0",
        "--capture-success-artifacts",
        "0",
        "--zip-fail-artifacts",
        "0",
        "--zip-success-artifacts",
        "0",
        "--artifact-dir",
        landing_artifact_dir,
        "--output-jsonl",
        landing_output_jsonl,
        "--report",
        landing_report_json,
        "--report-md",
        landing_report_md,
    ]

    started = time.time()
    subprocess.run(cmd, check=True)
    elapsed = round(time.time() - started, 2)

    landing_report = {}
    if os.path.exists(landing_report_json):
        with open(landing_report_json, "r", encoding="utf-8") as f:
            landing_report = json.load(f)

    records: List[Dict[str, Any]] = []
    if os.path.exists(landing_output_jsonl):
        with open(landing_output_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    by_doi = {str(r.get("doi") or "").strip().lower(): r for r in records}
    total_input = int(len(df))
    success_dois = {
        doi
        for doi, rec in by_doi.items()
        if str(rec.get("outcome") or "") == LANDING_SUCCESS_OUTCOME
    }
    access_rights = sum(
        1 for rec in records if str(rec.get("outcome") or "") == LANDING_ACCESS_RIGHTS_OUTCOME
    )
    success = len(success_dois)
    eligible_df = df[df["doi"].astype(str).str.lower().isin(success_dois)].copy()
    eligible_df["landing_precheck_outcome"] = eligible_df["doi"].astype(str).str.lower().map(
        lambda doi: str((by_doi.get(doi) or {}).get("outcome") or "")
    )
    eligible_df["landing_precheck_state"] = eligible_df["doi"].astype(str).str.lower().map(
        lambda doi: str((by_doi.get(doi) or {}).get("classifier_state") or "")
    )

    adjusted_denominator = max(0, total_input - access_rights)
    metrics = {
        "executed": True,
        "elapsed_seconds": elapsed,
        "total_input": total_input,
        "landing_success": success,
        "access_rights_failures": access_rights,
        "eligible_for_download": int(len(eligible_df)),
        "adjusted_denominator": adjusted_denominator,
        "raw_success_rate": round(success / total_input, 4) if total_input else 0.0,
        "adjusted_success_rate": round(success / adjusted_denominator, 4) if adjusted_denominator else 0.0,
        "artifacts": {
            "input_csv": landing_input_csv,
            "results_jsonl": landing_output_jsonl,
            "report_json": landing_report_json,
            "report_md": landing_report_md,
        },
        "report_summary": landing_report.get("summary", {}),
    }
    return eligible_df, metrics


def main(
    max_num=1000,
    citation_percentile=0.99,
    query=None,
    max_workers=1,
    output_dir="./Solid_State_Electrolyte_Battery_Li_Papers",
    doi_path=None,
    after_first_pass="stop",
    non_interactive=False,
    precheck_landing=False,
):
    start_time = time.time()
    max_workers = max(1, min(int(max_workers), SAFE_MAX_WORKERS))

    final_save_path = os.path.abspath(output_dir)
    oa_dir = os.path.join(final_save_path, "Open_Access")
    ca_dir = os.path.join(final_save_path, "Closed_Access")
    os.makedirs(final_save_path, exist_ok=True)
    os.makedirs(oa_dir, exist_ok=True)
    os.makedirs(ca_dir, exist_ok=True)

    outputs_dir = os.path.abspath("outputs")
    failed_jsonl_path = os.path.join(outputs_dir, "failed_papers.jsonl")
    summary_json_path = os.path.join(outputs_dir, "summary.json")
    attempts_jsonl_path = os.path.join(outputs_dir, "download_attempts.jsonl")
    attempts_summary_path = os.path.join(outputs_dir, "download_attempts_summary.json")
    os.makedirs(outputs_dir, exist_ok=True)
    failed_dedupe_keys = _load_failed_dedupe_keys(failed_jsonl_path)

    ta_query = (
        "('solid-state electrolyte' OR 'solid electrolyte') AND 'battery' AND 'Li' "
        "NOT ('review' OR 'opinion' OR 'perspective' OR 'survey' OR 'commentary')"
        if query is None
        else query
    )

    csv_path = doi_path or main_search(
        final_save_path,
        "Searched_DOIs.csv",
        ta_query,
        max_num=max_num,
        citation_percentile=citation_percentile,
    )
    df = pd.read_csv(csv_path)

    print(f"\n중복 및 doi 누락 제거 전 논문 수: {len(df)}건")
    df["doi_lower"] = df["doi"].astype(str).str.lower().str.strip()
    df = df.dropna(subset=["doi_lower"]).drop_duplicates(subset=["doi_lower"]).drop(columns=["doi_lower"])
    print(f"전처리 후 남은 전체 논문 수: {len(df)}건")
    print(f"다운로드 동시성(max_workers): {max_workers} (상한={SAFE_MAX_WORKERS})")

    input_total_before_precheck = int(len(df))
    landing_precheck_metrics: Dict[str, Any] = {"executed": False}
    if precheck_landing:
        print("\n" + "=" * 60)
        print("Landing precheck 시작")
        print("=" * 60)
        df, landing_precheck_metrics = _run_landing_precheck(
            df=df,
            final_save_path=final_save_path,
            max_workers=max_workers,
        )
        print(
            f"Landing precheck 완료: 성공={landing_precheck_metrics.get('landing_success', 0)} / "
            f"권한없음={landing_precheck_metrics.get('access_rights_failures', 0)} / "
            f"다운로드 투입={landing_precheck_metrics.get('eligible_for_download', 0)}"
        )

    first_results = _first_pass(df, oa_dir, ca_dir, max_workers=max_workers)

    df["download_status"] = [_status_text(r) for r in first_results]

    first_failures = [r for r in first_results if not r.get("success")]
    for fail in first_failures:
        _append_failed_jsonl(
            failed_jsonl_path,
            {
                "timestamp": int(time.time()),
                "attempt": 1,
                "doi": fail.get("doi"),
                "reason": fail.get("reason"),
                "stage": fail.get("stage"),
                "domain": fail.get("domain"),
                "http_status": fail.get("http_status"),
                "evidence": fail.get("evidence", []),
                "mode": "first",
            },
            failed_dedupe_keys,
        )

    first_summary = _summarize_failures(first_results)
    print("\n[1차 패스 실패 요약]")
    for reason in FAILURE_REASON_ORDER:
        print(f"  - {reason}: {first_summary.get(reason, 0)}")

    decision = _resolve_decision(non_interactive, after_first_pass, len(first_failures))

    deep_results: List[Dict[str, Any]] = []
    if decision == "deep":
        deep_results = _deep_retry(df, first_results, oa_dir, ca_dir)

        for item in deep_results:
            idx = item["index"]
            if item.get("success"):
                df.at[idx, "download_status"] = _status_text(item)
            else:
                _append_failed_jsonl(
                    failed_jsonl_path,
                    {
                        "timestamp": int(time.time()),
                        "attempt": 2,
                        "doi": item.get("doi"),
                        "reason": item.get("reason"),
                        "stage": item.get("stage"),
                        "domain": item.get("domain"),
                        "http_status": item.get("http_status"),
                        "evidence": item.get("evidence", []),
                        "mode": "deep",
                    },
                    failed_dedupe_keys,
                )

    elapsed_seconds = time.time() - start_time

    full_csv_path = os.path.join(final_save_path, "openalex_search_results_parallel.csv")
    df.to_csv(full_csv_path, index=False, encoding="utf-8-sig")

    failed_df = df[~df["download_status"].str.contains("Success", case=False, na=False)]
    failed_csv_path = os.path.join(final_save_path, "failed_papers.csv")
    failed_df.to_csv(failed_csv_path, index=False, encoding="utf-8-sig")

    live_metrics = _summarize_live_attempt_metrics(attempts_jsonl_path, attempts_summary_path)

    summary_payload = {
        "generated_at": int(time.time()),
        "input_total_before_precheck": input_total_before_precheck,
        "total_papers": int(len(df)),
        "after_first_pass": decision,
        "non_interactive": bool(non_interactive),
        "precheck_landing": bool(precheck_landing),
        "landing_precheck": landing_precheck_metrics,
        "first_pass": {
            "success": int(sum(1 for r in first_results if r.get("success"))),
            "failed": int(sum(1 for r in first_results if not r.get("success"))),
            "fail_reasons": first_summary,
        },
        "deep_retry": {
            "executed": decision == "deep",
            "total": int(len(deep_results)),
            "success": int(sum(1 for r in deep_results if r.get("success"))),
            "failed": int(sum(1 for r in deep_results if not r.get("success"))),
            "fail_reasons": _summarize_failures(deep_results),
        },
        "elapsed_seconds": round(elapsed_seconds, 2),
        "live_attempt_metrics": live_metrics,
        "effective_rates": {
            "download_raw_success_rate": round(
                sum(1 for r in first_results if r.get("success")) / len(df), 4
            )
            if len(df)
            else 0.0,
            "download_adjusted_success_rate": round(
                sum(1 for r in first_results if r.get("success"))
                / max(1, len(df) - int(first_summary.get(REASON_FAIL_ACCESS_RIGHTS, 0))),
                4,
            )
            if (len(df) - int(first_summary.get(REASON_FAIL_ACCESS_RIGHTS, 0))) > 0
            else 0.0,
            "end_to_end_adjusted_success_rate": round(
                sum(1 for r in first_results if r.get("success"))
                / max(1, int(landing_precheck_metrics.get("adjusted_denominator", len(df)) or len(df))),
                4,
            )
            if int(landing_precheck_metrics.get("adjusted_denominator", len(df)) or len(df)) > 0
            else 0.0,
        },
        "artifacts": {
            "results_csv": full_csv_path,
            "failed_csv": failed_csv_path,
            "failed_jsonl": failed_jsonl_path,
            "attempts_jsonl": attempts_jsonl_path,
            "attempts_summary_json": attempts_summary_path,
            "summary_json": summary_json_path,
        },
    }

    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("[작업 완료]")
    print(f"총 논문 수: {len(df)}")
    print(f"성공: {sum(df['download_status'].str.contains('Success', case=False, na=False))}")
    print(f"실패: {len(failed_df)}")
    print(f"의사결정: {decision}")
    print(f"실패 로그(JSONL): {failed_jsonl_path}")
    print(f"요약(JSON): {summary_json_path}")
    print("=" * 60)


if __name__ == "__main__":
    args = get_config()
    main(
        max_num=args.max_num,
        citation_percentile=args.citation_percentile,
        query=args.query,
        max_workers=args.max_workers,
        output_dir=args.output_dir,
        doi_path=args.doi_path,
        after_first_pass=args.after_first_pass,
        non_interactive=args.non_interactive,
        precheck_landing=bool(int(args.precheck_landing)),
    )
