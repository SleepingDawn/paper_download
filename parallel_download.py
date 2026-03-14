import json
import os
import inspect
import subprocess
import sys
import time
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from config import apply_runtime_preset, get_config
from pdf_pipeline import append_metrics_jsonl
from landing_classifier import (
    STATE_CHALLENGE_DETECTED,
    estimate_publisher_key,
    release_pacing_slot,
    reorder_inputs_for_pacing,
    reserve_pacing_slot,
)
from tools_exp import (
    _recover_ieee_stamp_pdf_url,
    _sanitize_doi_to_filename,
    coerce_headless_for_execution_env,
    download_using_api,
    download_with_cffi,
    download_with_drission,
    normalize_publisher_label,
    reap_stale_drission_orphan_browsers,
    resolve_browser_execution_env,
    resolve_browser_executable,
    setup_logger,
    try_manual_scihub,
)

REASON_SUCCESS = "SUCCESS"
REASON_FAIL_CAPTCHA = "FAIL_CAPTCHA"
REASON_FAIL_BLOCK = "FAIL_BLOCK"
REASON_FAIL_ACCESS_RIGHTS = "FAIL_ACCESS_RIGHTS"
REASON_FAIL_DOI_NOT_FOUND = "FAIL_DOI_NOT_FOUND"
REASON_FAIL_SSRN_CHALLENGE = "FAIL_SSRN_CHALLENGE"
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
    REASON_FAIL_DOI_NOT_FOUND,
    REASON_FAIL_SSRN_CHALLENGE,
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

PACING_PROFILE_OVERRIDES = {
    "elsevier": {
        "cooldown_multiplier_first": 4.0,
        "cooldown_multiplier_deep": 6.0,
        "global_spacing_multiplier": 3.0,
    },
    "spie": {
        "cooldown_multiplier_first": 2.5,
        "cooldown_multiplier_deep": 4.0,
        "global_spacing_multiplier": 2.0,
    },
}

SCHEDULER_TO_DOWNLOAD_LABEL = {
    "acs": "ACS",
    "aip": "AIP",
    "aps": "APS",
    "elsevier": "ELSEVIER",
    "frontiers": "Frontiers",
    "ieee": "IEEE",
    "iop": "IOP",
    "ivyspring": "Ivyspring",
    "mdpi": "MDPI",
    "nature": "Nature",
    "optica": "Optica",
    "powdermat": "Powdermat",
    "rsc": "RSC",
    "spie": "SPIE",
    "taylor_and_francis": "Taylor & Francis",
    "wiley": "WILEY",
}

API_SUPPORTED_PUBLISHERS = {"aip", "iop", "nature", "springer", "wiley"}

def _resolve_worker_max_tasks_per_child() -> Optional[int]:
    # macOS spawn + Manager proxy 조합에서는 worker recycle이 future 정리 단계에서
    # 영구 대기를 유발할 수 있어 기본 비활성화한다.
    if sys.platform == "darwin":
        return None

    raw = os.getenv("PDF_WORKER_MAX_TASKS_PER_CHILD", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return max(1, value)

def _process_pool_supports_max_tasks_per_child() -> bool:
    try:
        return "max_tasks_per_child" in inspect.signature(ProcessPoolExecutor).parameters
    except Exception:
        return False
NON_RETRYABLE_TERMINAL_REASONS = {
    REASON_FAIL_CAPTCHA,
    REASON_FAIL_BLOCK,
    REASON_FAIL_ACCESS_RIGHTS,
    REASON_FAIL_DOI_NOT_FOUND,
    REASON_FAIL_SSRN_CHALLENGE,
}
NON_DEEP_RETRY_REASONS = {
    REASON_FAIL_ACCESS_RIGHTS,
    REASON_FAIL_DOI_NOT_FOUND,
    REASON_FAIL_SSRN_CHALLENGE,
}


def _resolve_run_output_dir(output_dir: str) -> str:
    raw = (output_dir or "paper_download_run").strip()
    if os.path.isabs(raw):
        return os.path.abspath(raw)
    normalized = os.path.normpath(raw)
    if normalized == "outputs" or normalized.startswith(f"outputs{os.sep}"):
        return os.path.abspath(normalized)
    return os.path.abspath(os.path.join("outputs", normalized))


def _resolve_pdf_output_dir(pdf_output_dir: Optional[str], run_output_dir: str) -> str:
    if pdf_output_dir:
        return os.path.abspath(pdf_output_dir)
    run_name = os.path.basename(os.path.normpath(run_output_dir)) or "paper_download_run"
    return os.path.abspath(os.path.join("pdfs", run_name))


def _env_flag(name: str, default: int = 0) -> bool:
    raw = str(os.environ.get(name, str(default))).strip().lower()
    return raw in ("1", "true", "yes", "on")


@contextmanager
def _temporary_browser_env(headless: bool, abort_on_landing_block: bool):
    prev_headless = os.environ.get("PDF_BROWSER_HEADLESS")
    prev_abort = os.environ.get("PDF_ABORT_ON_LANDING_BLOCK")
    os.environ["PDF_BROWSER_HEADLESS"] = "1" if headless else "0"
    os.environ["PDF_ABORT_ON_LANDING_BLOCK"] = "1" if abort_on_landing_block else "0"
    try:
        yield
    finally:
        if prev_headless is None:
            os.environ.pop("PDF_BROWSER_HEADLESS", None)
        else:
            os.environ["PDF_BROWSER_HEADLESS"] = prev_headless
        if prev_abort is None:
            os.environ.pop("PDF_ABORT_ON_LANDING_BLOCK", None)
        else:
            os.environ["PDF_ABORT_ON_LANDING_BLOCK"] = prev_abort


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse

        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _doi_prefix(doi: str) -> str:
    norm = str(doi or "").strip().lower()
    if "/" not in norm:
        return ""
    return norm.split("/", 1)[0]


def _resolve_download_publisher_label(row_data: Dict[str, Any]) -> Optional[str]:
    doi = str(row_data.get("doi", "") or "").strip()
    raw_publisher = str(row_data.get("publisher", "") or "").strip()
    scheduler_key = str(row_data.get("scheduler_publisher") or "").strip().lower()

    normalized = normalize_publisher_label(raw_publisher, prefix=doi)
    if normalized:
        return normalized

    if scheduler_key == "nature" and _doi_prefix(doi) == "10.1007":
        return "Springer"

    if scheduler_key:
        return SCHEDULER_TO_DOWNLOAD_LABEL.get(scheduler_key)

    return normalize_publisher_label("", prefix=doi)


def _resolve_pacing_overrides(
    publisher_key: str,
    mode: str,
    publisher_cooldown_sec: float,
    global_start_spacing_sec: float,
) -> tuple[float, float]:
    key = str(publisher_key or "").strip().lower()
    profile = PACING_PROFILE_OVERRIDES.get(key, {})
    if str(mode or "") == "deep":
        cooldown_multiplier = float(
            profile.get("cooldown_multiplier_deep", profile.get("cooldown_multiplier_first", 1.0))
        )
    else:
        cooldown_multiplier = float(profile.get("cooldown_multiplier_first", 1.0))
    global_multiplier = float(profile.get("global_spacing_multiplier", 1.0))
    return (
        max(0.0, float(publisher_cooldown_sec or 0.0)) * max(1.0, cooldown_multiplier),
        max(0.0, float(global_start_spacing_sec or 0.0)) * max(1.0, global_multiplier),
    )


def _is_browser_only_pdf_wrapper(url: str) -> bool:
    low = str(url or "").strip().lower()
    if not low:
        return False
    return (
        "aip.scitation.org/doi/pdf/" in low
        or "avs.scitation.org/doi/pdf/" in low
    )


def _is_cookie_sensitive_wiley_pdf_url(url: str) -> bool:
    low = str(url or "").strip().lower()
    if "onlinelibrary.wiley.com" not in low:
        return False
    return any(token in low for token in ("/doi/pdfdirect/", "/doi/pdf/", "/doi/epdf/"))


def _is_rsc_article_pdf_url(url: str) -> bool:
    low = str(url or "").strip().lower()
    return "pubs.rsc.org" in low and "/content/articlepdf/" in low


def _is_ieee_stamp_pdf_url(url: str) -> bool:
    low = str(url or "").strip().lower()
    return "ieeexplore.ieee.org/stamppdf/getpdf.jsp" in low


def _canonicalize_direct_pdf_url(url: str, publisher_key: str = "") -> str:
    raw = str(url or "").strip()
    low = raw.lower()
    key = str(publisher_key or "").strip().lower()
    if not raw:
        return ""
    if "ieeexplore.ieee.org" in low or key == "ieee":
        recovered = _recover_ieee_stamp_pdf_url(raw)
        if recovered:
            return recovered
    return raw


def _should_try_browser_direct_oa_fallback(url: str, publisher_key: str, reason: str) -> bool:
    low = str(url or "").strip().lower()
    key = str(publisher_key or "").strip().lower()
    normalized_reason = str(reason or "").strip()
    if normalized_reason not in (
        REASON_FAIL_TIMEOUT_NETWORK,
        REASON_FAIL_WRONG_MIME,
        REASON_FAIL_NO_CANDIDATE,
    ):
        return False
    if key in {"aip", "ieee", "spie"}:
        return True
    return any(
        token in low
        for token in (
            "pubs.aip.org/",
            "ieeexplore.ieee.org/",
            "spiedigitallibrary.org/",
        )
    )


def _append_live_attempt_record(record: Dict[str, Any]) -> None:
    metrics_path = os.path.abspath(
        os.environ.get("PDF_ATTEMPTS_JSONL", os.path.join("outputs", "download_attempts.jsonl"))
    )
    append_metrics_jsonl(metrics_path, record)


def _build_attempt_metrics_extra(
    doi: str,
    publisher: str,
    scheduler_publisher: str,
    attempt: int,
    mode: str,
    source_stage: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "doi": doi,
        "publisher": publisher or "",
        "scheduler_publisher": scheduler_publisher or "",
        "workflow_attempt": int(attempt),
        "workflow_mode": mode,
        "source_stage": source_stage,
    }
    if extra:
        payload.update(extra)
    return payload


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
        "landing_attempted": False,
        "landing_success": False,
        "landing_state": "not_attempted",
        "landing_url": "",
        "landing_title": "",
        "scheduler_publisher": "",
        "scheduled_start_ms": 0,
        "actual_start_ms": 0,
        "pacing_wait_ms": 0,
        "pacing_jitter_sec": 0.0,
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
    if reason == "FAIL_DOI_NOT_FOUND":
        return REASON_FAIL_DOI_NOT_FOUND
    if reason == "FAIL_SSRN_CHALLENGE":
        return REASON_FAIL_SSRN_CHALLENGE
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


def _summarize_integrated_landing(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    attempted = [r for r in results if bool(r.get("landing_attempted"))]
    state_counts: Dict[str, int] = {}
    success = 0
    access_rights = 0
    direct_pdf_handoff = 0
    for rec in attempted:
        state = str(rec.get("landing_state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        if bool(rec.get("landing_success")):
            success += 1
        if state == "access_rights_block":
            access_rights += 1
        if state == "direct_pdf_handoff":
            direct_pdf_handoff += 1

    attempted_count = len(attempted)
    adjusted_denominator = max(0, attempted_count - access_rights)
    return {
        "attempted": attempted_count,
        "not_attempted": max(0, len(results) - attempted_count),
        "success": success,
        "access_rights_failures": access_rights,
        "direct_pdf_handoff": direct_pdf_handoff,
        "raw_success_rate": round(success / attempted_count, 4) if attempted_count else 0.0,
        "adjusted_denominator": adjusted_denominator,
        "adjusted_success_rate": round(success / adjusted_denominator, 4) if adjusted_denominator else 0.0,
        "state_counts": state_counts,
    }


def _prepare_download_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for idx, (_, row) in enumerate(df.iterrows()):
        rec = dict(row.to_dict())
        doi = str(rec.get("doi", "") or "").strip()
        publisher = str(rec.get("publisher", "") or "").strip()
        pdf_url = str(rec.get("pdf_url", "") or "").strip()
        rec["open_access"] = _coerce_boolish(rec.get("open_access"))
        rec["_row_index"] = idx
        rec["scheduler_publisher"] = estimate_publisher_key(doi, input_publisher=publisher, pdf_url=pdf_url)
        records.append(rec)
    return reorder_inputs_for_pacing(records)


def _download_result_to_pacing_state(result: Dict[str, Any]) -> str:
    if bool(result.get("success")) and bool(result.get("landing_success")):
        return "success_landing"
    if str(result.get("landing_state") or "") == "direct_pdf_handoff":
        return "direct_pdf_handoff"
    reason = str(result.get("reason") or "")
    if reason in (REASON_FAIL_CAPTCHA, REASON_FAIL_BLOCK):
        return STATE_CHALLENGE_DETECTED
    if reason == REASON_FAIL_DOI_NOT_FOUND:
        return "doi_not_found"
    return str(result.get("landing_state") or "")


def _backoff_sleep(base: int, attempt_idx: int) -> None:
    time.sleep(base * (2 ** attempt_idx))


def _single_download_attempt(
    row_data: Dict[str, Any],
    pdf_save_dir: str,
    artifact_dir: str,
    attempt: int,
    mode: str,
    headless: bool,
    abort_on_landing_block: bool,
) -> Dict[str, Any]:
    doi = str(row_data.get("doi", "")).strip()
    result = _result_template(doi=doi, attempt=attempt, mode=mode)

    if not doi or doi.lower() == "none" or doi.lower() == "nan":
        result["reason"] = REASON_FAIL_NO_CANDIDATE
        result["stage"] = "input"
        result["evidence"] = ["missing_doi"]
        return result

    publisher = _resolve_download_publisher_label(row_data)
    publisher_key = (publisher or "").lower()
    scheduler_publisher = str(row_data.get("scheduler_publisher") or "")
    pdf_url_oa = str(row_data.get("pdf_url", "")).strip()
    filename = _sanitize_doi_to_filename(doi)
    full_path = os.path.join(pdf_save_dir, filename)
    is_ssrn_doi = doi.lower().startswith("10.2139/ssrn.")

    if publisher == "arxiv" or "arxiv.org" in pdf_url_oa.lower() or doi.lower().startswith("10.1149/ma"):
        skip_reason = "policy_skip"
        if publisher == "arxiv" or "arxiv.org" in pdf_url_oa.lower():
            skip_reason = "arxiv_managed_outside_pipeline"
        elif doi.lower().startswith("10.1149/ma"):
            skip_reason = "ecs_meeting_abstract_pattern"
        logger = setup_logger(artifact_dir, filename)
        logger.info(f"[Skip] 다운로드 생략: doi={doi}, reason={skip_reason}")
        return {
            **result,
            "status": "Skipped",
            "reason": REASON_SUCCESS,
            "method": "skip",
            "success": True,
            "stage": "skip",
        }

    logger = setup_logger(artifact_dir, filename)
    attempt_trace: List[Dict[str, Any]] = []

    # 사용자 요청: Sci-Hub를 항상 최우선(1순위)으로 시도.
    try:
        scihub_budget = int(os.getenv("SCIHUB_MAX_TOTAL_S", "20"))
        if try_manual_scihub(doi, pdf_save_dir, logger, max_total_s=scihub_budget):
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

    if is_ssrn_doi:
        logger.info(f"[FastFail] SSRN official-path fast-fail 정책 적용: doi={doi}")
        return {
            **result,
            "status": REASON_FAIL_SSRN_CHALLENGE,
            "reason": REASON_FAIL_SSRN_CHALLENGE,
            "method": "policy",
            "success": False,
            "stage": "policy",
            "evidence": ["ssrn_official_path_fast_fail_after_scihub"],
        }

    browser_direct_oa_entry = ""

    if pdf_url_oa and pdf_url_oa.lower() not in ("none", "nan") and len(pdf_url_oa) > 10:
        pdf_url_oa = _canonicalize_direct_pdf_url(pdf_url_oa, publisher_key)
        if _is_browser_only_pdf_wrapper(pdf_url_oa):
            logger.info(f"        [DirectOA] browser-only wrapper 스킵: {pdf_url_oa}")
            attempt_trace.append(
                {
                    "strategy": "direct_oa_cffi",
                    "reason": REASON_FAIL_NO_CANDIDATE,
                    "evidence": [f"browser_only_wrapper={pdf_url_oa}"],
                }
            )
        elif _is_cookie_sensitive_wiley_pdf_url(pdf_url_oa):
            logger.info(f"        [DirectOA] Wiley cookie-sensitive direct URL 스킵 -> browser landing: {pdf_url_oa}")
            attempt_trace.append(
                {
                    "strategy": "direct_oa_cffi",
                    "reason": REASON_FAIL_NO_CANDIDATE,
                    "evidence": [f"cookie_sensitive_wiley_wrapper={pdf_url_oa}"],
                }
            )
        else:
            cffi_timeout = int(os.getenv("DIRECT_OA_CFFI_TIMEOUT_S", "12"))
            cffi_retry_limit = 0
            if _is_rsc_article_pdf_url(pdf_url_oa):
                cffi_timeout = max(cffi_timeout, int(os.getenv("RSC_DIRECT_OA_TIMEOUT_S", "25")))
                cffi_retry_limit = 1
            elif _is_ieee_stamp_pdf_url(pdf_url_oa):
                cffi_timeout = max(cffi_timeout, int(os.getenv("IEEE_DIRECT_TIMEOUT_S", "15")))
                cffi_retry_limit = 1
            cffi = None
            for cffi_try in range(cffi_retry_limit + 1):
                timeout_this = cffi_timeout if cffi_try == 0 else max(cffi_timeout + 8, cffi_timeout * 2)
                cffi = download_with_cffi(
                    pdf_url_oa,
                    full_path,
                    logger=logger,
                    return_detail=True,
                    timeout=timeout_this,
                    metrics_extra=_build_attempt_metrics_extra(
                        doi=doi,
                        publisher=publisher or "",
                        scheduler_publisher=scheduler_publisher,
                        attempt=attempt,
                        mode=mode,
                        source_stage="direct_oa",
                        extra={
                            "direct_oa_url": pdf_url_oa,
                            "direct_oa_try": cffi_try + 1,
                        },
                    ),
                )
                if cffi.get("ok"):
                    break
                if not (
                    cffi.get("reason") == REASON_FAIL_TIMEOUT_NETWORK
                    and cffi_try < cffi_retry_limit
                    and (_is_rsc_article_pdf_url(pdf_url_oa) or _is_ieee_stamp_pdf_url(pdf_url_oa))
                ):
                    break
                logger.info(
                    f"        [DirectOA] transient direct fetch timeout -> 재시도 {cffi_try + 2}/{cffi_retry_limit + 1} "
                    f"(timeout={timeout_this}s)"
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
            if _should_try_browser_direct_oa_fallback(pdf_url_oa, publisher_key, cffi.get("reason")):
                browser_direct_oa_entry = pdf_url_oa
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

    def _run_drission_result(
        entry_url_override: str = "",
        source_stage: str = "browser",
        source_url: str = "",
    ) -> Dict[str, Any]:
        chrome_path = resolve_browser_executable(os.environ.get("CHROME_PATH", ""), logger=logger)
        doi_url = f"https://doi.org/{doi}"
        browser_entry_url = str(entry_url_override or doi_url).strip() or doi_url
        drission_started_ms = int(time.time() * 1000)
        _append_live_attempt_record(
            {
                "record_type": "attempt_event",
                "event": "browser_start",
                "timestamp_ms": drission_started_ms,
                "doi": doi,
                "publisher": publisher or "",
                "scheduler_publisher": scheduler_publisher,
                "workflow_attempt": int(attempt),
                "workflow_mode": mode,
                "source_stage": source_stage,
                "url": str(source_url or browser_entry_url),
            }
        )
        with _temporary_browser_env(headless=headless, abort_on_landing_block=abort_on_landing_block):
            dr = download_with_drission(
                doi_url,
                pdf_save_dir,
                filename,
                chrome_path,
                max_attempts=2 if mode == "deep" else 1,
                logger=logger,
                mode=mode,
                return_detail=True,
                artifact_root=artifact_dir,
                preferred_entry_url_override=entry_url_override,
            )
        drission_finished_ms = int(time.time() * 1000)
        _append_live_attempt_record(
            {
                "record_type": "attempt_result",
                "success": bool(dr.get("ok")),
                "reason": _normalize_reason(dr.get("reason"), dr.get("http_status")),
                "strategy": "drission",
                "phase": str(dr.get("stage") or "drission"),
                "elapsed_ms": max(0, drission_finished_ms - drission_started_ms),
                "url": str(source_url or browser_entry_url),
                "final_url": str(dr.get("landing_url") or browser_entry_url),
                "domain": str(dr.get("domain") or ""),
                "status_code": dr.get("http_status"),
                "content_type": "",
                "content_disposition": "",
                "content_length": None,
                "redirect_chain": [],
                "first_bytes": "",
                "evidence": {
                    "landing_attempted": bool(dr.get("landing_attempted")),
                    "landing_success": bool(dr.get("landing_success")),
                    "landing_state": str(dr.get("landing_state") or "not_attempted"),
                    "landing_title": str(dr.get("landing_title") or ""),
                    "browser_session_mode": str(dr.get("browser_session_mode") or ""),
                    "browser_session_source": str(dr.get("browser_session_source") or ""),
                },
                "file_path": full_path if bool(dr.get("ok")) else None,
                "timestamp_ms": drission_finished_ms,
                "started_at_ms": drission_started_ms,
                "finished_at_ms": drission_finished_ms,
                "doi": doi,
                "publisher": publisher or "",
                "scheduler_publisher": scheduler_publisher,
                "workflow_attempt": int(attempt),
                "workflow_mode": mode,
                "source_stage": source_stage,
            }
        )
        if dr.get("ok"):
            return {
                **result,
                "status": "Success",
                "reason": REASON_SUCCESS,
                "method": "drission" if source_stage == "browser" else source_stage,
                "success": True,
                "stage": dr.get("stage", "drission"),
                "domain": dr.get("domain", ""),
                "landing_attempted": bool(dr.get("landing_attempted")),
                "landing_success": bool(dr.get("landing_success")),
                "landing_state": str(dr.get("landing_state") or "not_attempted"),
                "landing_url": str(dr.get("landing_url") or ""),
                "landing_title": str(dr.get("landing_title") or ""),
                "browser_session_mode": str(dr.get("browser_session_mode") or ""),
                "browser_session_source": str(dr.get("browser_session_source") or ""),
                "browser_profile_name": str(dr.get("browser_profile_name") or ""),
                "browser_user_data_dir": str(dr.get("browser_user_data_dir") or ""),
            }
        return {
            **result,
            "reason": _normalize_reason(dr.get("reason"), dr.get("http_status")),
            "stage": dr.get("stage", "drission"),
            "evidence": dr.get("evidence", ["download_failed"]) + [json.dumps({"trace": attempt_trace}, ensure_ascii=False)],
            "domain": dr.get("domain", ""),
            "http_status": dr.get("http_status"),
            "landing_attempted": bool(dr.get("landing_attempted")),
            "landing_success": bool(dr.get("landing_success")),
            "landing_state": str(dr.get("landing_state") or "not_attempted"),
            "landing_url": str(dr.get("landing_url") or ""),
            "landing_title": str(dr.get("landing_title") or ""),
            "browser_session_mode": str(dr.get("browser_session_mode") or ""),
            "browser_session_source": str(dr.get("browser_session_source") or ""),
            "browser_profile_name": str(dr.get("browser_profile_name") or ""),
            "browser_user_data_dir": str(dr.get("browser_user_data_dir") or ""),
        }

    if browser_direct_oa_entry:
        browser_direct = _run_drission_result(
            entry_url_override=browser_direct_oa_entry,
            source_stage="direct_oa_browser",
            source_url=browser_direct_oa_entry,
        )
        if browser_direct.get("success"):
            return browser_direct

    # 지원 함수가 없는 publisher, 그리고 Elsevier/ACS는 브라우저 쿠키 세션 경로를 우선한다.
    skip_api = (publisher_key not in API_SUPPORTED_PUBLISHERS) or (publisher_key in {"elsevier", "acs"})
    if not skip_api:
        try:
            api_ok = download_using_api(
                doi,
                pdf_save_dir,
                publisher,
                logger,
                metrics_extra=_build_attempt_metrics_extra(
                    doi=doi,
                    publisher=publisher or "",
                    scheduler_publisher=scheduler_publisher,
                    attempt=attempt,
                    mode=mode,
                    source_stage="publisher_direct",
                ),
            )
            if api_ok:
                return {
                    **result,
                    "status": "Success",
                    "reason": REASON_SUCCESS,
                    "method": "api",
                    "success": True,
                    "stage": "api",
                }
            if publisher_key == "iop":
                iop_browser_entry = f"https://iopscience.iop.org/article/{doi}/pdf"
                browser_direct = _run_drission_result(
                    entry_url_override=iop_browser_entry,
                    source_stage="publisher_direct_browser",
                    source_url=iop_browser_entry,
                )
                if browser_direct.get("success"):
                    return browser_direct
        except Exception as e:
            logger.warning(f"   API 다운로드 에러: {e}")
            attempt_trace.append({"strategy": "api", "reason": REASON_FAIL_TIMEOUT_NETWORK, "evidence": [str(e)]})
    else:
        attempt_trace.append(
            {
                "strategy": "api",
                "reason": REASON_FAIL_NO_CANDIDATE,
                "evidence": [f"skipped_{publisher_key or 'unknown'}_api"],
            }
        )

    return _run_drission_result()


def download_process_worker(
    row_data,
    pdf_save_dir,
    artifact_dir,
    attempt=1,
    mode="first",
    headless=False,
    abort_on_landing_block=True,
    pacing_state=None,
    pacing_lock=None,
    publisher_cooldown_sec=0.0,
    global_start_spacing_sec=0.0,
    jitter_min_sec=0.0,
    jitter_max_sec=0.0,
):
    network_retry_limit = 0 if mode == "first" else 2
    base_backoff = 2 if mode == "first" else 5
    pacing_info = {
        "publisher_key": str(row_data.get("scheduler_publisher") or ""),
        "requested_start_ms": 0,
        "actual_start_ms": 0,
        "wait_ms": 0,
        "jitter_sec": 0.0,
    }
    publisher_key = str(row_data.get("scheduler_publisher") or "")
    effective_publisher_cooldown_sec, effective_global_start_spacing_sec = _resolve_pacing_overrides(
        publisher_key=publisher_key,
        mode=mode,
        publisher_cooldown_sec=float(publisher_cooldown_sec or 0.0),
        global_start_spacing_sec=float(global_start_spacing_sec or 0.0),
    )
    if pacing_state is not None and pacing_lock is not None:
        pacing_info = reserve_pacing_slot(
            pacing_state,
            pacing_lock,
            publisher_key=publisher_key,
            cooldown_sec=effective_publisher_cooldown_sec,
            global_spacing_sec=effective_global_start_spacing_sec,
            jitter_min_sec=float(jitter_min_sec or 0.0),
            jitter_max_sec=float(jitter_max_sec or 0.0),
        )

    last_result = None
    try:
        for network_try in range(network_retry_limit + 1):
            last_result = _single_download_attempt(
                row_data,
                pdf_save_dir,
                artifact_dir,
                attempt=attempt,
                mode=mode,
                headless=bool(headless),
                abort_on_landing_block=bool(abort_on_landing_block),
            )
            last_result["scheduler_publisher"] = publisher_key
            last_result["scheduled_start_ms"] = int(pacing_info.get("requested_start_ms", 0) or 0)
            last_result["actual_start_ms"] = int(pacing_info.get("actual_start_ms", 0) or 0)
            last_result["pacing_wait_ms"] = int(pacing_info.get("wait_ms", 0) or 0)
            last_result["pacing_jitter_sec"] = float(pacing_info.get("jitter_sec", 0.0) or 0.0)

            if last_result.get("success"):
                return last_result

            reason = last_result.get("reason")
            if reason in NON_RETRYABLE_TERMINAL_REASONS:
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
    finally:
        if pacing_state is not None and pacing_lock is not None:
            release_pacing_slot(
                pacing_state,
                pacing_lock,
                publisher_key=publisher_key,
                classifier_state=_download_result_to_pacing_state(last_result or {}),
                reason_codes=[],
            )


def _first_pass(
    df: pd.DataFrame,
    oa_pdf_dir: str,
    ca_pdf_dir: str,
    oa_artifact_dir: str,
    ca_artifact_dir: str,
    max_workers: int,
    headless: bool,
    abort_on_landing_block: bool,
    publisher_cooldown_sec: float,
    global_start_spacing_sec: float,
    jitter_min_sec: float,
    jitter_max_sec: float,
    worker_max_tasks_per_child: Optional[int],
    pacing_state,
    pacing_lock,
) -> List[Dict[str, Any]]:
    rows = _prepare_download_records(df)
    results: List[Dict[str, Any]] = [None] * len(rows)

    executor_kwargs = {"max_workers": max_workers}
    if worker_max_tasks_per_child is not None and _process_pool_supports_max_tasks_per_child():
        executor_kwargs["max_tasks_per_child"] = worker_max_tasks_per_child

    with ProcessPoolExecutor(**executor_kwargs) as executor:
        future_to_index = {
            executor.submit(
                download_process_worker,
                row,
                oa_pdf_dir if row["open_access"] else ca_pdf_dir,
                oa_artifact_dir if row["open_access"] else ca_artifact_dir,
                1,
                "first",
                bool(headless),
                bool(abort_on_landing_block),
                pacing_state,
                pacing_lock,
                float(publisher_cooldown_sec),
                float(global_start_spacing_sec),
                float(jitter_min_sec),
                float(jitter_max_sec),
            ): int(row.get("_row_index", 0))
            for row in rows
        }

        for future in tqdm(as_completed(future_to_index), total=len(rows), desc="First Pass"):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                doi = str(df.iloc[idx].get("doi", ""))
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
    oa_pdf_dir: str,
    ca_pdf_dir: str,
    oa_artifact_dir: str,
    ca_artifact_dir: str,
    headless: bool,
    abort_on_landing_block: bool,
    publisher_cooldown_sec: float,
    global_start_spacing_sec: float,
    jitter_min_sec: float,
    jitter_max_sec: float,
    pacing_state,
    pacing_lock,
) -> List[Dict[str, Any]]:
    failed_indices = [
        i
        for i, r in enumerate(first_pass_results)
        if (not r.get("success")) and (str(r.get("reason") or "") not in NON_DEEP_RETRY_REASONS)
    ]
    deep_results: List[Dict[str, Any]] = []

    if not failed_indices:
        return deep_results

    print("\n" + "=" * 60)
    print(f"Deep retry 시작: 실패 {len(failed_indices)}건 (동시성=1, 보수적 딜레이)")
    print("=" * 60)

    prepared_rows = _prepare_download_records(df.iloc[failed_indices].reset_index())
    for row in tqdm(prepared_rows, desc="Deep Retry"):
        idx = int(row.get("index", row.get("_row_index", 0)))
        pdf_save_dir = oa_pdf_dir if row["open_access"] else ca_pdf_dir
        artifact_dir = oa_artifact_dir if row["open_access"] else ca_artifact_dir
        result = download_process_worker(
            row,
            pdf_save_dir,
            artifact_dir,
            attempt=2,
            mode="deep",
            headless=bool(headless),
            abort_on_landing_block=bool(abort_on_landing_block),
            pacing_state=pacing_state,
            pacing_lock=pacing_lock,
            publisher_cooldown_sec=float(publisher_cooldown_sec),
            global_start_spacing_sec=float(global_start_spacing_sec),
            jitter_min_sec=float(jitter_min_sec),
            jitter_max_sec=float(jitter_max_sec),
        )
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
    event_counts: Dict[str, int] = {}
    if os.path.exists(attempts_jsonl_path):
        with open(attempts_jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record_type = str(record.get("record_type") or "attempt_result")
                if record_type != "attempt_result":
                    event_name = str(record.get("event") or record_type)
                    event_counts[event_name] = event_counts.get(event_name, 0) + 1
                    continue
                records.append(record)

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
        "event_counts": event_counts,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _parse_json_column(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    if isinstance(value, (list, dict)):
        return value
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _coerce_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, (int, float)):
        return bool(value)
    raw = str(value or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off", ""}:
        return False
    return bool(value)


def _write_metadata_sidecars(df: pd.DataFrame, metadata_root_dir: str, pdf_root_dir: str) -> Dict[str, Any]:
    os.makedirs(metadata_root_dir, exist_ok=True)
    written = 0
    missing_pdf = 0
    skipped = 0
    removed_stale_skip_sidecars = 0

    for _, row in df.iterrows():
        doi = str(row.get("doi") or "").strip()
        if not doi:
            continue

        pdf_filename = _sanitize_doi_to_filename(doi)
        json_filename = os.path.splitext(pdf_filename)[0] + ".json"
        is_open_access = _coerce_boolish(row.get("open_access"))
        access_dir = "Open_Access" if is_open_access else "Closed_Access"
        download_status = str(row.get("download_status") or "").strip().lower()
        if download_status == "skipped":
            skipped += 1
            for skip_bucket in ("Open_Access", "Closed_Access"):
                stale_path = os.path.join(metadata_root_dir, skip_bucket, json_filename)
                if os.path.exists(stale_path):
                    try:
                        os.remove(stale_path)
                        removed_stale_skip_sidecars += 1
                    except OSError:
                        pass
            continue

        pdf_path = os.path.join(pdf_root_dir, access_dir, pdf_filename)
        pdf_exists = os.path.exists(pdf_path)
        if not pdf_exists:
            missing_pdf += 1

        out_dir = os.path.join(metadata_root_dir, access_dir)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, json_filename)

        row_payload = {str(k): _json_safe_value(v) for k, v in row.to_dict().items()}
        journal_issn = _parse_json_column(row.get("journal_issn_json"), [])
        authors = _parse_json_column(row.get("authors_json"), [])
        openalex_payload = {
            "id": _json_safe_value(row.get("openalex_id")),
            "doi": doi,
            "title": _json_safe_value(row.get("title")),
            "publisher": _json_safe_value(row.get("publisher")),
            "journal": {
                "name": _json_safe_value(row.get("journal")),
                "id": _json_safe_value(row.get("journal_id")),
                "type": _json_safe_value(row.get("journal_type")),
                "issn_l": _json_safe_value(row.get("journal_issn_l")),
                "issn": journal_issn if isinstance(journal_issn, list) else [],
            },
            "publication_date": _json_safe_value(row.get("publication_date")),
            "publication_year": _json_safe_value(row.get("publication_year")),
            "work_type": _json_safe_value(row.get("work_type")),
            "cited_by_count": _json_safe_value(row.get("cited_by_count")),
            "citation_normalized_percentile": _json_safe_value(row.get("citation_normalized_percentile")),
            "pdf_url": _json_safe_value(row.get("pdf_url")),
            "open_access": bool(is_open_access),
            "author_count": _json_safe_value(row.get("author_count")),
            "first_author": _json_safe_value(row.get("first_author")),
            "authors_display": _json_safe_value(row.get("authors_display")),
            "authors": authors if isinstance(authors, list) else [],
        }
        payload = {
            "doi": doi,
            "pdf_filename": pdf_filename,
            "json_filename": json_filename,
            "access_bucket": access_dir,
            "pdf_path": os.path.abspath(pdf_path),
            "pdf_exists": bool(pdf_exists),
            "openalex": openalex_payload,
            "record": row_payload,
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        written += 1

    return {
        "root_dir": os.path.abspath(metadata_root_dir),
        "written": int(written),
        "missing_pdf": int(missing_pdf),
        "skipped": int(skipped),
        "removed_stale_skip_sidecars": int(removed_stale_skip_sidecars),
    }


def _discover_session_seed_root(worker_profile_root: str, profile_name: str) -> str:
    base = os.path.abspath(str(worker_profile_root or "").strip())
    profile_name = str(profile_name or "Default").strip() or "Default"
    if not base or not os.path.isdir(base):
        return ""

    marker_hits: List[str] = []
    for root, _, files in os.walk(base):
        if ".codex_profile_seed_ready" in files and os.path.isdir(os.path.join(root, profile_name)):
            marker_hits.append(root)
    if marker_hits:
        marker_hits.sort()
        return marker_hits[0]

    direct_profile = os.path.join(base, profile_name)
    if os.path.isdir(direct_profile):
        return base
    return ""


def _run_landing_precheck(
    df: pd.DataFrame,
    run_output_dir: str,
    max_workers: int,
    headless: bool,
    execution_env: str,
    runtime_preset: str,
    profile_mode: str,
    profile_name: str,
    persistent_profile_dir: str,
    publisher_cooldown_sec: float,
    global_start_spacing_sec: float,
    jitter_min_sec: float,
    jitter_max_sec: float,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    landing_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "landing_access_repro.py")
    precheck_dir = os.path.join(run_output_dir, "landing_precheck")
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
        landing_script,
        "--runtime-preset",
        str(runtime_preset or "auto"),
        "--input",
        landing_input_csv,
        "--workers",
        str(max(1, min(int(max_workers), 2))),
        "--headless",
        "1" if bool(headless) else "0",
        "--execution-env",
        str(execution_env or "auto"),
        "--publisher-cooldown-sec",
        str(float(publisher_cooldown_sec)),
        "--global-start-spacing-sec",
        str(float(global_start_spacing_sec)),
        "--jitter-min-sec",
        str(float(jitter_min_sec)),
        "--jitter-max-sec",
        str(float(jitter_max_sec)),
        "--profile-mode",
        str(profile_mode or "auto"),
        "--profile-name",
        str(profile_name or "Default"),
        "--persistent-profile-dir",
        os.path.abspath(os.path.expanduser(str(persistent_profile_dir or "outputs/.chrome_user_data"))),
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
    session_seed_root = _discover_session_seed_root(
        worker_profile_root=str(landing_report.get("worker_profile_root") or ""),
        profile_name=str(landing_report.get("profile_name") or "Default"),
    )

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
        "session_seed_root": session_seed_root,
        "report_summary": landing_report.get("summary", {}),
    }
    return eligible_df, metrics


def main(
    max_num=1000,
    citation_percentile=0.99,
    query=None,
    max_workers=1,
    output_dir="outputs/paper_download_run",
    pdf_output_dir=None,
    doi_path=None,
    after_first_pass="stop",
    non_interactive=False,
    precheck_landing=False,
    headless=None,
    execution_env="auto",
    deep_retry_headless=None,
    abort_on_landing_block=True,
    publisher_cooldown_sec=7.0,
    global_start_spacing_sec=1.5,
    jitter_min_sec=0.7,
    jitter_max_sec=1.8,
    profile_mode="auto",
    profile_name="Default",
    persistent_profile_dir="outputs/.chrome_user_data",
    runtime_profile_root="",
    runtime_preset_requested="auto",
    runtime_preset_resolved="",
):
    start_time = time.time()
    max_workers = max(1, min(int(max_workers), SAFE_MAX_WORKERS))
    worker_max_tasks_per_child = _resolve_worker_max_tasks_per_child()
    worker_recycle_supported = _process_pool_supports_max_tasks_per_child()
    effective_worker_max_tasks_per_child = (
        worker_max_tasks_per_child if worker_recycle_supported else None
    )
    startup_orphan_reaped = 0
    shutdown_orphan_reaped = 0

    run_output_dir = _resolve_run_output_dir(output_dir)
    pdf_root_dir = _resolve_pdf_output_dir(pdf_output_dir, run_output_dir)
    oa_pdf_dir = os.path.join(pdf_root_dir, "Open_Access")
    ca_pdf_dir = os.path.join(pdf_root_dir, "Closed_Access")
    oa_artifact_dir = os.path.join(run_output_dir, "Open_Access")
    ca_artifact_dir = os.path.join(run_output_dir, "Closed_Access")
    os.makedirs(run_output_dir, exist_ok=True)
    os.makedirs(pdf_root_dir, exist_ok=True)
    os.makedirs(oa_pdf_dir, exist_ok=True)
    os.makedirs(ca_pdf_dir, exist_ok=True)
    os.makedirs(oa_artifact_dir, exist_ok=True)
    os.makedirs(ca_artifact_dir, exist_ok=True)
    startup_orphan_reaped = reap_stale_drission_orphan_browsers(current_pid=os.getpid())

    failed_jsonl_path = os.path.join(run_output_dir, "failed_papers.jsonl")
    summary_json_path = os.path.join(run_output_dir, "summary.json")
    attempts_jsonl_path = os.path.join(run_output_dir, "download_attempts.jsonl")
    attempts_summary_path = os.path.join(run_output_dir, "download_attempts_summary.json")
    os.environ["PDF_ATTEMPTS_JSONL"] = attempts_jsonl_path
    resolved_execution_env = resolve_browser_execution_env(execution_env)
    os.environ["PDF_BROWSER_EXECUTION_ENV"] = resolved_execution_env
    os.environ["PDF_BROWSER_PROFILE_MODE"] = str(profile_mode or "auto").strip().lower() or "auto"
    os.environ["PDF_BROWSER_PROFILE_NAME"] = str(profile_name or "Default").strip() or "Default"
    os.environ["PDF_BROWSER_PERSISTENT_PROFILE_DIR"] = os.path.abspath(
        os.path.expanduser(str(persistent_profile_dir or "outputs/.chrome_user_data"))
    )
    os.environ.pop("PDF_BROWSER_SESSION_SEED_ROOT", None)
    runtime_profile_root = str(runtime_profile_root or "").strip()
    if not runtime_profile_root:
        runtime_base = os.environ.get("SLURM_TMPDIR", "").strip() or os.path.join("/tmp", os.environ.get("USER", "user"))
        runtime_label = os.path.basename(os.path.normpath(run_output_dir)) or "paper_download_run"
        runtime_profile_root = os.path.join(runtime_base, "download_runtime_profiles", runtime_label)
    os.environ["PDF_BROWSER_RUNTIME_PROFILE_ROOT"] = os.path.abspath(runtime_profile_root)
    failed_dedupe_keys = _load_failed_dedupe_keys(failed_jsonl_path)

    ta_query = (
        "('solid-state electrolyte' OR 'solid electrolyte') AND 'battery' AND 'Li' "
        "NOT ('review' OR 'opinion' OR 'perspective' OR 'survey' OR 'commentary')"
        if query is None
        else query
    )

    if doi_path:
        csv_path = doi_path
    else:
        from openalex_search import main_search

        csv_path = main_search(
            run_output_dir,
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
    print(
        "runtime preset: "
        f"requested={str(runtime_preset_requested or 'auto')}, "
        f"resolved={str(runtime_preset_resolved or runtime_preset_requested or 'auto')}"
    )
    if worker_max_tasks_per_child is None:
        worker_recycle_label = "disabled"
    elif effective_worker_max_tasks_per_child is None:
        worker_recycle_label = "disabled (runtime unsupported)"
    else:
        worker_recycle_label = str(effective_worker_max_tasks_per_child)
    print(f"worker recycle(max_tasks_per_child): {worker_recycle_label}")
    if startup_orphan_reaped:
        print(f"시작 전 stale headless Chrome 정리: {startup_orphan_reaped}개")

    requested_headless = _env_flag("PDF_BROWSER_HEADLESS", 0) if headless is None else bool(headless)
    requested_deep_retry_headless = requested_headless if deep_retry_headless is None else bool(deep_retry_headless)
    resolved_headless = coerce_headless_for_execution_env(
        requested_headless,
        resolved_execution_env,
        context="download_first_pass",
    )
    resolved_deep_retry_headless = coerce_headless_for_execution_env(
        requested_deep_retry_headless,
        resolved_execution_env,
        context="download_deep_retry",
    )
    resolved_abort_on_landing_block = bool(abort_on_landing_block)
    if resolved_execution_env == "linux_cli" and (not requested_headless or not requested_deep_retry_headless):
        print("execution_env=linux_cli 이므로 headful 요청은 무시하고 headless로 강제합니다.")
    print(
        f"브라우저 모드(first/deep): {'headless' if resolved_headless else 'headful'} / "
        f"{'headless' if resolved_deep_retry_headless else 'headful'}"
    )
    print(f"브라우저 실행 환경: {resolved_execution_env}")
    print(f"landing challenge/block 즉시 중단: {resolved_abort_on_landing_block}")
    print(
        "download publisher pacing: "
        f"cooldown={float(publisher_cooldown_sec):.1f}s, "
        f"global_spacing={float(global_start_spacing_sec):.1f}s, "
        f"jitter=[{float(jitter_min_sec):.1f}, {float(jitter_max_sec):.1f}]s"
    )
    print(f"런 산출물 경로: {run_output_dir}")
    print(f"PDF 저장 경로: {pdf_root_dir}")

    input_total_before_precheck = int(len(df))
    landing_precheck_metrics: Dict[str, Any] = {"executed": False}
    if precheck_landing:
        print("\n" + "=" * 60)
        print("Landing precheck 시작")
        print("=" * 60)
        df, landing_precheck_metrics = _run_landing_precheck(
            df=df,
            run_output_dir=run_output_dir,
            max_workers=max_workers,
            headless=resolved_headless,
            execution_env=resolved_execution_env,
            runtime_preset=str(runtime_preset_resolved or runtime_preset_requested or "auto"),
            profile_mode=profile_mode,
            profile_name=profile_name,
            persistent_profile_dir=persistent_profile_dir,
            publisher_cooldown_sec=publisher_cooldown_sec,
            global_start_spacing_sec=global_start_spacing_sec,
            jitter_min_sec=jitter_min_sec,
            jitter_max_sec=jitter_max_sec,
        )
        print(
            f"Landing precheck 완료: 성공={landing_precheck_metrics.get('landing_success', 0)} / "
            f"권한없음={landing_precheck_metrics.get('access_rights_failures', 0)} / "
            f"다운로드 투입={landing_precheck_metrics.get('eligible_for_download', 0)}"
        )
        session_seed_root = str(landing_precheck_metrics.get("session_seed_root") or "").strip()
        if session_seed_root:
            os.environ["PDF_BROWSER_SESSION_SEED_ROOT"] = session_seed_root
            print(f"landing session seed root: {session_seed_root}")

    with Manager() as manager:
        pacing_state = manager.dict()
        pacing_lock = manager.Lock()

        first_results = _first_pass(
            df,
            oa_pdf_dir,
            ca_pdf_dir,
            oa_artifact_dir,
            ca_artifact_dir,
            max_workers=max_workers,
            headless=resolved_headless,
            abort_on_landing_block=resolved_abort_on_landing_block,
            publisher_cooldown_sec=float(publisher_cooldown_sec),
            global_start_spacing_sec=float(global_start_spacing_sec),
            jitter_min_sec=float(jitter_min_sec),
            jitter_max_sec=float(jitter_max_sec),
            worker_max_tasks_per_child=effective_worker_max_tasks_per_child,
            pacing_state=pacing_state,
            pacing_lock=pacing_lock,
        )

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
            deep_results = _deep_retry(
                df,
                first_results,
                oa_pdf_dir,
                ca_pdf_dir,
                oa_artifact_dir,
                ca_artifact_dir,
                headless=resolved_deep_retry_headless,
                abort_on_landing_block=resolved_abort_on_landing_block,
                publisher_cooldown_sec=float(publisher_cooldown_sec),
                global_start_spacing_sec=float(global_start_spacing_sec),
                jitter_min_sec=float(jitter_min_sec),
                jitter_max_sec=float(jitter_max_sec),
                pacing_state=pacing_state,
                pacing_lock=pacing_lock,
            )

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
    final_results = list(first_results)
    for item in deep_results:
        idx = item["index"]
        final_results[idx] = item
    df["landing_attempted"] = [bool(r.get("landing_attempted")) for r in final_results]
    df["landing_success"] = [bool(r.get("landing_success")) for r in final_results]
    df["landing_state"] = [str(r.get("landing_state") or "not_attempted") for r in final_results]
    df["landing_url"] = [str(r.get("landing_url") or "") for r in final_results]
    df["landing_title"] = [str(r.get("landing_title") or "") for r in final_results]
    df["browser_session_mode"] = [str(r.get("browser_session_mode") or "") for r in final_results]
    df["browser_session_source"] = [str(r.get("browser_session_source") or "") for r in final_results]
    df["browser_profile_name"] = [str(r.get("browser_profile_name") or "") for r in final_results]
    df["browser_user_data_dir"] = [str(r.get("browser_user_data_dir") or "") for r in final_results]
    df["scheduler_publisher"] = [str(r.get("scheduler_publisher") or "") for r in final_results]
    df["scheduled_start_ms"] = [int(r.get("scheduled_start_ms", 0) or 0) for r in final_results]
    df["actual_start_ms"] = [int(r.get("actual_start_ms", 0) or 0) for r in final_results]
    df["pacing_wait_ms"] = [int(r.get("pacing_wait_ms", 0) or 0) for r in final_results]
    df["pacing_jitter_sec"] = [float(r.get("pacing_jitter_sec", 0.0) or 0.0) for r in final_results]

    metadata_dir = os.path.join(run_output_dir, "metadata")
    metadata_manifest = _write_metadata_sidecars(
        df=df,
        metadata_root_dir=metadata_dir,
        pdf_root_dir=pdf_root_dir,
    )

    full_csv_path = os.path.join(run_output_dir, "openalex_search_results_parallel.csv")
    df.to_csv(full_csv_path, index=False, encoding="utf-8-sig")

    failed_df = df[~df["download_status"].str.contains("Success", case=False, na=False)]
    failed_csv_path = os.path.join(run_output_dir, "failed_papers.csv")
    failed_df.to_csv(failed_csv_path, index=False, encoding="utf-8-sig")

    shutdown_orphan_reaped = reap_stale_drission_orphan_browsers(current_pid=os.getpid())
    live_metrics = _summarize_live_attempt_metrics(attempts_jsonl_path, attempts_summary_path)
    integrated_landing_metrics = _summarize_integrated_landing(final_results)

    summary_payload = {
        "generated_at": int(time.time()),
        "input_total_before_precheck": input_total_before_precheck,
        "total_papers": int(len(df)),
        "after_first_pass": decision,
        "non_interactive": bool(non_interactive),
        "precheck_landing": bool(precheck_landing),
        "paths": {
            "run_output_dir": run_output_dir,
            "pdf_output_dir": pdf_root_dir,
            "open_access_pdf_dir": oa_pdf_dir,
            "closed_access_pdf_dir": ca_pdf_dir,
        },
        "download_browser": {
            "runtime_preset_requested": str(runtime_preset_requested or "auto"),
            "runtime_preset_resolved": str(runtime_preset_resolved or runtime_preset_requested or "auto"),
            "execution_env": resolved_execution_env,
            "headless": bool(resolved_headless),
            "deep_retry_headless": bool(resolved_deep_retry_headless),
            "abort_on_landing_block": bool(resolved_abort_on_landing_block),
            "profile_mode": os.environ.get("PDF_BROWSER_PROFILE_MODE", ""),
            "profile_name": os.environ.get("PDF_BROWSER_PROFILE_NAME", ""),
            "persistent_profile_dir": os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", ""),
            "runtime_profile_root": os.environ.get("PDF_BROWSER_RUNTIME_PROFILE_ROOT", ""),
            "session_seed_root": os.environ.get("PDF_BROWSER_SESSION_SEED_ROOT", ""),
        },
        "download_scheduler": {
            "publisher_cooldown_sec": float(publisher_cooldown_sec),
            "global_start_spacing_sec": float(global_start_spacing_sec),
            "jitter_min_sec": float(jitter_min_sec),
            "jitter_max_sec": float(jitter_max_sec),
            "worker_max_tasks_per_child_requested": worker_max_tasks_per_child,
            "worker_max_tasks_per_child": effective_worker_max_tasks_per_child,
            "worker_recycle_supported": bool(worker_recycle_supported),
        },
        "landing_precheck": landing_precheck_metrics,
        "integrated_landing": integrated_landing_metrics,
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
            "pdf_root_dir": pdf_root_dir,
            "metadata_root_dir": metadata_manifest.get("root_dir", metadata_dir),
        },
        "metadata_sidecars": metadata_manifest,
        "process_cleanup": {
            "startup_orphan_reaped": int(startup_orphan_reaped),
            "shutdown_orphan_reaped": int(shutdown_orphan_reaped),
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
    print(f"메타데이터(JSON) 경로: {metadata_manifest.get('root_dir', metadata_dir)}")
    if startup_orphan_reaped or shutdown_orphan_reaped:
        print(
            "stale headless Chrome 정리: "
            f"start={int(startup_orphan_reaped)}, end={int(shutdown_orphan_reaped)}"
        )
    print("=" * 60)


if __name__ == "__main__":
    args = get_config()
    runtime_config = apply_runtime_preset(args, workflow="download")
    main(
        max_num=args.max_num,
        citation_percentile=args.citation_percentile,
        query=args.query,
        max_workers=args.max_workers,
        output_dir=args.output_dir,
        pdf_output_dir=args.pdf_output_dir,
        doi_path=args.doi_path,
        after_first_pass=args.after_first_pass,
        non_interactive=args.non_interactive,
        precheck_landing=bool(int(args.precheck_landing)),
        headless=args.headless,
        execution_env=args.execution_env,
        deep_retry_headless=args.deep_retry_headless,
        abort_on_landing_block=bool(int(args.abort_on_landing_block)),
        publisher_cooldown_sec=args.publisher_cooldown_sec,
        global_start_spacing_sec=args.global_start_spacing_sec,
        jitter_min_sec=args.jitter_min_sec,
        jitter_max_sec=args.jitter_max_sec,
        profile_mode=args.profile_mode,
        profile_name=args.profile_name,
        persistent_profile_dir=args.persistent_profile_dir,
        runtime_profile_root=args.runtime_profile_root,
        runtime_preset_requested=getattr(args, "runtime_preset_requested", getattr(args, "runtime_preset", "auto")),
        runtime_preset_resolved=getattr(args, "runtime_preset_resolved", runtime_config.preset_name),
    )
