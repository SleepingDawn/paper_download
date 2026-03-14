import argparse
import json
import os
import random
import re
import shutil
import socket
import subprocess
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
from statistics import median
from typing import Any, Dict, List, Sequence, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlunparse
from urllib.request import urlopen

from DrissionPage import ChromiumOptions, ChromiumPage
from bs4 import BeautifulSoup
import requests

from landing_classifier import (
    collect_page_snapshot,
    DEFAULT_GLOBAL_START_SPACING_SEC,
    DEFAULT_JITTER_MAX_SEC,
    DEFAULT_JITTER_MIN_SEC,
    DEFAULT_MAX_NAV_ATTEMPTS,
    DEFAULT_PER_DOI_DEADLINE_SEC,
    DEFAULT_PER_PUBLISHER_COOLDOWN_SEC,
    SAFE_LANDING_MAX_WORKERS,
    STATE_BLANK_OR_INCOMPLETE,
    STATE_BROKEN_JS_SHELL,
    STATE_CHALLENGE_DETECTED,
    STATE_CONSENT_OR_INTERSTITIAL_BLOCK,
    STATE_DIRECT_PDF_HANDOFF,
    STATE_DOI_NOT_FOUND,
    STATE_DOMAIN_MISMATCH,
    STATE_NETWORK_ERROR,
    STATE_PUBLISHER_ERROR,
    STATE_SUCCESS_LANDING,
    STATE_TIMEOUT,
    STATE_UNKNOWN_NON_SUCCESS,
    SUCCESS_STATES,
    chunk_inputs_round_robin,
    classify_landing,
    compact_text_signature,
    expected_domains_for_record,
    load_landing_inputs,
    release_pacing_slot,
    render_experiment_markdown,
    reorder_inputs_for_pacing,
    reserve_pacing_slot,
    stabilize_page_state,
    suggest_remaining_weak_spots,
    summarize_classifier_states,
    _extract_main_like_text,
    _strip_visible_text,
)
from tools_exp import (
    _adopt_latest_tab,
    _apply_best_browser_profile,
    _capture_direct_downloaded_pdf,
    _click_elsevier_doi_link_in_retrieve,
    _dismiss_cookie_or_consent_banner,
    _extract_elsevier_retrieve_handoff_url,
    _get_current_files,
    _has_article_signal,
    _has_cookie_or_consent_signal,
    _has_pdf_action_signal,
    _is_elsevier_retrieve_url,
    _sanitize_doi_to_filename,
    build_elsevier_safe_entry_plan,
    build_landing_browser_session_plan,
    coerce_headless_for_execution_env,
    detect_access_issue,
    ensure_runtime_profile_ready,
    resolve_browser_executable,
    resolve_browser_execution_env,
    resolve_runtime_preset,
    _wait_for_elsevier_article_ready,
)

OUT_SUCCESS_ACCESS = "SUCCESS_ACCESS"
OUT_FAIL_CAPTCHA = "FAIL_CAPTCHA"
OUT_FAIL_BLOCK = "FAIL_BLOCK"
OUT_FAIL_ACCESS_RIGHTS = "FAIL_ACCESS_RIGHTS"
OUT_FAIL_DOI_NOT_FOUND = "FAIL_DOI_NOT_FOUND"
OUT_FAIL_NETWORK = "FAIL_NETWORK"
PROBE_PAGE_MODE_REUSE = "reuse_page"
PROBE_PAGE_MODE_FRESH_TAB = "fresh_tab"
INTERSTITIAL_TITLES = {"redirecting", "redirecting...", "loading", "please wait", "just a moment"}
DEFAULT_LOCAL_LANDING_WORKERS = 1
DEFAULT_LOCAL_HEADLESS = 0
DEFAULT_LOCAL_TIMEOUT_SEC = 15.0
DEFAULT_LOCAL_PER_DOI_DEADLINE_SEC = 45.0
ELSEVIER_ARTICLE_HOST_MARKERS = ("sciencedirect.com", "cell.com", "thelancet.com")
PDF_MIME_MARKERS = ("application/pdf", "application/x-pdf")
PDF_URL_MARKERS = (".pdf", "/pdf", "download=true", "pdfft")
APS_JOURNAL_SLUGS = {
    "physreva": "pra",
    "physrevb": "prb",
    "physrevc": "prc",
    "physrevd": "prd",
    "physreve": "pre",
    "physrevx": "prx",
    "physrevlett": "prl",
    "physrevapplied": "prapplied",
    "physrevmaterials": "prmaterials",
    "physrevresearch": "prresearch",
    "physrevfluids": "prfluids",
    "physrevaccelbeams": "prab",
    "revmodphys": "rmp",
}


def _is_headless_browser() -> bool:
    return os.getenv("PDF_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes")


def _browser_for_worker(
    chrome_path: str,
    worker_idx: int,
    worker_profile_root: str,
    worker_download_root: str,
    session_plan: Dict[str, Any],
    startup_retries: int = 3,
    retry_sleep_sec: float = 1.5,
) -> ChromiumPage:
    startup_retries = max(1, int(startup_retries))
    worker_profile_root = os.path.abspath(worker_profile_root)
    worker_download_root = os.path.abspath(worker_download_root)
    os.makedirs(worker_profile_root, exist_ok=True)
    os.makedirs(worker_download_root, exist_ok=True)
    worker_profile_name = str(session_plan.get("profile_name") or f"worker_{int(worker_idx)}").strip() or f"worker_{int(worker_idx)}"
    worker_user_data_dir = os.path.abspath(str(session_plan.get("user_data_dir") or worker_profile_root))
    worker_download_key = (
        _sanitize_doi_to_filename(str(session_plan.get("cache_key") or worker_profile_name)) or f"worker_{int(worker_idx)}"
    )
    worker_download_dir = os.path.join(worker_download_root, worker_download_key)
    os.makedirs(worker_user_data_dir, exist_ok=True)
    os.makedirs(worker_download_dir, exist_ok=True)

    last_err = None
    for _ in range(startup_retries):
        co = ChromiumOptions()
        if chrome_path and os.path.exists(chrome_path):
            co.set_browser_path(chrome_path)
        co.set_user_data_path(worker_user_data_dir)
        co.set_user(worker_profile_name)
        co.set_download_path(worker_download_dir)
        co.set_pref("download.prompt_for_download", False)
        co.set_pref("plugins.always_open_pdf_externally", True)
        co.set_pref("profile.default_content_settings.popups", 0)
        co.auto_port()
        _apply_best_browser_profile(co)
        try:
            return ChromiumPage(co)
        except Exception as e:
            last_err = e
            time.sleep(max(0.3, float(retry_sleep_sec)))

    raise RuntimeError(
        f"browser_init_failed(worker={worker_idx}, chrome_path={chrome_path}, "
        f"profile_root={worker_user_data_dir}, profile={worker_profile_name}, "
        f"session_mode={session_plan.get('session_mode')}, session_source={session_plan.get('session_source')}): {last_err}"
    )


def _resolve_browser_path(preferred_path: str) -> str:
    return resolve_browser_executable(preferred_path)


def _pick_free_local_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def _run_chrome_smoke(chrome_path: str, profile_root: str) -> Dict[str, str]:
    smoke_dir = os.path.join(profile_root, "_smoke")
    shutil.rmtree(smoke_dir, ignore_errors=True)
    os.makedirs(smoke_dir, exist_ok=True)
    port = _pick_free_local_port()

    cmd = [
        chrome_path,
        "--headless=new",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-domain-reliability",
        "--disable-sync",
        "--disable-extensions",
        "--metrics-recording-only",
        "--disable-features=MediaRouter,OptimizationHints",
        f"--user-data-dir={smoke_dir}",
        f"--remote-debugging-port={port}",
        "--remote-debugging-address=127.0.0.1",
        "about:blank",
    ]
    proc = None
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 25
        version_body = ""
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            try:
                with urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1.2) as resp:
                    version_body = (resp.read() or b"").decode("utf-8", errors="ignore")
                if version_body:
                    return {
                        "ok": "1",
                        "stderr": "",
                        "stdout": version_body[-500:],
                        "mode": "normal",
                        "returncode": "0",
                    }
            except Exception:
                time.sleep(0.5)
    except Exception as e:
        return {"ok": "0", "stderr": str(e), "stdout": "", "mode": "normal"}
    finally:
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    out = ""
    err = ""
    if proc is not None:
        try:
            out, err = proc.communicate(timeout=1)
        except Exception:
            pass
    return {
        "ok": "0",
        "stderr": (err or "")[-2000:],
        "stdout": (out or "")[-500:],
        "mode": "normal",
        "returncode": str(proc.returncode if proc is not None and proc.returncode is not None else -1),
    }


def _append_nav_step(chain: List[Dict[str, str]], step: str, requested_url: str, observed_url: str) -> None:
    chain.append(
        {
            "step": str(step or ""),
            "requested_url": str(requested_url or ""),
            "observed_url": str(observed_url or ""),
        }
    )


def _dedupe_url_chain(chain: Sequence[Dict[str, str]]) -> List[str]:
    ordered = []
    seen = set()
    for row in chain:
        for candidate in (row.get("requested_url"), row.get("observed_url")):
            url = str(candidate or "").strip()
            if not url or url in seen:
                continue
            ordered.append(url)
            seen.add(url)
    return ordered


def _extract_domain(url: str) -> str:
    raw = str(url or "").strip()
    if "://" not in raw:
        return ""
    try:
        return raw.split("/", 3)[2].lower()
    except Exception:
        return ""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_placeholder_landing(url: str, title: str, html: str) -> bool:
    low_url = str(url or "").strip().lower()
    low_title = str(title or "").strip().lower()
    html_len = len(str(html or ""))
    if not low_url or low_url.startswith("about:blank") or low_url.startswith("chrome://"):
        return True
    if low_url.endswith("doi.org") or "/doi.org/" in low_url:
        return True
    if low_title in ("", "about:blank", "loading", "redirecting", "redirecting...", "please wait"):
        return True
    return html_len < 220 and low_title == low_url.replace("https://", "").replace("http://", "")


def _wait_for_non_placeholder_state(
    page: ChromiumPage,
    deadline_monotonic: float,
    max_wait_sec: float = 3.5,
    poll_sec: float = 0.35,
) -> Tuple[str, str, str, int]:
    waited_ms = 0
    last_url = str(page.url or "")
    last_title = str(page.title or "")
    last_html = str(page.html or "")
    if not _is_placeholder_landing(last_url, last_title, last_html):
        return last_url, last_title, last_html, waited_ms

    start = time.perf_counter()
    limit = min(max(0.0, max_wait_sec), max(0.0, deadline_monotonic - time.monotonic()))
    while time.perf_counter() - start < limit and time.monotonic() < deadline_monotonic:
        time.sleep(max(0.15, poll_sec))
        last_url = str(page.url or last_url)
        last_title = str(page.title or last_title)
        last_html = str(page.html or last_html)
        waited_ms = int((time.perf_counter() - start) * 1000)
        if not _is_placeholder_landing(last_url, last_title, last_html):
            break
    return last_url, last_title, last_html, waited_ms


def _open_probe_page(controller_page: ChromiumPage, probe_page_mode: str) -> Tuple[ChromiumPage, Dict[str, Any]]:
    mode = str(probe_page_mode or PROBE_PAGE_MODE_REUSE).strip().lower()
    controller_tab_id = str(getattr(controller_page, "tab_id", "") or "")
    meta = {
        "probe_page_mode": mode,
        "fresh_tab": False,
        "controller_tab_id": controller_tab_id,
        "probe_tab_id": controller_tab_id,
        "open_error": "",
    }
    if mode != PROBE_PAGE_MODE_FRESH_TAB:
        return controller_page, meta

    try:
        probe_page = controller_page.new_tab("about:blank", background=False)
        probe_tab_id = str(getattr(probe_page, "tab_id", "") or "")
        meta.update({"fresh_tab": True, "probe_tab_id": probe_tab_id or controller_tab_id})
        time.sleep(0.2)
        return probe_page, meta
    except Exception as exc:
        meta.update(
            {
                "probe_page_mode": f"{PROBE_PAGE_MODE_REUSE}_fallback",
                "probe_tab_id": controller_tab_id,
                "open_error": str(exc)[:240],
            }
        )
        return controller_page, meta


def _close_probe_page(controller_page: ChromiumPage, probe_page: ChromiumPage, page_meta: Dict[str, Any]) -> None:
    if probe_page is None or probe_page is controller_page:
        return

    probe_tab_id = str(page_meta.get("probe_tab_id") or getattr(probe_page, "tab_id", "") or "")
    controller_tab_id = str(page_meta.get("controller_tab_id") or getattr(controller_page, "tab_id", "") or "")
    try:
        probe_page.close()
    except Exception:
        if probe_tab_id:
            try:
                controller_page.close_tabs(probe_tab_id)
            except Exception:
                pass
    if controller_tab_id:
        try:
            controller_page.activate_tab(controller_tab_id)
        except Exception:
            pass


def _open_temporary_tab(page: ChromiumPage, start_url: str = "about:blank") -> ChromiumPage | None:
    if page is None:
        return None
    try:
        temp_page = page.new_tab(start_url, background=False)
        time.sleep(0.2)
        return temp_page
    except Exception:
        return None


def _close_temporary_tab(page: ChromiumPage, temp_page: ChromiumPage | None) -> None:
    if page is None or temp_page is None or temp_page is page:
        return
    temp_tab_id = str(getattr(temp_page, "tab_id", "") or "")
    current_tab_id = str(getattr(page, "tab_id", "") or "")
    try:
        temp_page.close()
    except Exception:
        if temp_tab_id:
            try:
                page.close_tabs(temp_tab_id)
            except Exception:
                pass
    if current_tab_id:
        try:
            page.activate_tab(current_tab_id)
        except Exception:
            pass


def _prune_extra_tabs(page: ChromiumPage) -> None:
    if page is None:
        return
    try:
        current_tab_id = str(getattr(page, "tab_id", "") or "")
        tab_ids = list(getattr(page, "tab_ids", []) or [])
        for tab_id in tab_ids:
            tid = str(tab_id or "")
            if not tid or tid == current_tab_id:
                continue
            try:
                page.close_tabs(tid)
            except Exception:
                pass
        if current_tab_id:
            try:
                page.activate_tab(current_tab_id)
            except Exception:
                pass
    except Exception:
        pass


def _looks_like_blank_screen_context(final_url: str, title: str, html: str) -> bool:
    low_url = str(final_url or "").strip().lower()
    low_title = str(title or "").strip().lower()
    html_len = len(str(html or ""))
    if low_url.startswith("about:blank") or low_url.startswith("chrome://"):
        return True
    if _is_elsevier_retrieve_url(low_url):
        return True
    if low_title in ("", "about:blank", "loading", "redirecting", "redirecting...", "please wait"):
        return html_len < 1200
    if html_len < 220 and (not low_title or low_title == low_url.replace("https://", "").replace("http://", "")):
        return True
    return False


def _record_tab_transition(
    tab_transition_events: List[Dict[str, Any]],
    step_label: str,
    from_page: ChromiumPage,
    to_page: ChromiumPage,
    *,
    forced: bool = False,
) -> None:
    if to_page is None or from_page is None or to_page is from_page:
        return
    tab_transition_events.append(
        {
            "step": step_label,
            "forced": bool(forced),
            "from_tab_id": str(getattr(from_page, "tab_id", "") or ""),
            "to_tab_id": str(getattr(to_page, "tab_id", "") or ""),
            "from_url": str(getattr(from_page, "url", "") or ""),
            "to_url": str(getattr(to_page, "url", "") or ""),
            "from_title": str(getattr(from_page, "title", "") or "")[:160],
            "to_title": str(getattr(to_page, "title", "") or "")[:160],
        }
    )


def _adopt_latest_probe_tab(
    page: ChromiumPage,
    *,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
    tab_transition_events: List[Dict[str, Any]],
    step_label: str,
    force: bool = False,
) -> ChromiumPage:
    if page is None:
        return page
    current_url = str(page.url or "")
    current_title = str(page.title or "")
    current_html = ""
    if not force:
        try:
            current_html = str(page.html or "")
        except Exception:
            current_html = ""
        if not _looks_like_blank_screen_context(current_url, current_title, current_html):
            return page
    adopted = _adopt_latest_tab(page)
    if adopted is None or adopted is page:
        return page
    _record_tab_transition(tab_transition_events, step_label, page, adopted, forced=force)
    _append_nav_step(
        navigation_chain,
        step_label,
        current_url or "about:blank",
        str(getattr(adopted, "url", "") or current_url or "about:blank"),
    )
    attempt_timing.setdefault("tab_transition_steps", []).append(step_label)
    try:
        _dismiss_cookie_or_consent_banner(adopted)
    except Exception:
        pass
    return adopted


def _remaining_budget(deadline_monotonic: float, preferred_sec: float, floor_sec: float = 3.0) -> float:
    remaining = deadline_monotonic - time.monotonic()
    return max(float(floor_sec), min(float(preferred_sec), remaining))


def _normalize_elsevier_article_url(final_url: str, snapshot: Dict[str, Any]) -> str:
    raw = str(final_url or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    if not any(host in low for host in ELSEVIER_ARTICLE_HOST_MARKERS):
        return ""
    if not any(token in low for token in ("/science/article/pii/", "/fulltext/")):
        return ""
    meta = dict(snapshot.get("meta") or {})
    for candidate in (
        str(snapshot.get("canonical_url") or "").strip(),
        str(meta.get("citation_fulltext_html_url") or "").strip(),
        str(meta.get("citation_abstract_html_url") or "").strip(),
        str(meta.get("og:url") or "").strip(),
    ):
        if candidate and candidate.lower() != low and any(host in candidate.lower() for host in ELSEVIER_ARTICLE_HOST_MARKERS):
            return candidate
    parsed = urlparse(raw)
    if parsed.query and any(token in parsed.query.lower() for token in ("_returnurl=", "via%3dihub", "via=ihub")):
        return urlunparse(parsed._replace(query="", fragment=""))
    return ""


def _requests_history_first_party_url(
    doi_url: str,
    allowed_domains: Sequence[str],
    prefer_tokens: Sequence[str] = (),
    reject_domains: Sequence[str] = (),
) -> str:
    try:
        resp = requests.get(
            str(doi_url or "").strip(),
            allow_redirects=True,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except Exception:
        return ""
    candidates: List[str] = []
    chain = [getattr(h, "url", "") for h in (resp.history or [])] + [resp.url]
    for candidate in chain:
        cand = str(candidate or "").strip()
        if not cand:
            continue
        domain = _extract_domain(cand)
        if allowed_domains and not any(domain == d or domain.endswith(f".{d}") for d in allowed_domains):
            continue
        low = cand.lower()
        if any(token in low for token in reject_domains):
            continue
        candidates.append(cand)
    if not candidates:
        return ""
    for token in prefer_tokens:
        low_token = str(token or "").lower()
        for candidate in reversed(candidates):
            if low_token and low_token in candidate.lower():
                return candidate
    return candidates[-1]


def _build_aps_first_party_url(doi: str) -> str:
    norm = _normalize_doi_text(doi)
    if not norm.startswith("10.1103/"):
        return ""
    suffix = norm.split("/", 1)[1]
    journal = suffix.split(".", 1)[0].lower()
    slug = APS_JOURNAL_SLUGS.get(journal)
    if not slug:
        return ""
    return f"https://journals.aps.org/{slug}/abstract/{norm}"


def _powdermat_entry_url(doi: str) -> str:
    norm = _normalize_doi_text(doi)
    if not norm.startswith("10.4150/"):
        return ""
    return f"https://www.powdermat.org/journal/view.php?doi={quote(norm, safe='/')}"


def _extract_redirect_param(url: str, key: str = "Redirect") -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    except Exception:
        return ""
    return str(params.get(key) or "").strip()


def _format_elsevier_pii(raw_pii: str) -> str:
    pii = str(raw_pii or "").strip().upper()
    if not re.fullmatch(r"[A-Z]\d{16}", pii):
        return ""
    return f"{pii[0]}{pii[1:5]}-{pii[5:9]}({pii[9:11]}){pii[11:16]}-{pii[16]}"


def _headless_preferred_elsevier_urls(url: str) -> List[str]:
    raw = str(url or "").strip()
    if not raw:
        return []
    candidates: List[str] = []
    seen = set()

    def _push(candidate: str) -> None:
        cand = str(candidate or "").strip()
        if not cand:
            return
        key = cand.lower()
        if key in seen:
            return
        seen.add(key)
        candidates.append(cand)

    parsed = urlparse(raw)
    low = raw.lower()
    if "www.sciencedirect.com/science/article/pii/" in low:
        queryless = urlunparse(parsed._replace(query="", fragment=""))
        _push(queryless)
        _push(queryless.replace("/science/article/pii/", "/science/article/abs/pii/"))
    if "linkinghub.elsevier.com/retrieve/" in low:
        redirect = _extract_redirect_param(raw, key="Redirect")
        if redirect:
            for candidate in _headless_preferred_elsevier_urls(redirect):
                _push(candidate)
    if "cell.com/" in low and "/retrieve/pii/" in low:
        m = re.search(r"/retrieve/pii/([A-Z0-9]+)", raw, flags=re.IGNORECASE)
        formatted_pii = _format_elsevier_pii(m.group(1)) if m else ""
        if formatted_pii:
            path = parsed.path.replace("/retrieve/pii/" + m.group(1), f"/fulltext/{formatted_pii}")
            host = parsed.netloc or "cell.com"
            if host == "cell.com":
                host = "www.cell.com"
            _push(urlunparse(parsed._replace(netloc=host, path=path, query="", fragment="")))
    _push(raw)
    return candidates


def _resolve_elsevier_structural_entry_url(doi_url: str) -> str:
    plan = build_elsevier_safe_entry_plan(doi_url)
    return str(
        plan.get("entry_browser_url")
        or plan.get("entry_resolved_url")
        or plan.get("entry_url")
        or ""
    )


def _should_skip_elsevier_browser_open(entry_plan: Dict[str, Any]) -> bool:
    issue = str(entry_plan.get("entry_preflight_issue") or "").strip()
    if not issue:
        return False
    if issue in ("PRECHECK_RESOLVE_FAILED", "PRECHECK_NO_ENTRY_URL", "PRECHECK_REQUEST_FAILED", OUT_FAIL_DOI_NOT_FOUND):
        return True
    return False


def _resolve_structural_entry_url(record: Dict[str, Any], doi_url: str) -> str:
    doi = _normalize_doi_text(record.get("doi") or "")
    if not doi:
        return ""
    if doi.startswith("10.1016/") and _is_headless_browser():
        return _resolve_elsevier_structural_entry_url(doi_url) or ""
    if doi.startswith("10.1103/"):
        return _build_aps_first_party_url(doi) or ""
    if doi.startswith("10.1364/"):
        return _requests_history_first_party_url(
            doi_url=doi_url,
            allowed_domains=("opg.optica.org",),
            prefer_tokens=("viewmedia.cfm", "abstract.cfm"),
            reject_domains=("validate.perfdrive.com", "captcha.perfdrive.com"),
        )
    if doi.startswith("10.4150/"):
        return _powdermat_entry_url(doi) or _requests_history_first_party_url(
            doi_url=doi_url,
            allowed_domains=("powdermat.org", "www.powdermat.org"),
            prefer_tokens=("/journal/view.php?doi=", "/journal/view.php?number="),
            reject_domains=("/authors/copyright_transfer_agreement.php",),
        )
    return ""


def _extract_ieee_doc_id(url: str) -> str:
    m = re.search(r"/document/(\d+)", str(url or "").lower())
    return m.group(1) if m else ""


def _ieee_abstract_fallback_url(final_url: str, title: str, html: str) -> str:
    raw = str(final_url or "").strip()
    low = raw.lower()
    if "ieeexplore.ieee.org/document/" not in low:
        return ""
    blob = " ".join([str(title or "").lower(), str(html or "").lower()[:12000]])
    if "page not found" not in blob and "request rejected" not in blob and "not found" not in blob:
        return ""
    doc_id = _extract_ieee_doc_id(raw)
    if not doc_id:
        return ""
    return f"https://ieeexplore.ieee.org/abstract/document/{doc_id}"


def _extract_site_search_article_url(final_url: str, doi: str, input_title: str, title: str, html: str) -> str:
    raw_url = str(final_url or "").strip()
    low_blob = " ".join([str(title or "").lower(), str(html or "").lower()[:30000]])
    if "copyright transfer agreement" not in low_blob and "for contributors" not in low_blob:
        return ""
    if not raw_url:
        return ""
    soup = BeautifulSoup(str(html or ""), "html.parser")
    form = None
    for candidate in soup.find_all("form"):
        action = str(candidate.get("action") or "")
        if "search" in action.lower():
            form = candidate
            break
    if not form:
        return ""
    action = urljoin(raw_url, str(form.get("action") or "").strip())
    field_name = ""
    for inp in form.find_all("input"):
        if str(inp.get("type") or "").lower() in ("text", "search"):
            field_name = str(inp.get("name") or "").strip()
            if field_name:
                break
    if not field_name:
        return ""
    parsed_origin = urlparse(raw_url)
    origin = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": raw_url,
        "Origin": origin,
    }
    queries = [str(doi or "").strip(), str(input_title or "").strip()]
    seen = set()
    for query in queries:
        q = " ".join(query.split()).strip()
        if len(q) < 8 or q in seen:
            continue
        seen.add(q)
        try:
            resp = session.post(action, data={field_name: q}, headers=headers, timeout=15)
        except Exception:
            continue
        if not resp.ok or not resp.text:
            continue
        rsoup = BeautifulSoup(resp.text, "html.parser")
        for link in rsoup.select("a[href]"):
            href = str(link.get("href") or "").strip()
            text = " ".join(link.stripped_strings)
            if not href:
                continue
            abs_url = urljoin(action, href)
            low_abs = abs_url.lower()
            if urlparse(abs_url).netloc and urlparse(abs_url).netloc != parsed_origin.netloc:
                continue
            if any(token in low_abs for token in ("/articles/archive", "/articles/current", "/articles/search", "/policy/")):
                continue
            if "/journal/view.php?number=" not in low_abs:
                continue
            if q.lower() in text.lower() or (str(doi or "").strip().lower() in text.lower()):
                return abs_url
            if text and str(input_title or "").strip() and text.lower() in str(input_title).lower():
                return abs_url
    return ""


def _extract_powdermat_article_url(final_url: str, doi: str, input_title: str, title: str, html: str) -> str:
    norm = _normalize_doi_text(doi)
    if not norm.startswith("10.4150/"):
        return ""
    direct = _powdermat_entry_url(norm)
    raw_url = str(final_url or "").strip()
    low_blob = " ".join([str(title or "").lower(), str(html or "").lower()[:40000]])
    if direct and raw_url and direct.lower() != raw_url.lower():
        if any(marker in low_blob for marker in ("copyright transfer agreement", "for contributors", "editorial policies")):
            return direct
    soup = BeautifulSoup(str(html or ""), "html.parser")
    candidates: List[str] = []
    for tag in soup.select("a[href], form[action]"):
        href = str(tag.get("href") or tag.get("action") or "").strip()
        if not href:
            continue
        abs_url = urljoin(raw_url or "https://www.powdermat.org/", href)
        low = abs_url.lower()
        if "powdermat.org" not in _extract_domain(abs_url):
            continue
        if "/journal/view.php?doi=" in low or "/journal/view.php?number=" in low:
            candidates.append(abs_url)
    if direct:
        candidates.insert(0, direct)
    seen = set()
    for candidate in candidates:
        low = candidate.lower()
        if low in seen:
            continue
        seen.add(low)
        if "/authors/copyright_transfer_agreement.php" in low:
            continue
        if any(token in low for token in ("#sec", "#b", "&view=citations")):
            continue
        return candidate
    return ""


def _extract_targeted_recovery_url(
    record: Dict[str, Any],
    final_url: str,
    title: str,
    html: str,
    snapshot: Dict[str, Any],
) -> str:
    doi = str(record.get("doi") or "")
    input_title = str(record.get("input_title") or "")
    for candidate in (
        _ieee_abstract_fallback_url(final_url=final_url, title=title, html=html),
        _extract_powdermat_article_url(final_url=final_url, doi=doi, input_title=input_title, title=title, html=html),
        _extract_site_search_article_url(final_url=final_url, doi=doi, input_title=input_title, title=title, html=html),
        _normalize_elsevier_article_url(final_url=final_url, snapshot=snapshot),
    ):
        if candidate and str(candidate).strip().lower() != str(final_url or "").strip().lower():
            return candidate
    return ""


def _start_attempt_listener(page: ChromiumPage) -> bool:
    if page is None:
        return False
    try:
        page.listen.stop()
    except Exception:
        pass
    try:
        page.listen.clear()
    except Exception:
        pass
    try:
        page.listen.start(targets=True, is_regex=False, method=("GET", "POST"), res_type=True)
        return True
    except Exception:
        return False


def _drain_listener_packets(page: ChromiumPage, max_count: int = 80, timeout: float = 0.6) -> List[Any]:
    packets: List[Any] = []
    if page is None:
        return packets
    try:
        for packet in page.listen.steps(count=max(1, int(max_count)), timeout=max(0.1, float(timeout)), gap=1):
            if packet is not None:
                packets.append(packet)
    except Exception:
        pass
    return packets


def _extract_direct_pdf_event(record: Dict[str, Any], packets: Sequence[Any]) -> Dict[str, Any]:
    doi_norm = _normalize_doi_text(record.get("doi") or "")
    safe_name = os.path.basename(_sanitize_doi_to_filename(doi_norm)).lower()
    for packet in packets or ():
        url = str(getattr(packet, "url", "") or "").strip()
        low_url = url.lower()
        resource_type = str(getattr(packet, "resourceType", "") or "").strip()
        response = getattr(packet, "response", None)
        headers = {}
        try:
            headers = dict(getattr(response, "headers", {}) or {})
        except Exception:
            headers = {}
        content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
        content_disposition = str(headers.get("content-disposition") or headers.get("Content-Disposition") or "").lower()
        if resource_type.lower() != "document":
            continue
        pdf_like = (
            any(marker in content_type for marker in PDF_MIME_MARKERS)
            or ".pdf" in content_disposition
            or any(marker in low_url for marker in PDF_URL_MARKERS)
        )
        if not pdf_like:
            continue
        if doi_norm and doi_norm in low_url:
            matched = True
        elif safe_name and safe_name in low_url:
            matched = True
        elif ".pdf" in content_disposition or any(marker in content_type for marker in PDF_MIME_MARKERS):
            matched = True
        else:
            matched = False
        if not matched:
            continue
        return {
            "url": url,
            "resource_type": resource_type,
            "content_type": content_type,
            "content_disposition": content_disposition,
            "is_failed": bool(getattr(packet, "is_failed", False)),
        }
    return {}


def _capture_direct_pdf_handoff(
    record: Dict[str, Any],
    probe_page_meta: Dict[str, Any],
    artifact_dir: str,
    initial_download_files: Sequence[str],
    timeout_s: int,
    page: ChromiumPage | None = None,
    listener_timeout_s: float = 0.4,
) -> Tuple[str, Dict[str, Any]]:
    worker_download_dir = os.path.abspath(str(probe_page_meta.get("worker_download_dir") or "")).strip()
    event_info = _extract_direct_pdf_event(record=record, packets=_drain_listener_packets(page, timeout=listener_timeout_s)) if page else {}
    if not worker_download_dir or not os.path.isdir(worker_download_dir):
        return "", event_info
    safe = _sanitize_doi_to_filename(str(record.get("doi") or "")).replace(".pdf", "")
    direct_dir = os.path.abspath(os.path.join(artifact_dir or ".", "direct_pdf_handoff"))
    os.makedirs(direct_dir, exist_ok=True)
    tmp_target = os.path.join(direct_dir, f"{safe}.tmp.pdf")
    final_target = os.path.join(direct_dir, f"{safe}.pdf")
    try:
        ok = _capture_direct_downloaded_pdf(
            download_dir=worker_download_dir,
            initial_files=set(initial_download_files or ()),
            tmp_target_path=tmp_target,
            final_target_path=final_target,
            logger=None,
            timeout_s=max(0, int(timeout_s)),
            context="landing-direct-pdf",
        )
    except Exception:
        ok = False
    if ok and os.path.exists(final_target):
        return final_target, event_info
    return "", event_info


def _probe_temp_tab_url(
    page: ChromiumPage,
    record: Dict[str, Any],
    expected_domains: Sequence[str],
    target_url: str,
    deadline_monotonic: float,
    step_label: str,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
    tab_transition_events: List[Dict[str, Any]] | None = None,
    prefer_earliest_success: bool = False,
    settle_wait_sec: float = 0.6,
    stabilize_polls: int = 4,
) -> Dict[str, Any]:
    if tab_transition_events is None:
        tab_transition_events = []
    temp_page = _open_temporary_tab(page)
    if temp_page is None or time.monotonic() >= deadline_monotonic:
        _close_temporary_tab(page, temp_page)
        return {}

    try:
        timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 10.0), floor_sec=4.0)
        started = time.perf_counter()
        temp_page.get(target_url, retry=0, interval=0.4, timeout=timeout)
        attempt_timing[f"{step_label}_temp_ms"] = int((time.perf_counter() - started) * 1000)
        _append_nav_step(navigation_chain, step_label, target_url, temp_page.url or target_url)
        temp_page = _adopt_latest_probe_tab(
            temp_page,
            navigation_chain=navigation_chain,
            attempt_timing=attempt_timing,
            tab_transition_events=tab_transition_events,
            step_label=f"{step_label}_tab_sync",
            force=True,
        )
        _dismiss_cookie_or_consent_banner(temp_page)

        immediate_url = temp_page.url or target_url
        immediate_title = temp_page.title or ""
        immediate_html = temp_page.html or ""
        immediate_snapshot = collect_page_snapshot(temp_page, title=immediate_title, html=immediate_html)
        immediate_eval = _evaluate_page_state(
            record=record,
            expected_domains=expected_domains,
            final_url=immediate_url,
            title=immediate_title,
            html=immediate_html,
            snapshot=immediate_snapshot,
        )

        final_title, final_html, final_snapshot = stabilize_page_state(
            temp_page,
            title=immediate_title,
            html=immediate_html,
            deadline_monotonic=deadline_monotonic,
            settle_wait_sec=settle_wait_sec,
            stabilize_polls=stabilize_polls,
        )
        final_url = temp_page.url or immediate_url
        final_eval = _evaluate_page_state(
            record=record,
            expected_domains=expected_domains,
            final_url=final_url,
            title=final_title,
            html=final_html,
            snapshot=final_snapshot,
        )

        selected = {
            "page": temp_page,
            "final_url": final_url,
            "title": final_title,
            "html": final_html,
            "snapshot": final_snapshot,
            **final_eval,
        }
        if prefer_earliest_success and immediate_eval.get("classifier_state") in SUCCESS_STATES:
            selected = {
                "page": temp_page,
                "final_url": immediate_url,
                "title": immediate_title,
                "html": immediate_html,
                "snapshot": immediate_snapshot,
                **immediate_eval,
            }
        return selected
    except Exception:
        _close_temporary_tab(page, temp_page)
        return {}


def _recover_powdermat_article_target(
    page: ChromiumPage,
    record: Dict[str, Any],
    expected_domains: Sequence[str],
    final_url: str,
    deadline_monotonic: float,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
) -> Dict[str, Any]:
    low_final = str(final_url or "").lower()
    if "powdermat.org" not in low_final:
        return {}
    if "/authors/copyright_transfer_agreement.php" not in low_final:
        return {}
    target_url = _powdermat_entry_url(str(record.get("doi") or ""))
    if not target_url:
        return {}
    return _probe_temp_tab_url(
        page=page,
        record=record,
        expected_domains=expected_domains,
        target_url=target_url,
        deadline_monotonic=deadline_monotonic,
        step_label="powdermat_article_reopen",
        navigation_chain=navigation_chain,
        attempt_timing=attempt_timing,
        prefer_earliest_success=True,
        settle_wait_sec=0.35,
        stabilize_polls=3,
    )


def _build_static_snapshot(title: str, html: str, final_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(str(html or ""), "html.parser")
    meta: Dict[str, str] = {}
    for tag in soup.select("meta[name][content], meta[property][content]"):
        key = str(tag.get("name") or tag.get("property") or "").strip().lower()
        value = " ".join(str(tag.get("content") or "").split()).strip()
        if key and value and key not in meta:
            meta[key] = value
    body = soup.body
    main = soup.find("main") or soup.find("article")
    visible_text = _strip_visible_text(html)
    main_text = _extract_main_like_text(html)
    abstract_text = ""
    for tag in soup.select("#Abs1-content, .Abstract, .abstract, section.abstract, [data-title='Abstract']"):
        abstract_text = " ".join(tag.get_text(" ", strip=True).split())
        if abstract_text:
            break
    return {
        "meta": meta,
        "title": str(title or "").strip(),
        "canonical_url": str(final_url or "").strip(),
        "body_text_excerpt": visible_text[:480],
        "parsed_text_excerpt": visible_text[:480],
        "body_text_len": len(visible_text),
        "parsed_text_len": len(visible_text),
        "main_text_len": len(main_text),
        "parsed_main_text_len": len(main_text),
        "abstract_text_len": len(abstract_text),
        "has_main": main is not None,
        "has_article_tag": soup.find("article") is not None,
        "has_abstract_node": bool(abstract_text),
        "body_child_count": len(list(body.children)) if body else 0,
        "spinner_count": 0,
        "iframe_count": len(soup.find_all("iframe")),
        "ready_state": "complete",
        "html_len": len(str(html or "")),
    }


def _recover_powdermat_static_entry(
    record: Dict[str, Any],
    expected_domains: Sequence[str],
) -> Dict[str, Any]:
    target_url = _powdermat_entry_url(str(record.get("doi") or ""))
    if not target_url:
        return {}
    try:
        resp = requests.get(
            target_url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except Exception:
        return {}
    html = str(resp.text or "")
    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    if soup.title:
        title = " ".join(soup.title.get_text(" ", strip=True).split()).strip()
    final_url = str(resp.url or target_url).strip()
    snapshot = _build_static_snapshot(title=title, html=html, final_url=final_url)
    evaluated = _evaluate_page_state(
        record=record,
        expected_domains=expected_domains,
        final_url=final_url,
        title=title,
        html=html,
        snapshot=snapshot,
    )
    if evaluated.get("classifier_state") not in SUCCESS_STATES:
        return {}
    return {
        "page": None,
        "final_url": final_url,
        "title": title,
        "html": html,
        "snapshot": snapshot,
        **evaluated,
    }


def _recover_powdermat_via_back(
    page: ChromiumPage,
    record: Dict[str, Any],
    expected_domains: Sequence[str],
    final_url: str,
    deadline_monotonic: float,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
) -> Dict[str, Any]:
    low_final = str(final_url or "").lower()
    if "powdermat.org" not in low_final or "/authors/copyright_transfer_agreement.php" not in low_final:
        return {}
    if page is None or time.monotonic() >= deadline_monotonic:
        return {}
    try:
        timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 6.0), floor_sec=2.5)
        started = time.perf_counter()
        page.back()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current = str(page.url or "").strip()
            if current and current.lower() != low_final:
                break
            time.sleep(0.15)
        attempt_timing["powdermat_back_ms"] = int((time.perf_counter() - started) * 1000)
        _append_nav_step(navigation_chain, "powdermat_back", final_url, page.url or final_url)
        _dismiss_cookie_or_consent_banner(page)
        back_url = page.url or final_url
        back_title = page.title or ""
        back_html = page.html or ""
        back_title, back_html, back_snapshot = stabilize_page_state(
            page,
            title=back_title,
            html=back_html,
            deadline_monotonic=deadline_monotonic,
            settle_wait_sec=0.3,
            stabilize_polls=3,
        )
        back_url = page.url or back_url
        if "/authors/copyright_transfer_agreement.php" in str(back_url or "").lower():
            return {}
        evaluated = _evaluate_page_state(
            record=record,
            expected_domains=expected_domains,
            final_url=back_url,
            title=back_title,
            html=back_html,
            snapshot=back_snapshot,
        )
        if evaluated.get("classifier_state") not in SUCCESS_STATES:
            return {}
        return {
            "page": None,
            "final_url": back_url,
            "title": back_title,
            "html": back_html,
            "snapshot": back_snapshot,
            **evaluated,
        }
    except Exception:
        return {}


def _normalize_title_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _find_ieee_search_result_url(page: ChromiumPage, input_title: str, deadline_monotonic: float) -> str:
    if page is None:
        return ""
    title_key = _normalize_title_key(input_title)
    if len(title_key) < 12:
        return ""
    while time.monotonic() < deadline_monotonic:
        try:
            candidates = page.run_js(
                """return Array.from(document.querySelectorAll("a[href*='/document/']")).map(a => ({
                    href: a.href || '',
                    text: (a.innerText || a.textContent || '').trim()
                }));"""
            ) or []
        except Exception:
            candidates = []
        seen = set()
        exact = ""
        partial = ""
        for item in candidates:
            href = str((item or {}).get("href") or "").strip()
            text = str((item or {}).get("text") or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            low_href = href.lower()
            if "/document/" not in low_href or "citations" in low_href:
                continue
            text_key = _normalize_title_key(text)
            if not text_key:
                continue
            if text_key == title_key:
                exact = href
                break
            if title_key in text_key or text_key in title_key:
                partial = partial or href
        if exact or partial:
            return exact or partial
        time.sleep(0.5)
    return ""


def _recover_ieee_via_title_search(
    page: ChromiumPage,
    record: Dict[str, Any],
    expected_domains: Sequence[str],
    input_title: str,
    deadline_monotonic: float,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
) -> Dict[str, Any]:
    title_query = " ".join(str(input_title or "").split()).strip()
    if not page or len(title_query) < 12 or time.monotonic() >= deadline_monotonic:
        return {}
    search_url = f"https://ieeexplore.ieee.org/search/searchresult.jsp?queryText={quote(title_query, safe='')}"
    search_page = page
    temp_page = None
    if _is_headless_browser():
        temp_page = _open_temporary_tab(page)
        if temp_page is not None:
            search_page = temp_page
    timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 12.0), floor_sec=4.0)
    started = time.perf_counter()
    search_page.get(search_url, retry=0, interval=0.4, timeout=timeout)
    attempt_timing["ieee_title_search_ms"] = int((time.perf_counter() - started) * 1000)
    _append_nav_step(navigation_chain, "ieee_title_search", search_url, search_page.url or search_url)
    _dismiss_cookie_or_consent_banner(search_page)
    settle_until = min(deadline_monotonic, time.monotonic() + 8.0)
    while time.monotonic() < settle_until:
        try:
            has_result_cards = bool(
                search_page.run_js(
                    """return Boolean(
                        document.querySelector("a[href*='/document/']")
                        || document.body.innerText.includes('Showing')
                        || document.body.innerText.includes('No results found')
                    );"""
                )
            )
        except Exception:
            has_result_cards = False
        if has_result_cards:
            break
        time.sleep(0.5)
    result_url = _find_ieee_search_result_url(search_page, input_title=title_query, deadline_monotonic=deadline_monotonic)
    if not result_url or time.monotonic() >= deadline_monotonic:
        _close_temporary_tab(page, temp_page)
        return {}
    recovered = _probe_temp_tab_url(
        page=page,
        record=record,
        expected_domains=expected_domains,
        target_url=result_url,
        deadline_monotonic=deadline_monotonic,
        step_label="ieee_title_result",
        navigation_chain=navigation_chain,
        attempt_timing=attempt_timing,
        prefer_earliest_success=False,
        settle_wait_sec=0.7,
        stabilize_polls=4,
    )
    attempt_timing["ieee_title_result_url"] = result_url
    _close_temporary_tab(page, temp_page)
    return recovered


def _recover_elsevier_headless_temp_tab(
    page: ChromiumPage,
    record: Dict[str, Any],
    expected_domains: Sequence[str],
    doi_url: str,
    final_url: str,
    snapshot: Dict[str, Any],
    entry_plan: Dict[str, Any],
    deadline_monotonic: float,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
    tab_transition_events: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    if not _is_headless_browser():
        return {}
    if str(record.get("scheduler_publisher") or "") != "elsevier":
        return {}
    if time.monotonic() >= deadline_monotonic:
        return {}

    candidates: List[str] = []
    for raw in (
        _extract_preferred_article_url(final_url=final_url, snapshot=snapshot),
        _normalize_elsevier_article_url(final_url=final_url, snapshot=snapshot),
        str(entry_plan.get("entry_handoff_url") or ""),
        str(entry_plan.get("entry_url") or ""),
        str(entry_plan.get("entry_browser_url") or ""),
        str(entry_plan.get("entry_resolved_url") or ""),
        _resolve_elsevier_structural_entry_url(doi_url),
        final_url,
    ):
        for candidate in _headless_preferred_elsevier_urls(raw):
            if candidate and candidate not in candidates:
                candidates.append(candidate)

    for idx, candidate in enumerate(candidates):
        recovered = _probe_temp_tab_url(
            page=page,
            record=record,
            expected_domains=expected_domains,
            target_url=candidate,
            deadline_monotonic=deadline_monotonic,
            step_label=f"elsevier_headless_temp_{idx + 1}",
            navigation_chain=navigation_chain,
            attempt_timing=attempt_timing,
            tab_transition_events=tab_transition_events,
            prefer_earliest_success=False,
            settle_wait_sec=0.8,
            stabilize_polls=5,
        )
        if not recovered:
            continue
        if recovered.get("classifier_state") in SUCCESS_STATES:
            return recovered
        _close_temporary_tab(page, recovered.get("page"))
    return {}



def _extract_preferred_article_url(final_url: str, snapshot: Dict[str, Any]) -> str:
    current = str(final_url or "").strip()
    meta = dict(snapshot.get("meta") or {})
    candidates = [
        str(meta.get("citation_fulltext_html_url") or "").strip(),
        str(meta.get("citation_abstract_html_url") or "").strip(),
        str(meta.get("og:url") or "").strip(),
        str(snapshot.get("canonical_url") or "").strip(),
    ]
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        low = candidate.lower()
        if not low.startswith(("http://", "https://")):
            continue
        if low == current.lower():
            continue
        if "doi.org/" in low or low.endswith("doi.org"):
            continue
        return candidate
    return ""


def _normalize_doi_text(value: str) -> str:
    return str(value or "").strip().lower().replace("https://doi.org/", "").replace("http://doi.org/", "")


def _elsevier_article_markers_in_snapshot(snapshot: Dict[str, Any]) -> bool:
    meta = dict(snapshot.get("meta") or {})
    h1_text = str(snapshot.get("h1_text") or "").strip()
    if h1_text:
        return True
    for key in ("citation_doi", "citation_title", "citation_fulltext_html_url", "citation_journal_title", "og:url"):
        if str(meta.get(key) or "").strip():
            return True
    return False


def _looks_like_elsevier_article_shell(final_url: str, title: str, snapshot: Dict[str, Any], doi: str) -> bool:
    low_url = str(final_url or "").strip().lower()
    if not any(host in low_url for host in ELSEVIER_ARTICLE_HOST_MARKERS):
        return False
    if not any(token in low_url for token in ("/science/article/pii/", "/fulltext/")):
        return False
    meta = dict(snapshot.get("meta") or {})
    citation_doi = _normalize_doi_text(meta.get("citation_doi") or "")
    doi_norm = _normalize_doi_text(doi)
    if doi_norm and citation_doi and citation_doi != doi_norm:
        return False
    if not _elsevier_article_markers_in_snapshot(snapshot):
        return False
    title_clean = str(title or "").strip()
    if len(title_clean) < 18:
        return False
    body_text_len = int(snapshot.get("body_text_len", 0) or 0)
    main_text_len = int(snapshot.get("main_text_len", 0) or 0)
    parsed_main_text_len = int(snapshot.get("parsed_main_text_len", 0) or 0)
    if body_text_len >= 450 or main_text_len >= 120 or parsed_main_text_len >= 120:
        return False
    return True


def _resolve_elsevier_retrieve_entry_url(entry_plan: Dict[str, Any], current_url: str = "") -> str:
    for candidate in (
        str(current_url or "").strip(),
        str(entry_plan.get("entry_browser_url") or "").strip(),
        str(entry_plan.get("entry_resolved_url") or "").strip(),
        str(entry_plan.get("entry_url") or "").strip(),
    ):
        if candidate and _is_elsevier_retrieve_url(candidate):
            return candidate
    return ""


def _recover_elsevier_via_retrieve_link(
    page: ChromiumPage,
    *,
    doi: str,
    entry_plan: Dict[str, Any],
    deadline_monotonic: float,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
    tab_transition_events: List[Dict[str, Any]],
    step_prefix: str,
    settle_wait_sec: float = 0.8,
    stabilize_polls: int = 5,
) -> Dict[str, Any]:
    result = {
        "page": page,
        "final_url": str(getattr(page, "url", "") or ""),
        "title": str(getattr(page, "title", "") or ""),
        "html": "",
        "snapshot": {},
        "attempted": False,
        "retrieve_opened": False,
        "doi_click_used": False,
        "handoff_used": False,
        "handoff_url": "",
        "canonical_used": False,
        "recovery_steps": [],
    }
    if page is None or time.monotonic() >= deadline_monotonic:
        return result

    retrieve_url = _resolve_elsevier_retrieve_entry_url(entry_plan=entry_plan, current_url=result["final_url"])
    if not retrieve_url and not _is_elsevier_retrieve_url(result["final_url"]):
        return result

    result["attempted"] = True
    doi_norm = _normalize_doi_text(doi)

    if retrieve_url and not _is_elsevier_retrieve_url(result["final_url"]) and time.monotonic() < deadline_monotonic:
        timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 10.0), floor_sec=4.0)
        started = time.perf_counter()
        page.get(retrieve_url, retry=0, interval=0.4, timeout=timeout)
        attempt_timing[f"{step_prefix}_retrieve_ms"] = int((time.perf_counter() - started) * 1000)
        _append_nav_step(navigation_chain, f"{step_prefix}_retrieve", retrieve_url, page.url or retrieve_url)
        page = _adopt_latest_probe_tab(
            page,
            navigation_chain=navigation_chain,
            attempt_timing=attempt_timing,
            tab_transition_events=tab_transition_events,
            step_label=f"{step_prefix}_retrieve_tab_sync",
            force=True,
        )
        _dismiss_cookie_or_consent_banner(page)
        result["retrieve_opened"] = True
        result["recovery_steps"].append("retrieve_open")
        result["final_url"] = str(page.url or retrieve_url)
        result["title"] = str(page.title or "")

    current_url = str(page.url or result["final_url"] or "")
    current_html = str(page.html or "")
    if _is_elsevier_retrieve_url(current_url) and time.monotonic() < deadline_monotonic:
        clicked = _click_elsevier_doi_link_in_retrieve(page, doi_norm)
        if clicked:
            result["doi_click_used"] = True
            result["recovery_steps"].append("retrieve_doi_click")
            try:
                wait_timeout = _remaining_budget(deadline_monotonic, 6.0, floor_sec=2.0)
                _wait_for_elsevier_article_ready(page, doi_norm, timeout_s=min(6, int(max(2.0, wait_timeout))))
            except Exception:
                pass
            page = _adopt_latest_probe_tab(
                page,
                navigation_chain=navigation_chain,
                attempt_timing=attempt_timing,
                tab_transition_events=tab_transition_events,
                step_label=f"{step_prefix}_retrieve_click_tab_sync",
                force=True,
            )
            _append_nav_step(navigation_chain, f"{step_prefix}_retrieve_click", current_url, page.url or current_url)
            _dismiss_cookie_or_consent_banner(page)
            current_url = str(page.url or current_url)
            current_html = str(page.html or current_html)
        if _is_elsevier_retrieve_url(current_url) and time.monotonic() < deadline_monotonic:
            handoff = (
                _extract_elsevier_retrieve_handoff_url(current_url, current_html)
                or str(entry_plan.get("entry_handoff_url") or "").strip()
            )
            if handoff:
                timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 10.0), floor_sec=4.0)
                started = time.perf_counter()
                page.get(handoff, retry=0, interval=0.4, timeout=timeout)
                attempt_timing[f"{step_prefix}_handoff_ms"] = int((time.perf_counter() - started) * 1000)
                _append_nav_step(navigation_chain, f"{step_prefix}_handoff", handoff, page.url or handoff)
                page = _adopt_latest_probe_tab(
                    page,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                    tab_transition_events=tab_transition_events,
                    step_label=f"{step_prefix}_handoff_tab_sync",
                    force=True,
                )
                _dismiss_cookie_or_consent_banner(page)
                result["handoff_used"] = True
                result["handoff_url"] = handoff
                result["recovery_steps"].append("retrieve_handoff")
                current_url = str(page.url or handoff)

    canonical_url = _normalize_elsevier_article_url(final_url=current_url, snapshot={})
    if canonical_url and canonical_url.lower() != current_url.lower() and time.monotonic() < deadline_monotonic:
        timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 8.0), floor_sec=3.0)
        started = time.perf_counter()
        page.get(canonical_url, retry=0, interval=0.4, timeout=timeout)
        attempt_timing[f"{step_prefix}_canonical_ms"] = int((time.perf_counter() - started) * 1000)
        _append_nav_step(navigation_chain, f"{step_prefix}_canonical", canonical_url, page.url or canonical_url)
        page = _adopt_latest_probe_tab(
            page,
            navigation_chain=navigation_chain,
            attempt_timing=attempt_timing,
            tab_transition_events=tab_transition_events,
            step_label=f"{step_prefix}_canonical_tab_sync",
            force=True,
        )
        _dismiss_cookie_or_consent_banner(page)
        result["canonical_used"] = True
        result["recovery_steps"].append("canonical_normalize")
        current_url = str(page.url or canonical_url)

    if time.monotonic() < deadline_monotonic:
        try:
            wait_timeout = _remaining_budget(deadline_monotonic, 6.0, floor_sec=2.0)
            _wait_for_elsevier_article_ready(page, doi_norm, timeout_s=min(6, int(max(2.0, wait_timeout))))
        except Exception:
            pass
        page = _adopt_latest_probe_tab(
            page,
            navigation_chain=navigation_chain,
            attempt_timing=attempt_timing,
            tab_transition_events=tab_transition_events,
            step_label=f"{step_prefix}_final_tab_sync",
            force=True,
        )
        _dismiss_cookie_or_consent_banner(page)

    final_title = str(page.title or result["title"] or "")
    final_html = str(page.html or current_html or "")
    final_title, final_html, final_snapshot = stabilize_page_state(
        page,
        title=final_title,
        html=final_html,
        deadline_monotonic=deadline_monotonic,
        settle_wait_sec=settle_wait_sec,
        stabilize_polls=stabilize_polls,
    )
    result.update(
        {
            "page": page,
            "final_url": str(page.url or current_url or result["final_url"] or retrieve_url),
            "title": final_title,
            "html": final_html,
            "snapshot": final_snapshot,
        }
    )
    return result


def _recover_elsevier_article_shell(
    page: ChromiumPage,
    doi: str,
    final_url: str,
    title: str,
    html: str,
    snapshot: Dict[str, Any],
    entry_plan: Dict[str, Any],
    deadline_monotonic: float,
    navigation_chain: List[Dict[str, str]],
    attempt_timing: Dict[str, Any],
    tab_transition_events: List[Dict[str, Any]],
) -> Tuple[str, str, str, Dict[str, Any], str, Dict[str, Any]]:
    recovery_meta = {
        "initial_landing_type": "",
        "shell_recovery_attempted": False,
        "shell_recovery_strategy": "",
        "shell_recovery_outcome": "",
    }
    if page is None or time.monotonic() >= deadline_monotonic:
        return final_url, title, html, snapshot, "", recovery_meta
    if not _looks_like_elsevier_article_shell(final_url=final_url, title=title, snapshot=snapshot, doi=doi):
        return final_url, title, html, snapshot, "", recovery_meta

    recovery_meta["initial_landing_type"] = "elsevier_article_shell"
    recovery_meta["shell_recovery_attempted"] = True

    target_url = ""
    for candidate in (
        _extract_preferred_article_url(final_url=final_url, snapshot=snapshot),
        _normalize_elsevier_article_url(final_url=final_url, snapshot=snapshot),
        _normalize_elsevier_article_url(final_url=final_url, snapshot={}),
    ):
        cand = str(candidate or "").strip()
        if not cand:
            continue
        if _normalize_doi_text(cand) == _normalize_doi_text(final_url):
            continue
        target_url = cand
        break

    action = ""
    retrieve_recovery = _recover_elsevier_via_retrieve_link(
        page=page,
        doi=doi,
        entry_plan=entry_plan,
        deadline_monotonic=deadline_monotonic,
        navigation_chain=navigation_chain,
        attempt_timing=attempt_timing,
        tab_transition_events=tab_transition_events,
        step_prefix="elsevier_shell",
        settle_wait_sec=0.7,
        stabilize_polls=4,
    )
    if retrieve_recovery.get("attempted"):
        recovery_meta["shell_recovery_strategy"] = "retrieve_link_recovery"
        final_url = str(retrieve_recovery.get("final_url") or final_url)
        title = str(retrieve_recovery.get("title") or title)
        html = str(retrieve_recovery.get("html") or html)
        snapshot = dict(retrieve_recovery.get("snapshot") or snapshot)
        page = retrieve_recovery.get("page") or page
        used_steps = list(retrieve_recovery.get("recovery_steps") or [])
        action = "+".join(used_steps) if used_steps else "retrieve_link_recovery"
        if not _looks_like_elsevier_article_shell(final_url=final_url, title=title, snapshot=snapshot, doi=doi):
            recovery_meta["shell_recovery_outcome"] = "recovered_to_article_page"
            return final_url, title, html, snapshot, action, recovery_meta
        recovery_meta["shell_recovery_outcome"] = "still_shell_after_retrieve_recovery"

    if target_url and time.monotonic() < deadline_monotonic:
        timeout = _remaining_budget(deadline_monotonic, min(DEFAULT_LOCAL_TIMEOUT_SEC, 8.0), floor_sec=3.0)
        started = time.perf_counter()
        page.get(target_url, retry=0, interval=0.4, timeout=timeout)
        attempt_timing["elsevier_shell_reopen_ms"] = int((time.perf_counter() - started) * 1000)
        attempt_timing["elsevier_shell_reopen_url"] = target_url
        _append_nav_step(navigation_chain, "elsevier_shell_reopen", target_url, page.url or target_url)
        page = _adopt_latest_probe_tab(
            page,
            navigation_chain=navigation_chain,
            attempt_timing=attempt_timing,
            tab_transition_events=tab_transition_events,
            step_label="elsevier_shell_reopen_tab_sync",
            force=True,
        )
        _dismiss_cookie_or_consent_banner(page)
        final_url = page.url or target_url
        title = page.title or title
        html = page.html or html
        action = "reopen_exact_article"

    if time.monotonic() < deadline_monotonic:
        started = time.perf_counter()
        title, html, snapshot = stabilize_page_state(
            page,
            title=title,
            html=html,
            deadline_monotonic=deadline_monotonic,
            settle_wait_sec=0.8,
            stabilize_polls=8,
        )
        attempt_timing["elsevier_shell_stabilize_ms"] = int((time.perf_counter() - started) * 1000)
        final_url = page.url or final_url
        if action:
            action = f"{action}+hydrate_wait"
        else:
            action = "hydrate_wait"

    if not recovery_meta["shell_recovery_strategy"]:
        recovery_meta["shell_recovery_strategy"] = "reopen_exact_article"
    if not recovery_meta["shell_recovery_outcome"]:
        recovery_meta["shell_recovery_outcome"] = (
            "recovered_to_article_page"
            if not _looks_like_elsevier_article_shell(final_url=final_url, title=title, snapshot=snapshot, doi=doi)
            else "still_shell_after_reopen"
        )

    return final_url, title, html, snapshot, action, recovery_meta


def _should_try_preferred_article_handoff(final_url: str, title: str, snapshot: Dict[str, Any]) -> bool:
    low_url = str(final_url or "").strip().lower()
    low_title = str(title or "").strip().lower()
    main_text_len = int(snapshot.get("main_text_len", 0) or 0)
    body_text_len = int(snapshot.get("body_text_len", 0) or 0)
    if _is_elsevier_retrieve_url(low_url):
        return True
    if low_url.endswith("doi.org") or "/doi.org/" in low_url:
        return True
    if low_title in INTERSTITIAL_TITLES:
        return True
    if main_text_len < 80 and body_text_len < 220:
        return True
    return False


def _looks_like_timeout_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return "timed out" in message or "timeout" in message


def _try_broken_shell_fallback(page: ChromiumPage, deadline_monotonic: float) -> str:
    if page is None or time.monotonic() >= deadline_monotonic:
        return ""

    locators = (
        'css:button[data-track-id="return-old"]',
        'xpath://button[contains(translate(normalize-space(string(.)),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"old version")]',
        'xpath://a[contains(translate(normalize-space(string(.)),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"old version")]',
        'xpath://button[contains(translate(normalize-space(string(.)),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"legacy")]',
        'xpath://a[contains(translate(normalize-space(string(.)),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"legacy")]',
        'xpath://button[contains(translate(normalize-space(string(.)),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"classic version")]',
        'xpath://a[contains(translate(normalize-space(string(.)),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"classic version")]',
    )
    for locator in locators:
        try:
            el = page.ele(locator, timeout=0.8)
        except Exception:
            el = None
        if not el:
            continue
        try:
            el.click(by_js=True)
        except Exception:
            try:
                el.click()
            except Exception:
                continue
        time.sleep(min(1.2, max(0.2, deadline_monotonic - time.monotonic())))
        return "fallback_link_click"

    try:
        page.refresh(ignore_cache=True)
        time.sleep(min(1.0, max(0.2, deadline_monotonic - time.monotonic())))
        return "hard_refresh"
    except Exception:
        return ""


def _compat_outcome_from_state(classifier_state: str, reason_codes: Sequence[str]) -> str:
    reason_blob = " ".join(str(code or "").lower() for code in (reason_codes or []))
    if classifier_state in SUCCESS_STATES:
        return OUT_SUCCESS_ACCESS
    if classifier_state == STATE_CHALLENGE_DETECTED:
        return OUT_FAIL_CAPTCHA
    if classifier_state == STATE_DOI_NOT_FOUND:
        return OUT_FAIL_DOI_NOT_FOUND
    if classifier_state in (STATE_TIMEOUT, STATE_NETWORK_ERROR):
        return OUT_FAIL_NETWORK
    if "access_rights_gate" in reason_blob:
        return OUT_FAIL_ACCESS_RIGHTS
    return OUT_FAIL_BLOCK


def _evaluate_page_state(
    record: Dict[str, Any],
    expected_domains: Sequence[str],
    final_url: str,
    title: str,
    html: str,
    snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    issue, issue_evidence = detect_access_issue(title=title, html=html, url=final_url, domain="")
    article_signal = bool(_has_article_signal(title=title, html=html))
    pdf_action_signal = bool(_has_pdf_action_signal(title=title, html=html))
    consent_signal = bool(_has_cookie_or_consent_signal(title=title, html=html))
    legacy_success_like = _legacy_verify_landing_success(
        doi=str(record.get("doi") or ""),
        url=final_url,
        domain=_extract_domain(str(snapshot.get("canonical_url") or "") or final_url),
        title=title,
        html=html,
        article_signal=article_signal,
        pdf_action_signal=pdf_action_signal,
    )
    classified = classify_landing(
        doi=str(record.get("doi") or ""),
        input_publisher=str(record.get("input_publisher") or ""),
        scheduler_publisher=str(record.get("scheduler_publisher") or ""),
        final_url=final_url,
        title=title,
        html=html,
        snapshot=snapshot,
        issue=issue or "",
        issue_evidence=issue_evidence or [],
        exception_kind="",
        expected_domains=expected_domains,
    )
    return {
        "issue": issue or "",
        "issue_evidence": list(issue_evidence or []),
        "article_signal": article_signal,
        "pdf_action_signal": pdf_action_signal,
        "consent_signal": consent_signal,
        "legacy_success_like": bool(legacy_success_like),
        "classifier_state": classified.get("classifier_state", STATE_UNKNOWN_NON_SUCCESS),
        "reason_codes": list(classified.get("reason_codes", []) or []),
        "signal_summary": dict(classified.get("signal_summary") or {}),
        "reclassified_after_detector_fix": bool(classified.get("reclassified_after_detector_fix")),
        "reclassification_reason": str(classified.get("reclassification_reason") or ""),
    }


def _legacy_verify_landing_success(
    doi: str,
    url: str,
    domain: str,
    title: str,
    html: str,
    article_signal: bool,
    pdf_action_signal: bool,
) -> bool:
    low_url = str(url or "").lower()
    low_domain = str(domain or "").lower()
    low_title = str(title or "").strip().lower()

    if (not low_domain) or low_domain.endswith("doi.org"):
        return False
    if _is_elsevier_retrieve_url(low_url):
        return False
    for marker in ("__cf_chl_rt_tk=", "/cdn-cgi/challenge", "/cdn-cgi/l/chk_captcha", "challenges.cloudflare.com"):
        if marker in low_url:
            return False
    if str(doi or "").strip().lower().startswith("10.1016") and ("sciencedirect.com" not in low_domain):
        return False
    if article_signal or pdf_action_signal:
        return True
    if low_title in ("redirecting", "redirecting...", "redirect", "sciencedirect", "just a moment...", "please wait"):
        return False
    if any(marker in low_title for marker in ("just a moment", "security check", "access denied", "verify you are human")):
        return False
    if len(low_title) < 12:
        return False
    return True


def _should_retry_landing(classifier_state: str, reason_codes: Sequence[str], attempt_idx: int, max_attempts: int) -> bool:
    if attempt_idx + 1 >= max_attempts:
        return False
    codes = {str(code or "") for code in (reason_codes or [])}
    if classifier_state in (STATE_TIMEOUT, STATE_NETWORK_ERROR, STATE_BLANK_OR_INCOMPLETE):
        return True
    if classifier_state == STATE_DOI_NOT_FOUND:
        return False
    if classifier_state == STATE_BROKEN_JS_SHELL:
        return attempt_idx == 0
    if classifier_state == STATE_DOMAIN_MISMATCH:
        return False
    if classifier_state == STATE_UNKNOWN_NON_SUCCESS and ("insufficient_article_signals" in codes):
        if "content_populated" in codes:
            return False
        return True
    if classifier_state == STATE_CONSENT_OR_INTERSTITIAL_BLOCK and (
        "redirect_or_doi_domain" in codes or "interstitial_marker" in codes or "elsevier_retrieve_interstitial" in codes
    ):
        return True
    return False


def _build_artifact_zip(records: List[Dict[str, Any]], artifact_dir: str, zip_path: str, target: str) -> str:
    target = str(target or "").strip().lower()
    if target == "success":
        target_records = [r for r in records if r.get("outcome") == OUT_SUCCESS_ACCESS]
    else:
        target = "fail"
        target_records = [r for r in records if r.get("outcome") != OUT_SUCCESS_ACCESS]

    if not target_records:
        return ""

    abs_artifact_dir = os.path.abspath(artifact_dir)
    zip_path = os.path.abspath(zip_path)
    os.makedirs(os.path.dirname(zip_path) or ".", exist_ok=True)

    manifest = []
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rec in target_records:
            manifest.append(
                {
                    "doi": rec.get("doi", ""),
                    "input_publisher": rec.get("input_publisher", ""),
                    "classifier_state": rec.get("classifier_state", ""),
                    "outcome": rec.get("outcome", ""),
                    "resolved_url": rec.get("resolved_url", ""),
                    "reason_codes": rec.get("reason_codes", []),
                    "navigation_chain": rec.get("navigation_chain", []),
                    "entry_strategy": rec.get("entry_strategy", ""),
                    "entry_url": rec.get("entry_url", ""),
                    "entry_resolved_url": rec.get("entry_resolved_url", ""),
                    "entry_browser_url": rec.get("entry_browser_url", ""),
                    "entry_browser_kind": rec.get("entry_browser_kind", ""),
                    "entry_handoff_url": rec.get("entry_handoff_url", ""),
                    "entry_handoff_used": rec.get("entry_handoff_used", False),
                    "entry_redirect_chain_summary": rec.get("entry_redirect_chain_summary", []),
                    "entry_fallback_used": rec.get("entry_fallback_used", False),
                    "entry_fallback_reason": rec.get("entry_fallback_reason", ""),
                    "entry_preflight_issue": rec.get("entry_preflight_issue", ""),
                    "entry_browser_open_skipped": rec.get("entry_browser_open_skipped", False),
                    "initial_landing_type": rec.get("initial_landing_type", ""),
                    "shell_recovery_attempted": rec.get("shell_recovery_attempted", False),
                    "shell_recovery_strategy": rec.get("shell_recovery_strategy", ""),
                    "shell_recovery_outcome": rec.get("shell_recovery_outcome", ""),
                    "tab_transition_count": rec.get("tab_transition_count", 0),
                    "tab_transition_events": rec.get("tab_transition_events", []),
                    "reclassified_after_detector_fix": rec.get("reclassified_after_detector_fix", False),
                    "reclassification_reason": rec.get("reclassification_reason", ""),
                    "direct_pdf_path": rec.get("direct_pdf_path", ""),
                    "direct_pdf_event": rec.get("direct_pdf_event", {}),
                    "screenshot_path": rec.get("screenshot_path", ""),
                    "html_path": rec.get("html_path", ""),
                    "meta_path": rec.get("meta_path", ""),
                }
            )
            for key in ("screenshot_path", "html_path", "meta_path"):
                path = str(rec.get(key, "") or "").strip()
                if not path or not os.path.isfile(path):
                    continue
                if path.startswith(abs_artifact_dir):
                    rel = os.path.relpath(path, start=abs_artifact_dir)
                    arc = os.path.join("artifacts", rel)
                else:
                    arc = os.path.join("artifacts", os.path.basename(path))
                zf.write(path, arcname=arc)
        zf.writestr(f"manifest_{target}.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    return zip_path


def _save_probe_artifacts(
    page: ChromiumPage,
    record: Dict[str, Any],
    artifact_dir: str,
    include_html: bool,
    include_screenshot: bool = True,
) -> Dict[str, str]:
    out = {"screenshot": "", "html": "", "meta": ""}
    if page is None or not artifact_dir:
        return out

    success = record.get("outcome") == OUT_SUCCESS_ACCESS
    bucket = "success" if success else "fail"
    artifact_bucket_dir = os.path.abspath(os.path.join(artifact_dir, bucket))
    os.makedirs(artifact_bucket_dir, exist_ok=True)
    safe = _sanitize_doi_to_filename(str(record.get("doi") or "")).replace(".pdf", "")
    ts = int(time.time() * 1000)
    prefix = "landing_success" if success else "landing_fail"
    screenshot_name = f"{prefix}_{safe}_{ts}.png"
    html_name = f"{prefix}_{safe}_{ts}.html"
    meta_name = f"{prefix}_{safe}_{ts}.json"

    if include_screenshot and not bool(record.get("entry_browser_open_skipped")):
        try:
            page.get_screenshot(path=artifact_bucket_dir, name=screenshot_name, full_page=False)
            out["screenshot"] = os.path.abspath(os.path.join(artifact_bucket_dir, screenshot_name))
        except Exception:
            pass

    if include_html:
        try:
            html_path = os.path.abspath(os.path.join(artifact_bucket_dir, html_name))
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(str(record.get("html") or ""))
            out["html"] = html_path
        except Exception:
            pass

    try:
        meta_path = os.path.abspath(os.path.join(artifact_bucket_dir, meta_name))
        meta_payload = {
            "doi": record.get("doi", ""),
            "input_publisher": record.get("input_publisher", ""),
            "scheduler_publisher": record.get("scheduler_publisher", ""),
            "captured_at_ms": ts,
            "worker_idx": record.get("worker_idx", 0),
            "browser_identity": record.get("browser_identity", ""),
            "probe_page_mode": record.get("probe_page_mode", ""),
            "probe_tab_id": record.get("probe_tab_id", ""),
            "scheduled_start_ms": record.get("scheduled_start_ms", 0),
            "actual_start_ms": record.get("actual_start_ms", 0),
            "pacing_wait_ms": record.get("pacing_wait_ms", 0),
            "pacing_penalty_wait_ms": record.get("pacing_penalty_wait_ms", 0),
            "classifier_state": record.get("classifier_state", ""),
            "outcome": record.get("outcome", ""),
            "final_url": record.get("resolved_url", ""),
            "navigation_chain": record.get("navigation_chain", []),
            "title": str(record.get("title") or "")[:400],
            "reason_codes": record.get("reason_codes", []),
            "signal_summary": record.get("signal_summary", {}),
            "timing_breakdown": record.get("timing_breakdown", {}),
            "attempt_history": record.get("attempt_history", []),
            "direct_pdf_path": record.get("direct_pdf_path", ""),
            "direct_pdf_event": record.get("direct_pdf_event", {}),
            "legacy_success_like": bool(record.get("legacy_success_like")),
            "dom_signature": record.get("dom_signature", ""),
            "html_len": int(record.get("html_len", 0) or 0),
            "entry_strategy": record.get("entry_strategy", ""),
            "entry_url": record.get("entry_url", ""),
            "entry_resolved_url": record.get("entry_resolved_url", ""),
            "entry_browser_url": record.get("entry_browser_url", ""),
            "entry_browser_kind": record.get("entry_browser_kind", ""),
            "entry_handoff_url": record.get("entry_handoff_url", ""),
            "entry_handoff_used": bool(record.get("entry_handoff_used")),
            "entry_redirect_chain_summary": record.get("entry_redirect_chain_summary", []),
            "entry_fallback_used": bool(record.get("entry_fallback_used")),
            "entry_fallback_reason": record.get("entry_fallback_reason", ""),
            "entry_preflight_issue": record.get("entry_preflight_issue", ""),
            "entry_preflight_evidence": record.get("entry_preflight_evidence", []),
            "entry_preflight_http_status": record.get("entry_preflight_http_status", ""),
            "entry_browser_open_skipped": bool(record.get("entry_browser_open_skipped")),
            "initial_landing_type": record.get("initial_landing_type", ""),
            "shell_recovery_attempted": bool(record.get("shell_recovery_attempted")),
            "shell_recovery_strategy": record.get("shell_recovery_strategy", ""),
            "shell_recovery_outcome": record.get("shell_recovery_outcome", ""),
            "tab_transition_count": int(record.get("tab_transition_count", 0) or 0),
            "tab_transition_events": record.get("tab_transition_events", []),
            "reclassified_after_detector_fix": bool(record.get("reclassified_after_detector_fix")),
            "reclassification_reason": record.get("reclassification_reason", ""),
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_payload, f, ensure_ascii=False, indent=2)
        out["meta"] = meta_path
    except Exception:
        pass

    return out


def _probe_one(
    page: ChromiumPage,
    record: Dict[str, Any],
    timeout_sec: float,
    per_doi_deadline_sec: float,
    max_nav_attempts: int,
    capture_fail_artifacts: bool,
    capture_fail_screenshot: bool,
    capture_success_artifacts: bool,
    capture_success_html: bool,
    artifact_dir: str,
    worker_idx: int,
    browser_identity: str,
    scheduled_start_ms: int,
    actual_start_ms: int,
    pacing_wait_ms: int,
    pacing_penalty_wait_ms: int,
    pacing_jitter_sec: float,
    probe_page_meta: Dict[str, Any],
) -> Dict[str, Any]:
    started = time.perf_counter()
    deadline = time.monotonic() + min(110.0, max(15.0, float(per_doi_deadline_sec)))
    doi = str(record.get("doi") or "")
    doi_url = f"https://doi.org/{doi}"
    input_publisher = str(record.get("input_publisher") or "")
    scheduler_publisher = str(record.get("scheduler_publisher") or "")
    expected_domains = expected_domains_for_record(record)
    navigation_chain: List[Dict[str, str]] = []
    final_url = ""
    title = ""
    html = ""
    snapshot: Dict[str, Any] = {}
    issue = ""
    issue_evidence: List[str] = []
    classifier_state = STATE_UNKNOWN_NON_SUCCESS
    reason_codes: List[str] = []
    exception_kind = ""
    exception_message = ""
    legacy_success_like = False
    article_signal = False
    pdf_action_signal = False
    consent_signal = False
    direct_pdf_path = ""
    direct_pdf_event: Dict[str, Any] = {}
    entry_plan: Dict[str, Any] = {}
    entry_browser_open_skipped = False
    entry_handoff_used = False
    entry_handoff_url = ""
    initial_landing_type = ""
    shell_recovery_attempted = False
    shell_recovery_strategy = ""
    shell_recovery_outcome = ""
    reclassified_after_detector_fix = False
    reclassification_reason = ""
    tab_transition_events: List[Dict[str, Any]] = []
    attempt_history: List[Dict[str, Any]] = []
    timing_breakdown: Dict[str, Any] = {
        "scheduled_start_ms": int(scheduled_start_ms or 0),
        "actual_start_ms": int(actual_start_ms or _now_ms()),
        "pacing_wait_ms": int(max(0, pacing_wait_ms or 0)),
        "pacing_penalty_wait_ms": int(max(0, pacing_penalty_wait_ms or 0)),
        "pacing_jitter_sec": float(pacing_jitter_sec or 0.0),
        "attempts": [],
    }
    snapshot_signal_summary: Dict[str, Any] = {}
    artifact_page = page
    temp_artifact_page: ChromiumPage | None = None
    powdermat_success_candidate: Dict[str, Any] = {}

    for attempt_idx in range(max(1, int(max_nav_attempts))):
        attempt_started = time.perf_counter()
        attempt_timing: Dict[str, Any] = {"attempt": attempt_idx + 1}
        if time.monotonic() >= deadline:
            exception_kind = "timeout"
            exception_message = "per_doi_deadline_exceeded_before_navigation"
            classifier_state = STATE_TIMEOUT
            reason_codes = ["per_doi_deadline_exceeded"]
            attempt_timing["attempt_elapsed_ms"] = int((time.perf_counter() - attempt_started) * 1000)
            timing_breakdown["attempts"].append(attempt_timing)
            break

        try:
            initial_download_files: List[str] = []
            worker_download_dir = str(probe_page_meta.get("worker_download_dir") or "").strip()
            if worker_download_dir:
                try:
                    initial_download_files = sorted(_get_current_files(worker_download_dir))
                except Exception:
                    initial_download_files = []
            _prune_extra_tabs(page)
            try:
                reset_started = time.perf_counter()
                page.get("about:blank", retry=0, interval=0.2, timeout=5)
                _append_nav_step(navigation_chain, "pre_reset", "about:blank", page.url or "about:blank")
                attempt_timing["pre_reset_ms"] = int((time.perf_counter() - reset_started) * 1000)
            except Exception:
                attempt_timing["pre_reset_ms"] = int((time.perf_counter() - attempt_started) * 1000)
            listener_started = _start_attempt_listener(page)
            attempt_timing["network_listener"] = bool(listener_started)

            step_timeout = _remaining_budget(deadline, timeout_sec, floor_sec=5.0)
            entry_url = ""
            if doi.startswith("10.1016/"):
                entry_plan = build_elsevier_safe_entry_plan(doi_url)
                if entry_plan.get("entry_strategy"):
                    attempt_timing["entry_strategy"] = str(entry_plan.get("entry_strategy") or "")
                if entry_plan.get("entry_browser_url"):
                    attempt_timing["entry_browser_url"] = str(entry_plan.get("entry_browser_url") or "")
                if entry_plan.get("entry_url"):
                    attempt_timing["entry_url_candidate"] = str(entry_plan.get("entry_url") or "")
                if entry_plan.get("entry_handoff_url"):
                    attempt_timing["entry_handoff_url"] = str(entry_plan.get("entry_handoff_url") or "")[:240]
                if entry_plan.get("entry_redirect_chain_summary"):
                    attempt_timing["entry_redirect_chain_summary"] = list(entry_plan.get("entry_redirect_chain_summary") or [])
                if entry_plan.get("entry_fallback_reason"):
                    attempt_timing["entry_fallback_reason"] = str(entry_plan.get("entry_fallback_reason") or "")
                if entry_plan.get("entry_resolved_url"):
                    _append_nav_step(
                        navigation_chain,
                        "elsevier_resolve",
                        doi_url,
                        str(entry_plan.get("entry_resolved_url") or doi_url),
                    )
                if _should_skip_elsevier_browser_open(entry_plan):
                    final_url = str(
                        entry_plan.get("entry_preflight_url")
                        or entry_plan.get("entry_browser_url")
                        or entry_plan.get("entry_url")
                        or entry_plan.get("entry_resolved_url")
                        or doi_url
                    )
                    title = str(entry_plan.get("entry_preflight_title") or "")
                    html = str(entry_plan.get("entry_preflight_html") or "")
                    issue = str(entry_plan.get("entry_preflight_issue") or "FAIL_BLOCK")
                    issue_evidence = list(entry_plan.get("entry_preflight_evidence") or [])
                    if issue == OUT_FAIL_DOI_NOT_FOUND:
                        classifier_state = STATE_DOI_NOT_FOUND
                    elif issue == OUT_FAIL_ACCESS_RIGHTS:
                        classifier_state = STATE_CONSENT_OR_INTERSTITIAL_BLOCK
                    elif issue in (OUT_FAIL_BLOCK, OUT_FAIL_CAPTCHA):
                        classifier_state = STATE_CHALLENGE_DETECTED
                    else:
                        classifier_state = STATE_UNKNOWN_NON_SUCCESS
                    reason_codes = list(dict.fromkeys(issue_evidence + ["elsevier_preflight_skip"]))
                    _append_nav_step(
                        navigation_chain,
                        "elsevier_preflight_skip",
                        str(entry_plan.get("entry_browser_url") or entry_plan.get("entry_url") or doi_url),
                        final_url or doi_url,
                    )
                    attempt_timing["entry_browser_open_skipped"] = True
                    entry_browser_open_skipped = True
                    attempt_timing["entry_preflight_issue"] = issue
                    attempt_timing["attempt_elapsed_ms"] = int((time.perf_counter() - attempt_started) * 1000)
                    timing_breakdown["attempts"].append(attempt_timing)
                    attempt_history.append(
                        {
                            "attempt": attempt_idx + 1,
                            "classifier_state": classifier_state,
                            "reason_codes": reason_codes[:8],
                            "final_url": final_url,
                            "timing_ms": dict(attempt_timing),
                        }
                    )
                    try:
                        page.listen.stop()
                        page.listen.clear()
                    except Exception:
                        pass
                    break
                entry_url = str(
                    entry_plan.get("entry_browser_url")
                    or entry_plan.get("entry_resolved_url")
                    or entry_plan.get("entry_url")
                    or ""
                )
            if not entry_url and not doi.startswith("10.1016/"):
                entry_url = _resolve_structural_entry_url(record=record, doi_url=doi_url)
            nav_url = entry_url or doi_url
            if entry_url and entry_url.lower() != doi_url.lower():
                attempt_timing["entry_url_override"] = entry_url
            nav_started = time.perf_counter()
            page.get(nav_url, retry=0, interval=0.5, timeout=step_timeout)
            attempt_timing["doi_get_ms"] = int((time.perf_counter() - nav_started) * 1000)
            _append_nav_step(navigation_chain, "doi_get", nav_url, page.url or nav_url)
            page = _adopt_latest_probe_tab(
                page,
                navigation_chain=navigation_chain,
                attempt_timing=attempt_timing,
                tab_transition_events=tab_transition_events,
                step_label="doi_get_tab_sync",
                force=True,
            )
            _dismiss_cookie_or_consent_banner(page)
            if (
                doi.startswith("10.1016/")
                and time.monotonic() < deadline
                and (
                    _is_elsevier_retrieve_url(page.url or nav_url)
                    or str(entry_plan.get("entry_handoff_url") or "").strip()
                )
            ):
                initial_elsevier_recovery = _recover_elsevier_via_retrieve_link(
                    page=page,
                    doi=doi,
                    entry_plan=entry_plan,
                    deadline_monotonic=deadline,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                    tab_transition_events=tab_transition_events,
                    step_prefix="elsevier_initial",
                )
                if initial_elsevier_recovery.get("attempted"):
                    page = initial_elsevier_recovery.get("page") or page
                    entry_handoff_used = bool(initial_elsevier_recovery.get("handoff_used") or initial_elsevier_recovery.get("doi_click_used"))
                    entry_handoff_url = str(initial_elsevier_recovery.get("handoff_url") or entry_handoff_url)
            if str(record.get("scheduler_publisher") or "") == "powdermat":
                immediate_url = page.url or nav_url
                immediate_title = page.title or ""
                immediate_html = page.html or ""
                immediate_snapshot = collect_page_snapshot(page, title=immediate_title, html=immediate_html)
                immediate_eval = _evaluate_page_state(
                    record=record,
                    expected_domains=expected_domains,
                    final_url=immediate_url,
                    title=immediate_title,
                    html=immediate_html,
                    snapshot=immediate_snapshot,
                )
                if (
                    immediate_eval.get("classifier_state") in SUCCESS_STATES
                    and "/journal/view.php?" in str(immediate_url or "").lower()
                ):
                    powdermat_success_candidate = {
                        "final_url": immediate_url,
                        "title": immediate_title,
                        "html": immediate_html,
                        "snapshot": immediate_snapshot,
                        **immediate_eval,
                    }
                    attempt_timing["powdermat_early_success_url"] = immediate_url
            direct_pdf_path, direct_pdf_event = _capture_direct_pdf_handoff(
                record=record,
                probe_page_meta=probe_page_meta,
                artifact_dir=artifact_dir,
                initial_download_files=initial_download_files,
                timeout_s=0,
                page=page,
                listener_timeout_s=0.4,
            )
            if direct_pdf_path or direct_pdf_event:
                final_url = page.url or nav_url or doi_url
                title = page.title or ""
                html = page.html or ""
                classifier_state = STATE_DIRECT_PDF_HANDOFF
                reason_codes = ["direct_pdf_downloaded" if direct_pdf_path else "direct_pdf_response_observed"]
                attempt_timing["direct_pdf_path"] = direct_pdf_path
                if direct_pdf_event:
                    attempt_timing["direct_pdf_event"] = dict(direct_pdf_event)
                attempt_timing["attempt_elapsed_ms"] = int((time.perf_counter() - attempt_started) * 1000)
                timing_breakdown["attempts"].append(attempt_timing)
                attempt_history.append(
                    {
                        "attempt": attempt_idx + 1,
                        "classifier_state": classifier_state,
                        "reason_codes": reason_codes[:8],
                        "final_url": final_url,
                        "timing_ms": dict(attempt_timing),
                    }
                )
                try:
                    page.listen.stop()
                    page.listen.clear()
                except Exception:
                    pass
                break

            final_url = page.url or nav_url or doi_url
            title = page.title or ""
            html = page.html or ""
            final_url, title, html, placeholder_wait_ms = _wait_for_non_placeholder_state(
                page=page,
                deadline_monotonic=deadline,
            )
            attempt_timing["placeholder_wait_ms"] = placeholder_wait_ms
            if placeholder_wait_ms > 0:
                _append_nav_step(navigation_chain, "placeholder_wait", nav_url, final_url or nav_url)
            if _looks_like_blank_screen_context(final_url, title, html):
                page = _adopt_latest_probe_tab(
                    page,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                    tab_transition_events=tab_transition_events,
                    step_label="placeholder_tab_sync",
                    force=True,
                )
                final_url = page.url or final_url or nav_url or doi_url
                title = page.title or title
                html = page.html or html
            if not direct_pdf_path and not direct_pdf_event:
                direct_pdf_path, direct_pdf_event = _capture_direct_pdf_handoff(
                    record=record,
                    probe_page_meta=probe_page_meta,
                    artifact_dir=artifact_dir,
                    initial_download_files=initial_download_files,
                    timeout_s=2,
                    page=page,
                    listener_timeout_s=0.8,
                )
            if direct_pdf_path or direct_pdf_event:
                classifier_state = STATE_DIRECT_PDF_HANDOFF
                reason_codes = ["direct_pdf_downloaded" if direct_pdf_path else "direct_pdf_response_observed"]
                attempt_timing["direct_pdf_path"] = direct_pdf_path
                if direct_pdf_event:
                    attempt_timing["direct_pdf_event"] = dict(direct_pdf_event)
                attempt_timing["attempt_elapsed_ms"] = int((time.perf_counter() - attempt_started) * 1000)
                timing_breakdown["attempts"].append(attempt_timing)
                attempt_history.append(
                    {
                        "attempt": attempt_idx + 1,
                        "classifier_state": classifier_state,
                        "reason_codes": reason_codes[:8],
                        "final_url": final_url or nav_url or doi_url,
                        "timing_ms": dict(attempt_timing),
                    }
                )
                try:
                    page.listen.stop()
                    page.listen.clear()
                except Exception:
                    pass
                break

            if _is_elsevier_retrieve_url(final_url) and time.monotonic() < deadline:
                time.sleep(min(1.2, max(0.0, deadline - time.monotonic())))
                _dismiss_cookie_or_consent_banner(page)
                final_url = page.url or final_url
                title = page.title or title
                html = page.html or html
                if _is_elsevier_retrieve_url(final_url):
                    clicked = _click_elsevier_doi_link_in_retrieve(page, doi)
                    if clicked and time.monotonic() < deadline:
                        click_wait_timeout = _remaining_budget(deadline, min(timeout_sec, 8.0), floor_sec=3.0)
                        wait_started = time.perf_counter()
                        _wait_for_elsevier_article_ready(page, doi, timeout_s=min(8, int(max(3.0, click_wait_timeout))))
                        attempt_timing["elsevier_retrieve_click_wait_ms"] = int((time.perf_counter() - wait_started) * 1000)
                        page = _adopt_latest_probe_tab(
                            page,
                            navigation_chain=navigation_chain,
                            attempt_timing=attempt_timing,
                            tab_transition_events=tab_transition_events,
                            step_label="elsevier_retrieve_click_tab_sync",
                            force=True,
                        )
                        _dismiss_cookie_or_consent_banner(page)
                        final_url = page.url or final_url
                        title = page.title or title
                        html = page.html or html
                        if not _is_elsevier_retrieve_url(final_url):
                            entry_handoff_used = True
                            entry_handoff_url = "retrieve_doi_click"
                            _append_nav_step(navigation_chain, "elsevier_retrieve_click", doi_url, final_url or doi_url)
                    handoff = _extract_elsevier_retrieve_handoff_url(final_url, html) or str(entry_plan.get("entry_handoff_url") or "")
                    if handoff and time.monotonic() < deadline:
                        handoff_timeout = _remaining_budget(deadline, min(timeout_sec, 12.0), floor_sec=4.0)
                        handoff_started = time.perf_counter()
                        page.get(handoff, retry=0, interval=0.5, timeout=handoff_timeout)
                        attempt_timing["elsevier_handoff_ms"] = int((time.perf_counter() - handoff_started) * 1000)
                        _append_nav_step(navigation_chain, "elsevier_handoff", handoff, page.url or handoff)
                        page = _adopt_latest_probe_tab(
                            page,
                            navigation_chain=navigation_chain,
                            attempt_timing=attempt_timing,
                            tab_transition_events=tab_transition_events,
                            step_label="elsevier_handoff_tab_sync",
                            force=True,
                        )
                        entry_handoff_used = True
                        entry_handoff_url = handoff
                        _dismiss_cookie_or_consent_banner(page)
                        final_url = page.url or final_url
                        title = page.title or title
                        html = page.html or html
            elsevier_canonical_url = _normalize_elsevier_article_url(final_url=final_url, snapshot={})
            if elsevier_canonical_url and time.monotonic() < deadline:
                canonical_timeout = _remaining_budget(deadline, min(timeout_sec, 8.0), floor_sec=3.0)
                canonical_started = time.perf_counter()
                page.get(elsevier_canonical_url, retry=0, interval=0.4, timeout=canonical_timeout)
                attempt_timing["elsevier_canonical_ms"] = int((time.perf_counter() - canonical_started) * 1000)
                _append_nav_step(navigation_chain, "elsevier_canonical", elsevier_canonical_url, page.url or elsevier_canonical_url)
                page = _adopt_latest_probe_tab(
                    page,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                    tab_transition_events=tab_transition_events,
                    step_label="elsevier_canonical_tab_sync",
                    force=True,
                )
                _dismiss_cookie_or_consent_banner(page)
                final_url = page.url or elsevier_canonical_url
                title = page.title or title
                html = page.html or html

            stabilize_started = time.perf_counter()
            title, html, snapshot = stabilize_page_state(page, title=title, html=html, deadline_monotonic=deadline)
            attempt_timing["stabilize_ms"] = int((time.perf_counter() - stabilize_started) * 1000)
            final_url = page.url or final_url or doi_url
            preferred_article_url = ""
            if _should_try_preferred_article_handoff(final_url=final_url, title=title, snapshot=snapshot):
                preferred_article_url = _extract_preferred_article_url(final_url=final_url, snapshot=snapshot)
            if preferred_article_url and time.monotonic() < deadline:
                preferred_timeout = _remaining_budget(deadline, min(timeout_sec, 12.0), floor_sec=4.0)
                preferred_started = time.perf_counter()
                page.get(preferred_article_url, retry=0, interval=0.5, timeout=preferred_timeout)
                attempt_timing["preferred_handoff_ms"] = int((time.perf_counter() - preferred_started) * 1000)
                _append_nav_step(
                    navigation_chain,
                    "preferred_handoff",
                    preferred_article_url,
                    page.url or preferred_article_url,
                )
                page = _adopt_latest_probe_tab(
                    page,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                    tab_transition_events=tab_transition_events,
                    step_label="preferred_handoff_tab_sync",
                    force=True,
                )
                _dismiss_cookie_or_consent_banner(page)
                final_url = page.url or preferred_article_url
                title = page.title or title
                html = page.html or html
                stabilize_started = time.perf_counter()
                title, html, snapshot = stabilize_page_state(page, title=title, html=html, deadline_monotonic=deadline)
                attempt_timing["preferred_handoff_stabilize_ms"] = int((time.perf_counter() - stabilize_started) * 1000)
                final_url = page.url or final_url or preferred_article_url
            final_url, title, html, snapshot, elsevier_shell_action, shell_recovery_meta = _recover_elsevier_article_shell(
                page=page,
                doi=doi,
                final_url=final_url,
                title=title,
                html=html,
                snapshot=snapshot,
                entry_plan=entry_plan,
                deadline_monotonic=deadline,
                navigation_chain=navigation_chain,
                attempt_timing=attempt_timing,
                tab_transition_events=tab_transition_events,
            )
            if elsevier_shell_action:
                attempt_timing["elsevier_shell_action"] = elsevier_shell_action
            initial_landing_type = str(shell_recovery_meta.get("initial_landing_type") or initial_landing_type)
            shell_recovery_attempted = bool(shell_recovery_meta.get("shell_recovery_attempted") or shell_recovery_attempted)
            shell_recovery_strategy = str(shell_recovery_meta.get("shell_recovery_strategy") or shell_recovery_strategy)
            shell_recovery_outcome = str(shell_recovery_meta.get("shell_recovery_outcome") or shell_recovery_outcome)
            issue, issue_evidence = detect_access_issue(title=title, html=html, url=final_url, domain="")
            article_signal = bool(_has_article_signal(title=title, html=html))
            pdf_action_signal = bool(_has_pdf_action_signal(title=title, html=html))
            consent_signal = bool(_has_cookie_or_consent_signal(title=title, html=html))
            legacy_success_like = _legacy_verify_landing_success(
                doi=doi,
                url=final_url,
                domain=_extract_domain(str(snapshot.get("canonical_url") or "") or final_url),
                title=title,
                html=html,
                article_signal=article_signal,
                pdf_action_signal=pdf_action_signal,
            )

            classified = classify_landing(
                doi=doi,
                input_publisher=input_publisher,
                scheduler_publisher=scheduler_publisher,
                final_url=final_url,
                title=title,
                html=html,
                snapshot=snapshot,
                issue=issue or "",
                issue_evidence=issue_evidence or [],
                exception_kind="",
                expected_domains=expected_domains,
            )
            classifier_state = classified.get("classifier_state", STATE_UNKNOWN_NON_SUCCESS)
            reason_codes = list(classified.get("reason_codes", []) or [])
            snapshot_signal_summary = dict(classified.get("signal_summary") or {})
            reclassified_after_detector_fix = bool(classified.get("reclassified_after_detector_fix"))
            reclassification_reason = str(classified.get("reclassification_reason") or "")

            if classifier_state == STATE_BROKEN_JS_SHELL and attempt_idx == 0 and time.monotonic() < deadline:
                recovery_started = time.perf_counter()
                recovery_action = _try_broken_shell_fallback(page, deadline_monotonic=deadline)
                attempt_timing["broken_shell_recovery_action"] = recovery_action or ""
                attempt_timing["broken_shell_recovery_ms"] = int((time.perf_counter() - recovery_started) * 1000)
                if recovery_action:
                    _dismiss_cookie_or_consent_banner(page)
                    final_url = page.url or final_url or doi_url
                    title = page.title or title
                    html = page.html or html
                    stabilize_started = time.perf_counter()
                    title, html, snapshot = stabilize_page_state(page, title=title, html=html, deadline_monotonic=deadline)
                    attempt_timing["broken_shell_recovery_stabilize_ms"] = int((time.perf_counter() - stabilize_started) * 1000)
                    final_url = page.url or final_url or doi_url
                    issue, issue_evidence = detect_access_issue(title=title, html=html, url=final_url, domain="")
                    article_signal = bool(_has_article_signal(title=title, html=html))
                    pdf_action_signal = bool(_has_pdf_action_signal(title=title, html=html))
                    consent_signal = bool(_has_cookie_or_consent_signal(title=title, html=html))
                    legacy_success_like = _legacy_verify_landing_success(
                        doi=doi,
                        url=final_url,
                        domain=_extract_domain(str(snapshot.get("canonical_url") or "") or final_url),
                        title=title,
                        html=html,
                        article_signal=article_signal,
                        pdf_action_signal=pdf_action_signal,
                    )
                    classified = classify_landing(
                        doi=doi,
                        input_publisher=input_publisher,
                        scheduler_publisher=scheduler_publisher,
                        final_url=final_url,
                        title=title,
                        html=html,
                        snapshot=snapshot,
                        issue=issue or "",
                        issue_evidence=issue_evidence or [],
                        exception_kind="",
                        expected_domains=expected_domains,
                    )
                    classifier_state = classified.get("classifier_state", STATE_UNKNOWN_NON_SUCCESS)
                    reason_codes = list(classified.get("reason_codes", []) or [])
                    snapshot_signal_summary = dict(classified.get("signal_summary") or {})
                    reclassified_after_detector_fix = bool(classified.get("reclassified_after_detector_fix"))
                    reclassification_reason = str(classified.get("reclassification_reason") or "")

            if (
                classifier_state not in SUCCESS_STATES
                and str(record.get("scheduler_publisher") or "") == "elsevier"
                and attempt_idx == 0
                and time.monotonic() < deadline
            ):
                elsevier_temp_recovery = _recover_elsevier_headless_temp_tab(
                    page=page,
                    record=record,
                    expected_domains=expected_domains,
                    doi_url=doi_url,
                    final_url=final_url,
                    snapshot=snapshot,
                    entry_plan=entry_plan,
                    deadline_monotonic=deadline,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                    tab_transition_events=tab_transition_events,
                )
                if elsevier_temp_recovery:
                    final_url = elsevier_temp_recovery.get("final_url", final_url)
                    title = elsevier_temp_recovery.get("title", title)
                    html = elsevier_temp_recovery.get("html", html)
                    snapshot = dict(elsevier_temp_recovery.get("snapshot") or snapshot)
                    issue = str(elsevier_temp_recovery.get("issue") or "")
                    issue_evidence = list(elsevier_temp_recovery.get("issue_evidence") or [])
                    article_signal = bool(elsevier_temp_recovery.get("article_signal"))
                    pdf_action_signal = bool(elsevier_temp_recovery.get("pdf_action_signal"))
                    consent_signal = bool(elsevier_temp_recovery.get("consent_signal"))
                    legacy_success_like = bool(elsevier_temp_recovery.get("legacy_success_like"))
                    classifier_state = str(elsevier_temp_recovery.get("classifier_state") or classifier_state)
                    reason_codes = list(elsevier_temp_recovery.get("reason_codes") or reason_codes)
                    snapshot_signal_summary = dict(elsevier_temp_recovery.get("signal_summary") or snapshot_signal_summary)
                    reclassified_after_detector_fix = bool(elsevier_temp_recovery.get("reclassified_after_detector_fix") or reclassified_after_detector_fix)
                    reclassification_reason = str(elsevier_temp_recovery.get("reclassification_reason") or reclassification_reason)
                    temp_page = elsevier_temp_recovery.get("page")
                    if classifier_state in SUCCESS_STATES and temp_page is not None:
                        if temp_artifact_page is not None and temp_artifact_page is not temp_page:
                            _close_temporary_tab(page, temp_artifact_page)
                        artifact_page = temp_page
                        temp_artifact_page = temp_page
                    else:
                        _close_temporary_tab(page, temp_page)

            if (
                classifier_state == STATE_PUBLISHER_ERROR
                and str(record.get("scheduler_publisher") or "") == "ieee"
                and attempt_idx == 0
                and time.monotonic() < deadline
            ):
                ieee_recovery = _recover_ieee_via_title_search(
                    page=page,
                    record=record,
                    expected_domains=expected_domains,
                    input_title=str(record.get("input_title") or ""),
                    deadline_monotonic=deadline,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                )
                if ieee_recovery:
                    final_url = ieee_recovery.get("final_url", final_url)
                    title = ieee_recovery.get("title", title)
                    html = ieee_recovery.get("html", html)
                    snapshot = dict(ieee_recovery.get("snapshot") or snapshot)
                    issue = str(ieee_recovery.get("issue") or "")
                    issue_evidence = list(ieee_recovery.get("issue_evidence") or [])
                    article_signal = bool(ieee_recovery.get("article_signal"))
                    pdf_action_signal = bool(ieee_recovery.get("pdf_action_signal"))
                    consent_signal = bool(ieee_recovery.get("consent_signal"))
                    legacy_success_like = bool(ieee_recovery.get("legacy_success_like"))
                    classifier_state = str(ieee_recovery.get("classifier_state") or classifier_state)
                    reason_codes = list(ieee_recovery.get("reason_codes") or reason_codes)
                    snapshot_signal_summary = dict(ieee_recovery.get("signal_summary") or snapshot_signal_summary)
                    reclassified_after_detector_fix = bool(ieee_recovery.get("reclassified_after_detector_fix") or reclassified_after_detector_fix)
                    reclassification_reason = str(ieee_recovery.get("reclassification_reason") or reclassification_reason)
                    temp_page = ieee_recovery.get("page")
                    if classifier_state in SUCCESS_STATES and temp_page is not None:
                        if temp_artifact_page is not None and temp_artifact_page is not temp_page:
                            _close_temporary_tab(page, temp_artifact_page)
                        artifact_page = temp_page
                        temp_artifact_page = temp_page
                    else:
                        _close_temporary_tab(page, temp_page)

            if (
                classifier_state not in SUCCESS_STATES
                and classifier_state != STATE_CHALLENGE_DETECTED
                and attempt_idx == 0
                and time.monotonic() < deadline
            ):
                targeted_recovery_url = _extract_targeted_recovery_url(
                    record=record,
                    final_url=final_url,
                    title=title,
                    html=html,
                    snapshot=snapshot,
                )
                if targeted_recovery_url:
                    recover_timeout = _remaining_budget(deadline, min(timeout_sec, 12.0), floor_sec=4.0)
                    recover_started = time.perf_counter()
                    page.get(targeted_recovery_url, retry=0, interval=0.5, timeout=recover_timeout)
                    attempt_timing["targeted_recovery_ms"] = int((time.perf_counter() - recover_started) * 1000)
                    attempt_timing["targeted_recovery_url"] = targeted_recovery_url
                    _append_nav_step(navigation_chain, "targeted_recovery", targeted_recovery_url, page.url or targeted_recovery_url)
                    page = _adopt_latest_probe_tab(
                        page,
                        navigation_chain=navigation_chain,
                        attempt_timing=attempt_timing,
                        tab_transition_events=tab_transition_events,
                        step_label="targeted_recovery_tab_sync",
                        force=True,
                    )
                    _dismiss_cookie_or_consent_banner(page)
                    final_url = page.url or targeted_recovery_url
                    title = page.title or title
                    html = page.html or html
                    stabilize_started = time.perf_counter()
                    title, html, snapshot = stabilize_page_state(page, title=title, html=html, deadline_monotonic=deadline)
                    attempt_timing["targeted_recovery_stabilize_ms"] = int((time.perf_counter() - stabilize_started) * 1000)
                    final_url = page.url or final_url or targeted_recovery_url
                    issue, issue_evidence = detect_access_issue(title=title, html=html, url=final_url, domain="")
                    article_signal = bool(_has_article_signal(title=title, html=html))
                    pdf_action_signal = bool(_has_pdf_action_signal(title=title, html=html))
                    consent_signal = bool(_has_cookie_or_consent_signal(title=title, html=html))
                    legacy_success_like = _legacy_verify_landing_success(
                        doi=doi,
                        url=final_url,
                        domain=_extract_domain(str(snapshot.get("canonical_url") or "") or final_url),
                        title=title,
                        html=html,
                        article_signal=article_signal,
                        pdf_action_signal=pdf_action_signal,
                    )
                    classified = classify_landing(
                        doi=doi,
                        input_publisher=input_publisher,
                        scheduler_publisher=scheduler_publisher,
                        final_url=final_url,
                        title=title,
                        html=html,
                        snapshot=snapshot,
                        issue=issue or "",
                        issue_evidence=issue_evidence or [],
                        exception_kind="",
                        expected_domains=expected_domains,
                    )
                    classifier_state = classified.get("classifier_state", STATE_UNKNOWN_NON_SUCCESS)
                    reason_codes = list(classified.get("reason_codes", []) or [])
                    snapshot_signal_summary = dict(classified.get("signal_summary") or {})
                    reclassified_after_detector_fix = bool(classified.get("reclassified_after_detector_fix"))
                    reclassification_reason = str(classified.get("reclassification_reason") or "")

            if (
                classifier_state not in SUCCESS_STATES
                and classifier_state != STATE_CHALLENGE_DETECTED
                and str(record.get("scheduler_publisher") or "") == "powdermat"
                and attempt_idx == 0
                and time.monotonic() < deadline
            ):
                powdermat_static_recovery = _recover_powdermat_static_entry(
                    record=record,
                    expected_domains=expected_domains,
                )
                if powdermat_static_recovery:
                    final_url = powdermat_static_recovery.get("final_url", final_url)
                    title = powdermat_static_recovery.get("title", title)
                    html = powdermat_static_recovery.get("html", html)
                    snapshot = dict(powdermat_static_recovery.get("snapshot") or snapshot)
                    issue = str(powdermat_static_recovery.get("issue") or "")
                    issue_evidence = list(powdermat_static_recovery.get("issue_evidence") or [])
                    article_signal = bool(powdermat_static_recovery.get("article_signal"))
                    pdf_action_signal = bool(powdermat_static_recovery.get("pdf_action_signal"))
                    consent_signal = bool(powdermat_static_recovery.get("consent_signal"))
                    legacy_success_like = bool(powdermat_static_recovery.get("legacy_success_like"))
                    classifier_state = str(powdermat_static_recovery.get("classifier_state") or classifier_state)
                    reason_codes = list(dict.fromkeys(list(powdermat_static_recovery.get("reason_codes") or reason_codes) + ["powdermat_static_entry"]))
                    snapshot_signal_summary = dict(powdermat_static_recovery.get("signal_summary") or snapshot_signal_summary)
                powdermat_back_recovery = _recover_powdermat_via_back(
                    page=page,
                    record=record,
                    expected_domains=expected_domains,
                    final_url=final_url,
                    deadline_monotonic=deadline,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                )
                if powdermat_back_recovery and classifier_state not in SUCCESS_STATES:
                    final_url = powdermat_back_recovery.get("final_url", final_url)
                    title = powdermat_back_recovery.get("title", title)
                    html = powdermat_back_recovery.get("html", html)
                    snapshot = dict(powdermat_back_recovery.get("snapshot") or snapshot)
                    issue = str(powdermat_back_recovery.get("issue") or "")
                    issue_evidence = list(powdermat_back_recovery.get("issue_evidence") or [])
                    article_signal = bool(powdermat_back_recovery.get("article_signal"))
                    pdf_action_signal = bool(powdermat_back_recovery.get("pdf_action_signal"))
                    consent_signal = bool(powdermat_back_recovery.get("consent_signal"))
                    legacy_success_like = bool(powdermat_back_recovery.get("legacy_success_like"))
                    classifier_state = str(powdermat_back_recovery.get("classifier_state") or classifier_state)
                    reason_codes = list(dict.fromkeys(list(powdermat_back_recovery.get("reason_codes") or reason_codes) + ["powdermat_back_recovery"]))
                    snapshot_signal_summary = dict(powdermat_back_recovery.get("signal_summary") or snapshot_signal_summary)
                powdermat_recovery = _recover_powdermat_article_target(
                    page=page,
                    record=record,
                    expected_domains=expected_domains,
                    final_url=final_url,
                    deadline_monotonic=deadline,
                    navigation_chain=navigation_chain,
                    attempt_timing=attempt_timing,
                )
                if powdermat_recovery and classifier_state not in SUCCESS_STATES:
                    final_url = powdermat_recovery.get("final_url", final_url)
                    title = powdermat_recovery.get("title", title)
                    html = powdermat_recovery.get("html", html)
                    snapshot = dict(powdermat_recovery.get("snapshot") or snapshot)
                    issue = str(powdermat_recovery.get("issue") or "")
                    issue_evidence = list(powdermat_recovery.get("issue_evidence") or [])
                    article_signal = bool(powdermat_recovery.get("article_signal"))
                    pdf_action_signal = bool(powdermat_recovery.get("pdf_action_signal"))
                    consent_signal = bool(powdermat_recovery.get("consent_signal"))
                    legacy_success_like = bool(powdermat_recovery.get("legacy_success_like"))
                    classifier_state = str(powdermat_recovery.get("classifier_state") or classifier_state)
                    reason_codes = list(powdermat_recovery.get("reason_codes") or reason_codes)
                    snapshot_signal_summary = dict(powdermat_recovery.get("signal_summary") or snapshot_signal_summary)
                    temp_page = powdermat_recovery.get("page")
                    if classifier_state in SUCCESS_STATES and temp_page is not None:
                        if temp_artifact_page is not None and temp_artifact_page is not temp_page:
                            _close_temporary_tab(page, temp_artifact_page)
                        artifact_page = temp_page
                        temp_artifact_page = temp_page
                    else:
                        _close_temporary_tab(page, temp_page)
            if (
                classifier_state not in SUCCESS_STATES
                and powdermat_success_candidate
                and "/authors/copyright_transfer_agreement.php" in str(final_url or "").lower()
            ):
                final_url = powdermat_success_candidate.get("final_url", final_url)
                title = powdermat_success_candidate.get("title", title)
                html = powdermat_success_candidate.get("html", html)
                snapshot = dict(powdermat_success_candidate.get("snapshot") or snapshot)
                issue = str(powdermat_success_candidate.get("issue") or issue)
                issue_evidence = list(powdermat_success_candidate.get("issue_evidence") or issue_evidence)
                article_signal = bool(powdermat_success_candidate.get("article_signal"))
                pdf_action_signal = bool(powdermat_success_candidate.get("pdf_action_signal"))
                consent_signal = bool(powdermat_success_candidate.get("consent_signal"))
                legacy_success_like = bool(powdermat_success_candidate.get("legacy_success_like"))
                classifier_state = str(powdermat_success_candidate.get("classifier_state") or classifier_state)
                reason_codes = list(dict.fromkeys(list(powdermat_success_candidate.get("reason_codes") or reason_codes) + ["powdermat_post_article_redirect"]))
                snapshot_signal_summary = dict(powdermat_success_candidate.get("signal_summary") or snapshot_signal_summary)
                attempt_timing["powdermat_preserved_success"] = True
            try:
                page.listen.stop()
                page.listen.clear()
            except Exception:
                pass

            attempt_timing["attempt_elapsed_ms"] = int((time.perf_counter() - attempt_started) * 1000)
            timing_breakdown["attempts"].append(attempt_timing)

            if not _should_retry_landing(classifier_state, reason_codes, attempt_idx, max_nav_attempts):
                attempt_history.append(
                    {
                        "attempt": attempt_idx + 1,
                        "classifier_state": classifier_state,
                        "reason_codes": reason_codes[:8],
                        "final_url": final_url,
                        "timing_ms": dict(attempt_timing),
                    }
                )
                break

            backoff_sec = min(4.5, 1.5 * (2 ** attempt_idx) + random.uniform(0.2, 0.8))
            attempt_timing["retry_backoff_ms"] = int(backoff_sec * 1000)
            attempt_history.append(
                {
                    "attempt": attempt_idx + 1,
                    "classifier_state": classifier_state,
                    "reason_codes": reason_codes[:8],
                    "final_url": final_url,
                    "timing_ms": dict(attempt_timing),
                }
            )
            if time.monotonic() + backoff_sec >= deadline:
                break
            time.sleep(backoff_sec)
            issue = ""
            issue_evidence = []
            continue

        except Exception as e:
            exception_message = str(e)[:400]
            exception_kind = "timeout" if _looks_like_timeout_error(e) else "network"
            classifier_state = STATE_TIMEOUT if exception_kind == "timeout" else STATE_NETWORK_ERROR
            reason_codes = ["navigation_timeout" if exception_kind == "timeout" else "navigation_network_error"]
            try:
                page.listen.stop()
                page.listen.clear()
            except Exception:
                pass
            attempt_timing["exception_kind"] = exception_kind
            attempt_timing["attempt_elapsed_ms"] = int((time.perf_counter() - attempt_started) * 1000)
            timing_breakdown["attempts"].append(attempt_timing)
            attempt_history.append(
                {
                    "attempt": attempt_idx + 1,
                    "classifier_state": classifier_state,
                    "reason_codes": reason_codes[:8],
                    "final_url": final_url or doi_url,
                    "timing_ms": dict(attempt_timing),
                }
            )
            if not _should_retry_landing(classifier_state, reason_codes, attempt_idx, max_nav_attempts):
                break
            backoff_sec = min(4.5, 1.5 * (2 ** attempt_idx) + random.uniform(0.2, 0.8))
            if time.monotonic() + backoff_sec >= deadline:
                break
            time.sleep(backoff_sec)

    if not snapshot:
        snapshot = {
            "body_text_len": 0,
            "main_text_len": 0,
            "body_text_excerpt": "",
            "parsed_text_excerpt": "",
            "meta": {},
        }

    if not reason_codes:
        classified = classify_landing(
            doi=doi,
            input_publisher=input_publisher,
            scheduler_publisher=scheduler_publisher,
            final_url=final_url or doi_url,
            title=title,
            html=html,
            snapshot=snapshot,
            issue=issue or "",
            issue_evidence=issue_evidence or [],
            exception_kind=exception_kind,
            expected_domains=expected_domains,
        )
        classifier_state = classified.get("classifier_state", STATE_UNKNOWN_NON_SUCCESS)
        reason_codes = list(classified.get("reason_codes", []) or [])
        snapshot_signal_summary = dict(classified.get("signal_summary") or {})
        reclassified_after_detector_fix = bool(classified.get("reclassified_after_detector_fix"))
        reclassification_reason = str(classified.get("reclassification_reason") or "")
    else:
        snapshot_signal_summary = {
            "body_text_len": int(snapshot.get("body_text_len", 0) or 0),
            "main_text_len": int(snapshot.get("main_text_len", 0) or 0),
            "meta_keys": sorted(k for k, v in dict(snapshot.get("meta") or {}).items() if str(v or "").strip()),
        }

    if exception_message:
        reason_codes = list(dict.fromkeys(list(reason_codes) + [exception_message]))

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    outcome = _compat_outcome_from_state(classifier_state, reason_codes)
    dom_signature = compact_text_signature(snapshot)
    resolved_chain = _dedupe_url_chain(navigation_chain)

    result = {
        "doi": doi,
        "doi_url": doi_url,
        "input_publisher": input_publisher,
        "input_title": str(record.get("input_title") or "")[:240],
        "scheduler_publisher": scheduler_publisher,
        "resolved_url": final_url or doi_url,
        "resolved_url_chain": resolved_chain,
        "navigation_chain": navigation_chain,
        "title": str(title or "")[:240],
        "worker_idx": int(worker_idx),
        "browser_identity": str(browser_identity or ""),
        "browser_session_mode": str(probe_page_meta.get("browser_session_mode") or ""),
        "browser_session_source": str(probe_page_meta.get("browser_session_source") or ""),
        "browser_session_decision_reason": str(probe_page_meta.get("browser_session_decision_reason") or ""),
        "browser_profile_name": str(probe_page_meta.get("browser_profile_name") or ""),
        "browser_user_data_dir": str(probe_page_meta.get("browser_user_data_dir") or ""),
        "probe_page_mode": str(probe_page_meta.get("probe_page_mode") or ""),
        "controller_tab_id": str(probe_page_meta.get("controller_tab_id") or ""),
        "probe_tab_id": str(probe_page_meta.get("probe_tab_id") or ""),
        "probe_page_fresh_tab": bool(probe_page_meta.get("fresh_tab")),
        "tab_transition_events": list(tab_transition_events),
        "tab_transition_count": len(tab_transition_events),
        "entry_strategy": str(entry_plan.get("entry_strategy") or ""),
        "entry_url": str(entry_plan.get("entry_url") or ""),
        "entry_resolved_url": str(entry_plan.get("entry_resolved_url") or ""),
        "entry_browser_url": str(entry_plan.get("entry_browser_url") or ""),
        "entry_browser_kind": str(entry_plan.get("entry_browser_kind") or ""),
        "entry_handoff_url": str(entry_handoff_url or entry_plan.get("entry_handoff_url") or ""),
        "entry_handoff_used": bool(entry_handoff_used),
        "entry_redirect_chain_summary": list(entry_plan.get("entry_redirect_chain_summary") or []),
        "entry_fallback_used": bool(entry_plan.get("entry_fallback_used")),
        "entry_fallback_reason": str(entry_plan.get("entry_fallback_reason") or ""),
        "entry_preflight_issue": str(entry_plan.get("entry_preflight_issue") or ""),
        "entry_preflight_evidence": list(entry_plan.get("entry_preflight_evidence") or []),
        "entry_preflight_http_status": entry_plan.get("entry_preflight_http_status"),
        "entry_browser_open_skipped": bool(entry_browser_open_skipped),
        "initial_landing_type": initial_landing_type,
        "shell_recovery_attempted": bool(shell_recovery_attempted),
        "shell_recovery_strategy": shell_recovery_strategy,
        "shell_recovery_outcome": shell_recovery_outcome,
        "scheduled_start_ms": int(timing_breakdown.get("scheduled_start_ms", 0) or 0),
        "actual_start_ms": int(timing_breakdown.get("actual_start_ms", 0) or 0),
        "pacing_wait_ms": int(timing_breakdown.get("pacing_wait_ms", 0) or 0),
        "pacing_penalty_wait_ms": int(timing_breakdown.get("pacing_penalty_wait_ms", 0) or 0),
        "pacing_jitter_sec": float(timing_breakdown.get("pacing_jitter_sec", 0.0) or 0.0),
        "classifier_state": classifier_state,
        "outcome": outcome,
        "issue": issue or "",
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "article_signal": article_signal,
        "pdf_action_signal": pdf_action_signal,
        "consent_signal": consent_signal,
        "direct_pdf_path": direct_pdf_path,
        "direct_pdf_event": dict(direct_pdf_event or {}),
        "legacy_success_like": bool(legacy_success_like),
        "reclassified_after_detector_fix": bool(reclassified_after_detector_fix),
        "reclassification_reason": reclassification_reason,
        "signal_summary": snapshot_signal_summary,
        "dom_signature": dom_signature,
        "expected_domains": list(expected_domains),
        "attempt_history": attempt_history,
        "retry_count": max(0, len(attempt_history) - 1),
        "timing_breakdown": timing_breakdown,
        "html_len": len(str(html or "")),
        "elapsed_ms": elapsed_ms,
        "timestamp_ms": _now_ms(),
        "html": html,
    }

    is_failure = outcome != OUT_SUCCESS_ACCESS
    should_capture = (
        (capture_fail_artifacts and is_failure)
        or (capture_success_artifacts and not is_failure)
    )
    artifacts = {"screenshot": "", "html": "", "meta": ""}
    if should_capture:
        artifacts = _save_probe_artifacts(
            page=artifact_page,
            record=result,
            artifact_dir=artifact_dir,
            include_html=is_failure or bool(capture_success_html),
            include_screenshot=(not is_failure) or bool(capture_fail_screenshot),
        )
    if temp_artifact_page is not None:
        _close_temporary_tab(page, temp_artifact_page)

    result["screenshot_path"] = artifacts.get("screenshot", "")
    result["html_path"] = artifacts.get("html", "")
    result["meta_path"] = artifacts.get("meta", "")
    result.pop("html", None)
    return result


def _worker_run(
    worker_idx: int,
    records: Sequence[Dict[str, Any]],
    out_jsonl: str,
    chrome_path: str,
    worker_profile_root: str,
    worker_download_root: str,
    startup_retries: int,
    timeout_sec: float,
    per_doi_deadline_sec: float,
    max_nav_attempts: int,
    progress_every: int,
    capture_fail_artifacts: bool,
    capture_fail_screenshot: bool,
    capture_success_artifacts: bool,
    capture_success_html: bool,
    artifact_dir: str,
    pacing_state,
    pacing_lock,
    publisher_cooldown_sec: float,
    global_start_spacing_sec: float,
    jitter_min_sec: float,
    jitter_max_sec: float,
    probe_page_mode: str,
) -> Dict[str, Any]:
    controller_pages: Dict[str, ChromiumPage] = {}
    done = 0
    success = 0
    worker_records: List[Dict[str, Any]] = []
    try:
        with open(out_jsonl, "w", encoding="utf-8") as f:
            for rec in records:
                session_plan = build_landing_browser_session_plan(
                    str(rec.get("doi") or ""),
                    worker_profile_root=worker_profile_root,
                    worker_idx=worker_idx,
                )
                session_cache_key = str(session_plan.get("cache_key") or "temp")
                controller_page = controller_pages.get(session_cache_key)
                if controller_page is None:
                    controller_page = _browser_for_worker(
                        chrome_path=chrome_path,
                        worker_idx=worker_idx,
                        worker_profile_root=worker_profile_root,
                        worker_download_root=worker_download_root,
                        session_plan=session_plan,
                        startup_retries=startup_retries,
                    )
                    controller_pages[session_cache_key] = controller_page
                browser_identity = str(session_plan.get("browser_identity") or f"worker_{int(worker_idx)}:{session_cache_key}")
                publisher_key = str(rec.get("scheduler_publisher") or "")
                pacing_info = reserve_pacing_slot(
                    pacing_state,
                    pacing_lock,
                    publisher_key=publisher_key,
                    cooldown_sec=publisher_cooldown_sec,
                    global_spacing_sec=global_start_spacing_sec,
                    jitter_min_sec=jitter_min_sec,
                    jitter_max_sec=jitter_max_sec,
                )
                probe_page, page_meta = _open_probe_page(controller_page, probe_page_mode=probe_page_mode)
                download_key = _sanitize_doi_to_filename(session_cache_key) or f"worker_{int(worker_idx)}"
                page_meta["worker_download_dir"] = os.path.join(os.path.abspath(worker_download_root), download_key)
                page_meta["browser_session_mode"] = str(session_plan.get("session_mode") or "")
                page_meta["browser_session_source"] = str(session_plan.get("session_source") or "")
                page_meta["browser_session_decision_reason"] = str(session_plan.get("session_decision_reason") or "")
                page_meta["browser_profile_name"] = str(session_plan.get("profile_name") or "")
                page_meta["browser_user_data_dir"] = str(session_plan.get("user_data_dir") or "")
                probe_started_ms = _now_ms()
                result: Dict[str, Any] = {}
                try:
                    result = _probe_one(
                        page=probe_page,
                        record=rec,
                        timeout_sec=timeout_sec,
                        per_doi_deadline_sec=per_doi_deadline_sec,
                        max_nav_attempts=max_nav_attempts,
                        capture_fail_artifacts=capture_fail_artifacts,
                        capture_fail_screenshot=capture_fail_screenshot,
                        capture_success_artifacts=capture_success_artifacts,
                        capture_success_html=capture_success_html,
                        artifact_dir=artifact_dir,
                        worker_idx=worker_idx,
                        browser_identity=browser_identity,
                        scheduled_start_ms=int(pacing_info.get("requested_start_ms", 0) or 0),
                        actual_start_ms=int(probe_started_ms),
                        pacing_wait_ms=int(pacing_info.get("wait_ms", 0) or 0),
                        pacing_penalty_wait_ms=int(pacing_info.get("penalty_wait_ms", 0) or 0),
                        pacing_jitter_sec=float(pacing_info.get("jitter_sec", 0.0) or 0.0),
                        probe_page_meta=page_meta,
                    )
                finally:
                    _close_probe_page(controller_page, probe_page, page_meta)
                    release_pacing_slot(
                        pacing_state,
                        pacing_lock,
                        publisher_key=publisher_key,
                        classifier_state=str((result or {}).get("classifier_state") or ""),
                        reason_codes=list((result or {}).get("reason_codes") or []),
                    )

                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                worker_records.append(result)
                done += 1
                if result.get("classifier_state") in SUCCESS_STATES:
                    success += 1
                if progress_every > 0 and (done % progress_every == 0 or done == len(records)):
                    print(
                        f"[landing_repro|worker={worker_idx}] progress {done}/{len(records)} "
                        f"(success={success}, last={result.get('classifier_state')})",
                        flush=True,
                    )
    finally:
        for controller_page in controller_pages.values():
            try:
                controller_page.quit()
            except Exception:
                pass
    return {"worker": worker_idx, "done": done, "success": success, "records": worker_records}


def _summarize(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(records)
    elapsed = sorted(int(rec.get("elapsed_ms", 0) or 0) for rec in records)

    def _pct(value: float) -> float:
        if not elapsed:
            return 0.0
        if len(elapsed) == 1:
            return float(elapsed[0])
        index = (len(elapsed) - 1) * value
        lo = int(index)
        hi = min(lo + 1, len(elapsed) - 1)
        frac = index - lo
        return float(elapsed[lo] * (1 - frac) + elapsed[hi] * frac)

    classifier_summary = summarize_classifier_states(records)
    outcome_counts = Counter(str(rec.get("outcome") or "") for rec in records)
    by_domain: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"n": 0, "elapsed": [], "states": Counter()})
    for rec in records:
        domain = str(rec.get("resolved_url") or "")
        try:
            domain = domain.split("/")[2].lower()
        except Exception:
            domain = ""
        state = str(rec.get("classifier_state") or "")
        row = by_domain[domain]
        row["n"] += 1
        row["elapsed"].append(int(rec.get("elapsed_ms", 0) or 0))
        row["states"][state] += 1

    domain_rows = []
    for domain, payload in by_domain.items():
        vals = sorted(payload["elapsed"])
        domain_rows.append(
            {
                "domain": domain,
                "sample_size": payload["n"],
                "counts": dict(payload["states"]),
                "p50_elapsed_ms": round(float(median(vals)), 1) if vals else 0.0,
                "p90_elapsed_ms": round(float(vals[int((len(vals) - 1) * 0.9)]), 1) if vals else 0.0,
            }
        )
    domain_rows.sort(key=lambda row: (-row["sample_size"], row["domain"]))

    success_count = sum(
        int(classifier_summary.get("classifier_counts", {}).get(state, 0) or 0) for state in SUCCESS_STATES
    )
    return {
        "total_valid": total,
        "success_ratio": round(success_count / total, 4) if total else 0.0,
        "classifier_counts": classifier_summary.get("classifier_counts", {}),
        "outcome_counts": dict(outcome_counts),
        "top_reason_codes": classifier_summary.get("top_reason_codes", []),
        "publisher_breakdown": classifier_summary.get("publisher_breakdown", []),
        "legacy_success_like_count": classifier_summary.get("legacy_success_like_count", 0),
        "legacy_reclassified_non_success": classifier_summary.get("legacy_reclassified_non_success", []),
        "p50_elapsed_ms": round(_pct(0.5), 1),
        "p90_elapsed_ms": round(_pct(0.9), 1),
        "domain_breakdown_top20": domain_rows[:20],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Landing-only DOI access probe with explicit landing classification")
    parser.add_argument("--input", type=str, default="ready_to_download.csv")
    parser.add_argument("--max-dois", type=int, default=0)
    parser.add_argument("--workers", type=int, default=DEFAULT_LOCAL_LANDING_WORKERS)
    parser.add_argument("--startup-retries", type=int, default=3)
    parser.add_argument("--timeout-sec", type=float, default=DEFAULT_LOCAL_TIMEOUT_SEC)
    parser.add_argument("--per-doi-deadline-sec", type=float, default=DEFAULT_LOCAL_PER_DOI_DEADLINE_SEC)
    parser.add_argument("--max-nav-attempts", type=int, default=DEFAULT_MAX_NAV_ATTEMPTS)
    parser.add_argument("--publisher-cooldown-sec", type=float, default=DEFAULT_PER_PUBLISHER_COOLDOWN_SEC)
    parser.add_argument("--global-start-spacing-sec", type=float, default=DEFAULT_GLOBAL_START_SPACING_SEC)
    parser.add_argument("--jitter-min-sec", type=float, default=DEFAULT_JITTER_MIN_SEC)
    parser.add_argument("--jitter-max-sec", type=float, default=DEFAULT_JITTER_MAX_SEC)
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--chrome-path", type=str, default=os.environ.get("CHROME_PATH", ""))
    parser.add_argument("--headless", type=int, default=DEFAULT_LOCAL_HEADLESS, choices=[0, 1])
    parser.add_argument(
        "--runtime-preset",
        type=str,
        default=os.environ.get("PDF_BROWSER_RUNTIME_PRESET", "auto"),
        choices=["auto", "local_mac", "linux_cli_seeded"],
    )
    parser.add_argument(
        "--execution-env",
        type=str,
        default=os.environ.get("PDF_BROWSER_EXECUTION_ENV", "auto"),
        choices=["auto", "desktop", "linux_server"],
    )
    parser.add_argument("--humanized-browser", type=int, default=1, choices=[0, 1])
    parser.add_argument("--assume-institution-access", type=int, default=1, choices=[0, 1])
    parser.add_argument("--profile-mode", type=str, default=os.environ.get("PDF_BROWSER_PROFILE_MODE", "auto"))
    parser.add_argument("--profile-name", type=str, default=os.environ.get("PDF_BROWSER_PROFILE_NAME", "Default"))
    parser.add_argument(
        "--persistent-profile-dir",
        type=str,
        default=os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", "outputs/.chrome_user_data"),
    )
    parser.add_argument("--worker-profile-root", type=str, default="")
    parser.add_argument("--clean-worker-profiles", type=int, default=1, choices=[0, 1])
    parser.add_argument("--capture-fail-artifacts", type=int, default=1, choices=[0, 1])
    parser.add_argument("--capture-fail-screenshot", type=int, default=0, choices=[0, 1])
    parser.add_argument("--capture-success-artifacts", type=int, default=1, choices=[0, 1])
    parser.add_argument("--capture-success-html", type=int, default=0, choices=[0, 1])
    parser.add_argument("--artifact-dir", type=str, default="outputs/landing_access_artifacts")
    parser.add_argument("--zip-fail-artifacts", type=int, default=1, choices=[0, 1])
    parser.add_argument("--artifact-zip", type=str, default="outputs/landing_access_failures.zip")
    parser.add_argument("--zip-success-artifacts", type=int, default=1, choices=[0, 1])
    parser.add_argument("--success-artifact-zip", type=str, default="outputs/landing_access_successes.zip")
    parser.add_argument("--probe-page-mode", type=str, default=PROBE_PAGE_MODE_REUSE, choices=[PROBE_PAGE_MODE_REUSE, PROBE_PAGE_MODE_FRESH_TAB])
    parser.add_argument("--output-jsonl", type=str, default="outputs/landing_access_repro.jsonl")
    parser.add_argument("--report", type=str, default="outputs/landing_access_repro_report.json")
    parser.add_argument("--report-md", type=str, default="")
    args = parser.parse_args()

    records = load_landing_inputs(args.input)
    if args.max_dois and args.max_dois > 0:
        records = records[: args.max_dois]
    if not records:
        raise RuntimeError("No valid DOI found.")
    ordered_records = reorder_inputs_for_pacing(records)

    requested_workers = max(1, int(args.workers))
    workers = min(requested_workers, SAFE_LANDING_MAX_WORKERS, len(ordered_records))
    chunks = chunk_inputs_round_robin(ordered_records, workers)

    resolved_runtime_preset = resolve_runtime_preset(args.runtime_preset)
    os.environ["PDF_BROWSER_RUNTIME_PRESET"] = resolved_runtime_preset
    resolved_execution_env = resolve_browser_execution_env(args.execution_env)
    requested_headless = bool(int(args.headless))
    resolved_headless = coerce_headless_for_execution_env(
        requested_headless,
        resolved_execution_env,
        context="landing_precheck",
    )
    os.environ["PDF_BROWSER_EXECUTION_ENV"] = resolved_execution_env
    os.environ["PDF_BROWSER_HEADLESS"] = "1" if resolved_headless else "0"
    os.environ["PDF_BROWSER_HUMANIZED"] = "1" if int(args.humanized_browser) == 1 else "0"
    os.environ["PDF_ASSUME_INSTITUTION_ACCESS"] = "1" if int(args.assume_institution_access) == 1 else "0"
    resolved_profile_mode = str(args.profile_mode or "auto").strip() or "auto"
    resolved_profile_name = str(args.profile_name or "Default").strip() or "Default"
    resolved_persistent_profile_dir = os.path.abspath(str(args.persistent_profile_dir))
    os.environ["PDF_BROWSER_PROFILE_MODE"] = resolved_profile_mode
    os.environ["PDF_BROWSER_PROFILE_NAME"] = resolved_profile_name
    os.environ["PDF_BROWSER_PERSISTENT_PROFILE_DIR"] = resolved_persistent_profile_dir
    profile_inspection = ensure_runtime_profile_ready(
        runtime_preset=resolved_runtime_preset,
        profile_mode=resolved_profile_mode,
        persistent_profile_dir=resolved_persistent_profile_dir,
        profile_name=resolved_profile_name,
    )

    worker_profile_root = str(args.worker_profile_root or "").strip()
    run_base = os.path.join("/tmp", os.environ.get("USER", "user"))
    if not worker_profile_root:
        worker_profile_root = os.path.join(run_base, "landing_worker_profiles")
    worker_profile_root = os.path.abspath(worker_profile_root)
    if int(args.clean_worker_profiles) == 1 and os.path.isdir(worker_profile_root):
        shutil.rmtree(worker_profile_root, ignore_errors=True)
    os.makedirs(worker_profile_root, exist_ok=True)
    worker_download_root = os.path.abspath(os.path.join(run_base, "landing_worker_downloads"))
    os.makedirs(worker_download_root, exist_ok=True)

    os.makedirs(os.path.dirname(args.output_jsonl) or ".", exist_ok=True)
    os.makedirs(args.artifact_dir, exist_ok=True)
    worker_files = [f"{args.output_jsonl}.worker{i}.jsonl" for i in range(workers)]
    for path in worker_files:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    chrome_path = _resolve_browser_path(str(args.chrome_path or "").strip())
    if not chrome_path:
        raise RuntimeError(
            "Chrome/Chromium executable not found. "
            "Set --chrome-path or CHROME_PATH. "
            "Tried PATH plus common Linux/macOS app paths."
        )

    print(json.dumps({"resolved_chrome_path": chrome_path}, ensure_ascii=False), flush=True)
    print(json.dumps({"runtime_preset": resolved_runtime_preset}, ensure_ascii=False), flush=True)
    print(json.dumps({"execution_env": resolved_execution_env}, ensure_ascii=False), flush=True)
    print(json.dumps({"worker_profile_root": worker_profile_root}, ensure_ascii=False), flush=True)
    print(json.dumps({"worker_download_root": worker_download_root}, ensure_ascii=False), flush=True)
    if profile_inspection.get("checked"):
        print(
            json.dumps(
                {
                    "seed_profile_root": profile_inspection.get("profile_root", ""),
                    "seed_profile_ok": bool(profile_inspection.get("ok")),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    if workers != requested_workers:
        print(
            json.dumps(
                {
                    "workers_requested": requested_workers,
                    "workers_effective": workers,
                    "reason": f"capped_for_safe_local_landing_max={SAFE_LANDING_MAX_WORKERS}",
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    smoke_normal = _run_chrome_smoke(chrome_path=chrome_path, profile_root=worker_profile_root)
    if smoke_normal.get("ok") != "1":
        artifact_smoke = os.path.abspath(os.path.join(args.artifact_dir, "chrome_smoke_fail.json"))
        with open(artifact_smoke, "w", encoding="utf-8") as f:
            json.dump(
                {"normal": smoke_normal, "chrome_path": chrome_path},
                f,
                ensure_ascii=False,
                indent=2,
            )
        raise RuntimeError(f"chrome_smoke_failed: {artifact_smoke}")
    else:
        print(json.dumps({"chrome_smoke": "ok"}, ensure_ascii=False), flush=True)

    all_records: List[Dict[str, Any]] = []
    with Manager() as manager:
        pacing_state = manager.dict()
        pacing_lock = manager.Lock()
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futures = []
            for idx, chunk in enumerate(chunks):
                futures.append(
                    ex.submit(
                        _worker_run,
                        idx,
                        chunk,
                        worker_files[idx],
                        chrome_path,
                        worker_profile_root,
                        worker_download_root,
                        int(args.startup_retries),
                        float(args.timeout_sec),
                        float(args.per_doi_deadline_sec),
                        int(args.max_nav_attempts),
                        int(args.progress_every),
                        bool(int(args.capture_fail_artifacts)),
                        bool(int(args.capture_fail_screenshot)),
                        bool(int(args.capture_success_artifacts)),
                        bool(int(args.capture_success_html)),
                        os.path.abspath(args.artifact_dir),
                        pacing_state,
                        pacing_lock,
                        float(args.publisher_cooldown_sec),
                        float(args.global_start_spacing_sec),
                        float(args.jitter_min_sec),
                        float(args.jitter_max_sec),
                        str(args.probe_page_mode or PROBE_PAGE_MODE_REUSE),
                    )
                )
            for future in as_completed(futures):
                result = future.result()
                all_records.extend(result["records"])
                print(
                    f"[landing_repro|worker={result['worker']}] done {result['done']} success={result['success']}",
                    flush=True,
                )

    with open(args.output_jsonl, "w", encoding="utf-8") as out_f:
        for path in worker_files:
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    out_f.write(line)

    summary = _summarize(all_records)
    remaining_weak_spots = suggest_remaining_weak_spots(summary)
    failure_artifact_zip = ""
    success_artifact_zip = ""
    if int(args.capture_fail_artifacts) == 1 and int(args.zip_fail_artifacts) == 1:
        failure_artifact_zip = _build_artifact_zip(
            records=all_records,
            artifact_dir=args.artifact_dir,
            zip_path=args.artifact_zip,
            target="fail",
        )
    if int(args.capture_success_artifacts) == 1 and int(args.zip_success_artifacts) == 1:
        success_artifact_zip = _build_artifact_zip(
            records=all_records,
            artifact_dir=args.artifact_dir,
            zip_path=args.success_artifact_zip,
            target="success",
        )

    report = {
        "generated_at": int(time.time()),
        "input_csv": os.path.abspath(args.input),
        "sample_size": len(ordered_records),
        "workers_requested": requested_workers,
        "workers_effective": workers,
        "runtime_preset": resolved_runtime_preset,
        "execution_env": resolved_execution_env,
        "headless": bool(resolved_headless),
        "headless_requested": bool(requested_headless),
        "humanized_browser": os.environ.get("PDF_BROWSER_HUMANIZED", "1"),
        "assume_institution_access": bool(int(args.assume_institution_access)),
        "startup_retries": int(args.startup_retries),
        "timeout_sec": float(args.timeout_sec),
        "per_doi_deadline_sec": float(args.per_doi_deadline_sec),
        "max_nav_attempts": int(args.max_nav_attempts),
        "publisher_cooldown_sec": float(args.publisher_cooldown_sec),
        "global_start_spacing_sec": float(args.global_start_spacing_sec),
        "jitter_sec": [float(args.jitter_min_sec), float(args.jitter_max_sec)],
        "profile_mode": os.environ.get("PDF_BROWSER_PROFILE_MODE", ""),
        "profile_name": os.environ.get("PDF_BROWSER_PROFILE_NAME", ""),
        "persistent_profile_dir": os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", ""),
        "seed_profile_checked": bool(profile_inspection.get("checked")),
        "seed_profile_ok": bool(profile_inspection.get("ok")),
        "worker_profile_root": worker_profile_root,
        "probe_page_mode": str(args.probe_page_mode or PROBE_PAGE_MODE_REUSE),
        "capture_fail_artifacts": bool(int(args.capture_fail_artifacts)),
        "capture_fail_screenshot": bool(int(args.capture_fail_screenshot)),
        "capture_success_artifacts": bool(int(args.capture_success_artifacts)),
        "capture_success_html": bool(int(args.capture_success_html)),
        "criteria": (
            "success_landing only when expected publisher/target is reached, visible content is populated, "
            "article metadata or stable article markers exist, and no challenge/interstitial/blank heuristics fire."
        ),
        "summary": summary,
        "publishers_covered": sorted(
            {
                str(rec.get("input_publisher") or rec.get("scheduler_publisher") or "").strip()
                for rec in ordered_records
                if str(rec.get("input_publisher") or rec.get("scheduler_publisher") or "").strip()
            }
        ),
        "remaining_weak_spots": remaining_weak_spots,
        "output_jsonl": os.path.abspath(args.output_jsonl),
        "artifact_dir": os.path.abspath(args.artifact_dir),
        "failure_artifact_zip": failure_artifact_zip,
        "success_artifact_zip": success_artifact_zip,
    }

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    report_md = str(args.report_md or "").strip()
    if report_md:
        os.makedirs(os.path.dirname(report_md) or ".", exist_ok=True)
        with open(report_md, "w", encoding="utf-8") as f:
            f.write(render_experiment_markdown(report))

    print(
        json.dumps(
            {
                "sample_size": len(ordered_records),
                "workers_effective": workers,
                "classifier_counts": summary.get("classifier_counts", {}),
                "legacy_reclassified_non_success": len(summary.get("legacy_reclassified_non_success", [])),
                "report": os.path.abspath(args.report),
                "report_md": os.path.abspath(report_md) if report_md else "",
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
