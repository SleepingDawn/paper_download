import os
import re
import time
import shutil
import logging
import requests
import base64
import random
import json

from typing import Set
from urllib.parse import urljoin, quote
from seleniumbase import Driver
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests # 이름 충돌 방지
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Keys
from config import WILEY_API_KEY
from pdf_pipeline import (
    REASON_FAIL_HTTP_STATUS,
    REASON_FAIL_REDIRECT_LOOP,
    REASON_FAIL_TIMEOUT_NETWORK,
    append_metrics_jsonl,
    download_pdf,
)
# from CloudflareBypasser import CloudflareBypasser

DEFAULT_DOWNLOAD_PATH = os.path.abspath("./downloaded_files")

# Browser profile tuned to look closer to a normal desktop session.
# NOTE:
# - --lang must be locale list without q-values.
# - HTTP Accept-Language can contain q-values.
BEST_BROWSER_LANG = "en-US"
BEST_BROWSER_ACCEPT_LANGUAGE = "en-US,en;q=0.9"
BEST_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
BEST_BROWSER_WINDOW = "1728,1117"
MAX_ACTION_WAIT_S = int(os.getenv("PDF_ACTION_MAX_WAIT_S", "60"))
HIGH_FRICTION_DOMAINS = (
    "acs.org",
    "sciencedirect.com",
    "aip.org",
    "wiley.com",
    "rsc.org",
    "mdpi.com",
    "tandfonline.com",
    "ieeexplore.ieee.org",
)


def _apply_best_browser_profile(co: ChromiumOptions) -> None:
    headless = os.getenv("PDF_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes")
    no_sandbox = os.getenv("PDF_BROWSER_NO_SANDBOX", "0").strip().lower() in ("1", "true", "yes")

    if headless:
        co.set_argument("--headless=new")
        co.set_argument("--disable-gpu")
    co.no_imgs(False)
    co.mute(False)
    co.set_argument("--disable-blink-features=AutomationControlled")
    co.set_argument(f"--window-size={BEST_BROWSER_WINDOW}")
    co.set_argument(f"--lang={BEST_BROWSER_LANG}")
    co.set_pref("intl.accept_languages", BEST_BROWSER_LANG)
    co.set_argument("--start-maximized")
    try:
        co.set_load_mode("eager")
    except Exception:
        pass
    if no_sandbox:
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-dev-shm-usage")
    co.set_user_agent(BEST_BROWSER_UA)
# =======================================================
# Logger
# =======================================================
def setup_logger(save_dir: str, filename: str) -> logging.Logger:
    filename = _sanitize_doi_to_filename(filename) if filename else "unknown"
    log_dir = os.path.join(save_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"download_log_{filename}.txt"

    logger = logging.getLogger("Paper_PDF_Downloader")
    logger.setLevel(logging.DEBUG)
    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(os.path.join(log_dir, log_filename), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(">> %(message)s"))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

# =======================================================
# Utilities
# =======================================================
def _sanitize_doi_to_filename(doi_url: str) -> str:
    clean = doi_url.strip().replace("https://doi.org/", "").replace("http://doi.org/", "")
    return clean.strip("/").replace("/", "_").replace(":", "-") + ".pdf"

def _get_current_files(download_dir: str) -> Set[str]:
    if not os.path.exists(download_dir): return set()
    return set(os.listdir(download_dir))


def _eles_quick(page, locator: str, timeout: float = 0.6):
    try:
        return page.eles(locator, timeout=timeout)
    except TypeError:
        try:
            return page.eles(locator)
        except Exception:
            return []
    except Exception:
        return []


def _ele_quick(page, locator: str, timeout: float = 0.6):
    try:
        return page.ele(locator, timeout=timeout)
    except TypeError:
        try:
            return page.ele(locator)
        except Exception:
            return None
    except Exception:
        return None


def _is_valid_pdf(file_path: str) -> bool:
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 1000:
            return False
        with open(file_path, 'rb') as f:
            header = f.read(4)
            return header.startswith(b'%PDF')
    except: return False

def _wait_for_new_file_diff(download_dir: str, initial_files: Set[str], timeout_s: int = 30, logger = None):
    timeout_s = max(1, min(int(timeout_s), MAX_ACTION_WAIT_S))
    if logger:
        logger.info(f"     파일 감지 및 유효성 검사 (최대 {timeout_s}초)...")
    t0 = time.time()
    while (time.time() - t0) < timeout_s:
        try:
            current_files = _get_current_files(download_dir)
            new_items = current_files - initial_files
            if not new_items:
                time.sleep(0.25)
                continue
            
            valid_pdfs = [f for f in new_items if f.lower().endswith(".pdf")]
            for pdf in valid_pdfs:
                full_path = os.path.join(download_dir, pdf)
                if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                    prev_size = -1
                    stable_count = 0
                    for _ in range(3):
                        curr = os.path.getsize(full_path)
                        if curr == prev_size: stable_count += 1
                        else: stable_count = 0
                        prev_size = curr
                        if stable_count >= 2:
                            if _is_valid_pdf(full_path):
                                if logger:
                                    logger.info(f"        정상 PDF 확인 완료 (크기: {curr} bytes): {pdf}")
                                return full_path
                            else:
                                pass
                        time.sleep(0.2)
            time.sleep(0.25)
        except Exception:
            time.sleep(0.25)
    if logger:
        logger.info("       파일 감지 타임아웃")
    return None

def _safe_screenshot(page, path: str, name: str, logger=None):
    """
    DrissionPage의 get_screenshot 메서드를 사용하여 스크린샷을 저장합니다.
    path: 저장할 폴더 경로 (예: ./logs/screenshots)
    name: 파일명 (예: capture.png)
    """
    try:
        # 폴더 생성
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        
        #  DrissionPage get_screenshot 호출
        saved_path = page.get_screenshot(path=path, name=name, full_page=True)
        
        if logger: 
            logger.info(f"  스크린샷 저장 성공: {saved_path}")

    except Exception as e:
        # 전체 페이지 캡처 실패 시 (메모리 부족, 무한 스크롤 등), 보이는 화면(Viewport)만 재시도
        try:
            if logger: 
                logger.warning(f"  전체 페이지 스크린샷 실패 ({e}), 보이는 화면만 캡처 시도...")
            
            # 파일명에 visible_ 접두사를 붙여 재시도
            retry_name = "visible_" + name
            page.get_screenshot(path=path, name=retry_name, full_page=False)
            
        except Exception as e2:
            # 재시도마저 실패한 경우
            pass
            # if logger: logger.warning(f"  스크린샷 저장 최종 실패 : {e2}")


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _close_page_safely(page, logger=None):
    if page is None:
        return
    try:
        page.quit()
    except Exception as e:
        if logger:
            logger.warning(f"     [Drission] 브라우저 종료 실패(무시): {e}")


def _is_high_friction_domain(url_or_domain: str) -> bool:
    d = _extract_domain(url_or_domain) if "://" in str(url_or_domain) else str(url_or_domain or "").lower()
    return any(key in d for key in HIGH_FRICTION_DOMAINS)


def _finalize_downloaded_file(downloaded_path: str, target_path: str, logger=None) -> bool:
    if not downloaded_path or not os.path.exists(downloaded_path):
        return False
    if not _is_valid_pdf(downloaded_path):
        if logger:
            logger.warning(f"        유효하지 않은 PDF(스킵): {downloaded_path}")
        return False

    try:
        if os.path.abspath(downloaded_path) != os.path.abspath(target_path):
            if os.path.exists(target_path):
                os.remove(target_path)
            shutil.move(downloaded_path, target_path)
        return _is_valid_pdf(target_path)
    except Exception as e:
        if logger:
            logger.warning(f"        파일 정리 실패: {e}")
        return False


def _looks_like_pdf_link(url: str) -> bool:
    low = str(url or "").lower()
    if not low:
        return False
    good_tokens = (".pdf", "/doi/pdf", "/articlepdf", "/pdfft", "download=true", "article-pdf")
    bad_tokens = ("/proceedings", "/session", "/program", "/toc", "/contents")
    if any(b in low for b in bad_tokens):
        return False
    return any(g in low for g in good_tokens)


def _try_click_pdf_button_download(page, pdf_btn, save_dir: str, full_save_path: str, logger=None, wait_timeout_s: int = 25) -> bool:
    if page is None or pdf_btn is None:
        return False
    try:
        initial_files = _get_current_files(save_dir)
        try:
            pdf_btn.click(by_js=True)
        except Exception:
            pdf_btn.click()

        downloaded_path = _wait_for_new_file_diff(save_dir, initial_files, timeout_s=wait_timeout_s, logger=logger)
        if not downloaded_path:
            return False

        if _finalize_downloaded_file(downloaded_path, full_save_path, logger=logger):
            if logger:
                logger.info("        [Drission] 버튼 클릭 기반 다운로드 성공")
            return True
    except Exception as e:
        if logger:
            logger.warning(f"        버튼 클릭 기반 다운로드 실패: {e}")
    return False


def _doi_from_doi_url(doi_url: str) -> str:
    raw = str(doi_url or "").strip()
    if "doi.org/" in raw:
        raw = raw.split("doi.org/", 1)[1]
    raw = raw.split("?", 1)[0].split("#", 1)[0].strip().lower()
    return raw


def _extract_sciencedirect_pii_from_url(url: str) -> str:
    raw = str(url or "")
    m = re.search(r"/pii/([A-Z0-9]+)", raw, flags=re.IGNORECASE)
    return (m.group(1) if m else "").upper()


def _extract_sciencedirect_pii_from_text(text: str) -> str:
    raw = str(text or "")
    m = re.search(r"1-s2\.0-([A-Z0-9]+)-main\.pdf", raw, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"/pii/([A-Z0-9]+)", raw, flags=re.IGNORECASE)
    return (m.group(1) if m else "").upper()


def _extract_meta_content(page, meta_name: str) -> str:
    if page is None:
        return ""
    try:
        el = _ele_quick(page, f'css:meta[name="{meta_name}"]', timeout=0.5)
        if el:
            return str(el.attr("content") or "").strip()
    except Exception:
        pass
    return ""


def _extract_elsevier_target_pii(page) -> str:
    if page is None:
        return ""
    for candidate in (
        _extract_sciencedirect_pii_from_url(getattr(page, "url", "") or ""),
        _extract_sciencedirect_pii_from_text(_extract_meta_content(page, "citation_pdf_url")),
        _extract_sciencedirect_pii_from_text(_extract_meta_content(page, "citation_abstract_html_url")),
    ):
        if candidate:
            return candidate
    return ""


def _is_elsevier_target_page(page, target_doi: str, target_pii: str) -> bool:
    if page is None:
        return False
    current_url = str(getattr(page, "url", "") or "").lower()
    doi_norm = str(target_doi or "").strip().lower()
    pii_norm = str(target_pii or "").strip().upper()

    if pii_norm and f"/pii/{pii_norm.lower()}" in current_url:
        return True

    citation_doi = _extract_meta_content(page, "citation_doi").lower()
    if doi_norm and citation_doi and citation_doi == doi_norm:
        return True

    return False


def _is_recommended_or_related_blob(blob: str) -> bool:
    low = str(blob or "").lower()
    bad_tokens = (
        "recommended",
        "related",
        "suggested",
        "similar article",
        "you may also like",
        "more like this",
    )
    return any(tok in low for tok in bad_tokens)


def _select_best_clickable_pdf_element(page, xpaths, logger=None, must_tokens=None, ban_tokens=None):
    candidates = []
    must_tokens = [str(t).lower() for t in (must_tokens or []) if str(t).strip()]
    ban_tokens = [str(t).lower() for t in (ban_tokens or []) if str(t).strip()]
    for xp in xpaths:
        for el in _eles_quick(page, f"xpath:{xp}", timeout=0.5):
            try:
                if not el.states.is_displayed:
                    continue
            except Exception:
                pass
            text = (el.text or "").strip()
            title = (el.attr("title") or "").strip()
            aria = (el.attr("aria-label") or "").strip()
            href = (el.attr("href") or "").strip()
            blob = f"{text} {title} {aria} {href}".lower()
            if any(
                k in blob
                for k in (
                    "figure",
                    "supplement",
                    "dataset",
                    "graphical abstract",
                    "citation",
                    "export",
                    "powerpoint",
                    "ms-power",
                    "ppt",
                    "bibtex",
                )
            ):
                continue
            if _is_recommended_or_related_blob(blob):
                continue
            if ban_tokens and any(tok in blob for tok in ban_tokens):
                continue
            if not any(k in blob for k in ("pdf", ".pdf", "/pdfft", "articlepdf", "open pdf", "view pdf")):
                continue
            if must_tokens and not any(tok in blob for tok in must_tokens):
                continue
            score = 0
            for k in ("open pdf", "view pdf", "download pdf", "/pdfft", ".pdf", "articlepdf", "pdf"):
                if k in blob:
                    score += 2
            candidates.append((score, el, blob))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    if logger:
        logger.info(f"        [ClickSelect] 후보 {len(candidates)}개 중 최고점={candidates[0][0]}")
    return candidates[0][1]


def _click_once_wait_file(
    page,
    el,
    tmp_dir: str,
    tmp_path: str,
    wait_s: int,
    logger=None,
    post_click_guard=None,
    downloaded_file_guard=None,
) -> bool:
    if page is None or el is None:
        return False
    try:
        initial_files = _get_current_files(tmp_dir)
        try:
            el.click()
        except Exception:
            el.click(by_js=True)
        if post_click_guard:
            try:
                time.sleep(0.8)
                if not post_click_guard():
                    if logger:
                        logger.info("        [ClickGuard] 대상 논문 컨텍스트 불일치로 클릭 결과 무시")
                    return False
            except Exception:
                return False
        downloaded = _wait_for_new_file_diff(tmp_dir, initial_files, timeout_s=wait_s, logger=logger)
        if downloaded and downloaded_file_guard:
            try:
                if not downloaded_file_guard(downloaded):
                    try:
                        os.remove(downloaded)
                    except Exception:
                        pass
                    if logger:
                        logger.info("        [ClickGuard] 다운로드 파일이 대상 논문과 불일치하여 폐기")
                    return False
            except Exception:
                return False
        if downloaded and _finalize_downloaded_file(downloaded, tmp_path, logger=logger):
            return True
    except Exception as e:
        if logger:
            logger.warning(f"        클릭-대기 실패: {e}")
    return False


def _attempt_elsevier_two_step_click_download(page, doi: str, tmp_dir: str, tmp_path: str, logger=None) -> bool:
    if page is None:
        return False
    _dismiss_cookie_or_consent_banner(page, logger=logger)
    doi_norm = str(doi or "").strip().lower()
    target_pii = _extract_elsevier_target_pii(page)
    if logger:
        logger.info(f"        [Elsevier] target_doi={doi_norm}, target_pii={target_pii or 'N/A'}")

    if not target_pii and not doi_norm:
        if logger:
            logger.info("        [Elsevier] 타겟 식별자 부족으로 클릭 플로우 스킵")
        return False

    def _context_guard() -> bool:
        return _is_elsevier_target_page(page, doi_norm, target_pii)

    def _file_guard(downloaded_path: str) -> bool:
        if not target_pii:
            return True
        found_pii = _extract_sciencedirect_pii_from_text(os.path.basename(str(downloaded_path or "")))
        if not found_pii:
            return True
        return found_pii == target_pii

    token_candidates = []
    if target_pii:
        token_candidates.append(f"/pii/{target_pii.lower()}")
        token_candidates.append(target_pii.lower())

    # 1) Article page에서 Open/View/Download PDF 클릭
    article_xpaths = [
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
        "//a[contains(@href,'/pdfft') or contains(@href,'.pdf')]",
    ]
    step1 = _select_best_clickable_pdf_element(
        page,
        article_xpaths,
        logger=logger,
        must_tokens=token_candidates,
    )
    if not step1:
        step1 = _select_best_clickable_pdf_element(page, article_xpaths, logger=logger)
    if step1 and _click_once_wait_file(
        page,
        step1,
        tmp_dir,
        tmp_path,
        wait_s=6,
        logger=logger,
        post_click_guard=_context_guard,
        downloaded_file_guard=_file_guard,
    ):
        if logger:
            logger.info(f"        [Elsevier] 1단계 클릭으로 다운로드 성공: {doi}")
        return True

    # 2) viewer/pdfft 상태라면 Download 버튼 한 번 더 클릭
    current_url = str(page.url or "").lower()
    if "/pdfft" in current_url or ".pdf" in current_url or "sciencedirect.com" in current_url:
        viewer_xpaths = [
            "//button[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
            "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            "//a[contains(@download,'.pdf') or contains(@href,'.pdf')]",
        ]
        step2 = _select_best_clickable_pdf_element(page, viewer_xpaths, logger=logger, must_tokens=token_candidates)
        if not step2:
            step2 = _select_best_clickable_pdf_element(page, viewer_xpaths, logger=logger)
        if step2 and _click_once_wait_file(
            page,
            step2,
            tmp_dir,
            tmp_path,
            wait_s=6,
            logger=logger,
            post_click_guard=_context_guard,
            downloaded_file_guard=_file_guard,
        ):
            if logger:
                logger.info(f"        [Elsevier] 2단계 클릭으로 다운로드 성공: {doi}")
            return True
    return False


def _has_article_signal(title: str = "", html: str = "") -> bool:
    t = (title or "").lower()
    h = (html or "").lower()
    markers = (
        "name=\"citation_title\"",
        "name='citation_title'",
        "name=\"citation_doi\"",
        "name='citation_doi'",
        "name=\"citation_pdf_url\"",
        "/doi/pdf/",
        "/pdfft?",
        "article",
        "abstract",
        "references",
    )
    if len(t) >= 35 and not any(k in t for k in ("just a moment", "attention required", "validate user")):
        return True
    return any(m in h for m in markers)


def _has_pdf_action_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    markers = (
        "view pdf",
        "open pdf",
        "download pdf",
        "open",
        "/pdfft",
        "citation_pdf_url",
        "article-pdf",
    )
    return any(m in blob for m in markers)


def _has_cookie_or_consent_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    markers = (
        "cookie",
        "consent",
        "manage preferences",
        "managepreferences",
        "accept all",
        "acceptall",
        "reject non-essential",
        "reject all",
        "privacy policy",
        "onetrust",
        "continue with only essential cookies",
    )
    return any(m in blob for m in markers)


def _has_auth_required_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    strong_markers = (
        "password required",
        "password to view",
        "please enter your password",
        "enter password",
        "password protected",
        "authenticated access only",
    )
    if any(m in blob for m in strong_markers):
        return True
    # 단독 authenticate 텍스트(스크립트/메타) 오탐 방지: password 문맥 동반 시에만 차단
    return ("authenticate" in blob or "authentication required" in blob) and ("password" in blob)


def _dismiss_cookie_or_consent_banner(page, logger=None) -> bool:
    if page is None:
        return False
    xpaths = [
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'acceptall')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'i agree')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'agree')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'acceptall')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'reject non-essential')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'reject all')]",
        "//button[contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'onetrust-accept')]",
    ]
    for xp in xpaths:
        elems = _eles_quick(page, f"xpath:{xp}", timeout=0.5)
        for el in elems:
            try:
                if not el.states.is_displayed:
                    continue
            except Exception:
                pass
            try:
                el.click(by_js=True)
            except Exception:
                try:
                    el.click()
                except Exception:
                    continue
            if logger:
                logger.info("        [ConsentGate] 쿠키/동의 배너 클릭 처리")
            time.sleep(0.8)
            return True
    return False


def _click_viewer_open_button(page, logger=None) -> bool:
    if page is None:
        return False
    xpaths = [
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
    ]
    for xp in xpaths:
        elems = _eles_quick(page, f"xpath:{xp}", timeout=0.5)
        for el in elems:
            try:
                if not el.states.is_displayed:
                    continue
            except Exception:
                pass
            text = (el.text or "").strip().lower()
            title = (el.attr("title") or "").strip().lower()
            aria = (el.attr("aria-label") or "").strip().lower()
            blob = f"{text} {title} {aria}"
            if any(k in blob for k in ("figure", "supplement", "dataset", "powerpoint", "citation", "export")):
                continue
            try:
                try:
                    el.click(by_js=True)
                except Exception:
                    el.click()
                if logger:
                    logger.info("        [ViewerGate] Open/View 버튼 클릭 시도")
                time.sleep(1.0)
                return True
            except Exception:
                continue
    return False


def _should_soft_continue_issue(
    issue: str,
    evidence: list,
    title: str,
    html: str,
    domain: str,
) -> bool:
    if issue not in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
        return False
    if _has_auth_required_signal(title=title, html=html):
        return False
    if not _is_high_friction_domain(domain):
        return False

    if _has_cookie_or_consent_signal(title=title, html=html) and (
        _has_article_signal(title=title, html=html) or _has_pdf_action_signal(title=title, html=html)
    ):
        return True

    t = (title or "").lower()
    hard_title_markers = (
        "just a moment",
        "attention required",
        "validate user",
        "verify you are human",
        "are you a robot",
        "access denied",
    )
    if any(k in t for k in hard_title_markers):
        return False

    ev = [str(x).lower() for x in (evidence or [])]
    soft_markers = ("keyword=too many requests", "keyword=/cdn-cgi/challenge")
    if any(m in ev_item for ev_item in ev for m in soft_markers):
        return _has_article_signal(title=title, html=html) or _has_pdf_action_signal(title=title, html=html)

    return False


def detect_access_issue(title: str = "", html: str = "", http_status: int = None):
    """
    캡차/차단 신호를 감지해 (reason, evidence)를 반환.
    reason: FAIL_CAPTCHA | FAIL_BLOCK | None
    """
    t = (title or "").lower()
    h = (html or "").lower()
    evidence = []
    article_like = _has_article_signal(title=title, html=html)
    pdf_action_like = _has_pdf_action_signal(title=title, html=html)
    consent_like = _has_cookie_or_consent_signal(title=title, html=html)
    auth_required_like = _has_auth_required_signal(title=title, html=html)

    if auth_required_like:
        evidence.append("keyword=auth_required")
        return "FAIL_BLOCK", evidence

    # Cookie/consent overlay가 뜬 정상 페이지를 차단으로 오판하지 않도록 우선 예외처리
    if consent_like and (article_like or pdf_action_like):
        evidence.append("soft=consent_gate_detected")
        return None, evidence

    # Avoid false positives from normal pages that include analytics/captcha-related assets.
    title_captcha_keywords = [
        "just a moment",
        "잠시만",
        "verify you are human",
        "are you human",
        "are you a robot",
        "i am not a robot",
        "validate user",
    ]
    title_block_keywords = [
        "attention required",
        "access denied",
        "forbidden",
        "request blocked",
        "too many requests",
        "security check",
    ]
    html_captcha_markers = [
        "cf-turnstile",
        "challenge-form",
        "captcha-box",
        "validate user",
        "verify you are human",
        "are you a robot",
    ]
    html_block_markers = [
        "error code 1020",
        "request blocked",
        "too many requests",
        "access denied",
        "forbidden",
        "cloudflare_error_1000",
        "/cdn-cgi/challenge",
        "unusual traffic",
    ]

    if http_status in (403, 429):
        evidence.append(f"http_status={http_status}")
        return "FAIL_BLOCK", evidence

    for kw in title_captcha_keywords:
        if kw in t or kw in h:
            evidence.append(f"keyword={kw}")
            return "FAIL_CAPTCHA", evidence

    for kw in title_block_keywords:
        if kw in t:
            if kw in ("forbidden", "too many requests", "security check") and (article_like or pdf_action_like or consent_like):
                evidence.append(f"soft_keyword={kw}")
                continue
            evidence.append(f"keyword={kw}")
            return "FAIL_BLOCK", evidence

    for kw in html_captcha_markers:
        if kw in h:
            evidence.append(f"keyword={kw}")
            return "FAIL_CAPTCHA", evidence

    for kw in html_block_markers:
        if kw in t or kw in h:
            if kw in ("forbidden", "too many requests", "/cdn-cgi/challenge", "access denied") and (
                article_like or pdf_action_like or consent_like
            ):
                evidence.append(f"soft_keyword={kw}")
                continue
            evidence.append(f"keyword={kw}")
            return "FAIL_BLOCK", evidence

    return None, evidence


def _resolve_pdf_pipeline_mode() -> str:
    env_mode = os.getenv("PDF_PIPELINE_MODE", "").strip().lower()
    if env_mode in ("baseline", "candidate"):
        return env_mode

    report_path = os.path.abspath(os.path.join("outputs", "benchmark_report.json"))
    if os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)
            if ((report.get("gate") or {}).get("passed")) is True:
                return "candidate"
        except Exception:
            pass

    return "baseline"


# =======================================================
# Download Logics
# =======================================================
# 1. JS Injection (DrissionPage 버전)
# =======================================================

def download_pdf_via_js_injection(page, url, filename, save_dir, logger):
    """
    DrissionPage의 run_js를 사용하여 비동기 fetch 수행 후 Base64 데이터 반환
    """
    logger.info(f"  [Drission] JS Fetch & Base64 Return 시도: {url[:80]}...")
    
    # DrissionPage는 run_js에 인자를 전달하면 자동으로 함수로 래핑하여 실행합니다.
    # Promise를 리턴하면 Python에서 await되어 결과값을 받을 수 있습니다.
    js_script = """
        var targetUrl = arguments[0];
        
        // async 함수 정의 및 즉시 실행하여 Promise 반환
        return (async function(url) {
            // 뷰어 내부라면 src 사용 보정
            if (url === window.location.href) {
                var embed = document.querySelector('embed[type="application/pdf"]');
                if (embed && embed.src) url = embed.src;
            }

            try {
                const response = await fetch(url);
                if (!response.ok) throw new Error('Network response was not ok: ' + response.status);
                
                var ctype = response.headers.get('content-type');
                if (ctype && (ctype.includes('text/html') || ctype.includes('application/json'))) {
                    throw new Error('DETECTED_HTML_OR_JSON');
                }
                
                const blob = await response.blob();
                if (blob.size < 2000) throw new Error('TOO_SMALL');
                
                // Blob -> Base64 변환
                return await new Promise((resolve, reject) => {
                    var reader = new FileReader();
                    reader.readAsDataURL(blob); 
                    reader.onloadend = function() {
                        resolve(reader.result); // 성공 시 데이터 리턴
                    };
                    reader.onerror = function(err) {
                        reject("FAILED: " + err.message);
                    };
                });

            } catch (error) {
                if (error.message === 'DETECTED_HTML_OR_JSON') return "DETECTED_HTML_OR_JSON";
                return "FAILED: " + error.message;
            }
        })(targetUrl);
    """
    
    try:
        # 60초 타임아웃 설정은 DrissionPage 옵션이나 로직으로 처리 필요하지만, 
        # run_js 자체는 동기적으로 결과를 기다림 (내부적으로 CDP awaitPromise 사용)
        result = page.run_js(js_script, url)
        
        # 1. 실패/에러 케이스 처리
        if not result or str(result).startswith("FAILED"):
            logger.warning(f"     JS Fetch 실패: {result}")
            return False
        
        if str(result) == "DETECTED_HTML_OR_JSON":
            logger.warning("     JS HTML 감지됨")
            return False

        # 2. 성공 케이스 (Base64 데이터 수신)
        if str(result).startswith("data:"):
            # "data:application/pdf;base64," 헤더 제거
            try:
                header, encoded = str(result).split(",", 1)
                data = base64.b64decode(encoded)
                
                # 파일 저장
                file_path = os.path.join(save_dir, filename)
                with open(file_path, "wb") as f:
                    f.write(data)
                    
                logger.info(f"     JS 데이터 수신 및 파일 저장 완료: {file_path}")
                return True
            except Exception as e:
                logger.error(f"     Base64 디코딩/저장 실패: {e}")
                return False
            
        return False

    except Exception as e:
        logger.error(f"     JS 실행 중 파이썬 에러: {e}")
        return False


# =======================================================
# 2. Requests Force Download (DrissionPage 연동)
# =======================================================

def force_download_with_requests(page, pdf_url, referer_url, save_path, logger):
    """
    DrissionPage의 쿠키와 User-Agent를 가져와 requests로 다운로드 시도
    """
    try:
        logger.info(f"requests 시도 (Referer: {referer_url})")
        
        # DrissionPage에서 쿠키 가져오기 page.cookies -> [dict, list]
        cookies = page.cookies()[0]
        
        session = requests.Session()
        session.cookies.update(cookies)
        
        # User-Agent 가져오기
        user_agent = page.user_agent
        
        headers = {
            "User-Agent": user_agent,
            "Referer": referer_url,
            "Accept": "application/pdf,application/x-pdf,*/*",
        }
        
        response = session.get(pdf_url, headers=headers, stream=True, timeout=12)
        
        if response.status_code == 200:
            ctype = response.headers.get("Content-Type", "").lower()
            if "html" in ctype or "json" in ctype:
                logger.error(f"requests 실패: 서버가 PDF 대신 {ctype}을 보냈습니다.")
                return False

            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            if _is_valid_pdf(save_path): # _is_valid_pdf는 tools_exp.py 내부에 정의된 함수 사용
                logger.info("  requests 다운로드 성공 (유효한 PDF)")
                return True
            else:
                logger.error("  requests 실패: 파일 손상/HTML 감지")
                if os.path.exists(save_path):
                    os.remove(save_path)
                return False
        return False
    except Exception as e:
        logger.error(f"requests 오류: {e}")
        return False


# =======================================================
# 3. Navigation Download (DrissionPage 버전)
# =======================================================

def download_pdf_via_navigation(page, url, download_dir, logger, timeout_s=30):
    """
    브라우저 네비게이션 -> (가능하면) 버튼 클릭으로 다운로드.
    download_dir 인자는 하위호환을 위해 유지하지만 실제로는 target file path로 사용한다.
    """
    if logger is None:
        import logging
        logger = logging.getLogger("SafetyLogger")

    target_path = download_dir
    target_dir = os.path.dirname(target_path) if os.path.splitext(target_path)[1] else target_path
    os.makedirs(target_dir, exist_ok=True)

    logger.info(f"     브라우저 네비게이션 다운로드 시도: {url}")

    try:
        initial_files = _get_current_files(target_dir)

        # 1) 페이지 이동 (무제한 대기 방지)
        try:
            nav_timeout = max(6, min(int(timeout_s), 12))
        except Exception:
            nav_timeout = 10
        page.get(url, retry=0, interval=0.5, timeout=nav_timeout)
        time.sleep(random.uniform(0.4, 0.9))
        _dismiss_cookie_or_consent_banner(page, logger=logger)
        if _has_auth_required_signal(title=page.title or "", html=page.html or ""):
            logger.warning("        인증/비밀번호 요구 페이지 감지 -> 다운로드 포기")
            return None

        # direct PDF URL이면 이동만으로 다운로드가 시작될 수 있으므로 먼저 짧게 확인
        if any(k in (url or "").lower() for k in (".pdf", "/pdfft", "download=true")):
            maybe_file = _wait_for_new_file_diff(target_dir, initial_files, min(timeout_s, 8), logger=logger)
            if maybe_file and _finalize_downloaded_file(maybe_file, target_path, logger=logger):
                logger.info("        자동 다운로드 감지/확정")
                return target_path

        # viewer 페이지에서 Open 버튼만 누르면 내려오는 경우 대응
        _click_viewer_open_button(page, logger=logger)
        maybe_file = _wait_for_new_file_diff(target_dir, initial_files, min(timeout_s, 6), logger=logger)
        if maybe_file and _finalize_downloaded_file(maybe_file, target_path, logger=logger):
            logger.info("        viewer Open/View 후 다운로드 감지/확정")
            return target_path

        # 2) 버튼 클릭
        try:
            button_xpath = """
                //a[contains(@class, 'pdf') or contains(@title, 'Download') or contains(text(), 'View PDF') or contains(text(), 'Download PDF')] |
                //button[contains(text(), 'View PDF') or contains(text(), 'Download')] |
                //span[contains(text(), 'View PDF') or contains(text(), 'Download')] |
                //a[contains(text(), 'Open')] | //button[contains(text(), 'Open')] |
                //a[contains(@href, '.pdf')] |
                //button[@aria-label='Download'] |
                //button[@aria-label='Download this article'] |
                //a[@title='Download this article'] |
                //button[@title='Download this article'] |
                //a[@aria-label='Download this article'] |
                //*[@id='pdf-download-icon'] |
                //a[contains(text(), '원문보기')] |
                //a[contains(text(), 'PDF 다운로드')] |
                //a[contains(@title, '원문보기')] |
                //img[contains(@alt, 'PDF')] |
                //a[contains(@href, 'down') and contains(@href, 'pdf')]
            """
            buttons = _eles_quick(page, f'xpath:{button_xpath}', timeout=0.8)
            clicked = False
            for btn in buttons:
                if not btn.states.is_displayed:
                    continue
                text = (btn.text or "").strip()
                title = (btn.attr("title") or "").strip()
                aria = (btn.attr("aria-label") or "").strip()
                href = (btn.attr("href") or "").strip()
                blob = f"{text} {title} {aria} {href}".lower()

                # 이미지/보조자료/고해상도 버튼 제외: 같은 DOI에서 다중 파일 생성 방지
                if any(
                    k in blob
                    for k in (
                        "hi-res",
                        "image",
                        "figure",
                        "supplement",
                        "graphical abstract",
                        "dataset",
                        "powerpoint",
                        "ms-power",
                        "ppt",
                        "citation",
                        "export",
                        "bibtex",
                        "ris",
                    )
                ):
                    continue
                # PDF 성격의 버튼만 허용
                if not any(k in blob for k in ("pdf", "view pdf", "open pdf", "/pdfft", ".pdf", "articlepdf")):
                    continue

                btn_info = text or title or aria or "ICON"
                logger.info(f"         버튼 발견: {btn_info[:20]}... 클릭 시도")
                try:
                    btn.click()
                    logger.info("        [Plan A] GUI 클릭 성공")
                except Exception:
                    logger.warning("        GUI 클릭 실패 -> [Plan B] JS 클릭 시도")
                    btn.click(by_js=True)
                clicked = True
                time.sleep(0.7)
                break

            if not clicked:
                logger.warning("        클릭할 PDF 버튼을 못 찾음 (이동만으로 다운로드됐을 수 있음)")
                _dismiss_cookie_or_consent_banner(page, logger=logger)
                _click_viewer_open_button(page, logger=logger)
        except Exception as e:
            logger.warning(f"        버튼 클릭 로직 에러 (무시): {e}")

        # 3) 파일 생성 대기 및 확정
        new_file_path = _wait_for_new_file_diff(target_dir, initial_files, timeout_s, logger=logger)
        if new_file_path and _finalize_downloaded_file(new_file_path, target_path, logger=logger):
            logger.info(f"        파일명/유효성 확인 완료: {target_path}")
            return target_path

        page_src = (page.html or "")
        if "Forbidden" in page_src or "Access Denied" in page_src:
            logger.warning("        403 Forbidden 감지됨")
        elif "challenge" in page_src:
            logger.warning("        캡차 화면 감지됨")
        else:
            logger.warning("        파일 생성 안됨 (타임아웃)")
        return None
    except Exception as e:
        logger.error(f"        네비게이션 다운로드 중 에러: {e}")
        return None

# =======================================================
# 4. CFFI 다운로더
# =======================================================
def download_with_cffi(url, save_path, referer=None, cookies=None, ua=None, logger=None, return_detail=False, timeout=20):
    if os.path.isdir(save_path):
        try: shutil.rmtree(save_path)
        except: pass

    try:
        timeout = max(2, min(int(timeout), MAX_ACTION_WAIT_S))
        if not ua:
            ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        headers = {
            "User-Agent": ua,
            "Referer": referer if referer else "https://www.google.com",
            "Accept": "application/pdf,application/x-pdf,*/*",
            "Accept-Language": BEST_BROWSER_ACCEPT_LANGUAGE,
        }

        cookie_count = 0
        if cookies:
            if isinstance(cookies, dict): cookie_count = len(cookies)
            else: cookie_count = len(cookies)

        if logger:
            logger.info(f"        [CFFI] 다운로드 시도 (쿠키: {cookie_count}개)")

        pipeline_mode = _resolve_pdf_pipeline_mode()

        attempt = download_pdf(
            url,
            save_path,
            strategy_mode=pipeline_mode,
            timeout=timeout,
            min_size=1024,
            headers=headers,
            cookies=cookies,
            strategy_name=f"cffi_{pipeline_mode}",
            phase="direct",
        )

        # 계측 누적 (append-only)
        metrics_path = os.path.abspath(os.path.join("outputs", "download_attempts.jsonl"))
        append_metrics_jsonl(metrics_path, attempt)

        if logger:
            if attempt.success:
                logger.info(
                    f"        [CFFI] 다운로드 성공! "
                    f"(status={attempt.status_code}, elapsed={attempt.elapsed_ms}ms, mode={pipeline_mode})"
                )
            else:
                logger.warning(
                    f"        [CFFI] 실패 reason={attempt.reason}, "
                    f"status={attempt.status_code}, elapsed={attempt.elapsed_ms}ms, mode={pipeline_mode}"
                )

        if return_detail:
            evidence = [
                f"status_code={attempt.status_code}",
                f"content_type={attempt.content_type}",
                f"content_disposition={attempt.content_disposition}",
                f"content_length={attempt.content_length}",
                f"redirect_chain={' -> '.join(attempt.redirect_chain)}",
                f"first_bytes={attempt.first_bytes}",
                f"elapsed_ms={attempt.elapsed_ms}",
                f"strategy={attempt.strategy}",
                f"phase={attempt.phase}",
            ]
            # 429 Retry-After 보강
            if attempt.reason == REASON_FAIL_HTTP_STATUS and attempt.status_code == 429:
                retry_after = attempt.evidence.get("retry_after") if isinstance(attempt.evidence, dict) else None
                if retry_after:
                    evidence.append(f"retry_after={retry_after}")

            return {
                "ok": attempt.success,
                "reason": attempt.reason if not attempt.success else "SUCCESS",
                "evidence": evidence,
                "http_status": attempt.status_code,
            }

        return attempt.success

    except Exception as e:
        if logger:
            logger.warning(f"        [CFFI] 에러: {e}")
        if return_detail:
            reason = REASON_FAIL_TIMEOUT_NETWORK
            if "redirect" in str(e).lower():
                reason = REASON_FAIL_REDIRECT_LOOP
            return {"ok": False, "reason": reason, "evidence": [str(e)], "http_status": None}
        return False

# =======================================================
# DrissionPage cloudflare turnstile bypasser
# =======================================================

def solve_captcha_drission(page, logger):
    issue, evidence = detect_access_issue(title=page.title, html=page.html)
    if issue == "FAIL_CAPTCHA":
        logger.warning(f"        캡차 감지됨. 즉시 중단합니다. evidence={evidence}")
        return True
    return False


# =======================================================
# DrissionPage 크롤러
# =======================================================
def download_with_drission(
    doi_url,
    save_dir,
    filename,
    chrome_path,
    max_attempts=2,
    logger=None,
    mode="first",
    return_detail=False,
    hard_timeout_s=None,
):
    # 폴더 생성
    os.makedirs(save_dir, exist_ok=True)
    full_save_path = os.path.join(save_dir, filename)
    browser_tmp_root = os.path.join(save_dir, ".browser_tmp")
    browser_tmp_dir = os.path.join(browser_tmp_root, os.path.splitext(filename)[0])
    tmp_save_path = os.path.join(browser_tmp_dir, filename)
    
    # 기존 파일 정리
    if os.path.exists(full_save_path):
        try: os.remove(full_save_path)
        except: pass
    try:
        if os.path.exists(browser_tmp_dir):
            shutil.rmtree(browser_tmp_dir)
        os.makedirs(browser_tmp_dir, exist_ok=True)
    except Exception:
        pass

    # --- 옵션 설정 ---
    co = ChromiumOptions()
    co.set_browser_path(chrome_path)
    co.auto_port()
    _apply_best_browser_profile(co)
    
    # 다운로드 설정
    co.set_pref('download.default_directory', browser_tmp_dir) # 다운로드 경로 지정(doi 단위 임시 디렉터리)
    co.set_pref('download.prompt_for_download', False)  # 저장 여부 묻지 않기
    co.set_pref('plugins.always_open_pdf_externally', True) # PDF를 브라우저에서 열지 않고 다운로드
    co.set_pref('profile.default_content_settings.popups', 0) # 팝업 차단 해제

    page = None
    for init_attempt in range(3): # 최대 3번 브라우저 실행 시도
        try:
            page = ChromiumPage(co)
            break # 성공하면 루프 탈출
        except Exception as e:
            if logger: logger.warning(f"     [Drission] 브라우저 실행 실패({init_attempt+1}/3): {e} -> 재시도 중...")
            time.sleep(2) # 2초 대기 후 재시도
            
    if page is None:
        if logger: logger.error(f"     [Drission] 브라우저 초기화 최종 실패. 이 논문은 스킵합니다.")
        if return_detail:
            return {
                "ok": False,
                "reason": "FAIL_NETWORK",
                "evidence": ["browser_init_failed"],
                "stage": "drission-init",
                "domain": _extract_domain(doi_url),
                "http_status": None,
            }
        return False
    
    if mode == "first":
        max_attempts = 1
    per_attempt_timeout = 24 if mode == "deep" else 12
    per_attempt_sleep = 2 if mode == "deep" else 0

    def _detail(ok, reason, evidence=None, stage="drission", http_status=None):
        payload = {
            "ok": ok,
            "reason": reason,
            "evidence": evidence or [],
            "stage": stage,
            "domain": _extract_domain(doi_url),
            "http_status": http_status,
        }
        return payload if return_detail else ok

    def _ret(ok, reason, evidence=None, stage="drission", http_status=None):
        if not ok and page:
            try:
                _safe_screenshot(
                    page,
                    os.path.join(save_dir, "logs", "screenshots"),
                    f"final_fail_capture_{filename}.png",
                    logger,
                )
            except Exception:
                pass
        payload = _detail(ok, reason, evidence=evidence, stage=stage, http_status=http_status)
        try:
            if os.path.exists(browser_tmp_dir):
                shutil.rmtree(browser_tmp_dir)
            if os.path.isdir(browser_tmp_root) and not os.listdir(browser_tmp_root):
                os.rmdir(browser_tmp_root)
        except Exception:
            pass
        _close_page_safely(page, logger)
        return payload
    
    for attempt in range(1, max_attempts + 1):
        try:
            def _over_budget() -> bool:
                return False

            if page is None:
                for init_try in range(3):
                    try:
                        page = ChromiumPage(co)
                        break
                    except Exception as e:
                        time.sleep(2)
                
                if page is None:
                    if logger: logger.error(f"     [Drission] 브라우저 생성 실패 (재시도 {attempt}). 다음 시도로 넘어갑니다.")
                    continue
            
            
            logger.info(f"     [Drission] 접속 시도 ({attempt}/{max_attempts}): {doi_url}")
            
            # 페이지 접속
            page.get(doi_url, retry=0, interval=0.5, timeout=min(per_attempt_timeout, MAX_ACTION_WAIT_S))
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            current_domain = _extract_domain(page.url)
            referer_url = page.url
            page_title = page.title or ""
            page_html = page.html or ""

            issue, evidence = detect_access_issue(title=page_title, html=page_html)
            # 요청사항 반영:
            # hard-fail 판단은 landing 단계에서만 수행하고, 이후 단계는 다운로드 시도까지 진행한다.
            if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                if current_domain.endswith("doi.org"):
                    if logger:
                        logger.info(f"        [Drission] landing issue on doi.org, publisher 이동 시도 계속: {evidence}")
                elif _should_soft_continue_issue(issue, evidence, page_title, page_html, current_domain):
                    if logger:
                        logger.info(f"        [Drission] soft-continue (landing {issue}): {evidence}")
                else:
                    if logger:
                        logger.warning(f"        landing 단계 차단/캡차 감지로 중단: {evidence}")
                    return _ret(False, issue, evidence, stage="landing")

            high_friction = _is_high_friction_domain(current_domain)
            is_sciencedirect = "sciencedirect.com" in current_domain
            is_acs = "acs.org" in current_domain
            doi_norm = _doi_from_doi_url(doi_url)

            if is_sciencedirect and mode == "first":
                logger.info(f"        [Elsevier] 2단계 클릭 다운로드 우선 시도: {doi_norm}")
                if _attempt_elsevier_two_step_click_download(
                    page=page,
                    doi=doi_norm,
                    tmp_dir=browser_tmp_dir,
                    tmp_path=tmp_save_path,
                    logger=logger,
                ):
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="elsevier-two-step-click")
                logger.info("        [Elsevier] 클릭 플로우 실패, 기존 다운로드 경로로 계속")

            # --- PDF 링크 탐색 ---
            pdf_url = None
            pdf_btn = _ele_quick(page, 'text:Download PDF', timeout=0.5) or \
                      _ele_quick(page, 'text:View PDF', timeout=0.5) or \
                      _ele_quick(page, 'text:PDF', timeout=0.5) or \
                      _ele_quick(page, 'tag:a@@title:PDF', timeout=0.5) or \
                      _ele_quick(page, 'css:a[href*=".pdf"]', timeout=0.5)
            
            # 1. Meta 태그
            meta = _ele_quick(page, 'xpath://meta[@name="citation_pdf_url"]', timeout=0.5)
            if meta: pdf_url = meta.attr('content')
            
            # 2. 버튼/링크 패턴 매칭
            if not pdf_url:
                if pdf_btn:
                    btn_href = pdf_btn.attr('href')
                    if _looks_like_pdf_link(btn_href):
                        pdf_url = btn_href
                    elif btn_href and logger:
                        logger.info(f"        [LinkFilter] PDF 링크 후보 제외(weak): {btn_href}")
            # 3. analyze_html
                if not pdf_url:
                    pdf_url = _analyze_html_structure_drission(page, logger)
                if pdf_url and "stamp.jsp" in pdf_url:
                    logger.info("        [IEEE] Stamp 링크 감지 -> 실제 PDF 주소 추출 시도")
                    
                    # 1. 해당 뷰어 페이지(stamp.jsp)로 이동
                    page.get(pdf_url, retry=0, interval=0.5, timeout=8 if mode == "deep" else 5)
                    time.sleep(0.6) # 로딩 대기
                    
                    # 2.   _analyze_html_structure_drission 재호출
                    real_url = _analyze_html_structure_drission(page, logger)
                    
                    if real_url and "stamp.jsp" not in real_url:
                        pdf_url = real_url
                        logger.info(f"        [IEEE] Real URL 교체 완료: {pdf_url}")
                    else:
                        logger.warning("        [IEEE] Real URL 추출 실패 (기본 링크 사용)")

            page_title = page.title or ""
            page_html = page.html or ""
            issue, evidence = detect_access_issue(title=page_title, html=page_html)
            if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK") and logger:
                logger.info(f"        [Drission] pdf-discovery issue 관측(계속 진행): {issue}, {evidence}")

            # 4. Iframe
            if not pdf_url:
                iframe = _ele_quick(page, 'tag:iframe@@src:.pdf', timeout=0.5)
                if iframe: pdf_url = iframe.attr('src')
            
            # 고차단 도메인은 실제 사용자 행동과 유사하게 버튼 클릭 다운로드를 우선 시도
            if high_friction and pdf_btn and (not is_acs) and (not is_sciencedirect):
                btn_wait_s = 18 if mode == "deep" else (6 if is_sciencedirect else 12)
                logger.info(f"        [Drission] 고차단 도메인({current_domain}) 버튼 클릭 다운로드 우선 시도")
                if _try_click_pdf_button_download(
                    page=page,
                    pdf_btn=pdf_btn,
                    save_dir=browser_tmp_dir,
                    full_save_path=tmp_save_path,
                    logger=logger,
                    wait_timeout_s=btn_wait_s,
                ):
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="button-click-download")

            # 일반/비지원 도메인도 js 기반 버튼 케이스가 있어 1회 클릭 시도
            if (not high_friction) and pdf_btn and (not is_acs) and (not is_sciencedirect):
                logger.info(f"        [Drission] 일반 도메인({current_domain}) 버튼 클릭 다운로드 1회 시도")
                if _try_click_pdf_button_download(
                    page=page,
                    pdf_btn=pdf_btn,
                    save_dir=browser_tmp_dir,
                    full_save_path=tmp_save_path,
                    logger=logger,
                    wait_timeout_s=8 if mode == "first" else 16,
                ):
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="button-click-download")

            # --- 다운로드 실행 ---
            if pdf_url:
                # 상대 경로를 절대 경로로 변환
                if not pdf_url.startswith('http'):
                    pdf_url = urljoin(page.url, pdf_url)
                
                logger.info(f"        PDF 링크 발견: {pdf_url}")
                if is_sciencedirect:
                    try:
                        _safe_screenshot(
                            page,
                            os.path.join(save_dir, "logs", "screenshots"),
                            f"pre_pdf_attempt_{filename}.png",
                            logger,
                        )
                    except Exception:
                        pass
                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_download"], stage="drission")

                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="already-downloaded")

                if not (is_acs and mode == "first"):
                    # Drissionpage 자체 다운로드 먼저 시도
                    logger.info("        1. Drission 자체 다운로드 시도")
                    try:
                        # [수정] path=폴더경로, rename=파일명 (확장자 포함 가능)
                        # file_exists='overwrite'로 중복 시 덮어쓰기
                        initial_files = _get_current_files(browser_tmp_dir)
                        clean_name = filename # 파일명 그대로 사용
                        page.download(pdf_url, goal_path=browser_tmp_dir, rename=clean_name, file_exists='overwrite')

                        if mode == "deep":
                            download_wait_s = 18
                        else:
                            download_wait_s = 4 if is_sciencedirect else (8 if is_acs else 6)
                        new_file = _wait_for_new_file_diff(browser_tmp_dir, initial_files, timeout_s=download_wait_s, logger=logger)
                        if new_file and _finalize_downloaded_file(new_file, tmp_save_path, logger=logger):
                            logger.info(f"        [Drission] 다운로드 성공")
                            if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                                return _ret(True, "SUCCESS", stage="drission-download")
                        if _is_valid_pdf(tmp_save_path):
                            logger.info(f"        [Drission] 다운로드 성공(파일명 정규화 경유)")
                            if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                                return _ret(True, "SUCCESS", stage="drission-download")
                        logger.info("        자체 다운로드 타임아웃")

                    except Exception as e:
                        logger.warning(f"        자체 다운로드 실패: {e}")
                        pass
                else:
                    logger.info("        [Drission] ACS first mode: 자체 다운로드 스킵(중복 트리거 방지)")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_navigation"], stage="drission")

                if not is_sciencedirect:
                    try :
                        nav_timeout = 8 if high_friction else 6
                        if download_pdf_via_navigation(page, pdf_url, tmp_save_path, logger, timeout_s=nav_timeout):
                            if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                                return _ret(True, "SUCCESS", stage="navigation-download")
                    except : pass

                if _is_valid_pdf(tmp_save_path):
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="post-navigation-file-check")
                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="post-navigation-file-check")

                if is_acs and mode == "first":
                    return _ret(False, "FAIL_PARSE", ["acs_single_click_flow_failed"], stage="drission")

                # Elsevier는 navigation/requests/js 연쇄 시도가 대부분 병목으로만 작동하므로 CFFI cookie 시도로 바로 전환.
                if is_sciencedirect and mode == "first":
                    if _over_budget():
                        return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_cffi"], stage="drission")
                    cookies_list = page.cookies()
                    current_cookies = {c['name']: c['value'] for c in cookies_list}
                    cffi_result = download_with_cffi(
                        pdf_url,
                        full_save_path,
                        referer=page.url,
                        cookies=current_cookies,
                        ua=BEST_BROWSER_UA,
                        logger=logger,
                        return_detail=True,
                        timeout=12,
                    )
                    if cffi_result.get("ok"):
                        return _ret(True, "SUCCESS", stage="cffi-download")
                    return _ret(False, "FAIL_PARSE", ["sciencedirect_pdf_not_downloadable"], stage="drission")

                if mode == "first":
                    return _ret(False, "FAIL_PARSE", ["first_mode_fastpath_exhausted"], stage="drission")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_requests"], stage="drission")

                try :
                    if force_download_with_requests(page, pdf_url, referer_url, full_save_path, logger):
                        return _ret(True, "SUCCESS", stage="requests-download")
                except: pass

                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="post-requests-file-check")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_js"], stage="drission")

                # ACS/Elsevier는 JS 주입 성공률이 낮고 오탐 HTML이 많아 1차 패스에서는 생략한다.
                if not (is_sciencedirect or is_acs):
                    try :
                        if download_pdf_via_js_injection(page, pdf_url, filename, save_dir, logger):
                            return _ret(True, "SUCCESS", stage="js-download")
                    except : pass

                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="post-js-file-check")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_cffi"], stage="drission")

                # 고차단 도메인도 마지막에는 쿠키 포함 CFFI를 시도 (직접 링크 접근은 후순위)
                cookies_list = page.cookies()
                current_cookies = {c['name']: c['value'] for c in cookies_list}
                try : 
                    cffi_result = download_with_cffi(
                        pdf_url,
                        full_save_path,
                        referer=page.url,
                        cookies=current_cookies,
                        ua=BEST_BROWSER_UA,
                        logger=logger,
                        return_detail=True,
                        timeout=120 if mode == "deep" else 60,
                    )
                    if cffi_result.get("ok"):
                        return _ret(True, "SUCCESS", stage="cffi-download")
                    if cffi_result.get("reason") in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                        return _ret(
                            False,
                            cffi_result.get("reason"),
                            cffi_result.get("evidence", []),
                            stage="cffi-download",
                            http_status=cffi_result.get("http_status"),
                        )
                except : pass
                
            else :
                logger.warning(f"        pdf 링크 미발견 : {doi_url}")

        except Exception as e:
            logger.warning(f"        시도 {attempt} 에러: {e}")
            # 에러 발생 시 브라우저 닫고 초기화 (다음 시도에서 재생성)
            if page:
                _close_page_safely(page, logger)
                page = None
            if attempt >= max_attempts:
                return _ret(False, "FAIL_NETWORK", [str(e)], stage="drission")
        
        time.sleep(per_attempt_sleep) # 재시도 전 대기

    return _ret(False, "FAIL_PARSE", ["pdf_link_not_found_or_download_failed"], stage="drission")


# =======================================================
# [핵심] 일반론적 HTML 구조 분석 (IEEE 로직 대폭 강화)
# =======================================================
def _analyze_html_structure_drission(page, logger):
    """
    DrissionPage 객체를 받아 HTML 구조를 분석하여 PDF 링크를 추출하는 함수
    (기존 Selenium analyze_html_structure의 DrissionPage 이식 버전)
    """
    current_url = page.url
    page_source = page.html
    logger.info("     [Drission] HTML 구조 정밀 분석 중...")

    # -------------------------------------------------------
    # 1. [IEEE 전용] stamp.jsp 페이지 처리
    # -------------------------------------------------------
    if "ieeexplore.ieee.org" in current_url and "stamp.jsp" in current_url:
        frame_wait_s = int(os.getenv("PDF_IEEE_IFRAME_WAIT_S", "8"))
        logger.info(f"        IEEE Stamp 페이지 감지. Iframe 로딩 대기중 (최대 {frame_wait_s}초)...")
        
        start_time = time.time()
        found_src = None
        
        while time.time() - start_time < frame_wait_s:
            try:
                # iframe 태그들 찾기
                frames = _eles_quick(page, 'tag:iframe', timeout=0.5)
                for f in frames:
                    s = f.attr('src')
                    if s:
                        # 조건: ielx7(전형적 패턴), .pdf, 또는 pdf가 포함된 긴 주소
                        if ("ielx7" in s or ".pdf" in s.lower() or "pdf" in s.lower()):
                            found_src = s
                            break
                
                if found_src:
                    break
                
                time.sleep(1) # 1초 대기 후 재시도
            except Exception:
                time.sleep(1)

        if found_src:
            if not found_src.startswith("http"):
                found_src = urljoin(current_url, found_src)
            logger.info(f"        IEEE Iframe SRC 발견: {found_src}")
            return found_src
        else:
            # 타임아웃 시 디버깅용 로그
            frames = _eles_quick(page, 'tag:iframe', timeout=0.5)
            src_list = [f.attr('src') for f in frames]
            logger.warning(f"        IEEE Iframe 로딩 실패. 발견된 iframe들: {src_list}")

    # # # -------------------------------------------------------
    # # # 2. ScienceDirect 전용 로직 -> new 시도
    # # # -------------------------------------------------------
    # if "sciencedirect.com" in page.url:
    #     import re
    #     current_url = page.url
    #     # URL에서 PII 추출 (예: /pii/S002195172030005X)
    #     match = re.search(r'/pii/([A-Z0-9]+)', current_url, re.IGNORECASE)
        
    #     if match:
    #         pii_code = match.group(1)
    #         # 이 URL 패턴이 403을 가장 잘 우회하는 "순수 PDF API" 형식입니다.
    #         # download=true 파라미터가 핵심입니다.
    #         pdf_heuristic_url = f"https://www.sciencedirect.com/science/article/pii/{pii_code}/pdfft?isDTM=true&download=true"
    #         logger.info(f"        [ScienceDirect] trying new url : {pdf_heuristic_url}")
    #         return pdf_heuristic_url

    # -------------------------------------------------------
    # 3. Iframe / Embed / Object (일반)
    # -------------------------------------------------------
    try:
        # css selector로 여러 태그 동시 검색
        frames = _eles_quick(page, 'css:iframe, embed, object', timeout=0.5)
        for frame in frames:
            src = frame.attr("src")
            if not src:
                # object 태그의 경우 data 속성을 사용하기도 함
                src = frame.attr("data")
            
            if src:
                src_lower = src.lower()
                if (".pdf" in src_lower or "pdfdirect" in src_lower or "ielx7" in src_lower or "blob:" in src_lower):
                    if not src.startswith("http") and not src.startswith("blob:"):
                        src = urljoin(current_url, src)
                    logger.info(f"        [Frame/Embed] 발견: {src}")
                    return src
    except Exception: 
        pass

    # -------------------------------------------------------
    # 4. Meta Tag
    # -------------------------------------------------------
    try:
        # citation_pdf_url 메타 태그 검색
        meta_pdf = _ele_quick(page, 'css:meta[name="citation_pdf_url"]', timeout=0.5)
        if meta_pdf:
            content = meta_pdf.attr("content")
            if content and content != current_url:
                logger.info(f"        [Meta Tag] 발견: {content}")
                return content
    except Exception: 
        pass

    # -------------------------------------------------------
    # 5. Regex (페이지 소스 텍스트 검색)
    # -------------------------------------------------------
    patterns = [r'"pdfUrl":"([^"]+)"', r'"pdfPath":"([^"]+)"', r'content="([^"]+\.pdf)"', r'src="([^"]+\.pdf)"']
    for pat in patterns:
        match = re.search(pat, page_source, re.IGNORECASE)
        if match:
            url = match.group(1)
            # 유니코드 이스케이프 (\u002F -> /) 처리
            if "\\" in url:
                try: url = url.encode().decode('unicode-escape')
                except: pass
            
            if not url.startswith("http"): 
                url = urljoin(current_url, url)
                
            if len(url) > 10 and url != current_url:
                logger.info(f"        [Regex] 발견: {url}")
                return url

    # -------------------------------------------------------
    # 6. Links (XPath 활용)
    # -------------------------------------------------------
    try:
        xpath_query = "//a[contains(translate(text(), 'PDF', 'pdf'), 'pdf') or contains(@href, '/pdf') or contains(@href, 'download=true')]"
        links = _eles_quick(page, f'xpath:{xpath_query}', timeout=0.5)
        best = None
        best_score = -10**9
        for link in links:
            href = link.attr("href")
            text = (link.text or "").strip().lower()
            if not href:
                continue
            href_low = href.lower()
            # javascript: 링크나 현재 페이지 링크 제외
            if "javascript" in href_low or href == current_url:
                continue

            abs_href = href if href.startswith("http") else urljoin(current_url, href)
            abs_low = abs_href.lower()

            score = 0
            if ".pdf" in abs_low:
                score += 8
            if any(k in abs_low for k in ("/doi/pdf", "/articlepdf", "/pdfft", "download=true")):
                score += 6
            if any(k in text for k in ("download pdf", "view pdf", "open pdf")):
                score += 4
            elif "pdf" in text:
                score += 2

            # 페이지/목차/프로시딩 링크는 우선순위를 강하게 낮춤
            if any(k in abs_low for k in ("/proceedings", "/session", "/program", "/contents", "/toc")):
                score -= 7
            if any(k in abs_low for k in (".jpg", ".jpeg", ".png", ".gif", ".svg")):
                score -= 10

            if score > best_score:
                best_score = score
                best = abs_href

        if best and best_score > 0:
            logger.info(f"        [Link] 발견(score={best_score}): {best}")
            return best
    except Exception:
        pass

    return None

# ======================================================
# sci-hub download
def try_manual_scihub(doi: str, pdf_dir: str, logger=None, max_total_s: int = 15) -> bool:
    mirrors = [
                "https://sci-hub.al",
                "https://sci-hub.mk",
                "https://sci-hub.ee",
                "https://sci-hub.vg",
                "https://sci-hub.kr",
               "https://sci-hub.st", 
               "https://sci-hub.red",
               "https://sci-hub.box", 
               "https://sci-hub.ru", 
               "https://sci-hub.in",
                # "https://sci-hub.se", 
               ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    filename = _sanitize_doi_to_filename(doi)
    filepath = os.path.join(pdf_dir, filename)

    if os.path.exists(filepath):
        logger.info(f"  - 이미 파일이 존재합니다: {filename}")
        return True

    max_total_s = max(3, min(int(max_total_s), MAX_ACTION_WAIT_S))
    started_at = time.monotonic()
    for mirror in mirrors:
        if (time.monotonic() - started_at) >= max_total_s:
            if logger:
                logger.info(f"Sci-hub 시간 예산 초과({max_total_s}s)로 중단")
            break
        try:
            target_url = f"{mirror}/{doi}"
            # print(f"  - Sci-Hub 접속 시도: {target_url}")
            resp = requests.get(target_url, headers=headers, timeout=(4, 6), verify=False)
            
            if resp.status_code != 200: continue
            
            content_type = resp.headers.get('Content-Type', '').lower()
            if 'application/pdf' in content_type or resp.content.startswith(b'%PDF'):
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                if logger: 
                    logger.info("Sci-Hub로 다운로드 성공!!!")
                return True
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            pdf_url = None

            # 1. Iframe 
            iframe = soup.select_one('iframe#pdf')
            if iframe:
                pdf_url = iframe.get('src')
            
            # 2. Embed 태그
            if not pdf_url:
                embed = soup.select_one('embed[type="application/pdf"]')
                if embed:
                    pdf_url = embed.get('src')
            
            # 3. 'save' 버튼 또는 링크
            if not pdf_url:
                save_btn = soup.select_one('div#buttons a[onclick]')
                if save_btn:
                    onclick_text = save_btn.get('onclick', '')
                    if "location.href" in onclick_text:
                        # location.href='url' 파싱
                        pdf_url = onclick_text.split("'")[1]
            
            # 4. div.download
            if not pdf_url:
                download_div = soup.find('div', class_='download')
                if download_div and download_div.find('a'):
                    pdf_url = download_div.find('a').get('href')

            if pdf_url:
                # URL 정규화
                if pdf_url.startswith('//'): pdf_url = 'https:' + pdf_url
                else: pdf_url = urljoin(mirror,pdf_url)
                pdf_url = pdf_url.split('#')[0]

                logger.info(f"  - PDF 주소 추출 성공: {pdf_url}")
                
                # 실제 파일 다운로드
                left = max_total_s - (time.monotonic() - started_at)
                if left <= 0:
                    break
                pdf_content = requests.get(pdf_url, headers=headers, timeout=(4, min(8, max(2, int(left)))), verify=False)
                if pdf_content.status_code == 200 and b'%PDF' in pdf_content.content[:1024]:
                    with open(filepath, 'wb') as f:
                        f.write(pdf_content.content)
                    logger.info("Sci-Hub로 다운로드 성공!!!")
                    return True
        except Exception as e:
            logger.warning(f"  - 미러 {mirror} 시도 중 오류: {e}")
            time.sleep(0.2)
            continue
    
    logger.warning("Sci-hub 방법 전부 실패")
    return False

    
    
# =======================================================
import re
from typing import Optional, Dict
import requests

PREFIX_EXACT_MAP: Dict[str, str] = {
    "10.1038": "Nature",
    "10.1021": "ACS",
    "10.1039": "RSC",
    "10.1063": "AIP",
    "10.1088": "IOP",
    "10.1109": "IEEE",
    "10.1016": "ELSEVIER",
    "10.1002": "WILEY",
    "10.1111": "WILEY",
    # CELL은 DOI prefix만으로 ELSEVIER(10.1016)와 분리가 어려움
}

# 2) Crossref가 돌려주는 registrant(스튜어드) name의 변형들을 "원하는 라벨"로 통일
def normalize_publisher_label(raw_name: str, prefix: Optional[str] = None) -> Optional[str]:
    """
    raw_name: Crossref /prefixes/{prefix} 응답의 message.name (등록자/스튜어드 이름)
    prefix: (선택) prefix를 같이 주면 보조 규칙에 활용
    """
    if not raw_name or str(raw_name) == 'non':
        return None

    n = raw_name.lower().strip()

    # Nature 계열: "Springer Nature" 같이 넓은 이름이 나오는 케이스가 있어, prefix 기반 보조룰 포함
    if prefix == "10.1038":
        return "Nature"
    if ("nature" in n) or ("springer" in n) or ("npg" in n):
        return "Nature"

    # ACS
    if ("american chemical society" in n) or ('acs' in n) or re.search(r"\bacs\b", n):
        return "ACS"

    # RSC
    if ("royal society of chemistry" in n) or re.search(r"\brsc\b", n):
        return "RSC"

    # AIP (AIP Publishing / American Institute of Physics 등 변형 흡수)
    if ("aip" in n) or ("american institute of physics" in n) or re.search(r"\baip\b", n):
        return "AIP"

    # IOP (IOP Publishing / Institute of Physics 등 변형 흡수)
    if ("iop publishing" in n) or ("institute of physics" in n) or re.search(r"\biop\b", n):
        return "IOP"

    # IEEE
    if ("institute of electrical and electronics engineers" in n) or re.search(r"\bieee\b", n):
        return "IEEE"

    # ELSEVIER
    if "elsevier" in n:
        return "ELSEVIER"

    # WILEY
    if ("wiley" in n) or ("advanced materials" in n):
        return "WILEY"

    # CELL (Cell Press 등)
    if ("cell press" in n) or re.search(r"\bcell\b", n):
        return "CELL"

    return None


def extract_doi_prefix(prefix_or_doi: str) -> Optional[str]:
    """
    입력이 '10.1016' 같은 prefix일 수도 있고, '10.1016/j.xxx...' 같은 DOI일 수도 있으니 prefix만 추출.
    """
    if not prefix_or_doi:
        return None
    m = re.search(r"(10\.\d{4,9})", prefix_or_doi.strip())
    return m.group(1) if m else None


def get_publisher_from_doi_prefix(
    prefix_or_doi: str,
    *,
    mailto: Optional[str] = None,
    timeout: float = 20.0,
    return_raw_if_unmapped: bool = False,
) -> Optional[str]:
    """
    Crossref REST API /prefixes/{prefix}를 이용해 prefix의 steward(등록자) 이름을 받고,
    이를 사용자가 원하는 퍼블리셔 라벨로 정규화해 반환.

    - 반환 예: "Nature", "ACS", "RSC", "AIP", "IOP", "IEEE", "ELSEVIER", "WILEY", "CELL"
    - 매핑 실패 시: None (혹은 return_raw_if_unmapped=True면 raw registrant name)
    """
    prefix = extract_doi_prefix(prefix_or_doi)
    if not prefix:
        return None

    # 1) prefix만으로 확정 가능한 경우 즉시 반환
    if prefix in PREFIX_EXACT_MAP:
        return PREFIX_EXACT_MAP[prefix]

    # 2) Crossref /prefixes/{prefix} 호출
    #    이 엔드포인트는 steward name과 member ID를 돌려줍니다. :contentReference[oaicite:1]{index=1}
    url = f"https://api.crossref.org/prefixes/{prefix}"
    params = {}
    if mailto:
        params["mailto"] = mailto  # polite pool 사용 권장 패턴에 부합 :contentReference[oaicite:2]{index=2}

    try:
        r = requests.get(url, params=params, timeout=timeout, headers={"Accept": "application/json"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
    except requests.RequestException:
        return None
    except ValueError:
        return None

    msg = data.get("message") or {}
    raw_name = msg.get("name") or ""

    # 3) raw registrant name -> 원하는 라벨로 정규화
    label = normalize_publisher_label(raw_name, prefix=prefix)
    if label:
        return label

    return raw_name if (return_raw_if_unmapped and raw_name) else None


    
# url로 직접 requests 다운로드 (PDF 유효성 검사 포함)
def _download_file(url: str, output_path: str, headers=None, session=None):
    req = session.get if session else requests.get
    
    try:
        response = req(url, headers=headers, stream=True, timeout=20) 
    except Exception as e:
        raise Exception(f"Request error for {url}: {e}")
        
    if response.status_code != 200:
        raise Exception(f"Failed to download {url} (status code: {response.status_code})")
        
    try:
        # 1. 일단 파일 쓰기
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # 2. 파일이 유효한 PDF인지 검사
        if _is_valid_pdf(output_path):
            return True # 성공 시 True 반환
        else:
            # 유효하지 않다면(HTML 등) 파일 삭제 후 에러 발생 -> Worker가 다음 단계로 넘어가게 유도
            if os.path.exists(output_path):
                os.remove(output_path)
            raise Exception("Downloaded file content is NOT a valid PDF (likely HTML or corrupted).")
            
    except Exception as e:
        # 쓰기 중 에러나 유효성 검사 실패 시 청소
        if os.path.exists(output_path):
            try: os.remove(output_path)
            except: pass
        raise Exception(f"Error validating/writing file {output_path}: {e}")


import re
from typing import Optional



def download_via_acspdf(doi: str, output_path: str, logger = None) -> bool:
    pdf_url = f"https://pubs.acs.org/doi/pdf/{doi}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://pubs.acs.org/doi/{doi}",
    }
    referer = headers["Referer"]
    return bool(download_with_cffi(pdf_url, output_path, referer, logger=logger))


def download_via_aippdf(doi: str, output_path: str, logger = None) -> bool:
    # 케이스에 따라 download=true가 더 잘 먹는 경우가 있어 2개를 순차 시도
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://aip.scitation.org/doi/{doi}",
    }
    referer = headers["Referer"]

    url1 = f"https://aip.scitation.org/doi/pdf/{doi}"
    if download_with_cffi(url1, output_path, referer):
        return True

    url2 = f"https://aip.scitation.org/doi/pdf/{doi}?download=true"
    return download_with_cffi(url2, output_path, referer)


def download_via_ioppdf(doi: str, output_path: str, logger = None) -> bool:
    pdf_url = f"https://iopscience.iop.org/article/{doi}/pdf"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://iopscience.iop.org/article/{doi}",
    }
    referer = headers["Referer"]
    return bool(download_with_cffi(pdf_url, output_path, referer, logger=logger))


def download_via_wiley(doi: str, output_path: str, logger = None):
    """
    Download the PDF of a Wiley article via the Wiley TDM API.
    Requires a Wiley API key.
    """
    api_key = WILEY_API_KEY
    if not api_key:
        raise Exception(
            "WILEY_API_KEY is not set. Please configure your Wiley API key.")
    base_url = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"
    url = base_url + doi
    headers = {"Wiley-TDM-Client-Token": api_key}
    response = requests.get(url, headers=headers)
    try:
        if response.status_code == 200:
            with open(output_path, 'wb') as file:
                file.write(response.content)
            if logger:
                logger.info(f"{doi} downloaded successfully via Wiley API")
            return True
        if logger:
            logger.warning(f"Wiley API failed status={response.status_code} for doi={doi}, direct PDF fallback 시도")

        fallback_referer = f"https://onlinelibrary.wiley.com/doi/{doi}"
        fallback_urls = [
            f"https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}?download=true",
            f"https://onlinelibrary.wiley.com/doi/pdf/{doi}",
        ]
        for fu in fallback_urls:
            if download_with_cffi(fu, output_path, referer=fallback_referer, logger=logger):
                return True
        return False
    except Exception as e:
        # Provide a more specific hint on failure
        raise Exception(
            f"Wiley API download failed: {e}. Ensure your API key is correct and you have access rights.")
        
def download_via_springerpdf(doi: str, output_path: str, logger = None):
    """
    Download the PDF of a Springer article (including Nature) by constructing the direct PDF URL.
    Note: This method mimics a browser and may not work for bulk or for closed-access content.
    """
    pdf_url = f"https://nature.com/articles/{doi}.pdf"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://nature.com/articles/{doi}"
    }
    referer = headers["Referer"]
    return bool(download_with_cffi(pdf_url, output_path, referer, logger=logger))
    
# tools_exp.py 에 추가

def download_via_sciencedirect(doi: str, output_path: str, logger=None) -> bool:
    # 1. 브라우저 세팅
    co = ChromiumOptions()
    co.auto_port()
    _apply_best_browser_profile(co)
    page = None

    try:
        page = ChromiumPage(co)
        # 2. Abstract 페이지 접속 
        target_url = f"https://doi.org/{doi}"
        if logger: logger.info(f"        [ScienceDirect] 페이지 접속 시도: {target_url}")
        
        page.get(target_url, retry=0, interval=0.5, timeout=10)
        page_title = page.title or ""
        page_html = page.html or ""
        current_domain = _extract_domain(page.url)
        
        # 3. 캡차/차단 감지 시 즉시 중단 (우회/자동 풀이 없음)
        issue, evidence = detect_access_issue(title=page_title, html=page_html)
        if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
            if _should_soft_continue_issue(issue, evidence, page_title, page_html, current_domain):
                if logger:
                    logger.info(f"        [ScienceDirect] soft-continue: {evidence}")
            else:
                if logger:
                    logger.warning(f"        [ScienceDirect] {issue} 감지: {evidence}")
                return False
        
        # 4. ScienceDirect URL 확인
        current_url = page.url
        if "sciencedirect.com" not in current_url:
            if logger: logger.warning("        [ScienceDirect] 리다이렉트 실패 (다른 사이트?)")
            return False

        # 5. PDF 링크 찾기 (메타 태그 우선)
        pdf_url = None
        meta_pdf = page.ele('css:meta[name="citation_pdf_url"]')
        if meta_pdf:
            pdf_url = meta_pdf.attr("content")
        
        if not pdf_url:
            # URL에서 PII 추출 (예: /science/article/pii/S002195172030005X)
            match = re.search(r'/pii/([A-Z0-9]+)', current_url, re.IGNORECASE)
            if match:
                pii = match.group(1)
                pdf_url = f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft?isDTM=true&download=true"
        
        if not pdf_url:
            if logger: logger.warning("        [ScienceDirect] PDF 링크를 찾을 수 없음")
            return False

        # 헤더 설정
        cookies_list = page.cookies()
        cookies = {c.get("name"): c.get("value") for c in cookies_list if c.get("name")}
        user_agent = page.user_agent
        
        headers = {
            "User-Agent": user_agent,
            "Referer": current_url, # 현재 페이지
            "Accept": "application/pdf,application/x-pdf,*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        
        if logger: logger.info(f"        [ScienceDirect] CFFI 다운로드 시도 (Referer: {current_url})")
        
        response = cffi_requests.get(
            pdf_url,
            headers=headers,
            cookies=cookies, # 브라우저 쿠키 주입
            impersonate="chrome110", # TLS Fingerprint 맞춤
            timeout=12,
            allow_redirects=True
        )
        
        if response.status_code == 200 and b'%PDF' in response.content[:100]:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            if logger: logger.info("        [ScienceDirect] 다운로드 성공")
            return True
        else:
            if logger:
                body_hint = ""
                try:
                    body_hint = (response.text or "")[:180].replace("\n", " ").replace("\r", " ")
                except Exception:
                    body_hint = ""
                logger.warning(
                    f"        [ScienceDirect] 실패 (Status: {response.status_code}, hint: {body_hint})"
                )
            return False
            
    except Exception as e:
        if logger: logger.error(f"        [ScienceDirect] 에러: {e}")
        return False
        
    finally:
        _close_page_safely(page, logger=logger)



def download_using_api(doi: str, output_path: str, publisher: str, logger = None):
    """
    Attempt to download the article PDF using publisher-specific API methods.
    Raises an Exception if no suitable method is found or if the download fails.
    """
    TOOL_FUNCTIONS = {
        "wiley": download_via_wiley,
        "nature": download_via_springerpdf,
        "acs": download_via_acspdf,
        "aip": download_via_aippdf,
        "iop": download_via_ioppdf,
    }
    
    filename = _sanitize_doi_to_filename(doi)
    filepath = os.path.join(output_path, filename)

    if not publisher :
        logger.warning("Publisher is Not recognized or Not supported, cannot use API method.")
        raise Exception("Publisher is Not recognized or Not supported, cannot use API method.")
    
    publisher_key = publisher.lower()
    if publisher_key in TOOL_FUNCTIONS:
        download_func = TOOL_FUNCTIONS[publisher_key]
        logger.info(f"Trying download using api or url for publisher : {publisher_key}, doi : {doi}")
        return download_func(doi, filepath, logger)
    else:
        logger.warning(f"No download method available for publisher: {publisher}")
        raise Exception(f"No download method available for publisher: {publisher}")

MOUSE_PATCH_JS =  """
function getRandomInt(min, max) {
return Math.floor(Math.random() * (max - min + 1)) + min;
}
let screenX = getRandomInt(800, 1200);
let screenY = getRandomInt(400, 600);
Object.defineProperty(MouseEvent.prototype, 'screenX', { value: screenX });
Object.defineProperty(MouseEvent.prototype, 'screenY', { value: screenY });
"""
