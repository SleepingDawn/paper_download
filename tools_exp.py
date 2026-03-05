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

def _is_valid_pdf(file_path: str) -> bool:
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 1000:
            return False
        with open(file_path, 'rb') as f:
            header = f.read(4)
            return header.startswith(b'%PDF')
    except: return False

def _wait_for_new_file_diff(download_dir: str, initial_files: Set[str], timeout_s: int = 30, logger = None):
    logger.info(f"     파일 감지 및 유효성 검사 (최대 {timeout_s}초)...")
    t0 = time.time()
    while (time.time() - t0) < timeout_s:
        try:
            current_files = _get_current_files(download_dir)
            new_items = current_files - initial_files
            if not new_items:
                time.sleep(1)
                continue
            
            valid_pdfs = [f for f in new_items if f.lower().endswith(".pdf")]
            for pdf in valid_pdfs:
                full_path = os.path.join(download_dir, pdf)
                if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                    prev_size = -1
                    stable_count = 0
                    for _ in range(5):
                        curr = os.path.getsize(full_path)
                        if curr == prev_size: stable_count += 1
                        else: stable_count = 0
                        prev_size = curr
                        if stable_count >= 2:
                            if _is_valid_pdf(full_path):
                                logger.info(f"        정상 PDF 확인 완료 (크기: {curr} bytes): {pdf}")
                                return full_path
                            else:
                                pass
                        time.sleep(0.5)
            time.sleep(1)
        except Exception: time.sleep(1)
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


def detect_access_issue(title: str = "", html: str = "", http_status: int = None):
    """
    캡차/차단 신호를 감지해 (reason, evidence)를 반환.
    reason: FAIL_CAPTCHA | FAIL_BLOCK | None
    """
    t = (title or "").lower()
    h = (html or "").lower()
    evidence = []

    captcha_keywords = [
        "turnstile",
        "recaptcha",
        "hcaptcha",
        "captcha",
        "are you human",
        "are you a robot",
        "verify you are human",
    ]
    block_keywords = [
        "access denied",
        "forbidden",
        "request blocked",
        "too many requests",
        "bot detected",
        "security check",
        "attention required",
        "cloudflare",
    ]

    if http_status in (403, 429):
        evidence.append(f"http_status={http_status}")
        return "FAIL_BLOCK", evidence

    for kw in captcha_keywords:
        if kw in t or kw in h:
            evidence.append(f"keyword={kw}")
            return "FAIL_CAPTCHA", evidence

    for kw in block_keywords:
        if kw in t or kw in h:
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
        
        response = session.get(pdf_url, headers=headers, stream=True, timeout=30)
        
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
    브라우저 네비게이션 -> GUI 클릭(Plan A) -> JS 클릭(Plan B) 순차 시도
    """
    if logger is None:
        import logging
        logger = logging.getLogger("SafetyLogger")
    
    logger.info(f"     브라우저 네비게이션 다운로드 시도: {url}")
    
    try:
        # tools_exp.py에 정의된 유틸리티 함수 사용
        initial_files = _get_current_files(download_dir)
        
        # 1. 페이지 이동
        page.get(url)
        time.sleep(random.uniform(4, 7)) # 로딩 대기
        
        # 2. 버튼 찾기 및 클릭
        try:
            # 다양한 다운로드 버튼 후보군 XPath
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
                //*[@id='pdf-download-icon']
                
                //a[contains(text(), '원문보기')] | 
                //a[contains(text(), 'PDF 다운로드')] |
                //a[contains(@title, '원문보기')] |
                //img[contains(@alt, 'PDF')] |
                //a[contains(@href, 'down') and contains(@href, 'pdf')]
            """
            
            # DrissionPage: eles()로 여러 요소 찾기
            buttons = page.eles(f'xpath:{button_xpath}')
            
            clicked = False
            for btn in buttons:
                # DrissionPage Element의 가시성 확인 (Selenium의 is_displayed()와 유사)
                # states.is_displayed 속성 사용
                if btn.states.is_displayed:
                    btn_info = btn.text.strip()
                    if not btn_info:
                        btn_info = btn.attr("title") or btn.attr("aria-label") or "ICON"
                    
                    logger.info(f"         버튼 발견: {btn_info[:20]}... 클릭 시도")
                    
                    # [Plan A] DrissionPage Native 클릭 (시뮬레이션)
                    try:
                        btn.click()
                        logger.info("        [Plan A] GUI 클릭 성공")
                        clicked = True
                    except Exception:
                        # [Plan B] JS 강제 클릭
                        logger.warning("        GUI 클릭 실패 -> [Plan B] JS 클릭 시도")
                        btn.click(by_js=True)
                        clicked = True
                    
                    if clicked:
                        time.sleep(5)
                        break
            
            if not clicked:
                logger.warning("        클릭할 버튼을 못 찾음 (이미 다운로드 시작됐을 수도 있음)")

        except Exception as e:
            logger.warning(f"        버튼 클릭 로직 에러 (무시): {e}")

        # 3. 파일 생성 대기
        # _wait_for_new_file_diff 함수는 기존과 동일하게 사용 (파일 시스템 감시이므로)
        new_file_path = _wait_for_new_file_diff(download_dir, initial_files, timeout_s, logger=logger)
        
        if new_file_path:
            final_path = download_dir
        
            if os.path.exists(final_path):
                try: os.remove(final_path)
                except: pass
                
            try:
                os.rename(new_file_path, final_path)
                logger.info(f"        파일명 변경 완료: {os.path.basename(new_file_path)} -> {final_path}")
                return final_path
            except Exception as e:
                logger.warning(f"        파일명 변경 실패 (그대로 유지): {e}")
                return new_file_path
            
        else:
            # 실패 시 원인 로그 구체화 (Page Source 검사)
            page_src = page.html
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
def download_with_cffi(url, save_path, referer=None, cookies=None, ua=None, logger=None, return_detail=False, timeout=60):
    if os.path.isdir(save_path):
        try: shutil.rmtree(save_path)
        except: pass

    try:
        if not ua:
            ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        headers = {
            "User-Agent": ua,
            "Referer": referer if referer else "https://www.google.com",
            "Accept": "application/pdf,application/x-pdf,*/*",
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
):
    # 폴더 생성
    os.makedirs(save_dir, exist_ok=True)
    full_save_path = os.path.join(save_dir, filename)
    
    # 기존 파일 정리
    if os.path.exists(full_save_path):
        try: os.remove(full_save_path)
        except: pass

    # --- 옵션 설정 ---
    co = ChromiumOptions()
    co.set_browser_path(chrome_path)
    co.auto_port() 
    
    co.set_argument('--headless=new') # New headless mode for chromium           
    co.no_imgs(True)            
    co.mute(True)               
    
    # 리눅스/Docker 환경 필수 옵션
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--disable-dev-shm-usage')
    # co.set_argument('--window-size=1920,1080')
    co.set_argument('--start-maximized')
    co.set_argument('--lang=ko_KR,ko;q=0.9,en-US;q=0.8,en;q=0.7')

    my_ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    co.set_user_agent(my_ua)
    
    # 다운로드 설정
    co.set_pref('download.default_directory', save_dir) # 다운로드 경로 지정
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
    
    per_attempt_timeout = 30 if mode == "deep" else 20
    per_attempt_sleep = 4 if mode == "deep" else 2

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
    
    for attempt in range(1, max_attempts + 1):
        try:
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
            page.get(doi_url, retry=1, interval=1, timeout=per_attempt_timeout)

            issue, evidence = detect_access_issue(title=page.title, html=page.html)
            if issue == "FAIL_CAPTCHA":
                if logger:
                    logger.warning(f"        캡차 감지로 즉시 중단: {evidence}")
                return _detail(False, "FAIL_CAPTCHA", evidence, stage="landing")
            if issue == "FAIL_BLOCK":
                if logger:
                    logger.warning(f"        차단 페이지 감지로 즉시 중단: {evidence}")
                return _detail(False, "FAIL_BLOCK", evidence, stage="landing")
            
            referer_url = page.url

            # --- PDF 링크 탐색 ---
            pdf_url = None
            
            # 1. Meta 태그
            meta = page.ele('xpath://meta[@name="citation_pdf_url"]')
            if meta: pdf_url = meta.attr('content')
            
            # 2. 버튼/링크 패턴 매칭
            if not pdf_url:
                # 텍스트나 속성으로 PDF 링크 찾기
                btn = page.ele('text:Download PDF') or \
                      page.ele('text:PDF') or \
                      page.ele('tag:a@@title:PDF') or \
                      page.ele('css:a[href*=".pdf"]')
                
                if btn: pdf_url = btn.attr('href')
            # 3. analyze_html
            if not pdf_url:
                pdf_url = _analyze_html_structure_drission(page, logger)
                if pdf_url and "stamp.jsp" in pdf_url:
                    logger.info("        [IEEE] Stamp 링크 감지 -> 실제 PDF 주소 추출 시도")
                    
                    # 1. 해당 뷰어 페이지(stamp.jsp)로 이동
                    page.get(pdf_url)
                    time.sleep(2) # 로딩 대기
                    
                    # 2.   _analyze_html_structure_drission 재호출
                    real_url = _analyze_html_structure_drission(page, logger)
                    
                    if real_url and "stamp.jsp" not in real_url:
                        pdf_url = real_url
                        logger.info(f"        [IEEE] Real URL 교체 완료: {pdf_url}")
                    else:
                        logger.warning("        [IEEE] Real URL 추출 실패 (기본 링크 사용)")

            issue, evidence = detect_access_issue(title=page.title, html=page.html)
            if issue == "FAIL_CAPTCHA":
                return _detail(False, "FAIL_CAPTCHA", evidence, stage="pdf-discovery")
            if issue == "FAIL_BLOCK":
                return _detail(False, "FAIL_BLOCK", evidence, stage="pdf-discovery")

            # 4. Iframe
            if not pdf_url:
                iframe = page.ele('tag:iframe@@src:.pdf')
                if iframe: pdf_url = iframe.attr('src')
            
            

            # --- 다운로드 실행 ---
            if pdf_url:
                # 상대 경로를 절대 경로로 변환
                if not pdf_url.startswith('http'):
                    pdf_url = urljoin(page.url, pdf_url)
                
                logger.info(f"        PDF 링크 발견: {pdf_url}")
                
                # Drissionpage 자체 다운로드 먼저 시도
                logger.info("        1. Drission 자체 다운로드 시도")
                try:
                    # [수정] path=폴더경로, rename=파일명 (확장자 포함 가능)
                    # file_exists='overwrite'로 중복 시 덮어쓰기
                    clean_name = filename # 파일명 그대로 사용
                    page.download(pdf_url, goal_path=save_dir, rename=clean_name, file_exists='overwrite')
                    
                    # 파일 생성 확인 대기 (최대 30초)
                    wait_time = 0
                    while wait_time < 30:
                        if os.path.exists(full_save_path) and os.path.getsize(full_save_path) > 1024:
                            logger.info(f"        [Drission] 다운로드 성공")
                            if page: page.quit()
                            return _detail(True, "SUCCESS", stage="drission-download")
                        time.sleep(1)
                        wait_time += 1
                    logger.info("        자체 다운로드 타임아웃")

                except Exception as e:
                    logger.warning(f"        자체 다운로드 실패: {e}")
                    pass

                # 1. 쿠키 리스트 가져오기 (인자 없이 호출)
                cookies_list = page.cookies()
                current_cookies = {c['name']: c['value'] for c in cookies_list}
                try : 
                    cffi_result = download_with_cffi(
                        pdf_url,
                        full_save_path,
                        referer=page.url,
                        cookies=current_cookies,
                        ua=my_ua,
                        logger=logger,
                        return_detail=True,
                        timeout=120 if mode == "deep" else 60,
                    )
                    if cffi_result.get("ok"):
                        if page: page.quit()
                        return _detail(True, "SUCCESS", stage="cffi-download")
                    if cffi_result.get("reason") in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                        return _detail(
                            False,
                            cffi_result.get("reason"),
                            cffi_result.get("evidence", []),
                            stage="cffi-download",
                            http_status=cffi_result.get("http_status"),
                        )
                except : pass
                
                try : 
                    if download_pdf_via_js_injection(page, pdf_url, filename, save_dir, logger):
                        return _detail(True, "SUCCESS", stage="js-download")
                except : pass
                
                try : 
                    if force_download_with_requests(page, pdf_url, referer_url, full_save_path, logger):
                        return _detail(True, "SUCCESS", stage="requests-download")
                except: pass
                
                try : 
                    if download_pdf_via_navigation(page, pdf_url, full_save_path, logger, timeout_s = 10):
                        return _detail(True, "SUCCESS", stage="navigation-download")
                except : pass
                
            else :
                logger.warning(f"        pdf 링크 미발견 : {doi_url}")

        except Exception as e:
            logger.warning(f"        시도 {attempt} 에러: {e}")
            # 에러 발생 시 브라우저 닫고 초기화 (다음 시도에서 재생성)
            if page:
                try: page.quit()
                except: pass
                page = None
            if attempt >= max_attempts:
                return _detail(False, "FAIL_NETWORK", [str(e)], stage="drission")
        
        time.sleep(per_attempt_sleep) # 재시도 전 대기

    # 모든 시도 실패 시 브라우저 종료
    if page:
        try: 
            _safe_screenshot(page, os.path.join(save_dir, "logs", "screenshots"), f"final_fail_capture_{filename}.png", logger)
            page.quit()
        except Exception as e: 
            logger.warning(f"can't take screeenshot error : {e}")
            pass
    return _detail(False, "FAIL_PARSE", ["pdf_link_not_found_or_download_failed"], stage="drission")


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
        logger.info("        IEEE Stamp 페이지 감지. Iframe 로딩 대기중 (최대 60초)...")
        
        start_time = time.time()
        found_src = None
        
        while time.time() - start_time < 60:
            try:
                # iframe 태그들 찾기
                frames = page.eles('tag:iframe')
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
            frames = page.eles('tag:iframe')
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
        frames = page.eles('css:iframe, embed, object')
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
        meta_pdf = page.ele('css:meta[name="citation_pdf_url"]')
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
        links = page.eles(f'xpath:{xpath_query}')
        for link in links:
            href = link.attr("href")
            # javascript: 링크나 현재 페이지 링크 제외
            if href and "javascript" not in href and href != current_url:
                if not href.startswith("http"):
                    href = urljoin(current_url, href)
                logger.info(f"        [Link] 발견: {href}")
                return href
    except Exception: 
        pass

    return None

# ======================================================
# sci-hub download
def try_manual_scihub(doi: str, pdf_dir: str, logger = None) -> bool:
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

    for mirror in mirrors:
        try:
            target_url = f"{mirror}/{doi}"
            # print(f"  - Sci-Hub 접속 시도: {target_url}")
            resp = requests.get(target_url, headers=headers, timeout=20, verify=False)
            
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
                pdf_content = requests.get(pdf_url, headers=headers, timeout=60, verify = False)
                if pdf_content.status_code == 200 and b'%PDF' in pdf_content.content[:1024]:
                    with open(filepath, 'wb') as f:
                        f.write(pdf_content.content)
                    logger.info("Sci-Hub로 다운로드 성공!!!")
                    return True
        except Exception as e:
            logger.warning(f"  - 미러 {mirror} 시도 중 오류: {e}")
            time.sleep(1)
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
    download_with_cffi(pdf_url, output_path, referer)


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
    download_with_cffi(pdf_url, output_path, referer)


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
            print(f'{doi} downloaded successfully')
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
    download_with_cffi(pdf_url, output_path, referer)
    
# tools_exp.py 에 추가

def download_via_sciencedirect(doi: str, output_path: str, logger=None) -> bool:
    # 1. 브라우저 세팅
    co = ChromiumOptions()
    co.auto_port()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    page = ChromiumPage(co)
    
    try:
        # 2. Abstract 페이지 접속 
        target_url = f"https://doi.org/{doi}"
        if logger: logger.info(f"        [ScienceDirect] 페이지 접속 시도: {target_url}")
        
        page.get(target_url)
        
        # 3. 캡차/차단 감지 시 즉시 중단 (우회/자동 풀이 없음)
        issue, evidence = detect_access_issue(title=page.title, html=page.html)
        if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
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
        cookies = page.cookies(as_dict=True) # 딕셔너리 형태로 추출
        user_agent = page.user_agent
        
        headers = {
            "User-Agent": user_agent,
            "Referer": current_url, # 현재 페이지
            "Accept": "application/pdf,application/x-pdf,*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Host": "www.sciencedirect.com" # 호스트 명시 권장
        }
        
        if logger: logger.info(f"        [ScienceDirect] CFFI 다운로드 시도 (Referer: {current_url})")
        
        response = cffi_requests.get(
            pdf_url,
            headers=headers,
            cookies=cookies, # 브라우저 쿠키 주입
            impersonate="chrome110", # TLS Fingerprint 맞춤
            timeout=60,
            allow_redirects=True
        )
        
        if response.status_code == 200 and b'%PDF' in response.content[:100]:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            if logger: logger.info("        [ScienceDirect] 다운로드 성공")
            return True
        else:
            if logger: logger.warning(f"        [ScienceDirect] 실패 (Status: {response.status_code})")
            return False
            
    except Exception as e:
        if logger: logger.error(f"        [ScienceDirect] 에러: {e}")
        return False
        
    finally:
        page.quit()



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
        "elsevier" : download_via_sciencedirect,
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
