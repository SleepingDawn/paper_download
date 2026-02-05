import os
import re
import time
import shutil
import logging
import requests
import base64
import random 

from typing import Set
from datetime import datetime
from urllib.parse import urljoin, quote
from selenium.webdriver.common.by import By
from seleniumbase import Driver
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException, MoveTargetOutOfBoundsException
from curl_cffi import requests as cffi_requests # 이름 충돌 방지
from DrissionPage import ChromiumPage, ChromiumOptions

DEFAULT_DOWNLOAD_PATH = os.path.abspath("./downloaded_files")
# =======================================================
# Logger
# =======================================================
def setup_logger(save_dir: str, filename: str) -> logging.Logger:
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
                                logger.infof("        정상 PDF 확인 완료 (크기: {curr} bytes): {pdf}")
                                return full_path
                            else:
                                pass
                        time.sleep(0.5)
            time.sleep(1)
        except Exception: time.sleep(1)
    logger.info("      ❌ 파일 감지 타임아웃")
    return None

def _safe_screenshot(driver_or_page, path: str, logger=None):
    """
    Selenium Driver 또는 DrissionPage 객체를 받아 안전하게 스크린샷을 저장합니다.
    """
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # 1. DrissionPage 객체인 경우
        if hasattr(driver_or_page, 'get_screenshot'):
            # full_page=True 옵션으로 전체 화면 캡처
            driver_or_page.get_screenshot(path=path, full_page=True)
            
        # 2. Selenium Driver인 경우 (기존 호환성 유지)
        elif hasattr(driver_or_page, 'save_screenshot'):
            driver_or_page.save_screenshot(path)
            
        msg = f"📸 스크린샷 저장 완료: {path}"
        if logger: 
            logger.info(msg)
        else:
            print(f"      {msg}")
            
    except Exception as e:
        err_msg = f"⚠️ 스크린샷 저장 실패: {e}"
        if logger:
            logger.warning(err_msg)
        else:
            print(f"      {err_msg}")




# =======================================================
# JS Injection
# =======================================================

def download_pdf_via_js_injection(driver, url, filename, save_dir, logger):
    logger.info(f"  JS Fetch & Base64 Return 시도: {url[:80]}...")
    
    # 변경된 JS 스크립트: 다운로드가 아닌 데이터를 Base64로 리턴함
    js_script = """
        var url = arguments[0];
        var callback = arguments[arguments.length - 1];

        // 뷰어 내부라면 src 사용
        if (url === window.location.href) {
            var embed = document.querySelector('embed[type="application/pdf"]');
            if (embed && embed.src) url = embed.src;
        }

        fetch(url)
        .then(response => {
            if (!response.ok) throw new Error('Network response was not ok: ' + response.status);
            var ctype = response.headers.get('content-type');
            
            if (ctype && (ctype.includes('text/html') || ctype.includes('application/json'))) {
                throw new Error('DETECTED_HTML_OR_JSON');
            }
            return response.blob();
        })
        .then(blob => {
            if (blob.size < 2000) throw new Error('TOO_SMALL');
            
            // Blob을 Base64 문자열로 변환
            var reader = new FileReader();
            reader.readAsDataURL(blob); 
            reader.onloadend = function() {
                // "data:application/pdf;base64,....." 형태의 문자열 반환
                callback(reader.result);
            }
        })
        .catch(error => {
            callback("FAILED: " + error.message);
        });
    """
    
    try:
        driver.set_script_timeout(60)
        # JS 실행 결과(Base64 문자열 혹은 에러 메시지)를 받음
        result = driver.execute_async_script(js_script, url)
        
        # 1. 실패/에러 케이스 처리
        if not result or str(result).startswith("FAILED"):
            logger.warning(f"     JS Fetch 실패: {result}")
            return str(result) # FAILED 메시지 반환
        
        if str(result) == "DETECTED_HTML_OR_JSON":
            logger.warning("     JS HTML 감지됨")
            return "DETECTED_HTML_OR_JSON"

        # 2. 성공 케이스 (Base64 데이터 수신)
        if str(result).startswith("data:"):
            # "data:application/pdf;base64," 헤더 제거
            header, encoded = str(result).split(",", 1)
            data = base64.b64decode(encoded)
            
            # Python이 직접 파일을 씀 (경로 문제 해결)
            file_path = os.path.join(save_dir, filename)
            with open(file_path, "wb") as f:
                f.write(data)
                
            logger.info(f"     JS 데이터 수신 및 파일 저장 완료: {file_path}")
            return "SUCCESS"
            
        return "ERROR: Unknown response"

    except Exception as e:
        logger.error(f"     JS 실행 중 파이썬 에러: {e}")
        return "ERROR"

def force_download_with_requests(driver, pdf_url, referer_url, save_path, logger):
    try:
        logger.info(f"requests 시도 (Referer: {referer_url})")
        selenium_cookies = driver.get_cookies()
        session = requests.Session()
        for cookie in selenium_cookies:
            session.cookies.set(cookie["name"], cookie["value"])
        user_agent = driver.execute_script("return navigator.userAgent;")
        
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
            
            if _is_valid_pdf(save_path):
                logger.info("  requests 다운로드 성공 (유효한 PDF)")
                return True
            else:
                logger.error("  requests 실패: 파일 손상/HTML 감지")
                os.remove(save_path)
                return False
        return False
    except Exception as e:
        logger.error(f"requests 오류: {e}")
        
def download_pdf_via_navigation(driver, url, download_dir, logger, timeout_s = 30):
    """
    브라우저 네비게이션 -> GUI 클릭(Plan A) -> JS 클릭(Plan B) 순차 시도
    """
    if logger is None:
        import logging
        logger = logging.getLogger("SafetyLogger")
        logger.setLevel(logging.INFO)
    logger.info(f"   ⚓ [Hybrid] 브라우저 네비게이션 다운로드 시도: {url}")
    try:
        initial_files = _get_current_files(download_dir)
        
        # 1. 페이지 이동
        driver.get(url)
        time.sleep(random.uniform(4, 7)) # 로딩 대기
        
        # 2. 버튼 찾기 및 클릭
        try:
            # 다양한 다운로드 버튼 후보군
            button_xpath = """
                //a[contains(@class, 'pdf') or contains(@title, 'Download') or contains(text(), 'View PDF') or contains(text(), 'Download PDF')] |
                //button[contains(text(), 'View PDF') or contains(text(), 'Download')] |
                //span[contains(text(), 'View PDF') or contains(text(), 'Download')] |
                
                //a[contains(@href, '.pdf')] |
                
                //button[@aria-label='Download'] | 
                //button[@aria-label='Download this article'] |
                //a[@title='Download this article'] |
                //button[@title='Download this article'] |
                //a[@aria-label='Download this article'] |
                //*[@id='pdf-download-icon']
            """
            buttons = driver.find_elements(By.XPATH, button_xpath)
            
            clicked = False
            for btn in buttons:
                if btn.is_displayed():
                    btn_info = btn.text.strip()
                    if not btn_info:
                        btn_info = btn.get_attribute("title") or btn.get_attribute("aria-label") or "ICON"
                    logger.info(f"         버튼 발견: {btn_info[:20]}... 클릭 시도")
                    
                    # [Plan A] 물리적 마우스 클릭 (ActionChains)
                    try:
                        actions = ActionChains(driver)
                        actions.move_to_element(btn).pause(0.5).click().perform()
                        logger.info("      🖱️ [Plan A] GUI 클릭 성공")
                        clicked = True
                    except (MoveTargetOutOfBoundsException, Exception):
                        # [Plan B] 화면 밖이거나 가려져 있으면 JS 강제 클릭
                        logger.warning("      ⚠️ GUI 클릭 실패 -> [Plan B] JS 클릭 시도")
                        driver.execute_script("arguments[0].click();", btn)
                        clicked = True
                    
                    if clicked:
                        time.sleep(5)
                        break
            
            if not clicked:
                logger.warning("      ⚠️ 클릭할 버튼을 못 찾음 (이미 다운로드 시작됐을 수도 있음)")

        except Exception as e:
            logger.warning(f"      ⚠️ 버튼 클릭 로직 에러 (무시): {e}")

        # 3. 파일 생성 대기 (타임아웃 45초)
        new_file_path = _wait_for_new_file_diff(download_dir, initial_files, timeout_s, logger=logger)
        
        if new_file_path:
            logger.info(f"      ✅ 다운로드 성공: {os.path.basename(new_file_path)}")
            return new_file_path
        else:
            # 실패 시 원인 로그 구체화
            if "Forbidden" in driver.page_source or "Access Denied" in driver.page_source:
                logger.warning("      ⛔ 403 Forbidden 감지됨")
            elif "challenge" in driver.page_source:
                logger.warning("      ⛔ 캡차 화면 감지됨")
            else:
                logger.warning("      ⚠️ 파일 생성 안됨 (타임아웃)")
            return None
            
    except Exception as e:
        logger.error(f"      ❌ 네비게이션 다운로드 중 에러: {e}")
        return None

# =======================================================
# 1. CFFI 다운로더
# =======================================================
def download_with_cffi(url, save_path, referer=None, cookies=None, ua=None):
    if os.path.isdir(save_path):
        try: shutil.rmtree(save_path)
        except: pass

    try:
        if not ua:
            ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        headers = {
            "User-Agent": ua,
            "Referer": referer if referer else "https://www.google.com",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        }

        cookie_count = 0
        if cookies:
            if isinstance(cookies, dict): cookie_count = len(cookies)
            else: cookie_count = len(cookies)

        print(f"      📡 [CFFI] 다운로드 시도 (쿠키: {cookie_count}개)")

        response = cffi_requests.get(
            url, 
            headers=headers, 
            cookies=cookies, 
            impersonate="chrome120", 
            timeout=60,
            allow_redirects=True
        )
        
        if response.status_code != 200:
            print(f"      ⚠️ [CFFI] 실패 (Status: {response.status_code})")
            return False

        content_type = response.headers.get('Content-Type', '').lower()
        if 'pdf' in content_type or response.content.startswith(b'%PDF'):
            with open(save_path, 'wb') as f:
                f.write(response.content)
            print(f"      ✅ [CFFI] 다운로드 성공! ({len(response.content)} bytes)")
            return True
        else:
            print(f"      ⚠️ [CFFI] 내용물이 PDF가 아님 (Type: {content_type})")
            return False

    except Exception as e:
        print(f"      ❌ [CFFI] 에러: {e}")
        return False

# =======================================================
# 2. DrissionPage 크롤러
# =======================================================
def download_with_drission(doi_url, save_dir, filename, chrome_path, max_attempts=5):
    save_path = os.path.join(save_dir, filename)
    logger = setup_logger(save_dir, filename)
    # 폴더 충돌 방지
    if os.path.isdir(save_path):
        try: shutil.rmtree(save_path)
        except: pass

    co = ChromiumOptions()
    co.set_browser_path(chrome_path)
    co.headless(True)            
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.auto_port()               
    
    my_ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    co.set_user_agent(my_ua)
    
    # [핵심] 브라우저 시작 시 다운로드 경로 미리 설정 (최신 버전 호환)
    co.set_download_path(save_dir) 

    page = None
    try:
        page = ChromiumPage(co)
        
        for attempt in range(1, max_attempts + 1):
            print(f"   🚀 [Drission] 접속 시도 ({attempt}/{max_attempts}): {doi_url}")
            
            try:
                page.get(doi_url)
                
                # --- CAPTCHA 처리 ---
                time.sleep(2)
                
                # Cloudflare
                if page.ele('@id=turnstile-wrapper') or "cloudflare" in page.title.lower():
                    print("      🛡️ Cloudflare 감지.")
                    try:
                        challenge = page.ele('@id=challenge-stage', timeout=2)
                        if challenge:
                            rect = challenge.rect
                            page.click(rect.location[0] + 10, rect.location[1] + 10)
                    except: pass
                    time.sleep(4)

                # hCaptcha
                if page.ele('tag:iframe@@src:hcaptcha') or "human" in page.html.lower():
                    print("      🛡️ hCaptcha 감지.")
                    try:
                        iframe = page.get_frame('@src^https://newassets.hcaptcha.com/captcha')
                        if iframe:
                            iframe.ele('@id=checkbox').click()
                            print("      👉 체크박스 클릭")
                            time.sleep(4)
                    except: pass
                
                page.wait.load_start()

                # --- PDF 링크 탐색 ---
                pdf_url = analyze_html_structure(page, logger=None)
                
                # (A) Meta
                meta = page.ele('xpath://meta[@name="citation_pdf_url"]')
                if meta: pdf_url = meta.attr('content')
                
                # (B) Buttons
                if not pdf_url:
                    patterns = [
                        'tag:a@@text():PDF', 'tag:a@@text():pdf', 'tag:a@@text():Download PDF',
                        'css:a[href*="/pdf/"]', 'css:a[title="High-Res PDF"]', 
                        'css:a.pdf-download-btn', 'css:.action-button'
                    ]
                    for pat in patterns:
                        btn = page.ele(pat)
                        if btn: 
                            pdf_url = btn.attr('href')
                            break

                # (C) Iframe
                if not pdf_url:
                    iframe = page.ele('tag:iframe')
                    if iframe:
                        src = iframe.attr('src')
                        if src and ('.pdf' in src or 'stamp.jsp' in src):
                            pdf_url = src

                if pdf_url:
                    if not pdf_url.startswith('http'):
                        pdf_url = urljoin(page.url, pdf_url)
                    
                    print(f"      🔎 PDF 링크 발견: {pdf_url}")
                    
                    # 쿠키 추출
                    try: current_cookies = page.cookies.as_dict()
                    except: 
                        try: current_cookies = page.get_cookies(as_dict=True)
                        except: current_cookies = {}

                    current_url = page.url 

                    # 1순위: CFFI
                    if download_with_cffi(pdf_url, save_path, referer=current_url, cookies=current_cookies, ua=my_ua):
                        return True
                    
                    # 2순위: Drission 직접 다운로드 (수정됨)
                    print("      ⚠️ CFFI 실패 -> Drission 직접 다운로드 시도")
                    
                    # [수정] 다운로드 경로 설정 (버전 호환성 확보)
                    try:
                        # 최신 버전: page.set.download_path 또는 download_set 사용 권장
                        # 하지만 가장 안전한 방법은 시작 시 co.set_download_path()를 쓰는 것입니다.
                        # 여기서는 파일명 변경을 위해 download 메서드의 인자를 활용합니다.
                        page.download(pdf_url, save_path, filename) 
                        return True
                    except:
                        # 구버전 방식 시도 (혹시 모를 대비)
                        try:
                            page.download.set_path(save_dir)
                            page.download.set_rename(filename)
                            page(pdf_url)
                            if page.wait.download_finish(timeout=60): return True
                        except Exception as e_down:
                             print(f"      ❌ 직접 다운로드 에러: {e_down}")

                else:
                    print("      ❌ PDF 링크 미발견")

            except Exception as e:
                print(f"      ⚠️ 시도 {attempt} 중 에러: {e}")

            if attempt < max_attempts:
                time.sleep(random.uniform(3, 5))

        print("      ❌ 최종 실패. 스크린샷 저장.")
        safe_name = filename.replace(".pdf", "").replace(".", "_")
        screenshot_path = os.path.join(save_dir, "logs", "screenshots", f"fail_{safe_name}.png")
        _safe_screenshot(page, screenshot_path)
        return False

    except Exception as e:
        print(f"      ❌ Drission 치명적 오류: {e}")
        return False
    finally:
        if page:
            try: page.quit()
            except: pass


# =======================================================
# [핵심] 일반론적 HTML 구조 분석 (IEEE 로직 대폭 강화)
# =======================================================
def analyze_html_structure(driver, logger):
    current_url = driver.current_url
    page_source = driver.page_source
    logger.info("     HTML 구조 정밀 분석 중...")

    # [IEEE 전용] stamp.jsp 페이지라면 iframe 로딩을 아주 끈질기게 기다림
    if "ieeexplore.ieee.org" in current_url and "stamp.jsp" in current_url:
        logger.info("        IEEE Stamp 페이지 감지. Iframe 로딩 대기중 (최대 60초)...")
        try:
            from selenium.webdriver.support.ui import WebDriverWait
            
            # 디버깅용: "발견"된 모든 iframe src를 기록
            def debug_and_find_iframe(d):
                frames = d.find_elements(By.TAG_NAME, "iframe")
                found_srcs = []
                target = None
                
                for f in frames:
                    s = f.get_attribute("src")
                    if s:
                        found_srcs.append(s)
                        # 조건: ielx7(전형적 패턴), .pdf, 또는 pdf가 포함된 긴 주소
                        if ("ielx7" in s or ".pdf" in s.lower() or "pdf" in s.lower()):
                            target = s
                            break
                
                if target: return target
                
                # 못 찾았으면 현재 발견된 것들 출력(디버깅) 후 False 리턴 (계속 대기)
                # (로그가 너무 많아질 수 있으니 5초에 한번씩만 찍히게 할 수도 있지만 여기선 생략)
                return False

            # 최대 60초 동안 유효한 iframe을 기다림 (사람이 CAPTCHA 풀 시간 확보)
            found_src = WebDriverWait(driver, 60).until(debug_and_find_iframe)
            
            if found_src:
                if not found_src.startswith("http"):
                    found_src = urljoin(current_url, found_src)
                logger.info(f"        IEEE Iframe SRC 발견: {found_src}")
                return found_src
                
        except Exception as e:
            # 타임아웃 시, 현재 있는 iframe이라도 다 긁어서 보여줌 (디버깅)
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            logger.warning(f"        IEEE Iframe 로딩 실패. 발견된 iframe들: {[f.get_attribute('src') for f in frames]}")
            
    # Science direct 전용 로직
    if "sciencedirect.com" in current_url and "/pii/" in current_url:
        # 현재 URL이 이미 /pdfft (PDF 직접 링크)가 아니라면 변환 시도
        if "/pdfft" not in current_url:
            # URL 예: .../article/pii/S0016003256911577?via=ihub
            # 목표:   .../article/pii/S0016003256911577/pdfft?pid=1-s2.0-S0016003256911577-main.pdf
            
            # 1. 쿼리 파라미터(?via=...) 제거
            clean_url = current_url.split("?")[0]
            
            # 2. "/pii/XXXX" 부분 추출
            pii_match = re.search(r"/pii/([^/?]+)", clean_url)
            if pii_match:
                pii_code = pii_match.group(1)
                clean_url = clean_url.split("/pii/")[0] + f"/pii/{pii_code}"
                # 3. /pdfft 및 pid 파라미터 추가
                pdf_heuristic_url = f"{clean_url}/pdfft?pid=1-s2.0-{pii_code}-main.pdf"
                logger.info(f"        ScienceDirect PII 감지 -> PDF 링크 추정: {pdf_heuristic_url}")
                return pdf_heuristic_url

    # 1. Iframe / Embed / Object (일반)
    try:
        frames = driver.find_elements(By.CSS_SELECTOR, "iframe, embed, object")
        for frame in frames:
            src = frame.get_attribute("src")
            if src:
                src_lower = src.lower()
                if (".pdf" in src_lower or "pdfdirect" in src_lower or "ielx7" in src_lower or "blob:" in src_lower):
                    if not src.startswith("http") and not src.startswith("blob:"):
                        src = urljoin(current_url, src)
                    logger.info(f"        [Frame/Embed] 발견: {src}")
                    return src
    except: pass

    # 2. Meta Tag
    try:
        meta_pdf = driver.find_element(By.CSS_SELECTOR, 'meta[name="citation_pdf_url"]')
        content = meta_pdf.get_attribute("content")
        if content and content != current_url:
            logger.info(f"        [Meta Tag] 발견: {content}")
            return content
    except: pass

    # 3. Regex
    patterns = [r'"pdfUrl":"([^"]+)"', r'"pdfPath":"([^"]+)"', r'content="([^"]+\.pdf)"', r'src="([^"]+\.pdf)"']
    for pat in patterns:
        match = re.search(pat, page_source, re.IGNORECASE)
        if match:
            url = match.group(1)
            url = url.encode().decode('unicode-escape') if "\\" in url else url
            if not url.startswith("http"): url = urljoin(current_url, url)
            if len(url) > 10 and url != current_url:
                logger.info(f"        [Regex] 발견: {url}")
                return url

    # 4. Links
    try:
        xpath_query = "//a[contains(translate(text(), 'PDF', 'pdf'), 'pdf') or contains(@href, '/pdf') or contains(@href, 'download=true')]"
        links = driver.find_elements(By.XPATH, xpath_query)
        for link in links:
            href = link.get_attribute("href")
            if href and "javascript" not in href and href != current_url:
                logger.info(f"        [Link] 발견: {href}")
                return href
    except: pass

    return None

# =======================================================
# Main Logic (CAPTCHA 대기 강화)
# =======================================================
def download_paper_pdf(doi_url, final_save_dir, default_download_dir, driver, max_attempts=3):
    safe_filename = _sanitize_doi_to_filename(doi_url)
    logger = setup_logger(final_save_dir, safe_filename)
    final_file_path = os.path.join(final_save_dir, safe_filename)

    if os.path.exists(final_file_path) and _is_valid_pdf(final_file_path):
        logger.info(f"  [Skip] 정상 파일 존재: {safe_filename}")
        return True

    logger.info(f"작업 시작: {doi_url}")

    try:
        # DOI 페이지 접속
        try: 
            driver.uc_open_with_reconnect(doi_url, reconnect_time = 4)
        except Exception as e:
            driver.get(doi_url)
            
        # captcha 처리 시도
        try : 
            if driver.is_element_visible('iframe[src*="challenge"]', timeout=3) or \
               driver.is_element_visible('iframe[title*="captcha"]', timeout=3):
                driver.uc_click_captcha()
                time.sleep(3)
        except Exception as e:
            pass
        
        # [Wiley/Elsevier 전용 URL 보정
        curr_url = driver.current_url
        if "wiley.com" in curr_url and "/epdf/" in curr_url:
            # ePDF 뷰어는 다운로드가 어려우므로 바로 PDF URL로 변환 시도
            pdf_direct_url = curr_url.replace("/epdf/", "/pdf/")
            logger.info(f"  Wiley ePDF 감지 -> PDF 직접 링크로 변환: {pdf_direct_url}")
            driver.get(pdf_direct_url)
            time.sleep(3)
        
        # CAPTCHA 감지 시 대기
        if "challenge" in driver.title.lower() or "captcha" in driver.page_source.lower() or "security" in driver.title.lower():
            logger.warning("CAPTCHA/보안화면 감지! 10초 대기")
            time.sleep(5) 
            
            # # 대기 후 여전히 캡차인지 확인
            # if "challenge" in driver.title.lower():
            #      logger.warning("   ⚠️ 여전히 캡차 화면입니다. 10초 더 대기...")
            #      time.sleep(10)

        article_page_url = driver.current_url
        
        # html 구조 분석
        pdf_url = analyze_html_structure(driver, logger)
        if not pdf_url: pdf_url = driver.current_url
        
        logger.info(f"  초기 타겟 URL: {pdf_url}")

        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            logger.info(f"  다운로드 시도 ({attempt}/{max_attempts}) : {pdf_url}")
            timeout_s = 10 
            if attempt == max_attempts:
                timeout_s = 30  # 마지막 시도는 좀 더 길게 대기

            # JS Fetch Injection 
            # -------------------------------------------------------
            js_result = download_pdf_via_js_injection(driver, pdf_url, safe_filename, default_download_dir, logger)
            
            if js_result == "SUCCESS":
                src = os.path.join(default_download_dir, safe_filename)
                if os.path.exists(src) and _is_valid_pdf(src):
                    shutil.move(src, final_file_path)
                    logger.info(f"    [JS] 다운로드 및 이동 완료")
                    return True
            
            # JS가 403이나 HTML을 뱉으면, 브라우저가 직접 이동하게 함
            # -------------------------------------------------------
            elif "FAILED" in js_result or "DETECTED_HTML" in js_result:
                logger.warning("    JS 방식 실패 -> 브라우저 네비게이션 방식 시도")
                
                # 만약 URL이 html 페이지 같다면 다시 분석
                if "DETECTED_HTML" in js_result:
                     new_url = analyze_html_structure(driver, logger)
                     if new_url and new_url != pdf_url:
                         pdf_url = new_url # 링크 갱신 후 다음 루프/네비게이션에서 사용
                
                downloaded_file = download_pdf_via_navigation(driver, pdf_url, default_download_dir, logger, timeout_s)
                
                if downloaded_file:
                    shutil.move(downloaded_file, final_file_path)
                    logger.info(f"   [Nav] 다운로드 및 이동 완료")
                    return True
                else:
                    logger.warning("    네비게이션 방식도 실패.")

            # 재시도 
            if attempt < max_attempts:
                logger.info("  링크 재탐색 및 재시도...")
                time.sleep(2)
                # 링크를 다시 찾아보기
                new_url_scan = analyze_html_structure(driver, logger)
                if new_url_scan: pdf_url = new_url_scan

        logger.info(" 브라우저 다운로드 실패 -> requests 시도")
        if force_download_with_requests(driver, pdf_url, article_page_url, final_file_path, logger):
            return True
        os.makedirs(os.path.join(final_save_dir, "logs", "screenshots"), exist_ok=True)
        _safe_screenshot(driver, os.path.join(final_save_dir, "logs", "screenshots", f"final_fail_capture_{safe_filename}.png"), logger)

        logger.error("❌ 최종 실패")
        return False

    except Exception as e:
        _safe_screenshot(driver, os.path.join(final_save_dir, "logs", "screenshots", f"final_fail_capture_{safe_filename}.png"), logger)
        logger.error(f"에러: {e}")
        return False
# ======================================================
# sci-hub download
def try_manual_scihub(doi: str, pdf_dir: str) -> bool:
    """보내주신 HTML 구조(div.download, div.pdf object)를 바탕으로 다운로드합니다."""
    mirrors = [
               "https://sci-hub.red"
               "https://sci-hub.box", 
               "https://sci-hub.st", 
               "https://sci-hub.ru", 
               "https://www.sci-hub.in",
                "https://sci-hub.se", 
               ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://sci-hub.se/'
    }

    filename = _sanitize_doi_to_filename(doi)
    filepath = os.path.join(pdf_dir, filename)

    if os.path.exists(filepath):
        print(f"  - 이미 파일이 존재합니다: {filename}")
        return True

    for mirror in mirrors:
        try:
            target_url = f"{mirror}/{doi}"
            # print(f"  - Sci-Hub 접속 시도: {target_url}")
            resp = requests.get(target_url, headers=headers, timeout=20)
            
            if resp.status_code != 200: continue
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            pdf_url = None

            # 1. <div class="download"> 내부의 <a> 태그 (직접 다운로드 링크)
            download_div = soup.find('div', class_='download')
            if download_div and download_div.find('a'):
                pdf_url = download_div.find('a').get('href')

            # 2. <div class="pdf"> 내부의 <object> 태그 (임베드 데이터)
            if not pdf_url:
                pdf_obj = soup.select_one('div.pdf object')
                if pdf_obj:
                    pdf_url = pdf_obj.get('data')

            if pdf_url:
                # URL 정규화
                if pdf_url.startswith('//'): pdf_url = 'https:' + pdf_url
                elif pdf_url.startswith('/'): pdf_url = mirror + pdf_url
                pdf_url = pdf_url.split('#')[0]

                print(f"  - PDF 주소 추출 성공: {pdf_url}")
                
                # 실제 파일 다운로드
                pdf_content = requests.get(pdf_url, headers=headers, timeout=60)
                if pdf_content.status_code == 200 and b'%PDF' in pdf_content.content[:1024]:
                    with open(filepath, 'wb') as f:
                        f.write(pdf_content.content)
                    return True
        except Exception as e:
            print(f"  - 미러 {mirror} 시도 중 오류: {e}")
            continue
    
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
    if not raw_name:
        return None

    n = raw_name.strip().lower()

    # Nature 계열: "Springer Nature" 같이 넓은 이름이 나오는 케이스가 있어, prefix 기반 보조룰 포함
    if prefix == "10.1038":
        return "Nature"
    if ("springer nature" in n) or ("nature publishing" in n) or ("nature portfolio" in n) or ("npg" in n):
        return "Nature"

    # ACS
    if ("american chemical society" in n) or re.search(r"\bacs\b", n):
        return "ACS"

    # RSC
    if ("royal society of chemistry" in n) or re.search(r"\brsc\b", n):
        return "RSC"

    # AIP (AIP Publishing / American Institute of Physics 등 변형 흡수)
    if ("aip publishing" in n) or ("american institute of physics" in n) or re.search(r"\baip\b", n):
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
    if ("wiley" in n) or ("wiley-blackwell" in n) or ("john wiley" in n):
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
from urllib.parse import quote


def _quote_doi_for_path(doi: str) -> str:
    # DOI는 path에 들어가므로 특수문자 대비 (슬래시는 유지)
    return quote(doi.strip(), safe="/")


def _doi_suffix(doi: str) -> str:
    # 10.1038/s41586-... -> s41586-...
    return doi.split("/", 1)[1].strip() if "/" in doi else doi.strip()


def _doi_prefix(doi: str) -> Optional[str]:
    m = re.search(r"(10\.\d{4,9})", doi or "")
    return m.group(1) if m else None


def download_via_acspdf(doi: str, output_path: str) -> bool:
    doi_q = _quote_doi_for_path(doi)
    pdf_url = f"https://pubs.acs.org/doi/pdf/{doi_q}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://pubs.acs.org/doi/{doi_q}",
    }
    referer = headers["Referer"]
    download_with_cffi(pdf_url, output_path, referer)


def download_via_aippdf(doi: str, output_path: str) -> bool:
    doi_q = _quote_doi_for_path(doi)
    # 케이스에 따라 download=true가 더 잘 먹는 경우가 있어 2개를 순차 시도
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://aip.scitation.org/doi/{doi_q}",
    }
    referer = headers["Referer"]

    url1 = f"https://aip.scitation.org/doi/pdf/{doi_q}"
    if download_with_cffi(url1, output_path, referer):
        return True

    url2 = f"https://aip.scitation.org/doi/pdf/{doi_q}?download=true"
    return download_with_cffi(url2, output_path, referer)


def download_via_ioppdf(doi: str, output_path: str) -> bool:
    doi_q = _quote_doi_for_path(doi)
    pdf_url = f"https://iopscience.iop.org/article/{doi_q}/pdf"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://iopscience.iop.org/article/{doi_q}",
    }
    referer = headers["Referer"]
    download_with_cffi(pdf_url, output_path, referer)


def download_via_wiley(doi: str, output_path: str):
    """
    Download the PDF of a Wiley article via the Wiley TDM API.
    Requires a Wiley API key.
    """
    api_key = "b4b01dd9-bf66-4a57-a791-0e7f3ff95a39"
    if not api_key:
        raise Exception(
            "WILEY_API_KEY is not set. Please configure your Wiley API key.")
    base_url = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"
    url = base_url + doi
    headers = {"Wiley-TDM-Client-Token": api_key}
    try:
        return _download_file(url, output_path, headers=headers)
    except Exception as e:
        # Provide a more specific hint on failure
        raise Exception(
            f"Wiley API download failed: {e}. Ensure your API key is correct and you have access rights.")
        
def download_via_springerpdf(doi: str, output_path: str):
    """
    Download the PDF of a Springer article (including Nature) by constructing the direct PDF URL.
    Note: This method mimics a browser and may not work for bulk or for closed-access content.
    """
    pdf_url = f"https://link.springer.com/content/pdf/{doi}.pdf"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://link.springer.com/article/{doi}"
    }
    referer = headers["Referer"]
    download_with_cffi(pdf_url, output_path, referer)



def download_using_api(doi: str, output_path: str, publisher: str):
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
        raise Exception("Publisher is Not recognized or Not supported, cannot use API method.")
    
    publisher_key = publisher.lower().replace(" ", "")
    if publisher_key in TOOL_FUNCTIONS:
        download_func = TOOL_FUNCTIONS[publisher_key]
        return download_func(doi, filepath)
    else:
        raise Exception(f"No download method available for publisher: {publisher}")
