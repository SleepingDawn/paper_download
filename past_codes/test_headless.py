import os
import re
import time
import shutil
import logging
import requests
from typing import Set
from datetime import datetime
from urllib.parse import urljoin
from selenium.webdriver.common.by import By
from seleniumbase import Driver

# =======================================================
# Logger
# =======================================================
def setup_logger(save_dir: str) -> logging.Logger:
    log_dir = os.path.join(save_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"download_log_{timestamp}.txt"
    
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

def _wait_for_new_file_diff(download_dir: str, initial_files: Set[str], timeout_s: int = 30):
    print(f"   ⏳ 파일 감지 및 유효성 검사 (최대 {timeout_s}초)...")
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
                                print(f"      ✅ 정상 PDF 확인 완료 (크기: {curr} bytes): {pdf}")
                                return full_path
                            else:
                                pass
                        time.sleep(0.5)
            time.sleep(1)
        except Exception: time.sleep(1)
    print("      ❌ 파일 감지 타임아웃")
    return None

def _safe_screenshot(driver, path: str, logger=None):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        driver.save_screenshot(path)
        if logger: logger.info(f"🖼️ 스크린샷 저장: {path}")
    except: pass




# =======================================================
# JS Injection
# =======================================================
def download_pdf_via_js_injection(driver, url, filename, download_dir, logger):
    logger.info(f"💉 JS Blob Injection 시도: {url[:80]}...")
    
    js_script = """
        var url = arguments[0];
        var filename = arguments[1];
        var callback = arguments[arguments.length - 1];

        // [안전장치] 현재 페이지가 뷰어라면 내부 embed src를 우선 사용
        if (url === window.location.href) {
            var embed = document.querySelector('embed[type="application/pdf"]');
            if (embed && embed.src) url = embed.src;
        }

        fetch(url)
        .then(response => {
            if (!response.ok) throw new Error('Network response was not ok: ' + response.status);
            var ctype = response.headers.get('content-type');
            
            // [핵심] HTML 감지 시 명확한 에러 발생
            if (ctype && (ctype.includes('text/html') || ctype.includes('application/json'))) {
                throw new Error('DETECTED_HTML_OR_JSON');
            }
            return response.blob();
        })
        .then(blob => {
            if (blob.size < 2000) throw new Error('TOO_SMALL');
            
            var a = document.createElement('a');
            var objectUrl = window.URL.createObjectURL(blob);
            a.href = objectUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            setTimeout(() => {
                window.URL.revokeObjectURL(objectUrl);
                document.body.removeChild(a);
                callback("SUCCESS");
            }, 2000);
        })
        .catch(error => {
            callback("FAILED: " + error.message);
        });
    """
    try:
        driver.set_script_timeout(60)
        result = driver.execute_async_script(js_script, url, filename)
        
        if result == "SUCCESS":
            logger.info("   ✅ JS 명령 성공")
            return "SUCCESS"
        else:
            logger.warning(f"   ⚠️ JS 실패: {result}")
            return result
    except Exception as e:
        logger.error(f"   ❌ JS 에러: {e}")
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
                logger.info("✅ requests 다운로드 성공 (유효한 PDF)")
                return True
            else:
                logger.error("❌ requests 실패: 파일 손상/HTML 감지")
                os.remove(save_path)
                return False
        return False
    except Exception as e:
        logger.error(f"requests 오류: {e}")
# =======================================================
        return False
# [핵심] 일반론적 HTML 구조 분석 (IEEE 로직 대폭 강화)
# =======================================================
def analyze_html_structure(driver, logger):
    current_url = driver.current_url
    page_source = driver.page_source
    logger.info("   🔍 HTML 구조 정밀 분석 중...")

    # [IEEE 전용] stamp.jsp 페이지라면 iframe 로딩을 아주 끈질기게 기다림
    if "ieeexplore.ieee.org" in current_url and "stamp.jsp" in current_url:
        logger.info("      ⏳ IEEE Stamp 페이지 감지. Iframe 로딩 대기중 (최대 60초)...")
        try:
            from selenium.webdriver.support.ui import WebDriverWait
            
            # 디버깅용: 발견된 모든 iframe src를 기록
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
                logger.info(f"      ✅ IEEE Iframe SRC 발견: {found_src}")
                return found_src
                
        except Exception as e:
            # 타임아웃 시, 현재 있는 iframe이라도 다 긁어서 보여줌 (디버깅)
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            logger.warning(f"      ⚠️ IEEE Iframe 로딩 실패. 발견된 iframe들: {[f.get_attribute('src') for f in frames]}")

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
                    logger.info(f"      🔹 [Frame/Embed] 발견: {src}")
                    return src
    except: pass

    # 2. Meta Tag
    try:
        meta_pdf = driver.find_element(By.CSS_SELECTOR, 'meta[name="citation_pdf_url"]')
        content = meta_pdf.get_attribute("content")
        if content and content != current_url:
            logger.info(f"      🔹 [Meta Tag] 발견: {content}")
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
                logger.info(f"      🔹 [Regex] 발견: {url}")
                return url

    # 4. Links
    try:
        xpath_query = "//a[contains(translate(text(), 'PDF', 'pdf'), 'pdf') or contains(@href, '/pdf') or contains(@href, 'download=true')]"
        links = driver.find_elements(By.XPATH, xpath_query)
        for link in links:
            href = link.get_attribute("href")
            if href and "javascript" not in href and href != current_url:
                logger.info(f"      🔹 [Link] 발견: {href}")
                return href
    except: pass

    return None

# =======================================================
# Main Logic (CAPTCHA 대기 강화)
# =======================================================
def download_paper_pdf(doi_url, final_save_dir, default_download_dir, driver, max_attempts=3):
    logger = setup_logger(final_save_dir)
    safe_filename = _sanitize_doi_to_filename(doi_url)
    final_file_path = os.path.join(final_save_dir, safe_filename)

    if os.path.exists(final_file_path) and _is_valid_pdf(final_file_path):
        logger.info(f"⏭️ [Skip] 정상 파일 존재: {safe_filename}")
        return True

    logger.info(f"작업 시작: {doi_url}")
    initial_files = _get_current_files(default_download_dir)

    try:
        driver.get(doi_url)
        time.sleep(5)
        
        # [수정] CAPTCHA 감지 시 넉넉하게 대기 (사람이 풀 수 있도록)
        if "challenge" in driver.title.lower() or "captcha" in driver.page_source.lower() or "security" in driver.title.lower():
            logger.warning("🚨 CAPTCHA/보안화면 감지! 10초 대기합니다. (직접 풀어주세요)")
            time.sleep(10) # 10초 -> 30초로 증가
            
            # 대기 후 여전히 캡차인지 확인
            if "challenge" in driver.title.lower():
                 logger.warning("   ⚠️ 여전히 캡차 화면입니다. 10초 더 대기...")
                 time.sleep(10)

        article_page_url = driver.current_url
        
        pdf_url = analyze_html_structure(driver, logger)
        if not pdf_url: pdf_url = driver.current_url
        
        logger.info(f"⬇️ 초기 타겟 URL: {pdf_url}")

        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            logger.info(f"🔄 다운로드 시도 ({attempt}/{max_attempts}) : {pdf_url}")

            if pdf_url != driver.current_url:
                driver.execute_script("window.location.href = arguments[0];", pdf_url)

            if _wait_for_new_file_diff(default_download_dir, initial_files, timeout_s=15):
                new_files = _get_current_files(default_download_dir) - initial_files
                for pdf in new_files:
                    if pdf.lower().endswith(".pdf"):
                        src = os.path.join(default_download_dir, pdf)
                        if _is_valid_pdf(src):
                            time.sleep(1)
                            shutil.move(src, final_file_path)
                            logger.info(f"✅ 이동 완료: {safe_filename}")
                            return True

            js_result = download_pdf_via_js_injection(driver, pdf_url, safe_filename, default_download_dir, logger)
            
            if "SUCCESS" in js_result:
                if _wait_for_new_file_diff(default_download_dir, initial_files, timeout_s=15):
                    new_files = _get_current_files(default_download_dir) - initial_files
                    for pdf in new_files:
                        if pdf.lower().endswith(".pdf"):
                            src = os.path.join(default_download_dir, pdf)
                            if _is_valid_pdf(src):
                                time.sleep(1)
                                shutil.move(src, final_file_path)
                                logger.info(f"✅ 이동 완료: {safe_filename}")
                                return True
                logger.warning("   ⚠️ JS 성공했으나 파일 미발견. 재시도...")
                continue 

            elif "DETECTED_HTML" in js_result or "FAILED" in js_result:
                logger.warning("   🚨 HTML 껍데기 감지. 내부 링크 재탐색...")
                new_url = analyze_html_structure(driver, logger)
                
                if new_url and new_url != pdf_url:
                    logger.info(f"   🔎 내부 링크 발견: {new_url}")
                    pdf_url = new_url 
                    continue
                else:
                    if attempt < max_attempts:
                        logger.warning("   ⚠️ 내부 링크 실패. 페이지 새로고침 후 재시도...")
                        driver.refresh()
                        time.sleep(5)
                        continue 
                    else:
                        logger.error("   ❌ 최대 시도 초과.")
                        break
            else:
                break

        logger.info("⚠️ 브라우저 다운로드 실패 -> requests 시도")
        if force_download_with_requests(driver, pdf_url, article_page_url, final_file_path, logger):
            return True
        _safe_screenshot(driver, os.path.join(final_file_path, "logs", f"final_fail_capture_{safe_filename}.png"), logger)

        logger.error("❌ 최종 실패")
        return False

    except Exception as e:
        logger.error(f"에러: {e}")
        return False

if __name__ == "__main__":
    doi_examples = [
    "https://doi.org/10.1038/s41598-022-24212-7",  # Springer Nature 
    "https://doi.org/10.1021/acsaelm.5c00605",     # ACS Publications
    "https://doi.org/10.1039/D5TC04196A",          # RSC Publications 
    "https://doi.org/10.1116/6.0004967",           # AIP 1 
    "https://doi.org/10.1063/5.0284894",           # AIP 2
    "https://doi.org/10.1088/1361-6641/aacec0",    # IOP
    "https://doi.org/10.1016/j.cej.2025.172405",   # Elsevier 
    "https://doi.org/10.1002/ppap.202400186",      # Wiley
    "https://doi.org/10.1016/j.cell.2025.11.035",  # Cell Press 
    "https://doi.org/10.1109/IEDM50854.2024.10873505", # IEEE 
]

    final_save_path = os.path.abspath("./paper_downloads_final_headless")
    os.makedirs(final_save_path, exist_ok=True)
    default_download_path = os.path.abspath("./downloaded_files")
    os.makedirs(default_download_path, exist_ok=True)
    
    sb_options = { "uc": True, "headless2": True, "external_pdf": True }
    
    print(">> 브라우저 시작...")
    driver = Driver(**sb_options)
    try:
        driver.uc_open_with_reconnect("https://www.google.com", 3)
        for doi in doi_examples:
            print(f"\n--- 진행: {doi} ---")
            download_paper_pdf(doi, final_save_path, default_download_path, driver)
    except Exception as e: print(f"메인 에러: {e}")
    finally:
        print("종료합니다.")
        time.sleep(3)
        driver.quit()