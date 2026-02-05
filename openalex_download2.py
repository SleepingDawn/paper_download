import os
import pandas as pd
import time
import requests

from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Iterable, Any
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from seleniumbase import Driver
from tools import download_paper_pdf, get_publisher_from_doi_prefix, download_using_api, try_manual_scihub

OPENALEX_ENDPOINT = "https://api.openalex.org/works"

def iter_openalex_works(
    search: Optional[str] = None,
    filter_str: Optional[str] = None,
    select_fields: Optional[List[str]] = None,
    sort: Optional[str] = None,
    per_page: int = 200,
    mailto: Optional[str] = None,
    max_records: Optional[int] = None,
    sleep_sec: float = 0.15,
) -> Iterable[Dict[str, Any]]:
    session = requests.Session()
    params = {"per-page": max(1, min(per_page, 200)), "cursor": "*"}
    if mailto: params["mailto"] = mailto
    if search: params["search"] = search
    if filter_str: params["filter"] = filter_str
    if sort: params["sort"] = sort
    if select_fields: params["select"] = ",".join(select_fields)

    fetched = 0
    while True:
        r = session.get(OPENALEX_ENDPOINT, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"OpenAlex HTTP {r.status_code}: {r.text[:300]}")
        data = r.json()
        results = data.get("results", [])
        for work in results:
            yield work
            fetched += 1
            if max_records is not None and fetched >= max_records: return
        next_cursor = (data.get("meta") or {}).get("next_cursor")
        if not next_cursor: return
        params["cursor"] = next_cursor
        time.sleep(sleep_sec)

def extract_row(work: Dict[str, Any]) -> Dict[str, Any]:
    doi = work.get("doi")
    if not doi:
        doi = (work.get("ids") or {}).get("doi")
    
    primary_loc = work.get("primary_location") or {}
    source = primary_loc.get("source") or {}

    return {
        "doi": doi.replace("https://doi.org/", "") if doi else None,
        "title": work.get("title"),
        "publication_year": work.get("publication_year"),
        "cited_by_count": work.get("cited_by_count"),
        "pdf_url": primary_loc.get("pdf_url"), # arXiv 필터링용
    }


def OpenAlex_search(pdf_save_dir="./downloaded_pdfs", csv_name="search_results.csv", query=None):
    # 1. 설정
    PDF_SAVE_DIR = pdf_save_dir
    CSV_NAME = os.path.join(pdf_save_dir, csv_name)
    os.makedirs(PDF_SAVE_DIR, exist_ok=True)

    TA_QUERY = query if query else "IGZO TFT"
    print(f"검색 필터:\n{TA_QUERY}\n")
    FILTER = f'type:article|review,title_and_abstract.search:({TA_QUERY})'
    SELECT = ["id", "doi", "title", "publication_year", "cited_by_count", "primary_location", "ids"]

    # 2. 데이터 수집
    print("OpenAlex에서 데이터를 수집 중입니다...")
    works_data = []
    for w in iter_openalex_works(
        filter_str=FILTER,
        sort="cited_by_count:desc",
        select_fields=SELECT,
        mailto="yongyong0206@snu.ac.kr", # 이메일 수정 권장
        max_records=200 # 테스트를 위해 10개만 설정
    ):
        works_data.append(extract_row(w))

    # 3. CSV 저장
    df = pd.DataFrame(works_data)
    df.to_csv(CSV_NAME, index=False, encoding='utf-8-sig')
    print(f"메타데이터 저장 완료: {CSV_NAME}")
    return df


def main():
    final_save_path = os.path.abspath("./paper_downloads_final")
    os.makedirs(final_save_path, exist_ok=True)
    default_download_path = os.path.abspath("./downloaded_files")
    os.makedirs(default_download_path, exist_ok=True)
    
    sb_options = { "uc": True, "headless2": True, "external_pdf": True }
    # driver 오류에 대비한 최대 시도
    max_attempts = 3


    print(">> 브라우저 시작...")
    driver = Driver(**sb_options)
    driver.uc_open_with_reconnect("https://www.google.com", 3)
    
    print(">> OopenAlex 검색 및 메타데이터 수집...")
    # OpenAlex 검색 필터
    # 디스플레이
    NOT_QUERY1 = 'NOT ("display" OR "active matrix" OR AMOLED OR OLED OR LCD OR "pixel circuit" OR "backplane" OR "driver circuit" OR "touch panel")'
    # 광센서
    NOT_QUERY2 = 'NOT (photodetector OR photosensor OR "image sensor" OR "gas sensor" OR biosensor OR "UV sensor" OR "phototransistor")'
    # 배터리/ 촉매 / 에너지소자
    NOT_QUERY3 = 'NOT (battery OR "supercapacitor" OR catalysis OR catalyst OR photocatalysis OR "solar cell" OR "electrode material")'
    # 전체 제외
    EXCLUDE_QUERY = f'{NOT_QUERY1} AND {NOT_QUERY2} AND {NOT_QUERY3}'
    
    # 검색 filter
    ### n-type crystalline IGZO 계열 “메모리 트랜지스터/채널”
    SEARCH_QUERY = '("indium gallium zinc oxide" OR IGZO OR InGaZnO OR "In-Ga-Zn-O" OR IGO OR IZO OR InZnO OR IGTO OR "In-Ga-Sn-O" OR IZTO OR InZnSnO OR ZTO OR ZnSnO) AND (crystalline OR polycrystalline OR epitaxial OR "single crystal" OR nanocrystalline) AND ("memory transistor" OR "TFT memory" OR "nonvolatile memory" OR NVM OR "charge trap" OR "charge trapping" OR "floating gate" OR "charge storage" OR "threshold voltage shift") AND (TFT OR transistor OR "thin-film transistor" OR channel)'
    ### n-type crystalline IGZO + ferroelectric gate 채널
    # SEARCH_QUERY = '("indium gallium zinc oxide" OR IGZO OR InGaZnO OR "In-Ga-Zn-O" OR IGO OR IZO OR InZnO OR IGTO OR "In-Ga-Sn-O" OR IZTO OR InZnSnO OR ZTO OR ZnSnO) AND (crystalline OR polycrystalline OR epitaxial OR "single crystal" OR nanocrystalline) AND (ferroelectric OR FeFET OR "ferroelectric field-effect transistor" OR HZO OR "Hf0.5Zr0.5O2" OR "hafnium zirconium oxide") AND (TFT OR transistor OR "thin-film transistor" OR channel)'
    ### n-type crystalline IGZO “ReRAM/Memristor 채널”
    # SEARCH_QUERY = '("indium gallium zinc oxide" OR IGZO OR InGaZnO OR "In-Ga-Zn-O" OR IGO OR IZO OR InZnO OR IGTO OR "In-Ga-Sn-O" OR IZTO OR InZnSnO OR ZTO OR ZnSnO) AND (crystalline OR polycrystalline OR nanocrystalline) AND ("resistive switching" OR ReRAM OR memristor OR "resistance switching" OR "conductive filament" OR "bipolar switching") AND (device OR thin-film OR "oxide memory")'
    # p-type
    

    TA_QUERY = '("Indium oxide" OR In2O3 OR IGO OR InGaO OR IZO OR InZnO OR IGZO OR InGaZnO OR IZTO OR InZnSnO OR IGTO OR InGaSnO OR ZTO OR ZnSnO) AND (crystalline) AND (oxide) AND (TFT OR channel OR semiconductor)'
    # TA_QUERY = f'({SEARCH_QUERY}) AND {EXCLUDE_QUERY}'
    
    # CSV 저장 경로 변수 분리
    full_csv_name = "openalex_search_results_with_status.csv"
    failed_csv_name = "failed_papers.csv"
    
    df = OpenAlex_search(pdf_save_dir=final_save_path, csv_name="temp_search_results.csv", query=TA_QUERY)
    df['download_status'] = 'Pending' 
    
    print("\nPDF 다운로드를 시작합니다 (arXiv 제외)...")
    
    # [추가 1] 시간 측정 시작
    start_time = time.time()
    
    # 성공 카운트 초기화
    api_downaload_success_count = 0
    sci_hub_success_count = 0
    crawling_success_count = 0
    
    for i, row in df.iterrows():
        doi = str(row['doi'])
        pdf_url_oa = str(row['pdf_url']).lower()
        
        print(f"[{i+1}/{len(df)}] {doi}")
        publisher = get_publisher_from_doi_prefix(doi) if doi and doi != 'None' else None

        # Skip arxiv papers
        if publisher == 'arxiv' or "arxiv.org" in pdf_url_oa:
            print("  - 결과: Skip (arXiv 논문)")
            df.at[i, 'download_status'] = 'Skipped (arXiv)'
            continue

        if doi and doi != 'None':
            try:
                print(f"\n--- 진행: {doi} (via API) ---")
                if download_using_api(doi, final_save_path):
                    print("  - 결과: 성공 (API)")
                    df.at[i, 'download_status'] = 'Success (API)'
                    api_downaload_success_count += 1
                    continue
                else:
                    print("  - 결과: API 다운로드 실패 or 없음, Sci-Hub 시도 중...")
            except Exception as e_api:
                print(f"  - API 다운로드 에러: {e_api}")
                print("  - Sci-Hub 시도 중...")
            try:
                # DOI가 Wiley 또는 Springer인 경우 API 처리
                # Nature API usagge)
                if try_manual_scihub(doi, final_save_path):
                    print("  - 결과: 성공 (Sci-Hub)")
                    df.at[i, 'download_status'] = 'Success (Sci-Hub)'
                    sci_hub_success_count += 1
                    time.sleep(2) 
                else:
                    print("  - 결과: Sci-Hub 실패, 헤드리스 크롤링 시도 중...")
                    doi_url = "https://doi.org/" + doi 
                    print(f"\n--- 진행: {doi_url} ---")
                    
                    try:
                        if download_paper_pdf(doi_url, final_save_path, default_download_path, driver, max_attempts=max_attempts):
                            print("  - 결과: 성공 (Crawling)")
                            df.at[i, 'download_status'] = 'Success (Crawling)'
                            crawling_success_count += 1
                        else :    
                            print(f"  - 크롤링 에러: ")
                            df.at[i, 'download_status'] = f'Failed (Crawling Error)'   
                    except Exception as e_crawl:
                        print(f"  - 크롤링 에러: {e_crawl}")
                        df.at[i, 'download_status'] = f'Failed (Crawling Error: {str(e_crawl)})'

            except Exception as e:
                print(f"메인 에러: {e}")
                df.at[i, 'download_status'] = f'Failed (General Error: {str(e)})'
        else:
            print("  - 결과: DOI 없음")
            df.at[i, 'download_status'] = 'Failed (No DOI)'

    # [추가 2] 시간 측정 종료 및 계산
    end_time = time.time()
    elapsed_seconds = end_time - start_time
    
    # 시, 분, 초 변환
    hours = int(elapsed_seconds // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)

    # 결과 저장 (기존 코드)
    print("\n>> 결과 저장 중...")
    full_csv_path = os.path.join(final_save_path, full_csv_name)
    df.to_csv(full_csv_path, index=False, encoding='utf-8-sig')
    
    failed_df = df[df['download_status'].str.contains('Failed', case=False, na=False)]
    if not failed_df.empty:
        failed_csv_path = os.path.join(final_save_path, failed_csv_name)
        failed_df.to_csv(failed_csv_path, index=False, encoding='utf-8-sig')

    driver.quit()

    # [추가 3] 최종 요약 리포트 출력
    print("="*50)
    print(f"       [작업 완료 리포트]")
    print("="*50)
    print(f"총 처리 문서 수 : {len(df)} 건")
    print(f"성공 (Sci-Hub)  : {sci_hub_success_count} 건")
    print(f"성공 (Crawling) : {crawling_success_count} 건")
    print(f"실패 / 스킵     : {len(df) - (sci_hub_success_count + crawling_success_count)} 건")
    print("-" * 50)
    print(f"총 소요 시간    : {hours}시간 {minutes}분 {seconds}초")
    print(f"평균 처리 시간   : {elapsed_seconds / len(df):.2f} 초/문서")
    print("="*50)
    print(f"failed paper_dois: {df[df['download_status'].str.contains('Failed', case=False, na=False)]['doi'].tolist()}")

if __name__ == "__main__":
    main()