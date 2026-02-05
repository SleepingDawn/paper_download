import os
import pandas as pd
import time
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm  # 진행률 표시를 위해 추가 (pip install tqdm)

# 기존 라이브러리 임포트
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Iterable, Any
from seleniumbase import Driver
from tools_exp import download_with_cffi, download_with_drission, get_publisher_from_doi_prefix, try_manual_scihub, download_using_api, download_paper_pdf
from openalex_search import main_search

# --- OpenAlex  ---
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
    open_access_loc = work.get("open_access") or {}
    
    return {
        "doi": doi.replace("https://doi.org/", "") if doi else None,
        "title": work.get("title"),
        "publication_year": work.get("publication_year"),
        "cited_by_count": work.get("cited_by_count"),
        "pdf_url": primary_loc.get("pdf_url"), 
        "open_access": open_access_loc.get("is_oa"),
    }

def OpenAlex_search(pdf_save_dir="./downloaded_pdfs", csv_name="search_results.csv", query=None):
    # 기존 로직 유지
    PDF_SAVE_DIR = pdf_save_dir
    CSV_NAME = os.path.join(pdf_save_dir, csv_name)
    os.makedirs(PDF_SAVE_DIR, exist_ok=True)

    TA_QUERY = query if query else "IGZO TFT"
    print(f"검색 필터:\n{TA_QUERY}\n")
    FILTER = f'type:article,title_and_abstract.search:({TA_QUERY})'
    SELECT = ["id", "doi", "title", "publication_year", "cited_by_count", "primary_location", "ids", "open_access"]

    print("OpenAlex에서 데이터를 수집 중입니다...")
    works_data = []
    for w in iter_openalex_works(
        filter_str=FILTER,
        sort="cited_by_count:desc",
        select_fields=SELECT,
        mailto="yongyong0206@snu.ac.kr", 
        max_records=500
    ):
        works_data.append(extract_row(w))

    df = pd.DataFrame(works_data)
    df.to_csv(CSV_NAME, index=False, encoding='utf-8-sig')
    print(f"메타데이터 저장 완료: {CSV_NAME}")
    return df

# -----------------------------------------------------------
# 병렬 처리를 위한 단위 작업(Worker) 함수
# -----------------------------------------------------------
def download_process_worker(row_data, final_save_path, default_download_path):
    """
    개별 논문 하나를 처리하는 함수입니다.
    이 함수는 각 프로세스(Process)에서 독립적으로 실행됩니다.
    """
    doi = str(row_data['doi'])
    pdf_url_oa = str(row_data['pdf_url']).lower()
    
    # 결과 반환용 딕셔너리
    result = {
        'doi': doi,
        'status': 'Pending',
        'method': None
    }

    if not doi or doi == 'None':
        result['status'] = 'Failed (No DOI)'
        return result

    publisher = get_publisher_from_doi_prefix(doi)

    # 1. ArXiv, Conference Paper(ECS Meetings) Skip
    if publisher == 'arxiv' or "arxiv.org" in pdf_url_oa or doi.strip().lower().startswith("10.1149/ma"):
        result['status'] = 'Skipped (arXiv or Conference Paper)'
        return result

    # 2. API Download 
    try:
        if download_using_api(doi, final_save_path, publisher):
            result['status'] = 'Success (API)'
            result['method'] = 'api'
            return result
    except Exception:
        pass # 실패 시 다음 단계로

    # 3. Sci-Hub Manual Download
    try:
        if try_manual_scihub(doi, final_save_path):
            result['status'] = 'Success (Sci-Hub)'
            result['method'] = 'scihub'
            # 병렬 처리 시 너무 빠른 연속 요청 방지를 위한 짧은 슬립
            time.sleep(1) 
            return result
    except Exception:
        pass

    # 4. DrissionPage 크롤링 시도
    try:
        # 크롬 경로 지정
        filename = doi.replace('/', '_').replace(':', '-') + ".pdf"
        chrome_path = "/home/yongyong0206/chrome-linux64/chrome"
        doi_url = "https://doi.org/" + doi
        
        # DrissionPage 함수 호출
        if download_with_drission(doi_url, final_save_path, filename, chrome_path):
            result['status'] = 'Success (Drission)'
            result['method'] = 'crawling'
        else:
            result['status'] = 'Failed (Not Found)'
            
    except Exception as e:
        print(f"   Drission 크롤링 중 오류: {e}")
        result['status'] = f'Failed (Error: {str(e)})'

    return result

# -----------------------------------------------------------
# Main 실행부
# -----------------------------------------------------------
def main(max_num=1000, citation_percentile=0.99, query=None, output_dir="./Solid_State_Electrolyte_Battery_Li_Papers"):
    MAX_NUM = max_num
    CITATION_PERCENTILE = citation_percentile
    final_save_path = os.path.abspath(output_dir)
    OA_save_path = os.path.join(final_save_path, "Open_Access")
    CA_save_path = os.path.join(final_save_path, "Closed_Access")
    os.makedirs(final_save_path, exist_ok=True)
    os.makedirs(OA_save_path, exist_ok=True)    
    os.makedirs(CA_save_path, exist_ok=True)    
    
    default_download_path = os.path.abspath("./downloaded_files")
    os.makedirs(default_download_path, exist_ok=True)
    
    # 쿼리 설정 
    TA_QUERY = "('solid-state electrolyte' OR 'solid electrolyte') AND 'battery' AND 'Li' NOT ('review' OR 'opinion' OR 'perspective' OR 'survey' OR 'commentary')" if query is None else query
    
    # OpenAlex 검색 
    # df = OpenAlex_search(pdf_save_dir=final_save_path, csv_name="temp_search_results.csv", query=TA_QUERY)
    csv_path = main_search(final_save_path, "Searched_DOIs.csv", TA_QUERY, max_num=1000, citation_percentile=CITATION_PERCENTILE)
    df = pd.read_csv(csv_path)
    
    # 중복 DOI 제거
    print(f"\n중복 및 doi 누락 제거 전 논문 수: {len(df)}건")
    df['doi_lower'] = df['doi'].astype(str).str.lower().str.strip()
    # df = df.drop(df['doi_lower'] == '')
    df = df.dropna(subset=['doi_lower'])
    df = df.drop_duplicates(subset=['doi_lower'])
    df = df.drop(columns=['doi_lower'])
    print(f"전처리 후 남은 전체 논문 수: {len(df)}건")
    print(f"Open Access 논문 수: {len(df[df['open_access'] == True])}건")
    print(f"Closed Access 논문 수: {len(df[df['open_access'] == False])}건")
    
    df['download_status'] = 'Pending'
    
    print("\nPDF 다운로드를 병렬로 시작합니다 (arXiv 제외)...")
    start_time = time.time()
    
    # 통계용 카운터
    stats = {'api': 0, 'scihub': 0, 'crawling': 0, 'failed': 0, 'skipped': 0}

    # --- 멀티 프로세싱 설정 ---
    # max_workers: 동시에 띄울 프로세스 수. 
    #  MAX_WORKERS = max(os.cpu_count() // 2 , 2)
    MAX_WORKERS = 4

    # 데이터 준비: 함수에 넘길 인자들을 리스트로 변환
    rows = [row for _, row in df.iterrows()]
    
    # ProcessPoolExecutor 시작
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # future 객체들을 담을 딕셔너리 (Future -> 원본 row index 매핑용)
        future_to_index = {
            executor.submit(download_process_worker, row, OA_save_path if row['open_access'] else CA_save_path, default_download_path): i
            for i, row in enumerate(rows)
        }
        
        # as_completed: 작업이 끝나는 순서대로 처리
        for future in tqdm(as_completed(future_to_index), total=len(rows), desc="Processing Papers"):
            idx = future_to_index[future]
            try:
                result = future.result()
                
                # 결과 DF에 반영
                df.at[idx, 'download_status'] = result['status']
                
                # 통계 업데이트
                if result['method'] == 'api': stats['api'] += 1
                elif result['method'] == 'scihub': stats['scihub'] += 1
                elif result['method'] == 'crawling': stats['crawling'] += 1
                elif 'Skipped' in result['status']: stats['skipped'] += 1
                else: stats['failed'] += 1
                
            except Exception as e:
                df.at[idx, 'download_status'] = f'Failed (System Error: {str(e)})'
                stats['failed'] += 1

    # 시간 계산
    end_time = time.time()
    elapsed_seconds = end_time - start_time
    hours = int(elapsed_seconds // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)

    # 결과 저장
    print("\n>> 결과 저장 중...")
    full_csv_name = "openalex_search_results_parallel.csv"
    full_csv_path = os.path.join(final_save_path, full_csv_name)
    df.to_csv(full_csv_path, index=False, encoding='utf-8-sig')
    
    # 실패 목록 저장
    failed_df = df[df['download_status'].str.contains('Failed', case=False, na=False)]
    if not failed_df.empty:
        failed_csv_path = os.path.join(final_save_path, "failed_papers.csv")
        failed_df.to_csv(failed_csv_path, index=False, encoding='utf-8-sig')
        
    # 실패한 논문 재시도(IEEE 등 같은 저널 방문시 차단되는 경우 방지)
    failed_indices = df[~df['download_status'].str.contains('Success', case=False, na=False)].index
    
    if len(failed_indices) > 0:
        print(f"\n" + "="*50)
        print(f"   실패한 {len(failed_indices)}건을 재시도합니다.")
        print(f"   60초간 대기 후, 5초 간격으로 순차 실행.")
        print("="*50)
        
        # 1분 쿨다운 
        time.sleep(60)  

        # 재시도는 순차적으로 처리
        for idx in tqdm(failed_indices, desc="Retrying Failed Papers"):
            row = df.loc[idx]
            doi = row['doi']
            
            # skipped 된 건은 재시도하지 않음
            if "Skipped" in str(row['download_status']):
                continue

            try:
                # worker 함수를 직접 호출 (순차 실행)
                result = download_process_worker(row, OA_save_path if row['open_access'] else CA_save_path, default_download_path)
                
                # 결과 업데이트
                new_status = result['status']
                df.at[idx, 'download_status'] = f"{new_status} (Retry)"
                
                # 통계 업데이트 (성공한 경우만)
                if 'Success' in new_status:
                    method = result.get('method', 'unknown')
                    stats[method] = stats.get(method, 0) + 1
                    # 기존 failed 카운트 하나 줄임
                    stats['failed'] -= 1
                    print(f"   --> 재시도 성공: {doi}")
                
            except Exception as e:
                print(f"   --> 재시도 에러 ({doi}): {e}")

            time.sleep(5) 

    else:
        print("\n✨ 모든 다운로드가 1차 시도에서 성공했거나 실패 건이 없습니다.")

    # 최종 리포트
    print("="*50)
    print(f"       [병렬 작업 완료 리포트]")
    print("="*50)
    print(f"총 처리 문서 수 : {len(df)} 건")
    print(f"성공 (API)      : {stats['api']} 건")
    print(f"성공 (Sci-Hub)  : {stats['scihub']} 건")
    print(f"성공 (Crawling) : {stats['crawling']} 건")
    print(f"실패           : {stats['failed']} 건")
    print(f"스킵 (arXiv)    : {stats['skipped']} 건")
    print("-" * 50)
    print(f"총 소요 시간    : {hours}시간 {minutes}분 {seconds}초")
    print(f"평균 처리 시간   : {elapsed_seconds / len(df):.2f} 초/문서")
    print(f"사용 프로세스 수 : {MAX_WORKERS}")
    print("="*50)

if __name__ == "__main__":
    main()
