import os
import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Iterable, Any
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import matplotlib.pyplot as plt

OPENALEX_ENDPOINT = "https://api.openalex.org/works"
MAX_NUM = 1000
CITATION_PERCENTILE_THRESHOLD = 0.99  # 상위 1% 인용 논문 기준

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
    citation_normalized_percentile = work.get("citation_normalized_percentile") or {}
    open_access_loc = work.get("open_access") or {}

    # 1순위: 호스트 조직 이름 (예: Elsevier BV)
    publisher = source.get("host_organization_name")
    # 2순위: 없는 경우 저널/소스 이름 (예: Nature Communications)
    if not publisher:
        publisher = source.get("display_name")

    return {
        "doi": doi.replace("https://doi.org/", "") if doi else None,
        "title": work.get("title"),
        "publisher" : publisher,
        "publication_year": work.get("publication_year"),
        "cited_by_count": work.get("cited_by_count"),
        "citation_normalized_percentile": citation_normalized_percentile.get("value") if citation_normalized_percentile else None,
        "pdf_url": primary_loc.get("pdf_url"), # arXiv 필터링용
        "open_access": open_access_loc.get("is_oa")
    }

# 데이터를 모두 가져와서
# 1. citation_normalized_percentile 기준 상위 1% 논문과 그 외 논문을 구분
# 2. 나머지 논문들은 overall citation count 기준 상위 논문으로 추가 선정

def main_search(pdf_save_dir = None, csv_name = None, query = None, max_num = 1000, citation_percentile = 0.99) -> str:
    # 1. 설정
    PDF_SAVE_DIR = pdf_save_dir if pdf_save_dir else "./Solid_State_Electrolyte_Battery_Li_Papers"
    CSV_NAME = csv_name if csv_name else "Searched_DOIs.csv"
    CSV_PATH = os.path.join(PDF_SAVE_DIR, CSV_NAME)
    os.makedirs(PDF_SAVE_DIR, exist_ok=True)

    TA_QUERY = query if query else "('solid-state electrolyte' OR 'solid electrolyte') AND 'battery' AND 'Li' NOT ('review' OR 'opinion' OR 'perspective' OR 'overview' OR 'roadmap')"
    print(f"검색 필터:\n{TA_QUERY}\n")
    # OpenAlex 필터 및 선택 필드 설정
    # Type : Article
    # Search : Abstract + Title
    base_filter = f'type:article,title_and_abstract.search:({TA_QUERY})'
    # percentile_filter = f'cited_by_count.percentile.min:{CITATION_PERCENTILE_THRESHOLD}'
    # PERCENTILE_FILTER = f"{base_filter},{percentile_filter}"

    SELECT = ["doi", "title", "publication_year", "cited_by_count", "primary_location", "citation_normalized_percentile", "open_access"]

    # 2. 연도별 상위 인용 논문 데이터 수집
    print(f"Step 1: 전체 논문 수집 중...")
    yearly_percentile_data = []
    for w in iter_openalex_works(
        filter_str=base_filter,
        sort="cited_by_count:desc",
        select_fields=SELECT,
        mailto="yongyong0206@snu.ac.kr",
        max_records=1000000, # 모두 가져오기
    ):
        row = extract_row(w)
        if row['citation_normalized_percentile'] is not None and row['citation_normalized_percentile'] >= citation_percentile:
            row['source'] = f'Yearly TOP {100 - citation_percentile * 100}% cited'
            row['priority'] = 1  # 연도별 상위 인용 논문 우선순위 높게 설정
        else:
            row['source'] = 'Overall Top Citation'
            row['priority'] = 2  # 그 외 논문 우선순위 낮게 설정
        yearly_percentile_data.append(row)
    print(f"총 수집된 논문 수: {len(yearly_percentile_data)}건")
    
    # DOI 전처리
    df_combined = pd.DataFrame(yearly_percentile_data)
    df_combined = df_combined.dropna(subset=['doi'])
    df_combined['doi'] = df_combined['doi'].astype(str).str.lower().str.strip()
    df_combined = df_combined[df_combined['doi'] != '']
    df_combined = df_combined.drop_duplicates(subset=['doi'])
    print(f"중복 및 doi 누락 제거 후 총 논문 수: {len(df_combined)}건")
    
    # 정렬
    df_combined = df_combined.sort_values(by=['priority', 'cited_by_count'], ascending=[True, False])
    df = df_combined.head(max_num)

    # 5.  CSV 저장
    df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"메타데이터 저장 완료: {CSV_PATH}")
    
    # 통계 출력
    total_count = len(df)
    print(f"Total Count: {total_count}")
    print(f"Count (citation_normalized_percentile >= {citation_percentile*100}%): {len(df[df['priority'] == 1])}")
    print(f"Count (Overall Top Citation): {len(df[df['priority'] == 2])}")
    
    # 연도별 통계
    print("\n" + "="*30)
    print(" [연도별 선정 논문 수 통계]")
    print("="*30)
    
    if 'publication_year' in df.columns:
        year_counts = df['publication_year'].value_counts().sort_index(ascending=False)
        for year, count in year_counts.items():
            print(f" {year}년 : {count}편")

    print("="*30)
    
    # 그래프로 시각화 
    if 'publication_year' in df.columns:
        plt.figure(figsize=(10, 6))
        year_counts.plot(kind='bar')
        plt.title('Number of Selected Papers by Publication Year')
        plt.xlabel('Publication Year')
        plt.ylabel('Number of Papers')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.gca().invert_xaxis()
        # plt.show()
        fig_path = os.path.join(PDF_SAVE_DIR, 'selected_papers_by_year.png')
        plt.savefig(fig_path)
        plt.close()
    return CSV_PATH

if __name__ == "__main__":
    main_search()
