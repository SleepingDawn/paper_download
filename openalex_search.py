import json
import os
import re
import pandas as pd
import time
import requests
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Iterable, Any, Tuple
from urllib.parse import urlparse, unquote

OPENALEX_ENDPOINT = "https://api.openalex.org/works"
OPENALEX_MAILTO = os.environ.get("OPENALEX_MAILTO", "yongyong0206@snu.ac.kr").strip() or "yongyong0206@snu.ac.kr"
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


def _extract_work_doi(work: Dict[str, Any]) -> Optional[str]:
    doi = work.get("doi")
    if not doi:
        doi = (work.get("ids") or {}).get("doi")
    if not doi:
        return None
    return str(doi).replace("https://doi.org/", "").replace("http://doi.org/", "").strip()


def _normalize_title(text: Any) -> str:
    raw = str(text or "").casefold()
    raw = raw.replace("–", "-").replace("—", "-").replace("‐", "-")
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


def _is_ssrn_doi(doi: Any) -> bool:
    return str(doi or "").strip().lower().startswith("10.2139/ssrn.")


def _primary_source(work: Dict[str, Any]) -> Dict[str, Any]:
    return ((work.get("primary_location") or {}).get("source") or {})


def _is_repository_like_work(work: Dict[str, Any]) -> bool:
    doi = str(_extract_work_doi(work) or "").strip().lower()
    source = _primary_source(work)
    source_type = str(source.get("type") or "").strip().lower()
    source_name = str(source.get("display_name") or "").strip().lower()
    if _is_ssrn_doi(doi):
        return True
    if doi.startswith("10.5281/zenodo.") or doi.startswith("10.6084/m9.figshare."):
        return True
    return source_type == "repository" or "zenodo" in source_name or "figshare" in source_name


def _extract_doi_from_url(url: Any) -> Optional[str]:
    raw = str(url or "").strip()
    if not raw:
        return None
    low = raw.lower()
    marker = "doi.org/"
    if marker not in low:
        return None
    idx = low.find(marker)
    doi = raw[idx + len(marker):].strip().strip("/").split("?", 1)[0].split("#", 1)[0].strip()
    doi = unquote(doi).strip().rstrip(".,);")
    return doi or None


def _location_candidates(work: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    primary = work.get("primary_location")
    if isinstance(primary, dict):
        out.append(primary)
    for loc in work.get("locations") or []:
        if isinstance(loc, dict):
            out.append(loc)
    return out


def _location_priority(loc: Dict[str, Any]) -> int:
    source = loc.get("source") or {}
    source_type = str(source.get("type") or "").strip().lower()
    display_name = str(source.get("display_name") or "").strip().lower()
    score = 0
    if source_type == "journal":
        score += 6
    elif source_type and source_type != "repository":
        score += 3
    if loc.get("is_published") is True:
        score += 3
    if loc.get("version") == "publishedVersion":
        score += 2
    if "ssrn" in display_name:
        score -= 6
    return score


def _openalex_get_single_work_by_doi(doi: str) -> Optional[Dict[str, Any]]:
    doi = str(doi or "").strip()
    if not doi:
        return None
    params = {
        "filter": f"doi:{doi}",
        "select": "id,ids,doi,title,type,publication_date,publication_year,cited_by_count,primary_location,locations,authorships,citation_normalized_percentile,open_access",
        "mailto": OPENALEX_MAILTO,
        "per-page": 1,
    }
    try:
        r = requests.get(OPENALEX_ENDPOINT, params=params, timeout=30)
        if r.status_code != 200:
            return None
        results = (r.json() or {}).get("results") or []
        return results[0] if results else None
    except Exception:
        return None


def _try_resolve_published_work_from_locations(work: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], float]:
    original_doi = str(_extract_work_doi(work) or "").strip().lower()
    ranked = sorted(_location_candidates(work), key=_location_priority, reverse=True)
    for loc in ranked:
        for candidate_url in (loc.get("landing_page_url"), loc.get("pdf_url")):
            cand_doi = _extract_doi_from_url(candidate_url)
            if not cand_doi or _is_ssrn_doi(cand_doi):
                continue
            if original_doi and cand_doi.lower() == original_doi:
                continue
            fetched = _openalex_get_single_work_by_doi(cand_doi)
            fetched_doi = str(_extract_work_doi(fetched) or "").strip().lower() if fetched else ""
            if fetched and fetched_doi and fetched_doi != original_doi:
                return fetched, 0.98
    return None, 0.0


def _search_published_work_by_title(work: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], float]:
    title = str(work.get("title") or "").strip()
    if not title:
        return None, 0.0
    first_author_id = ""
    authorships = work.get("authorships") or []
    if authorships and isinstance(authorships[0], dict):
        first_author_id = str((authorships[0].get("author") or {}).get("id") or "").strip()

    filters = ["type:article"]
    year = work.get("publication_year")
    if isinstance(year, int):
        filters.append(f"from_publication_date:{max(1900, year - 1)}-01-01")
        filters.append(f"to_publication_date:{year + 3}-12-31")
    if first_author_id:
        filters.append(f"authorships.author.id:{first_author_id}")

    params = {
        "search": title,
        "filter": ",".join(filters),
        "select": "id,ids,doi,title,type,publication_date,publication_year,cited_by_count,primary_location,locations,authorships,citation_normalized_percentile,open_access",
        "mailto": OPENALEX_MAILTO,
        "per-page": 15,
    }
    try:
        r = requests.get(OPENALEX_ENDPOINT, params=params, timeout=30)
        if r.status_code != 200:
            return None, 0.0
        results = (r.json() or {}).get("results") or []
    except Exception:
        return None, 0.0

    original_doi = _extract_work_doi(work)
    original_cited = int(work.get("cited_by_count") or 0)
    repository_mode = _is_repository_like_work(work) and (not _is_ssrn_doi(original_doi))
    original_title_norm = _normalize_title(title)
    best_work = None
    best_score = -999.0
    for cand in results:
        cand_doi = _extract_work_doi(cand)
        if not cand_doi or _is_ssrn_doi(cand_doi):
            continue
        if original_doi and cand_doi.lower() == original_doi.lower():
            continue

        cand_title_norm = _normalize_title(cand.get("title"))
        similarity = SequenceMatcher(None, original_title_norm, cand_title_norm).ratio()
        similarity_floor = 0.92 if repository_mode else 0.84
        if similarity < similarity_floor:
            continue

        score = similarity * 10.0
        cand_year = cand.get("publication_year")
        if isinstance(year, int) and isinstance(cand_year, int):
            diff = abs(cand_year - year)
            if diff == 0:
                score += 2.0
            elif diff == 1:
                score += 1.0
            elif diff > 3:
                score -= 3.0

        source = ((cand.get("primary_location") or {}).get("source") or {})
        source_type = str(source.get("type") or "").strip().lower()
        source_name = str(source.get("display_name") or "").strip().lower()
        if source_type == "journal":
            score += 3.0
        elif source_type and source_type != "repository":
            score += 1.0
        if "ssrn" in source_name:
            score -= 8.0

        cited = cand.get("cited_by_count")
        if isinstance(cited, int):
            score += min(cited, 200) / 100.0
            if repository_mode and cited < original_cited:
                score -= 2.0

        if repository_mode and source_type != "journal":
            continue

        if score > best_score:
            best_score = score
            best_work = cand

    if best_work is None:
        return None, 0.0
    if repository_mode and best_score < 12.5:
        return None, 0.0
    confidence = min(0.97, max(0.55, best_score / 15.0))
    return best_work, round(confidence, 3)


def _resolve_preferred_work(work: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    original_doi = _extract_work_doi(work)
    original_work_id = work.get("id")
    original_source_type = str(_primary_source(work).get("type") or "").strip().lower()
    resolution = {
        "original_doi": original_doi,
        "original_openalex_id": original_work_id,
        "original_source_type": original_source_type,
        "doi_resolution_method": "none",
        "doi_resolution_confidence": 0.0,
        "resolved_from_ssrn": False,
        "resolved_from_repository": False,
        "resolved_target_openalex_id": original_work_id,
    }
    if not _is_repository_like_work(work):
        return work, resolution

    loc_work, loc_conf = _try_resolve_published_work_from_locations(work)
    if loc_work is not None:
        resolution.update(
            {
                "doi_resolution_method": "location",
                "doi_resolution_confidence": loc_conf,
                "resolved_from_ssrn": _is_ssrn_doi(original_doi),
                "resolved_from_repository": True,
                "resolved_target_openalex_id": loc_work.get("id"),
            }
        )
        return loc_work, resolution

    search_work, search_conf = _search_published_work_by_title(work)
    if search_work is not None:
        resolution.update(
            {
                "doi_resolution_method": "title_match",
                "doi_resolution_confidence": search_conf,
                "resolved_from_ssrn": _is_ssrn_doi(original_doi),
                "resolved_from_repository": True,
                "resolved_target_openalex_id": search_work.get("id"),
            }
        )
        return search_work, resolution

    return work, resolution

def extract_row(work: Dict[str, Any]) -> Dict[str, Any]:
    preferred_work, resolution = _resolve_preferred_work(work)
    doi = _extract_work_doi(preferred_work)

    primary_loc = preferred_work.get("primary_location") or {}
    source = primary_loc.get("source") or {}
    citation_normalized_percentile = preferred_work.get("citation_normalized_percentile") or {}
    open_access_loc = preferred_work.get("open_access") or {}
    authorships = preferred_work.get("authorships") or []

    # 1순위: 호스트 조직 이름 (예: Elsevier BV)
    publisher = source.get("host_organization_name")
    # 2순위: 없는 경우 저널/소스 이름 (예: Nature Communications)
    if not publisher:
        publisher = source.get("display_name")

    author_entries = []
    author_names = []
    for authorship in authorships:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author") or {}
        display_name = (
            author.get("display_name")
            or authorship.get("raw_author_name")
            or ""
        )
        if not display_name:
            continue
        author_names.append(display_name)
        author_entries.append(
            {
                "display_name": display_name,
                "id": author.get("id"),
                "orcid": author.get("orcid"),
                "author_position": authorship.get("author_position"),
                "is_corresponding": authorship.get("is_corresponding"),
            }
        )

    return {
        "openalex_id": preferred_work.get("id"),
        "doi": doi,
        "original_doi": resolution.get("original_doi"),
        "title": preferred_work.get("title"),
        "publisher" : publisher,
        "journal": source.get("display_name"),
        "journal_id": source.get("id"),
        "journal_type": source.get("type"),
        "journal_issn_l": source.get("issn_l"),
        "journal_issn_json": json.dumps(source.get("issn") or [], ensure_ascii=False),
        "publication_date": preferred_work.get("publication_date"),
        "publication_year": preferred_work.get("publication_year"),
        "work_type": preferred_work.get("type"),
        "cited_by_count": preferred_work.get("cited_by_count"),
        "citation_normalized_percentile": citation_normalized_percentile.get("value") if citation_normalized_percentile else None,
        "pdf_url": primary_loc.get("pdf_url"), # arXiv 필터링용
        "open_access": open_access_loc.get("is_oa"),
        "author_count": len(author_entries),
        "first_author": author_names[0] if author_names else None,
        "authors_display": "; ".join(author_names),
        "authors_json": json.dumps(author_entries, ensure_ascii=False),
        "doi_resolution_method": resolution.get("doi_resolution_method"),
        "doi_resolution_confidence": resolution.get("doi_resolution_confidence"),
        "resolved_from_ssrn": resolution.get("resolved_from_ssrn"),
        "resolved_from_repository": resolution.get("resolved_from_repository"),
        "original_source_type": resolution.get("original_source_type"),
        "original_openalex_id": resolution.get("original_openalex_id"),
        "resolved_target_openalex_id": resolution.get("resolved_target_openalex_id"),
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

    SELECT = [
        "id",
        "ids",
        "doi",
        "title",
        "type",
        "publication_date",
        "publication_year",
        "cited_by_count",
        "primary_location",
        "locations",
        "authorships",
        "citation_normalized_percentile",
        "open_access",
    ]

    # 2. 연도별 상위 인용 논문 데이터 수집
    print(f"Step 1: 전체 논문 수집 중...")
    yearly_percentile_data = []
    for w in iter_openalex_works(
        filter_str=base_filter,
        sort="cited_by_count:desc",
        select_fields=SELECT,
        mailto=OPENALEX_MAILTO,
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
    if 'resolved_from_ssrn' in df.columns:
        resolved_ssrn = int(df['resolved_from_ssrn'].fillna(False).astype(bool).sum())
        print(f"Count (SSRN -> published DOI resolved): {resolved_ssrn}")
    
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
        try:
            import matplotlib.pyplot as plt

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
        except Exception as exc:
            print(f"연도별 그래프 생성을 건너뜁니다: {exc}")
    return CSV_PATH

if __name__ == "__main__":
    main_search()
