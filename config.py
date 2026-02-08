import argparse
import os

CHROME_PATH = "/usr/bin/google-chrome"  # 리눅스 예시
# CHROME_PATH = "C:/Program Files/Google/Chrome/Application/chrome.exe" # 윈도우 예시

WILEY_API_KEY = "b4b01dd9-bf66-4a57-a791-0e7f3ff95a39"

DEFAULT_DOWNLOAD_DIR = "./downloaded_files"
DEFAULT_OUTPUT_DIR = "./Solid_State_Electrolyte_Battery_Li_Papers"

def get_config():
    parser = argparse.ArgumentParser(description="OpenAlex Paper Downloader with DrissionPage")

    # 검색 및 다운로드 설정
    parser.add_argument("--query", type=str, default=None,
                        help="검색 쿼리 (기본값: None -> 코드 내 기본 쿼리 사용)")
    
    parser.add_argument("--max_num", type=int, default=1000,
                        help="최대 다운로드 논문 수 (기본값: 1000)")
    
    parser.add_argument("--citation_percentile", type=float, default=0.99,
                        help="인용 상위 퍼센트 필터 (기본값: 0.99)")
    
    # 시스템 설정
    parser.add_argument("--max_workers", type=int, default=4,
                        help="병렬 다운로드 프로세스 수 (기본값: 4)")
    
    parser.add_argument("--output_dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"결과 저장 경로 (기본값: {DEFAULT_OUTPUT_DIR})")
    
    # 외부 doi list import
    parser.add_argument("--doi_path", type=str, default = None,
                        help ="doi리스트 경로")

    args = parser.parse_args()
    return args