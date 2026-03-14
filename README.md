# Paper Download Pipeline

DOI 목록을 입력받아 PDF를 내려받는 파이프라인입니다.

이 저장소에서 다루는 핵심 흐름은 두 가지입니다.

1. `landing_access_repro.py`
   DOI 랜딩이 실제 논문 페이지까지 안정적으로 도달하는지 검사합니다.
2. `parallel_download.py`
   랜딩 확인과 PDF 다운로드를 한 번에 수행합니다.

이 문서는 실제 사용 예시, 필요한 입력 형식, 기본 세팅, 다운로드 전략, bot-detection 회피 방식, domain별 특수 전략만 정리합니다.

## 설치

```bash
cd /Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download
python3 -m pip install -r requirements.txt
```

Chrome/Chromium이 필요합니다. PATH에서 자동으로 찾지 못하면 `CHROME_PATH`를 지정하세요.

```bash
command -v google-chrome || command -v google-chrome-stable || command -v chromium || command -v chromium-browser
ls "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
```

## 필요한 입력과 형식

### 1. 다운로드용 CSV

`parallel_download.py --doi_path ...`를 사용할 때는 아래 컬럼을 권장합니다.

필수:

- `doi`
- `open_access`

강력 권장:

- `publisher`
- `pdf_url`
- `title`

주의:

- 현재 다운로드 파이프라인은 `open_access` 컬럼을 실제 경로 분기에 사용합니다.
- 따라서 `doi`만 넣으면 안 되고, 최소한 `open_access`까지는 있어야 합니다.
- `open_access` 값은 `True` / `False` 형태를 권장합니다.

예시:

```csv
doi,publisher,pdf_url,open_access,title
10.1016/j.scitotenv.2024.172816,Elsevier,,False,Environmental assessment title
10.1038/s41467-023-41868-5,Nature,https://www.nature.com/articles/s41467-023-41868-5.pdf,True,Nature example title
10.1109/JEDS.2023.3253137,IEEE,https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=10061582,True,IEEE example title
```

컬럼 설명:

- `doi`: DOI 원문 문자열
- `publisher`: 퍼블리셔 이름 또는 힌트. 전략 선택 정확도를 높입니다.
- `pdf_url`: 이미 알고 있는 PDF 또는 viewer URL. OA 논문에서 특히 유용합니다.
- `open_access`: `True`면 `Open_Access/`, `False`면 `Closed_Access/` 아래로 저장됩니다.
- `title`: landing 진단과 일부 publisher 보조 복구에 도움을 줍니다.

### 2. 랜딩 검사 입력

`landing_access_repro.py --input ...`는 보통 같은 CSV를 그대로 사용할 수 있습니다.

landing-only 검사에서는 `doi`만 있어도 돌아가지만, 아래 컬럼이 같이 있으면 분류 정확도가 더 좋아집니다.

- `publisher`
- `title`
- `pdf_url`

### 3. 검색 기반 실행

CSV가 없으면 `parallel_download.py --query ...`로 OpenAlex 검색 후 다운로드할 수 있습니다.

이 경우 내부에서 CSV를 생성해 같은 파이프라인으로 내려갑니다.

OpenAlex 검색 단계 주의:

- 현재 search는 `type:article`만 수집합니다.
- SSRN, Zenodo, Figshare 같은 repository DOI가 잡힌 work라도, `title + first author + year` 기준으로 published work가 강하게 매칭되면 검색 단계에서 published DOI로 치환합니다.
- 이때 원래 repository DOI는 버리지 않고 CSV의 `original_doi` 컬럼에 같이 남깁니다.
- 반대로 published 후보가 약하면 repository DOI를 그대로 유지합니다.

다운로드 없이 OpenAlex 검색 결과 CSV만 먼저 보고 싶다면, 현재는 별도 CLI 대신 `openalex_search.main_search()`를 직접 호출하는 방식이 가장 간단합니다.

## 기본 세팅

### 다운로드 기본값

`parallel_download.py` 기본값:

- `max_workers=1`
- `after-first-pass=stop`
- `precheck-landing=0`
- `abort-on-landing-block=1`
- `runtime_preset=auto`
- `headless=None`
- `deep_retry_headless=None`
- `output_dir=outputs/paper_download_run`
- `pdf_output_dir=None`
- `publisher_cooldown_sec=7.0`
- `global_start_spacing_sec=1.5`
- `jitter_min_sec=0.7`
- `jitter_max_sec=1.8`

실제 해석:

- `runtime_preset=auto`는 기존 local desktop 동작을 유지하되, display가 없는 Linux에서는 `linux_server`로 해석합니다.
- `headless=None`이면 `PDF_BROWSER_HEADLESS` 환경변수를 따릅니다.
- 환경변수도 없으면 local desktop 기준으로 `headful`로 동작합니다.
- `linux_server`로 해석되면 headful 요청이 들어와도 headless로 강제합니다.
- `linux_cli_seeded` preset은 `/docs/linux_seed_profile_setup.md` 기준 Linux seeded profile root를 `persistent_profile_dir`로 받아, stateful 세션에서 macOS 시스템 프로필 대신 그 경로를 사용합니다.
- `deep_retry_headless=None`이면 1차 패스의 `headless` 값을 그대로 따릅니다.
- `pdf_output_dir`를 생략하면 `pdfs/<run_name>/`를 자동 사용합니다.
- `abort-on-landing-block=1`이 기본이라, landing에서 `captcha/challenge/block`가 보이면 즉시 중단합니다.

### 랜딩 검사 기본값

`landing_access_repro.py` 기본값:

- `input=ready_to_download.csv`
- `workers=1`
- `headless=0`
- `timeout_sec=15`
- `per_doi_deadline_sec=45`
- `max_nav_attempts=2`
- `probe_page_mode=reuse_page`
- `capture_fail_screenshot=0`
- `profile_mode=auto`
- `profile_name=Default`
- `persistent_profile_dir=outputs/.chrome_user_data`

실제 해석:

- 기본 랜딩 검사는 local desktop 안정성 기준으로 `headful + single worker`입니다.
- 단, `linux_cli_seeded` 또는 display 없는 Linux에서는 `headless`로 강제됩니다.
- 실패 스크린샷은 기본적으로 저장하지 않고, HTML/JSON 진단 위주로 남깁니다.

### Linux 서버 preset

Linux 서버에서는 `/docs/linux_seed_profile_setup.md`를 기준으로 profile을 준비한 뒤, 아래처럼 `linux_cli_seeded` preset을 쓰는 것이 기준 경로입니다.

```bash
python3 -u parallel_download.py \
  --runtime-preset linux_cli_seeded \
  --persistent-profile-dir /path/to/linux_chrome_user_data_seed \
  --profile-name Default \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --non-interactive
```

중요:

- `linux_cli_seeded`는 GUI/headful을 허용하지 않습니다.
- `persistent_profile_dir`는 tar를 푼 최상위 `user-data-dir` root여야 하며, 최소 `Default/Preferences`가 있어야 합니다.
- stateful 세션이 필요한 DOI만 seeded profile clone을 쓰고, 나머지 흐름은 기존 `local_mac` baseline 제어 흐름을 유지합니다.

## 실제 사용 예시

### 1. 다운로드 전 OpenAlex 검색 CSV만 만들 때

현재 `openalex_search.py`는 독립 CLI 인자를 직접 받지 않으므로, 아래처럼 Python one-liner로 `main_search()`만 호출하는 방식이 가장 안전합니다.

```bash
python3 - <<'PY'
from openalex_search import main_search

csv_path = main_search(
    pdf_save_dir="outputs/search_only_run",
    csv_name="Searched_DOIs.csv",
    query='("argyrodite*" OR "Li6PS5Cl" OR "Li6PS5Br" OR "Li6PS5I" OR "Li6PS5X") AND ("Li-ion" OR "Lithium-ion" OR "Lithium" OR "solid-state electrolyte*" OR "solid state electrolyte*" OR "solid electrolyte*" OR "SSE*" OR "all-solid-state" OR "ASSB*" OR "solid-state batter*" OR "solid state batter*" OR "SSB*" OR "ionic conduct*")',
    max_num=5000,
    citation_percentile=0.99,
)
print(csv_path)
PY
```

산출물:

- `outputs/search_only_run/Searched_DOIs.csv`

추가로 확인할 수 있는 컬럼:

- `original_doi`: 원래 OpenAlex가 준 DOI
- `doi_resolution_method`: `none`, `location`, `title_match`
- `doi_resolution_confidence`: published DOI 치환 신뢰도
- `resolved_from_ssrn`: SSRN DOI가 출판본 DOI로 바뀌었는지 여부
- `resolved_from_repository`: repository DOI가 출판본 DOI로 바뀌었는지 여부
- `original_source_type`: 원래 OpenAlex source type

### 2. 랜딩만 테스트할 때

다운로드 없이 DOI가 실제 논문 랜딩까지 가는지만 확인합니다.

```bash
python3 -u landing_access_repro.py \
  --input ready_to_download.csv \
  --workers 1 \
  --headless 1 \
  --timeout-sec 15 \
  --per-doi-deadline-sec 45 \
  --output-jsonl outputs/landing_access_repro.jsonl \
  --report outputs/landing_access_repro_report.json \
  --report-md outputs/landing_access_repro_report.md
```

### 3. 랜딩부터 다운로드까지 한꺼번에 테스트할 때

`precheck-landing 0`이면 별도 선검사 없이, 실제 다운로드 과정 안에서 landing 상태도 같이 기록합니다.

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --headless 1 \
  --precheck-landing 0 \
  --abort-on-landing-block 1 \
  --after-first-pass stop \
  --output_dir outputs/run_all_in_one \
  --pdf_output_dir pdfs/run_all_in_one \
  --non-interactive
```

### 4. 랜딩을 먼저 통과한 DOI만 다운로드할 때

`precheck-landing 1`이면 landing-only 검사 결과 중 성공한 DOI만 다운로드 큐에 넣습니다.

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --headless 1 \
  --precheck-landing 1 \
  --abort-on-landing-block 1 \
  --after-first-pass stop \
  --output_dir outputs/run_with_precheck \
  --pdf_output_dir pdfs/run_with_precheck \
  --non-interactive
```

### 5. Open access만 테스트할 때

현재는 `open access only` 전용 옵션이 없으므로, `open_access=True` 행만 담은 CSV를 따로 만들어 넣는 방식이 가장 안전합니다.

OA-only CSV 생성 예시:

```bash
python3 - <<'PY'
import pandas as pd
df = pd.read_csv('ready_to_download.csv')
oa = df[df['open_access'] == True].copy()
oa.to_csv('ready_to_download_oa_only.csv', index=False)
print(len(oa))
PY
```

실행:

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download_oa_only.csv \
  --max_workers 1 \
  --headless 1 \
  --precheck-landing 0 \
  --after-first-pass stop \
  --output_dir outputs/run_oa_only \
  --pdf_output_dir pdfs/run_oa_only \
  --non-interactive
```

### 6. Headless를 끄고 테스트할 때

기본적으로는 local desktop 안정성 확인에 적합한 설정입니다.

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --headless 0 \
  --precheck-landing 0 \
  --after-first-pass stop \
  --output_dir outputs/run_headful \
  --pdf_output_dir pdfs/run_headful \
  --non-interactive
```

### 7. Headless를 켜고 테스트할 때

서버 또는 batch 실행용 기본 예시입니다.

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --headless 1 \
  --precheck-landing 0 \
  --after-first-pass stop \
  --output_dir outputs/run_headless \
  --pdf_output_dir pdfs/run_headless \
  --non-interactive
```

### 8. Retry 모드를 끄고 테스트할 때

1차 패스만 보고 끝냅니다.

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --headless 1 \
  --after-first-pass stop \
  --output_dir outputs/run_no_retry \
  --pdf_output_dir pdfs/run_no_retry \
  --non-interactive
```

### 9. Retry 모드를 켜고 테스트할 때

1차 실패건만 deep retry를 추가로 수행합니다.

```bash
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --headless 1 \
  --deep-retry-headless 1 \
  --after-first-pass deep \
  --output_dir outputs/run_with_retry \
  --pdf_output_dir pdfs/run_with_retry \
  --non-interactive
```

의미:

- 1차 패스는 `headless`
- deep retry도 `headless`
- 1차에서 실패한 논문만 2차로 다시 시도

## 결과 파일과 경로 규칙

상대 경로를 쓰면 아래처럼 정리됩니다.

- `--output_dir run_x` -> `outputs/run_x/`
- `--pdf_output_dir` 생략 -> `pdfs/run_x/`

주요 산출물:

- `<output_dir>/openalex_search_results_parallel.csv`
- `<output_dir>/failed_papers.csv`
- `<output_dir>/failed_papers.jsonl`
- `<output_dir>/download_attempts.jsonl`
- `<output_dir>/download_attempts_summary.json`
- `<output_dir>/summary.json`
- `<output_dir>/metadata/Open_Access/*.json`
- `<output_dir>/metadata/Closed_Access/*.json`
- `<output_dir>/Open_Access/logs/download_log_*.txt`
- `<output_dir>/Closed_Access/logs/download_log_*.txt`
- `<output_dir>/**/logs/screenshots/final_fail_capture_*.png`
- `<pdf_output_dir>/Open_Access/*.pdf`
- `<pdf_output_dir>/Closed_Access/*.pdf`

`precheck-landing 1`일 때만 추가 생성:

- `<output_dir>/landing_precheck/landing_input.csv`
- `<output_dir>/landing_precheck/landing_results.jsonl`
- `<output_dir>/landing_precheck/landing_report.json`
- `<output_dir>/landing_precheck/landing_report.md`

## 실험 보고

실측 benchmark와 실패 사례 후속 분석은 루트 `README`에 적지 않고 `experiment/` 아래에 분리합니다.

- 실험 인덱스: `experiment/README.md`
- 100건 랜덤 benchmark 보고: `experiment/benchmark_random100_seed20260311.md`

## 다운로드 전략

`parallel_download.py`의 1차 패스 순서는 아래와 같습니다.

1. `Sci-Hub`
2. `direct OA (CFFI)`
3. `publisher API`
4. `DrissionPage` 브라우저 다운로드

브라우저 단계는 다시 아래 순서로 동작합니다.

1. DOI 또는 article URL landing
2. landing에서 `captcha/challenge/block/access-rights` 감지
3. PDF 후보 탐색
   - `citation_pdf_url` 메타
   - 버튼 `href`
   - HTML 구조 분석
   - iframe/embed/object
   - 필요 시 `doi.org`에 남은 랜딩을 최종 article URL로 재해석
4. 버튼 클릭 또는 viewer 진입
5. direct-download 감지
   - DOI 진입 직후
   - PDF 버튼 클릭 직후
   - navigation 직후
   - `page.download(...)` 반환 결과
6. 필요 시 navigation / requests / JS / cookie-aware CFFI fallback

실무적으로는 아래처럼 이해하면 됩니다.

- OA 논문은 `pdf_url`이나 direct PDF가 있으면 빠르게 끝냅니다.
- OpenAlex search에서 SSRN/Zenodo/Figshare 같은 repository work가 출판본 DOI로 치환되면, 이후 다운로드는 repository가 아니라 출판본 publisher 전략으로 진행합니다.
- high-friction publisher는 브라우저와 기존 세션을 사용해 사람 브라우저에 더 가깝게 접근합니다.
- challenge가 보이면 억지로 더 깊게 두드리기보다 빠르게 종료합니다.
- `after-first-pass deep`를 켜면 실패한 DOI만 한 번 더 재시도합니다.

## Bot-detection 회피 방식

핵심 원칙은 `사람 브라우저처럼 보이되, 위험한 상태는 빨리 감지하고 멈춘다`입니다.

적용 방식:

- 시스템 Chrome 또는 지속 프로필 재사용
  - 고마찰 publisher에서는 `profile-mode auto`로 시스템 Chrome 프로필을 우선 사용
  - 시스템 프로필이 없으면 `outputs/.chrome_user_data`를 fallback으로 사용
- Humanized browser 설정
  - UA 자동 선택
  - 언어, 창 크기, 브라우저 fingerprint 보정
  - 과도한 자동화 흔적을 줄이는 설정 사용
- 쿠키/동의 배너 자동 처리
  - consent, accept, continue, dismiss 계열 버튼 자동 클릭
- landing hard-fail 정책
  - `captcha/challenge/block`가 보이면 기본적으로 즉시 종료
  - 정상 article로 오인한 채 더 깊이 들어가지 않도록 차단
- publisher pacing
  - 같은 publisher를 연속으로 세게 치지 않도록 reorder
- publisher cooldown
- global start spacing
- random jitter
- direct download 우선
  - 가능한 경우 버튼 클릭 한 번 또는 direct PDF로 끝내고, 불필요한 추가 네비게이션을 줄임

## 운영 팁

- 대량 실행 전에는 먼저 5~10개 DOI로 샘플 검증을 하는 편이 안전합니다.
- 같은 publisher를 너무 자주 연속해서 재시도하지 않는 것이 중요합니다.

## Domain별 특별 전략

이 섹션은 실제 코드에서 분기하는 전략만 정리합니다.

### Elsevier

Elsevier는 가장 많은 예외 처리가 들어가 있습니다.

- `viewer-first` 전략
  - article page를 안정화한 뒤 `View PDF`를 누릅니다.
- DOI/PII 가드
  - 추천 논문, 관련 논문, 잘못 열린 탭을 피하기 위해 target DOI/PII가 맞는지 확인합니다.
- retrieve/interstitial 복구
  - `linkinghub` 또는 retrieve/interstitial에 걸리면 article URL로 복구를 시도합니다.
- article shell 복구
  - 본문 없는 shell page가 뜨면 article reopen을 시도합니다.
- auxiliary overlay 제거
  - `Reading Assistant`, 추천 패널, overlay/backdrop를 숨기거나 닫습니다.
- signed PDF viewer 회수
  - 새 탭으로 열린 signed PDF viewer를 잡아 cookie-aware CFFI로 실제 PDF를 받습니다.
- headless fresh-tab recovery
  - headless에서 dead `View PDF` 버튼이 뜨는 경우 새 탭 복구를 한 번 더 시도합니다.
- signed `papers.cfm/pdfft` navigation recovery
  - 1차 패스에서도 짧은 navigation recovery를 허용합니다.

### AIP / AVS

- `aip.scitation.org/doi/pdf/...`와 `avs.scitation.org/doi/pdf/...`는 direct PDF로 보이지만 실제로는 JS gate wrapper인 경우가 많습니다.
- 이 wrapper는 direct OA/API 단계에서 스킵하고 브라우저 landing으로 넘깁니다.
- 브라우저에서는 `pubs.aip.org/...article-pdf...` 경로를 우선 사용합니다.
- viewer 또는 article-pdf에서 새 탭/후보 URL을 수집하고, page cookie를 실은 recovery를 시도합니다.
- 즉, `wrapper 직접 요청`보다 `브라우저로 article-pdf까지 진입`하는 것이 핵심 전략입니다.

### IEEE

- `stamp.jsp`는 최종 PDF가 아닌 중간 viewer인 경우가 많습니다.
- stamp 페이지에서 iframe의 실제 PDF URL을 추출해 `ielx7/...pdf`로 교체합니다.
- 버튼 요소가 stale 되더라도 DownloadKit 결과를 직접 확정해 파일을 놓치지 않도록 처리합니다.
- 실험 기준으로는 현재 main downloader의 stamp/iframe 복구만으로 old-failure 샘플이 성공했기 때문에, title 기반 검색 복구는 아직 landing 진단 전용입니다.

### ACS

- 우선 `pubs.acs.org/doi/pdf/<doi>` direct 경로를 사용합니다.
- ACS는 중복 다운로드 트리거가 생기기 쉬워 1차 패스에서 일부 브라우저 다운로드 경로를 보수적으로 운용합니다.
- direct PDF가 막히면 브라우저 경로로 넘어갑니다.

### Wiley

- Wiley API 키가 있으면 TDM API를 우선 사용합니다.
- 실패 시 `pdfdirect` 또는 일반 `pdf` 경로로 fallback 합니다.

### Springer / Nature

- article landing 후 `Download PDF`가 곧바로 파일 저장으로 이어지는 경로를 공통 direct-download로 잡습니다.
- Nature 계열은 direct article PDF URL도 우선 시도합니다.

### IOP

- `iopscience.iop.org/article/<doi>/pdf` direct 경로를 우선 사용합니다.

### RSC

- `Download options` 위젯이 지연 로딩되는 경우가 있어 짧은 안정화 대기를 둡니다.

### Powdermat

- agreement 또는 비정상 article shell처럼 보이는 화면이 떠도 서버 HTML을 다시 읽어 article snapshot을 재평가합니다.
- `10.4150/...` DOI는 article HTML의 `journal_download('pdf', sid, filename)` 패턴을 해석해 `/upload/pdf/<filename>` 또는 `/upload/article/<filename>` direct URL로 복구합니다.
- 이 경로는 unknown publisher로 분류돼도 작동하며, headless 1차 패스에서 direct PDF로 바로 내려받도록 설계되어 있습니다.

### Ceramist

- `10.31613/...` DOI는 article HTML의 `citation_pdf_url`과 `journal_download("pdf", ...)` 패턴을 직접 읽어 article PDF URL로 복구합니다.
- 브라우저가 `doi.org`에 머물러도 request 기반 resolver로 direct PDF를 회수합니다.

### KJMM

- `10.3365/...` DOI는 `journal-by-doi.cshtml -> /kjmm/ArticleDetail/RD_R/<idx> -> /Common/pdf_viewer -> /openAccess/cart_openAccess2015.asp` 흐름을 해석해 최종 PDF URL을 찾습니다.
- 따라서 headless에서 `doi.org`에 머무는 케이스도 site-specific resolver로 복구합니다.

### SSRN

- Sci-Hub는 여전히 1순위로 시도합니다.
- 다만 official SSRN 경로는 challenge가 반복돼 ROI가 낮아, Sci-Hub 실패 후에는 `FAIL_SSRN_CHALLENGE`로 빠르게 종료하고 deep retry도 하지 않습니다.
- 검색 단계에서 published DOI로 치환된 work는 이 SSRN fast-fail 경로를 타지 않고, 출판본 DOI 기준 일반 downloader로 내려갑니다.

### Zenodo / Figshare / Repository DOI

- repository landing이 bot wall이나 unusual traffic에 자주 막히는 경우가 있어, 검색 단계에서 먼저 published article DOI로 치환할 수 있으면 그쪽을 우선합니다.
- 현재는 `title + first author + year`가 강하게 일치하고 journal source가 확인되는 경우에만 보수적으로 치환합니다.
- published 후보가 약하면 repository DOI를 그대로 유지하고, 다운로드 단계에서 일반 repository 전략을 따릅니다.

### 진단 전용 / 보류

- `old version`, `legacy`, `classic version` 링크를 눌러 깨진 shell을 우회하는 generic fallback은 현재 landing 진단에만 있습니다.
- 이번 검증에서는 이 fallback이 실제 다운로드 성공률을 바꾼 live 사례를 확보하지 못해 main downloader에는 아직 넣지 않았습니다.
- `10.7567/...` SSDM/atlas 계열은 단순 extractor보다 접근 제약이 먼저 관찰되어, 현재는 별도 site-specific downloader를 두지 않습니다.
