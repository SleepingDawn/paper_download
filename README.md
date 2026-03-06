# Paper Download Pipeline

DOI 목록을 입력받아 PDF를 다운로드하는 파이프라인입니다.  
`Sci-Hub -> direct OA(CFFI) -> publisher API -> Drission 브라우저` 순서로 시도하며, 실패 원인/로그/스크린샷을 함께 남깁니다.

## 1. 설치

```bash
cd /Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download
python3 -m pip install -r requirements.txt
```

서버(헤드리스) 환경이면 Chrome/Chromium 설치 후 `CHROME_PATH`를 지정하세요.

## 2. 빠른 실행 예시

### 2-0. 서버(Linux CLI)에서 랜딩만 검사 (다운로드 없음)

`landing_access_repro.py`는 DOI 랜딩 성공 여부만 검사하고, 실패 시 스크린샷/HTML/메타 로그를 남깁니다.

먼저 서버에서 브라우저 경로를 확인하세요:

```bash
which google-chrome || which google-chrome-stable || which chromium-browser || which chromium || which chrome
```

```bash
python3 -u landing_access_repro.py \
  --input ready_to_download.csv \
  --max-dois 100 \
  --workers 5 \
  --startup-retries 3 \
  --timeout-sec 20 \
  --headless 1 \
  --no-sandbox 1 \
  --server-tuned 1 \
  --single-process 0 \
  --profile-mode auto \
  --profile-name Default \
  --persistent-profile-dir outputs/.chrome_user_data \
  --worker-profile-root "${SLURM_TMPDIR:-/tmp/$USER}/landing_worker_profiles" \
  --clean-worker-profiles 1 \
  --capture-fail-artifacts 1 \
  --artifact-dir outputs/landing_access_artifacts \
  --output-jsonl outputs/landing_access_repro.top100.jsonl \
  --report outputs/landing_access_repro.top100.report.json
```

산출물:
- `outputs/landing_access_repro.*.jsonl`
- `outputs/landing_access_repro.*.report.json`
- `outputs/landing_access_artifacts/landing_fail_*.png`
- `outputs/landing_access_artifacts/landing_fail_*.html`
- `outputs/landing_access_artifacts/landing_fail_*.json`

### 2-1. DOI CSV로 바로 다운로드

```bash
PDF_BROWSER_HEADLESS=1 \
CHROME_PATH=/usr/bin/google-chrome \
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 4 \
  --output_dir outputs/run_ready_w4 \
  --after-first-pass stop \
  --non-interactive
```

### 2-2. 검색 + 다운로드(OpenAlex)

```bash
PDF_BROWSER_HEADLESS=1 \
python3 -u parallel_download.py \
  --query "('solid-state electrolyte' OR 'solid electrolyte') AND battery AND Li" \
  --max_num 300 \
  --max_workers 2 \
  --output_dir outputs/run_search_w2 \
  --after-first-pass stop \
  --non-interactive
```

### 2-3. 1차 실패 건 deep retry까지 수행

```bash
PDF_BROWSER_HEADLESS=1 \
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 4 \
  --output_dir outputs/run_ready_w4_deep \
  --after-first-pass deep \
  --non-interactive
```

## 3. 입력 CSV 형식

최소 `doi` 컬럼이 필요합니다.  
가능하면 아래 컬럼이 있으면 전략 선택이 정확해집니다.

- `doi`
- `publisher`
- `pdf_url`
- `open_access` (`True/False`)
- `title` (선택)

## 4. 현재 다운로드 전략(실행 순서)

`parallel_download.py` 기준 1차 패스 전략:

1. `Sci-Hub` (항상 1순위)
2. `direct OA CFFI` (`pdf_url`이 있으면 바로 시도)
3. `publisher API` (Elsevier API는 시간 절약을 위해 생략)
4. `DrissionPage` 브라우저 전략

Drission 내부 전략:

- DOI landing
- 차단/캡차/인증 요구 판정
- PDF 링크 탐색
  - `citation_pdf_url` 메타
  - 버튼 href
  - HTML 분석(regex/link/iframe)
- 도메인별 클릭 전략
  - Elsevier: article -> viewer 2단계 클릭
  - 고차단/일반 도메인: PDF 버튼 1회 클릭 + 파일 감지
- 링크가 있으면 `page.download` 또는 navigation 다운로드
- 1차 패스는 빠른 경로만 사용하고 느린 fallback(requests/js/추가 CFFI)은 생략

## 5. Bot-detection 회피 방식

핵심은 “사람 브라우저처럼 보이되, 실패 시 빠르게 포기”입니다.

- 브라우저 프로파일 보정
  - 고정 UA, 언어/윈도우 크기, `AutomationControlled` 비활성화
  - `eager` load mode
  - `auto` 프로필 모드에서 고마찰 DOI(Elsevier/AIP/RSC/MDPI)는 시스템 프로필 우선 사용
  - 시스템 프로필이 없으면 `outputs/.chrome_user_data` 지속 프로필로 자동 fallback
- 동의/쿠키 배너 자동 처리
  - `accept/reject/continue`류 버튼 자동 클릭
- 접근 판정 로직
  - `detect_access_issue()`로 `FAIL_CAPTCHA/FAIL_BLOCK` 판단
  - 쿠키/동의 오버레이는 차단으로 오판하지 않도록 soft 처리
  - `validate user` 페이지는 우회 클릭 없이 차단으로 즉시 분류
  - `authenticate/password required` 페이지는 즉시 중단
- 도메인 특화 처리
  - Elsevier 추천 논문 오클릭 방지: DOI/PII 컨텍스트 가드
  - 약한 링크(`proceedings`, `toc` 등) 필터링

## 6. 시간 절약 정책

현재는 “논문 전체 하드캡”이 아니라 “각 액션의 대기 상한”을 둡니다.

- 무기한 대기 금지
  - `page.get(..., timeout=...)`
  - 파일 감지 대기, CFFI timeout, Sci-Hub 총 시간 제한
- 액션 간 불필요 sleep 최소화
  - 클릭 후 대기/파일 안정화 루프를 짧게 유지
- 1차 패스 fast-path
  - 느린 우회 전략은 deep 모드에서만 수행

## 7. 주요 환경변수

- `PDF_BROWSER_HEADLESS=1` : 헤드리스 실행
- `PDF_BROWSER_NO_SANDBOX=1` : 서버 컨테이너에서 필요할 수 있음
- `CHROME_PATH=/path/to/chrome` : 브라우저 실행 파일 경로
- `PDF_BROWSER_PROFILE_MODE=auto|temp` : 브라우저 프로필 전략 (`auto` 권장)
- `PDF_BROWSER_PROFILE_NAME=Default` : 시스템 프로필 이름
- `PDF_BROWSER_PERSISTENT_PROFILE_DIR=outputs/.chrome_user_data` : 시스템 프로필 미존재 시 fallback 프로필 경로
- `PDF_ACTION_MAX_WAIT_S=60` : 액션 단위 최대 대기(초)
- `SCIHUB_MAX_TOTAL_S=20` : Sci-Hub 전체 시도 예산(초)
- `DIRECT_OA_CFFI_TIMEOUT_S=12` : direct OA CFFI timeout(초)
- `PDF_IEEE_IFRAME_WAIT_S=8` : IEEE stamp iframe 대기(초)
- `PDF_PIPELINE_MODE=baseline|candidate` : CFFI 파이프라인 모드

## 8. 산출물

실행 후 아래 파일이 생성됩니다.

- `<output_dir>/openalex_search_results_parallel.csv`
- `<output_dir>/failed_papers.csv`
- `<output_dir>/Open_Access/*.pdf`
- `<output_dir>/Closed_Access/*.pdf`
- `<output_dir>/Open_Access/logs/download_log_*.txt`
- `<output_dir>/Closed_Access/logs/download_log_*.txt`
- `<output_dir>/**/logs/screenshots/final_fail_capture_*.png`
- `outputs/summary.json`
- `outputs/failed_papers.jsonl`
- `outputs/download_attempts.jsonl`
- `outputs/download_attempts_summary.json`

## 9. 트러블슈팅

### `[CFFI] 실패 reason=FAIL_WRONG_MIME, status=200`

의미:

- HTTP는 성공(200)했지만, 응답이 PDF가 아니라 HTML/게이트 페이지
- 보통 쿠키 페이지, 인증 페이지, 차단 페이지

대응:

- 브라우저(Drission) 경로로 넘어가 버튼 클릭 기반 다운로드 시도
- 인증 요구(`password/authenticate`)는 즉시 실패 처리

### 창/메모리 사용량이 큰 경우

- `PDF_BROWSER_HEADLESS=1`
- `max_workers`를 1~4 수준으로 제한
- 서버 환경이면 `PDF_BROWSER_NO_SANDBOX=1` 검토

### `BrowserConnectError`가 날 때

- 워커 충돌 가능성이 높으므로:
  - `--worker-profile-root`를 지정해 워커별 프로필 분리
  - `--clean-worker-profiles 1`로 stale lock 제거
  - `--startup-retries 3` 이상 사용
- 실행 전에 Chrome 스모크 체크를 수행하며, 실패하면 `outputs/landing_access_artifacts/chrome_smoke_fail.json`에 stderr를 남깁니다.
- 스모크 체크는 `single-process` fallback을 자동 시도합니다.
- 여전히 실패하면 먼저 `--workers 1`로 단건 검증 후 병렬 수를 올리세요.

---

실무 권장:

- 대량 다운로드는 `top10 -> top50 -> top100` 순으로 확장
- `--after-first-pass stop`으로 1차 결과를 먼저 확인한 뒤 deep retry 실행
