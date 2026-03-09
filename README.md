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

`landing_access_repro.py`는 DOI 랜딩 성공 여부만 검사하고, 실패 시 HTML/메타 로그를 남깁니다. 실패 스크린샷은 `--capture-fail-screenshot 1`일 때만 저장됩니다.
새 분류기는 `success_landing`, `challenge_detected`, `blank_or_incomplete`, `consent_or_interstitial_block`, `broken_js_shell`, `domain_mismatch`, `publisher_error`, `timeout`, `network_error`, `unknown_non_success` 상태를 기록합니다.
기본 probe 모드는 실험상 더 안정적이었던 `reuse_page`이며, `fresh_tab`은 A/B 비교용으로만 남겨두었습니다.
현재 기본 실행값은 로컬 랜딩 성공 우선 기준으로 `--workers 1`, `--headless 0`입니다.

먼저 서버에서 브라우저 경로를 확인하세요:

```bash
which google-chrome || which google-chrome-stable || which chromium-browser || which chromium || which chrome
```

```bash
python3 -u landing_access_repro.py \
  --input ready_to_download.csv \
  --max-dois 100 \
  --workers 1 \
  --startup-retries 3 \
  --timeout-sec 18 \
  --per-doi-deadline-sec 75 \
  --max-nav-attempts 2 \
  --publisher-cooldown-sec 7 \
  --global-start-spacing-sec 1.5 \
  --jitter-min-sec 0.7 \
  --jitter-max-sec 1.8 \
  --headless 0 \
  --no-sandbox 1 \
  --server-tuned 1 \
  --single-process 0 \
  --humanized-browser 1 \
  --assume-institution-access 1 \
  --profile-mode auto \
  --profile-name Default \
  --persistent-profile-dir outputs/.chrome_user_data \
  --worker-profile-root "${SLURM_TMPDIR:-/tmp/$USER}/landing_worker_profiles" \
  --clean-worker-profiles 1 \
  --capture-fail-artifacts 1 \
  --capture-fail-screenshot 0 \
  --capture-success-artifacts 1 \
  --capture-success-html 0 \
  --artifact-dir outputs/landing_access_artifacts \
  --zip-fail-artifacts 1 \
  --artifact-zip outputs/landing_access_failures.zip \
  --zip-success-artifacts 1 \
  --success-artifact-zip outputs/landing_access_successes.zip \
  --probe-page-mode reuse_page \
  --output-jsonl outputs/landing_access_repro.top100.jsonl \
  --report outputs/landing_access_repro.top100.report.json \
  --report-md outputs/landing_access_repro.top100.report.md
```

산출물:
- `outputs/landing_access_repro.*.jsonl`
- `outputs/landing_access_repro.*.report.json`
- `outputs/landing_access_repro.*.report.md`
- `outputs/landing_access_artifacts/fail/landing_fail_*.html`
- `outputs/landing_access_artifacts/fail/landing_fail_*.json`
- `outputs/landing_access_artifacts/fail/landing_fail_*.png` (`--capture-fail-screenshot 1`일 때만 생성)
- `outputs/landing_access_artifacts/success/landing_success_*.png`
- `outputs/landing_access_artifacts/success/landing_success_*.json`
- `outputs/landing_access_failures.zip` (실패 케이스 묶음, `manifest_fail.json` 포함)
- `outputs/landing_access_successes.zip` (성공 케이스 묶음, `manifest_success.json` 포함)

참고:
- 로컬 성공 우선 기본값은 `workers=1`, `headless=0`, `probe_page_mode=reuse_page`입니다.
- 실패 스크린샷 기본값은 `--capture-fail-screenshot 0`입니다. 실패 HTML/JSON만 남기고 싶으면 그대로 두고, PNG까지 필요할 때만 `1`로 켜면 됩니다.
- 워커 수는 안전상 최대 2로 제한됩니다. 같은 퍼블리셔는 전역 cooldown/jitter를 두고 순차적으로 시작합니다.
- DOI당 전체 시간은 `--per-doi-deadline-sec`로 하드캡되며 2분 미만으로 유지됩니다.
- 결과 JSONL/메타에는 `worker_idx`, `probe_page_mode`, `scheduled_start_ms`, `actual_start_ms`, `pacing_wait_ms`, `timing_breakdown`, `attempt_history`가 함께 기록됩니다.
- `challenge_detected`가 뜬 퍼블리셔는 같은 워커/세션에서 곧바로 다시 두드리지 않도록 추가 holdoff를 둡니다.
- Elsevier `linkinghub/.../retrieve/pii/...` + 제목 `Redirecting` 상태는 성공으로 보지 않습니다.
- 스크립트는 DOI/interstitial 페이지에서 추출 가능한 canonical/article URL이 있으면 같은 시도 안에서 1회만 더 따라가고, 그 뒤에도 retrieve/interstitial에 머무르면 비성공으로 기록합니다.
- `validate.perfdrive.com` 같은 벤더 스크립트 참조만으로는 challenge로 보지 않고, 실제 challenge UI/문구/URL 신호가 있을 때만 `challenge_detected`로 기록합니다.
- JS shell이 깨져 기사 본문이 렌더링되지 않으면 `broken_js_shell`, publisher metadata와 실제 기사 도메인 계열이 어긋나면 `domain_mismatch`로 별도 기록합니다.
- `institutional login`, `shibboleth`, `openathens` 같은 내비게이션 문구가 있어도 실제 article metadata와 본문이 충분하면 성공으로 인정합니다. 반대로 본문을 가리는 consent/login/paywall은 계속 비성공으로 유지합니다.

실험 비교 예시:

```bash
python3 -u landing_experiment_compare.py \
  --baseline-label baseline_20260308 \
  --baseline-report outputs/landing_exp_20260308/report.json \
  --baseline-results outputs/landing_exp_20260308/results.jsonl \
  --candidate classifier_only=outputs/landing_exp_20260308_batch1/report.json:outputs/landing_exp_20260308_batch1/results.jsonl \
  --candidate fresh_tab=outputs/landing_exp_20260308_batch2/report.json:outputs/landing_exp_20260308_batch2/results.jsonl \
  --output-json outputs/landing_exp_20260308_compare/comparison.json \
  --output-md outputs/landing_exp_20260308_compare/comparison.md
```

### 2-1. DOI CSV로 바로 다운로드

```bash
PDF_BROWSER_HEADLESS=1 \
CHROME_PATH=/usr/bin/google-chrome \
python3 -u parallel_download.py \
  --doi_path ready_to_download.csv \
  --max_workers 4 \
  --precheck-landing 0 \
  --output_dir outputs/run_ready_w4 \
  --after-first-pass stop \
  --non-interactive
```

랜딩 선확인이 필요하면 `--precheck-landing 1`을 추가합니다. 이 옵션은 다운로드 전에 `landing_access_repro.py`를 먼저 실행해 `SUCCESS_ACCESS`로 판정된 DOI만 다운로드 큐에 넣습니다.

### 2-2. 검색 + 다운로드(OpenAlex)

```bash
PDF_BROWSER_HEADLESS=1 \
python3 -u parallel_download.py \
  --query "('solid-state electrolyte' OR 'solid electrolyte') AND battery AND Li" \
  --max_num 300 \
  --max_workers 2 \
  --precheck-landing 0 \
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
  --precheck-landing 0 \
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

`--precheck-landing 1`일 때 추가 동작:

- 다운로드 전에 `landing_access_repro.py`를 별도 실행
- `SUCCESS_ACCESS`만 다운로드 대상으로 유지
- `FAIL_ACCESS_RIGHTS`는 landing 단계 권한 없음으로 별도 집계
- `<output_dir>/landing_precheck/` 아래에 precheck 입력/결과/report 저장

## 5. Bot-detection 회피 방식

핵심은 “사람 브라우저처럼 보이되, 실패 시 빠르게 포기”입니다.

- 브라우저 프로파일 보정
  - OS/헤드리스 환경에 맞는 UA 자동 선택, 언어/윈도우 크기, `AutomationControlled` 비활성화
  - `eager` load mode
  - `auto` 프로필 모드에서 고마찰 DOI(Elsevier/AIP/RSC/MDPI)는 시스템 프로필 우선 사용
  - 시스템 프로필이 없으면 `outputs/.chrome_user_data` 지속 프로필로 자동 fallback
- 동의/쿠키 배너 자동 처리
  - `accept/reject/continue`류 버튼 자동 클릭
- 접근 판정 로직
  - `detect_access_issue()`로 `FAIL_CAPTCHA/FAIL_BLOCK` 판단
  - 쿠키/동의 오버레이는 차단으로 오판하지 않도록 soft 처리
  - URL에 challenge 토큰(`__cf_chl_rt_tk`, `/cdn-cgi/challenge` 등)이 있으면 성공으로 보지 않음
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
- `PDF_BROWSER_HUMANIZED=1` : 과도한 `--disable-*` 플래그를 줄여 사람 브라우저 지문을 우선
- `PDF_BROWSER_UA_PLATFORM=linux|mac` : UA 플랫폼 강제(기본은 자동)
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
- `<output_dir>/landing_precheck/landing_input.csv` (`--precheck-landing 1`일 때만 생성)
- `<output_dir>/landing_precheck/landing_results.jsonl` (`--precheck-landing 1`일 때만 생성)
- `<output_dir>/landing_precheck/landing_report.json` (`--precheck-landing 1`일 때만 생성)
- `<output_dir>/landing_precheck/landing_report.md` (`--precheck-landing 1`일 때만 생성)
- `outputs/summary.json`
- `outputs/failed_papers.jsonl`

`outputs/summary.json`에는 아래 항목이 포함됩니다.

- `precheck_landing`
- `landing_precheck`
- `effective_rates`
  - `download_raw_success_rate`
  - `download_adjusted_success_rate`
  - `end_to_end_adjusted_success_rate`
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
- `--assume-institution-access 1`이면 soft login/paywall gate를 권한 실패 대신 bot/challenge 의심(`FAIL_BLOCK`)으로 분류합니다.
- 여전히 실패하면 먼저 `--workers 1`로 단건 검증 후 병렬 수를 올리세요.

---

실무 권장:

- 대량 다운로드는 `top10 -> top50 -> top100` 순으로 확장
- `--after-first-pass stop`으로 1차 결과를 먼저 확인한 뒤 deep retry 실행
