# Linux Headless Experiment Plan

## 목적

- Linux 서버 headless 환경에서 DOI -> publisher landing -> 실제 PDF 다운로드가 각각 어디까지 성립하는지 분리 측정한다.
- 현재 `local_mac`에서 성공하던 다운로드 유도 로직은 유지하고, Linux 서버 차이 때문에 필요한 부분만 검증한다.
- `ready_to_download.csv`를 기준으로 publisher-stratified 샘플을 구성해 특정 publisher 편향을 줄인다.

## 입력과 샘플링 기준

- 입력 원본: `ready_to_download.csv`
- 샘플 생성기: `experiment/build_linux_headless_suite.py`
- 생성 산출물:
  - `experiment/linux_headless_suite/pilot_sample.csv`
  - `experiment/linux_headless_suite/full_sample.csv`
  - `experiment/linux_headless_suite/suite_manifest.json`

### 그룹 기준

- canonical publisher group:
  - `acs`, `elsevier`, `cell`, `wiley`, `aip`, `nature`, `springer`, `rsc`, `iop`, `mdpi`, `ieee`, `aps`, `taylor_and_francis`
- `cell`은 generic Elsevier와 분리한다.
  - DOI token과 `cell.com` PDF 진입점을 함께 사용해 Cell-family를 따로 고른다.
- `springer`는 `link.springer.com` 흐름을 `nature.com`과 분리해 관찰한다.

### 샘플 bucket

- `landing_closed`
  - `pdf_url` direct handoff가 없고 OA가 아닌 경우. 랜딩과 권한/세션 의존성이 큰 케이스.
- `landing_oa`
  - direct handoff는 없지만 OA인 경우. 랜딩 성공 여부와 PDF 버튼/링크 후속 동작을 분리하기 좋다.
- `direct_pdf_oa`
  - 실제 PDF 진입점으로 보이는 `pdf_url`이 있는 OA 케이스. "landing 없이도 바로 다운로드 유도 가능한지" 확인한다.
- `direct_pdf_closed`
  - direct PDF 진입점은 있으나 access gate가 생길 수 있는 케이스.

### suite 구성

- `pilot` 13건
  - 각 major publisher 1건씩 + `rsc` 2건 + `cell` 2건
  - 목적: Linux seed profile, headless-only 실행, 결과 logging이 정상 동작하는지 빠르게 확인
- `full` 31건
  - `acs/elsevier/cell/wiley/aip/nature/rsc` 각 3건
  - `springer/iop/mdpi/ieee` 각 2건
  - `aps/taylor_and_francis` 각 1건
  - 목적: publisher별 landing/download 차이를 실제로 비교

## 실험 질문과 측정 항목

### Q1. Linux 서버에서 publisher landing이 실제로 되는가?

- 측정 도구: `landing_access_repro.py`
- 성공 기준:
  - `classifier_state in {success_landing, direct_pdf_handoff}`
  - expected publisher domain에 도달
  - challenge/interstitial/blank heuristics 미발생

### Q2. landing이 아니라 실제 다운로드까지 되는가?

- 측정 도구: `parallel_download.py`
- 성공 기준:
  - `download_status`가 success
  - 결과 CSV의 `experiment_download_bucket == download_success`
  - 실제 PDF 저장 경로와 metadata sidecar가 생성

### Q3. `local_mac` 대비 Linux 차이로 필요한 변경은 무엇인가?

- 비교 지점:
  - 브라우저 실행 파일 탐색 경로
  - seeded profile root 로딩
  - runtime profile clone 경로(`/tmp/...`)와 정리
  - headful 가정 제거 여부
  - download directory, artifact directory 절대경로 처리
  - session/cookie 재사용 source가 macOS system profile이 아니라 Linux seed clone인지
  - publisher별 동기화 타이밍과 blank/challenge 빈도 변화

## 분류 매트릭스

landing probe와 download run을 따로 수집한 뒤, 최종 요약에서 아래 버킷으로 합친다.

- `landing_success`
- `download_success`
- `landing_success_no_download`
- `challenge_or_interstitial`
- `blank_or_incomplete`
- `timeout_or_error`
- `environment_or_config_failure`
- `access_rights`
- `doi_not_found`
- `other_non_success`

판단 기준:

- landing probe는 `classifier_state`와 `reason_codes`를 사용한다.
- download run은 `parallel_download.py`가 기록하는 `experiment_landing_bucket`, `experiment_download_bucket`, `download_result_reason`, `download_result_stage`를 사용한다.
- 둘을 merge해 DOI별 `combined_bucket`을 만든다.

## 실행 절차

### 1. 샘플 재생성

```bash
python3 experiment/build_linux_headless_suite.py
```

### 2. pilot 준비 또는 실행

```bash
python3 experiment/run_linux_headless_suite.py \
  --suite pilot \
  --runtime-preset linux_cli_seeded \
  --execution-env linux_server \
  --persistent-profile-dir /path/to/linux_chrome_user_data_seed
```

실행까지 하려면 `--execute`를 추가한다.

### 3. full 실행

```bash
python3 experiment/run_linux_headless_suite.py \
  --suite full \
  --runtime-preset linux_cli_seeded \
  --execution-env linux_server \
  --persistent-profile-dir /path/to/linux_chrome_user_data_seed \
  --execute
```

runner가 생성하는 것:

- `outputs/linux_headless_suite_runs/<suite>_<timestamp>/run_suite.sh`
- `execution_manifest.json`
- landing stdout/stderr log
- download stdout/stderr log
- merged summary CSV/JSON/Markdown

## local_mac 대비 우선 검증 포인트

1. Linux seeded profile이 실제 runtime path에 연결되는가
   - `browser_session_source`가 `linux_seed_clone` 또는 동일 계열로 나와야 한다.
2. headless 강제가 끝까지 유지되는가
   - `runtime_preset=linux_cli_seeded`, `execution_env=linux_server`, `headless=true`
3. GUI 의존 동작이 남아 있지 않은가
   - Chrome smoke, landing probe, download run이 모두 headless-only로 기동돼야 한다.
4. landing과 download가 분리 관측되는가
   - landing 성공인데 download 실패인 DOI를 별도 bucket으로 확인할 수 있어야 한다.
5. direct PDF handoff와 landing-required 케이스가 모두 포함됐는가
   - generic Elsevier는 landing-heavy로, Cell/RSC/Nature/Wiley/MDPI 등은 direct-PDF 케이스를 포함한다.

## 우선순위별 후속 코드 검증

1. `parallel_download.py`의 `experiment_*_bucket`과 `download_result_*` 컬럼이 실제 실패 진단에 충분한지
2. Linux seed profile clone 실패 시 evidence/stage가 항상 환경 이슈로 분류되는지
3. publisher별로 headless에서 blank/incomplete가 늘어나는 경우 추가 wait/retry가 필요한지
4. Cell-family와 generic Elsevier가 같은 분기에서 오작동하지 않는지
5. `link.springer.com`과 `nature.com`이 동일 분류로 취급돼 누락되는 분기가 없는지

## 현재 준비 상태

- pilot/full suite CSV와 manifest 생성 완료
- Linux headless runner와 summary merger 추가 완료
- 실제 Ubuntu 서버에서의 end-to-end 실행은 아직 별도 검증 필요
