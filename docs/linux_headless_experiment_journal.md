# Linux Headless Landing Experiment Journal

## 1. 개요

이 문서는 `local_mac` 기준 로직을 Linux 서버 headless 환경으로 옮기면서 수행한 landing/download 실험, 운영 스크립트 정비, 분류기 보정, DOI 재시도 억제, publisher별 landing 전략 조정 작업을 한곳에 정리한 실험 저널이다.

핵심 목적은 아래 세 가지였다.

- Linux 서버 headless 환경에서 `DOI -> publisher landing -> 다운로드`가 어디까지 안정적으로 성립하는지 분리 측정
- `local_mac`에서 동작하던 합리적인 로직은 최대한 보존하고, Linux/headless 차이 때문에 깨지는 층만 보수
- 반복 실패 DOI를 무의미하게 다시 치지 않도록 실험 설계와 runner를 재구성

이 저널은 다음 근거를 우선 사용한다.

- 저장소 문서: `docs/linux_seed_profile_setup.md`, `experiment/linux_headless_experiment_plan.md`, `experiment/README.md`
- 저장소 코드/스크립트: `experiment/*.py`, `scripts/*.sh`, `landing_access_repro.py`, `landing_classifier.py`, `tools_exp.py`, `parallel_download.py`
- git 이력: `codex/linux_exp` 브랜치 최근 커밋
- 저장소 내부 산출물: `outputs/` 아래 historical run 디렉터리와 seed/profile 흔적
- 외부 artifact bundle 근거: 사용자 제공 `pilot_20260314_210007_bundle.tar.gz`, `pilot_20260314_213217_bundle.tar.gz`, `pilot_20260314_232524_bundle.tar.gz`, `pilot_20260315_000015_bundle.tar.gz`, `pilot_20260315_004518_bundle.tar.gz`, `pilot_20260315_012307_bundle.tar.gz`, `pilot_20260315_014015_bundle.tar.gz`

주의:

- 외부 bundle은 저장소 밖 artifact이므로, 본문에서는 "외부 bundle 근거"로 명시한다.
- 사용자 요청의 세부 문구는 대화 기준으로 복원한 부분이 있으므로, 저장소 파일에 직접 남지 않은 항목은 "대화 요청 기준"이라고 표시한다.

## 2. 사용자 요청과 제약

아래 항목은 대화 요청 기준으로 여러 단계에 걸쳐 반복적으로 명시됐다.

- Linux 서버, no GUI, headful 의존 제거
- `local_mac`에서 이미 타당한 로직은 보존하고, 필요한 부분만 Linux/headless에 맞게 수정
- landing 신뢰성을 우선 검증하고, landing과 download를 분리 관찰
- "무언가 로드됨"을 성공으로 보지 말고, 실제 publisher article/homepage 도달 여부를 엄격히 판정
- Sci-Hub가 publisher-native 성공률을 가리지 않도록 결과를 분리 집계
- 2024+ 최근 DOI를 main validation set으로 우선 사용
- 같은 DOI를 반복 타격하지 말고 retry protection, cooldown, DOI rotation을 도입
- 서버 경로/환경값을 먼저 수집하고, 추측하지 말 것
- SSH 세션이 끊겨도 계속 도는 background 실행, persistent log, artifact collector를 제공할 것
- 실험 제안 시 항상 아래를 함께 적을 것
  - 어떻게 실행하는지
  - 중간 로그를 어떻게 보는지
  - 끝났는지 어떻게 확인하는지
  - 결과를 어떻게 전달하는지
- landing 쪽에 넣은 의미 있는 patch는 main download path에도 같이 반영해 둘을 align할 것

## 3. 관련 문서, 스크립트, 근거 파일

### 저장소 문서

- `docs/linux_seed_profile_setup.md`
  - Linux seed profile 준비, warm-up, packaging, 서버 복사/검증 절차
- `experiment/linux_headless_experiment_plan.md`
  - publisher-stratified suite, 2024+ recent cohort, Sci-Hub confound 분리, landing/download bucket 정의
- `experiment/README.md`
  - Linux headless 실험 폴더 구조와 운영 스크립트 요약

### 운영 스크립트

- `scripts/prepare_linux_server_env.sh`
  - `SEED_PROFILE`, `PROFILE_NAME`, `CHROME_PATH`, `PYTHON_BIN`, `RUNS_ROOT`, `LOGS_ROOT`, `COLLECT_ROOT`를 `config/linux_server.env`로 기록
- `scripts/run_linux_suite_bg.sh`
  - `nohup` 기반 background launcher
- `scripts/check_linux_suite_status.sh`
  - PID, `execution_manifest.json`, stage 로그, stderr tail 확인
- `scripts/collect_linux_suite_artifacts.sh`
  - 분석용 tar.gz bundle 생성
- `scripts/check_linux_seed_profile.py`
  - seed profile 구조 검증

### 실험 설계/실행 코드

- `experiment/build_linux_headless_suite.py`
- `experiment/linux_headless_suite_lib.py`
- `experiment/run_linux_headless_suite.py`
- `experiment/summarize_linux_headless_suite.py`

### landing/download 핵심 코드

- `landing_access_repro.py`
- `landing_classifier.py`
- `tools_exp.py`
- `parallel_download.py`

## 4. 실험 타임라인

아래 순서는 git 이력, 문서, 외부 bundle 시점, 현재 코드 상태를 합쳐 복원했다.

| 시점 | 근거 | 요약 |
| --- | --- | --- |
| `0608e8d` (`local_mac`) | git log | Linux seeded profile 문서가 `local_mac` 계열에 먼저 존재. 이후 Linux 이식의 기준점 역할 |
| `84ef0f7` | git log | `local_mac` 로직을 Linux로 적응시키는 초기 포팅 작업 |
| `b102414`, `a8fb665` | git log | Linux 실험 가이드 및 seed profile 검증 절차 추가 |
| `43b6976`, `5c3b981` | git log + `scripts/prepare_linux_server_env.sh` | 환경 변수 선수집, `config/linux_server.env` 기반 운영 정착 |
| `41b031a`, `58b225d` | git log | 최근 실험 산출물 리뷰 및 landing/download 문제 보정 |
| `e254cfb` | git log + `experiment/linux_headless_suite_lib.py` | DOI retry protection, attempt ledger, controlled retry/skip 도입 |
| `8b57c7b` | git log + Elsevier 관련 코드 | Elsevier landing flow를 direct article 진입에서 official retrieve/handoff 중심으로 재구성 |
| `cee09cd` | git log + `parallel_download.py`, `tools_exp.py` | landing 쪽 의미를 main download path에도 정렬 |
| `00d58ca` | git log | seed profile output 충돌 완화, pull 장애 해소 |
| 외부 bundle `20260314`~`20260315` | 사용자 제공 artifact | Elsevier/AIP/Springer misclassification, blank screenshot, retry pressure, shell recovery, AIP no-retry, AIP DOI-first 진입 문제를 실제 run 단위로 분석 |

## 5. 주요 시도와 패치

### 5.1 Linux seed profile 도입

- 가설
  - macOS system profile 의존을 제거하고 Linux에서 생성한 seeded profile을 복제해 쓰면 headless server에서도 세션/쿠키를 더 일관되게 재사용할 수 있다.
- 구현
  - `docs/linux_seed_profile_setup.md`
  - `scripts/check_linux_seed_profile.py`
  - `scripts/build_linux_seed_bundle.sh`
  - `scripts/package_linux_seed_profile.py`
- 기대 효과
  - 서버에서 `persistent-profile-dir`를 명시적으로 관리하고, 런타임 clone을 안전하게 재사용
- 관찰 결과
  - seed profile 구조 검증과 env 준비 절차는 정착
  - 다만 Elsevier/AIP는 seed profile만으로 challenge가 사라지지 않음
- 배운 점
  - seeded profile은 필요조건일 수는 있어도 충분조건은 아님

### 5.2 원격 실행, persistent log, artifact 수집 표준화

- 가설
  - SSH 세션 종료에 영향을 받지 않는 launcher와 표준 로그/수집 경로가 있어야 실험 반복과 분석이 가능하다.
- 구현
  - `scripts/prepare_linux_server_env.sh`
  - `scripts/run_linux_suite_bg.sh`
  - `scripts/check_linux_suite_status.sh`
  - `scripts/tail_linux_suite_logs.sh`
  - `scripts/collect_linux_suite_artifacts.sh`
  - `config/linux_server.env.example`
- 기대 효과
  - 서버 환경값 재사용, 백그라운드 실행, 결과 회수의 반복 가능성 확보
- 관찰 결과
  - 현재 운영 워크플로우는 `prepare -> run -> check/tail -> collect`로 정리됨
  - `execution_manifest.json`, `run_suite.sh`, root launcher log, bundle tar.gz가 공통 산출물로 자리잡음
- 배운 점
  - 코드 수정 이전에 환경/경로/로그 표준화가 먼저 되어야 실패 분석 비용이 줄어든다

### 5.3 publisher-stratified suite와 2024+ recent cohort

- 가설
  - publisher 다양성을 유지하되 2024+ DOI를 우선하면, Sci-Hub confound를 줄이고 publisher-native 후속 로직을 더 직접적으로 볼 수 있다.
- 구현
  - `experiment/linux_headless_experiment_plan.md`
  - `experiment/build_linux_headless_suite.py`
  - `experiment/linux_headless_suite_lib.py`
  - `experiment/linux_headless_suite/pilot_sample.csv`
  - `experiment/linux_headless_suite/full_sample.csv`
  - `experiment/linux_headless_suite/suite_manifest.json`
- 기대 효과
  - recent-primary vs legacy-fallback를 분리 집계
  - RSC/Cell 포함 유지
- 관찰 결과
  - 계획 문서와 suite 산출물에 `validation_cohort`, `scihub_confound_risk`, `selection_reason`가 기록됨
- 배운 점
  - 실험 설계가 바뀌지 않으면 코드 개선 효과가 Sci-Hub 성공률에 가려질 수 있다

### 5.4 Sci-Hub confound 분리

- 가설
  - `download success`를 하나로 보면 publisher-native 성공과 Sci-Hub-assisted 성공이 섞여 해석이 왜곡된다.
- 구현
  - `parallel_download.py`
  - `experiment/summarize_linux_headless_suite.py`
  - `experiment/linux_headless_experiment_plan.md`
- 기대 효과
  - `publisher_native_download`, `scihub_assisted_download`, `download_success_unknown`, `landing_success_no_download` 분리
- 관찰 결과
  - 현재 summary JSON/CSV는 source category와 combined bucket을 분리 기록
- 배운 점
  - 실험 성공 정의를 바꾸지 않으면 landing/download 개선 여부를 잘못 읽게 된다

### 5.5 DOI retry protection과 rotation

- 가설
  - 같은 DOI를 반복해서 치는 구조가 challenge/rate-limit/IP risk를 키우며, 근본 원인을 더 흐린다.
- 구현
  - `experiment/linux_headless_suite_lib.py`
  - `experiment/build_linux_headless_suite.py`
  - `experiment/run_linux_headless_suite.py`
  - `experiment/summarize_linux_headless_suite.py`
- 핵심 필드
  - `prior_attempt_state`, `prior_attempt_count`, `prior_success_count`, `prior_hard_block_count`
  - `retry_protection_action`, `retry_protection_reason`
  - `controlled_retry`, `skipped_due_to_retry_protection`
- 기대 효과
  - builder가 fresh DOI를 우선 고르고, runner가 stale sample도 다시 필터링
- 관찰 결과
  - 코드상 ledger/skip/controlled retry 로직은 도입됨
  - 현재 로컬 워크스페이스에는 `outputs/linux_headless_suite_attempt_ledger.jsonl` 파일이 보이지 않으므로, 실제 누적 ledger 상태는 별도 확인 필요
- 배운 점
  - 반복 실패를 publisher bug로 보기 전에 실험 설계 자체가 같은 DOI를 재타격하고 있지 않은지 먼저 봐야 한다

### 5.6 Elsevier landing 재구성

- 초기 가설
  - direct `sciencedirect article/pii` 진입이 너무 공격적이어서 challenge/interstitial을 유발한다.
- 재사용한 `local_mac` 로직
  - retrieve/handoff/canonical normalize
  - retrieve 페이지 DOI 클릭 복구
  - article shell reopen
  - latest-tab adoption과 hydrate wait
- 구현 파일
  - `tools_exp.py`
  - `landing_access_repro.py`
  - `landing_classifier.py`
  - `parallel_download.py`
  - `experiment/summarize_linux_headless_suite.py`
- 핵심 변경
  - official `doi.org -> linkinghub.elsevier.com/retrieve/...` 경로를 먼저 풀고 browser entry 전략에 반영
  - shell page와 real article page를 분리
  - `entry_strategy`, `entry_browser_url`, `entry_handoff_url`, `entry_preflight_issue`, `landing_shell_recovery_*` 기록
  - 분류기가 shell-only page를 진짜 landing으로 세지 않도록 보강
- 외부 bundle 관찰
  - `pilot_20260314_210007` / `pilot_20260314_213217`:
    - Elsevier 1건, Cell 2건이 challenge/interstitial
  - `pilot_20260315_000015`:
    - Elsevier 2건이 `success_landing`
  - `pilot_20260315_012307`:
    - Elsevier 1건이 `landing_success`, `publisher_native_download`
- 배운 점
  - Elsevier는 landing layer가 핵심이며, direct article URL 진입과 shell page 오판이 가장 큰 초기 문제였다

### 5.7 Springer/`10.1007_` 오분류 수정

- 문제
  - 외부 bundle `pilot_20260314_232524`의 `10.1007/s12598-024-02864-w`는 실제로 Wiley article page에 landed 했는데 `domain_mismatch`로 실패 처리됐다.
- 증거
  - fail artifact JSON:
    - `final_url=https://onlinelibrary.wiley.com/doi/10.1007/s12598-024-02864-w`
    - `title=... Wiley Online Library`
    - `reason_codes=["content_populated","doi_match","domain_mismatch_article_like","expected_domain_mismatch","strong_meta_present"]`
- 구현
  - `landing_classifier.py`
- 기대 효과
  - legitimate cross-host landing을 `success_landing`으로 재분류
- 관찰 결과
  - 현재 classifier 코드에는 `reclassified_after_detector_fix`와 cross-host legitimate landing 처리 경로가 존재
- 남은 점
  - 이 수정이 실제 server rerun에서 다시 확인됐는지는 저장소 내 최신 bundle 근거가 부족하다 `[blocked]`

### 5.8 AIP blank screenshot 진단과 no-retry patch

- 초기 가설
  - blank screenshot이 wrong tab, stale handle, about:blank, renderer crash일 수 있다.
- 외부 bundle 증거
  - `pilot_20260315_004518`의 `10.1116/6.0003790`
    - `navigation_chain=pre_reset -> aip_resolve -> doi_get -> aip_recovery_1 -> aip_recovery_2`
    - `landing_recovery_attempted=true`
    - `landing_recovery_outcome=still_challenged_after_canonical_entry`
    - 최종 `challenge_detected`
  - `pilot_20260315_012307`의 `10.1116/6.0003847`
    - `entry_strategy=aip_official_doi_resolve`
    - `entry_browser_url`은 canonical article-abstract
    - `landing_recovery_outcome=challenge_detected_no_retry`
    - `runtime_diagnostics.ready_state=complete`
    - `runtime_diagnostics.tab_state.total_tab_count=1`
    - `runtime_diagnostics.blank_screenshot_likely=true`
    - `runtime_diagnostics.blank_screenshot_reason=challenge_shell_unrendered_or_minimally_rendered`
- 구현
  - `landing_access_repro.py`
  - `tools_exp.py`
  - `experiment/summarize_linux_headless_suite.py`
- 핵심 변경
  - runtime diagnostics 추가: readyState, HTML 길이, body text 길이, iframe, viewport, tab count, console/runtime/network summary
  - delayed screenshot 추가
  - AIP challenge에서 같은 DOI를 한 시도 안에서 다시 열지 않도록 no-retry 처리
  - `entry_preflight_issue_overridden` 도입
- 관찰 결과
  - 이전 patch는 "patch 미적용"이 아니라 실제 runtime path에 반영돼 있었다
  - 다만 blank screenshot 감소/진단 강화에는 도움이 됐지만 server AIP landing success 자체는 개선하지 못했다
- 배운 점
  - blank screenshot은 tab bug의 강한 증거가 아니라, challenge shell이 거의 렌더되지 않은 headless 화면일 가능성이 높다

### 5.9 AIP DOI-first browser entry 전환

- 새 가설
  - AIP에서 canonical `article-abstract` URL을 browser가 바로 여는 진입 전략이 server/headless에서 challenge를 더 잘 유발할 수 있다.
  - browser가 `https://doi.org/...`를 직접 밟고 publisher redirect를 따라가게 하는 편이 더 공식적이고 저마찰일 수 있다.
- 구현
  - `tools_exp.py`
    - `build_aip_safe_entry_plan()`
    - `entry_browser_kind=official_doi_redirect`
    - `entry_preflight_url` 분리
  - `landing_access_repro.py`
  - `parallel_download.py`
  - `experiment/summarize_linux_headless_suite.py`
- 기대 효과
  - landing path와 download path가 동일한 AIP entry semantics를 사용
  - preflight 403과 browser landing 결과를 분리 기록
- 현재 상태
  - 코드상 DOI-first 경로와 `entry_preflight_url`, `entry_preflight_issue_overridden` 필드는 존재
  - 그러나 이 최신 AIP strategy가 실제 Linux 서버 bundle에서 성공으로 검증된 근거는 아직 없다 `[blocked]`

### 5.10 landing patch와 main download patch 정렬

- 사용자 요청
  - landing에서 수정한 의미 있는 로직은 main download path에도 항상 같이 반영
- 구현
  - `tools_exp.py` shared entry-plan detail
  - `parallel_download.py` CSV fields
  - `experiment/summarize_linux_headless_suite.py` merged summary fields
- 현재 공통 필드
  - `entry_strategy`
  - `entry_browser_url`
  - `entry_browser_kind`
  - `entry_preflight_url`
  - `entry_preflight_issue`
  - `entry_preflight_issue_overridden`
  - `landing_recovery_attempted`
  - `landing_recovery_strategy`
  - `landing_recovery_outcome`
- 배운 점
  - landing과 download가 다른 의미론을 쓰면 결과 비교와 회귀 판정이 불가능해진다

### 5.11 AIP prebrowser article preflight 제거와 logging 전파 보강

- 최신 문제 재정의
  - 외부 bundle `pilot_20260315_014015`의 `10.1116/6.0003261`는 landing probe에서 이미
    - `entry_strategy=aip_official_doi_resolve`
    - `entry_browser_kind=official_doi_redirect`
    - `landing_recovery_outcome=challenge_detected_no_retry`
    로 기록됐다.
  - 즉 최신 AIP DOI-first/no-retry patch는 landing runtime path에서 실제로 실행됐다.
  - 그러나 같은 run의 download metadata sidecar에는 `landing_entry_*`가 비어 있어, "patch가 browser/runtime에 적용됨"과 "최종 metadata/summary에 남음"이 분리돼 있었다.
  - 또한 코드상 `build_aip_safe_entry_plan()`은 browser가 열리기 전에
    - DOI resolve `GET`
    - canonical article/article-abstract preflight `GET`
    를 수행하고 있었다.
  - `pilot_20260315_014015` download log에는 browser open 전에 이미
    - `preflight_issue=FAIL_BLOCK`
    - redirect chain `302:https://doi.org/... -> 403:https://pubs.aip.org/...`
    가 찍혔다.
- 새 가설
  - 최신 AIP 전략은 "DOI-first"로 보였지만 실제로는 browser 앞단에서 AIP article 리소스를 먼저 1~2번 더 치고 있었다.
  - 이 prebrowser article preflight가 Linux headless/IP trust 조건에서 challenge 압력을 더 키웠을 가능성이 높다.
  - 따라서 AIP는 canonical article preflight를 기본값에서 제거하고, DOI redirect target은 "location-only" 수준으로만 가볍게 관찰한 뒤 browser에게 실제 landing을 맡기는 쪽이 더 저마찰이다.
- 구현
  - `tools_exp.py`
    - AIP entry plan을 redirect-location-only probe 기본값으로 변경
    - browser open 전 article preflight `GET`는 기본 비활성화
    - 새 필드 추가:
      - `entry_strategy_variant`
      - `entry_redirect_probe_mode`
      - `entry_prebrowser_request_count`
    - 필요 시 env `PDF_BROWSER_LANDING_AIP_ARTICLE_PREFLIGHT=1`로 이전 preflight 동작을 다시 켤 수 있게 유지
  - `parallel_download.py`
    - `download_with_drission()`이 반환한 AIP entry/recovery/challenge detail이 final result/CSV/metadata sidecar까지 전파되도록 보강
  - `landing_access_repro.py`
    - AIP entry variant/probe mode/prebrowser request count/challenge bool을 landing artifact JSONL 및 artifact sidecar에 기록
  - `experiment/summarize_linux_headless_suite.py`
    - landing/download merged summary에 AIP entry variant/probe mode/request count/challenge bool을 추가
- 통제 검증
  - 로컬 headless 검증: `outputs/aip_patch_validation_20260315_local/landing/`
  - 입력 DOI:
    - `10.1116/6.0004298`
    - `10.1063/5.0246311`
  - 조건:
    - `local_mac` preset
    - headless Chrome
    - worker 1
    - AIP publisher cooldown 45초
    - `max_nav_attempts=1`
    - `PDF_BROWSER_LANDING_AIP_ARTICLE_PREFLIGHT=0`
  - 결과:
    - `sample_size=2`
    - `classifier_counts={"success_landing":2}`
    - 두 DOI 모두
      - `entry_strategy=aip_official_doi_resolve`
      - `entry_strategy_variant=doi_redirect_only_no_article_preflight`
      - `entry_redirect_probe_mode=doi_location_only`
      - `entry_prebrowser_request_count=1`
      - `entry_browser_kind=official_doi_redirect`
      - `challenge_detected=false`
      - `tab_transition_count=0`
      - `runtime_blank=false`
    - 대표 success artifact:
      - `outputs/aip_patch_validation_20260315_local/landing/artifacts/success/landing_success_10.1116_6.0004298_1773508422474.json`
      - `outputs/aip_patch_validation_20260315_local/landing/artifacts/success/landing_success_10.1063_5.0246311_1773508492736.json`
- 관찰 결과
  - 최신 Linux fail 근거와 새 local validation을 합치면, AIP 실패의 1차 원인은 tab loss가 아니라 "stable landing 전 challenge 발생"으로 보는 해석이 더 강해졌다.
  - 동시에 기존 DOI-first patch는 실제 runtime path에 적용돼 있었지만, prebrowser article preflight까지 제거된 것은 이번 patch부터다.
  - download metadata sidecar의 AIP entry field 누락은 browser/runtime 미적용이 아니라 result propagation bug였다.
- 배운 점
  - AIP에서 "official DOI-first"라는 이름만으로 충분하지 않다. browser 앞단에서 article을 또 치면 저마찰 진입이 아니다.
  - `patch exists in code`와 `patch changed runtime behavior`는 artifact/log/sidecar 각각에서 따로 확인해야 한다.
  - 새 patch는 local headless에서는 의미 있는 안정화 신호를 보였지만, Linux server/headless 동일 조건에서의 재확인은 아직 남아 있다 `[blocked]`

### 5.12 AIP blank/Google symptom 분해와 structural landing patch

- 최신 source-of-truth
  - 외부 bundle `aip_micro_20260315_preflightoff`를 기준으로 다시 확인했다.
  - landing probe 2건은 둘 다 `challenge_detected=true`였고
    - `entry_strategy=aip_official_doi_resolve`
    - `entry_strategy_variant=doi_redirect_only_no_article_preflight`
    - `entry_redirect_probe_mode=doi_location_only`
    - `entry_prebrowser_request_count=1`
    가 실제 artifact JSONL에 기록돼 있었다.
  - 즉 이전 AIP patch는 "코드에만 있음"이 아니라 최신 실패 런타임에도 실제 적용됐다.
- blank screen이 의미한 것
  - bundle의 landing fail artifact는 screenshot이 거의 빈 흰 화면처럼 보였지만, 같은 artifact JSON/HTML에는
    - `title=Just a moment...`
    - `challenge_detected=true`
    - `tab_transition_count=0`
    - `ready_state=complete`
    - final HTML에 Cloudflare challenge shell
    이 남아 있었다.
  - 따라서 최신 Linux AIP의 blank screen은 주로
    - wrong active tab
    - stale page handle
    - pure `about:blank`
    - renderer reset
    보다 "challenge/interstitial shell이 headless에서 거의 비어 보인 상태"를 뜻한다.
- Google default page가 의미한 것
  - 같은 bundle의 download metadata record에서 `10.1063/5.0207496`는
    - `landing_state=blank_or_incomplete`
    - `landing_url=https://doi.org/10.1063/5.0207496`
    - `landing_title=New Tab`
    - fail screenshot은 Google default/new-tab page
    로 남아 있었다.
  - 이 symptom은 publisher challenge와 별개로
    - wrong active tab/page selection
    - startup/default page context가 실제 target navigation을 덮어씀
    - unresolved DOI recovery가 canonical target으로 못 넘어감
    을 강하게 시사한다.
  - 특히 당시 AIP DOI recovery는 `_resolve_doi_redirect_target()`이 `entry_browser_url`을 먼저 반환해, `doi.org` 진입 케이스에서는 recovery가 DOI URL을 그대로 다시 주는 no-op였다.
- 새 가설
  - AIP 실패는 단일 원인보다 두 층으로 나뉜다.
    - landing probe 층: stable article landing 전에 publisher challenge가 먼저 뜬다.
    - download browser 층: challenge와 별개로 default/new-tab context를 publisher landing으로 오인할 수 있는 tab lifecycle 결함이 있다.
  - 따라서 AIP는 retry를 더 쌓는 대신
    - DOI resolve 후 canonical target 선택을 바로잡고
    - active tab을 한 탭으로 정리한 뒤
    - default/new-tab을 publisher landing으로 분류하지 않고
    - 필요할 때만 canonical target으로 재진입하는 구조가 필요하다.
- 구현
  - `tools_exp.py`
    - `_resolve_doi_redirect_target()`에서 AIP recovery target 우선순위를
      - `entry_url`
      - `entry_resolved_url`
      - `entry_handoff_url`
      - `entry_browser_url`
      순으로 바꿨다.
    - download path에 landing probe와 같은 성격의 tab hygiene를 추가했다.
      - AIP 진입 전 extra tab prune
      - `about:blank` pre-reset
      - active tab / total tab 수 기록
    - browser default page detector를 추가했다.
      - `chrome://newtab`
      - title `New Tab`
      - Google default/home markers
    - AIP recovery를 default-page aware 방식으로 재설계했다.
      - default/new-tab 또는 unresolved DOI이면 canonical target 우선
      - 필요 시 fresh-tab canonical recovery 1회
      - challenge가 보이면 no-retry 종료 유지
    - AIP download 결과에 아래 필드를 추가했다.
      - `landing_initial_target_url`
      - `landing_default_page_detected`
      - `landing_default_page_kind`
      - `landing_tab_transition_count`
      - `landing_tab_transition_events`
      - `landing_final_active_tab_id`
      - `landing_final_total_tab_count`
      - `landing_final_screenshot_path`
      - `landing_final_html_path`
    - AIP는 stable landing이 확인되는 순간 snapshot을 먼저 남기도록 보강했다.
  - `parallel_download.py`
    - 위 landing telemetry가 final CSV와 metadata sidecar `record`까지 남도록 전파했다.
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 AIP default-page/tab/artifact 필드를 추가했다.
- 통제 검증
  - 입력 CSV: `outputs/_aip_structural_validation_20260315_input.csv`
  - 실행 1: landing-only local headless
    - 명령
      - `python landing_access_repro.py --input outputs/_aip_structural_validation_20260315_input.csv --workers 1 --headless 1 --runtime-preset local_mac --execution-env desktop --artifact-dir outputs/aip_structural_validation_20260315_local/landing/artifacts --output-jsonl outputs/aip_structural_validation_20260315_local/landing/landing_access_repro.jsonl --report outputs/aip_structural_validation_20260315_local/landing/landing_access_repro_report.json --report-md outputs/aip_structural_validation_20260315_local/landing/landing_access_repro_report.md --capture-fail-artifacts 1 --capture-success-artifacts 1 --capture-success-html 1`
    - 중간 확인
      - stdout progress line
      - `outputs/aip_structural_validation_20260315_local/landing/landing_access_repro.jsonl`
    - 종료 확인
      - `landing_access_repro_report.json` 생성
      - process exit `0`
    - 결과
      - `sample_size=2`
      - `classifier_counts={"success_landing":2}`
      - 두 DOI 모두 `challenge_detected=false`, `tab_transition_count=0`, `total_tab_count=1`
      - blank screen / Google default page 모두 재현되지 않았다.
  - 실행 2: download path local smoke
    - 출력
      - `outputs/aip_structural_validation_20260315_local/download/download_validation_results.json`
      - `outputs/aip_structural_validation_20260315_local/download/download_validation.log`
    - 중간 확인
      - `tail -n 80 outputs/aip_structural_validation_20260315_local/download/download_validation.log`
    - 종료 확인
      - `download_validation_results.json` 생성
      - process exit `0`
    - 결과
      - `10.1063/5.0207496`
        - `landing_state=success_landing`
        - `landing_default_page_detected=false`
        - `landing_final_total_tab_count=1`
        - download까지 성공
        - landing screenshot/html path가 기록됨
      - `10.1116/6.0004298`
        - `landing_state=success_landing`
        - `landing_default_page_detected=false`
        - `landing_final_total_tab_count=1`
        - 이후 PDF click 단계에서 browser disconnect로 final result는 `FAIL_NETWORK`
        - 그러나 landing 자체는 real AIP article-abstract page까지 도달했다.
    - 해석
      - regression DOI였던 `10.1063/5.0207496`에서 Google default page가 재현되지 않았고, real article landing + download success로 바뀌었다.
      - 즉 default/new-tab symptom의 주원인은 publisher 자체보다 download path의 tab/target handling 결함 쪽이었다.
  - 실행 3: controlled single-DOI rerun
    - 이유
      - stable landing 직후 snapshot 저장 patch가 실제 failure artifact path도 채우는지 확인하기 위해 `10.1116/6.0004298` 한 건만 즉시 재검증했다.
    - 출력
      - `outputs/aip_structural_validation_20260315_local/download_single_10.1116_6.0004298/download_validation_result.json`
      - `outputs/aip_structural_validation_20260315_local/download_single_10.1116_6.0004298/download_validation.log`
    - 결과
      - 같은 DOI를 짧은 간격으로 다시 치자 `landing_state=challenge_or_block`, `reason=FAIL_BLOCK`로 바뀌었다.
      - fail screenshot/html path는 새 필드에 기록됐다.
    - 해석
      - repeated DOI hit 자체가 AIP challenge 확률을 빠르게 올릴 수 있음을 다시 확인했다.
      - AIP는 patch correctness와 별개로 DOI rotation / low-frequency sampling이 필수다.
- before vs after
  - before: `aip_micro_20260315_preflightoff` (Linux headless)
    - landing probe success `0/2`
    - blank screenshot `2/2` but 실제 HTML은 challenge shell
    - Google default page symptom `1/2` in download path
    - patch branch는 적용됐지만 runtime path가 challenge/default-page로 갈라졌다
  - after: `aip_structural_validation_20260315_local` (local headless)
    - landing probe success `2/2`
    - blank screenshot `0/2`
    - Google default page symptom `0/2`
    - download smoke에서 stable landing `2/2`
    - 그중 `1/2`는 download success, `1/2`는 landing 후 click-stage browser disconnect
- 배운 점
  - blank screen과 Google default page는 같은 failure가 아니다.
    - blank screen은 현재까지 challenge shell 해석이 더 강하다.
    - Google default page는 tab/page lifecycle 또는 canonical target recovery bug 쪽 신호다.
  - AIP current workflow를 "그냥 publisher 문제"로만 보면 안 된다.
    - latest Linux evidence는 challenge를 보여주지만,
    - Google default page는 우리 쪽 landing workflow 결함이 실제로 섞여 있었다.
  - 같은 DOI를 연달아 다시 치면 challenge로 바뀔 수 있으므로, AIP 검증은 fresh/low-frequency와 tight control을 반드시 유지해야 한다.

### 5.13 AIP context bootstrap 전략과 seed-profile warming 해석 분리

- 문제 재정의
  - 최신 Linux fresh run `aip_micro_20260315_structural_patch_fresh`는
    - `entry_strategy_variant=doi_redirect_only_no_article_preflight`
    - `browser_session_source=linux_seed_clone`
    - `challenge_detected=true`
    - `tab_transition_count=0`
    - `title=Just a moment...`
    로 끝났다.
  - 반면 `local_mac`와 local `linux_cli_seeded`는 같은 AIP DOI에서 landing success가 반복 관찰됐다.
  - 따라서 "AIP logic 자체가 항상 틀림"보다는
    - Linux server outbound network / IP trust
    - seeded profile continuity 부족
    - publisher context initialization 부재
    중 어느 층이 더 큰지 분리할 필요가 있었다.
- 추가 진단 1: local cold-profile vs stateful-profile
  - 입력 DOI:
    - `10.1063/5.0257779`
  - 실행 A:
    - `outputs/aip_profile_diag_20260315_local_temp/`
    - `runtime_preset=local_mac`
    - `profile_mode=temp`
  - 결과 A:
    - `classifier_counts={"success_landing":1}`
  - 실행 B:
    - `outputs/aip_profile_diag_20260315_local_linuxseeded/`
    - `runtime_preset=linux_cli_seeded`
    - `execution_env=linux_server`
    - `persistent_profile_dir=outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed`
  - 결과 B:
    - `classifier_counts={"success_landing":1}`
  - 해석
    - local AIP success는 "기존 macOS 프로필이 따뜻해서만" 설명되지는 않았다.
    - cold temp profile과 local linux-seeded clone에서도 article landing 자체는 성립했다.
    - 따라서 최신 Linux server failure를 pure tab bug나 pure profile absence로만 설명하는 건 부족하다.
- 새 가설
  - Linux server에서는 AIP article DOI landing 전에 publisher-side challenge가 너무 일찍 붙는다.
  - 하지만 local 실험과 manual-warm 관찰을 합치면, "publisher journal/home context를 먼저 여는 bootstrap"이 같은 세션의 뒤이은 DOI/article landing에 도움이 될 수 있다.
  - 이건 anti-bot 우회가 아니라
    - legitimate publisher-page landing
    - DOI/article 접근 전 journal root or publisher root 초기화
    라는 점에서 strategy-layer 수정으로 볼 수 있다.
- 구현
  - `tools_exp.py`
    - AIP canonical entry에서 journal root / publisher root를 계산하는 `_derive_aip_context_target()` 추가
    - Linux/server 성격일 때 기본 활성화되는 `_aip_context_bootstrap_enabled()` 추가
    - `build_aip_safe_entry_plan()`이
      - `entry_context_url`
      - `entry_context_kind`
      - `entry_strategy_variant=doi_redirect_with_context_bootstrap_no_article_preflight`
      를 기록하도록 수정
    - download path에 `_maybe_bootstrap_aip_entry_context()`를 넣어 DOI/article 진입 전 AIP context page를 먼저 열고 결과를 기록
  - `landing_access_repro.py`
    - landing probe도 같은 `_maybe_bootstrap_aip_entry_context()`를 사용해 DOI navigation 전에 context bootstrap을 수행
    - 아래 필드를 artifact JSONL / success/fail sidecar에 남김
      - `entry_context_bootstrap_attempted`
      - `entry_context_bootstrap_outcome`
      - `entry_context_bootstrap_final_url`
      - `entry_context_bootstrap_final_title`
  - `parallel_download.py`
    - download CSV / metadata sidecar에 위 context bootstrap 필드 전파
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 landing/download context bootstrap 필드 추가
- 통제 검증
  - 실행 C:
    - `outputs/aip_context_bootstrap_validation_20260315_local/`
    - 입력 DOI `10.1063/5.0257779`
    - `runtime_preset=linux_cli_seeded`
    - `execution_env=linux_server`
  - 결과 C:
    - 최종 `success_landing`
    - `entry_strategy_variant=doi_redirect_with_context_bootstrap_no_article_preflight`
    - `entry_context_url=https://pubs.aip.org/jcp`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_challenge`
    - 그 뒤 최종 DOI/article landing은 success
  - 해석 C:
    - journal root 자체는 challenge로 보였지만, 같은 세션의 뒤이은 DOI/article landing은 성공했다.
    - 즉 context bootstrap은 "challenge가 없는 homepage를 반드시 먼저 띄운다"라기보다, legitimate context initialization 단계로 읽는 편이 맞다.
  - 실행 D:
    - `outputs/aip_context_bootstrap_validation2_20260315_local/`
    - 입력 DOI `10.1116/6.0004298`
    - 같은 local `linux_cli_seeded` 조건
  - 결과 D:
    - 최종 `success_landing`
    - `entry_context_url=https://pubs.aip.org/jva`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_ready`
    - journal root title은 `Journal of Vacuum Science & Technology A | AIP Publishing`
  - 해석 D:
    - 두 번째 DOI에서는 context page 자체도 clean landing이었고, 이후 DOI/article landing도 success였다.
- seed profile 교체 질문에 대한 정리
  - UTM에서 AIP manual visit 후 그 profile을 새 seed profile로 다시 교체하면 결과가 달라질 수는 있다.
  - 이유는 현재 Linux workflow가 persistent seed profile을 clone해 쓰기 때문이다.
  - 그러나 그 의미는 우선
    - cookie/bootstrap/session state reuse 효과
    - warmed seed가 다음 clone에 전파되는 효과
    이지, landing flow가 구조적으로 해결됐다는 뜻은 아니다.
- 배운 점
  - local cold-profile success까지 감안하면, AIP current failure를 "profile만 바꾸면 됨"으로 단순화하면 안 된다.
  - 동시에 manual-warm 가설은 완전히 틀린 게 아니라, "publisher context initialization"이라는 합법적 strategy로 code에 옮길 수 있다.
  - context page가 challenge를 띄우더라도 같은 세션의 DOI/article landing이 이어서 성공할 수 있으므로, earliest challenge page와 final landing page를 분리 로깅해야 한다.

## 6. Publisher별 결과 요약

### Elsevier / Cell-family

- 초기 문제
  - landing layer에서 direct article 진입이 너무 이르고 공격적이었음
  - shell page와 real page 구분이 약했음
- 외부 bundle 관찰
  - `pilot_20260314_210007` / `pilot_20260314_213217`
    - Elsevier 1건, Cell 2건 challenge/interstitial
  - `pilot_20260315_000015`
    - Elsevier 2건 landing success
  - `pilot_20260315_012307`
    - Elsevier 1건 landing success + publisher-native download
- 현재 판단
  - Elsevier landing 전략은 초기 direct-entry 단계보다 개선됨
  - 다만 shell recovery가 명시적으로 필요한 live case를 최신 server bundle에서 다시 검증한 증거는 제한적이다 `[blocked]`

### AIP / `10.1063`, `10.1116`

- 초기 문제
  - blank screenshot, challenge page, canonical article-abstract direct entry, repeated recovery attempts가 섞여 보였음
- 외부 bundle 관찰
  - `pilot_20260314_210007`, `pilot_20260314_213217`: AIP 1건씩 challenge
  - `pilot_20260314_232524`: `10.1116/6.0004868` challenge
  - `pilot_20260315_000015`: `10.1116/6.0003941` challenge
  - `pilot_20260315_004518`: `10.1116/6.0003790` challenge, canonical recovery 두 번 수행
  - `pilot_20260315_012307`: `10.1116/6.0003847` challenge, no-retry + runtime diagnostics 적용
  - `pilot_20260315_014015`: `10.1116/6.0003261` challenge, DOI-first + no-retry는 runtime에 실제 적용됐지만 browser 앞단 preflight 403이 선행
  - local validation `outputs/aip_patch_validation_20260315_local/landing/`
    - `10.1116/6.0004298`: success landing
    - `10.1063/5.0246311`: success landing
- 확인된 사실
  - blank screenshot은 현재까지 challenge shell을 더 잘 설명한다
  - no-retry 및 진단 확장은 실제 runtime에 적용됐다
  - DOI-first patch도 `pilot_20260315_014015` landing runtime path에 실제 적용됐다
  - 다만 이전 구현은 browser open 전에 article preflight를 수행하고 있었고, 이 부분이 이번에 제거됐다
- 현재 판단
  - AIP 최신 Linux 실패는 주로 challenge/interstitial이 stable landing 전에 발생하는 문제로 보이며, tab/page-context loss는 1차 원인으로 보기 어렵다
  - 이전 "DOI-first"는 실제 runtime에 적용됐지만 browser 앞단 preflight까지 포함돼 있어 충분히 저마찰하지 않았다
  - 새 redirect-only/no-article-preflight branch는 local headless에서 2/2 success였지만, Linux server 재검증은 아직 부족하다 `[blocked]`

### Springer / `10.1007_`

- 확인된 사실
  - `pilot_20260314_232524`의 `10.1007/s12598-024-02864-w`는 실제 Wiley article page였는데 실패로 오분류됨
- 현재 판단
  - 분류기 보정 방향은 맞다
  - 실제 server rerun으로 false-failure 감소가 확인됐는지는 미확인 `[blocked]`

### RSC

- 계획상 역할
  - pilot에서 2건 명시 포함
  - full에서도 대표 publisher로 유지
- 관찰
  - 외부 bundle들에서 RSC는 상대적으로 안정적인 landing/download 축에 속했다
- 현재 판단
  - 회귀 감시용 publisher로 계속 포함할 가치가 높다

### 기타 ACS / Wiley / Nature / IOP / MDPI / IEEE

- 최근 pilot bundle들에서 대체로 landing success 축에 존재
- 다만 Nature/Springer/Wiley는 cross-host, gate, timing 차이가 섞일 수 있어 분류기의 보수적 판정이 필요

## 7. 실패 패턴과 교훈

### 반복적으로 나타난 실패 패턴

- direct article URL을 browser가 너무 일찍 여는 공격적 진입
- shell page를 real article page와 구분하지 못하는 오판
- preflight request 결과와 browser landing 결과를 같은 것으로 보는 해석
- browser open 전에 같은 AIP article을 먼저 치는 prebrowser preflight
- blank screenshot을 tab bug로 과잉 해석하는 문제
- 동일 DOI를 여러 run에서 반복 타격하는 실험 설계

### 실제로 오분류였던 것

- `10.1007/s12598-024-02864-w`
  - real landing이었지만 `domain_mismatch` 실패로 처리됨

### blank screenshot이 의미한 것

- 현재까지의 강한 해석
  - headless에서 거의 렌더되지 않은 challenge/interstitial shell
- 현재까지의 약한 해석
  - wrong active tab
  - stale handle
  - pure about:blank
  - zero-size viewport

### tab/page-state 문제

- 의심은 많았지만, AIP latest fail bundle 기준으로는 `total_tab_count=1`, `tab_transition_count=0`, `ready_state=complete`라서 1차 원인으로 보기 어렵다
- 다만 latest-tab adoption, tab sync, stale context 방어는 Elsevier/AIP 양쪽에서 계속 보강되었다

### runtime/summary 불일치

- `pilot_20260315_014015`에서는 download stderr/log에 AIP DOI-first branch가 찍혔지만 metadata sidecar의 `landing_entry_*`는 비어 있었다
- 따라서 일부 요약에서는 AIP latest fail이 `challenge_or_interstitial`보다 `blank_or_incomplete` 쪽으로 더 약하게 보일 수 있었다
- 이번 patch에서 download result propagation을 보강해 이 불일치를 줄였다

### `local_mac`가 실제로 도움이 된 부분

- Elsevier:
  - retrieve-link recovery
  - handoff/canonical normalize
  - article shell reopen
  - tab adoption
- AIP:
  - 실질적인 publisher-specific landing recovery는 거의 없었고, generic browser landing 성향 정도만 참고 가능

## 8. 로깅과 운영 워크플로우

### 표준 실행 순서

```bash
bash scripts/prepare_linux_server_env.sh
bash scripts/run_linux_suite_bg.sh --suite pilot
bash scripts/check_linux_suite_status.sh <run-name>
bash scripts/collect_linux_suite_artifacts.sh <run-name>
```

### 사용자에게 먼저 수집한 환경값

- `HOME`
- `PWD`
- `SEED_PROFILE`
- `PROFILE_NAME`
- `CHROME_PATH`
- `PYTHON_BIN`
- `VIRTUAL_ENV`
- `PDF_BROWSER_NO_SANDBOX`
- `TMPDIR`
- `XDG_RUNTIME_DIR`
- `RUNS_ROOT`
- `LOGS_ROOT`
- `COLLECT_ROOT`

### run 단위 산출물 구조

- root launcher
  - `logs/<run>.cmd.sh`
  - `logs/<run>.log`
  - `logs/<run>.pid`
  - `logs/<run>.run_dir`
- run dir
  - `execution_manifest.json`
  - `run_suite.sh`
  - `logs/seed_profile_check.*`
  - `logs/landing.*`
  - `logs/download.*`
  - `logs/summarize.*`
  - `landing/landing_access_repro.jsonl`
  - `landing/landing_access_repro_report.json`
  - `landing/artifacts/...`
  - `download/run/openalex_search_results_parallel.csv`
  - `download/run/summary.json`
  - `summary/merged_results.csv`
  - `summary/suite_summary.json`
  - `summary/retry_protection_skips.csv`

### 최근 확장된 주요 로깅 필드

- landing/download 공통 entry 의미론
  - `entry_strategy`
  - `entry_strategy_variant`
  - `entry_redirect_probe_mode`
  - `entry_prebrowser_request_count`
  - `entry_url`
  - `entry_resolved_url`
  - `entry_browser_url`
  - `entry_browser_kind`
  - `entry_handoff_url`
  - `entry_context_url`
  - `entry_context_kind`
  - `entry_preflight_url`
  - `entry_preflight_issue`
  - `entry_preflight_issue_overridden`
- landing quality
  - `challenge_detected`
  - `initial_landing_type`
  - `landing_recovery_attempted`
  - `landing_recovery_strategy`
  - `landing_recovery_outcome`
  - `shell_recovery_*`
  - `reclassified_after_detector_fix`
- runtime diagnostics
  - `readyState`
  - DOM/visible text 길이
  - tab count / active tab id
  - viewport
  - iframe summary
  - console/runtime/network summary
  - `blank_screenshot_likely`
- retry protection
  - `prior_attempt_*`
  - `retry_protection_action`
  - `retry_protection_reason`

### manual visit 진단용 profile snapshot 도구

- 추가 스크립트
  - `scripts/snapshot_browser_profile_state.py`
  - `scripts/compare_browser_profile_snapshots.py`
- 목적
  - Ubuntu server에서 같은 persistent seed profile과 `Default` profile로 manual DOI landing 1회를 수행하기 전후에
    - `Local State`
    - `Cookies`
    - `Default/Local Storage`
    - `Default/IndexedDB`
    의 경로, mtime, size, hash를 남기기 위함
- 해석 원칙
  - manual visit이 이후 automation을 일시적으로 개선하더라도, 그것은 우선 session/bootstrap/cookie state warming 진단 신호다.
  - 이것만으로 landing flow가 구조적으로 해결됐다고 결론 내리면 안 된다.
  - manual visit 진단은 같은 persistent profile root, 같은 `Default` profile, 가능한 한 같은 outbound network 조건에서 수행돼야 한다.
  - 다른 VM/다른 IP에서의 manual visit 결과는 server-side AIP failure 원인 해석에 직접 연결하기 어렵다 `[blocked]`.

## 9. 현재 상태

### 확인된 개선

- Linux seed profile 준비/검증/패키징 절차가 문서화되고 스크립트화됨
- server env 수집과 `config/linux_server.env` 기반 재사용 경로가 정착
- `nohup` 기반 background run, status, log tail, artifact collect 워크플로우가 정착
- recent 2024+ 중심 suite와 Sci-Hub confound 분리 설계가 문서/코드/산출물에 반영됨
- retry protection과 DOI rotation 코드가 builder/runner/summary에 반영됨
- Elsevier landing은 초기 direct-entry 방식보다 공식 retrieve/handoff 중심으로 개선됨
- `10.1007_`류 cross-host landing 오분류를 고칠 classifier 경로가 추가됨
- AIP blank screenshot에 대한 runtime diagnostics와 no-retry challenge 처리가 실제 bundle에 반영됨
- AIP redirect-only/no-article-preflight branch와 entry request-count logging이 추가됨
- AIP download metadata/summary 경로에 entry detail/challenge bool 전파가 보강됨
- landing 쪽 entry/recovery semantics가 download path와 summary에도 반영됨
- AIP download path가 default/new-tab landing을 별도 진단하고 canonical target recovery를 수행하도록 구조화됨
- AIP download result/metadata/suite summary에 default page, tab transition, final artifact path가 기록됨
- regression DOI `10.1063/5.0207496` local smoke에서 Google default page symptom이 사라지고 real article landing + download success로 바뀜
- manual visit 전후 persistent profile 변화를 비교할 snapshot/diff 도구가 추가됨
- AIP context bootstrap branch가 추가됐고, local `linux_cli_seeded`에서 journal-root bootstrap 후 DOI/article landing success가 재현됨
- local AIP는 cold temp profile과 local linux-seeded clone에서도 landing success가 관찰돼, pure profile absence만으로는 최신 Linux failure를 설명할 수 없다는 근거가 추가됨
- AIP browser entry 기본값이 Linux/server 계열에서는 `doi.org` browser-open 대신 resolve된 publisher canonical article entry로 전환되도록 수정됨
- 새 publisher-direct branch가 local `linux_cli_seeded` 검증에서 실제 runtime으로 실행됐고, `context_challenge` 이후에도 article landing success가 재현됨

### 아직 미검증 또는 근거 부족

- 최신 AIP redirect-only/no-article-preflight branch가 실제 Linux 서버 bundle에서 landing success를 올렸는지 `[blocked]`
- Elsevier shell recovery가 "shell-only live case"에서 end-to-end로 회복되는지 `[blocked]`
- `10.1007_` classifier fix가 server rerun에서 false-failure를 실제로 줄였는지 `[blocked]`
- attempt ledger 파일 자체의 최신 누적 상태는 현재 로컬 워크스페이스에서 확인되지 않음 `[blocked]`
- 새 AIP structural patch가 Linux server/headless에서도 Google default page incidence를 실제로 0으로 낮추는지 `[blocked]`
- AIP stable landing 이후의 downstream click/download disconnect가 Linux headless에서도 남는지 `[blocked]`
- 새 AIP context bootstrap branch가 Linux server/headless에서 challenge incidence를 실제로 낮추는지 `[blocked]`
- 최신 fresh server run 기준으로는 AIP context bootstrap branch가 실제 runtime에서 실행됐지만 `context_challenge -> challenge_detected_no_retry` 2/2로 끝났고, 아직 Linux server에서 landing success 개선 근거는 없다.
- 새 publisher-direct AIP branch가 Linux server/headless에서 `doi_redirect` 대비 challenge incidence를 실제로 낮추는지 `[blocked]`

### 구조적 위험

- Elsevier/AIP는 코드 수정만으로 해결되지 않는 publisher-side challenge/IP trust 문제가 남아 있을 수 있음
- 외부 bundle들이 저장소 밖에 있어 장기 보존성이 낮음

## 10. 다음 권장 작업

1. 최신 코드 기준으로 Linux server에서 AIP micro-run을 다시 실행해 structural patch가 실제로 적용되는지 확인
   - 우선 확인 필드:
     - `landing_entry_browser_url`
     - `landing_entry_browser_kind`
     - `landing_entry_strategy_variant`
     - `landing_entry_redirect_probe_mode`
     - `landing_entry_prebrowser_request_count`
     - `landing_entry_preflight_url`
     - `landing_entry_preflight_issue`
     - `landing_probe_state`
   - 필수 확인 항목:
     - `landing_default_page_detected`
     - `landing_default_page_kind`
     - `landing_tab_transition_count`
     - `landing_final_total_tab_count`
     - `landing_final_screenshot_path`
     - `landing_final_html_path`
2. Linux/headless 동일 조건에서 fresh/low-frequency AIP DOI 1~2건만 다시 검증해
   - challenge가 여전히 first article landing 전에 뜨는지
   - Google default page symptom이 사라졌는지
   - download path에서 canonical recovery가 실제로 쓰였는지
   - `entry_context_url`, `entry_context_kind`, `entry_context_bootstrap_*`가 실제 runtime artifact와 summary에 남는지
   - context page 자체가 `context_ready`인지 `context_challenge`인지
   를 확인
3. `10.1007_` 계열 DOI를 fresh/low-frequency 케이스로 1건만 다시 검증해 classifier false negative fix를 server artifact로 재확인
4. Elsevier shell-like case가 다시 나오면 `landing_shell_recovery_*`와 final classifier를 함께 확인해 live recovery 성공 여부를 증거화
5. 외부 bundle 중 핵심 run의 `suite_summary.json`, `merged_results.csv`, 대표 fail artifact JSON을 `docs/` 또는 별도 `analysis/` 아래 장기 보존 형태로 남길지 결정
6. 이후 새 실험 제안 시 아래 4개를 항상 같이 제공
   - 실행 명령
   - 중간 로그 확인 명령
   - 종료 확인 명령
   - artifact 수집/전달 명령
7. AIP manual visit 진단은 아래 순서를 지킬 것
   - manual visit 전 fresh AIP DOI baseline automation 1회
   - 같은 persistent profile에 대해 before snapshot 저장
   - 같은 persistent profile root와 `Default` profile로 DOI landing 1회만 수동 방문
   - 직후 다른 fresh AIP DOI로 post-manual automation 1회
   - after snapshot과 before/after diff를 bundle에 함께 포함

## 11. 부록: 최근 외부 bundle에서 확인한 대표 상태

### `pilot_20260314_210007`

- `sample_total=13`
- `combined_bucket_counts={"challenge_or_interstitial":4,"publisher_native_download":9}`
- landing classifier는 `unknown_non_success` 3건, `challenge_detected` 1건이 섞여 있었음

### `pilot_20260314_213217`

- `sample_total=13`
- `combined_bucket_counts={"challenge_or_interstitial":4,"publisher_native_download":9}`
- 동일 실패군이 `challenge_detected`로 더 명확히 정리됨

### `pilot_20260314_232524`

- 대표 실패:
  - `10.1007/s12598-024-02864-w`: real Wiley landing인데 `domain_mismatch`
  - `10.1116/6.0004868`: AIP challenge

### `pilot_20260315_000015`

- Elsevier 2건 landing success
- AIP `10.1116/6.0003941` challenge

### `pilot_20260315_004518`

- AIP `10.1116/6.0003790`
  - `aip_recovery_1`, `aip_recovery_2`까지 갔지만 여전히 challenge

### `pilot_20260315_012307`

- `sample_total=4`
- `combined_bucket_counts={"challenge_or_interstitial":1,"publisher_native_download":2,"scihub_assisted_download":1}`
- AIP `10.1116/6.0003847`
  - no-retry patch와 runtime diagnostics가 실제로 적용됨
  - blank screenshot은 challenge shell 해석을 더 강하게 지지

### `pilot_20260315_014015`

- `sample_total=4`
- landing probe 쪽 AIP `10.1116/6.0003261`
  - `entry_strategy=aip_official_doi_resolve`
  - `entry_browser_kind=official_doi_redirect`
  - `landing_recovery_outcome=challenge_detected_no_retry`
  - final HTML은 Cloudflare `Just a moment...`
  - `tab_transition_count=0`, `ready_state=complete`
- download log 쪽 동일 DOI
  - AIP DOI-first branch 로그는 찍혔지만 metadata sidecar의 `landing_entry_*`는 비어 있었음
  - 이 차이로 combined summary는 AIP를 `blank_or_incomplete` 쪽으로 약하게 반영

### `aip_patch_validation_20260315_local`

- 실행 위치: `outputs/aip_patch_validation_20260315_local/landing/`
- 입력 DOI:
  - `10.1116/6.0004298`
  - `10.1063/5.0246311`
- 결과:
  - `sample_total=2`
  - `classifier_counts={"success_landing":2}`
  - 두 DOI 모두 `entry_strategy_variant=doi_redirect_only_no_article_preflight`
  - `entry_redirect_probe_mode=doi_location_only`
  - `entry_prebrowser_request_count=1`
  - `challenge_detected=false`
  - success artifact와 HTML/screenshot이 저장됨
- 해석
  - 새 AIP branch는 local headless에서 실제 browser runtime에 적용됐고, low-friction DOI landing이 stable article landing으로 이어졌다
  - 단, Linux server/headless 동일 조건 재검증은 아직 남아 있다 `[blocked]`

### `aip_micro_20260315_preflightoff`

- landing probe
  - `sample_total=2`
  - 두 DOI 모두 `challenge_detected=true`
  - branch는 실제로
    - `entry_strategy_variant=doi_redirect_only_no_article_preflight`
    - `entry_redirect_probe_mode=doi_location_only`
    - `entry_prebrowser_request_count=1`
    로 기록됨
  - blank screenshot은 challenge shell 해석이 더 강함
- download path
  - `10.1063/5.0207496`
    - `landing_title=New Tab`
    - Google default/new-tab screenshot
    - `landing_url=https://doi.org/...`
  - `10.1116/6.0003838`
    - challenge 쪽으로 종료
  - 해석
    - AIP latest failure에는 publisher challenge와 별개로 workflow-level default-page/tab-context 결함이 섞여 있었음

### `aip_structural_validation_20260315_local`

- landing-only
  - 실행 위치: `outputs/aip_structural_validation_20260315_local/landing/`
  - 입력 DOI:
    - `10.1063/5.0207496`
    - `10.1116/6.0004298`
  - 결과:
    - `sample_size=2`
    - `classifier_counts={"success_landing":2}`
    - blank / Google default page `0/2`
- download smoke
  - 실행 위치: `outputs/aip_structural_validation_20260315_local/download/`
  - 결과:
    - `10.1063/5.0207496`
      - `landing_state=success_landing`
      - `landing_default_page_detected=false`
      - download success
    - `10.1116/6.0004298`
      - `landing_state=success_landing`
      - `landing_default_page_detected=false`
      - 이후 click-stage browser disconnect
  - controlled rerun:
    - `outputs/aip_structural_validation_20260315_local/download_single_10.1116_6.0004298/`
    - 즉시 재시도에서는 `challenge_or_block`로 바뀌어 fail artifact가 저장됨
- 해석
  - structural patch 이후 local headless에서는 Google default page symptom이 사라졌다.
  - 다만 repeated DOI hit는 local에서도 challenge를 다시 유발할 수 있어, AIP 검증은 저빈도/회전이 필수다.

### `aip_micro_20260315_structural_patch_fresh`

- 실행 맥락
  - Linux `runtime_preset=linux_cli_seeded`
  - `execution_env=linux_server`
  - `profile_mode=auto`
  - `profile_name=Default`
  - persistent seed profile 검사 결과 `ok=true`
  - landing/download 모두 stateful clone 사용
- landing
  - 입력 DOI:
    - `10.1063/5.0246311`
    - `10.1116/6.0004298`
  - 결과:
    - `classifier_counts={"challenge_detected":2}`
    - 두 DOI 모두
      - `entry_strategy_variant=doi_redirect_only_no_article_preflight`
      - `entry_redirect_probe_mode=doi_location_only`
      - `entry_prebrowser_request_count=1`
      - `tab_transition_count=0`
      - `total_tab_count=1`
      - `title=Just a moment...`
      - challenge HTML dump 저장
  - 해석
    - fresh structural patch run에서도 Linux landing은 여전히 "single-tab challenge"였다.
    - 이 bundle에서는 Google default page symptom이 보이지 않았고, wrong-tab 근거도 없다.
- download
  - 결과:
    - 두 DOI 모두 `FAIL_BLOCK`
    - `landing_state=challenge_or_block`
    - `landing_default_page_detected=false`
    - `landing_tab_transition_count=0`
    - `landing_final_total_tab_count=1`
    - `browser_session_source=linux_seed_clone`
  - 해석
    - 최신 Linux failure는 challenge/interstitial이 주증상이고, default-page/tab bug는 재현되지 않았다.
    - 즉 current Linux failure의 우선 원인은 profile/session warming 부족 또는 publisher-side challenge이며, Google default page는 별도 구조 버그였을 가능성이 높다.
- profile/session reuse 관찰
  - landing report에는 `session_seed_root=null`이었다.
  - 현재 suite는 landing과 download를 별도 단계로 돌리며, 이번 run에서는 landing에서 생긴 세션 상태를 download가 이어받지 않았다.
  - download는 persistent Linux seed profile을 직접 clone한 `linux_seed_clone`만 사용했다.
- 배운 점
  - Ubuntu server에서 manual visit이 도움이 된다면, 그 효과는 우선
    - persistent seed profile에 challenge clearance/cookie/bootstrap state가 기록되었는지
    - 그 상태가 다음 clone에 전파되는지
    를 진단하는 신호로 읽어야 한다.
  - manual visit이 도움이 되더라도 landing flow가 구조적으로 해결됐다는 뜻은 아니다.

### `aip_profile_diag_20260315_local_temp`

- 실행 위치
  - `outputs/aip_profile_diag_20260315_local_temp/`
- 입력 DOI
  - `10.1063/5.0257779`
- 조건
  - `runtime_preset=local_mac`
  - `execution_env=desktop`
  - `profile_mode=temp`
- 결과
  - `sample_size=1`
  - `classifier_counts={"success_landing":1}`
- 해석
  - local AIP는 cold temp profile에서도 article landing이 성립했다.
  - 따라서 local success를 기존 persistent profile warming만으로 설명하는 건 부족하다.

### `aip_profile_diag_20260315_local_linuxseeded`

- 실행 위치
  - `outputs/aip_profile_diag_20260315_local_linuxseeded/`
- 입력 DOI
  - `10.1063/5.0257779`
- 조건
  - `runtime_preset=linux_cli_seeded`
  - `execution_env=linux_server`
  - `persistent_profile_dir=outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed`
- 결과
  - `sample_size=1`
  - `classifier_counts={"success_landing":1}`
- 해석
  - local 머신에서는 Linux-style seeded clone 경로도 AIP landing을 막지 않았다.
  - 최신 server failure는 code path 자체보다 server-side challenge 조건이 더 강할 가능성을 지지한다.

### `aip_context_bootstrap_validation_20260315_local`

- 실행 위치
  - `outputs/aip_context_bootstrap_validation_20260315_local/`
- 입력 DOI
  - `10.1063/5.0257779`
- 조건
  - `runtime_preset=linux_cli_seeded`
  - `execution_env=linux_server`
  - 새 `entry_context_url` / `entry_context_bootstrap_*` branch 포함
- 결과
  - `sample_size=1`
  - `classifier_counts={"success_landing":1}`
  - artifact에는
    - `entry_strategy_variant=doi_redirect_with_context_bootstrap_no_article_preflight`
    - `entry_context_url=https://pubs.aip.org/jcp`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_challenge`
    - `entry_context_bootstrap_final_title=Just a moment...`
    가 남음
- 해석
  - journal root 자체는 challenge였지만, 같은 세션의 뒤이은 DOI/article landing은 success였다.
  - context bootstrap은 "무조건 clean homepage"가 아니라 session initialization 단계로 읽는 편이 맞다.

### `aip_context_bootstrap_validation2_20260315_local`

- 실행 위치
  - `outputs/aip_context_bootstrap_validation2_20260315_local/`
- 입력 DOI
  - `10.1116/6.0004298`
- 결과
  - `sample_size=1`
  - `classifier_counts={"success_landing":1}`
  - artifact에는
    - `entry_context_url=https://pubs.aip.org/jva`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_ready`
    - `entry_context_bootstrap_final_title=Journal of Vacuum Science & Technology A | AIP Publishing`
    가 남음
- 해석
  - 어떤 DOI는 journal root가 clean landing이고, 어떤 DOI는 challenge shell이더라도 뒤이은 article landing이 가능했다.
  - 따라서 AIP context bootstrap은 low-friction publisher-page initialization 전략으로 유지할 가치가 있다.

### `aip_context_bootstrap_linux_20260315`

- 실행 위치
  - `outputs/linux_headless_suite_runs/aip_context_bootstrap_linux_20260315/`
- 입력 CSV
  - `outputs/_aip_profile_diag_20260315_input.csv`
- 결과
  - `status=skipped_retry_protection_all_rows`
  - `effective_sample_total=0`
  - `skip_reason_counts={"prior_success_exists":1}`
- 해석
  - 이번 server run은 AIP context bootstrap branch 평가로 이어지지 못했다.
  - 즉 새로운 성공/실패 근거는 추가되지 않았고, retry protection이 prior-success DOI 재사용을 막은 정상 동작으로 해석하는 편이 맞다.
  - 다음 server 검증은 fresh/low-frequency AIP DOI로 다시 구성해야 한다.

### `aip_context_bootstrap_linux_20260315_fresh`

- 실행 위치
  - bundle extract: `/tmp/aip_context_bootstrap_linux_20260315_fresh/`
  - run dir: `outputs/linux_headless_suite_runs/aip_context_bootstrap_linux_20260315_fresh/`
- 입력 DOI
  - `10.1116/6.0003316`
  - `10.1063/5.0188699`
- 실행 맥락
  - `runtime_preset=linux_cli_seeded`
  - `execution_env=linux_server`
  - `profile_mode=auto`
  - `profile_name=Default`
  - persistent seed profile 검사 결과 `seed_profile_ok=true`
  - execution manifest `status=completed_ok`
- landing
  - 결과:
    - `sample_total=2`
    - `classifier_counts={"challenge_detected":2}`
    - `combined_bucket_counts={"challenge_or_interstitial":2}`
  - 두 DOI 공통 artifact:
    - `entry_strategy_variant=doi_redirect_with_context_bootstrap_no_article_preflight`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_challenge`
    - `entry_context_bootstrap_final_title=Just a moment...`
    - `landing_recovery_outcome=challenge_detected_no_retry`
    - `tab_transition_count=0`
    - fail HTML에 `Just a moment`와 `__cf_chl_rt_tk`가 남음
  - DOI별 context page:
    - `10.1116/6.0003316` -> `entry_context_url=https://pubs.aip.org/jva`
    - `10.1063/5.0188699` -> `entry_context_url=https://pubs.aip.org/jap`
  - 해석
    - 이번 bundle은 새 AIP context-bootstrap patch가 "코드에만 존재"한 것이 아니라 Linux server runtime에서 실제로 적용됐음을 보여준다.
    - 가장 이른 분기 실패 지점은 DOI handoff 이후가 아니라 journal-root context bootstrap 자체였다.
    - 즉 이번 failure의 1차 원인은 wrong DOI entry path라기보다, Linux server/headless에서 AIP publisher first-contact가 바로 challenge로 평가되는 쪽에 더 가깝다.
    - Google default page, wrong active tab, tab-loss 가설은 이번 bundle 근거상 약하다. 두 DOI 모두 single-tab/no-transition 패턴으로 끝났다.
- download
  - 결과:
    - `integrated_landing.state_counts={"challenge_or_block":2}`
    - 두 DOI 모두 `FAIL_BLOCK`
    - merged summary 기준
      - `download_session_source=linux_seed_clone`
      - `download_challenge_detected=True`
      - `download_default_page_detected=False`
      - `download_tab_transition_count=0`
      - `download_final_total_tab_count=1`
  - 관찰
    - `download/run/metadata/*.json` sidecar 일부 필드는 이번 bundle에서도 `null`로 비어 있었다.
    - 반면 `summary/merged_results.csv`에는 landing/download challenge 필드가 채워져 있었다.
  - 해석
    - 원인 분석 자체에는 landing artifact와 merged summary가 충분했지만, download metadata sidecar만으로는 이번 run을 완전히 재구성할 수 없었다.
    - logging/summary 전파는 일부 경로에서 아직 일관되지 않을 수 있다.
- profile/session reuse 해석
  - landing/download 모두 `linux_seed_clone` 기반 stateful clone을 사용했다.
  - 즉 "persistent seed profile을 아예 안 썼다"는 가설은 이번 bundle과 맞지 않는다.
  - 다만 persistent seed clone만으로도 server-side first-contact challenge를 피하지는 못했다.
- 배운 점
  - AIP context bootstrap은 local에서는 session initialization 전략으로 의미가 있었지만, 최신 Linux server/headless fresh run에서는 challenge를 낮추지 못했다.
  - 이번 시점에서 AIP Linux failure의 주원인을 "entry path bug"로만 보는 것은 근거가 약하다.
  - 현재 더 강한 가설은
    - server/IP 조건에서의 AIP first-contact challenge
    - warmed session/bootstrap state 부재
    - 또는 그 둘의 결합
    이다.
  - 따라서 다음 단계는 retry를 더 쌓기보다
    - manual warm-state 진단
    - seed/profile state carry-over 검증
    - server와 local의 session initialization 차이 비교
    쪽이 우선이다.

### `aip_publisher_direct_validation_20260315_local`

- 실행 위치
  - `outputs/aip_publisher_direct_validation_20260315_local/`
- 입력 DOI
  - `10.1063/5.0246311`
- 왜 테스트했나
  - 최신 server failure는 `context bootstrap`까지 적용돼도 browser가 여전히 `https://doi.org/...`를 열고 있었다.
  - local에서는 `context_challenge`가 떠도 뒤이은 article landing이 성공한 사례가 있었으므로, browser-side `doi.org` navigation 자체가 challenge pressure를 더 키우는지 분리할 필요가 있었다.
  - 따라서 Linux/server 계열 기본 browser entry를 `doi.org`가 아닌 resolve된 AIP canonical article/article-abstract URL로 바꾸는 구조 patch를 검증했다.
- 적용 패치
  - `tools_exp.py`
    - AIP entry plan에 `_resolve_aip_browser_entry_mode()` 추가
    - Linux `runtime_preset=linux_cli_seeded` 또는 `execution_env=linux_server`이고 `entry_context_url`과 canonical article URL이 있으면 browser entry 기본값을 `publisher_direct`로 전환
    - 새 runtime variant:
      - `publisher_canonical_with_context_bootstrap_no_article_preflight`
      - `publisher_canonical_no_context_no_article_preflight`
    - logger에 `browser_kind`, `context_url` 출력 추가
- 결과
  - `sample_size=1`
  - `classifier_counts={"success_landing":1}`
  - runtime artifact에는
    - `entry_strategy_variant=publisher_canonical_with_context_bootstrap_no_article_preflight`
    - `entry_browser_url=https://pubs.aip.org/apl/article-abstract/...`
    - `entry_browser_kind=canonical_article_abstract`
    - `entry_context_url=https://pubs.aip.org/apl`
    - `entry_context_bootstrap_outcome=context_challenge`
    가 남음
  - navigation chain은
    - `aip_resolve`
    - `aip_context_bootstrap`
    - `doi_get` 단계에서 실제 요청 URL이 `https://pubs.aip.org/apl/article-abstract/...`
    순서로 기록됨
- 해석
  - 새 patch는 runtime에서 실제로 적용됐고, browser는 더 이상 `doi.org`를 열지 않았다.
  - local 기준으로는 journal-root가 challenge shell이어도, 같은 세션에서 canonical publisher article entry로 바로 들어가면 stable article landing이 가능했다.
  - 즉 `context bootstrap + publisher direct article entry`는 AIP에 대해 legitimate하고 low-friction한 전략으로 유지할 가치가 있다.
  - 다만 이것이 Linux server/headless에서도 같은 효과를 내는지는 아직 미검증이다 `[blocked]`.

### `aip_publisher_direct_linux_20260315_fresh`

- 실행 위치
  - `outputs/linux_headless_suite_runs/aip_publisher_direct_linux_20260315_fresh/`
- 입력 CSV
  - `outputs/_aip_context_bootstrap_fresh_20260315.csv`
- 결과
  - `status=skipped_retry_protection_all_rows`
  - `effective_sample_total=0`
  - `skip_reason_counts={"prior_hard_block_exists":2}`
  - landing/download/summarize 모두 `skipped=true`
- 해석
  - 이번 server run은 새 `publisher_direct` AIP branch를 평가하지 못했다.
  - 이유는 branch 로직이 아니라 입력 DOI 둘 모두가 이미 hard-block ledger 이력을 갖고 있었기 때문이다.
  - 따라서 이번 결과로는 `publisher_canonical_with_context_bootstrap_no_article_preflight`가 Linux server/headless에서 성공했는지 실패했는지 판단할 수 없다.
  - 다음 server 검증은 반드시 fresh/low-frequency AIP DOI로 다시 구성해야 한다.

### `aip_publisher_direct_linux_20260315_fresh2`

- 실행 위치
  - `outputs/linux_headless_suite_runs/aip_publisher_direct_linux_20260315_fresh2/`
- 입력 CSV
  - `outputs/_aip_publisher_direct_fresh2_20260315.csv`
- 결과
  - fresh CSV 생성 단계에서 `rows=0`
  - 생성된 입력 CSV는 header만 있고 DOI row가 없었다.
  - run 결과:
    - `status=skipped_retry_protection_all_rows`
    - `source_sample_total=0`
    - `effective_sample_total=0`
    - `skipped_total=0`
    - landing/download/summarize 모두 `skipped=true`
- 해석
  - 이번 server run은 retry protection에 막힌 것도 아니고 branch가 실패한 것도 아니다.
  - 더 근본적으로, 현재 `experiment/linux_headless_suite/full_sample.csv` 안에서 server ledger 기준 fresh AIP DOI 후보가 이미 고갈된 상태였다.
  - 따라서 현 시점의 blocker는 code path가 아니라 fresh DOI pool 부재다.
  - 이 상태에서는 같은 in-repo sample만 재조합해도 AIP publisher-direct Linux validation을 더 진행할 수 없다 `[blocked]`.

### 5.15 AIP context-challenge handoff 분리와 empty-source status 분리

- 구조 문제
  - 최신 AIP server bundle들은 `context bootstrap -> challenge`가 먼저 발생했고, 그 뒤 entry navigation도 같은 tab/page를 그대로 사용했다.
  - 이 구조에서는 challenge shell 컨텍스트와 article navigation 컨텍스트가 분리되지 않아, legitimate publisher-direct entry를 쓰더라도 page lifecycle이 challenge shell에 묶일 위험이 있었다.
  - 별도로, `source_sample_total=0`인 경우에도 suite status가 `skipped_retry_protection_all_rows`처럼 보이면서 "retry protection이 막았는지", "입력 샘플이 비었는지"가 구분되지 않았다.
- 적용 패치
  - `tools_exp.py`
    - `_aip_context_challenge_fresh_tab_enabled()` 추가
    - `_prepare_aip_entry_navigation_page()` 추가
    - `context_bootstrap_outcome=context_challenge`이고 browser entry가 canonical AIP article/article-abstract URL이면, Linux/server 계열에서 fresh tab으로 handoff 후 navigation 하도록 변경
    - download path에도 같은 handoff 로직 적용
  - `landing_access_repro.py`
    - 같은 fresh-tab handoff 로직 적용
    - 결과 JSONL에 `entry_navigation_route` 기록 추가
  - `parallel_download.py`
    - download 결과/CSV에 `landing_entry_navigation_route` 전파 추가
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 `landing_entry_navigation_route`, `download_entry_navigation_route` 추가
  - `experiment/run_linux_headless_suite.py`
    - `source_sample_total=0`이면 `blocked_empty_source_sample` 상태로 분기
    - stage skip reason도 `empty_source_sample`로 명시
- local 검증
  - `outputs/aip_publisher_direct_handoff_validation_20260315_local/`
    - DOI `10.1063/5.0246311`
    - `success_landing`
    - `entry_strategy_variant=publisher_canonical_with_context_bootstrap_no_article_preflight`
    - `entry_navigation_route=same_tab`
    - 이번 run에서는 `entry_context_bootstrap_outcome=context_ready`
  - `outputs/aip_publisher_direct_handoff_validation2_20260315_local/`
    - DOI `10.1063/5.0257779`
    - `success_landing`
    - `entry_strategy_variant=publisher_canonical_with_context_bootstrap_no_article_preflight`
    - `entry_navigation_route=same_tab`
    - 이번 run에서도 `entry_context_bootstrap_outcome=context_ready`
  - `outputs/linux_headless_suite_runs/empty_source_status_validation_20260315/`
    - empty CSV로 실행
    - `status=blocked_empty_source_sample`
    - landing/download/summarize skip reason이 모두 `empty_source_sample`
- 해석
  - AIP publisher-direct entry는 local headless에서 계속 stable landing을 만들고 있다.
  - 다만 새 fresh-tab handoff branch는 이번 local 검증 2건에서 context page가 모두 `context_ready`여서 live `context_challenge` 케이스로는 아직 실제 실행되지 않았다 `[blocked]`.
  - empty-input 상태는 이제 retry-protection skip과 구분돼, 최신 `fresh2` 같은 bundle을 더 정확하게 해석할 수 있게 됐다.

### 5.16 AIP repeated context-bootstrap 억제와 cache-state 진단 추가

- 문제 재정의
  - 최신 Linux server 실패 근거 `aip_context_bootstrap_linux_20260315_fresh`에서는 worker 1개가 같은 stateful clone 세션에서 DOI 2건을 순차 처리했고, 두 DOI 모두
    - `entry_context_bootstrap_outcome=context_challenge`
    - `entry_context_url=https://pubs.aip.org/<journal>`
    - single-tab / no-transition
    로 끝났다.
  - 즉 당시 workflow는 첫 DOI에서 journal root first-contact challenge를 이미 본 뒤에도, 같은 세션의 다음 DOI에서 journal root context bootstrap을 다시 시도하고 있었다.
  - local에서는 context bootstrap이 initialization에 도움이 될 수 있었지만, Linux server에서 이미 `context_challenge`가 난 세션에 같은 journal root를 다시 치는 것은 challenge pressure만 늘리고 이득이 적다.
- 적용 패치
  - `tools_exp.py`
    - `_AIP_CONTEXT_BOOTSTRAP_CACHE`를 `Set[str]`에서 `Dict[str, str]`로 바꿨다.
    - `_maybe_bootstrap_aip_entry_context()`가 session+context 단위로
      - `context_ready`
      - `context_challenge`
      를 cache에 기록하도록 수정했다.
    - 같은 세션에서 이미 `context_challenge`가 기록된 context URL이면 journal root를 다시 열지 않고
      - `entry_context_bootstrap_outcome=skipped_after_prior_context_challenge`
      - `entry_context_bootstrap_cache_hit=true`
      - `entry_context_bootstrap_cache_state=context_challenge`
      로 남긴 뒤 article navigation 단계로 바로 넘어가도록 했다.
  - `landing_access_repro.py`
    - attempt timing에 context-bootstrap cache hit/state를 추가 기록
  - `parallel_download.py`
    - download 결과/CSV에 context-bootstrap cache hit/state 전파 추가
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 landing/download context-bootstrap cache hit/state 추가
- 왜 이 패치인가
  - 이 수정은 challenge bypass가 아니라, 이미 "도움이 안 된다"고 관찰된 AIP journal-root context bootstrap을 같은 세션에서 반복하지 않게 만드는 것이다.
  - 특히 `fresh DOI 2건 / worker 1 / same stateful clone` 구성의 micro-run에서, 첫 DOI가 session을 이미 `context_challenge` 상태로 만들었다면 둘째 DOI는 publisher-direct article landing만 평가하도록 분기시키는 효과가 있다.
- 검증
  - 정적 검증:
    - `python -m py_compile tools_exp.py landing_access_repro.py parallel_download.py experiment/summarize_linux_headless_suite.py experiment/run_linux_headless_suite.py`
  - lightweight cache-branch smoke:
    - `PDF_BROWSER_AIP_CONTEXT_BOOTSTRAP=1 PDF_BROWSER_RUNTIME_PRESET=linux_cli_seeded PDF_BROWSER_EXECUTION_ENV=linux_server python - <<'PY' ...`
    - cached `context_challenge` -> `skipped_after_prior_context_challenge`
    - cached `context_ready` -> `reused_existing_session_context`
  - 런타임 검증:
    - 아직 fresh Linux server AIP DOI가 없어 미실행 `[blocked]`
- 해석
  - 이 patch는 "Linux server에서 AIP landing을 성공시켰다"는 증거가 아니다.
  - 다만 최신 실패에서 확인된 구조적 약점
    - same-session repeated context bootstrap after prior challenge
    를 직접 겨냥한 저위험 수정이다.
  - 다음 server micro-run에서는
    - 첫 DOI: `entry_context_bootstrap_outcome=context_challenge`
    - 같은 session의 다음 DOI: `entry_context_bootstrap_outcome=skipped_after_prior_context_challenge`
      또는 cache hit/state가 artifact/summary에 남는지
    를 우선 확인해야 한다.

### 5.17 AIP first-contact article-first reorder (initial context bootstrap deferred)

- 가설
  - 최신 Linux server 실패에서 가장 일관된 earliest failure는 article landing 이전의 journal-root context bootstrap이었다.
  - 따라서 Linux/server에서 `publisher_direct` canonical article entry가 가능할 때는
    - journal-root bootstrap을 first-contact 기본 경로에서 제거하고
    - canonical article/article-abstract entry를 먼저 시도하는 것이
    - 더 낮은 압력의 legitimate landing path일 수 있다.
- 왜 이 가설을 세웠나
  - `aip_context_bootstrap_linux_20260315_fresh`
    - `entry_context_bootstrap_outcome=context_challenge` 2/2
    - `challenge_detected=true` 2/2
    - `tab_transition_count=0`
    - `total_tab_count=1`
    - fail HTML에 `__cf_chl_rt_tk`
  - 즉 당시 실패는 wrong-tab보다 "journal root first-contact가 너무 이르게 challenge로 평가됨" 쪽이 더 강했다.
  - 반면 local `aip_publisher_direct_validation_20260315_local`에서는
    - `publisher_canonical_with_context_bootstrap_no_article_preflight`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_challenge`
    상태에서도 final article landing은 success였다.
  - 이 차이는 "context bootstrap이 반드시 필요한가"보다 "Linux server에서 first-contact surface를 줄여야 하는가"를 먼저 보게 만들었다.
- 적용 패치
  - `tools_exp.py`
    - `_resolve_aip_context_bootstrap_mode()` 추가
    - Linux/server + `publisher_direct` canonical entry이면 기본 `entry_context_bootstrap_mode=deferred`
    - 새 runtime variant:
      - `publisher_canonical_context_deferred_no_article_preflight`
    - `_maybe_bootstrap_aip_entry_context()`가 `deferred` mode에서는 journal-root를 열지 않고
      - `entry_context_bootstrap_attempted=false`
      - `entry_context_bootstrap_outcome=deferred_initial_bootstrap`
      로 반환하도록 변경
    - logger에 `context_mode` 추가
  - `landing_access_repro.py`
    - landing artifact JSONL에 `entry_context_bootstrap_mode` 추가
  - `parallel_download.py`
    - download 결과/CSV에 `landing_entry_context_bootstrap_mode` 추가
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 landing/download context-bootstrap mode 추가
- 통제 검증
  - 입력 CSV:
    - `outputs/_aip_first_contact_deferred_validation_20260319_input.csv`
    - DOI
      - `10.1063/5.0293851`
      - `10.1063/5.0114275`
  - 실행:
    - `python landing_access_repro.py --input outputs/_aip_first_contact_deferred_validation_20260319_input.csv --workers 1 --headless 1 --runtime-preset linux_cli_seeded --execution-env linux_server --profile-mode auto --profile-name Default --persistent-profile-dir outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed --artifact-dir outputs/aip_first_contact_deferred_validation_20260319_local/artifacts --capture-fail-screenshot 1 --capture-success-artifacts 1 --capture-success-html 1 --output-jsonl outputs/aip_first_contact_deferred_validation_20260319_local/landing_access_repro.jsonl --report outputs/aip_first_contact_deferred_validation_20260319_local/landing_access_repro_report.json --report-md outputs/aip_first_contact_deferred_validation_20260319_local/landing_access_repro_report.md`
  - 결과:
    - `outputs/aip_first_contact_deferred_validation_20260319_local/landing_access_repro_report.json`
    - `sample_size=2`
    - `classifier_counts={"success_landing":2}`
    - 두 DOI 모두
      - `entry_strategy_variant=publisher_canonical_context_deferred_no_article_preflight`
      - `entry_context_bootstrap_mode=deferred`
      - `entry_context_bootstrap_attempted=false`
      - `entry_context_bootstrap_outcome=deferred_initial_bootstrap`
      - `challenge_detected=false`
      - `tab_transition_count=0`
      - stable article landing success
- before vs after
  - before local publisher-direct validation:
    - `outputs/aip_publisher_direct_validation_20260315_local/landing_access_repro.jsonl`
    - `entry_strategy_variant=publisher_canonical_with_context_bootstrap_no_article_preflight`
    - `entry_context_bootstrap_attempted=true`
    - `entry_context_bootstrap_outcome=context_challenge`
    - landing success `1/1`
  - after deferred-first-contact validation:
    - `outputs/aip_first_contact_deferred_validation_20260319_local/landing_access_repro.jsonl`
    - `entry_context_bootstrap_attempted=false` `2/2`
    - `entry_context_bootstrap_outcome=deferred_initial_bootstrap` `2/2`
    - landing success `2/2`
  - server baseline:
    - `aip_context_bootstrap_linux_20260315_fresh`
    - `context_challenge` `2/2`
    - landing success `0/2`
- 배운 점
  - AIP journal-root bootstrap은 local에선 도움이 될 수 있었지만, Linux server failure evidence 기준으로는 first-contact 기본 경로로 두기엔 너무 challenge-prone하다.
  - publisher-direct canonical article entry는 legitimate하고 더 낮은 압력의 first-contact 후보로 유지할 가치가 있다.
  - 이번 patch는 "context bootstrap을 제거"한 것이 아니라, "initial first-contact ordering에서 뒤로 미룬 것"이다.
  - local 검증 기준으로는 strategy branch가 의도대로 바뀌었고, bootstrap challenge surface를 first-contact에서 제거할 수 있었다.
  - 다만 Linux server/IP에서 실제 `context_challenge` 빈도가 줄어드는지는 아직 별도 fresh DOI 검증이 필요하다 `[blocked]`.

## 5.18 Retry protection explicit blocking 제거

- 배경
  - Linux server 실험에서 가장 자주 나온 launcher 상태 중 하나가
    - `status=skipped_retry_protection_all_rows`
  - 대표 로그:
    - `retry_protection={"enabled": true, "max_attempts_per_doi": 1, "retry_cooldown_hours": 72, "allow_success_reruns": false, "allow_hard_block_reruns": false, "allow_repeated_attempts": false, "source_sample_total": 2, "effective_sample_total": 0, "skipped_total": 2, "action_counts": {"skipped_due_to_retry_protection": 2}, "skip_reason_counts": {"max_attempts_reached": 1, "prior_hard_block_exists": 1}}`
  - 이 상태는 실제 AIP landing branch 평가 전에 sample 자체를 비워 버렸고, 결과적으로 strategy 개선 여부보다 "실행이 됐는가"가 먼저 막혔다.
- 원인 코드
  - `experiment/linux_headless_suite_lib.py`
    - `apply_retry_protection()`
    - 아래 이유들에서 `keep_row=False`로 row를 sample에서 제거하고 있었다.
      - `prior_success_exists`
      - `prior_hard_block_exists`
      - `max_attempts_reached`
      - `cooldown_active`
  - `experiment/run_linux_headless_suite.py`
    - `effective_sample_rows = retry_protection["allowed_rows"]`
    - `skipped_sample_rows = retry_protection["skipped_rows"]`
    - `len(effective_sample_rows) == 0`이면
      - `status=skipped_retry_protection_all_rows`
      - stage reason `all_rows_skipped_retry_protection`
      로 종료했다.
- 적용 패치
  - `experiment/linux_headless_suite_lib.py`
    - retry protection을 blocking에서 annotate-only로 변경
    - 반복/성공/하드블록/쿨다운 이력은 여전히
      - `retry_protection_action`
      - `retry_protection_reason`
      로 기록하지만
    - row를 sample에서 제거하지 않도록 수정
    - `skipped_rows=[]` 고정
    - 새 요약 필드:
      - `repeated_reason_counts`
  - `experiment/run_linux_headless_suite.py`
    - execution manifest의 `retry_protection`을
      - `enabled=false`
      - `mode=annotate_only`
      로 표기
    - `skipped_retry_protection_all_rows` 및 `all_rows_skipped_retry_protection` 종료 경로 제거
    - source sample이 비지 않은 한 seed check와 실험 stage가 실제로 진행되도록 변경
- 검증
  - 문법 검증:
    - `python -m py_compile experiment/linux_headless_suite_lib.py experiment/run_linux_headless_suite.py experiment/build_linux_headless_suite.py experiment/summarize_linux_headless_suite.py`
  - 함수 단위 smoke:
    - prior attempt 3회, success 1회, hard block 1회인 DOI를 넣고 `apply_retry_protection()` 호출
    - 결과:
      - `allowed=1`
      - `skipped=0`
      - `action=repeated_attempt`
      - `reason=prior_success_exists`
      - `repeated_reason_counts={"prior_success_exists": 1}`
- before vs after
  - before:
    - 반복 DOI는 `effective_sample_total=0`이 될 수 있었고
    - suite 자체가 실행되지 않았다.
  - after:
    - 같은 DOI도 sample에 남고
    - 이전 이력은 annotate-only로 남는다.
    - 즉 "연속 시도 차단"은 제거되고 "연속 시도 기록"만 남는다.
- 배운 점
  - 현재 AIP 문제는 first-contact challenge 자체가 핵심인데, retry protection이 실험 관찰을 더 자주 막고 있었다.
  - attempt ledger는 진단용 metadata로는 유용하지만, row exclusion policy로 쓰면 AIP 원인 검증을 방해한다.
  - candidate selection에서 prior attempt를 정렬 키로 쓰는 부분은 남아 있다. 이건 차단이 아니라 우선순위 편향이므로 이번 변경 범위에서는 유지했다.

## 5.19 Latest `experiment/results` AIP failure 재분석: JS/cookie 메시지 해석과 article-first 세부 조정

- 새로 분석한 증거
  - 기준 artifact:
    - `experiment/results/aip_first_contact_deferred_linux_20260319_bundle`
  - 대표 fail JSONL/HTML:
    - `experiment/results/aip_first_contact_deferred_linux_20260319_bundle/outputs/linux_headless_suite_runs/aip_first_contact_deferred_linux_20260319/landing/landing_access_repro.jsonl.worker0.jsonl`
    - `.../landing/artifacts/fail/landing_fail_10.1063_5.0207496_1773890191933.html`
    - `.../landing/artifacts/fail/landing_fail_10.1116_6.0004298_1773890292536.html`
- 확인된 사실
  - 최신 server run에서 새 deferred branch는 실제 runtime에 탔다.
    - `entry_strategy_variant=publisher_canonical_context_deferred_no_article_preflight`
    - `entry_context_bootstrap_outcome=deferred_initial_bootstrap`
    - 즉 "context bootstrap을 먼저 열지 않는" patch는 코드에만 있던 것이 아니라 실제로 실행되었다.
  - 두 DOI 모두 first-contact 경로는
    - `about:blank`
    - `doi.org`
    - `pubs.aip.org/.../article/...`
    - `pubs.aip.org/.../article-abstract/...`
    - `pubs.aip.org/.../article-abstract/...?...__cf_chl_rt_tk=...`
    순으로 끝났다.
  - challenge는 final landing classification 이전에 이미 article-direct first-contact에서 발생했다.
    - `title=Just a moment...`
    - `issue=FAIL_BLOCK`
    - `landing_recovery_outcome=challenge_detected_no_retry`
  - worker JSONL에 남은 runtime diagnostics 기준:
    - `ready_state=complete`
    - `console_runtime_errors.capture_available=true`
    - `error_events=[]`
    - 즉 JS runtime probe 자체는 실행됐다.
  - fail HTML에는 공통으로
    - `Enable JavaScript and cookies to continue`
    - `window._cf_chl_opt`
    - `/cdn-cgi/challenge-platform`
    - `__cf_chl_rt_tk`
    가 있었다.
- 해석
  - `Enable JavaScript and cookies to continue`는 현재 근거상 "브라우저에서 JS가 꺼져 있다"는 직접 증거가 아니다.
  - 더 강한 해석은:
    - Cloudflare challenge shell의 `<noscript>` 안내 문구가 저장된 것이고
    - challenge 페이지가 그렇게 말하고 있을 뿐이다.
  - 이 해석을 지지하는 근거:
    - 현재 런치 코드에는 `disable-javascript`류 플래그가 없다.
    - headless Chrome은 `--headless=new`로 뜬다.
    - latest worker JSONL의 runtime probe가 정상 실행됐다.
    - fail HTML 안에 challenge orchestration script와 `window._cf_chl_opt`가 같이 있다.
  - 따라서 현재 AIP Linux/server 실패에서 JS/cookie는
    - "완전히 비활성화된 직접 원인"보다는
    - "first-contact challenge shell이 내는 표면 메시지"일 가능성이 더 높다.
  - 다만 현재 JSONL/merged summary에는
    - `navigator.cookieEnabled`
    - cookie jar count
    - profile cookie DB writable 여부
    처럼 진단에 직접 필요한 값이 없어서, cookie/session이 실제로 기대대로 attach/reuse됐는지까지는 즉시 읽기 어려웠다.
- 추가 가설
  - latest fail 2건은 둘 다 `entry_browser_kind=canonical_article_abstract`였다.
  - resolve 결과는 이미 canonical `/article/...` URL이었는데, browser first-contact 전에 `/article-abstract/...`로 다시 정규화됐다.
  - Linux/server에선 이 synthetic abstract rewrite가 불필요한 redirect/variation surface를 늘려 challenge pressure를 키울 수 있다.
  - 그래서 Linux/server 기본 AIP first-contact는 abstract보다 resolve된 canonical article URL을 우선하는 편이 더 낮은 압력의 legitimate entry일 수 있다.
- 적용 패치
  - `tools_exp.py`
    - `_resolve_aip_prefer_abstract()` 추가
    - `PDF_BROWSER_LANDING_AIP_PREFER_ABSTRACT=auto` 기본 의미를 변경
      - local/non-server: `abstract`
      - Linux/server: `article`
    - 즉 Linux/server에서는 DOI resolve가 준 canonical `/article/...` URL을 기본 first-contact로 사용
    - 새 진단 필드:
      - `entry_url_preference`
  - `landing_access_repro.py`
    - runtime diagnostics에 아래를 추가
      - `js_runtime_probe_ok`
      - `js_probe_error`
      - `navigator_cookie_enabled`
      - `document_cookie_len`
      - `challenge_script_present`
      - `cf_chl_opt_present`
      - `noscript_cookie_hint_present`
      - `cookie_jar_probe_ok`
      - `cookie_jar_count`
      - `aip_cookie_count`
      - `cloudflare_cookie_count`
      - `profile_cookie_db_exists`
      - `profile_cookie_db_writable`
      - `profile_storage_exists`
      - `profile_preferences_exists`
    - 위 필드들을 top-level result에도 올려서 JSONL/artefact에서 바로 비교 가능하게 함
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 위 landing JS/cookie/profile fields와 `landing_entry_url_preference` 추가
- 검증
  - 문법 검증:
    - `python -m py_compile tools_exp.py landing_access_repro.py experiment/summarize_linux_headless_suite.py`
  - helper smoke:
    - Linux/server env auto mode에서
      - `server_auto_prefer_abstract=False`
      - normalized entry URL이 `/article/...`
    - 강제 override `PDF_BROWSER_LANDING_AIP_PREFER_ABSTRACT=1`에서는 `/article-abstract/...`
  - latest artifact reinterpretation smoke:
    - latest fail HTML 두 건 모두
      - `Enable JavaScript and cookies to continue=True`
      - `window._cf_chl_opt=True`
    - latest worker JSONL 두 건 모두
      - `ready_state=complete`
      - `console_capture=True`
      - JS runtime errors `[]`
- before vs after
  - before:
    - Linux/server deferred-first-contact branch도 `entry_browser_kind=canonical_article_abstract`
    - JS/cookie 진단은 artifact raw HTML을 직접 읽어야만 해석 가능
  - after:
    - Linux/server 기본 AIP first-contact는 canonical article 우선
    - 다음 run부터 JS/cookie/profile 상태가 JSONL/summary에 직접 남는다
- 배운 점
  - latest fail evidence 기준으로 `Enable JavaScript and cookies to continue`는 root cause라기보다 challenge shell의 symptom에 가깝다.
  - JS/cookie를 "켜는" patch보다
    - challenge-prone first-contact surface를 줄이고
    - 실제 JS/cookie/profile 상태를 명시적으로 기록하는 patch가 더 근거 있다.
  - 이번 patch는 runtime visibility를 높이고 Linux/server entry path를 더 단순한 canonical article 쪽으로 줄였지만, 아직 fresh DOI로 server micro-run을 다시 돌린 증거는 없다 `[blocked]`.

## 5.20 Server operator transcript에서 확인된 환경 사실과 운영 제약

- 출처
  - 아래 항목은 repo file이 아니라 2026-03-19 operator shell transcript에서 직접 확인된 사실이다.
  - 목적은 "디스크 설정"과 "실제 서버 런타임"을 구분하고, 이후 정보 수집 명령을 서버 기본 명령만으로 다시 설계하기 위함이다.

- 서버 기본 환경
  - host:
    - `n137.hpc`
  - OS:
    - `Fedora Linux 36 (Workstation Edition)`
  - kernel / arch:
    - `Linux 6.6.13-200.fc36.x86_64`
    - `x86_64`
  - 세션:
    - `DISPLAY=localhost:10.0`
    - `XDG_SESSION_TYPE=tty`
    - `SSH_CONNECTION` 존재
    - `xdg-open` 존재
    - `xvfb-run` 없음
  - 해석:
    - "완전한 no-GUI 서버"로 단정할 수는 없고, 적어도 X11 forwarding 환경은 있다.
    - 다만 실제 GUI manual visit이 유효하게 수행됐는지는 아직 확인되지 않았다 `[blocked]`.

- 실제 AIP run identity
  - latest run:
    - `RUN_NAME=aip_article_first_js_cookie_diag_20260319`
  - branch / commit:
    - `codex/linux_exp`
    - `ecb48064`

- 실제 runner와 런치 경로
  - `logs/aip_article_first_js_cookie_diag_20260319.cmd.sh` 기준 환경:
    - `SEED_PROFILE=/home/yongyong0206/paper_search/paper_download/outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed`
    - `PROFILE_NAME=Default`
    - `CHROME_PATH=/home/yongyong0206/chrome-linux/chrome-linux64/chrome`
  - 같은 cmd.sh 기준 실행 명령:
    - `experiment/run_linux_headless_suite.py`
    - `--runtime-preset linux_cli_seeded`
    - `--execution-env linux_server`
    - `--headless 1`
    - `--sample-csv outputs/_aip_structural_validation_20260315_input.csv`
  - `outputs/linux_headless_suite_runs/aip_article_first_js_cookie_diag_20260319/run_suite.sh` 기준 stage:
    - `landing_access_repro.py`
    - `parallel_download.py`
    - `experiment/summarize_linux_headless_suite.py`
  - 해석:
    - 실제 run path는 wrapper script를 통해 env를 주입하고 있었고, interactive shell의 현재 env와 동일하지 않았다.

- interactive shell env의 함정
  - operator shell에서 직접 확인된 값:
    - `CHROME_PATH=`
    - `SEED_PROFILE=`
    - `PROFILE_NAME=`
  - 즉 interactive shell은 run wrapper env를 자동 상속하지 않았다.
  - 이 때문에 아래 점검들은 무효 또는 false negative였다.
    - `scripts/check_linux_seed_profile.py --profile-root "$SEED_PROFILE" ...`
      - 실제 seed가 아니라 repo root `/home/yongyong0206/paper_search/paper_download`를 검사
    - manual GUI visit command
      - `"$CHROME_PATH"`가 빈 값인 상태에서 실행돼 유효한 수동 방문 증거가 되지 못함
    - JS/cookie smoke
      - `KeyError: 'CHROME_PATH'`
  - 배운 점:
    - 이후 server fact-gathering은 항상
      - wrapper cmd.sh에서 env를 읽거나
      - 필요한 env를 명시적으로 export한 뒤
      - 검증해야 한다.

- 최신 live AIP JSONL에서 확인된 실제 runtime path
  - `outputs/linux_headless_suite_runs/aip_article_first_js_cookie_diag_20260319/landing/landing_access_repro.jsonl`
  - 두 DOI 모두:
    - `browser_user_data_dir=/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
    - `browser_profile_name=Default`
    - `browser_session_mode=stateful`
    - `browser_session_source=linux_seed_clone`
    - `title=Just a moment...`
    - `challenge_detected=true`
  - 중요:
    - 이번 run은 이미 `article-abstract`가 아니라 canonical `article` URL을 first-contact로 사용했다.
      - `10.1063/5.0207496`
        - `entry_browser_url=https://pubs.aip.org/jcp/article/160/20/204701/3294391/...`
      - `10.1116/6.0004298`
        - `entry_browser_url=https://pubs.aip.org/jva/article/43/3/032401/3339646/...`
    - 최종 URL은 둘 다 `__cf_chl_rt_tk`가 붙은 challenge URL이었다.
  - 해석:
    - latest patch는 실제 access path를 바꿨다.
    - 그럼에도 identical outcome이 반복됐으므로, 문제는 이제 단순 entry path보다
      - profile/session reuse
      - cookie persistence
      - manual reproducibility on same server/profile
      - network/server-IP conditions
      쪽으로 더 무게가 간다.

- package / command 제약
  - `rg` 미설치
  - `chromedriver` 미설치
  - 둘 다 package install prompt는 떴지만 auth 부족으로 설치 실패
  - 해석:
    - 이후 operator-facing checklist는
      - `grep`
      - `find`
      - `sed`
      - `ps`
      - `stat`
      - 이미 있는 `python`
      - 이미 있는 Chrome binary
      만으로 구성해야 한다.
    - `rg`, `chromedriver`, 새 package 설치를 전제로 한 안내는 피해야 한다.

- artifact 위치
  - live outputs:
    - `outputs/linux_headless_suite_runs/aip_article_first_js_cookie_diag_20260319/...`
  - operator transcript에서 확인된 file들:
    - `execution_manifest.json`
    - `landing/landing_access_repro.jsonl`
    - `landing/landing_access_repro_report.json`
    - `logs/*.log`
    - `summary/suite_summary.json`
  - shallow listing 기준 `experiment/results`:
    - `experiment/results/aip_first_contact_deferred_linux_20260319_bundle.tar.gz`
  - 해석:
    - 현재 server shell에서 즉시 분석 가능한 1차 evidence는 `outputs/linux_headless_suite_runs/...` 아래 live run dir이다.
    - `experiment/results`는 curated bundle 저장소로 유지하되, live diagnosis 자체는 outputs를 기준으로 보는 편이 실용적이다.

- 현재 남은 미확정 항목
  - `CHROME_PATH` binary version은 아직 operator shell에서 실제 binary로 재확인되지 않았다 `[blocked]`.
  - 같은 server / same profile / same browser에서 manual AIP visit이 실제로 어떤 화면이 되는지는 아직 유효하게 측정되지 않았다 `[blocked]`.
  - generic JS execution / generic cookie persistence도 아직 유효한 env로 재측정되지 않았다 `[blocked]`.

## 5.21 Server follow-up facts: live Chrome path, cookie persistence, manual GUI 실패

- 출처
  - 2026-03-19 operator shell follow-up transcript
  - 목적:
    - 이전 turn에서 비어 있던 env를 실제 run 값으로 다시 export한 뒤
    - browser/profile/runtime behavior를 재측정

- 재확인된 런타임 값
  - `CHROME_PATH=/home/yongyong0206/chrome-linux/chrome-linux64/chrome`
  - `SEED_PROFILE=/home/yongyong0206/paper_search/paper_download/outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed`
  - `PROFILE_NAME=Default`
  - `WORKER_DIR=/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
  - browser binary:
    - `Google Chrome for Testing 146.0.7680.66`

- seed / worker profile 상태
  - `scripts/check_linux_seed_profile.py`
    - seed profile: `ok=true`
    - worker clone profile: `ok=true`
  - both profiles had:
    - `Local State`
    - `Default/Preferences`
    - `Default/Cookies`
    - storage dirs
  - 해석:
    - seed profile 자체가 비어 있거나 손상된 것은 아니다.
    - worker clone 디스크 구조도 정상이다.

- generic cookie persistence test
  - command:
    - headless Chrome `--user-data-dir=$WORKER_DIR --profile-directory=Default --dump-dom https://httpbin.org/cookies/set?...`
    - then `https://httpbin.org/cookies`
  - 결과:
    - `codex_cookie_probe=1`가 set/readback 모두 성공
    - worker `Default/Cookies` file mtime도 증가
  - stderr:
    - `Failed global descriptor lookup: 7`
    - `components/dbus/xdg/request.cc:169`
  - 해석:
    - generic cookie set/persist/reuse는 현재 server/browser/profile 조합에서 동작한다.
    - 따라서 AIP fail HTML의 `Enable JavaScript and cookies to continue`를
      - "cookies가 전혀 저장되지 않음"의 직접 증거로 해석하기는 더 어려워졌다.

- generic JS smoke
  - command:
    - headless Chrome `--dump-dom data:text/html,...<script>...`
  - 결과:
    - DOM output 없이
      - `ERROR:base/memory/shared_memory_switch.cc:289 Failed global descriptor lookup: 7`
      만 출력
  - 해석:
    - 이 one-shot `--dump-dom data:` smoke는 실패했다.
    - 하지만 이것만으로 "현재 AIP runtime에서 JS가 비활성화"라고 결론내리기는 어렵다.
    - 이유:
      - latest AIP worker JSONL에서는 runtime probe가 돌았고
      - fail HTML에는 challenge orchestration script가 있었다.
    - 따라서 이 smoke는 현재 시점에서
      - "JS disabled 증거"가 아니라
      - "이 환경의 `--dump-dom data:` smoke가 신뢰하기 어려움"으로 취급한다 `[blocked]`.

- manual GUI path
  - operator note:
    - VS Code 접속 환경에서는 browser window가 보이지 않았고 종료함
  - direct command:
    - `"$CHROME_PATH" --user-data-dir="$SEED_PROFILE" --profile-directory="$PROFILE_NAME" ... "https://doi.org/10.1063/5.0207496"`
  - 결과:
    - visible window / final title / final URL을 확보하지 못한 채 `Ctrl+C`
  - 해석:
    - same-server manual GUI reproducibility는 아직 유효하게 확보되지 않았다 `[blocked]`.

- 네트워크 기본 사실
  - proxy env:
    - empty
  - DNS:
    - `pubs.aip.org -> 104.18.12.179 / 104.18.13.179`
    - `doi.org -> 172.67.69.3 / 104.26.4.132 ...`
  - route:
    - default via `192.168.100.1 dev eth0`
  - 해석:
    - transcript만으로는 별도 proxy/VPN 설정 증거는 없다.
    - AIP/doi 모두 Cloudflare fronted host라는 점은 재확인됐다.

- 가장 중요한 새 구조 단서
  - `ps -ef` during live run (`aip_runtime_probe_20260319`)에서 확인된 실제 browser process:
    - DrissionPage-launched live Chrome main process used
      - `--user-data-dir=/tmp/DrissionPage/autoPortData/13084`
      - `--profile-directory=Default`
    - not
      - `/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
  - 반면 landing JSONL record에는
    - `browser_user_data_dir=/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
    - `browser_session_source=linux_seed_clone`
    로 남았다.
  - 해석:
    - 현재 가장 강한 runtime mismatch hypothesis는
      - "코드/JSONL은 worker clone profile을 쓴다고 기록하지만,
         실제 Chrome process는 DrissionPage autoPortData temp user-data-dir에서 실행된다"
      이다.
    - 이것이 사실이면
      - seed/worker profile warming
      - cookie persistence
      - session reuse
      - entry-path patch
      들이 실제 live AIP access path에 material effect를 거의 주지 못할 수 있다.

- 현재 AIP diagnosis에 미치는 영향
  - 기존 strongest cause:
    - Linux server/IP first-contact challenge before stable landing
  - 새 strongest structural suspicion:
    - intended stateful profile path가 actual Chrome runtime에 직접 연결되지 않을 수 있음
  - 즉 다음 patch prior question은
    - "AIP entry path를 또 바꿀 것인가"
      보다
    - "DrissionPage autoPort launch가 왜 separate `autoPortData` root를 쓰는가, 그리고 worker clone state가 그 안으로 실제 복제되는가"
      를 먼저 확인해야 한다.

- 다음 확인 필요 항목
  - live `autoPortData/<port>` dir의 존재와 profile files를 worker clone과 비교 `[blocked]`
  - DrissionPage `auto_port()`가 user-data-dir을 override하는지, clone하는지, symlink하는지 `[blocked]`
  - live challenge run의 actual cookie DB가 `autoPortData` 쪽인지 worker clone 쪽인지 `[blocked]`

### 5.22 DrissionPage `auto_port()`가 stateful worker profile을 실 runtime에서 덮어쓴 구조 확인

- 출처
  - 2026-03-19 operator shell follow-up transcript (추가)
  - 저장소 코드:
    - `landing_access_repro.py`
    - `tools_exp.py`
  - 로컬 package source:
    - `DrissionPage/_configs/chromium_options.py`
    - `DrissionPage/_base/chromium.py`
    - `DrissionPage/_functions/tools.py`

- 추가 확보된 서버 evidence
  - `AUTO_DIR=/tmp/DrissionPage/autoPortData/10368`
  - live autoPort profile files:
    - `Local State`
    - `Default/Preferences`
    - `Default/Cookies`
  - worker vs autoPort 비교:
    - `cookies_cmp=1`
    - `prefs_cmp=1`
  - file size도 달랐다.
    - worker `Default/Cookies`: `86016`
    - autoPort `Default/Cookies`: `28672`
  - 해석:
    - live Chrome가 worker clone을 그대로 쓰는 것이 아니라
      별도 autoPortData root를 가진 독립 profile에서 돌고 있다는 근거가 더 강해졌다.

- plain Chrome CLI AIP article direct probe
  - command:
    - `"$CHROME_PATH" --headless=new --disable-gpu --disable-dev-shm-usage --user-data-dir="$WORKER_DIR" --profile-directory="$PROFILE_NAME" --dump-dom "https://pubs.aip.org/jcp/article/..."`
  - 결과:
    - operator note: `1분간 반응이 없어 종료`
  - 해석:
    - same server / same worker profile / plain CLI article direct도
      빠른 정상 landing 증거를 주지 못했다.
    - 다만 final DOM/title/artifact가 남지 않았으므로
      이것만으로 "plain Chrome CLI도 동일 challenge"라고 확정할 수는 없다 `[blocked]`.

- landing stdout/stderr 재확인
  - `landing.stdout.log`:
    - `resolved_chrome_path=/home/yongyong0206/chrome-linux/chrome-linux64/chrome`
    - `runtime_preset=linux_cli_seeded`
    - `execution_env=linux_server`
    - `worker_profile_root=/tmp/yongyong0206/landing_worker_profiles`
    - `seed_profile_ok=true`
    - `chrome_smoke=ok`
    - final counts: `challenge_detected=2`
  - `landing.stderr.log`:
    - empty
  - 해석:
    - current smoke check는 "Chrome can launch"만 보장할 뿐
      live AIP browser가 intended worker clone을 실제로 쓰는지는 보장하지 못한다.

- 코드/패키지 근거로 확인된 직접 원인
  - 저장소 코드의 기존 흐름:
    - `landing_access_repro.py::_browser_for_worker()`
      - `co.set_user_data_path(worker_user_data_dir)`
      - `co.set_user(worker_profile_name)`
      - `co.auto_port()`
    - `tools_exp.py::download_with_drission()`
      - `_apply_browser_session_plan(co, session_plan, ...)`
      - `co.auto_port()`
  - DrissionPage package behavior:
    - `ChromiumOptions.set_user_data_path(path)`:
      - `--user-data-dir=<path>` 설정
      - `_auto_port = False`
    - 이후 `ChromiumOptions.auto_port()`:
      - `_auto_port`를 다시 켠다
    - `DrissionPage._base.chromium.handle_options()`:
      - `_auto_port`가 켜져 있으면 `PortFinder(...).get_port()`로
        `/tmp/DrissionPage/autoPortData/<port>`를 생성하고
        다시 `set_user_data_path(path)`를 호출한다
  - 로컬 옵션 단위 재현:
    - old sequence:
      - `set_user_data_path('/tmp/worker_profile')`
      - `auto_port()`
      - `handle_options(...)`
      -> `user_data_path=/.../DrissionPage/autoPortData/<port>`
    - new sequence:
      - `set_user_data_path('/tmp/worker_profile')`
      - `set_local_port(12345)`
      - `handle_options(...)`
      -> `user_data_path=/tmp/worker_profile`
  - 해석:
    - 지금까지의 strongest structural suspicion은 `[blocked]`가 아니라
      **코드/패키지 레벨에서 confirmed** 되었다.
    - 기존 stateful seed/worker profile 전략은
      actual live browser profile로 온전히 전달되지 않았을 가능성이 매우 높다.

- 이번 패치
  - 목적:
    - stateful worker clone을 actual live Chrome `user-data-dir`로 유지하면서
      port collision만 피한다.
  - 적용 파일:
    - `landing_access_repro.py`
      - `co.auto_port()` 제거
      - `co.set_local_port(_pick_free_local_port())` 사용
      - JSONL result에
        - `browser_effective_user_data_dir`
        - `browser_debug_address`
        추가
    - `tools_exp.py`
      - `co.auto_port()` 제거
      - `co.set_local_port(_pick_free_local_port())` 사용
      - detail payload에
        - `browser_effective_user_data_dir`
        - `browser_debug_address`
        추가
    - `experiment/summarize_linux_headless_suite.py`
      - merged summary에
        - `landing_probe_browser_user_data_dir`
        - `landing_probe_browser_effective_user_data_dir`
        - `landing_probe_browser_debug_address`
        추가

- 왜 이 패치가 지금 가장 우선인가
  - 기존 AIP patch들은
    - entry path
    - context bootstrap ordering
    - publisher direct article 선택
    를 계속 조정해 왔다.
  - 그러나 actual live browser가 intended worker profile을 안 쓰면
    - cookie/session reuse
    - warmed seed
    - profile-based first-contact mitigation
    자체가 실제 runtime path에 material effect를 주기 어렵다.
  - 따라서 current weakest point는
    - "AIP entry ordering"
      이전에
    - "stateful profile이 live browser launch에 실제 연결되는가"
    이다.

- 검증
  - `python -m py_compile tools_exp.py landing_access_repro.py experiment/summarize_linux_headless_suite.py`
    - passed
  - options-level repro:
    - old sequence -> `autoPortData/<port>`
    - new sequence -> intended `/tmp/worker_profile`
  - 아직 미검증:
    - patched server run에서 실제 Chrome process가 이제
      `--user-data-dir=/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
      로 뜨는지 `[blocked]`
    - patched server run에서 AIP challenge incidence가 실제 줄어드는지 `[blocked]`

- 배운 점
  - 지금까지 AIP가 "같은 challenge outcome"을 반복한 이유 중 하나는
    - entry path patch 자체보다
    - live browser launch가 intended stateful profile과 분리되어 있었기 때문일 수 있다.
  - 앞으로 AIP 실험은 결과 해석 전에 반드시
    - recorded `browser_user_data_dir`
    - actual Chrome `--user-data-dir`
    - new `browser_effective_user_data_dir`
    이 셋이 일치하는지 먼저 봐야 한다.

### 5.23 `aip_profile_path_fix_probe_20260319`: profile path fix는 runtime에 적용됐지만 failure mode가 `challenge`에서 `page disconnected`로 이동

- 출처
  - `experiment/results/aip_profile_path_fix_probe_20260319_bundle.tar.gz`

- 실험 목적
  - `5.22` patch 이후 actual live browser가 이제 worker clone profile을 쓰는지 확인
  - profile/session patch가 실제 access path에 material effect를 갖는지 검증

- 실행 요약
  - input:
    - `outputs/_aip_structural_validation_20260315_input.csv`
  - runtime:
    - `linux_cli_seeded`
    - `linux_server`
    - `headless=1`
    - `workers=1`
  - artifact:
    - `outputs/aip_profile_path_fix_probe_20260319/*`

- 확인된 사실
  - report summary:
    - `classifier_counts.network_error = 2`
    - `outcome_counts.FAIL_NETWORK = 2`
  - two DOI records 모두:
    - `entry_strategy_variant=publisher_canonical_context_deferred_no_article_preflight`
    - `entry_browser_kind=canonical_article`
    - `browser_user_data_dir=/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
    - `browser_effective_user_data_dir=/tmp/yongyong0206/landing_worker_profiles/stateful_worker_0`
    - `browser_debug_address=127.0.0.1:46703`
  - 해석:
    - 이번 patch는 **실제 runtime에 적용되었다.**
    - 즉 "recorded profile path와 live browser path mismatch"는 이번 run에서는 해소된 것으로 보인다.

- failure mode 변화
  - before (`aip_article_first_js_cookie_diag_20260319`):
    - `challenge_detected=true`
    - title `Just a moment...`
    - fail HTML contained challenge shell / `__cf_chl_rt_tk`
  - after (`aip_profile_path_fix_probe_20260319`):
    - `challenge_detected=false`
    - `classifier_state=network_error`
    - `reason_codes`:
      - `navigation_network_error`
      - `The connection to the page has been disconnected. Version: 4.1.1.2`
    - `runtime_probe_installed=false`
    - `network_listener=false`
    - `html_len=0`
    - fail HTML files were zero-byte
    - `current_url` remained DOI URL
    - `tab_state.total_tab_count=0`
  - 해석:
    - challenge shell이 다시 잡힌 것이 아니라,
      browser/page control channel이 final article landing 전에 먼저 끊겼다.
    - 즉 현재 failure step은
      - "AIP article reached but challenge"
        가 아니라
      - "**stateful live browser attached 상태에서 navigation 중 page/browser connection lost**"
      로 이동했다.

- 이 결과가 의미하는 것
  - positive:
    - previous profile mismatch patch는 cosmetic change가 아니라
      actual runtime path를 바꿨다.
    - 따라서 이후 실험부터는 seed/worker profile 기반 patch가
      실제로 landing path에 영향을 줄 수 있게 되었다.
  - negative:
    - profile path를 바로잡는 것만으로 AIP landing success는 얻지 못했다.
    - 오히려 challenge failure가 사라지고
      browser disconnect failure가 드러났다.

- 현재 strongest hypothesis
  - AIP access failure는 이제 두 층으로 분리해서 봐야 한다.
  - phase 1:
    - 기존에는 live profile mismatch 때문에
      intended stateful session reuse가 실제로 적용되지 않았다.
  - phase 2:
    - profile mismatch를 고치자
      live stateful browser session 자체가 DrissionPage control path에서 불안정해졌다.
  - 즉 current weakest point는 이제
    - entry ordering
      보다
    - **stateful browser lifecycle / attached page survivability**
    쪽이다.

- 아직 말할 수 없는 것 `[blocked]`
  - 이 disconnect가
    - browser process crash인지
    - tab target loss인지
    - reused controller page corruption인지
    - stateful profile + DrissionPage attach interaction인지
    는 이번 bundle만으로 확정할 수 없다.
  - `ps -ef` live process snapshot이 bundle에 포함되지 않아
    actual process-level termination은 확인 불가 `[blocked]`.

- 다음 patch priority
  - 1순위:
    - stateful session launch 이후 controller/probe page lifecycle을 더 강하게 진단
      - browser pid
      - initial tabs_count / tab_ids
      - page alive 여부
      - navigation 직전/직후 disconnect point
  - 2순위:
    - stateful session에서는 startup controller page를 그대로 재사용하지 말고
      fresh controlled tab를 생성해 그 tab로 navigation
      또는 zero-tab / disconnected page면 browser restart
  - 3순위:
    - challenge heuristics보다 먼저
      `page disconnected`를 structural session error로 분류하고
      별도 recovery path를 둔다

- 배운 점
  - "patch exists"와 "patch affected runtime"를 분리해야 한다는 교훈이 이번에도 반복됐다.
  - 이번 patch는 실제 runtime에 먹었고,
    그 결과 failure signature 자체가 바뀌었다.

### 5.24 stateful browser lifecycle survivability patch: AIP Linux stateful session은 fresh browser + fresh controlled tab을 기본값으로 전환

- 출발점
  - `5.23` 이후 최신 strongest diagnosis:
    - AIP landing bottleneck은 더 이상 주로 `challenge_detected`가 아니다.
    - 현재 weakest point는 **stateful live browser attached page survivability**이다.
  - supporting evidence:
    - latest bundle:
      - `classifier_state=network_error`
      - `navigation_network_error`
      - `The connection to the page has been disconnected. Version: 4.1.1.2`
      - `runtime_probe_installed=false`
      - `network_listener=false`
      - zero-byte fail HTML
    - `browser_user_data_dir == browser_effective_user_data_dir`
      -> profile mismatch는 이미 해소되었음
  - external reference:
    - DrissionPage issue `#530`
      - title: `The connection to the page has been disconnected. Version: 4.1.0.17`
    - confirmed fact:
      - disconnect message family가 동일하다.
    - inference:
      - 이번 patch direction은 AIP navigation tuning보다
        browser/page attach lifecycle 안정화가 더 직접적이라고 판단했다.
    - non-adopted:
      - `auto_port()` 계열 복귀는 `5.22`에서 actual `user-data-dir` mismatch를 만든 근거가 있어 채택하지 않았다.

- 변경 파일
  - `landing_access_repro.py`
  - `experiment/summarize_linux_headless_suite.py`

- patch hypothesis
  - AIP stateful Linux session에서 disconnect는
    - stale startup/controller page reuse
    - probe tab attach 직후 survivability 미검증
    - disconnected page handle을 들고 navigation에 진입
    중 하나일 가능성이 높다.
  - 따라서 profile/session reuse는 **on-disk worker profile** 수준으로만 유지하고,
    live browser/page object reuse는 약하게 가져가야 한다.

- 적용한 변경
  - AIP + `stateful` + `linux_cli_seeded` + `linux_server` 조합이면
    - controller page reuse를 기본적으로 금지
    - DOI마다 fresh browser를 다시 띄우도록 유지
  - same condition에서 probe page mode 기본값을
    - `reuse_page`
      -> `fresh_tab`
    로 변경
    - env override:
      - `PDF_BROWSER_LANDING_AIP_STATEFUL_FRESH_TAB=0` -> old reuse behavior
      - `=1` -> explicit fresh tab
      - unset/auto -> fresh tab
  - `_ensure_probe_page_for_record()` 추가
    - controller browser creation
    - fresh controlled tab open
    - immediate lifecycle snapshot
    - survivability validation
    - first probe page가 not survivable이면 browser/page를 한 번 더 재생성
  - new diagnostics:
    - `probe_page_mode_requested`
    - `probe_page_mode_effective`
    - `probe_open_attempts`
    - `probe_open_succeeded`
    - `probe_attach_restart_reason`
    - `controller_page_reused`
    - `controller_reuse_allowed`
    - `controller_restart_reason`
    - `controller_restart_count`
    - `controller_create_attempts`
    - `controller_lifecycle_before_open`
    - `probe_lifecycle_after_open`
    - `page_disconnect_observed`
    - `page_disconnect_stage`
    - `browser_process_alive`
    - `page_access_ok`
    - `page_probe_error`
    - `final_active_tab_id`
    - `final_total_tab_count`
    - `network_listener_started`
    - `network_listener_error`
    - `runtime_probe_installed`
    - `runtime_probe_error`
  - artifact fail/success meta JSON과 merged summary CSV에도 위 필드를 전파

- 추가 교정
  - local helper smoke에서
    - `active_tab_id`는 존재하지만
    - `tab_ids=[]`, `total_tab_count=0`
    인 경우가 재현되었다.
  - 따라서 survivability 판정은
    - `tab_ids > 0`
      만으로 보지 않고,
    - `browser_process_alive`
    - `page_access_ok`
    - `active_tab_id`
    조합도 survive signal로 인정하도록 완화했다.
  - 이건 latest fail meta의
    - `active_tab_id`는 있었지만
    - `total_tab_count=0`
    인 패턴과도 맞는다.

- 검증
  - `python -m py_compile landing_access_repro.py experiment/summarize_linux_headless_suite.py`
    - passed
  - branch selection smoke:
    - stateful AIP Linux condition에서
      - `session_mode=stateful`
      - `session_source=linux_seed_clone`
      - `reuse_allowed=False`
      - default `effective_mode=fresh_tab`
    - override:
      - `PDF_BROWSER_LANDING_AIP_STATEFUL_FRESH_TAB=0` -> `reuse_page`
      - `=1` -> `fresh_tab`
  - helper smoke (local, no DOI navigation):
    - `probe_page_mode_requested=reuse_page`
    - `probe_page_mode_effective=fresh_tab`
    - `probe_open_attempts=1`
    - `probe_open_succeeded=True`
    - `controller_page_reused=False`
    - `controller_reuse_allowed=False`
    - `browser_effective_user_data_dir=/tmp/codex_probe_profiles/stateful_worker_0`
    - note:
      - `total_tab_count=0` even when `active_tab_id` and `page_access_ok` existed
      - this directly motivated the survivability criterion correction above

- 해석
  - 이번 patch는 challenge 대응 patch가 아니라,
    **live stateful browser attach lifecycle을 안정화하는 patch**다.
  - 아직 server AIP DOI run으로 검증되지는 않았지만,
    적어도 next run에서는 다음 둘을 분리해서 볼 수 있게 되었다.
    - browser/process survived but page attach lost
    - controller/probe creation 자체가 fragile

- 다음 서버 검증에서 꼭 봐야 할 것
  - `probe_page_mode_effective=fresh_tab`
  - `controller_page_reused=false`
  - `probe_open_succeeded`
  - `probe_attach_restart_reason`
  - `browser_effective_user_data_dir`
  - `browser_process_alive`
  - `final_active_tab_id`
  - `final_total_tab_count`
  - `page_disconnect_observed`
  - `page_disconnect_stage`
  - `runtime_probe_installed`
  - `network_listener_started`

- 아직 불확실한 것 `[blocked]`
  - server에서 fresh browser + fresh controlled tab이 실제 disconnect incidence를 줄이는지 `[blocked]`
  - disconnect가
    - browser crash
    - renderer/target loss
    - attach race
    중 무엇이 주원인인지는 아직 `[blocked]`
  - `fresh_tab` default가 server에서 도움이 되는지, 아니면 `reuse_page` override가 더 나은지는 실제 micro-run 전까지 `[blocked]`

### 5.25 `aip_stateful_lifecycle_probe_20260319`: lifecycle patch 이후 failure mode는 다시 `challenge_detected`, multi-tab path는 실제로 거의 안 탔다

- 출처
  - `experiment/results/aip_stateful_lifecycle_probe_20260319_bundle.tar.gz`

- 이번 bundle이 보여준 것
  - report summary:
    - `classifier_counts.challenge_detected = 2`
    - `outcome_counts.FAIL_CAPTCHA = 2`
  - two DOI records 모두:
    - `entry_strategy_variant=publisher_canonical_context_deferred_no_article_preflight`
    - `entry_browser_kind=canonical_article`
    - `entry_context_bootstrap_attempted=false`
    - `entry_context_bootstrap_outcome=deferred_initial_bootstrap`
    - `probe_page_mode_effective=fresh_tab`
    - `controller_page_reused=false`
    - `probe_open_succeeded=true`
    - `page_disconnect_observed=false`
    - `runtime_probe_installed=true`
    - `network_listener_started=true`
    - `tab_transition_count=0`
  - final URLs:
    - both canonical article URL에 바로 `__cf_chl_rt_tk=...`가 붙었다.
  - fail HTML:
    - `<title>Just a moment...</title>`
    - `/cdn-cgi/challenge-platform`
    - `window._cf_chl_opt`
    - `<noscript>Enable JavaScript and cookies to continue</noscript>`

- 최신 진단
  - 이번 run은 `5.23`의 disconnect run과 달리,
    - lifecycle/attach는 이번 attempt에서 **충분히 살아 있었다.**
    - failure는 다시 **first-contact challenge/CAPTCHA**다.
  - confirmed fact:
    - browser/page survived long enough to finish navigation and runtime diagnostics
    - challenge shell이 fully rendered 되었다
  - inference:
    - `5.24` lifecycle patch는 disconnect 가설을 분리하는 데는 유효했지만,
      이번 run의 direct blocker는 challenge pressure였다.

- actual runtime path reconstruction
  - navigation chain:
    - `pre_reset -> aip_resolve -> doi_get`
  - browser-open path:
    - `doi.org`는 browser에서 직접 열지 않고
    - pre-resolve 후 canonical AIP article URL을 same-tab navigation으로 열었다
  - bootstrap:
    - journal-root bootstrap은 attempted되지 않았다
  - recovery:
    - challenge가 즉시 감지되어 no-retry
    - tab recovery / fallback path는 사실상 타지 않았다

- "8 tabs" 관련 판단
  - latest bundle 기준으로는
    - `8 tabs` 또는 유사한 multi-tab fallback path가 **실제로 exercised되지 않았다.**
  - evidence:
    - `tab_transition_count=0`
    - `entry_context_bootstrap_attempted=false`
    - `entry_navigation_route=same_tab`
    - `probe_open_attempts=1`
    - `probe_page_mode_effective=fresh_tab`
  - 해석:
    - 이번 run에서 실제 tab creation은
      - fresh browser의 controller page
      - plus one fresh probe tab
      정도였을 가능성이 가장 높다.
    - exact peak tab count는
      - runtime diagnostics에서 `tab_ids=[]`, `total_tab_count=0`로 나와
      - DrissionPage tab enumeration quirk 때문에 정확히는 `[blocked]`
    - 그러나 적어도 이번 AIP challenge path는
      - 8-tab strategy가 current bottleneck이 아니고
      - challenge가 first-contact에서 이미 붙어서
        그 이후 tab-heavy fallback이 발동할 기회조차 없었다.

- memory/time inefficiency 판단
  - 이번 run에서 observable cost:
    - 각 DOI `attempt_elapsed_ms ~= 21.7s`
    - second DOI는 pacing wait `~16.3s`
  - `[blocked]`
    - process RSS / browser memory / tab별 cost는 bundle에 없다
  - 결론:
    - exact memory inefficiency는 정량화 불가 `[blocked]`
    - 하지만 latest run 기준으로는 "too many tabs"보다
      "challenge를 유발하는 first-contact setup surface"가 더 직접적 문제다.

- 이번 턴 patch hypothesis
  - latest bundle은
    - fresh tab creation
    - pre-reset
    - listener install
    - runtime probe install
    가 있어도 challenge를 피하지 못했다.
  - 따라서 next patch는
    - lifecycle보다
    - **first-contact surface reduction**
    를 우선해야 한다.

- 적용한 변경
  - `landing_access_repro.py`
    - AIP stateful Linux/server 기본 probe page mode를
      - `fresh_tab`
        -> `reuse_page`
      로 되돌림
    - `PDF_BROWSER_AIP_LOW_PRESSURE_FIRST_CONTACT` 추가
      - auto default:
        - AIP + stateful + linux_cli_seeded + linux_server + first attempt에서 enabled
    - low-pressure first-contact에서는
      - `pre_reset` skip
      - network listener skip
      - runtime error probe install skip
      - direct-PDF handoff capture skip
      - 즉 first browser contact를 canonical article same-tab navigation 하나로 최대한 단순화
    - new diagnostics:
      - `aip_first_contact_policy`
      - `aip_low_pressure_first_contact`
      - `tab_lifecycle_sequence`
      - `peak_tab_count_observed`
      - `reduced_tab_path_used`
  - `tools_exp.py`
    - `PDF_BROWSER_AIP_CONTEXT_CHALLENGE_FRESH_TAB` auto default -> disabled
    - `PDF_BROWSER_AIP_FRESH_TAB_RECOVERY` helper 추가
    - AIP fresh-tab recovery auto default -> disabled on linux server/runtime
    - rationale:
      - challenge가 이미 발생한 뒤 tab을 더 여는 것은 current evidence상 도움이 없다
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 위 low-pressure / reduced-tab diagnostics 추가

- patch verification
  - `python -m py_compile landing_access_repro.py tools_exp.py experiment/summarize_linux_headless_suite.py`
    - passed
  - branch smoke:
    - stateful AIP Linux condition에서
      - `effective_mode_default=reuse_page`
      - `low_pressure_first_contact=True`
      - `context_challenge_fresh_tab_auto=False`
      - `fresh_tab_recovery_auto=False`
    - override:
      - `PDF_BROWSER_LANDING_AIP_STATEFUL_FRESH_TAB=1` -> `fresh_tab`

- 배운 점
  - 이번 bundle은 "tab 최적화"보다 "first-contact challenge 억제"가 우선이라는 점을 더 강하게 보여준다.
  - multi-tab recovery는 코드에 존재하지만,
    latest AIP challenge path에서는 실제로 거의 타지 않았다.
  - 따라서 다음 실험은
    - lower-pressure first-contact가 challenge incidence를 줄이는지
    - reduced-tab path가 실제 runtime에 탔는지
    를 먼저 봐야 한다.

- 다음 검증에서 꼭 볼 것
  - `aip_first_contact_policy=aip_low_pressure_minimal_surface`
  - `aip_low_pressure_first_contact=true`
  - `probe_page_mode_effective=reuse_page`
  - `reduced_tab_path_used=true`
  - `peak_tab_count_observed`
  - `challenge_detected`
  - `entry_context_bootstrap_attempted`
  - `tab_transition_count`

- 아직 불확실한 것 `[blocked]`
  - lower-pressure first-contact가 실제로 challenge incidence를 줄이는지 `[blocked]`
  - this server/IP에서 compliant browser landing만으로 AIP challenge를 consistently 피할 수 있는지 `[blocked]`
  - current AIP canonical article path 말고 더 나은 compliant first-contact URL ordering이 실제로 있는지 `[blocked]`
  - 따라서 현재 AIP 진단은 다시 challenge-only narrative로 단순화하면 안 된다.

5.26 `aip_low_pressure_probe_20260319_bundle`: "다른 논문을 방문했다가 원래 DOI로 복귀"는 실제 AIP strategy가 아니라 stale startup page contamination이었고, AIP landing path는 direct DOI로 단순화해야 한다

- 출처
  - `experiment/results/aip_low_pressure_probe_20260319_bundle.tar.gz`

- 이번 bundle이 실제로 보여준 것
  - report summary:
    - `classifier_counts.network_error = 1`
    - `classifier_counts.challenge_detected = 1`
    - `outcome_counts.FAIL_NETWORK = 1`
    - `outcome_counts.FAIL_CAPTCHA = 1`
  - DOI `10.1063/5.0207496`
    - `entry_strategy=aip_official_doi_resolve`
    - `entry_strategy_variant=publisher_canonical_context_deferred_no_article_preflight`
    - `aip_first_contact_policy=aip_low_pressure_minimal_surface`
    - `probe_page_mode_effective=reuse_page`
    - `navigation_chain=[aip_resolve]`
    - fail reason:
      - `navigation_network_error`
      - `The connection to the page has been disconnected. Version: 4.1.1.2`
    - `tab_lifecycle_sequence.controller_before_open.current_url`
      - `https://pubs.rsc.org/en/content/articlepdf/2024/tc/d3tc03497f`
    - `controller_before_open.total_tab_count = 10`
  - DOI `10.1116/6.0004298`
    - `entry_strategy=aip_official_doi_resolve`
    - `probe_page_mode_effective=reuse_page`
    - `navigation_chain=[aip_resolve, doi_get]`
    - final:
      - canonical AIP article URL + `__cf_chl_rt_tk=...`
      - `title=Just a moment...`
    - `tab_lifecycle_sequence.controller_before_open.current_url`
      - `https://www.sciencedirect.com/science/article/pii/S0167931725000164`

- user suspicion 검증
  - suspicion:
    - JSON artifact상 "여러 다른 article page를 들렀다가 원래 DOI로 돌아온다"
  - confirmed fact:
    - 이번 bundle에서 실제 navigation chain은
      - `aip_resolve`
      - 또는 `aip_resolve -> doi_get`
      뿐이다.
    - AIP runtime path가 여러 AIP article page를 intentionally 순회했다는 근거는 없다.
  - confirmed fact:
    - fail meta의 "다른 논문 페이지"는
      - 현재 attempt가 새로 방문한 warm-up page가 아니라
      - stateful browser가 시작할 때 이미 들고 있던 startup/controller page였다.
  - conclusion:
    - user suspicion은 **완전히는 맞지 않는다.**
    - "다른 논문을 방문 후 복귀" methodology가 실제 exercised AIP path였던 것은 아니고,
      stale controller/startup page contamination이 그렇게 보이게 만들었다.

- methodology review
  - `visit other pages then return` strategy:
    - AIP landing current runtime path에서 실제 benefit evidence 없음
    - latest bundle에서도 exercised되지 않음
    - AIP landing 전용 alternate recovery (`_recover_aip_canonical_landing`)는
      canonical/resolved candidate를 다시 순회하는 branch였지만,
      journal 상 Linux server 성공 근거를 만들지 못했다.
  - `DOI pre-analysis before browser launch` strategy:
    - latest bundle에서 실제 exercised됨
      - `entry_strategy=aip_official_doi_resolve`
      - browser launch 전에 `requests` 기반 DOI resolve 수행
      - browser는 canonical publisher article URL로 들어감
    - confirmed fact:
      - Linux server AIP 실패 run들 대부분이 이 strategy와 함께 발생했다.
    - no confirmed evidence:
      - AIP Linux server landing 성공률을 올렸다는 근거 없음
    - conclusion:
      - 최소한 AIP landing path에서는 evidence-backed strategy로 볼 수 없다.

- patch decision
  - AIP landing은 더 이상
    - pre-resolve DOI
    - canonical article preselection
    - alternate candidate revisit
    를 사용하지 않는다.
  - 새 원칙:
    - browser/session 준비
    - clean controlled page 확보
    - `https://doi.org/<doi>` direct navigation
    - 결과 classification

- 적용한 변경
  - `landing_access_repro.py`
    - AIP landing path에서 `build_aip_safe_entry_plan()` 호출 제거
    - `_build_aip_direct_doi_entry_plan()` 추가
      - `entry_strategy=aip_direct_browser_doi`
      - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
      - `entry_browser_url=https://doi.org/<doi>`
      - `entry_redirect_probe_mode=none`
      - `entry_prebrowser_request_count=0`
      - `entry_context_bootstrap_mode=disabled`
    - AIP stateful Linux session 기본 probe page mode를 다시 `fresh_tab`으로 환원
      - latest bundle에서 `reuse_page`가 stale startup page contamination과 결합했기 때문
    - `probe_page_mode_effective != fresh_tab` fallback일 때만
      - `about:blank` reset 후 direct DOI navigation
    - AIP landing에서 삭제:
      - `_recover_aip_canonical_landing()`
      - `_looks_like_aip_blank_or_incomplete()` (AIP landing alternate recovery 전용 helper)
    - AIP landing에서 bypass:
      - context bootstrap path
      - preferred handoff
      - targeted recovery
    - new diagnostics:
      - `aip_direct_doi_path_used`
      - `aip_alternate_pages_opened`
      - `entry_preanalysis_ran`
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 위 direct-DOI / preanalysis / alternate-page diagnostics 추가

- 왜 이 patch인가
  - latest bundle은
    - complexity가 실제로 도움이 되었다는 근거를 주지 못했다.
    - 오히려
      - stale startup page contamination
      - browser 밖 DOI pre-analysis
      - canonical preselection
      이 current AIP path를 불필요하게 복잡하게 만들고 있었다.
  - 따라서 next valid experiment는
    - AIP path를 direct DOI single-path로 줄인 뒤
    - challenge incidence와 landing outcome을 다시 보는 것이다.

- patch verification
  - `python -m py_compile landing_access_repro.py experiment/summarize_linux_headless_suite.py`
    - passed
  - helper smoke:
    - `entry_strategy=aip_direct_browser_doi`
    - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
    - `entry_browser_url=https://doi.org/...`
    - `entry_prebrowser_request_count=0`
    - `entry_redirect_probe_mode=none`
    - `entry_preanalysis_ran_expected=False`
  - server-condition probe mode smoke:
    - `PDF_BROWSER_RUNTIME_PRESET=linux_cli_seeded`
    - `PDF_BROWSER_EXECUTION_ENV=linux_server`
    - `probe_page_mode_effective=fresh_tab`

- 배운 점
  - latest AIP bundle은 "other pages warm-up"보다
    - stale stateful startup page contamination
    - and unsupported pre-resolve complexity
    를 더 직접적으로 보여줬다.
  - AIP landing에 대해서는
    - "publisher canonical preselection"
    - "alternate candidate revisit"
    가 evidence-backed라고 말할 수 없다.
  - 따라서 AIP path는 direct DOI로 줄이는 편이
    - behavior 설명 가능성
    - runtime observability
    - experiment interpretability
    면에서 낫다.

- 다음 검증에서 꼭 볼 것
  - `entry_strategy=aip_direct_browser_doi`
  - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
  - `entry_browser_url=https://doi.org/...`
  - `entry_preanalysis_ran=false`
  - `aip_direct_doi_path_used=true`
  - `aip_alternate_pages_opened=false`
  - `probe_page_mode_effective=fresh_tab`
  - `navigation_chain`가 `doi_get` 중심으로 단순화됐는지

- 아직 불확실한 것 `[blocked]`
  - direct DOI path가 실제 Linux server/IP에서 challenge incidence를 낮추는지 `[blocked]`
  - stale startup tabs가 stateful profile restore 때문인지, browser startup default 때문인지까지는 `[blocked]`
  - AIP 이외 publisher까지 pre-analysis removal을 일반화할 근거는 아직 부족하다 `[blocked]`

5.27 archived AIP output cleanup: bundle로 보존된 AIP 로컬 산출물과 empty-source validation output 정리

- rationale
  - 아래 산출물은
    - journal에 이미 요약되었고
    - 필요한 경우 `experiment/results/*.tar.gz` 또는 journal 기록으로 다시 참조 가능하므로
    - workspace clutter를 줄이기 위해 삭제했다.
  - seed profile / input CSV / `experiment/results` bundle은 유지했다.

- deleted output dirs
  - `outputs/aip_context_bootstrap_validation2_20260315_local`
  - `outputs/aip_context_bootstrap_validation_20260315_local`
  - `outputs/aip_first_contact_deferred_validation_20260319_local`
  - `outputs/aip_patch_validation_20260315_local`
  - `outputs/aip_profile_diag_20260315_local_linuxseeded`
  - `outputs/aip_profile_diag_20260315_local_temp`
  - `outputs/aip_publisher_direct_handoff_validation2_20260315_local`
  - `outputs/aip_publisher_direct_handoff_validation_20260315_local`
  - `outputs/aip_publisher_direct_validation_20260315_local`
  - `outputs/aip_structural_validation_20260315_local`
  - `outputs/linux_headless_suite_runs/empty_source_status_validation_20260315`

- note
  - git working tree에서는 위 output file들이 대량 `D`로 보이지만,
    이건 user-requested cleanup에 따른 expected change다.

5.28 stale startup tabs cleanup patch: AIP direct DOI path 전에 restore된 다른 저널 탭을 닫고 controller page를 blank로 초기화

- 출발점
  - latest bundle의 remaining issues는 user가 지적한 두 가지였다.
    - `tab_lifecycle_sequence`에 다른 저널 page가 계속 보임
    - `peak_tab_count_observed=10`
  - latest fail JSON 재확인:
    - `controller_page_reused=false`
    - `controller_reuse_allowed=false`
    - `controller_create_attempts=1`
    - 그럼에도 `controller_before_open.current_url`이
      - RSC PDF
      - ScienceDirect article
      로 남아 있었다.
  - confirmed fact:
    - 이건 current attempt가 warm-up으로 다른 저널을 방문했다기보다
      **새 stateful browser startup 자체가 restore된 기존 tab state를 들고 시작한 것**이다.

- root cause interpretation
  - remaining alternate-journal visits의 직접 원인은
    - AIP direct DOI branch 이전에
    - startup controller page가 restore된 non-AIP tabs를 그대로 유지한 채 probe phase로 들어간 것
    이다.
  - `peak_tab_count_observed=10`도 같은 원인에서 나왔다.
    - AIP strategy가 10개 tab을 intentionally 연 것이 아니라
    - startup restore state를 정리하지 못해 peak가 inflated되었다.

- 적용한 변경
  - `landing_access_repro.py`
    - `_trim_tabs_to_ids()` 추가
      - 지정 tab만 남기고 나머지 tabs를 즉시 닫는 helper
    - `_prepare_aip_controller_page()` 추가
      - AIP + stateful + linux_cli_seeded + linux_server 조건이면
        - controller startup snapshot 기록
        - restore된 extra tabs close
        - surviving controller page를 `about:blank`로 reset
    - `_ensure_controller_page_for_record()`가
      - browser 생성 직후 위 cleanup을 항상 통과하도록 변경
    - `_ensure_probe_page_for_record()`가
      - probe tab open 직후 다시 keep-set trim을 한 번 더 수행
      - 의도는 controller + probe two-tab ceiling 유지
    - new diagnostics:
      - `controller_lifecycle_initial`
      - `startup_tab_cleanup_applied`
      - `startup_tab_cleanup_before_count`
      - `startup_tab_cleanup_after_count`
      - `startup_tab_cleanup_closed_count`
      - `startup_page_reset_to_blank`
      - `startup_page_reset_error`
      - `post_open_tab_trim_closed_count`
  - `experiment/summarize_linux_headless_suite.py`
    - merged summary에 위 startup-tab cleanup diagnostics 추가

- intended runtime path proof
  - `_probe_worker_records()`는 매 DOI마다 `_ensure_probe_page_for_record()`를 호출한다.
  - `_ensure_probe_page_for_record()`는 항상 `_ensure_controller_page_for_record()`를 거친다.
  - 따라서 AIP stateful Linux/server record는
    - browser startup 직후 cleanup
    - then fresh probe tab open
    - then direct DOI navigation
    순서를 **반드시** 탄다.
  - 이건 단순히 code가 존재하는 수준이 아니라
    intended exercised runtime path에 들어가 있는 patch다.

- before / after expectation
  - before:
    - `controller_before_open.current_url`에 non-AIP journal page가 보일 수 있음
    - `total_tab_count`가 restore state 때문에 10까지 커질 수 있음
    - probe phase 진입 전에 이미 stale journal context가 섞임
  - after:
    - `controller_lifecycle_initial`에는 old restore state가 남을 수 있어도
    - `controller_before_open`은 cleanup 후 snapshot이므로
      - `about:blank` 또는 empty startup page
      - `startup_tab_cleanup_after_count <= 1`
      가 기대값이다.
    - `probe_after_open.total_tab_count <= 2`
      - controller blank tab 1
      - probe tab 1
    - direct DOI navigation 이전에 unrelated journal page가 runtime path에 남아 있지 않아야 한다.

- patch verification
  - `python -m py_compile landing_access_repro.py experiment/summarize_linux_headless_suite.py`
    - passed
  - branch smoke:
    - server-condition AIP stateful record에서
      - `probe_page_mode=fresh_tab`
      - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`

- 다음 검증에서 꼭 볼 것
  - `controller_lifecycle_initial.current_url`
    - stale restore 흔적이 있었는지
  - `controller_before_open.current_url`
    - blank로 정리됐는지
  - `startup_tab_cleanup_before_count`
  - `startup_tab_cleanup_after_count`
  - `startup_tab_cleanup_closed_count`
  - `probe_after_open.total_tab_count`
  - `peak_tab_count_observed`
  - `aip_direct_doi_path_used=true`
  - `aip_alternate_pages_opened=false`

- 아직 불확실한 것 `[blocked]`
  - actual live server rerun 없이
    - cleanup 후 observed peak tab count가 실제로 `<=2`였는지는 아직 `[blocked]`
  - stale startup tabs의 원인이
    - Chrome session restore pref인지
    - stateful profile content 자체인지
    는 아직 `[blocked]`

5.29 Ubuntu VM에서 "필요한 publisher 탭을 모두 열어 둔 프로필"을 가져오는 전략의 의미 검증

- 질문
  - Ubuntu VM에서 profile을 seed로 가져올 때
    - AIP, Elsevier, RSC 등 필요한 publisher를 전부 tab으로 열어 둔 상태로 profile을 저장해 오면
    - Linux server landing에 도움이 되는가
    를 검증했다.

- 현재 evidence
  - latest AIP bundle `aip_low_pressure_probe_20260319_bundle`에서는
    - `tab_lifecycle_sequence`에 RSC PDF / ScienceDirect article 같은 non-AIP page가 보였고
    - `peak_tab_count_observed=10`까지 올라갔다.
  - 하지만 이건 "current attempt가 warm-up으로 다른 publisher를 방문했다"기보다
    - stateful browser startup 시점에 restore된 old tabs가 controller page로 붙은 것에 가깝다.
  - 이후 넣은 startup cleanup patch는
    - `_prepare_aip_controller_page()`
    - `_trim_tabs_to_ids()`
    로 이 restore tabs를 닫고 controller page를 `about:blank`로 reset하도록 바꿨다.

- code-grounded interpretation
  - 현재 AIP stateful Linux/server path는
    - `_ensure_controller_page_for_record()`
    - `_prepare_aip_controller_page()`
    - `_ensure_probe_page_for_record()`
    순서로 들어간다.
  - 즉 startup 시점에 이미 여러 publisher tab이 열려 있어도
    - keep-set 밖 tab은 닫히고
    - surviving controller page도 blank reset된다.
  - 따라서 **현재 intended runtime path에서는 "미리 열어 둔 publisher tabs"는 적극적으로 제거되는 대상**이다.

- 검증 결론
  - 현재 코드 기준으로는
    - Ubuntu VM에서 publisher tabs를 많이 열어 둔 profile을 seed로 가져오는 전략은
    - AIP landing에 의미 있는 positive signal로 취급되지 않는다.
  - 더 강하게 말하면
    - latest evidence상 그 전략은 benefit가 확인된 적이 없고
    - 오히려
      - stale startup contamination
      - peak tab inflation
      - runtime interpretation 혼탁
      을 만든 쪽에 가깝다.
  - 따라서 지금 단계의 working conclusion은:
    - **"publisher를 미리 tab으로 열어 두기"는 현재 AIP path에서 의미 있는 전략이 아니라, 피해야 할 confound**다.

- 다만 남는 제한
  - 이 결론은
    - "현재 AIP landing 코드와 latest bundle evidence 기준"의 결론이다.
  - 만약 향후 별도 가설로
    - 특정 publisher homepage session cookie / consent state / institution banner state
    만을 seed profile에 남겨 놓고
    - startup restore tabs는 남기지 않는
    통제된 profile comparison 실험을 한다면
    - 그것은 다른 질문이다.
  - 하지만 그 경우에도 검증 단위는
    - "tabs를 열어 둔 상태"가 아니라
    - "쿠키/세션 저장 상태"여야 한다.

- 다음 기준
  - 앞으로 profile seed 전략은
    - restore tabs를 많이 보존하는 방향이 아니라
    - clean startup + persisted cookies/storage + direct DOI nav
    기준으로 평가한다.

5.30 Linux CLI server headful experiment support via Xvfb

- 목적
  - Linux CLI server에서 terminal-only 상태를 유지하면서도
    browser 자체는 non-headless로 띄우는 실험 경로를 추가했다.
  - 전제:
    - server에 `~/.local/bin/Xvfb`가 설치되어 있고
    - `DISPLAY`, `PATH`, `LD_LIBRARY_PATH`만 맞추면
      Xvfb 위에서 headful Chrome이 뜬다고 가정한다.

- 확인한 현재 launch path
  - suite launcher:
    - `scripts/run_linux_suite_bg.sh`
  - experiment entrypoint:
    - `experiment/run_linux_headless_suite.py`
  - browser launch:
    - `landing_access_repro.py::_browser_for_worker()`
    - `tools_exp.py::_apply_best_browser_profile()`
  - Selenium/Playwright config:
    - 별도 Playwright config file은 없었다.
    - Selenium 계열은 `tools_exp.py`의 `seleniumbase.Driver` import만 확인했고,
      current landing path의 primary browser control은 DrissionPage였다.

- 문제
  - 기존 `tools_exp.py::coerce_headless_for_execution_env()`는
    - `execution_env=linux_server`면
    - `headless=0` 요청도 무조건 `headless=true`로 강제했다.
  - 따라서 server에서 Xvfb display를 준비해도
    current runtime path는 headful 실험으로 내려갈 수 없었다.
  - 또한 `landing_access_repro.py::_run_chrome_smoke()`도
    - smoke probe를 무조건 `--headless=new`로 띄우고 있어
    - headful/Xvfb 경로의 사전검증과 맞지 않았다.

- 적용한 변경
  - `scripts/with_xvfb.sh` 추가
    - Xvfb start
    - `DISPLAY`, `PATH`, `LD_LIBRARY_PATH` export
    - `PDF_BROWSER_ALLOW_HEADFUL_LINUX_SERVER=1`
    - `PDF_BROWSER_XVFB_ACTIVE=1`
    - child process 종료 시 Xvfb cleanup
  - `scripts/run_linux_suite_bg.sh`
    - new options:
      - `--xvfb <auto|0|1>`
      - `--xvfb-display`
      - `--xvfb-bin`
      - `--xvfb-screen`
    - `headless=0` + `execution_env=linux_server` + `xvfb enabled`면
      generated cmd가 `scripts/with_xvfb.sh -- python ...run_linux_headless_suite.py ...`를 실행하도록 변경
    - launcher root log에
      - `headless`
      - `xvfb_enabled`
      - `xvfb_bin`
      - `xvfb_display`
      - `xvfb_screen`
      - `xvfb_log`
      을 남기도록 추가
  - `tools_exp.py`
    - `_linux_server_headful_allowed()` 추가
    - `DISPLAY`가 있고 `PDF_BROWSER_ALLOW_HEADFUL_LINUX_SERVER=1`이면
      `linux_server`에서도 headful 유지 허용
    - 그 외에는 기존처럼 headless 강제 유지
  - `landing_access_repro.py`
    - `_run_chrome_smoke()`가 `PDF_BROWSER_HEADLESS`를 따라
      - headless면 `--headless=new`
      - headful이면 non-headless smoke
      로 동작하도록 수정

- why this integration point
  - 가장 낮은 위험 지점은
    - `scripts/run_linux_suite_bg.sh`
    였다.
  - 이유:
    - suite 전체 프로세스(landing/download/summarize)를 한 번에 감쌀 수 있고
    - 기존 CLI workflow를 유지하면서
    - server headful only case에만 selective하게 적용할 수 있기 때문이다.
  - `run_linux_headless_suite.py` 내부 subprocess마다 Xvfb를 따로 붙이는 방식은
    - lifecycle과 cleanup이 더 복잡해져
    - 현재 목적에 비해 invasive했다.

- lightweight verification
  - syntax / import
    - `bash -n scripts/with_xvfb.sh scripts/run_linux_suite_bg.sh`
    - `python -m py_compile tools_exp.py landing_access_repro.py experiment/run_linux_headless_suite.py`
    - passed
  - headless coercion smoke
    - no display / no override:
      - `coerce_headless_for_execution_env(False, "linux_server") -> True`
    - `DISPLAY=:99` + `PDF_BROWSER_ALLOW_HEADFUL_LINUX_SERVER=1`:
      - `coerce_headless_for_execution_env(False, "linux_server") -> False`
  - wrapper smoke
    - current desktop env에는 real `~/.local/bin/Xvfb`가 없어서
      fake Xvfb stub로 wrapper lifecycle을 검증했다.
    - 확인된 것:
      - child process가 `DISPLAY=:201`을 받음
      - `PDF_BROWSER_ALLOW_HEADFUL_LINUX_SERVER=1` 전달됨
      - child exit 후 `[xvfb] stopped pid=...` cleanup 수행
  - launcher integration smoke
    - fake Xvfb + fake python shim으로
      `scripts/run_linux_suite_bg.sh --headless 0 --execution-env linux_server --xvfb 1 ...`
      경로를 검증했다.
    - 확인된 것:
      - generated cmd가 `scripts/with_xvfb.sh`를 감싸서 실행함
      - root log에 `xvfb_enabled=1`, `xvfb_display=:202`
      - child process가 `DISPLAY=:202`
      - child args에 `--headless 0`
      - wrapper cleanup 후 `[xvfb] stopped pid=...`

- 아직 불확실한 것 `[blocked]`
  - current desktop workspace에는 real `~/.local/bin/Xvfb`가 없어
    - actual server binary로 browser를 띄운 live verification은 아직 `[blocked]`
  - 따라서 남은 최종 검증은
    - real Linux server에서
    - `headless=0`
    - `xvfb_enabled=1`
    - landing/browser logs에서 `--headless=new`가 사라졌는지
    를 확인하는 것이다.

5.31 `xvfb_headful_probe_20260319_bundle`: headful/Xvfb landing improvement 확인, 하지만 download path는 아직 분리되어 있고 temp cleanup / timeout 문제가 남아 있음

- bundle summary
  - run: `xvfb_headful_probe_20260319`
  - manifest:
    - `status=completed_ok`
    - `headless=false`
    - `xvfb_enabled=1`
    - `Xvfb :99` start/stop 확인
  - 즉 Xvfb wrapper integration은 real server run에서도 실제로 동작했다.

- landing 결과
  - `landing_access_repro.jsonl`
    - `10.1063/5.0207496`
      - `entry_strategy=aip_direct_browser_doi`
      - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
      - `resolved_url=https://pubs.aip.org/aip/jcp/article/...`
      - challenge 없이 stable article landing success
    - `10.1116/6.0004298`
      - 같은 direct DOI branch
      - final URL이 `__cf_chl_rt_tk` challenge URL
      - `Just a moment...`
  - important:
    - startup cleanup patch가 먹어서
      - `controller_before_open.current_url=about:blank`
      - `peak_tab_count_observed=1`
    - 즉 stale startup tabs / 10-tab inflation 문제는 이 run에서는 해소된 것으로 보인다.

- download 결과
  - `download/run/summary.json`
    - success 0 / failed 2
    - bucket:
      - `challenge_or_interstitial=1`
      - `timeout_or_error=1`
  - `download.stderr.log`
    - download path는 여전히
      - `entry_strategy=aip_official_doi_resolve`
      - `variant=publisher_canonical_context_deferred_no_article_preflight`
      를 사용했다.
    - 즉 landing probe에서 성공시킨
      - `aip_direct_browser_doi`
      - `direct_doi_browser_start_no_preanalysis`
      가 **download runtime path에는 아직 적용되지 않았다.**
  - 결론:
    - current repo는
      - landing probe AIP path
      - download-integrated AIP path
      가 서로 다른 strategy를 쓰고 있다.
    - therefore landing success 1건이 바로 download path success로 이어지지 않았다.

- PDF file disappearance interpretation
  - current bundle에는 실제 성공 PDF가 남아 있지 않다.
  - `find outputs/.../download -type f` 기준으로도
    - final pdf file은 없고
    - logs/html/png/json만 남았다.
  - code 확인:
    - `tools_exp.py::download_pdf()`
      - DOI별 임시 download dir를
        - `artifact_root/.browser_tmp/<doi-key>/`
        아래에 만든다.
      - `_ret()`에서 성공/실패와 무관하게
        - `shutil.rmtree(browser_tmp_dir)`
        - empty면 `browser_tmp_root`도 정리한다.
    - 따라서 **final target path로 move되기 전의 파일은 attempt 종료 시 삭제될 수 있다.**
  - 이 구조는
    - "강제 종료 직전 temp dir에만 있던 PDF"
    - "잘못된 파일이라 guard에서 폐기된 PDF"
    를 bundle에서 보존하지 못한다.
  - 즉 현재 문제는 "성공 PDF가 삭제됐다"라기보다
    - temp-stage evidence retention이 부족한 문제에 가깝다.

- metadata sidecar lesson
  - `download/run/metadata/Closed_Access/*.json`은
    - result sidecar처럼 보이지만
    - 실제로는 largely input/OpenAlex snapshot이다.
  - 그래서 `result`, `downloaded_file`, `file_check`, `landing_entry_strategy`가 대부분 `null`이었다.
  - 실 download 판단값은
    - `openalex_search_results_parallel.csv`
    - `failed_papers.jsonl`
    - per-doi text log
    에 있고, metadata json은 forensics용으로는 부족하다.

- timeout slowness interpretation
  - first DOI는 `download.stderr.log`에서 disconnect가 비교적 이르게 발생했는데도
    progress bar 완료까지 `319.65s`가 걸렸다.
  - 현재 code상 nominal per-attempt timeout은
    - AIP first pass에서 `12s`
    정도라서,
    - 이 5분대 지연은 `page.get()` timeout 그 자체로 설명되지 않는다.
  - strongest current hypothesis:
    - browser disconnect 이후
      - process teardown / quit fallback / worker future completion
      중 한 구간이 과도하게 오래 걸린다.
  - 다만 bundle만으로는
    - 정확히 어떤 함수가 300s를 소비했는지는 아직 `[blocked]`
    - more granular timing log가 필요하다.

- `main` branch + `local_mac` 전략 판단
  - 이 run은 headful/Xvfb에서 AIP DOI 1건 landing success를 보여줬다.
  - 따라서
    - "headful이면 `local_mac` 스타일의 simpler browser path가 다시 먹힐 가능성"
    은 이전보다 분명히 올라갔다.
  - 그러나 wholesale rollback to `main`은 아직 성급하다.
    - 이유:
      - current server stack에는
        - Xvfb wrapper
        - Linux seeded profile clone
        - startup cleanup
        - stateful Linux temp/runtime dirs
        같은 Linux-specific scaffolding이 이미 필요하다고 확인됐기 때문이다.
  - working conclusion:
    - **`main/local_mac`의 simpler browser strategy를 참고/부분 이식하는 것은 유망**
    - but **branch 전체를 그대로 되돌리는 것은 low-risk choice가 아니다**
  - priority는
    - current Linux/Xvfb stack 위에
    - landing/download AIP strategy mismatch를 먼저 해소하는 것이다.

- next fixes implied by this bundle
  - AIP download path도 landing probe와 같은
    - `aip_direct_browser_doi`
    - `direct_doi_browser_start_no_preanalysis`
    로 맞출 것
  - temp `.browser_tmp` dir에 있는 downloaded artifact를
    - failure/kill case에서도 sidecar + preserve copy로 남길 것
  - disconnect 이후
    - browser quit
    - kill tree
    - future completion
    단계별 elapsed logging을 추가해 300s stall point를 계측할 것

- 아직 불확실한 것 `[blocked]`
  - "wrong PDF가 실제로 한 번 final path에 저장되었다가 나중에 삭제된 것"인지는
    current bundle만으로는 입증되지 않았다 `[blocked]`
  - disconnect 후 300s stall의 정확한 call site도 아직 `[blocked]`

## 5.32 2026-03-19 Linux + Xvfb headful default화 및 landing/download 정합성 복구

- source of truth
  - `docs/linux_seed_profile_setup.md`
  - `docs/xvfb_local_build_guide.md`
  - `README.md`
  - current branch `codex/linux_exp`
  - `local_mac` branch code
  - latest real run:
    - `experiment/results/xvfb_headful_probe_20260319_bundle.tar.gz`

- why this turn
  - user requirement changed:
    - Linux + Xvfb headful을 optional fallback이 아니라 **default runtime model**로 취급
    - successful `local_mac` strategy를 가능한 한 재사용
    - landing/download가 같은 browser/runtime strategy를 유지하도록 강제
  - bundle evidence상 current repo는
    - landing AIP path는 `aip_direct_browser_doi`
    - download AIP path는 `aip_official_doi_resolve`
    로 이미 drift가 발생해 있었다.

- docs/environment understanding
  - additional follow-up question은 이번 턴에서 불필요했다.
  - 이유:
    - `docs/xvfb_local_build_guide.md`에
      - `~/.local/bin/Xvfb`
      - `PATH`
      - `LD_LIBRARY_PATH`
      - `DISPLAY`
      가 source of truth로 이미 정리돼 있었고,
    - latest server run log에서
      - Xvfb wrapper start/stop
      - headful run
      - AIP landing success 1건
      이 실제 runtime evidence로 확인됐기 때문이다.

- `local_mac` branch에서 실제로 차용한 핵심
  - strategy-level:
    - headful browser request를 굳이 headless로 덮어쓰지 않는다.
    - browser 밖 pre-analysis보다 browser direct navigation을 우선한다.
    - same runtime strategy를 landing/download에 같이 적용한다.
  - not reused literally:
    - macOS system profile assumptions
    - local desktop only 전제
  - Linux adaptation:
    - Xvfb-backed display를 먼저 보장
    - Linux seeded profile clone/runtime dir는 그대로 유지

- code drift diagnosis before patch
  - `landing_access_repro.py`
    - AIP는 이미 `_build_aip_direct_doi_entry_plan()`
      - `entry_strategy=aip_direct_browser_doi`
      - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
      로 단순화되어 있었다.
  - `tools_exp.py::_build_download_entry_plan()`
    - AIP에서 아직 `build_aip_safe_entry_plan()`을 호출하고 있었다.
    - 따라서 real bundle에서 download stderr에
      - `aip_official_doi_resolve`
      - `publisher_canonical_context_deferred_no_article_preflight`
      가 남았다.
  - suite entrypoints
    - `scripts/run_linux_suite_bg.sh` default `HEADLESS=1`
    - `experiment/run_linux_headless_suite.py` default `--headless=1`
    - docs/README도 Linux server headful이 기본이 아닌 것처럼 설명하고 있었다.

- patches applied
  - `tools_exp.py`
    - `ensure_linux_xvfb_headful_runtime()` 추가
      - `linux_server` + requested headful + no display면
        - `~/.local/bin/Xvfb`(or env override) 자동 기동
        - `DISPLAY`, `PATH`, `LD_LIBRARY_PATH` 세팅
        - 종료 시 clean stop
      - 이미 display가 있으면 `PDF_BROWSER_ALLOW_HEADFUL_LINUX_SERVER=1`만 보장
    - `_linux_server_headful_allowed()` 완화
      - explicit false가 아니면 `DISPLAY` 존재만으로 headful 허용
    - `build_aip_direct_doi_entry_plan()`를 `tools_exp.py` 공용 helper로 승격
    - `_build_download_entry_plan()`에서 AIP default를
      - `build_aip_safe_entry_plan()`
      - 에서
      - `build_aip_direct_doi_entry_plan()`
      로 변경
  - `landing_access_repro.py`
    - local duplicate `_build_aip_direct_doi_entry_plan()`는 공용 helper thin wrapper로 변경
    - CLI entry에서 `ensure_linux_xvfb_headful_runtime()` 적용
      - direct invocation도 Xvfb headful default를 따르게 함
  - `parallel_download.py`
    - CLI entry에서 `ensure_linux_xvfb_headful_runtime()` 적용
      - direct invocation도 same default
  - `experiment/run_linux_headless_suite.py`
    - `--headless` default `1 -> 0`
    - description에서 "headless" 고정 표현 제거
  - `scripts/run_linux_suite_bg.sh`
    - `HEADLESS=1 -> 0`
    - help text도 default 0으로 수정
  - docs
    - `README.md`
      - Linux server에서 headless 강제라는 옛 설명 제거
      - `linux_cli_seeded`를 Xvfb headful default로 다시 기술
    - `docs/linux_seed_profile_setup.md`
      - server example에 `--headless 0` 추가
      - Xvfb guide를 함께 참조하도록 명시
    - `config.py`
      - CLI help text를 Linux headful default에 맞게 조정

- verification
  - `python -m py_compile tools_exp.py landing_access_repro.py parallel_download.py experiment/run_linux_headless_suite.py config.py`
    - pass
  - direct helper smoke:
    - `tools_exp._build_download_entry_plan("https://doi.org/10.1063/5.0207496")`
    - result:
      - `entry_strategy=aip_direct_browser_doi`
      - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
      - `entry_browser_url=https://doi.org/10.1063/5.0207496`
      - `entry_prebrowser_request_count=0`
  - Xvfb runtime smoke:
    - `DISPLAY` 없는 env에서 fake Xvfb binary로 확인
    - result:
      - `started=True`
      - `display=:123`
      - `coerce_headless_for_execution_env(False, "linux_server") -> False`
  - suite defaults smoke:
    - `experiment/run_linux_headless_suite.py` parser default `0`
    - `scripts/run_linux_suite_bg.sh` default `0`

- working conclusion after patch
  - Linux + Xvfb headful은 이제
    - suite launcher
    - direct landing CLI
    - direct download CLI
    에서 모두 default path로 정렬됐다.
  - AIP strategy는 landing/download 양쪽에서
    - direct browser DOI
    - no preanalysis
    로 다시 sync됐다.
  - 이는 `local_mac`의 "simpler headful browser path"를
    Linux/Xvfb stack 위에 가장 작은 범위로 이식한 것이다.

- still not solved this turn
  - `download_with_drission()` temp `.browser_tmp` artifact retention 부족
  - disconnect 후 300s stall의 precise timing breakdown
  - headful direct DOI로도 challenge가 남는 DOI(`10.1116/...`)는 여전히 존재

- next recommended experiments
  - same Xvfb headful default에서
    - landing + download가 둘 다 `aip_direct_browser_doi`를 실제 runtime log에 남기는지 확인
  - fresh/low-frequency AIP DOI로
    - headful direct DOI landing rate
    - integrated download follow-through
    를 재측정
  - separate follow-up patch:
    - temp artifact preservation
    - teardown timing instrumentation

- `[blocked]`
  - whole `main` branch를 그대로 가져오는 것이 현재 code보다 안전한지는 아직 미입증 `[blocked]`
  - 이유:
    - Linux/Xvfb-specific wrapper/profile/runtime scaffolding은 현재 branch에 이미 얹혀 있고,
    - 이번 turn에서는 strategy-level reuse만 검증했지 branch-level rollback은 검증하지 않았다.

## 5.33. 2026-03-19 Standalone landing 제거 및 download 단일 경로화

- source of truth
  - `docs/xvfb_local_build_guide.md`
  - `docs/linux_seed_profile_setup.md`
  - `docs/linux_headless_experiment_journal.md`
  - 최신 runtime 근거:
    - `experiment/results/xvfb_headful_probe_20260319_bundle.tar.gz`

- why
  - 최신 코드 상태에서도 suite runner는 여전히
    - standalone `landing_access_repro.py`
    - integrated `parallel_download.py`
    를 따로 실행하고 있었다.
  - 하지만 실제 download flow 안에는 이미
    - page entry selection
    - landing success/failure classification
    - challenge/interstitial detection
    - landing screenshot/html capture
    가 들어 있었다.
  - 결과적으로 landing/download drift를 다시 만들 위험이 컸다.

- what changed
  - `parallel_download.py`
    - `precheck-landing`의 실제 선검사 실행 경로 제거
    - deprecated 안내만 남기고, landing 검증은 항상 download 단일 패스로 수행
    - integrated landing 진단 필드를 CSV export에 추가
      - `browser_effective_user_data_dir`
      - `browser_debug_address`
      - `landing_peak_tab_count_observed`
      - `landing_reduced_tab_path_used`
      - `landing_page_disconnect_*`
      - `landing_probe_*`
      - `landing_controller_*`
      - `landing_startup_tab_cleanup_*`
      - `landing_tab_lifecycle_sequence`
      - `landing_js_*`
      - `landing_cookie/profile diagnostics`
      - `landing_aip_*`
      - `landing_entry_preanalysis_ran`
  - `experiment/run_linux_headless_suite.py`
    - standalone landing stage 제거
    - suite는 이제 `download -> summarize`만 실행
    - `executed_commands.landing`은 `merged_into_download`로 기록
  - `experiment/summarize_linux_headless_suite.py`
    - standalone landing JSONL이 없어도, download CSV의 integrated landing 필드로 summary를 생성
    - download-only run이면 그 데이터를 synthetic landing record로 간주
    - standalone landing report가 없으면 `download summary.integrated_landing`을 fallback summary로 사용
  - docs
    - `README.md`
      - canonical flow를 `parallel_download.py` 하나로 정리
      - landing-only usage 제거
      - `precheck-landing`은 deprecated/no-op로 명시
    - `docs/linux_seed_profile_setup.md`
      - landing-only 예시 제거
      - Linux/Xvfb headful 전제를 다시 명시
    - `experiment/README.md`
      - suite runner/summarizer 설명을 integrated flow 기준으로 갱신
    - `experiment/linux_headless_experiment_plan.md`
      - landing 측정 도구를 standalone script가 아니라 integrated landing field 기준으로 수정
    - `config.py`
      - `--precheck-landing` help를 deprecated/no-op 설명으로 수정

- deleted files
  - `landing_access_repro.py`
    - code/scripts/current docs 기준 live reference 제거 후 삭제
    - historical journal과 archived result bundle 안의 언급은 그대로 유지

- verification
  - `grep -R "landing_access_repro.py|landing_jsonl|landing-report" ...`
    - current code/scripts/docs 기준 live reference는 제거됨
    - 남은 항목은 historical journal, archived result bundle, optional backward-compatible parser arg뿐
  - `python -m py_compile tools_exp.py parallel_download.py experiment/run_linux_headless_suite.py experiment/summarize_linux_headless_suite.py config.py`
    - pass
  - `bash -n scripts/run_linux_suite_bg.sh scripts/with_xvfb.sh`
    - pass
  - helper smoke:
    - `build_aip_direct_doi_entry_plan("10.1063/5.0207496")`
      - `entry_strategy=aip_direct_browser_doi`
      - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
    - `coerce_headless_for_execution_env(False, "linux_server")`
      - `False`

- working conclusion
  - Linux + Xvfb headful 기본 전략은 유지한 채,
    landing validation responsibility를 download flow로 완전히 이동했다.
  - 이제 page-entry behavior의 single source of truth는 `parallel_download.py`다.
  - suite/summary/docs도 그 전제를 따르도록 맞췄다.

- remaining risks
  - 일부 historical docs/journal에는 `landing_access_repro.py` 언급이 남아 있다.
    - 이는 과거 실험 재현 기록이라 의도적으로 유지
  - `experiment/summarize_linux_headless_suite.py`는 backward compatibility를 위해
    `--landing-jsonl`, `--landing-report` optional arg를 아직 받는다.
    - 하지만 current suite path는 더 이상 이를 공급하지 않는다.

- `[blocked]`
  - merged flow에 대한 실제 Linux server rerun bundle은 아직 없다 `[blocked]`
  - 즉:
    - suite runner가 standalone landing 없이도 end-to-end 결과를 안정적으로 내는지
    - integrated landing summary가 실제 run artifact와 완전히 맞는지
    는 다음 server run으로 최종 확인이 필요하다.

### 5.34 2026-03-19 Codex: benchmark bundle `publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013` 정밀 분석 및 보완

- analyzed bundle
  - `experiment/results/publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013_bundle.tar.gz`
  - unpacked and inspected:
    - root log / execution manifest
    - `download/run/openalex_search_results_parallel.csv`
    - `download/run/failed_papers.jsonl`
    - `download/run/download_attempts_summary.json`
    - per-DOI HTML / screenshots / metadata sidecars
    - `summary/merged_results.csv`
    - `summary/publisher_summary.csv`

- confirmed runtime facts
  - Linux + Xvfb headful runtime was actually used.
    - root log contained:
      - `headless=0`
      - `xvfb_enabled=1`
      - `Xvfb` start/stop lines
  - suite path was the merged flow.
    - `landing_cmd_present=False`
    - `merged_into_download_present=True`
    - only download + summarize executed

- per-publisher outcome patterns from evidence
  - Elsevier
    - 4 samples
    - 3 native download successes
    - 1 landing success but no download: `10.1016/j.apcatb.2024.124297`
    - failure evidence:
      - article landing itself succeeded
      - 2-step click opened/targeted signed viewer
      - CFFI on signed viewer returned `FAIL_WRONG_MIME`
      - saved fail HTML was browser PDF embedder shell, not real article HTML
  - AIP
    - 4 samples
    - 1 direct DOI handoff success: `10.1116/6.0004298`
    - 2 Cloudflare challenge failures: `10.1063/5.0246311`, `10.1116/6.0004868`
    - 1 browser/page disconnect before stable landing: `10.1063/5.0257779`
  - IOP
    - 3 samples
    - all succeeded (`direct_oa` or API)
    - merged flow did not need separate landing-only behavior here
  - IEEE
    - 4 samples
    - 3 native fastpath successes
    - 1 Sci-Hub-assisted success

- confirmed failure points
  - Elsevier viewer fallback weakness
    - after signed PDF viewer handoff, `FAIL_WRONG_MIME` fell through to generic parser path
    - generic parser then continued without a viewer-aware recovery
    - evidence: fail HTML was a Chrome PDF embedder shell and not a useful article page
  - merged-flow diagnostics drift
    - `landing_peak_tab_count_observed=0` for all rows even when `landing_final_total_tab_count` was `10~12`
    - `landing_tab_lifecycle_sequence=[]` for all rows
    - `landing_probe_browser_process_alive=False` / `landing_probe_page_access_ok=False` were not trustworthy as runtime diagnostics
    - cause: standalone landing deletion left several fields exported by `parallel_download.py` but not populated by `download_with_drission()`
  - publisher summary rollup bug
    - benchmark CSV had `benchmark_group` / `scheduler_publisher`
    - summary still rolled everything into `other`
    - cause: summarizer only trusted `experiment_publisher_group`

- non-confirmed / corrected interpretations
  - metadata sidecar null-top-level problem was *not* reproduced in this bundle.
    - sidecars did include `record` and `openalex` payloads with real values.
    - previous suspicion was withdrawn after direct file inspection.
  - cross-worker shared profile collision was not evidenced.
    - worker runtime profile roots were per-process.
    - concurrency/resource fragility remains a possible inference, but not a confirmed root cause from this bundle alone.

- implemented fixes
  - `tools_exp.py`
    - fixed `force_download_with_requests()` cookie extraction.
      - previous code used only the first `page.cookies()` item.
      - now it builds a real cookie jar from all browser cookies.
    - added `_looks_like_browser_pdf_embedder_shell()`.
    - hardened `_download_elsevier_signed_pdf_from_viewer()`.
      - if signed-viewer CFFI ends with `FAIL_WRONG_MIME`, retry once with `requests` + browser cookies + referer instead of immediately falling through.
    - added merged-flow tab/runtime diagnostics directly inside `download_with_drission()`.
      - `peak_tab_count_observed`
      - `tab_lifecycle_sequence`
      - `browser_process_alive`
      - `page_access_ok`
      - `page_probe_error`
      - `startup_tab_cleanup_*`
      - `startup_page_reset_to_blank`
    - added startup tab cleanup and blank reset before navigation in the unified flow.
      - goal: reduce restored-tab contamination and keep headful Linux startup closer to the successful local_mac-style clean browser start
    - added post-Elsevier-click tab trim so extra tabs do not accumulate silently before generic fallback.
  - `experiment/summarize_linux_headless_suite.py`
    - added publisher rollup fallback order:
      - `experiment_publisher_group`
      - `benchmark_group`
      - `scheduler_publisher`
      - normalized `publisher`
    - added display-name fallback from `publisher` when `publisher_display_name` is missing

- verification
  - `python -m py_compile tools_exp.py parallel_download.py experiment/summarize_linux_headless_suite.py experiment/run_linux_headless_suite.py config.py`
    - pass
  - reran summarizer against the analyzed bundle’s download outputs using the benchmark input CSV
    - publisher rollup changed from a single `other` bucket to:
      - `elsevier`: 4
      - `aip`: 4
      - `iop`: 3
      - `ieee`: 4
    - this confirmed the summary fix on real benchmark evidence

- reproduction / re-check commands
  - unpack bundle:
    - `python - <<'PY'`
    - `import tarfile`
    - `bundle='experiment/results/publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013_bundle.tar.gz'`
    - `out='/private/tmp/publisher_benchmark_bundle_inspect'`
    - `with tarfile.open(bundle,'r:gz') as tf: tf.extractall(out)`
    - `print(out)`
    - `PY`
  - re-run summarizer with current code:
    - `python experiment/summarize_linux_headless_suite.py --suite full --sample-csv outputs/benchmark_inputs/publisher_download_benchmark_elsevier_aip_ieee_spie_iop.csv --download-results-csv /private/tmp/publisher_benchmark_bundle_inspect/outputs/linux_headless_suite_runs/publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013/download/run/openalex_search_results_parallel.csv --download-summary-json /private/tmp/publisher_benchmark_bundle_inspect/outputs/linux_headless_suite_runs/publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013/download/run/summary.json --merged-csv /private/tmp/codex_summary_recheck_20260319/merged_results.csv --publisher-summary-csv /private/tmp/codex_summary_recheck_20260319/publisher_summary.csv --summary-json /private/tmp/codex_summary_recheck_20260319/suite_summary.json --summary-md /private/tmp/codex_summary_recheck_20260319/suite_summary.md`

- remaining uncertainty
  - Elsevier signed-viewer fallback improvement is code-complete, but still needs a fresh server rerun to confirm it recovers `10.1016/j.apcatb.2024.124297`
  - AIP challenge failures remain confirmed but not fixed here.
    - this turn focused on artifact-evidenced logic defects, not speculative challenge workarounds
  - AIP disconnect for `10.1063/5.0257779` remains only partially explained.
    - evidence supports a browser/page runtime disconnect, but the exact low-level trigger is still `[blocked]`

### 5.35 2026-03-19 Codex: AIP access path 정밀 진단 및 headful+profile 활용 보완

- analyzed bundle again with AIP-only focus
  - `experiment/results/publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013_bundle.tar.gz`
  - inspected AIP-specific artifacts:
    - `download/run/openalex_search_results_parallel.csv`
    - `download/run/failed_papers.jsonl`
    - `download/run/Closed_Access/logs/download_log_10.1063_5.0257779.pdf.pdf.txt`
    - `download/run/Closed_Access/logs/html/landing_fail_10.1063_5.0246311.html`
    - `download/run/Closed_Access/logs/html/landing_fail_10.1116_6.0004868.html`
    - `download/run/Open_Access/logs/html/landing_success_10.1116_6.0004298.html`
    - benchmark input rows in `outputs/benchmark_inputs/publisher_download_benchmark_elsevier_aip_ieee_spie_iop.csv`

- confirmed AIP outcome split
  - `10.1116/6.0004298`
    - direct DOI route succeeded
    - article page was reached and direct PDF handoff completed
  - `10.1063/5.0246311`, `10.1116/6.0004868`
    - direct DOI route reached canonical AIP article URL shape
    - final page was Cloudflare challenge shell (`Just a moment...`, `__cf_chl_rt_tk`, `challenge-platform`)
    - this is a real first-contact challenge outcome, not a classifier artifact
  - `10.1063/5.0257779`
    - did not reach a stable article/challenge landing state
    - log showed repeated startup-tab pruning immediately after browser open
    - then a long stall and finally `The connection to the page has been disconnected. Version: 4.1.1.2`
    - this is best explained as runtime/page survivability failure on the initial controlled page

- step-by-step understanding of the current AIP flow before this patch
  - unified flow already used `aip_direct_browser_doi`
    - `entry_strategy_variant=direct_doi_browser_start_no_preanalysis`
    - `entry_browser_url=https://doi.org/<doi>`
  - no separate landing-only path was involved
  - in headful Linux runs the code still started from the current controller page, then:
    - pruned restored tabs
    - reset page to `about:blank`
    - navigated the same page to DOI
  - this worked for the successful AIP DOI, but the disconnect case showed that reusing the startup controller page was still not deterministic enough even after tab pruning

- confirmed root cause that is fixable without speculative anti-bot work
  - the meaningful defect was not “wrong entry ordering” anymore
  - the meaningful defect was:
    - headful + seeded-profile Linux run still reused the startup controller page for the AIP first-contact DOI navigation
    - that page could already be contaminated by session-restore/runtime state even after extra tabs were closed
    - when the page/runtime detached during the first DOI navigation, the result was recorded only as generic network failure and hid that landing never truly started

- implemented AIP-specific fixes
  - `tools_exp.py`
    - added `_aip_direct_entry_fresh_tab_enabled()`
      - default: enabled for `linux_server` when effective headless mode is false
      - env override: `PDF_BROWSER_AIP_DIRECT_DOI_FRESH_TAB`
    - updated `_prepare_aip_entry_navigation_page()`
      - for AIP direct DOI first-contact on Linux headful, open a fresh temporary tab first
      - navigate DOI from that fresh tab instead of reusing the startup controller page
      - new route marker: `fresh_tab_before_direct_doi`
    - strengthened disconnect diagnostics in `download_with_drission()`
      - added:
        - `page_disconnect_observed`
        - `page_disconnect_stage`
      - main DOI navigation disconnect is now tagged explicitly (`main_navigation` / `unexpected_retry_navigation`)
      - if disconnect happens before stable landing, `landing_state` is set to `runtime_disconnect`
      - terminal classification stays `FAIL_TIMEOUT/NETWORK`, but now the landing state explains that the failure happened before real landing validation or download trigger

- why this is a meaningful patch
  - it uses the newly available headful + seeded-profile runtime as an asset:
    - keep cookies/session from the persistent profile
    - avoid reusing a fragile startup page for first contact
  - it does not reintroduce a separate landing-only path
  - it keeps AIP behavior inside the unified download flow
  - it makes the failure split explicit:
    - `challenge_or_block`
    - `runtime_disconnect`
    - `direct_pdf_handoff`

- verification
  - `python -m py_compile tools_exp.py`
    - pass
  - smoke check:
    - `build_aip_direct_doi_entry_plan('https://doi.org/10.1063/5.0257779')`
      - still returns direct DOI browser entry
    - `_aip_direct_entry_fresh_tab_enabled()`
      - true under `PDF_BROWSER_EXECUTION_ENV=linux_server` and `PDF_BROWSER_HEADLESS=0`

- re-check commands for next server run
  - `bash scripts/run_linux_suite_bg.sh --suite full --run-name aip_focus_xvfb_headful_recheck --sample-csv outputs/benchmark_inputs/publisher_download_benchmark_elsevier_aip_ieee_spie_iop.csv --download-workers 3 --after-first-pass stop --runtime-preset linux_cli_seeded --execution-env linux_server --headless 0 --xvfb 1 --xvfb-bin "$HOME/.local/bin/Xvfb" --xvfb-display :99`
  - inspect these fields in `download/run/openalex_search_results_parallel.csv`
    - `landing_entry_navigation_route`
    - `landing_page_disconnect_observed`
    - `landing_page_disconnect_stage`
    - `landing_state`
    - `landing_final_url`

- remaining uncertainty
  - Cloudflare challenge outcomes for `10.1063/5.0246311` and `10.1116/6.0004868` remain unchanged by this patch.
    - this patch intentionally does not attempt challenge bypass.
  - exact low-level reason why the reused startup page detached for `10.1063/5.0257779` is still not fully proven.
    - current conclusion is an evidence-backed inference from logs and runtime symptoms.

### 5.36 2026-03-19 Codex: Drission browser startup failure (`The browser connection fails`) 원인 확정 및 launch isolation 보완

- observed symptom
  - recent headful Linux/Xvfb runs emitted:
    - `[Drission] 브라우저 실행 실패(1/3):`
    - `The browser connection fails.`
    - `Address: 127.0.0.1:<port>`
    - `Tip:`
      - `the user folder does not conflict with the open browser`
      - `if no interface system, please add '--headless=new'`
      - `if the system is Linux, try adding '--no-sandbox'`
  - one concrete artifact:
    - `publisher_download_benchmark_elsevier_aip_ieee_iop_xvfb_20260319_165013`
    - `download/run/Open_Access/logs/download_log_10.1016_j.ccr.2024.215942.pdf.pdf.txt`

- confirmed code-level causes
  - retries reused the same launch configuration.
    - `download_with_drission()` created `ChromiumOptions()` once
    - `co.set_local_port(_pick_free_local_port())` ran once
    - browser init retry loop then retried `ChromiumPage(co)` with the same port / same options
    - therefore a bad first attach could repeat the same root cause
  - cleanup matcher was stale.
    - `_is_drission_browser_root_command()` only matched old `DrissionPage/autoPortData` style command lines
    - current runtime uses cloned `user-data-dir` roots under `/tmp/.../download_runtime_profiles/...`
    - so `_kill_browser_processes_by_user_data_dir()` could miss the actual runtime Chrome root process
  - Linux `--no-sandbox` was still opt-in only.
    - code added it only when `PDF_BROWSER_NO_SANDBOX=1`
    - current Linux/Xvfb server path is rootless and the observed Drission error explicitly recommended `--no-sandbox`
    - this did not prove sandbox was the only cause, but it was a confirmed missing stabilizer in the default Linux path
  - diagnostics were insufficient.
    - launch failure logs did not show which `port`, `DISPLAY`, `user_data_dir`, or `no_sandbox` setting was actually used

- implemented fixes
  - `tools_exp.py`
    - `_linux_no_sandbox_enabled()`
      - `linux_server` now defaults to `--no-sandbox --disable-setuid-sandbox`
      - explicit `PDF_BROWSER_NO_SANDBOX=0` still disables it
    - `_is_drission_browser_root_command()`
      - broadened to match current Chrome/Chromium root processes with:
        - `--remote-debugging-port=...`
        - `--user-data-dir=...`
      - excludes helper / crashpad / typed child processes
    - `_kill_browser_processes_by_debug_port()`
      - added targeted cleanup by remote debugging port
    - `_cleanup_browser_startup_artifacts()`
      - removes `Singleton*` and `DevToolsActivePort` only for owned runtime dirs (`temp` or `*_clone`)
    - `download_with_drission()`
      - browser init now rebuilds `ChromiumOptions()` per init attempt
      - each init attempt gets a fresh local port
      - failed init performs:
        - kill by debug port
        - kill by owned user-data-dir
        - startup artifact cleanup
      - logs now print the actual launch configuration:
        - `port`
        - `display`
        - `headless`
        - `no_sandbox`
        - `user_data_dir`
      - detail payload now records:
        - `browser_launch_port`
        - `browser_launch_display`
        - `browser_launch_headless`
        - `browser_launch_no_sandbox`
        - `browser_init_attempts`

- why this fits the current direction
  - Linux + Xvfb headful remains the default runtime model
  - no separate landing-only path was reintroduced
  - startup recovery is now aligned with the unified download flow
  - retries no longer blindly repeat the same broken attach state

- lightweight verification
  - `python -m py_compile tools_exp.py parallel_download.py experiment/run_linux_headless_suite.py config.py`
    - pass
  - smoke:
    - `PDF_BROWSER_EXECUTION_ENV=linux_server`, `PDF_BROWSER_HEADLESS=0`, no explicit `PDF_BROWSER_NO_SANDBOX`
    - `_linux_no_sandbox_enabled('linux_server') -> True`
    - `_apply_best_browser_profile(ChromiumOptions())` now includes:
      - `--no-sandbox`
      - `--disable-setuid-sandbox`
    - `_is_drission_browser_root_command('--remote-debugging-port=41349 --user-data-dir=/tmp/...') -> True`

- re-check command
  - `bash scripts/run_linux_suite_bg.sh --suite full --run-name drission_startup_recheck_20260319 --sample-csv outputs/benchmark_inputs/publisher_download_benchmark_elsevier_aip_ieee_spie_iop.csv --download-workers 3 --after-first-pass stop --runtime-preset linux_cli_seeded --execution-env linux_server --headless 0 --xvfb 1 --xvfb-bin "$HOME/.local/bin/Xvfb" --xvfb-display :99`
  - inspect:
    - `logs/<run>.log`
    - `outputs/linux_headless_suite_runs/<run>/logs/download.stderr.log`
    - `download/run/openalex_search_results_parallel.csv`
    - especially:
      - `browser_launch_port`
      - `browser_launch_display`
      - `browser_launch_no_sandbox`
      - `browser_init_attempts`

- remaining uncertainty
  - this patch addresses confirmed launch-stability defects and missing diagnostics
  - it does not prove that every future `browser connection fails` was caused by sandbox/port/profile issues alone
  - a fresh server rerun is still required to measure how much startup determinism improved under real benchmark concurrency

## 5.37 2026-03-19: `drission_startup_verify_20260319_210654` bundle re-analysis and launch regression fix

- analyzed bundle
  - `/Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download/experiment/results/drission_startup_verify_20260319_210654_bundle.tar.gz`
  - unpacked under `/private/tmp/drission_startup_verify_20260319_210654`

- files that mattered
  - root run log
    - `logs/drission_startup_verify_20260319_210654.log`
  - execution manifest
    - `outputs/linux_headless_suite_runs/drission_startup_verify_20260319_210654/execution_manifest.json`
  - unified download outputs
    - `.../download/run/openalex_search_results_parallel.csv`
    - `.../download/run/failed_papers.jsonl`
    - `.../download/run/summary.json`
    - `.../logs/download.stderr.log`

- confirmed observations
  - Linux + Xvfb headful was actually used.
    - root log showed `headless=0`, `xvfb_enabled=1`, and Xvfb start/stop
  - unified landing+download flow was actually used.
    - `execution_manifest.json` had `landing.skipped=true` with reason `merged_into_download`
  - the dominant failure in this verification bundle was **not** a residual Drission attach error.
    - 6 / 7 rows failed at `download_result_stage=worker`
    - `download_evidence=["name 'headless' is not defined"]`
    - affected publishers:
      - Elsevier
      - AIP
      - IEEE
      - one `other` row
  - only 1 / 7 row succeeded, and it was a direct OA IOP path that bypassed the broken browser branch.

- confirmed root cause
  - the previous startup patch introduced a regression inside `tools_exp.py::download_with_drission()`
  - nested helper `_make_browser_options()` called:
    - `coerce_headless_for_execution_env(bool(headless), ...)`
  - but `download_with_drission()` has no local `headless` variable
  - therefore worker processes raised `NameError: name 'headless' is not defined` before the patched Drission startup loop could run
  - this explains why:
    - browser launch diagnostics were mostly missing
    - `landing_state` stayed `not_attempted`
    - `browser_launch_*` fields were empty in the bundle

- implemented fixes
  - `tools_exp.py`
    - `download_with_drission()`
      - now derives `requested_browser_headless` from `PDF_BROWSER_HEADLESS`
      - reuses a single resolved execution env instead of recomputing it
      - no longer references undefined `headless`
      - launch diagnostics now also record:
        - `browser_launch_binary_path`
        - `browser_launch_worker_label`
      - launch log line now prints:
        - `attempt`
        - `port`
        - `display`
        - `headless`
        - `no_sandbox`
        - `worker`
        - `binary`
        - `user_data_dir`
  - `parallel_download.py`
    - unified result export now persists:
      - `browser_launch_binary_path`
      - `browser_launch_worker_label`
      - `browser_launch_port`
      - `browser_launch_display`
      - `browser_launch_headless`
      - `browser_launch_no_sandbox`
      - `browser_init_attempts`
    - `future.result()` worker exception handling now stores:
      - exception class
      - tail of traceback
      - `reason=FAIL_UNKNOWN`
      - `stage=worker_exception`
    - this avoids future opaque `stage=worker` / `FAIL_TIMEOUT/NETWORK` misclassification for pure Python regressions

- why this matters
  - the startup-fix verification bundle could not actually validate the intended Drission attach patch because the launch helper crashed first
  - this regression fix is required before any publisher-level conclusions from that benchmark are meaningful
  - Linux + Xvfb headful remains the default runtime model
  - unified landing+download remains the only page-entry path

- re-check command
  - focused verification subset:
    - `bash scripts/run_linux_suite_bg.sh --suite full --run-name drission_startup_verify_rerun_20260319 --seed-profile "$SEED_PROFILE" --profile-name "${PROFILE_NAME:-Default}" --sample-csv outputs/benchmark_inputs/publisher_download_benchmark_startup_verify_20260319.csv --download-workers 3 --after-first-pass stop --runtime-preset linux_cli_seeded --execution-env linux_server --headless 0 --chrome-path "$CHROME_PATH" --xvfb 1 --xvfb-bin "$HOME/.local/bin/Xvfb" --xvfb-display :99`
  - inspect after rerun:
    - `logs/<run>.log`
    - `outputs/linux_headless_suite_runs/<run>/logs/download.stderr.log`
    - `outputs/linux_headless_suite_runs/<run>/download/run/openalex_search_results_parallel.csv`
    - check especially:
      - `browser_launch_binary_path`
      - `browser_launch_worker_label`
      - `browser_launch_port`
      - `browser_launch_display`
      - `browser_launch_no_sandbox`
      - `browser_init_attempts`

- remaining uncertainty
  - until the rerun is completed, this fix only proves that the worker-level regression was removed in code
  - residual publisher-specific failures from the old bundle remain weak evidence because most rows never reached real browser startup
