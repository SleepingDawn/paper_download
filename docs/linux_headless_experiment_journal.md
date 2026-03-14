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

### 아직 미검증 또는 근거 부족

- 최신 AIP redirect-only/no-article-preflight branch가 실제 Linux 서버 bundle에서 landing success를 올렸는지 `[blocked]`
- Elsevier shell recovery가 "shell-only live case"에서 end-to-end로 회복되는지 `[blocked]`
- `10.1007_` classifier fix가 server rerun에서 false-failure를 실제로 줄였는지 `[blocked]`
- attempt ledger 파일 자체의 최신 누적 상태는 현재 로컬 워크스페이스에서 확인되지 않음 `[blocked]`
- 새 AIP structural patch가 Linux server/headless에서도 Google default page incidence를 실제로 0으로 낮추는지 `[blocked]`
- AIP stable landing 이후의 downstream click/download disconnect가 Linux headless에서도 남는지 `[blocked]`
- 새 AIP context bootstrap branch가 Linux server/headless에서 challenge incidence를 실제로 낮추는지 `[blocked]`

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
