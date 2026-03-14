# Publisher Download Benchmark

## 목적

publisher별 실제 다운로드 품질을 빠르게 비교하는 서버용 benchmark다.

이 입력은 landing-only 분류보다 `parallel_download.py`의 end-to-end download 경로를 보는 데 초점을 둔다.

## 입력

- `experiment/publisher_download_benchmark_20260314.csv`

생성 규칙:

- 원본: `ready_to_download.csv`
- 대상 publisher: `elsevier, acs, wiley, aip, iop, ieee, spie, springer, nature, rsc, mdpi, aps`
- publisher당 `4`건
- 가능한 경우 아래를 우선 섞어 뽑음
  - closed access
  - open access + pdf_url 있음
  - open access + pdf_url 없음
  - 남는 1건은 score 상위 DOI

IOP bucket에는 현재 서버 관측과 맞추기 위해 `10.1088`, `10.1149`, `10.7567` prefix를 같이 포함한다.

## 재생성

```bash
python3 experiment/build_publisher_triage_benchmark.py \
  --input ready_to_download.csv \
  --output experiment/publisher_download_benchmark_20260314.csv \
  --per-publisher 4
```

## 권장 실행

다운로드 benchmark는 전역 precheck 없이 돌리는 편이 맞다.

전체 48건 benchmark:

```bash
bash scripts/run_publisher_benchmark_detached.sh \
  --profile-root ~/chrome_profiles/linux_chromium_user_data_seed \
  --run-name publisher_download_benchmark_20260314
```

특정 publisher만:

```bash
bash scripts/run_publisher_benchmark_detached.sh \
  --profile-root ~/chrome_profiles/linux_chromium_user_data_seed \
  --publisher elsevier \
  --run-name publisher_download_benchmark_elsevier_20260314
```

여러 publisher 묶음:

```bash
bash scripts/run_publisher_benchmark_detached.sh \
  --profile-root ~/chrome_profiles/linux_chromium_user_data_seed \
  --publisher elsevier,wiley,iop \
  --run-name publisher_download_benchmark_high_friction_20260314
```

로그 확인:

```bash
tail -f logs/publisher_download_benchmark_20260314.log
```

종료 후 bundle:

```bash
bash scripts/package_analysis_bundle.sh publisher_download_benchmark_20260314
```

## 분석 포인트

- Elsevier: `FAIL_NO_CANDIDATE`, `FAIL_CAPTCHA`, `landing_state`
- Wiley: cookie/viewer wrapper 실패가 남는지
- AIP/IOP: challenge/time out 비율과 seeded profile 효과
- IEEE/SPIE: viewer wrapper와 candidate recovery
- Nature/Springer/RSC/MDPI/APS: control group으로 정상 비율 유지 여부
