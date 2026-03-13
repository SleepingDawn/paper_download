# Publisher Triage Benchmark

## 목적

publisher별로 어떤 실패가 반복되는지 빠르게 확인하기 위한 균형 샘플 벤치마크다.

이 벤치마크는 다음을 동시에 보려는 용도다.

- landing challenge가 주원인인 publisher
- viewer wrapper / signed PDF / candidate recovery가 약한 publisher
- direct OA control 역할을 하는 publisher

## 입력 생성

생성 스크립트:

- `experiment/build_publisher_triage_benchmark.py`

기본 생성 규칙:

- 원본 입력: `ready_to_download.csv`
- 대상 publisher: `elsevier, acs, wiley, aip, iop, ieee, spie, springer, nature, rsc, mdpi, aps`
- publisher당 기본 `3`건
- 가능한 경우 아래를 섞어 뽑음
  - closed access 1건
  - open access + pdf_url 있음 1건
  - open access + pdf_url 없음 1건

산출 CSV:

- `experiment/publisher_triage_benchmark_20260313.csv`

추가 컬럼:

- `scheduler_publisher`
- `benchmark_group`
- `benchmark_case_hint`
- `benchmark_rank_within_publisher`

`benchmark_case_hint` 해석:

- `landing_challenge`
- `viewer_wrapper`
- `asset_gate`
- `cookie_or_viewer_gate`
- `direct_pdf_control`

## 재생성 명령

```bash
python3 experiment/build_publisher_triage_benchmark.py \
  --input ready_to_download.csv \
  --output experiment/publisher_triage_benchmark_20260313.csv \
  --per-publisher 3
```

## 권장 실행

landing-only:

```bash
python3 -u landing_access_repro.py \
  --input experiment/publisher_triage_benchmark_20260313.csv \
  --workers 1 \
  --runtime-preset linux_cli_cold \
  --output-jsonl outputs/publisher_triage_benchmark_20260313_landing.jsonl \
  --report outputs/publisher_triage_benchmark_20260313_landing.report.json \
  --report-md outputs/publisher_triage_benchmark_20260313_landing.report.md
```

download:

```bash
PDF_WORKER_MAX_TASKS_PER_CHILD=20 \
python3 -u parallel_download.py \
  --doi_path experiment/publisher_triage_benchmark_20260313.csv \
  --runtime-preset linux_cli_cold \
  --max_workers 1 \
  --precheck-landing 0 \
  --abort-on-landing-block 1 \
  --after-first-pass stop \
  --output_dir outputs/publisher_triage_benchmark_20260313_run \
  --pdf_output_dir pdfs/publisher_triage_benchmark_20260313_run \
  --non-interactive
```

## 분석 포인트

- Elsevier/ACS/AIP/IOP: landing state가 `challenge_or_block`로 몰리는지
- IEEE: `stamp.jsp/getPDF.jsp` 이후 `FAIL_VIEWER_HTML`이 남는지
- SPIE: article landing 이후 `FAIL_NO_CANDIDATE`가 남는지
- Wiley: `cookieAbsent`나 viewer wrapper가 남는지
- Nature/Springer/RSC/MDPI/APS: direct OA control publisher로 정상 비율이 유지되는지
