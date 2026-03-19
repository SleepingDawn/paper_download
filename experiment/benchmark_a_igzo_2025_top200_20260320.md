# A-IGZO 2025 Citation Top-200 Benchmark

## 목적

`a-igzo`를 OpenAlex `title_and_abstract.search`로 검색한 뒤, `publication_year=2025`인 article만 citation 순으로 정렬해 top 200 입력을 준비한다.

이 문서는 아래를 기록한다.

- 입력 생성 조건
- 생성된 CSV 경로와 통계
- Linux + Xvfb headful 실험 준비 명령

## 입력 생성

- 생성 스크립트: `experiment/build_query_benchmark.py`
- query: `a-igzo`
- year: `2025`
- limit: `200`
- 출력 CSV: `experiment/benchmark_a_igzo_2025_top200_20260320.csv`

생성 명령:

```bash
python3 experiment/build_query_benchmark.py \
  --query 'a-igzo' \
  --year 2025 \
  --limit 200 \
  --output-csv experiment/benchmark_a_igzo_2025_top200_20260320.csv
```

## 입력 통계

- 총 행 수: `200`
- publication_year: `2025`만 포함
- citation 범위:
  - 최대 `36`
  - 최소 `1`
  - 중앙값 `3.0`
- open access:
  - `True`: `69`
  - `False`: `131`
- 상위 publisher 분포:
  - `Elsevier BV`: `48`
  - `American Chemical Society`: `34`
  - `Wiley`: `33`
  - `Institute of Electrical and Electronics Engineers`: `24`
  - `Springer Science+Business Media`: `11`
  - `American Institute of Physics`: `10`

상위 10건:

1. `10.1016/j.commatsci.2025.113701` (`36`, Elsevier BV)
2. `10.1016/j.nanoen.2025.110837` (`21`, Elsevier BV)
3. `10.1002/adts.202500182` (`20`, Wiley)
4. `10.1016/j.jmst.2024.12.052` (`18`, Elsevier BV)
5. `10.1088/2631-8695/ada721` (`17`, IOP Publishing)
6. `10.1002/advs.202500568` (`15`, Wiley)
7. `10.1007/s10904-025-03629-3` (`15`, Springer Science+Business Media)
8. `10.1021/acsaelm.5c00605` (`14`, American Chemical Society)
9. `10.1002/adom.202500634` (`12`, Wiley)
10. `10.1016/j.jallcom.2025.179753` (`12`, Elsevier BV)

## 실행 준비

권장 런타임:

- Linux + Xvfb headful
- unified landing+download flow
- `runtime-preset=linux_cli_seeded`
- `execution-env=linux_server`
- `headless=0`

서버 준비:

```bash
cd /home/yongyong0206/paper_search/paper_download
source /home/yongyong0206/paper_search/paper_download/.venv/bin/activate

bash scripts/prepare_linux_server_env.sh \
  --seed-profile /home/yongyong0206/paper_search/paper_download/outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed \
  --chrome-path /home/yongyong0206/chrome-linux/chrome-linux64/chrome \
  --python /home/yongyong0206/paper_search/paper_download/.venv/bin/python3

source /home/yongyong0206/paper_search/paper_download/config/linux_server.env
```

실행:

```bash
cd /home/yongyong0206/paper_search/paper_download
export RUN_NAME="a_igzo_2025_top200_$(date +%Y%m%d_%H%M%S)"

bash scripts/run_linux_suite_bg.sh \
  --suite full \
  --run-name "$RUN_NAME" \
  --seed-profile "$SEED_PROFILE" \
  --profile-name "${PROFILE_NAME:-Default}" \
  --sample-csv /home/yongyong0206/paper_search/paper_download/experiment/benchmark_a_igzo_2025_top200_20260320.csv \
  --download-workers 3 \
  --after-first-pass stop \
  --runtime-preset linux_cli_seeded \
  --execution-env linux_server \
  --headless 0 \
  --chrome-path "$CHROME_PATH" \
  --xvfb 1 \
  --xvfb-bin "$HOME/.local/bin/Xvfb" \
  --xvfb-display :99
```

모니터링:

```bash
cd /home/yongyong0206/paper_search/paper_download
bash scripts/check_linux_suite_status.sh "$RUN_NAME"
bash scripts/tail_linux_suite_logs.sh "$RUN_NAME" all
```

종료 확인:

```bash
cd /home/yongyong0206/paper_search/paper_download
bash scripts/check_linux_suite_status.sh "$RUN_NAME" 60
ps -fp "$(cat logs/${RUN_NAME}.pid)" || true
```

결과 수집:

```bash
cd /home/yongyong0206/paper_search/paper_download
mkdir -p experiment/results

bash scripts/collect_linux_suite_artifacts.sh \
  "$RUN_NAME" \
  --include-pdfs 1 \
  --output "/home/yongyong0206/paper_search/paper_download/experiment/results/${RUN_NAME}_bundle.tar.gz"
```

## 메모

- CSV는 기존 benchmark와 같은 선두 컬럼 `doi,publisher,pdf_url,open_access,title`를 유지한다.
- 추가로 `cited_by_count`, `publication_year`, `openalex_id`, resolution/routing 관련 메타를 같이 넣어 provenance를 남긴다.
- runner는 추가 컬럼을 보존한 채 unified download flow로 전달할 수 있다.
