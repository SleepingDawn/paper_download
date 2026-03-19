# OLED 2025+ Citation Top-100 Benchmark

## 목적

`OLED`를 OpenAlex `title_and_abstract.search`로 검색한 뒤, `publication_year>=2025`인 article만 citation 순으로 정렬해 top 100 입력을 준비한다.

이 문서는 아래를 기록한다.

- 입력 생성 조건
- 생성된 CSV 경로와 통계
- Linux + Xvfb headful 실험 준비 명령

## 입력 생성

- 생성 스크립트: `experiment/build_query_benchmark.py`
- query: `OLED`
- min_year: `2025`
- top_k: `100`
- sort: `cited_by_count:desc`
- benchmark_name: `benchmark_oled_2025plus_top100_20260320`
- 출력 CSV: `experiment/benchmark_oled_2025plus_top100_20260320.csv`

생성 명령:

```bash
python3 experiment/build_query_benchmark.py \
  --query OLED \
  --min-year 2025 \
  --top-k 100 \
  --benchmark-name benchmark_oled_2025plus_top100_20260320
```

명시 경로를 쓰고 싶으면:

```bash
python3 experiment/build_query_benchmark.py \
  --query OLED \
  --min-year 2025 \
  --top-k 100 \
  --output-csv experiment/benchmark_oled_2025plus_top100_20260320.csv
```

## 입력 통계

- 총 행 수: `100`
- publication_year: `2025`만 포함
- citation 범위:
  - 최대 `67`
  - 최소 `10`
- 출력 CSV: `experiment/benchmark_oled_2025plus_top100_20260320.csv`

상위 5건:

1. `10.1126/science.adt3011` (`67`, American Association for the Advancement of Science)
2. `10.3390/jcs9010042` (`67`, Multidisciplinary Digital Publishing Institute)
3. `10.1038/s41467-024-55680-2` (`49`, Nature Portfolio)
4. `10.1002/adom.202402653` (`41`, Wiley)
5. `10.1038/s41467-024-55564-5` (`36`, Nature Portfolio)

## 검증

생성된 CSV가 정말 `OLED`, `2025년 이후`, `citation top 100`인지 확인:

```bash
python3 - <<'PY'
import csv
from pathlib import Path
p = Path('experiment/benchmark_oled_2025plus_top100_20260320.csv')
with p.open(encoding='utf-8-sig', newline='') as f:
    rows = list(csv.DictReader(f))
print('rows=', len(rows))
years = sorted({int(r['publication_year']) for r in rows if str(r.get('publication_year') or '').strip()})
counts = [int(r.get('cited_by_count') or 0) for r in rows]
print('years=', years)
print('top_cited=', max(counts) if counts else 0)
print('bottom_cited=', min(counts) if counts else 0)
for r in rows[:10]:
    print(r['benchmark_rank'], r['doi'], r['publication_year'], r['cited_by_count'], r['publisher'])
PY
```

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
export RUN_NAME="oled_2025plus_top100_$(date +%Y%m%d_%H%M%S)"

bash scripts/run_linux_suite_bg.sh \
  --suite full \
  --run-name "$RUN_NAME" \
  --seed-profile "$SEED_PROFILE" \
  --profile-name "${PROFILE_NAME:-Default}" \
  --sample-csv /home/yongyong0206/paper_search/paper_download/experiment/benchmark_oled_2025plus_top100_20260320.csv \
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
- 추가로 `cited_by_count`, `publication_year`, `benchmark_min_year`, `benchmark_rank`, `benchmark_source` 등을 같이 넣어 provenance를 남긴다.
- `build_query_benchmark.py`는 기존 `--year` exact-year 동작도 그대로 유지한다.
