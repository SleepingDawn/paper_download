# Paper Download Pipeline

DOI/benchmark CSV를 입력으로 받아, Linux 서버에서 브라우저 랜딩 검증과 PDF 다운로드를 **하나의 통합 흐름**으로 실행하는 파이프라인입니다.

현재 기준 운영 모델은 명확합니다.

- **Linux 서버**
- **Xvfb 위의 headful Chrome/Chromium**
- **seeded Linux Chrome profile**
- **unified landing+download flow**

이 README는 현재 branch의 실제 Linux 서버 사용 경로만 설명합니다.

## 현재 구조

- `parallel_download.py`
  - 실제 다운로드 엔진입니다.
  - DOI 랜딩, challenge/block 판정, publisher-specific 처리, generic PDF 취득, 결과 기록을 모두 여기서 수행합니다.
- `experiment/run_linux_headless_suite.py`
  - benchmark CSV를 받아 실험 run 디렉토리를 만들고, `parallel_download.py` 실행과 요약 산출물을 연결합니다.
- `scripts/run_linux_suite_bg.sh`
  - Linux 서버에서 `nohup` 기반 background run을 시작하는 표준 launcher입니다.
- `scripts/check_linux_suite_status.sh`
  - run 상태와 최근 로그를 확인합니다.
- `scripts/tail_linux_suite_logs.sh`
  - root/stage 로그를 실시간으로 봅니다.
- `scripts/collect_linux_suite_artifacts.sh`
  - 결과를 bundle(`.tar.gz`)로 수집합니다.
- `experiment/build_query_benchmark.py`
  - OpenAlex `title_and_abstract.search` 기준 benchmark CSV를 재현 가능하게 생성합니다.

관련 문서:

- `docs/linux_seed_profile_setup.md`
- `docs/xvfb_local_build_guide.md`
- `docs/linux_headless_experiment_journal.md`
- `experiment/README.md`

## Linux 서버 초기 설정

### 1. Python 환경

```bash
cd /path/to/paper_download
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### 2. Chrome/Chromium 준비

서버에서 실행 가능한 브라우저 binary가 필요합니다.

예시:

```bash
command -v google-chrome
command -v google-chrome-stable
command -v chromium
command -v chromium-browser
```

이 repo의 Linux 실험은 보통 아래처럼 명시 경로를 사용합니다.

```text
$HOME/chrome-linux/chrome-linux64/chrome
```

### 3. Xvfb 준비

기본 실행 모델은 **headless가 아니라 Xvfb 위의 headful 브라우저**입니다.

- 시스템에 `Xvfb`가 있으면 그대로 사용
- 없고 `sudo`도 없으면 `docs/xvfb_local_build_guide.md` 기준으로 `~/.local/bin/Xvfb`를 빌드

최소 확인:

```bash
command -v Xvfb
command -v xauth
command -v xkbcomp
```

현재 launcher는 보통 아래 경로를 기대합니다.

```text
$HOME/.local/bin/Xvfb
```

### 4. Linux seeded profile 준비

`linux_cli_seeded` preset은 **Linux에서 생성한 Chrome user-data-dir root**를 기대합니다.

기대 구조:

```text
linux_chrome_user_data_seed/
  Local State
  Default/
    Preferences
    Network/Cookies
    Local Storage/
    IndexedDB/
```

seed profile 준비/검증은 `docs/linux_seed_profile_setup.md`를 따릅니다.

검증 예시:

```bash
python3 scripts/check_linux_seed_profile.py \
  --profile-root /absolute/path/to/linux_chrome_user_data_seed \
  --profile-name Default
```

### 5. API key / mailto 로컬 비밀값 설정

API key와 mailto는 tracked Python/config 파일에 직접 넣지 말고, repo root의 `.env`에서 관리합니다.

```bash
cd /home/yongyong0206/paper_search/paper_download
cp .env.example .env
```

`.env` 예시:

```dotenv
WILEY_API_KEY=...
OPENALEX_MAILTO=your_email@example.com
```

동작 방식:

- [config.py](/Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download/config.py)와 [openalex_search.py](/Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download/openalex_search.py)는 시작 시 repo root의 `.env`를 자동 로드합니다.
- 실제 `.env`는 [`.gitignore`](/Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download/.gitignore)에 의해 추적되지 않습니다.
- 커밋 대상은 [`.env.example`](/Users/seyong/Desktop/SNU/26W_MDIL_Intern/paper_search/paper_download/.env.example)만 유지합니다.

### 6. 서버 전용 env 파일 생성

`config/linux_server.env`는 커밋 대상이 아니라, 서버에서 생성하는 machine-local 설정 파일입니다.

예제 템플릿:

```text
config/linux_server.env.example
```

실제 생성은 아래 스크립트를 사용합니다.

```bash
bash scripts/prepare_linux_server_env.sh \
  --seed-profile /absolute/path/to/linux_chrome_user_data_seed \
  --chrome-path /absolute/path/to/chrome \
  --python /absolute/path/to/python3
```

이 스크립트는 다음 값을 `config/linux_server.env`에 기록합니다.

- `SEED_PROFILE`
- `PROFILE_NAME`
- `CHROME_PATH`
- `PYTHON_BIN`
- `RUNS_ROOT`
- `LOGS_ROOT`
- `COLLECT_ROOT`
- 선택 override:
  - `PDF_BROWSER_NO_SANDBOX`
  - `TMPDIR`
  - `XDG_RUNTIME_DIR`

사용 전:

```bash
source config/linux_server.env
```

### 6. `--seed-profile`, `--chrome-path`, `--python` 값을 찾는 방법

현재 repo의 코드와 실험 문서 기준으로 가장 자주 쓰는 Linux 서버 값은 다음입니다.

- `--seed-profile`
  - `/home/yongyong0206/paper_search/paper_download/outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed`
- `--chrome-path`
  - `/home/yongyong0206/chrome-linux/chrome-linux64/chrome`
- `--python`
  - `/home/yongyong0206/paper_search/paper_download/.venv/bin/python3`

또한 이 저장소에는 **공유 가능한 Linux seed profile archive**가 하나 포함되어 있습니다.

- `docs/linux_chromium_user_data_seed.tar.gz`

이 파일은 `scripts/prepare_linux_server_env.sh`가 seed profile directory가 없을 때 자동으로 풀어 쓰는 공식 입력 경로입니다.

이 값들은 다음 근거에서 확인됩니다.

- `scripts/prepare_linux_server_env.sh` 기본값
  - seed profile default: `outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed`
  - chrome default: `$HOME/chrome-linux/chrome-linux64/chrome`
  - python default: `command -v python3`
- `experiment/benchmark_oled_2025plus_top100_20260320.md`와 최근 Linux 실험 문서의 실제 실행 예시

서버에서 직접 확인하는 명령:

```bash
cd /home/yongyong0206/paper_search/paper_download

python3 - <<'PY'
from pathlib import Path
print(Path('outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed').resolve())
PY

test -d /home/yongyong0206/paper_search/paper_download/outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed && echo "seed-profile: ok"
test -x /home/yongyong0206/chrome-linux/chrome-linux64/chrome && echo "chrome-path: ok"
test -x /home/yongyong0206/paper_search/paper_download/.venv/bin/python3 && echo "python: ok"
```

대체 경로를 찾고 싶으면:

```bash
command -v python3
command -v google-chrome
command -v google-chrome-stable
command -v chromium
command -v chromium-browser
find "$HOME" -maxdepth 4 -type f -name chrome 2>/dev/null | sed -n '1,20p'
find outputs -maxdepth 3 -type d -name 'linux_chromium_user_data_seed' 2>/dev/null
```

가장 안전한 방법은 직접 추측하지 말고 아래를 실행해 env 파일을 생성한 뒤 그 값을 쓰는 것입니다.

```bash
bash scripts/prepare_linux_server_env.sh \
  --seed-profile /home/yongyong0206/paper_search/paper_download/outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed \
  --chrome-path /home/yongyong0206/chrome-linux/chrome-linux64/chrome \
  --python /home/yongyong0206/paper_search/paper_download/.venv/bin/python3

source config/linux_server.env
printf 'SEED_PROFILE=%s\nCHROME_PATH=%s\nPYTHON_BIN=%s\n' "$SEED_PROFILE" "$CHROME_PATH" "$PYTHON_BIN"
```

`docs/linux_chromium_user_data_seed.tar.gz`를 직접 써서 seed profile을 준비하고 싶으면:

```bash
cd /home/yongyong0206/paper_search/paper_download

mkdir -p outputs/linux_seed_profile_from_docs
tar -xzf docs/linux_chromium_user_data_seed.tar.gz -C outputs/linux_seed_profile_from_docs

test -d outputs/linux_seed_profile_from_docs/linux_chromium_user_data_seed && echo "seed-profile extracted"
```

이후에는 같은 경로를 `--seed-profile` 또는 `SEED_PROFILE`로 사용하면 됩니다.

## 기본 런타임 가정

현재 Linux 서버 기본값은 다음 조합입니다.

- `runtime-preset=linux_cli_seeded`
- `execution-env=linux_server`
- `headless=0`
- `xvfb=1`

즉, **Linux + Xvfb + headful Chrome**이 기본 경로입니다.

추가 메모:

- `linux_server`에서는 rootless Chrome 안정성을 위해 `--no-sandbox` 경로가 기본적으로 중요합니다.
- launcher/root log에는 `headless=0`, `xvfb_enabled=1`, `xvfb_display=:99` 같은 값이 남습니다.
- unified landing+download flow를 유지하기 때문에, 별도 landing-only 선검사는 현재 권장 경로가 아닙니다.

## 입력 준비

### 1. benchmark CSV 생성

재현 가능한 benchmark 생성은 `experiment/build_query_benchmark.py`를 사용합니다.

주요 인자:

- `--query`
- `--year`
- `--min-year`
- `--max-year`
- `--limit` / `--top-k`
- `--sort`
- `--citation-percentile-min`
- `--benchmark-name`
- `--output-csv`

동작 규칙:

- `--query`는 필수입니다.
- `--year`는 exact year 필터입니다.
- `--min-year`/`--max-year`는 year range 필터입니다.
- `--year`와 `--min-year`/`--max-year`는 함께 쓸 수 없습니다.
- `--top-k`는 `--limit`의 alias이고, 주어지면 `--limit`보다 우선합니다.
- `--sort` 기본값은 `cited_by_count:desc`입니다.
- `--citation-percentile-min`을 주면 OpenAlex의 `citation_normalized_percentile` 값이 threshold 이상인 논문을 먼저 배치하고, 그 안에서 `cited_by_count`로 정렬한 뒤 나머지를 overall citation 순으로 채웁니다.
- `--output-csv`를 생략하면 `--benchmark-name`이 필요하고, 출력은 `experiment/<benchmark-name>.csv`가 됩니다.
- `--limit` 기본값은 `200`입니다.

예시: `OLED`, `publication_year>=2025`, citation top 100

```bash
python3 experiment/build_query_benchmark.py \
  --query OLED \
  --min-year 2025 \
  --top-k 100 \
  --benchmark-name benchmark_oled_2025plus_top100_20260320
```

예시: `OLED`, `publication_year>=2025`, 상위 1%(`0.99`) yearly/subfield citation percentile 우선 + 최종 top 100

```bash
python3 experiment/build_query_benchmark.py \
  --query OLED \
  --min-year 2025 \
  --top-k 100 \
  --citation-percentile-min 0.99 \
  --benchmark-name benchmark_oled_2025plus_top100_p99_20260320
```

생성 결과:

```text
experiment/benchmark_oled_2025plus_top100_20260320.csv
```

검증 예시:

```bash
python3 - <<'PY'
import csv
from pathlib import Path
p = Path('experiment/benchmark_oled_2025plus_top100_20260320.csv')
with p.open(encoding='utf-8-sig', newline='') as f:
    rows = list(csv.DictReader(f))
print('rows=', len(rows))
print('years=', sorted({int(r['publication_year']) for r in rows if str(r.get('publication_year') or '').strip()}))
print('top_cited=', max(int(r.get('cited_by_count') or 0) for r in rows))
print('bottom_cited=', min(int(r.get('cited_by_count') or 0) for r in rows))
PY
```

### 2. 기존 benchmark 사용

현재 repo에는 다음 benchmark가 이미 있습니다.

- `experiment/benchmark_oled_2025plus_top100_20260320.csv`
- `experiment/benchmark_a_igzo_2025_top200_20260320.csv`
- `experiment/benchmark_random100_seed20260311.csv`

### 3. 다운로드 입력 CSV 형식

download/experiment 흐름은 CSV가 아래 선두 컬럼을 가지는 것을 전제로 가장 잘 동작합니다.

권장 컬럼:

- `doi`
- `publisher`
- `pdf_url`
- `open_access`
- `title`

추가 provenance 컬럼은 그대로 유지해도 됩니다.

예시:

```csv
doi,publisher,pdf_url,open_access,title
10.1016/j.scitotenv.2024.172816,Elsevier,,False,Example title
10.1038/s41467-023-41868-5,Nature,https://www.nature.com/articles/s41467-023-41868-5.pdf,True,Example title
```

## 표준 실험 실행 경로

권장 entrypoint는 `scripts/run_linux_suite_bg.sh`입니다.

### 1. 서버 준비

```bash
cd /path/to/paper_download
source .venv/bin/activate

bash scripts/prepare_linux_server_env.sh \
  --seed-profile /absolute/path/to/linux_chrome_user_data_seed \
  --chrome-path /absolute/path/to/chrome \
  --python /absolute/path/to/python3

source config/linux_server.env
```

### 2. full run 시작

예시:

```bash
cd /path/to/paper_download
export RUN_NAME="oled_2025plus_top100_$(date +%Y%m%d_%H%M%S)"

bash scripts/run_linux_suite_bg.sh \
  --suite full \
  --run-name "$RUN_NAME" \
  --seed-profile "$SEED_PROFILE" \
  --profile-name "${PROFILE_NAME:-Default}" \
  --sample-csv /path/to/paper_download/experiment/benchmark_oled_2025plus_top100_20260320.csv \
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

`run_linux_suite_bg.sh`는 내부에서:

- `experiment/run_linux_headless_suite.py --execute`
- `scripts/with_xvfb.sh`
- `parallel_download.py`

를 연결합니다.

### 3. `run_linux_headless_suite.py` 직접 실행

이 entrypoint는 아래 인자를 받습니다.

- `--suite {pilot,full}`
- `--sample-csv`
- `--run-dir`
- `--execute`
- `--runtime-preset`
- `--execution-env`
- `--headless`
- `--profile-name`
- `--persistent-profile-dir`
- `--download-workers`
- `--after-first-pass`

직접 실행도 가능하지만, 서버에서는 background launcher를 권장합니다.

## entrypoint 인자 설명

기준 entrypoint는 `experiment/run_linux_headless_suite.py`입니다. 현재 parser 기본값은 다음과 같습니다.

- `--suite {pilot,full}`
  - 기본값: `pilot`
  - `pilot_sample.csv` 또는 `full_sample.csv`를 고를 때 사용합니다.
  - `--sample-csv`를 따로 주면 그 CSV를 우선 사용합니다.
- `--sample-csv`
  - 기본값: 없음
  - 지정하지 않으면 `suite_dir/<suite>_sample.csv`를 사용합니다.
  - benchmark CSV를 직접 넣고 싶을 때 가장 자주 쓰는 인자입니다.
- `--run-dir`
  - 기본값: `outputs/<suite>_<timestamp>`
  - run 디렉토리를 명시 고정하고 싶을 때 사용합니다.
- `--execute`
  - 기본값: 꺼짐
  - 없으면 run 디렉토리, shell script, manifest만 준비하고 실제 다운로드는 실행하지 않습니다.
  - 실제 서버 실행에는 반드시 넣어야 합니다.
- `--runtime-preset`
  - 기본값: `linux_cli_seeded`
  - 현재 Linux 서버 권장값도 `linux_cli_seeded`입니다.
  - choice에는 `auto`, `local_mac`, `linux_cli_seeded`가 있지만, 현재 README 기준 운영값은 `linux_cli_seeded`입니다.
- `--execution-env`
  - 기본값: `linux_server`
  - 현재 Linux 서버 권장값도 `linux_server`입니다.
- `--headless`
  - 기본값: `0`
  - 현재 Linux 서버 기본 모델은 Xvfb 위 headful이므로 `0`이 권장값이자 parser 기본값입니다.
- `--profile-name`
  - 기본값: 환경변수 `PDF_BROWSER_PROFILE_NAME` 또는 `Default`
  - seeded profile 안의 Chrome profile 이름을 가리킵니다.
- `--persistent-profile-dir`
  - 기본값: 환경변수 `PDF_BROWSER_PERSISTENT_PROFILE_DIR`가 있으면 그것, 없으면 `None`
  - `linux_cli_seeded`로 실제 실행할 때는 사실상 필요합니다.
  - 없으면 seed profile check가 실패하고 실행 manifest에 실패 이유가 기록됩니다.
- `--download-workers`
  - 기본값: `1`
  - `parallel_download.py --max_workers`로 전달됩니다.
  - 최근 Linux 실험 문서에서는 `3`을 자주 사용했습니다.
- `--after-first-pass`
  - 기본값: `stop`
  - `stop`: 1차 패스 후 종료
  - `deep`: 실패 케이스를 심화 재시도

직접 실행 예시:

```bash
cd /home/yongyong0206/paper_search/paper_download
source .venv/bin/activate
source config/linux_server.env

python3 experiment/run_linux_headless_suite.py \
  --suite full \
  --sample-csv /home/yongyong0206/paper_search/paper_download/experiment/benchmark_oled_2025plus_top100_20260320.csv \
  --run-dir /home/yongyong0206/paper_search/paper_download/outputs/oled_manual_direct \
  --execute \
  --runtime-preset linux_cli_seeded \
  --execution-env linux_server \
  --headless 0 \
  --profile-name "${PROFILE_NAME:-Default}" \
  --persistent-profile-dir "$SEED_PROFILE" \
  --download-workers 3 \
  --after-first-pass stop
```

## 진행 확인

### 상태 확인

```bash
bash scripts/check_linux_suite_status.sh "$RUN_NAME" 40
```

확인 항목:

- `run_dir`
- `pid_file`
- `process_alive`
- `execution_manifest`
- stage outputs
- root log tail
- download/landing/summarize log tail

### 실시간 로그 tail

```bash
bash scripts/tail_linux_suite_logs.sh "$RUN_NAME" all
```

stage 선택:

- `all`
- `root`
- `landing`
- `download`
- `summarize`

## 종료 확인과 중단

### 종료 확인

```bash
bash scripts/check_linux_suite_status.sh "$RUN_NAME" 60
```

로그 파일 증가 여부까지 보고 싶으면:

```bash
stat "outputs/${RUN_NAME}/logs/download.stderr.log"
sleep 5
stat "outputs/${RUN_NAME}/logs/download.stderr.log"
```

### 실행 중인 run 중단

현재 branch에서는 `logs/<run>.pid`의 wrapper PID에 `TERM`을 보내면 child process group까지 같이 내려가도록 맞춰져 있습니다.

```bash
kill -TERM "$(cat "logs/${RUN_NAME}.pid")"
sleep 3
bash scripts/check_linux_suite_status.sh "$RUN_NAME" 20
```

정상 종료가 안 되면:

```bash
kill -KILL "$(cat "logs/${RUN_NAME}.pid")"
```

## 결과 수집과 bundle 생성

실험 종료 후 bundle 수집:

```bash
mkdir -p experiment/results

bash scripts/collect_linux_suite_artifacts.sh \
  "$RUN_NAME" \
  --include-pdfs 1 \
  --output "/path/to/paper_download/experiment/results/${RUN_NAME}_bundle.tar.gz"
```

수집 대상:

- root launcher log
- run directory
- `download/run`
- `summary`
- 선택적으로 `download/pdfs`

## 결과 확인 위치

기본 run 산출물:

```text
outputs/<run-name>/
```

주요 파일:

- `download/run/openalex_search_results_parallel.csv`
- `download/run/failed_papers.csv`
- `download/run/download_attempts.jsonl`
- `download/run/summary.json`
- `summary/suite_summary.json`
- `logs/download.stderr.log`
- `metadata/`

bundle 수집 후:

```text
experiment/results/<run-name>_bundle.tar.gz
```

## 직접 다운로드 엔진 실행

권장 경로는 suite runner지만, 필요하면 엔진을 직접 호출할 수 있습니다.

CLI:

```bash
python3 parallel_download.py --help
```

대표 인자:

- `--doi_path`
- `--query`
- `--output_dir`
- `--pdf_output_dir`
- `--max_workers`
- `--after-first-pass`
- `--runtime-preset`
- `--execution-env`
- `--headless`
- `--profile-mode`
- `--persistent-profile-dir`
- `--profile-name`

Linux 서버 기준 direct invocation 예시:

```bash
python3 -u parallel_download.py \
  --doi_path experiment/benchmark_oled_2025plus_top100_20260320.csv \
  --output_dir outputs/manual_oled_run \
  --max_workers 1 \
  --after-first-pass stop \
  --runtime-preset linux_cli_seeded \
  --execution-env linux_server \
  --headless 0 \
  --persistent-profile-dir "$SEED_PROFILE" \
  --profile-name "${PROFILE_NAME:-Default}" \
  --non-interactive
```

다만 서버 운영은 `run_linux_suite_bg.sh` 경로가 더 안전합니다.

## 중요 troubleshooting

### 1. `Xvfb`가 없음

- `command -v Xvfb`가 비면 `docs/xvfb_local_build_guide.md`를 따릅니다.
- 이 repo는 `~/.local/bin/Xvfb` 경로를 자주 사용합니다.

### 2. `seed profile directory not found`

- `scripts/prepare_linux_server_env.sh`는 seed profile이 없으면 docs tar를 자동 추출하려 시도합니다.
- 기본 archive 경로는 `docs/linux_chromium_user_data_seed.tar.gz`입니다.
- 그래도 실패하면 `docs/linux_seed_profile_setup.md` 기준으로 seed를 다시 준비해야 합니다.
- `scripts/check_linux_seed_profile.py`로 구조를 먼저 확인하세요.

### 3. `chrome executable not executable`

- `CHROME_PATH`가 실제 binary를 가리키는지 확인합니다.
- `prepare_linux_server_env.sh`는 binary 실행 가능 여부를 먼저 검사합니다.

### 4. 서버에서 headful이 안 뜸

- Linux 기본 경로는 headless fallback이 아니라 Xvfb headful입니다.
- root log에 `headless=0`, `xvfb_enabled=1`, `xvfb_display=:99`가 남는지 확인하세요.

### 5. `no-sandbox` 관련 오류

- current Linux server path는 rootless Chrome 기준입니다.
- `linux_server`에서는 `--no-sandbox` 계열이 중요한 전제입니다.
- 필요하면 `config/linux_server.env`에서 `PDF_BROWSER_NO_SANDBOX` 값을 점검하세요.

### 6. 이전 run이 남아서 launcher가 막힘

- `logs/<run>.pid`가 살아 있으면 새 run을 막습니다.
- 상태 확인:

```bash
bash scripts/check_linux_suite_status.sh "$RUN_NAME"
```

- 종료:

```bash
kill -TERM "$(cat "logs/${RUN_NAME}.pid")"
```

### 7. benchmark/query 생성 결과를 다시 확인하고 싶음

- 생성한 CSV 자체를 열어 `publication_year`, `cited_by_count`, `benchmark_rank`를 확인하면 됩니다.
- 관련 benchmark 입력 예시는 `experiment/` 아래 CSV와 짝이 되는 `.md` 파일들에 남아 있습니다.
