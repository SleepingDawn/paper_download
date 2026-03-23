#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: bash scripts/run_aip_headful_experiment_batch.sh [--date-tag <YYYYmmdd>] [--mixed-workers <n>] [--poll-seconds <n>] [--slurm-time <time>] [--max-wait-seconds <n>] [--wait-buffer-seconds <n>]

Submits the current Linux+Xvfb headful AIP verification runs through Slurm and waits for completion sequentially:
  1) AIP-only baseline (fresh-tab direct DOI default)
  2) AIP-only control (same-tab direct DOI)
  3) mixed-publisher benchmark recheck

Outputs:
  - per-run standard bundle: experiment/results/<run_name>_bundle.tar.gz
  - batch summary: experiment/results/<batch_prefix>_summary.txt
EOF
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=scripts/_linux_suite_env.sh
source "$REPO_ROOT/scripts/_linux_suite_env.sh"

DATE_TAG=$(date +%Y%m%d)
MIXED_WORKERS=3
POLL_SECONDS=20
SLURM_TIME_VALUE=$(linux_suite_slurm_time_limit)
SLURM_SIGNAL_VALUE=$(linux_suite_slurm_signal)
SLURM_PARTITION_VALUE=$(linux_suite_slurm_partition)
SLURM_ACCOUNT_VALUE=$(linux_suite_slurm_account)
SLURM_MEM_VALUE=$(linux_suite_slurm_mem)
SLURM_CPUS_VALUE=$(linux_suite_slurm_cpus)
WAIT_BUFFER_SECONDS=$(linux_suite_job_timeout_buffer_seconds)
MAX_WAIT_SECONDS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date-tag) DATE_TAG=${2:-}; shift 2 ;;
    --mixed-workers) MIXED_WORKERS=${2:-}; shift 2 ;;
    --poll-seconds) POLL_SECONDS=${2:-}; shift 2 ;;
    --slurm-time) SLURM_TIME_VALUE=${2:-}; shift 2 ;;
    --slurm-signal) SLURM_SIGNAL_VALUE=${2:-}; shift 2 ;;
    --slurm-partition) SLURM_PARTITION_VALUE=${2:-}; shift 2 ;;
    --slurm-account) SLURM_ACCOUNT_VALUE=${2:-}; shift 2 ;;
    --slurm-mem) SLURM_MEM_VALUE=${2:-}; shift 2 ;;
    --slurm-cpus) SLURM_CPUS_VALUE=${2:-}; shift 2 ;;
    --wait-buffer-seconds) WAIT_BUFFER_SECONDS=${2:-}; shift 2 ;;
    --max-wait-seconds) MAX_WAIT_SECONDS=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if ! [[ "$MIXED_WORKERS" =~ ^[0-9]+$ && "$POLL_SECONDS" =~ ^[0-9]+$ && "$WAIT_BUFFER_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "mixed-workers, poll-seconds, wait-buffer-seconds must be integers" >&2
  exit 1
fi
if [[ -n "$MAX_WAIT_SECONDS" && ! "$MAX_WAIT_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "max-wait-seconds must be an integer" >&2
  exit 1
fi
if [[ -n "$SLURM_CPUS_VALUE" && ! "$SLURM_CPUS_VALUE" =~ ^[0-9]+$ ]]; then
  echo "slurm-cpus must be an integer" >&2
  exit 1
fi
if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch not found; this batch launcher requires Slurm" >&2
  exit 1
fi
if ! command -v scancel >/dev/null 2>&1; then
  echo "scancel not found; this batch launcher requires Slurm" >&2
  exit 1
fi
if [[ -z "$MAX_WAIT_SECONDS" ]]; then
  BASE_WAIT_SECONDS=$(linux_suite_duration_to_seconds "$SLURM_TIME_VALUE") || BASE_WAIT_SECONDS=0
  if (( BASE_WAIT_SECONDS <= 0 )); then
    echo "unable to derive max wait from --slurm-time=$SLURM_TIME_VALUE" >&2
    exit 1
  fi
  MAX_WAIT_SECONDS=$((BASE_WAIT_SECONDS + WAIT_BUFFER_SECONDS))
fi

cd "$REPO_ROOT"

if [[ -f "$REPO_ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.venv/bin/activate"
fi

set -a
source "$REPO_ROOT/config/linux_server.env"
set +a

if [[ -z "${SEED_PROFILE:-}" || -z "${CHROME_PATH:-}" ]]; then
  echo "linux_server.env must define SEED_PROFILE and CHROME_PATH" >&2
  exit 1
fi

if [[ ! -x "$HOME/.local/bin/Xvfb" ]]; then
  echo "Xvfb binary not found: $HOME/.local/bin/Xvfb" >&2
  exit 1
fi

AIP_INPUT="$REPO_ROOT/outputs/benchmark_inputs/aip_focus_${DATE_TAG}.csv"
SOURCE_INPUT="$REPO_ROOT/outputs/benchmark_inputs/publisher_download_benchmark_elsevier_aip_ieee_spie_iop.csv"

python - <<PY
import csv
from pathlib import Path
src = Path(r"$SOURCE_INPUT")
dst = Path(r"$AIP_INPUT")
rows = list(csv.DictReader(src.open(encoding="utf-8", newline="")))
if not rows:
    raise SystemExit("source benchmark csv is empty")
fieldnames = list(rows[0].keys())
def is_aip_row(row):
    candidates = [
        row.get("scheduler_publisher"),
        row.get("benchmark_group"),
        row.get("source_publisher"),
        row.get("publisher"),
    ]
    normalized = [str(value or "").strip().lower() for value in candidates if str(value or "").strip()]
    for value in normalized:
        if value == "aip":
            return True
        if "american institute of physics" in value:
            return True
    return False

aip_rows = [row for row in rows if is_aip_row(row)]
if not aip_rows:
    raise SystemExit("no AIP rows found in source benchmark csv")
with dst.open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(aip_rows)
print(dst)
print(f"aip_rows={len(aip_rows)}")
for row in aip_rows:
    print(row.get("doi", ""))
PY

mkdir -p "$REPO_ROOT/experiment/results"

BATCH_PREFIX="aip_headful_batch_${DATE_TAG}_$(date +%H%M%S)"
SUMMARY_FILE="$REPO_ROOT/experiment/results/${BATCH_PREFIX}_summary.txt"
: > "$SUMMARY_FILE"
{
  echo "batch_prefix=$BATCH_PREFIX"
  echo "slurm_time=$SLURM_TIME_VALUE"
  echo "slurm_signal=$SLURM_SIGNAL_VALUE"
  echo "max_wait_seconds=$MAX_WAIT_SECONDS"
} >>"$SUMMARY_FILE"

run_and_wait() {
  local run_name=$1
  local sample_csv=$2
  local workers=$3
  local fresh_tab_mode=$4
  local launch_output
  local job_id
  local started_at
  local status_output
  local now
  local elapsed
  local launch_cmd=()

  echo "=== START ${run_name} ===" | tee -a "$SUMMARY_FILE"
  if [[ "$fresh_tab_mode" == "auto" ]]; then
    unset PDF_BROWSER_AIP_DIRECT_DOI_FRESH_TAB
    echo "fresh_tab_mode=auto" | tee -a "$SUMMARY_FILE"
  else
    export PDF_BROWSER_AIP_DIRECT_DOI_FRESH_TAB="$fresh_tab_mode"
    echo "fresh_tab_mode=$fresh_tab_mode" | tee -a "$SUMMARY_FILE"
  fi

  launch_cmd=(
    bash "$REPO_ROOT/scripts/run_linux_suite_bg.sh"
    --suite full
    --run-name "$run_name"
    --seed-profile "$SEED_PROFILE"
    --profile-name "${PROFILE_NAME:-Default}"
    --sample-csv "$sample_csv"
    --download-workers "$workers"
    --after-first-pass stop
    --runtime-preset linux_cli_seeded
    --execution-env linux_server
    --headless 0
    --chrome-path "$CHROME_PATH"
    --submit-mode slurm
    --slurm-time "$SLURM_TIME_VALUE"
    --slurm-signal "$SLURM_SIGNAL_VALUE"
    --xvfb 1
    --xvfb-bin "$HOME/.local/bin/Xvfb"
    --xvfb-display :99
  )
  if [[ -n "$SLURM_PARTITION_VALUE" ]]; then
    launch_cmd+=(--slurm-partition "$SLURM_PARTITION_VALUE")
  fi
  if [[ -n "$SLURM_ACCOUNT_VALUE" ]]; then
    launch_cmd+=(--slurm-account "$SLURM_ACCOUNT_VALUE")
  fi
  if [[ -n "$SLURM_MEM_VALUE" ]]; then
    launch_cmd+=(--slurm-mem "$SLURM_MEM_VALUE")
  fi
  if [[ -n "$SLURM_CPUS_VALUE" ]]; then
    launch_cmd+=(--slurm-cpus "$SLURM_CPUS_VALUE")
  fi
  launch_output=$("${launch_cmd[@]}")
  printf '%s\n' "$launch_output" | tee -a "$SUMMARY_FILE"
  job_id=$(printf '%s\n' "$launch_output" | awk -F= '/^job_id=/{print $2; exit}')
  if [[ -z "$job_id" ]]; then
    echo "failed_to_extract_job_id_for_run=$run_name" | tee -a "$SUMMARY_FILE"
    return 1
  fi
  echo "submitted_job_id=$job_id" | tee -a "$SUMMARY_FILE"
  started_at=$(date +%s)

  while true; do
    status_output=$(bash "$REPO_ROOT/scripts/check_linux_suite_status.sh" "$run_name" 20)
    printf '%s\n' "$status_output" >> "$SUMMARY_FILE"
    if printf '%s\n' "$status_output" | grep -q 'process_alive=false'; then
      break
    fi
    now=$(date +%s)
    elapsed=$((now - started_at))
    if (( elapsed >= MAX_WAIT_SECONDS )); then
      echo "max_wait_exceeded_seconds=$MAX_WAIT_SECONDS run_name=$run_name job_id=$job_id" | tee -a "$SUMMARY_FILE"
      scancel "$job_id" || true
      sleep 5
      status_output=$(bash "$REPO_ROOT/scripts/check_linux_suite_status.sh" "$run_name" 20)
      printf '%s\n' "$status_output" >> "$SUMMARY_FILE"
      echo "job_cancelled_due_to_timeout=$job_id" | tee -a "$SUMMARY_FILE"
      return 1
    fi
    sleep "$POLL_SECONDS"
  done

  bash "$REPO_ROOT/scripts/collect_linux_suite_artifacts.sh" \
    "$run_name" \
    --include-pdfs 1 \
    --output "$REPO_ROOT/experiment/results/${run_name}_bundle.tar.gz" | tee -a "$SUMMARY_FILE"

  python - <<PY | tee -a "$SUMMARY_FILE"
import csv, json
from pathlib import Path
run_name = r"$run_name"
run_dir = Path(r"$REPO_ROOT/outputs") / run_name
csv_path = run_dir / "download" / "run" / "openalex_search_results_parallel.csv"
summary_path = run_dir / "download" / "run" / "summary.json"
print(f"[run-summary] {run_name}")
if summary_path.exists():
    obj = json.loads(summary_path.read_text(encoding="utf-8"))
    print(json.dumps({
        "success_count": obj.get("success_count"),
        "failure_count": obj.get("failure_count"),
        "mode": obj.get("mode"),
    }, ensure_ascii=False))
if csv_path.exists():
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8", newline="")))
    for row in rows:
        print(json.dumps({
            "doi": row.get("doi"),
            "publisher": row.get("publisher"),
            "result": row.get("result"),
            "source": row.get("source"),
            "landing_state": row.get("landing_state"),
            "landing_entry_navigation_route": row.get("landing_entry_navigation_route"),
            "landing_page_disconnect_observed": row.get("landing_page_disconnect_observed"),
            "landing_page_disconnect_stage": row.get("landing_page_disconnect_stage"),
        }, ensure_ascii=False))
PY

  echo "=== END ${run_name} ===" | tee -a "$SUMMARY_FILE"
  echo | tee -a "$SUMMARY_FILE"
}

RUN_A="aip_fresh_tab_baseline_${DATE_TAG}_$(date +%H%M%S)"
run_and_wait "$RUN_A" "$AIP_INPUT" 1 "auto"

RUN_B="aip_same_tab_control_${DATE_TAG}_$(date +%H%M%S)"
run_and_wait "$RUN_B" "$AIP_INPUT" 1 "0"

unset PDF_BROWSER_AIP_DIRECT_DOI_FRESH_TAB
RUN_C="publisher_benchmark_recheck_${DATE_TAG}_$(date +%H%M%S)"
run_and_wait "$RUN_C" "$SOURCE_INPUT" "$MIXED_WORKERS" "auto"

echo "batch_summary=$SUMMARY_FILE"
