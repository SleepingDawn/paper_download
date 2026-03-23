#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: bash scripts/run_linux_suite_bg.sh --suite {pilot|full} [options]

options:
  --suite <pilot|full>          experiment suite
  --seed-profile <dir>          Linux seeded Chrome user-data-dir root (or config/linux_server.env)
  --profile-name <name>         Chrome profile name (default: Default or config/linux_server.env)
  --run-name <name>             run/log prefix (default: <suite>_YYYYmmdd_HHMMSS)
  --run-dir <dir>               explicit run directory (default: outputs/<run-name>)
  --sample-csv <path>           override suite CSV
  --landing-workers <n>         landing workers (default: 2)
  --download-workers <n>        download workers (default: 1)
  --after-first-pass <mode>     stop|deep (default: stop)
  --headless <0|1>              default: 0
  --python <path>               python executable (default: config/linux_server.env or current python3)
  --runtime-preset <value>      default: linux_cli_seeded
  --execution-env <value>       default: linux_server
  --chrome-path <path>          optional explicit browser binary (or config/linux_server.env)
  --no-sandbox <0|1>            export PDF_BROWSER_NO_SANDBOX (default: preserve current env/config)
  --xvfb <auto|0|1>             auto-start Xvfb for headful linux_server runs (default: auto)
  --xvfb-display <value>        Xvfb display (default: :99 or env PDF_BROWSER_XVFB_DISPLAY)
  --xvfb-bin <path>             Xvfb binary (default: ~/.local/bin/Xvfb or env PDF_BROWSER_XVFB_BIN)
  --xvfb-screen <WxHxD>         Xvfb screen geometry (default: 1280x1024x24)
  --submit-mode <auto|slurm|local>
                                default: slurm outside Slurm jobs, local inside Slurm jobs
  --slurm-time <time>           Slurm walltime limit (default: 12:00:00 or env LINUX_SUITE_SLURM_TIME_LIMIT)
  --slurm-signal <signal@secs>  pre-timeout signal (default: TERM@120)
  --slurm-partition <name>      optional Slurm partition
  --slurm-account <name>        optional Slurm account
  --slurm-mem <value>           optional Slurm --mem value
  --slurm-cpus <n>              optional Slurm cpus-per-task (default: max(download,landing,1)+1)
EOF
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=scripts/_linux_suite_env.sh
source "$SCRIPT_DIR/_linux_suite_env.sh"
REPO_ROOT=$(linux_suite_repo_root)
linux_suite_load_env "$REPO_ROOT"
RUNS_ROOT=$(linux_suite_runs_root "$REPO_ROOT")
LOGS_ROOT=$(linux_suite_logs_root "$REPO_ROOT")
ENV_FILE=$(linux_suite_env_file "$REPO_ROOT")

SUITE=""
SEED_PROFILE="${SEED_PROFILE:-}"
PROFILE_NAME="${PROFILE_NAME:-Default}"
RUN_NAME=""
RUN_DIR=""
SAMPLE_CSV=""
LANDING_WORKERS=2
DOWNLOAD_WORKERS=1
AFTER_FIRST_PASS="stop"
HEADLESS=0
RUNTIME_PRESET="linux_cli_seeded"
EXECUTION_ENV="linux_server"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"
CHROME_PATH_VALUE="${CHROME_PATH:-}"
NO_SANDBOX_VALUE="${PDF_BROWSER_NO_SANDBOX:-}"
XVFB_MODE="${PDF_BROWSER_XVFB_MODE:-auto}"
XVFB_DISPLAY_VALUE="${PDF_BROWSER_XVFB_DISPLAY:-:99}"
XVFB_BIN_VALUE="${PDF_BROWSER_XVFB_BIN:-$HOME/.local/bin/Xvfb}"
XVFB_SCREEN_VALUE="${PDF_BROWSER_XVFB_SCREEN:-1280x1024x24}"
SUBMIT_MODE="auto"
SLURM_TIME_VALUE="$(linux_suite_slurm_time_limit)"
SLURM_SIGNAL_VALUE="$(linux_suite_slurm_signal)"
SLURM_PARTITION_VALUE="$(linux_suite_slurm_partition)"
SLURM_ACCOUNT_VALUE="$(linux_suite_slurm_account)"
SLURM_MEM_VALUE="$(linux_suite_slurm_mem)"
SLURM_CPUS_VALUE="$(linux_suite_slurm_cpus)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --suite) SUITE=${2:-}; shift 2 ;;
    --seed-profile) SEED_PROFILE=${2:-}; shift 2 ;;
    --profile-name) PROFILE_NAME=${2:-}; shift 2 ;;
    --run-name) RUN_NAME=${2:-}; shift 2 ;;
    --run-dir) RUN_DIR=${2:-}; shift 2 ;;
    --sample-csv) SAMPLE_CSV=${2:-}; shift 2 ;;
    --landing-workers) LANDING_WORKERS=${2:-}; shift 2 ;;
    --download-workers) DOWNLOAD_WORKERS=${2:-}; shift 2 ;;
    --after-first-pass) AFTER_FIRST_PASS=${2:-}; shift 2 ;;
    --headless) HEADLESS=${2:-}; shift 2 ;;
    --python) PYTHON_BIN=${2:-}; shift 2 ;;
    --runtime-preset) RUNTIME_PRESET=${2:-}; shift 2 ;;
    --execution-env) EXECUTION_ENV=${2:-}; shift 2 ;;
    --chrome-path) CHROME_PATH_VALUE=${2:-}; shift 2 ;;
    --no-sandbox) NO_SANDBOX_VALUE=${2:-}; shift 2 ;;
    --xvfb) XVFB_MODE=${2:-}; shift 2 ;;
    --xvfb-display) XVFB_DISPLAY_VALUE=${2:-}; shift 2 ;;
    --xvfb-bin) XVFB_BIN_VALUE=${2:-}; shift 2 ;;
    --xvfb-screen) XVFB_SCREEN_VALUE=${2:-}; shift 2 ;;
    --submit-mode) SUBMIT_MODE=${2:-}; shift 2 ;;
    --slurm-time) SLURM_TIME_VALUE=${2:-}; shift 2 ;;
    --slurm-signal) SLURM_SIGNAL_VALUE=${2:-}; shift 2 ;;
    --slurm-partition) SLURM_PARTITION_VALUE=${2:-}; shift 2 ;;
    --slurm-account) SLURM_ACCOUNT_VALUE=${2:-}; shift 2 ;;
    --slurm-mem) SLURM_MEM_VALUE=${2:-}; shift 2 ;;
    --slurm-cpus) SLURM_CPUS_VALUE=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ "$SUBMIT_MODE" == "auto" ]]; then
  SUBMIT_MODE=$(linux_suite_default_submit_mode)
fi
if [[ "$SUBMIT_MODE" != "slurm" && "$SUBMIT_MODE" != "local" ]]; then
  echo "--submit-mode must be auto, slurm, or local" >&2
  exit 1
fi

if [[ -z "$SUITE" ]]; then
  echo "--suite is required" >&2
  exit 1
fi
if [[ "$SUITE" != "pilot" && "$SUITE" != "full" ]]; then
  echo "--suite must be pilot or full" >&2
  exit 1
fi
if [[ -z "$SEED_PROFILE" ]]; then
  echo "--seed-profile is required" >&2
  exit 1
fi
if [[ -z "$PYTHON_BIN" ]]; then
  echo "python executable not found" >&2
  exit 1
fi
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python executable not executable: $PYTHON_BIN" >&2
  exit 1
fi
if [[ ! -d "$SEED_PROFILE" ]]; then
  echo "seed profile directory not found: $SEED_PROFILE" >&2
  exit 1
fi
if [[ -z "$RUN_NAME" ]]; then
  RUN_NAME="${SUITE}_$(date +%Y%m%d_%H%M%S)"
fi
if [[ -z "$RUN_DIR" ]]; then
  RUN_DIR="$RUNS_ROOT/$RUN_NAME"
fi
if [[ -z "$SLURM_CPUS_VALUE" ]]; then
  if [[ "$LANDING_WORKERS" =~ ^[0-9]+$ && "$DOWNLOAD_WORKERS" =~ ^[0-9]+$ ]]; then
    if (( DOWNLOAD_WORKERS > LANDING_WORKERS )); then
      SLURM_CPUS_VALUE=$((DOWNLOAD_WORKERS + 1))
    else
      SLURM_CPUS_VALUE=$((LANDING_WORKERS + 1))
    fi
  else
    SLURM_CPUS_VALUE=2
  fi
fi
if [[ -n "$SLURM_CPUS_VALUE" && ! "$SLURM_CPUS_VALUE" =~ ^[0-9]+$ ]]; then
  echo "--slurm-cpus must be an integer" >&2
  exit 1
fi
if [[ "$SLURM_CPUS_VALUE" =~ ^[0-9]+$ ]] && (( SLURM_CPUS_VALUE < 1 )); then
  echo "--slurm-cpus must be >= 1" >&2
  exit 1
fi
if [[ "$SUBMIT_MODE" == "slurm" ]]; then
  if [[ -z "$SLURM_TIME_VALUE" ]]; then
    echo "--slurm-time is required in slurm mode" >&2
    exit 1
  fi
  if ! command -v sbatch >/dev/null 2>&1; then
    echo "sbatch not found; Slurm submit mode requires sbatch" >&2
    exit 1
  fi
fi

USE_XVFB=0
if [[ "$HEADLESS" == "0" && "$EXECUTION_ENV" == "linux_server" ]]; then
  case "$XVFB_MODE" in
    auto|1|true|yes|on) USE_XVFB=1 ;;
    0|false|no|off) USE_XVFB=0 ;;
    *)
      echo "--xvfb must be auto, 0, or 1" >&2
      exit 1
      ;;
  esac
fi

mkdir -p "$LOGS_ROOT"
RUN_DIR_ABS=$("$PYTHON_BIN" - <<PY
from pathlib import Path
print(Path("$RUN_DIR").resolve())
PY
)
RUN_NAME=$(basename "$RUN_DIR_ABS")
CMD_FILE="$LOGS_ROOT/${RUN_NAME}.cmd.sh"
LOG_FILE="$LOGS_ROOT/${RUN_NAME}.log"
PID_FILE="$LOGS_ROOT/${RUN_NAME}.pid"
JOB_ID_FILE="$LOGS_ROOT/${RUN_NAME}.job_id"
SCHEDULER_FILE="$LOGS_ROOT/${RUN_NAME}.scheduler"
SUBMIT_MODE_FILE="$LOGS_ROOT/${RUN_NAME}.submit_mode"
RUN_DIR_FILE="$LOGS_ROOT/${RUN_NAME}.run_dir"
XVFB_LOG_FILE="$LOGS_ROOT/${RUN_NAME}.xvfb.log"

if [[ "$SUBMIT_MODE" == "local" && -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "run already active: run_name=$RUN_NAME pid=$OLD_PID" >&2
    exit 1
  fi
fi
if [[ "$SUBMIT_MODE" == "slurm" && -f "$JOB_ID_FILE" ]]; then
  OLD_JOB_ID=$(cat "$JOB_ID_FILE" 2>/dev/null || true)
  if [[ -n "$OLD_JOB_ID" ]] && squeue -h -j "$OLD_JOB_ID" >/dev/null 2>&1; then
    if [[ -n "$(squeue -h -j "$OLD_JOB_ID" -o '%A' 2>/dev/null)" ]]; then
      echo "run already active: run_name=$RUN_NAME job_id=$OLD_JOB_ID" >&2
      exit 1
    fi
  fi
fi

mkdir -p "$(dirname "$RUN_DIR_ABS")" "$RUN_DIR_ABS"

EXTRA_ARGS=()
if [[ -n "$SAMPLE_CSV" ]]; then
  EXTRA_ARGS+=(--sample-csv "$SAMPLE_CSV")
fi

cat >"$CMD_FILE" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd $(printf '%q' "$REPO_ROOT")
export SEED_PROFILE=$(printf '%q' "$SEED_PROFILE")
export PROFILE_NAME=$(printf '%q' "$PROFILE_NAME")
echo "[launcher] started_at=\$(date -Is)"
echo "[launcher] run_name=$(printf '%q' "$RUN_NAME")"
echo "[launcher] run_dir=$(printf '%q' "$RUN_DIR_ABS")"
echo "[launcher] python=$(printf '%q' "$PYTHON_BIN")"
echo "[launcher] suite=$(printf '%q' "$SUITE")"
echo "[launcher] seed_profile=$(printf '%q' "$SEED_PROFILE")"
echo "[launcher] profile_name=$(printf '%q' "$PROFILE_NAME")"
echo "[launcher] submit_mode=$(printf '%q' "$SUBMIT_MODE")"
EOF

if [[ -n "$CHROME_PATH_VALUE" ]]; then
  printf 'export CHROME_PATH=%q\n' "$CHROME_PATH_VALUE" >>"$CMD_FILE"
fi
if [[ -n "$NO_SANDBOX_VALUE" ]]; then
  printf 'export PDF_BROWSER_NO_SANDBOX=%q\n' "$NO_SANDBOX_VALUE" >>"$CMD_FILE"
fi
if [[ "$USE_XVFB" == "1" ]]; then
  printf 'export XVFB_BIN=%q\n' "$XVFB_BIN_VALUE" >>"$CMD_FILE"
  printf 'export XVFB_DISPLAY=%q\n' "$XVFB_DISPLAY_VALUE" >>"$CMD_FILE"
  printf 'export XVFB_SCREEN=%q\n' "$XVFB_SCREEN_VALUE" >>"$CMD_FILE"
  printf 'export XVFB_LOG_FILE=%q\n' "$XVFB_LOG_FILE" >>"$CMD_FILE"
fi
{
  if [[ -n "$CHROME_PATH_VALUE" ]]; then
    echo "echo \"[launcher] chrome_path=$(printf '%q' "$CHROME_PATH_VALUE")\""
  fi
  echo "echo \"[launcher] headless=$(printf '%q' "$HEADLESS")\""
  echo "echo \"[launcher] xvfb_enabled=$(printf '%q' "$USE_XVFB")\""
  if [[ "$USE_XVFB" == "1" ]]; then
    echo "echo \"[launcher] xvfb_bin=$(printf '%q' "$XVFB_BIN_VALUE")\""
    echo "echo \"[launcher] xvfb_display=$(printf '%q' "$XVFB_DISPLAY_VALUE")\""
    echo "echo \"[launcher] xvfb_screen=$(printf '%q' "$XVFB_SCREEN_VALUE")\""
    echo "echo \"[launcher] xvfb_log=$(printf '%q' "$XVFB_LOG_FILE")\""
  fi
} >>"$CMD_FILE"

{
  if [[ "$USE_XVFB" == "1" ]]; then
    printf 'exec %q -- %q %q' "$REPO_ROOT/scripts/with_xvfb.sh" "$PYTHON_BIN" "$REPO_ROOT/experiment/run_linux_headless_suite.py"
  else
    printf 'exec %q %q' "$PYTHON_BIN" "$REPO_ROOT/experiment/run_linux_headless_suite.py"
  fi
  printf ' --suite %q' "$SUITE"
  printf ' --run-dir %q' "$RUN_DIR_ABS"
  printf ' --persistent-profile-dir %q' "$SEED_PROFILE"
  printf ' --profile-name %q' "$PROFILE_NAME"
  printf ' --runtime-preset %q' "$RUNTIME_PRESET"
  printf ' --execution-env %q' "$EXECUTION_ENV"
  printf ' --headless %q' "$HEADLESS"
  printf ' --landing-workers %q' "$LANDING_WORKERS"
  printf ' --download-workers %q' "$DOWNLOAD_WORKERS"
  printf ' --after-first-pass %q' "$AFTER_FIRST_PASS"
  for ((i=0; i<${#EXTRA_ARGS[@]}; i++)); do
    printf ' %q' "${EXTRA_ARGS[$i]}"
  done
  printf ' --execute\n'
} >>"$CMD_FILE"
chmod +x "$CMD_FILE"

printf '%s\n' "$RUN_DIR_ABS" >"$RUN_DIR_FILE"
: >"$LOG_FILE"
rm -f "$PID_FILE" "$JOB_ID_FILE" "$SCHEDULER_FILE"
printf '%s\n' "$SUBMIT_MODE" >"$SUBMIT_MODE_FILE"
: >"$LOG_FILE"
{
  echo "[submit] requested_at=$(date -Is)"
  echo "[submit] run_name=$RUN_NAME"
  echo "[submit] run_dir=$RUN_DIR_ABS"
  echo "[submit] submit_mode=$SUBMIT_MODE"
} >>"$LOG_FILE"

if [[ "$SUBMIT_MODE" == "slurm" ]]; then
  SBATCH_CMD=(
    sbatch
    --parsable
    --job-name "$RUN_NAME"
    --chdir "$REPO_ROOT"
    --output "$LOG_FILE"
    --error "$LOG_FILE"
    --open-mode append
    --signal "$SLURM_SIGNAL_VALUE"
    --time "$SLURM_TIME_VALUE"
  )
  if [[ -n "$SLURM_PARTITION_VALUE" ]]; then
    SBATCH_CMD+=(--partition "$SLURM_PARTITION_VALUE")
  fi
  if [[ -n "$SLURM_ACCOUNT_VALUE" ]]; then
    SBATCH_CMD+=(--account "$SLURM_ACCOUNT_VALUE")
  fi
  if [[ -n "$SLURM_MEM_VALUE" ]]; then
    SBATCH_CMD+=(--mem "$SLURM_MEM_VALUE")
  fi
  if [[ -n "$SLURM_CPUS_VALUE" ]]; then
    SBATCH_CMD+=(--cpus-per-task "$SLURM_CPUS_VALUE")
  fi
  SBATCH_CMD+=("$CMD_FILE")

  RAW_JOB_ID=$("${SBATCH_CMD[@]}")
  JOB_ID=${RAW_JOB_ID%%;*}
  printf '%s\n' "$JOB_ID" >"$JOB_ID_FILE"
  printf '%s\n' "slurm" >"$SCHEDULER_FILE"
  {
    echo "[submit] scheduler=slurm"
    echo "[submit] job_id=$JOB_ID"
    echo "[submit] slurm_time=$SLURM_TIME_VALUE"
    echo "[submit] slurm_signal=$SLURM_SIGNAL_VALUE"
    if [[ -n "$SLURM_PARTITION_VALUE" ]]; then
      echo "[submit] slurm_partition=$SLURM_PARTITION_VALUE"
    fi
    if [[ -n "$SLURM_ACCOUNT_VALUE" ]]; then
      echo "[submit] slurm_account=$SLURM_ACCOUNT_VALUE"
    fi
    if [[ -n "$SLURM_MEM_VALUE" ]]; then
      echo "[submit] slurm_mem=$SLURM_MEM_VALUE"
    fi
    if [[ -n "$SLURM_CPUS_VALUE" ]]; then
      echo "[submit] slurm_cpus=$SLURM_CPUS_VALUE"
    fi
  } >>"$LOG_FILE"

  echo "run_name=$RUN_NAME"
  echo "run_dir=$RUN_DIR_ABS"
  echo "submit_mode=$SUBMIT_MODE"
  echo "scheduler=slurm"
  echo "job_id=$JOB_ID"
  echo "cmd_file=$CMD_FILE"
  echo "log_file=$LOG_FILE"
  echo "job_id_file=$JOB_ID_FILE"
  echo "run_dir_file=$RUN_DIR_FILE"
  echo "export_run_name=RUN_NAME=$RUN_NAME"
  echo "status_hint=bash scripts/check_linux_suite_status.sh $RUN_NAME"
  echo "tail_hint=bash scripts/tail_linux_suite_logs.sh $RUN_NAME all"
  echo "collect_hint=bash scripts/collect_linux_suite_artifacts.sh $RUN_NAME"
  exit 0
fi

printf '%s\n' "local" >"$SCHEDULER_FILE"
{
  echo "[submit] scheduler=local"
  echo "[submit] env_file=$ENV_FILE"
} >>"$LOG_FILE"

nohup bash "$CMD_FILE" >>"$LOG_FILE" 2>&1 < /dev/null &
PID=$!
printf '%s\n' "$PID" >"$PID_FILE"

echo "run_name=$RUN_NAME"
echo "run_dir=$RUN_DIR_ABS"
echo "submit_mode=$SUBMIT_MODE"
echo "scheduler=local"
echo "pid=$PID"
echo "cmd_file=$CMD_FILE"
echo "log_file=$LOG_FILE"
echo "pid_file=$PID_FILE"
echo "run_dir_file=$RUN_DIR_FILE"
echo "export_run_name=RUN_NAME=$RUN_NAME"
echo "status_hint=bash scripts/check_linux_suite_status.sh $RUN_NAME"
echo "tail_hint=bash scripts/tail_linux_suite_logs.sh $RUN_NAME all"
echo "collect_hint=bash scripts/collect_linux_suite_artifacts.sh $RUN_NAME"
