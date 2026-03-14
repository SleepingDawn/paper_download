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
  --run-dir <dir>               explicit run directory (default: outputs/linux_headless_suite_runs/<run-name>)
  --sample-csv <path>           override suite CSV
  --landing-workers <n>         landing workers (default: 2)
  --download-workers <n>        download workers (default: 1)
  --after-first-pass <mode>     stop|deep (default: stop)
  --headless <0|1>              default: 1
  --python <path>               python executable (default: config/linux_server.env or current python3)
  --runtime-preset <value>      default: linux_cli_seeded
  --execution-env <value>       default: linux_server
  --chrome-path <path>          optional explicit browser binary (or config/linux_server.env)
  --no-sandbox <0|1>            export PDF_BROWSER_NO_SANDBOX (default: preserve current env/config)
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
HEADLESS=1
RUNTIME_PRESET="linux_cli_seeded"
EXECUTION_ENV="linux_server"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"
CHROME_PATH_VALUE="${CHROME_PATH:-}"
NO_SANDBOX_VALUE="${PDF_BROWSER_NO_SANDBOX:-}"

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
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

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
RUN_DIR_FILE="$LOGS_ROOT/${RUN_NAME}.run_dir"

if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "run already active: run_name=$RUN_NAME pid=$OLD_PID" >&2
    exit 1
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
EOF

if [[ -n "$CHROME_PATH_VALUE" ]]; then
  printf 'export CHROME_PATH=%q\n' "$CHROME_PATH_VALUE" >>"$CMD_FILE"
fi
if [[ -n "$NO_SANDBOX_VALUE" ]]; then
  printf 'export PDF_BROWSER_NO_SANDBOX=%q\n' "$NO_SANDBOX_VALUE" >>"$CMD_FILE"
fi

{
  printf 'exec %q %q' "$PYTHON_BIN" "$REPO_ROOT/experiment/run_linux_headless_suite.py"
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
{
  echo "[launcher] started_at=$(date -Is)"
  echo "[launcher] run_name=$RUN_NAME"
  echo "[launcher] run_dir=$RUN_DIR_ABS"
  echo "[launcher] python=$PYTHON_BIN"
  echo "[launcher] suite=$SUITE"
  echo "[launcher] env_file=$ENV_FILE"
  echo "[launcher] seed_profile=$SEED_PROFILE"
  echo "[launcher] profile_name=$PROFILE_NAME"
  if [[ -n "$CHROME_PATH_VALUE" ]]; then
    echo "[launcher] chrome_path=$CHROME_PATH_VALUE"
  fi
} >>"$LOG_FILE"

nohup bash "$CMD_FILE" >>"$LOG_FILE" 2>&1 < /dev/null &
PID=$!
printf '%s\n' "$PID" >"$PID_FILE"

echo "run_name=$RUN_NAME"
echo "run_dir=$RUN_DIR_ABS"
echo "pid=$PID"
echo "cmd_file=$CMD_FILE"
echo "log_file=$LOG_FILE"
echo "pid_file=$PID_FILE"
echo "run_dir_file=$RUN_DIR_FILE"
echo "status_hint=bash scripts/check_linux_suite_status.sh $RUN_NAME"
