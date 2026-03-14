#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
usage:
  bash scripts/run_parallel_detached.sh \
    --doi-path <csv> \
    --profile-root <linux_user_data_dir_root> \
    [--run-name <name>] \
    [--profile-name <name>] \
    [--runtime-preset <preset>] \
    [--max-workers <n>] \
    [--precheck-landing <0|1>] \
    [--after-first-pass <stop|deep>] \
    [--abort-on-landing-block <0|1>] \
    [--publisher-cooldown-sec <sec>] \
    [--global-start-spacing-sec <sec>] \
    [--jitter-min-sec <sec>] \
    [--jitter-max-sec <sec>] \
    [--python-bin <path>] \
    [-- <extra parallel_download.py args...>]
EOF
}

normalize_path() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import os
import sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
}

slugify() {
  python3 - "$1" <<'PY'
import re
import sys
text = sys.argv[1].strip()
text = re.sub(r'[^A-Za-z0-9._-]+', '_', text)
text = re.sub(r'_+', '_', text).strip('._-')
print(text or "run")
PY
}

DOI_PATH=""
PROFILE_ROOT=""
PROFILE_NAME="Default"
RUNTIME_PRESET="linux_cli_seeded"
MAX_WORKERS="1"
PRECHECK_LANDING="0"
AFTER_FIRST_PASS="stop"
ABORT_ON_LANDING_BLOCK="1"
PUBLISHER_COOLDOWN_SEC="4"
GLOBAL_START_SPACING_SEC="0.8"
JITTER_MIN_SEC="0.2"
JITTER_MAX_SEC="0.8"
PYTHON_BIN=""
RUN_NAME=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --doi-path)
      DOI_PATH="${2:-}"
      shift 2
      ;;
    --profile-root)
      PROFILE_ROOT="${2:-}"
      shift 2
      ;;
    --profile-name)
      PROFILE_NAME="${2:-}"
      shift 2
      ;;
    --runtime-preset)
      RUNTIME_PRESET="${2:-}"
      shift 2
      ;;
    --max-workers)
      MAX_WORKERS="${2:-}"
      shift 2
      ;;
    --precheck-landing)
      PRECHECK_LANDING="${2:-}"
      shift 2
      ;;
    --after-first-pass)
      AFTER_FIRST_PASS="${2:-}"
      shift 2
      ;;
    --abort-on-landing-block)
      ABORT_ON_LANDING_BLOCK="${2:-}"
      shift 2
      ;;
    --publisher-cooldown-sec)
      PUBLISHER_COOLDOWN_SEC="${2:-}"
      shift 2
      ;;
    --global-start-spacing-sec)
      GLOBAL_START_SPACING_SEC="${2:-}"
      shift 2
      ;;
    --jitter-min-sec)
      JITTER_MIN_SEC="${2:-}"
      shift 2
      ;;
    --jitter-max-sec)
      JITTER_MAX_SEC="${2:-}"
      shift 2
      ;;
    --python-bin)
      PYTHON_BIN="${2:-}"
      shift 2
      ;;
    --run-name)
      RUN_NAME="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$DOI_PATH" || -z "$PROFILE_ROOT" ]]; then
  usage >&2
  exit 2
fi

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
  else
    PYTHON_BIN="$(command -v python3)"
  fi
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python executable not found: $PYTHON_BIN" >&2
  exit 1
fi

DOI_PATH="$(normalize_path "$DOI_PATH")"
PROFILE_ROOT="$(normalize_path "$PROFILE_ROOT")"

if [[ ! -f "$DOI_PATH" ]]; then
  echo "doi csv not found: $DOI_PATH" >&2
  exit 1
fi

if [[ ! -d "$PROFILE_ROOT" ]]; then
  echo "profile root not found: $PROFILE_ROOT" >&2
  exit 1
fi

if [[ -z "$RUN_NAME" ]]; then
  base_name="$(basename "$DOI_PATH")"
  base_name="${base_name%.*}"
  timestamp="$(date +%Y%m%d_%H%M%S)"
  RUN_NAME="$(slugify "${base_name}_${timestamp}")"
fi

LOG_DIR="${REPO_ROOT}/logs"
OUTPUT_DIR="${REPO_ROOT}/outputs/${RUN_NAME}"
PDF_OUTPUT_DIR="${REPO_ROOT}/pdfs/${RUN_NAME}"
PID_PATH="${LOG_DIR}/${RUN_NAME}.pid"
LOG_PATH="${LOG_DIR}/${RUN_NAME}.log"
CMD_PATH="${LOG_DIR}/${RUN_NAME}.cmd.sh"

mkdir -p "$LOG_DIR" "$OUTPUT_DIR" "$PDF_OUTPUT_DIR"

CMD=(
  "$PYTHON_BIN"
  -u
  "${REPO_ROOT}/parallel_download.py"
  --doi_path "$DOI_PATH"
  --runtime-preset "$RUNTIME_PRESET"
  --persistent-profile-dir "$PROFILE_ROOT"
  --profile-name "$PROFILE_NAME"
  --max_workers "$MAX_WORKERS"
  --precheck-landing "$PRECHECK_LANDING"
  --after-first-pass "$AFTER_FIRST_PASS"
  --abort-on-landing-block "$ABORT_ON_LANDING_BLOCK"
  --publisher-cooldown-sec "$PUBLISHER_COOLDOWN_SEC"
  --global-start-spacing-sec "$GLOBAL_START_SPACING_SEC"
  --jitter-min-sec "$JITTER_MIN_SEC"
  --jitter-max-sec "$JITTER_MAX_SEC"
  --output_dir "$OUTPUT_DIR"
  --pdf_output_dir "$PDF_OUTPUT_DIR"
  --non-interactive
)

if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  CMD+=("${EXTRA_ARGS[@]}")
fi

{
  echo "#!/usr/bin/env bash"
  printf 'cd %q\n' "$REPO_ROOT"
  printf '%q ' "${CMD[@]}"
  echo
} > "$CMD_PATH"
chmod +x "$CMD_PATH"

cd "$REPO_ROOT"
nohup "${CMD[@]}" >"$LOG_PATH" 2>&1 < /dev/null &
PID=$!
echo "$PID" > "$PID_PATH"

cat <<EOF
started detached run
run_name: $RUN_NAME
pid: $PID
log: $LOG_PATH
pid_file: $PID_PATH
command_file: $CMD_PATH
output_dir: $OUTPUT_DIR
pdf_output_dir: $PDF_OUTPUT_DIR

follow log:
  tail -f $LOG_PATH

check process:
  ps -fp $PID

stop run:
  kill $PID
EOF
