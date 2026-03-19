#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: bash scripts/with_xvfb.sh [--display <value>] [--xvfb-bin <path>] [--screen <WxHxD>] -- <command> [args...]
EOF
}

XVFB_DISPLAY_VALUE="${XVFB_DISPLAY:-:99}"
XVFB_BIN_VALUE="${XVFB_BIN:-$HOME/.local/bin/Xvfb}"
XVFB_SCREEN_VALUE="${XVFB_SCREEN:-1280x1024x24}"
XVFB_LOG_FILE_VALUE="${XVFB_LOG_FILE:-}"
XVFB_STARTUP_DELAY_VALUE="${XVFB_STARTUP_DELAY_SEC:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --display) XVFB_DISPLAY_VALUE=${2:-}; shift 2 ;;
    --xvfb-bin) XVFB_BIN_VALUE=${2:-}; shift 2 ;;
    --screen) XVFB_SCREEN_VALUE=${2:-}; shift 2 ;;
    --) shift; break ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "command is required" >&2
  usage >&2
  exit 1
fi

XVFB_DIR=$(CDPATH= cd -- "$(dirname -- "$XVFB_BIN_VALUE")" && pwd)
export PATH="$XVFB_DIR${PATH:+:$PATH}"
export DISPLAY="$XVFB_DISPLAY_VALUE"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
export PDF_BROWSER_ALLOW_HEADFUL_LINUX_SERVER=1
export PDF_BROWSER_XVFB_ACTIVE=1

if [[ ! -x "$XVFB_BIN_VALUE" ]]; then
  echo "[xvfb] binary not executable: $XVFB_BIN_VALUE" >&2
  exit 1
fi

XVFB_PID=""
CMD_PID=""
CMD_PGID=""

resolve_pgid() {
  local pid=${1:-}
  if [[ -z "$pid" ]]; then
    return 1
  fi
  ps -o pgid= -p "$pid" 2>/dev/null | tr -d '[:space:]'
}

terminate_command_group() {
  local signal=${1:-TERM}
  local target_pid=${CMD_PID:-}
  local target_pgid=${CMD_PGID:-}
  if [[ -z "$target_pid" ]]; then
    return 0
  fi
  if [[ -z "$target_pgid" ]]; then
    target_pgid=$(resolve_pgid "$target_pid" || true)
  fi
  if [[ -n "$target_pgid" ]]; then
    kill "-$signal" -- "-$target_pgid" 2>/dev/null || true
  else
    kill "-$signal" "$target_pid" 2>/dev/null || true
  fi
}

cleanup() {
  local status=$?
  if [[ -n "$CMD_PID" ]] && kill -0 "$CMD_PID" 2>/dev/null; then
    terminate_command_group TERM
    sleep 1
    if kill -0 "$CMD_PID" 2>/dev/null; then
      terminate_command_group KILL
    fi
    wait "$CMD_PID" 2>/dev/null || true
  fi
  if [[ -n "$XVFB_PID" ]] && kill -0 "$XVFB_PID" 2>/dev/null; then
    kill "$XVFB_PID" 2>/dev/null || true
    wait "$XVFB_PID" 2>/dev/null || true
    echo "[xvfb] stopped pid=$XVFB_PID display=$DISPLAY"
  fi
  return $status
}

shutdown_from_signal() {
  local sig=${1:-TERM}
  trap - EXIT INT TERM
  cleanup
  if [[ "$sig" == "INT" ]]; then
    exit 130
  fi
  exit 143
}

trap cleanup EXIT
trap 'shutdown_from_signal INT' INT
trap 'shutdown_from_signal TERM' TERM

XVFB_CMD=("$XVFB_BIN_VALUE" "$DISPLAY" -screen 0 "$XVFB_SCREEN_VALUE")
if [[ -n "$XVFB_LOG_FILE_VALUE" ]]; then
  mkdir -p "$(dirname -- "$XVFB_LOG_FILE_VALUE")"
  "${XVFB_CMD[@]}" >>"$XVFB_LOG_FILE_VALUE" 2>&1 &
else
  "${XVFB_CMD[@]}" >/dev/null 2>&1 &
fi
XVFB_PID=$!
sleep "$XVFB_STARTUP_DELAY_VALUE"
if ! kill -0 "$XVFB_PID" 2>/dev/null; then
  echo "[xvfb] failed to start display=$DISPLAY bin=$XVFB_BIN_VALUE" >&2
  wait "$XVFB_PID" 2>/dev/null || true
  exit 1
fi

echo "[xvfb] started pid=$XVFB_PID display=$DISPLAY bin=$XVFB_BIN_VALUE"
echo "[xvfb] PATH=$PATH"
echo "[xvfb] LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}"

set +e
set -m
"$@" &
set +m
CMD_PID=$!
CMD_PGID=$(resolve_pgid "$CMD_PID" || true)
echo "[xvfb] command_started pid=$CMD_PID pgid=${CMD_PGID:-unknown}"
wait "$CMD_PID"
STATUS=$?
CMD_PID=""
CMD_PGID=""
set -e
exit "$STATUS"
