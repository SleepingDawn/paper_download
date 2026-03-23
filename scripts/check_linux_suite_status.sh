#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: bash scripts/check_linux_suite_status.sh <run-name-or-run-dir> [tail-lines]
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=scripts/_linux_suite_env.sh
source "$SCRIPT_DIR/_linux_suite_env.sh"
REPO_ROOT=$(linux_suite_repo_root)
linux_suite_load_env "$REPO_ROOT"
RUNS_ROOT=$(linux_suite_runs_root "$REPO_ROOT")
LOGS_ROOT=$(linux_suite_logs_root "$REPO_ROOT")
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"
RUN_REF=$1
TAIL_LINES=${2:-20}

resolve_run_dir() {
  local ref=$1
  if [[ -d "$ref" ]]; then
    "$PYTHON_BIN" - <<PY
from pathlib import Path
print(Path("$ref").resolve())
PY
    return 0
  fi
  local map_file="$LOGS_ROOT/${ref}.run_dir"
  if [[ -f "$map_file" ]]; then
    cat "$map_file"
    return 0
  fi
  local default_dir="$RUNS_ROOT/$ref"
  if [[ -d "$default_dir" ]]; then
    printf '%s\n' "$default_dir"
    return 0
  fi
  return 1
}

RUN_DIR=$(resolve_run_dir "$RUN_REF") || {
  echo "run not found: $RUN_REF" >&2
  exit 1
}
RUN_NAME=$(basename "$RUN_DIR")
ROOT_LOG="$LOGS_ROOT/${RUN_NAME}.log"
PID_FILE="$LOGS_ROOT/${RUN_NAME}.pid"
JOB_ID_FILE="$LOGS_ROOT/${RUN_NAME}.job_id"
SCHEDULER_FILE="$LOGS_ROOT/${RUN_NAME}.scheduler"
SUBMIT_MODE_FILE="$LOGS_ROOT/${RUN_NAME}.submit_mode"
CMD_FILE="$LOGS_ROOT/${RUN_NAME}.cmd.sh"
MANIFEST="$RUN_DIR/execution_manifest.json"
SUBMIT_MODE=$(cat "$SUBMIT_MODE_FILE" 2>/dev/null || true)
SCHEDULER=$(cat "$SCHEDULER_FILE" 2>/dev/null || true)
JOB_ID=$(cat "$JOB_ID_FILE" 2>/dev/null || true)

echo "run_name=$RUN_NAME"
echo "run_dir=$RUN_DIR"
echo "root_log=$ROOT_LOG"
echo "pid_file=$PID_FILE"
echo "job_id_file=$JOB_ID_FILE"
echo "cmd_file=$CMD_FILE"
echo "submit_mode=${SUBMIT_MODE:-unknown}"
echo "scheduler=${SCHEDULER:-unknown}"

if [[ "$SCHEDULER" == "slurm" && -n "$JOB_ID" ]]; then
  echo "job_id=$JOB_ID"
  if command -v squeue >/dev/null 2>&1; then
    SQUEUE_LINE=$(squeue -h -j "$JOB_ID" -o '%T|%M|%L|%R' 2>/dev/null | head -n 1 || true)
  else
    SQUEUE_LINE=""
  fi
  if [[ -n "$SQUEUE_LINE" ]]; then
    IFS='|' read -r JOB_STATE JOB_RUNTIME JOB_TIME_LEFT JOB_REASON <<<"$SQUEUE_LINE"
    echo "scheduler_state=$JOB_STATE"
    echo "scheduler_runtime=$JOB_RUNTIME"
    echo "scheduler_time_left=$JOB_TIME_LEFT"
    echo "scheduler_reason=$JOB_REASON"
    echo "process_alive=true"
  elif command -v sacct >/dev/null 2>&1; then
    SACCT_LINE=$(sacct -X -j "$JOB_ID" --format=JobIDRaw,State,ExitCode,Elapsed -P -n 2>/dev/null | awk -F'|' -v job="$JOB_ID" '$1==job {print; exit}')
    if [[ -n "$SACCT_LINE" ]]; then
      IFS='|' read -r SACCT_JOB_ID SACCT_STATE SACCT_EXIT SACCT_ELAPSED <<<"$SACCT_LINE"
      echo "scheduler_state=$SACCT_STATE"
      echo "scheduler_elapsed=$SACCT_ELAPSED"
      echo "scheduler_exit_code=$SACCT_EXIT"
      case "$SACCT_STATE" in
        PENDING|RUNNING|CONFIGURING|COMPLETING|SUSPENDED|RESIZING|REQUEUED)
          echo "process_alive=true"
          ;;
        *)
          echo "process_alive=false"
          ;;
      esac
    else
      echo "scheduler_state=unknown"
      echo "process_alive=unknown"
    fi
  else
    echo "scheduler_state=unknown"
    echo "process_alive=unknown"
  fi
elif [[ -f "$PID_FILE" ]]; then
  PID=$(cat "$PID_FILE" 2>/dev/null || true)
  echo "pid=$PID"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    PROC_STATE=$(ps -o stat= -p "$PID" 2>/dev/null | awk '{print $1}')
    echo "process_state=${PROC_STATE:-unknown}"
    if [[ "${PROC_STATE:-}" == Z* ]]; then
      echo "process_alive=false"
    else
      echo "process_alive=true"
    fi
  else
    echo "process_alive=false"
  fi
else
  echo "pid=missing"
  echo "process_alive=unknown"
fi

if [[ -f "$MANIFEST" ]]; then
  echo "execution_manifest=$MANIFEST"
  "$PYTHON_BIN" - <<PY
import json
from pathlib import Path
p = Path("$MANIFEST")
obj = json.loads(p.read_text(encoding="utf-8"))
summary = {
    "status": obj.get("status"),
    "run_id": obj.get("run_id"),
    "suite": obj.get("suite"),
    "git_short_commit": obj.get("git_short_commit"),
    "host": obj.get("host"),
    "runtime_preset": obj.get("runtime_preset"),
    "execution_env": obj.get("execution_env"),
    "headless": obj.get("headless"),
    "seed_profile_ok": (obj.get("seed_profile_check") or {}).get("ok"),
    "executed_commands": {},
}
for key, value in (obj.get("executed_commands") or {}).items():
    if isinstance(value, dict):
        summary["executed_commands"][key] = {
            "ok": value.get("ok"),
            "returncode": value.get("returncode"),
            "stdout": value.get("stdout"),
            "stderr": value.get("stderr"),
            "skipped": value.get("skipped", False),
        }
print(json.dumps(summary, ensure_ascii=False, indent=2))
PY
else
  echo "execution_manifest=missing"
fi

echo
echo "[stage outputs]"
if [[ -d "$RUN_DIR" ]]; then
  find "$RUN_DIR" -maxdepth 3 -type f | sort | sed -n '1,240p'
else
  echo "run_dir_missing=true"
fi

if [[ -f "$ROOT_LOG" ]]; then
  echo
  echo "[root log tail]"
  tail -n "$TAIL_LINES" "$ROOT_LOG"
fi

for stage_file in \
  "$RUN_DIR/logs/landing.stderr.log" \
  "$RUN_DIR/logs/download.stderr.log" \
  "$RUN_DIR/logs/summarize.stderr.log"
do
  if [[ -s "$stage_file" ]]; then
    echo
    echo "[tail] $stage_file"
    tail -n "$TAIL_LINES" "$stage_file"
  fi
done
