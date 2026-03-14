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
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUN_REF=$1
TAIL_LINES=${2:-20}

resolve_run_dir() {
  local ref=$1
  if [[ -d "$ref" ]]; then
    python3 - <<PY
from pathlib import Path
print(Path("$ref").resolve())
PY
    return 0
  fi
  local map_file="$REPO_ROOT/logs/${ref}.run_dir"
  if [[ -f "$map_file" ]]; then
    cat "$map_file"
    return 0
  fi
  local default_dir="$REPO_ROOT/outputs/linux_headless_suite_runs/$ref"
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
ROOT_LOG="$REPO_ROOT/logs/${RUN_NAME}.log"
PID_FILE="$REPO_ROOT/logs/${RUN_NAME}.pid"
CMD_FILE="$REPO_ROOT/logs/${RUN_NAME}.cmd.sh"
MANIFEST="$RUN_DIR/execution_manifest.json"

echo "run_name=$RUN_NAME"
echo "run_dir=$RUN_DIR"
echo "root_log=$ROOT_LOG"
echo "pid_file=$PID_FILE"
echo "cmd_file=$CMD_FILE"

if [[ -f "$PID_FILE" ]]; then
  PID=$(cat "$PID_FILE" 2>/dev/null || true)
  echo "pid=$PID"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    echo "process_alive=true"
  else
    echo "process_alive=false"
  fi
else
  echo "pid=missing"
  echo "process_alive=unknown"
fi

if [[ -f "$MANIFEST" ]]; then
  echo "execution_manifest=$MANIFEST"
  python3 - <<PY
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
