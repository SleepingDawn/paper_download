#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: bash scripts/tail_linux_suite_logs.sh <run-name-or-run-dir> [stage]

stage:
  all        root + landing/download/summarize stdout/stderr (default)
  root
  landing
  download
  summarize
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUN_REF=$1
STAGE=${2:-all}

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

FILES=()
case "$STAGE" in
  all)
    FILES+=("$REPO_ROOT/logs/${RUN_NAME}.log")
    FILES+=("$RUN_DIR/logs/landing.stdout.log" "$RUN_DIR/logs/landing.stderr.log")
    FILES+=("$RUN_DIR/logs/download.stdout.log" "$RUN_DIR/logs/download.stderr.log")
    FILES+=("$RUN_DIR/logs/summarize.stdout.log" "$RUN_DIR/logs/summarize.stderr.log")
    ;;
  root)
    FILES+=("$REPO_ROOT/logs/${RUN_NAME}.log")
    ;;
  landing)
    FILES+=("$RUN_DIR/logs/landing.stdout.log" "$RUN_DIR/logs/landing.stderr.log")
    ;;
  download)
    FILES+=("$RUN_DIR/logs/download.stdout.log" "$RUN_DIR/logs/download.stderr.log")
    ;;
  summarize)
    FILES+=("$RUN_DIR/logs/summarize.stdout.log" "$RUN_DIR/logs/summarize.stderr.log")
    ;;
  *)
    echo "unknown stage: $STAGE" >&2
    exit 1
    ;;
esac

EXISTING=()
for path in "${FILES[@]}"; do
  if [[ -f "$path" ]]; then
    EXISTING+=("$path")
  fi
done

if [[ ${#EXISTING[@]} -eq 0 ]]; then
  echo "no log files found for run=$RUN_NAME stage=$STAGE" >&2
  exit 1
fi

echo "tailing logs for run=$RUN_NAME stage=$STAGE"
tail -F "${EXISTING[@]}"
