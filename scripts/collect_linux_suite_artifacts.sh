#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: bash scripts/collect_linux_suite_artifacts.sh <run-name-or-run-dir> [--include-pdfs 0|1] [--output <bundle.tar.gz>]
EOF
}

if [[ $# -lt 1 ]]; then
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
COLLECT_ROOT=$(linux_suite_collect_root "$REPO_ROOT")
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"
RUN_REF=$1
shift

INCLUDE_PDFS=0
OUTPUT_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --include-pdfs) INCLUDE_PDFS=${2:-0}; shift 2 ;;
    --output) OUTPUT_PATH=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

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

relpath() {
  "$PYTHON_BIN" - <<PY
from pathlib import Path
root = Path("$REPO_ROOT").resolve()
path = Path("$1").resolve()
print(path.relative_to(root))
PY
}

RUN_DIR=$(resolve_run_dir "$RUN_REF") || {
  echo "run not found: $RUN_REF" >&2
  exit 1
}
RUN_NAME=$(basename "$RUN_DIR")
COLLECT_DIR="$COLLECT_ROOT"
mkdir -p "$COLLECT_DIR"
if [[ -z "$OUTPUT_PATH" ]]; then
  OUTPUT_PATH="$COLLECT_DIR/${RUN_NAME}_bundle.tar.gz"
fi

TMP_LIST=$(mktemp)
cleanup() {
  rm -f "$TMP_LIST"
}
trap cleanup EXIT

add_if_exists() {
  local path=$1
  if [[ -e "$path" ]]; then
    relpath "$path" >>"$TMP_LIST"
  fi
}

add_if_exists "$LOGS_ROOT/${RUN_NAME}.cmd.sh"
add_if_exists "$LOGS_ROOT/${RUN_NAME}.log"
add_if_exists "$LOGS_ROOT/${RUN_NAME}.pid"
add_if_exists "$LOGS_ROOT/${RUN_NAME}.run_dir"
add_if_exists "$RUN_DIR/execution_manifest.json"
add_if_exists "$RUN_DIR/run_suite.sh"
add_if_exists "$RUN_DIR/logs"
add_if_exists "$RUN_DIR/landing"
add_if_exists "$RUN_DIR/download/run"
add_if_exists "$RUN_DIR/summary"
if [[ "$INCLUDE_PDFS" == "1" ]]; then
  add_if_exists "$RUN_DIR/download/pdfs"
fi

sort -u "$TMP_LIST" -o "$TMP_LIST"

if [[ ! -s "$TMP_LIST" ]]; then
  echo "nothing to package for run=$RUN_NAME" >&2
  exit 1
fi

MANIFEST_TXT="$COLLECT_DIR/${RUN_NAME}_bundle_manifest.txt"
cp "$TMP_LIST" "$MANIFEST_TXT"

tar -czf "$OUTPUT_PATH" -C "$REPO_ROOT" -T "$TMP_LIST"

echo "run_name=$RUN_NAME"
echo "run_dir=$RUN_DIR"
echo "bundle=$OUTPUT_PATH"
echo "manifest=$MANIFEST_TXT"
