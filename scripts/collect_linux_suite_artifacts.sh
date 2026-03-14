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
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
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

relpath() {
  python3 - <<PY
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
COLLECT_DIR="$REPO_ROOT/outputs/collected_runs"
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

add_if_exists "$REPO_ROOT/logs/${RUN_NAME}.cmd.sh"
add_if_exists "$REPO_ROOT/logs/${RUN_NAME}.log"
add_if_exists "$REPO_ROOT/logs/${RUN_NAME}.pid"
add_if_exists "$REPO_ROOT/logs/${RUN_NAME}.run_dir"
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
