#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
usage:
  bash scripts/package_analysis_bundle.sh [run_name] [output_archive]

examples:
  bash scripts/package_analysis_bundle.sh
  bash scripts/package_analysis_bundle.sh paper_download_run
  bash scripts/package_analysis_bundle.sh paper_download_run ~/paper_download_run_analysis_bundle.tar.gz
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

RUN_NAME="${1:-paper_download_run}"
OUTPUT_ARCHIVE="${2:-$HOME/${RUN_NAME}_analysis_bundle.tar.gz}"

cd "$REPO_ROOT"

OUTPUT_ARCHIVE="$(python3 - "$OUTPUT_ARCHIVE" <<'PY'
import os
import sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

INCLUDE_PATHS=()
MISSING_PATHS=()

add_if_exists() {
  local path="$1"
  if [[ -e "$path" ]]; then
    INCLUDE_PATHS+=("$path")
  else
    MISSING_PATHS+=("$path")
  fi
}

add_if_exists "outputs/${RUN_NAME}/summary.json"
add_if_exists "outputs/${RUN_NAME}/openalex_search_results_parallel.csv"
add_if_exists "outputs/${RUN_NAME}/failed_papers.csv"
add_if_exists "outputs/${RUN_NAME}/failed_papers.jsonl"
add_if_exists "outputs/${RUN_NAME}/download_attempts_summary.json"
add_if_exists "outputs/${RUN_NAME}/download_attempts.jsonl"
add_if_exists "outputs/${RUN_NAME}/landing_precheck/landing_report.json"
add_if_exists "outputs/${RUN_NAME}/landing_precheck/landing_results.jsonl"
add_if_exists "outputs/${RUN_NAME}/metadata"

if [[ -f "logs/${RUN_NAME}.log" ]]; then
  INCLUDE_PATHS+=("logs/${RUN_NAME}.log")
fi
if [[ -f "logs/${RUN_NAME}.pid" ]]; then
  INCLUDE_PATHS+=("logs/${RUN_NAME}.pid")
fi
if [[ -f "logs/${RUN_NAME}.cmd.sh" ]]; then
  INCLUDE_PATHS+=("logs/${RUN_NAME}.cmd.sh")
fi

if [[ ${#INCLUDE_PATHS[@]} -eq 0 ]]; then
  echo "no matching artifacts found for run_name=${RUN_NAME}" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_ARCHIVE")"
tar -czf "$OUTPUT_ARCHIVE" "${INCLUDE_PATHS[@]}"

echo "created: $OUTPUT_ARCHIVE"
echo
echo "included:"
for path in "${INCLUDE_PATHS[@]}"; do
  echo "  $path"
done

if [[ ${#MISSING_PATHS[@]} -gt 0 ]]; then
  echo
  echo "missing (skipped):"
  for path in "${MISSING_PATHS[@]}"; do
    echo "  $path"
  done
fi
