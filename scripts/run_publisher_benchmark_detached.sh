#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
usage:
  bash scripts/run_publisher_benchmark_detached.sh \
    --profile-root <linux_user_data_dir_root> \
    [--publisher <name[,name...]>] \
    [--benchmark-csv <path>] \
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
    [-- <extra run_parallel_detached.sh args...>]

examples:
  bash scripts/run_publisher_benchmark_detached.sh \
    --profile-root ~/chrome_profiles/linux_chromium_user_data_seed

  bash scripts/run_publisher_benchmark_detached.sh \
    --profile-root ~/chrome_profiles/linux_chromium_user_data_seed \
    --publisher elsevier

  bash scripts/run_publisher_benchmark_detached.sh \
    --profile-root ~/chrome_profiles/linux_chromium_user_data_seed \
    --publisher elsevier,wiley,iop
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
text = sys.argv[1].strip().lower()
text = re.sub(r'[^a-z0-9._-]+', '_', text)
text = re.sub(r'_+', '_', text).strip('._-')
print(text or "publisher_benchmark")
PY
}

PROFILE_ROOT=""
BENCHMARK_CSV="${REPO_ROOT}/experiment/publisher_download_benchmark_20260314.csv"
PUBLISHER_FILTER=""
RUN_NAME=""
PROFILE_NAME="Default"
RUNTIME_PRESET="linux_cli_seeded"
MAX_WORKERS="1"
PRECHECK_LANDING="0"
AFTER_FIRST_PASS="stop"
ABORT_ON_LANDING_BLOCK="1"
PUBLISHER_COOLDOWN_SEC="15"
GLOBAL_START_SPACING_SEC="5"
JITTER_MIN_SEC="2"
JITTER_MAX_SEC="4"
PYTHON_BIN=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile-root)
      PROFILE_ROOT="${2:-}"
      shift 2
      ;;
    --benchmark-csv)
      BENCHMARK_CSV="${2:-}"
      shift 2
      ;;
    --publisher)
      PUBLISHER_FILTER="${2:-}"
      shift 2
      ;;
    --run-name)
      RUN_NAME="${2:-}"
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

if [[ -z "$PROFILE_ROOT" ]]; then
  usage >&2
  exit 2
fi

PROFILE_ROOT="$(normalize_path "$PROFILE_ROOT")"
BENCHMARK_CSV="$(normalize_path "$BENCHMARK_CSV")"

if [[ ! -d "$PROFILE_ROOT" ]]; then
  echo "profile root not found: $PROFILE_ROOT" >&2
  exit 1
fi

if [[ ! -f "$BENCHMARK_CSV" ]]; then
  echo "benchmark csv not found: $BENCHMARK_CSV" >&2
  exit 1
fi

FILTERED_CSV="$BENCHMARK_CSV"
PUBLISHER_TAG="all_publishers"

if [[ -n "$PUBLISHER_FILTER" ]]; then
  PUBLISHER_TAG="$(slugify "$PUBLISHER_FILTER")"
  FILTERED_CSV="${REPO_ROOT}/outputs/benchmark_inputs/publisher_download_benchmark_${PUBLISHER_TAG}.csv"
  mkdir -p "$(dirname "$FILTERED_CSV")"
  python3 - "$BENCHMARK_CSV" "$FILTERED_CSV" "$PUBLISHER_FILTER" <<'PY'
import csv
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
requested = {
    item.strip().lower()
    for item in str(sys.argv[3]).split(",")
    if item.strip()
}

with src.open("r", encoding="utf-8-sig", newline="") as f:
    rows = list(csv.DictReader(f))

fieldnames = rows[0].keys() if rows else []
filtered = [
    row for row in rows
    if str(row.get("benchmark_group", "")).strip().lower() in requested
]

if not filtered:
    available = sorted({
        str(row.get("benchmark_group", "")).strip().lower()
        for row in rows
        if str(row.get("benchmark_group", "")).strip()
    })
    raise SystemExit(
        "no rows matched requested publishers; available="
        + ",".join(available)
    )

dst.parent.mkdir(parents=True, exist_ok=True)
with dst.open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(filtered)

counts = {}
for row in filtered:
    key = str(row.get("benchmark_group", "")).strip().lower()
    counts[key] = counts.get(key, 0) + 1
print(f"wrote {len(filtered)} rows to {dst}")
for key in sorted(counts):
    print(f"{key}: {counts[key]}")
PY
fi

if [[ -z "$RUN_NAME" ]]; then
  RUN_NAME="publisher_download_benchmark_${PUBLISHER_TAG}_$(date +%Y%m%d_%H%M%S)"
fi

CMD=(
  bash
  "${REPO_ROOT}/scripts/run_parallel_detached.sh"
  --doi-path "$FILTERED_CSV"
  --profile-root "$PROFILE_ROOT"
  --run-name "$RUN_NAME"
  --profile-name "$PROFILE_NAME"
  --runtime-preset "$RUNTIME_PRESET"
  --max-workers "$MAX_WORKERS"
  --precheck-landing "$PRECHECK_LANDING"
  --after-first-pass "$AFTER_FIRST_PASS"
  --abort-on-landing-block "$ABORT_ON_LANDING_BLOCK"
  --publisher-cooldown-sec "$PUBLISHER_COOLDOWN_SEC"
  --global-start-spacing-sec "$GLOBAL_START_SPACING_SEC"
  --jitter-min-sec "$JITTER_MIN_SEC"
  --jitter-max-sec "$JITTER_MAX_SEC"
)

if [[ -n "$PYTHON_BIN" ]]; then
  CMD+=(--python-bin "$PYTHON_BIN")
fi

if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  CMD+=(-- "${EXTRA_ARGS[@]}")
fi

printf 'using benchmark csv: %s\n' "$FILTERED_CSV"
printf 'run name: %s\n' "$RUN_NAME"
if [[ -n "$PUBLISHER_FILTER" ]]; then
  printf 'publisher filter: %s\n' "$PUBLISHER_FILTER"
fi

"${CMD[@]}"
