#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <source_root> <output_archive> [profile_name]" >&2
  exit 2
fi

SOURCE_ROOT="$1"
OUTPUT_ARCHIVE="$2"
PROFILE_NAME="${3:-Default}"

cd "${REPO_ROOT}"

python3 scripts/check_linux_seed_profile.py \
  --profile-root "${SOURCE_ROOT}" \
  --profile-name "${PROFILE_NAME}"

python3 scripts/package_linux_seed_profile.py \
  --source-root "${SOURCE_ROOT}" \
  --output "${OUTPUT_ARCHIVE}" \
  --profile-name "${PROFILE_NAME}" \
  --overwrite
