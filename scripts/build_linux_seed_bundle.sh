#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: bash scripts/build_linux_seed_bundle.sh <source-root> <output.tar.gz> [profile-name]" >&2
  exit 1
fi

SOURCE_ROOT=$1
OUTPUT_PATH=$2
PROFILE_NAME=${3:-Default}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

python3 "$SCRIPT_DIR/check_linux_seed_profile.py" \
  --profile-root "$SOURCE_ROOT" \
  --profile-name "$PROFILE_NAME"

python3 "$SCRIPT_DIR/package_linux_seed_profile.py" \
  --source-root "$SOURCE_ROOT" \
  --output "$OUTPUT_PATH" \
  --profile-name "$PROFILE_NAME"
