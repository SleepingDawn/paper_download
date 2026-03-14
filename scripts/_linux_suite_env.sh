#!/usr/bin/env bash

linux_suite_repo_root() {
  local script_dir
  script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
  CDPATH= cd -- "$script_dir/.." && pwd
}

linux_suite_env_file() {
  local repo_root=$1
  printf '%s\n' "${LINUX_SUITE_ENV_FILE:-$repo_root/config/linux_server.env}"
}

linux_suite_load_env() {
  local repo_root=$1
  local env_file
  env_file=$(linux_suite_env_file "$repo_root")
  if [[ -f "$env_file" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    set +a
  fi
}

linux_suite_runs_root() {
  local repo_root=$1
  printf '%s\n' "${RUNS_ROOT:-$repo_root/outputs/linux_headless_suite_runs}"
}

linux_suite_logs_root() {
  local repo_root=$1
  printf '%s\n' "${LOGS_ROOT:-$repo_root/logs}"
}

linux_suite_collect_root() {
  local repo_root=$1
  printf '%s\n' "${COLLECT_ROOT:-$repo_root/outputs/collected_runs}"
}
