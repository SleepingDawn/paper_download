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
  printf '%s\n' "${RUNS_ROOT:-$repo_root/outputs}"
}

linux_suite_logs_root() {
  local repo_root=$1
  printf '%s\n' "${LOGS_ROOT:-$repo_root/logs}"
}

linux_suite_collect_root() {
  local repo_root=$1
  printf '%s\n' "${COLLECT_ROOT:-$repo_root/outputs/collected_runs}"
}

linux_suite_in_slurm_job() {
  [[ -n "${SLURM_JOB_ID:-}" ]]
}

linux_suite_default_submit_mode() {
  if linux_suite_in_slurm_job; then
    printf '%s\n' "local"
  else
    printf '%s\n' "slurm"
  fi
}

linux_suite_slurm_time_limit() {
  printf '%s\n' "${LINUX_SUITE_SLURM_TIME_LIMIT:-12:00:00}"
}

linux_suite_slurm_signal() {
  printf '%s\n' "${LINUX_SUITE_SLURM_SIGNAL:-TERM@120}"
}

linux_suite_slurm_partition() {
  printf '%s\n' "${LINUX_SUITE_SLURM_PARTITION:-}"
}

linux_suite_slurm_account() {
  printf '%s\n' "${LINUX_SUITE_SLURM_ACCOUNT:-}"
}

linux_suite_slurm_mem() {
  printf '%s\n' "${LINUX_SUITE_SLURM_MEM:-}"
}

linux_suite_slurm_cpus() {
  printf '%s\n' "${LINUX_SUITE_SLURM_CPUS:-}"
}

linux_suite_job_timeout_buffer_seconds() {
  printf '%s\n' "${LINUX_SUITE_JOB_TIMEOUT_BUFFER_SECONDS:-600}"
}

linux_suite_duration_to_seconds() {
  local raw=${1:-}
  local days=0
  local hours=0
  local minutes=0
  local seconds=0
  local time_part=$raw
  local field1=""
  local field2=""
  local field3=""

  if [[ -z "$raw" ]]; then
    printf '%s\n' "0"
    return 0
  fi

  if [[ "$raw" == *-* ]]; then
    days=${raw%%-*}
    time_part=${raw#*-}
  fi

  IFS=':' read -r field1 field2 field3 <<<"$time_part"

  if [[ -n "$field3" ]]; then
    hours=$field1
    minutes=$field2
    seconds=$field3
  elif [[ -n "$field2" ]]; then
    minutes=$field1
    seconds=$field2
  else
    minutes=$field1
  fi

  if ! [[ "$days" =~ ^[0-9]+$ && "$hours" =~ ^[0-9]+$ && "$minutes" =~ ^[0-9]+$ && "$seconds" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "0"
    return 1
  fi

  printf '%s\n' "$((10#$days * 86400 + 10#$hours * 3600 + 10#$minutes * 60 + 10#$seconds))"
}
