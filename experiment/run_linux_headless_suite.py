from __future__ import annotations

import argparse
import getpass
import json
import os
import platform
import shlex
import socket
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

from linux_headless_suite_lib import default_suite_dir, repo_root, write_json


def build_sample_if_needed(suite_dir: Path, rebuild: bool) -> None:
    manifest_path = suite_dir / "suite_manifest.json"
    pilot_path = suite_dir / "pilot_sample.csv"
    full_path = suite_dir / "full_sample.csv"
    if not rebuild and manifest_path.exists() and pilot_path.exists() and full_path.exists():
        return
    cmd = [sys.executable, str(repo_root() / "experiment" / "build_linux_headless_suite.py"), "--output-dir", str(suite_dir)]
    subprocess.run(cmd, cwd=repo_root(), check=True)


def shell_join(cmd: List[str]) -> str:
    return shlex.join([str(part) for part in cmd])


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_text_command(cmd: List[str], cwd: Path) -> str:
    try:
        completed = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return ""
    if completed.returncode != 0:
        return ""
    return str(completed.stdout or "").strip()


def run_command(cmd: List[str], cwd: Path, stdout_path: Path, stderr_path: Path) -> Dict[str, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
        completed = subprocess.run(cmd, cwd=cwd, stdout=stdout_handle, stderr=stderr_handle, check=False)
    return {
        "cmd": [str(part) for part in cmd],
        "cwd": str(cwd),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "returncode": int(completed.returncode),
        "ok": completed.returncode == 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare or execute Linux headless landing/download experiment suites.")
    parser.add_argument("--suite", choices=["pilot", "full"], default="pilot")
    parser.add_argument("--suite-dir", type=Path, default=default_suite_dir())
    parser.add_argument("--sample-csv", type=Path, default=None)
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument("--rebuild-samples", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--skip-landing", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--runtime-preset", choices=["auto", "local_mac", "linux_cli_seeded"], default="linux_cli_seeded")
    parser.add_argument("--execution-env", choices=["auto", "desktop", "linux_server"], default="linux_server")
    parser.add_argument("--headless", type=int, choices=[0, 1], default=1)
    parser.add_argument("--profile-name", type=str, default=os.environ.get("PDF_BROWSER_PROFILE_NAME", "Default"))
    parser.add_argument(
        "--persistent-profile-dir",
        type=Path,
        default=Path(os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", "")) if os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR") else None,
    )
    parser.add_argument("--landing-workers", type=int, default=2)
    parser.add_argument("--download-workers", type=int, default=1)
    parser.add_argument("--after-first-pass", choices=["stop", "deep"], default="stop")
    args = parser.parse_args()

    suite_dir = args.suite_dir.resolve()
    build_sample_if_needed(suite_dir, rebuild=bool(args.rebuild_samples))

    sample_csv = (args.sample_csv or (suite_dir / f"{args.suite}_sample.csv")).resolve()
    if not sample_csv.exists():
        raise FileNotFoundError(f"sample csv not found: {sample_csv}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = (args.run_dir or (repo_root() / "outputs" / "linux_headless_suite_runs" / f"{args.suite}_{timestamp}")).resolve()
    landing_dir = run_dir / "landing"
    download_dir = run_dir / "download"
    summary_dir = run_dir / "summary"
    logs_dir = run_dir / "logs"
    for path in (landing_dir, download_dir, summary_dir, logs_dir):
        path.mkdir(parents=True, exist_ok=True)

    profile_dir = args.persistent_profile_dir.resolve() if args.persistent_profile_dir else None
    landing_jsonl = landing_dir / "landing_access_repro.jsonl"
    landing_report = landing_dir / "landing_access_repro_report.json"
    landing_report_md = landing_dir / "landing_access_repro_report.md"
    landing_artifact_dir = landing_dir / "artifacts"
    landing_fail_zip = landing_dir / "landing_access_failures.zip"
    landing_success_zip = landing_dir / "landing_access_successes.zip"
    download_pdf_dir = download_dir / "pdfs"
    download_run_dir = download_dir / "run"
    download_results_csv = download_run_dir / "openalex_search_results_parallel.csv"
    download_summary_json = download_run_dir / "summary.json"
    merged_csv = summary_dir / "merged_results.csv"
    publisher_csv = summary_dir / "publisher_summary.csv"
    merged_json = summary_dir / "suite_summary.json"
    merged_md = summary_dir / "suite_summary.md"

    landing_cmd = [
        sys.executable,
        str(repo_root() / "landing_access_repro.py"),
        "--input",
        str(sample_csv),
        "--workers",
        str(args.landing_workers),
        "--headless",
        str(args.headless),
        "--runtime-preset",
        args.runtime_preset,
        "--execution-env",
        args.execution_env,
        "--profile-mode",
        "auto",
        "--profile-name",
        str(args.profile_name),
        "--artifact-dir",
        str(landing_artifact_dir),
        "--capture-fail-screenshot",
        "1",
        "--artifact-zip",
        str(landing_fail_zip),
        "--success-artifact-zip",
        str(landing_success_zip),
        "--output-jsonl",
        str(landing_jsonl),
        "--report",
        str(landing_report),
        "--report-md",
        str(landing_report_md),
    ]
    if profile_dir is not None:
        landing_cmd.extend(["--persistent-profile-dir", str(profile_dir)])

    download_cmd = [
        sys.executable,
        str(repo_root() / "parallel_download.py"),
        "--doi_path",
        str(sample_csv),
        "--max_workers",
        str(args.download_workers),
        "--output_dir",
        str(download_run_dir),
        "--pdf_output_dir",
        str(download_pdf_dir),
        "--non-interactive",
        "--after-first-pass",
        args.after_first_pass,
        "--precheck-landing",
        "0",
        "--headless",
        str(args.headless),
        "--runtime-preset",
        args.runtime_preset,
        "--execution-env",
        args.execution_env,
        "--deep-retry-headless",
        str(args.headless),
        "--profile-mode",
        "auto",
        "--profile-name",
        str(args.profile_name),
    ]
    if profile_dir is not None:
        download_cmd.extend(["--persistent-profile-dir", str(profile_dir)])

    summarize_cmd = [
        sys.executable,
        str(repo_root() / "experiment" / "summarize_linux_headless_suite.py"),
        "--suite",
        args.suite,
        "--sample-csv",
        str(sample_csv),
        "--landing-jsonl",
        str(landing_jsonl),
        "--landing-report",
        str(landing_report),
        "--download-results-csv",
        str(download_results_csv),
        "--download-summary-json",
        str(download_summary_json),
        "--merged-csv",
        str(merged_csv),
        "--publisher-summary-csv",
        str(publisher_csv),
        "--summary-json",
        str(merged_json),
        "--summary-md",
        str(merged_md),
    ]

    shell_script_path = run_dir / "run_suite.sh"
    shell_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"cd {shlex.quote(str(repo_root()))}",
    ]
    if not args.skip_landing:
        shell_lines.append(shell_join(landing_cmd))
    if not args.skip_download:
        shell_lines.append(shell_join(download_cmd))
    shell_lines.append(shell_join(summarize_cmd))
    shell_script_path.write_text("\n".join(shell_lines) + "\n", encoding="utf-8")
    shell_script_path.chmod(0o755)

    host_name = socket.gethostname()
    git_branch = read_text_command(["git", "branch", "--show-current"], cwd=repo_root())
    git_commit = read_text_command(["git", "rev-parse", "HEAD"], cwd=repo_root())
    git_short_commit = read_text_command(["git", "rev-parse", "--short", "HEAD"], cwd=repo_root())
    execution_manifest: Dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "run_id": str(run_dir.name),
        "suite": args.suite,
        "suite_dir": str(suite_dir),
        "repo_root": str(repo_root()),
        "sample_csv": str(sample_csv),
        "run_dir": str(run_dir),
        "host": host_name,
        "user": getpass.getuser(),
        "platform": platform.platform(),
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "git_branch": git_branch,
        "git_commit": git_commit,
        "git_short_commit": git_short_commit,
        "runtime_preset": args.runtime_preset,
        "execution_env": args.execution_env,
        "headless": bool(args.headless),
        "skip_landing": bool(args.skip_landing),
        "skip_download": bool(args.skip_download),
        "landing_workers": int(args.landing_workers),
        "download_workers": int(args.download_workers),
        "after_first_pass": str(args.after_first_pass),
        "profile_name": str(args.profile_name),
        "persistent_profile_dir": str(profile_dir) if profile_dir is not None else "",
        "environment_overrides": {
            "CHROME_PATH": str(os.environ.get("CHROME_PATH", "")).strip(),
            "PDF_BROWSER_NO_SANDBOX": str(os.environ.get("PDF_BROWSER_NO_SANDBOX", "")).strip(),
        },
        "paths": {
            "run_dir": str(run_dir),
            "landing_dir": str(landing_dir),
            "download_dir": str(download_dir),
            "download_run_dir": str(download_run_dir),
            "summary_dir": str(summary_dir),
            "logs_dir": str(logs_dir),
            "landing_artifact_dir": str(landing_artifact_dir),
            "landing_fail_zip": str(landing_fail_zip),
            "landing_success_zip": str(landing_success_zip),
            "download_pdf_dir": str(download_pdf_dir),
            "download_results_csv": str(download_results_csv),
            "download_summary_json": str(download_summary_json),
            "merged_csv": str(merged_csv),
            "publisher_summary_csv": str(publisher_csv),
            "summary_json": str(merged_json),
            "summary_md": str(merged_md),
        },
        "prepared_commands": {
            "landing": landing_cmd,
            "download": download_cmd,
            "summarize": summarize_cmd,
        },
        "shell_script": str(shell_script_path),
        "seed_profile_check": None,
        "executed_commands": {},
        "status": "prepared",
    }

    seed_check_failed = False
    if args.runtime_preset == "linux_cli_seeded":
        if profile_dir is None:
            seed_check_failed = True
            execution_manifest["seed_profile_check"] = {
                "ok": False,
                "reason": "persistent_profile_dir_required_for_linux_cli_seeded",
            }
        else:
            seed_check_cmd = [
                sys.executable,
                str(repo_root() / "scripts" / "check_linux_seed_profile.py"),
                "--profile-root",
                str(profile_dir),
                "--profile-name",
                str(args.profile_name),
            ]
            execution_manifest["prepared_commands"]["seed_profile_check"] = seed_check_cmd
            if args.execute:
                seed_result = run_command(
                    seed_check_cmd,
                    cwd=repo_root(),
                    stdout_path=logs_dir / "seed_profile_check.stdout.log",
                    stderr_path=logs_dir / "seed_profile_check.stderr.log",
                )
                try:
                    seed_result["inspection"] = json.loads(Path(seed_result["stdout"]).read_text(encoding="utf-8"))
                except Exception:
                    seed_result["inspection"] = {}
                execution_manifest["seed_profile_check"] = seed_result
                seed_check_failed = not bool(seed_result.get("ok"))
            else:
                execution_manifest["seed_profile_check"] = {
                    "ok": None,
                    "cmd": seed_check_cmd,
                }

    if args.execute and not seed_check_failed:
        if not args.skip_landing:
            execution_manifest["executed_commands"]["landing"] = run_command(
                landing_cmd,
                cwd=repo_root(),
                stdout_path=logs_dir / "landing.stdout.log",
                stderr_path=logs_dir / "landing.stderr.log",
            )
        if not args.skip_download:
            execution_manifest["executed_commands"]["download"] = run_command(
                download_cmd,
                cwd=repo_root(),
                stdout_path=logs_dir / "download.stdout.log",
                stderr_path=logs_dir / "download.stderr.log",
            )
        if (
            landing_jsonl.exists()
            or landing_report.exists()
            or download_results_csv.exists()
            or download_summary_json.exists()
        ):
            execution_manifest["executed_commands"]["summarize"] = run_command(
                summarize_cmd,
                cwd=repo_root(),
                stdout_path=logs_dir / "summarize.stdout.log",
                stderr_path=logs_dir / "summarize.stderr.log",
            )
    elif args.execute and seed_check_failed:
        execution_manifest["executed_commands"]["landing"] = {
            "ok": False,
            "returncode": None,
            "skipped": True,
            "reason": "seed_profile_check_failed",
        }
        execution_manifest["executed_commands"]["download"] = {
            "ok": False,
            "returncode": None,
            "skipped": True,
            "reason": "seed_profile_check_failed",
        }
        execution_manifest["status"] = "blocked_seed_profile"

    if args.execute and execution_manifest.get("status") != "blocked_seed_profile":
        stage_results = [
            payload
            for payload in execution_manifest["executed_commands"].values()
            if isinstance(payload, dict) and "ok" in payload
        ]
        if stage_results and all(bool(payload.get("ok")) for payload in stage_results):
            execution_manifest["status"] = "completed_ok"
        elif stage_results:
            execution_manifest["status"] = "completed_with_failures"
        else:
            execution_manifest["status"] = "executed_no_stage_records"
    elif not args.execute:
        execution_manifest["status"] = "prepared_only"
    execution_manifest["finished_at"] = utc_now_iso()

    manifest_path = run_dir / "execution_manifest.json"
    write_json(manifest_path, execution_manifest)

    print(f"sample_csv={sample_csv}")
    print(f"run_dir={run_dir}")
    print(f"shell_script={shell_script_path}")
    print(f"execution_manifest={manifest_path}")
    if execution_manifest["seed_profile_check"] is not None:
        print(f"seed_profile_check={json.dumps(execution_manifest['seed_profile_check'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
