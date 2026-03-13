import argparse
import os
import sys
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Sequence, Set

CHROME_PATH = "/usr/bin/google-chrome"  # 리눅스 예시
# CHROME_PATH = "C:/Program Files/Google/Chrome/Application/chrome.exe" # 윈도우 예시

WILEY_API_KEY = "b4b01dd9-bf66-4a57-a791-0e7f3ff95a39"

DEFAULT_DOWNLOAD_DIR = "./downloaded_files"
DEFAULT_OUTPUT_DIR = "outputs/paper_download_run"
DEFAULT_DOWNLOAD_PUBLISHER_COOLDOWN_SEC = float(os.getenv("DOWNLOAD_PER_PUBLISHER_COOLDOWN_SEC", "7"))
DEFAULT_DOWNLOAD_GLOBAL_START_SPACING_SEC = float(os.getenv("DOWNLOAD_GLOBAL_START_SPACING_SEC", "1.5"))
DEFAULT_DOWNLOAD_JITTER_MIN_SEC = float(os.getenv("DOWNLOAD_JITTER_MIN_SEC", "0.7"))
DEFAULT_DOWNLOAD_JITTER_MAX_SEC = float(os.getenv("DOWNLOAD_JITTER_MAX_SEC", "1.8"))
DEFAULT_PROFILE_NAME = os.environ.get("PDF_BROWSER_PROFILE_NAME", "Default")
DEFAULT_PERSISTENT_PROFILE_DIR = os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", "outputs/.chrome_user_data")
DEFAULT_RUNTIME_PROFILE_ROOT = os.environ.get("PDF_BROWSER_RUNTIME_PROFILE_ROOT", "")
RUNTIME_PRESET_CHOICES = ("auto", "local_mac", "linux_cli_cold", "linux_cli_seeded")


@dataclass(frozen=True)
class RuntimeConfig:
    preset_name: str
    workflow: str
    execution_env: Optional[str] = None
    headless: Optional[int] = None
    deep_retry_headless: Optional[int] = None
    precheck_landing: Optional[int] = None
    abort_on_landing_block: Optional[int] = None
    profile_mode: Optional[str] = None
    profile_name: Optional[str] = None
    persistent_profile_dir: Optional[str] = None
    runtime_profile_root: Optional[str] = None
    workers: Optional[int] = None
    publisher_cooldown_sec: Optional[float] = None
    global_start_spacing_sec: Optional[float] = None
    jitter_min_sec: Optional[float] = None
    jitter_max_sec: Optional[float] = None
    no_sandbox: Optional[int] = None
    server_tuned: Optional[int] = None
    single_process: Optional[int] = None
    humanized_browser: Optional[int] = None
    assume_institution_access: Optional[int] = None

    def as_args_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload.pop("preset_name", None)
        payload.pop("workflow", None)
        return {key: value for key, value in payload.items() if value is not None}


def _resolve_auto_execution_env(execution_env_hint: str = "") -> str:
    raw = str(execution_env_hint or os.environ.get("PDF_BROWSER_EXECUTION_ENV", "auto")).strip().lower().replace("-", "_")
    aliases = {
        "local": "desktop",
        "local_desktop": "desktop",
        "mac": "desktop",
        "macos": "desktop",
        "server": "linux_cli",
        "linuxcli": "linux_cli",
        "hpc": "linux_cli",
        "slurm": "linux_cli",
    }
    normalized = aliases.get(raw, raw)
    if normalized in ("desktop", "linux_cli"):
        return normalized
    if sys.platform.startswith("linux"):
        if any(os.environ.get(name, "").strip() for name in ("DISPLAY", "WAYLAND_DISPLAY", "MIR_SOCKET")):
            return "desktop"
        return "linux_cli"
    return "desktop"


def resolve_runtime_preset_name(runtime_preset: str = "auto", execution_env_hint: str = "") -> str:
    raw = str(runtime_preset or os.environ.get("PDF_RUNTIME_PRESET", "auto")).strip().lower().replace("-", "_")
    aliases = {
        "": "auto",
        "desktop": "local_mac",
        "local": "local_mac",
        "local_desktop": "local_mac",
        "mac": "local_mac",
        "macos": "local_mac",
        "linux": "linux_cli_cold",
        "server": "linux_cli_cold",
        "seeded": "linux_cli_seeded",
    }
    normalized = aliases.get(raw, raw)
    if normalized != "auto":
        if normalized not in RUNTIME_PRESET_CHOICES:
            return "local_mac"
        return normalized
    resolved_env = _resolve_auto_execution_env(execution_env_hint)
    if resolved_env == "linux_cli":
        return "linux_cli_cold"
    return "local_mac"


def _runtime_config_for_download(preset_name: str) -> RuntimeConfig:
    common = {
        "workflow": "download",
        "profile_name": DEFAULT_PROFILE_NAME,
        "persistent_profile_dir": DEFAULT_PERSISTENT_PROFILE_DIR,
        "runtime_profile_root": DEFAULT_RUNTIME_PROFILE_ROOT,
        "publisher_cooldown_sec": DEFAULT_DOWNLOAD_PUBLISHER_COOLDOWN_SEC,
        "global_start_spacing_sec": DEFAULT_DOWNLOAD_GLOBAL_START_SPACING_SEC,
        "jitter_min_sec": DEFAULT_DOWNLOAD_JITTER_MIN_SEC,
        "jitter_max_sec": DEFAULT_DOWNLOAD_JITTER_MAX_SEC,
        "precheck_landing": 0,
        "abort_on_landing_block": 1,
    }
    if preset_name == "linux_cli_cold":
        return RuntimeConfig(
            preset_name=preset_name,
            execution_env="linux_cli",
            headless=1,
            deep_retry_headless=1,
            profile_mode="temp",
            **common,
        )
    if preset_name == "linux_cli_seeded":
        return RuntimeConfig(
            preset_name=preset_name,
            execution_env="linux_cli",
            headless=1,
            deep_retry_headless=1,
            profile_mode="auto",
            **common,
        )
    return RuntimeConfig(
        preset_name="local_mac",
        execution_env="desktop",
        headless=0,
        deep_retry_headless=0,
        profile_mode="auto",
        **common,
    )


def _runtime_config_for_landing(preset_name: str) -> RuntimeConfig:
    common = {
        "workflow": "landing",
        "workers": 1,
        "publisher_cooldown_sec": DEFAULT_DOWNLOAD_PUBLISHER_COOLDOWN_SEC,
        "global_start_spacing_sec": DEFAULT_DOWNLOAD_GLOBAL_START_SPACING_SEC,
        "jitter_min_sec": DEFAULT_DOWNLOAD_JITTER_MIN_SEC,
        "jitter_max_sec": DEFAULT_DOWNLOAD_JITTER_MAX_SEC,
        "profile_name": DEFAULT_PROFILE_NAME,
        "persistent_profile_dir": DEFAULT_PERSISTENT_PROFILE_DIR,
        "no_sandbox": 1,
        "server_tuned": 1,
        "single_process": 0,
        "humanized_browser": 1,
        "assume_institution_access": 1,
    }
    if preset_name == "linux_cli_cold":
        return RuntimeConfig(
            preset_name=preset_name,
            execution_env="linux_cli",
            headless=1,
            profile_mode="temp",
            **common,
        )
    if preset_name == "linux_cli_seeded":
        return RuntimeConfig(
            preset_name=preset_name,
            execution_env="linux_cli",
            headless=1,
            profile_mode="auto",
            **common,
        )
    return RuntimeConfig(
        preset_name="local_mac",
        execution_env="desktop",
        headless=0,
        profile_mode="auto",
        **common,
    )


def build_runtime_config(runtime_preset: str, workflow: str, execution_env_hint: str = "") -> RuntimeConfig:
    resolved_preset = resolve_runtime_preset_name(runtime_preset, execution_env_hint=execution_env_hint)
    workflow_key = str(workflow or "").strip().lower()
    if workflow_key == "landing":
        return _runtime_config_for_landing(resolved_preset)
    return _runtime_config_for_download(resolved_preset)


def _collect_cli_override_keys(argv: Optional[Sequence[str]] = None) -> Set[str]:
    tokens = list(sys.argv[1:] if argv is None else argv)
    overrides: Set[str] = set()
    for token in tokens:
        if token == "--":
            break
        if not token.startswith("--"):
            continue
        option = token[2:]
        if not option:
            continue
        name = option.split("=", 1)[0].strip().replace("-", "_")
        if name:
            overrides.add(name)
    return overrides


def apply_runtime_preset(args, workflow: str, argv: Optional[Sequence[str]] = None) -> RuntimeConfig:
    override_keys = _collect_cli_override_keys(argv)
    requested_preset = str(getattr(args, "runtime_preset", "auto") or "auto").strip().lower()
    execution_env_hint = str(getattr(args, "execution_env", "auto") or "auto").strip()
    runtime_config = build_runtime_config(
        runtime_preset=requested_preset,
        workflow=workflow,
        execution_env_hint=execution_env_hint,
    )
    for key, value in runtime_config.as_args_dict().items():
        if not hasattr(args, key):
            continue
        if key in override_keys:
            continue
        setattr(args, key, value)

    setattr(args, "runtime_preset_requested", requested_preset or "auto")
    setattr(args, "runtime_preset_resolved", runtime_config.preset_name)
    return runtime_config


def add_runtime_preset_argument(parser: argparse.ArgumentParser, default: str = "auto") -> None:
    parser.add_argument(
        "--runtime-preset",
        type=str,
        default=os.environ.get("PDF_RUNTIME_PRESET", default),
        choices=RUNTIME_PRESET_CHOICES,
        help=(
            "실행 환경 preset. "
            "local_mac=desktop/headful 기본, "
            "linux_cli_cold=headless+temp profile, "
            "linux_cli_seeded=headless+stateful(auto) profile"
        ),
    )


def get_config():
    parser = argparse.ArgumentParser(description="OpenAlex Paper Downloader with DrissionPage")
    add_runtime_preset_argument(parser)

    # 검색 및 다운로드 설정
    parser.add_argument("--query", type=str, default=None,
                        help="검색 쿼리 (기본값: None -> 코드 내 기본 쿼리 사용)")
    
    parser.add_argument("--max_num", type=int, default=1000,
                        help="최대 다운로드 논문 수 (기본값: 1000)")
    
    parser.add_argument("--citation_percentile", type=float, default=0.99,
                        help="인용 상위 퍼센트 필터 (기본값: 0.99)")
    
    # 시스템 설정
    parser.add_argument("--max_workers", type=int, default=1,
                        help="병렬 다운로드 프로세스 수 (기본값: 1)")
    
    parser.add_argument("--output_dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"런 산출물 저장 경로. 상대 경로는 outputs/ 아래로 정리됨 (기본값: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument(
        "--pdf_output_dir",
        type=str,
        default=None,
        help="PDF 저장 루트. 미지정 시 pdfs/<run_name> 사용",
    )
    
    # 외부 doi list import
    parser.add_argument("--doi_path", type=str, default = None,
                        help ="doi리스트 경로")

    parser.add_argument(
        "--after-first-pass",
        type=str,
        choices=["stop", "deep"],
        default="stop",
        help="1차 패스 후 동작: stop(종료) 또는 deep(실패 건 심화 재시도). 기본값: stop",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="입력 프롬프트 없이 --after-first-pass 값으로 실행",
    )
    parser.add_argument(
        "--precheck-landing",
        type=int,
        default=0,
        choices=[0, 1],
        help="다운로드 전 landing_access_repro.py로 랜딩 성공 여부를 먼저 확인하고, 성공 DOI만 다운로드에 투입",
    )
    parser.add_argument(
        "--headless",
        type=int,
        default=None,
        choices=[0, 1],
        help="브라우저 다운로드 1차 패스 headless 모드. 미지정 시 PDF_BROWSER_HEADLESS 환경변수를 따른다.",
    )
    parser.add_argument(
        "--execution-env",
        type=str,
        default=os.environ.get("PDF_BROWSER_EXECUTION_ENV", "auto"),
        choices=["auto", "desktop", "linux_cli"],
        help="브라우저 실행 환경. linux_cli면 headful 요청을 무시하고 headless만 사용합니다.",
    )
    parser.add_argument(
        "--deep-retry-headless",
        type=int,
        default=None,
        choices=[0, 1],
        help="deep retry 브라우저 모드. 미지정 시 --headless 값을 따른다.",
    )
    parser.add_argument(
        "--abort-on-landing-block",
        type=int,
        default=1,
        choices=[0, 1],
        help="다운로드 전 landing 단계에서 captcha/challenge/block를 감지하면 즉시 중단할지 여부. 기본값: 1",
    )
    parser.add_argument(
        "--publisher-cooldown-sec",
        type=float,
        default=DEFAULT_DOWNLOAD_PUBLISHER_COOLDOWN_SEC,
        help=f"같은 publisher 재시작 전 최소 간격(초). 기본값: {DEFAULT_DOWNLOAD_PUBLISHER_COOLDOWN_SEC}",
    )
    parser.add_argument(
        "--global-start-spacing-sec",
        type=float,
        default=DEFAULT_DOWNLOAD_GLOBAL_START_SPACING_SEC,
        help=f"전체 DOI 시작 간 최소 간격(초). 기본값: {DEFAULT_DOWNLOAD_GLOBAL_START_SPACING_SEC}",
    )
    parser.add_argument(
        "--jitter-min-sec",
        type=float,
        default=DEFAULT_DOWNLOAD_JITTER_MIN_SEC,
        help=f"publisher pacing jitter 최소값(초). 기본값: {DEFAULT_DOWNLOAD_JITTER_MIN_SEC}",
    )
    parser.add_argument(
        "--jitter-max-sec",
        type=float,
        default=DEFAULT_DOWNLOAD_JITTER_MAX_SEC,
        help=f"publisher pacing jitter 최대값(초). 기본값: {DEFAULT_DOWNLOAD_JITTER_MAX_SEC}",
    )
    parser.add_argument(
        "--profile-mode",
        type=str,
        default=os.environ.get("PDF_BROWSER_PROFILE_MODE", "auto"),
        choices=["auto", "temp", "persistent", "system"],
        help="브라우저 세션 전략. auto는 고마찰 DOI에서만 stateful 프로필을 사용합니다.",
    )
    parser.add_argument(
        "--profile-name",
        type=str,
        default=os.environ.get("PDF_BROWSER_PROFILE_NAME", "Default"),
        help="재사용할 Chrome 프로필 이름",
    )
    parser.add_argument(
        "--persistent-profile-dir",
        type=str,
        default=os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", "outputs/.chrome_user_data"),
        help="시스템 프로필이 없을 때 사용할 지속 프로필 루트",
    )
    parser.add_argument(
        "--runtime-profile-root",
        type=str,
        default=os.environ.get("PDF_BROWSER_RUNTIME_PROFILE_ROOT", ""),
        help="다운로드 실행 중 사용할 런타임 프로필 루트. 미지정 시 SLURM_TMPDIR 또는 /tmp/$USER 아래를 사용",
    )

    args = parser.parse_args()
    return args
