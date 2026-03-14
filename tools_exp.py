import os
import re
import signal
import sys
import time
import shutil
import logging
import requests
import base64
import random
import json
from html import unescape as html_unescape

from typing import Any, Dict, Optional, Set
from urllib.parse import parse_qs, urljoin, quote, urlencode, urlparse, urlunparse
from seleniumbase import Driver
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests # 이름 충돌 방지
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Keys
from config import WILEY_API_KEY
from pdf_pipeline import (
    REASON_FAIL_HTTP_STATUS,
    REASON_FAIL_REDIRECT_LOOP,
    REASON_FAIL_TIMEOUT_NETWORK,
    REASON_FAIL_VIEWER_HTML,
    REASON_FAIL_WRONG_MIME,
    append_metrics_jsonl,
    download_pdf,
)
# from CloudflareBypasser import CloudflareBypasser

DEFAULT_DOWNLOAD_PATH = os.path.abspath("./downloaded_files")

# Browser profile tuned to look closer to a normal desktop session.
# NOTE:
# - --lang must be locale list without q-values.
# - HTTP Accept-Language can contain q-values.
BEST_BROWSER_LANG = "en-US"
BEST_BROWSER_LANG_PREF = "en-US,en"
BEST_BROWSER_ACCEPT_LANGUAGE = "en-US,en;q=0.9"
BEST_BROWSER_UA_MAC = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
BEST_BROWSER_UA_LINUX = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
# Backward-compat constant. Runtime selection is done by _resolve_best_browser_ua().
BEST_BROWSER_UA = BEST_BROWSER_UA_LINUX
BEST_BROWSER_WINDOW = "1728,1117"
MAX_ACTION_WAIT_S = int(os.getenv("PDF_ACTION_MAX_WAIT_S", "60"))
HIGH_FRICTION_DOMAINS = (
    "acs.org",
    "sciencedirect.com",
    "aip.org",
    "iop.org",
    "journals.aps.org",
    "wiley.com",
    "rsc.org",
    "mdpi.com",
    "tandfonline.com",
    "ieeexplore.ieee.org",
)
AUTO_PROFILE_DOI_PREFIXES = (
    "10.1016",  # Elsevier
    "10.1063",  # AIP
    "10.1116",  # AVS(AIP platform)
    "10.1088",  # IOP
    "10.1149",  # ECS legacy content hosted on IOP
    "10.7567",  # JJAP hosted on IOP
    "10.1103",  # APS
    "10.1002",  # Wiley
    "10.1039",  # RSC
    "10.3390",  # MDPI
)
SHARED_TEMP_PROFILE_BUCKETS = (
    ("10.1016", "elsevier"),
    ("10.1021", "acs"),
    ("10.1063", "aip"),
    ("10.1103", "aps"),
    ("10.1088", "iop"),
    ("10.1149", "iop"),
    ("10.7567", "iop"),
    ("10.1109", "ieee"),
    ("10.1117", "spie"),
    ("10.1002", "wiley"),
)
APS_JOURNAL_SLUGS = {
    "physreva": "pra",
    "physrevb": "prb",
    "physrevc": "prc",
    "physrevd": "prd",
    "physreve": "pre",
    "physrevx": "prx",
    "physrevlett": "prl",
    "physrevapplied": "prapplied",
    "physrevmaterials": "prmaterials",
    "physrevresearch": "prresearch",
    "physrevfluids": "prfluids",
    "physrevaccelbeams": "prab",
    "revmodphys": "rmp",
}
AIP_FIRST_PARTY_DOMAINS = ("pubs.aip.org", "avs.scitation.org", "aip.scitation.org")


class BrowserDisconnectedError(RuntimeError):
    """Raised when the underlying browser/page session is no longer usable."""


def _exc_message(exc) -> str:
    if exc is None:
        return ""
    if isinstance(exc, str):
        return exc
    try:
        return str(exc)
    except Exception:
        return repr(exc)


def _maybe_import_psutil():
    try:
        import psutil  # type: ignore

        return psutil
    except Exception:
        return None


def _other_download_runner_active(current_pid: Any = None) -> bool:
    psutil = _maybe_import_psutil()
    if psutil is None:
        return False
    try:
        current_pid_int = int(current_pid) if current_pid is not None else os.getpid()
    except Exception:
        current_pid_int = os.getpid()

    runner_markers = ("parallel_download.py", "landing_access_repro.py")
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            pid = int(proc.info.get("pid") or 0)
            if pid <= 0 or pid == current_pid_int:
                continue
            cmdline = proc.info.get("cmdline") or []
            cmd = " ".join(str(part) for part in cmdline if part)
            if any(marker in cmd for marker in runner_markers):
                return True
        except Exception:
            continue
    return False


def _process_exists(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except Exception:
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except OSError:
        return False


def _kill_process_tree(pid: Any, logger=None, reason: str = "") -> int:
    try:
        pid_int = int(pid)
    except Exception:
        return 0
    if pid_int <= 0 or pid_int == os.getpid():
        return 0

    psutil = _maybe_import_psutil()
    killed = 0
    if psutil is not None:
        try:
            proc = psutil.Process(pid_int)
        except Exception:
            return 0

        targets = []
        try:
            targets.extend(proc.children(recursive=True))
        except Exception:
            pass
        targets.append(proc)

        seen = set()
        ordered = []
        for target in reversed(targets):
            try:
                target_pid = int(target.pid)
            except Exception:
                continue
            if target_pid in seen or target_pid == os.getpid():
                continue
            seen.add(target_pid)
            ordered.append(target)

        for target in ordered:
            try:
                target.kill()
                killed += 1
            except Exception:
                pass

        if ordered:
            try:
                psutil.wait_procs(ordered, timeout=1.0)
            except Exception:
                pass
        if logger and killed:
            suffix = f" ({reason})" if reason else ""
            logger.info(f"     [Drission] 브라우저 프로세스 강제 종료: {killed}개 pid-tree 정리{suffix}")
        return killed

    try:
        os.kill(pid_int, signal.SIGKILL)
        killed = 1
    except Exception:
        killed = 0
    if logger and killed:
        suffix = f" ({reason})" if reason else ""
        logger.info(f"     [Drission] 브라우저 프로세스 강제 종료: pid={pid_int}{suffix}")
    return killed


def _is_drission_browser_root_command(cmd: str) -> bool:
    low = str(cmd or "").lower()
    if not low:
        return False
    if "--remote-debugging-port=" not in low:
        return False
    if "drissionpage/autop" not in low and "drissionpage/autoportdata" not in low:
        return False
    return "helper" not in low


def _kill_browser_processes_by_user_data_dir(user_data_dir: str, logger=None, only_orphans: bool = False) -> int:
    target_dir = os.path.abspath(str(user_data_dir or "").strip())
    if not target_dir:
        return 0

    psutil = _maybe_import_psutil()
    if psutil is None:
        return 0

    killed = 0
    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            cmd = " ".join(str(part) for part in cmdline if part)
            if not cmd:
                continue
            if target_dir not in cmd:
                continue
            if not _is_drission_browser_root_command(cmd):
                continue
            if only_orphans and int(proc.info.get("ppid") or 0) != 1:
                continue
            killed += _kill_process_tree(proc.info.get("pid"), logger=logger, reason=f"user-data-dir={target_dir}")
        except Exception:
            continue
    return killed


def _collect_browser_cleanup_hints(page, session_plan: Dict[str, Any] = None) -> Dict[str, Any]:
    hints = {
        "browser_pid": None,
        "user_data_dir": "",
    }
    if session_plan:
        hints["user_data_dir"] = str(session_plan.get("user_data_dir") or "").strip()

    browser = None
    try:
        browser = getattr(page, "browser", None)
    except Exception:
        browser = None

    if browser is not None:
        try:
            pid = getattr(browser, "process_id", None)
            if pid:
                hints["browser_pid"] = int(pid)
        except Exception:
            pass
        try:
            options = getattr(browser, "_chromium_options", None)
            user_data_dir = getattr(options, "user_data_path", None) if options is not None else None
            if user_data_dir:
                hints["user_data_dir"] = str(user_data_dir).strip()
        except Exception:
            pass

    return hints


def reap_stale_drission_orphan_browsers(logger=None, current_pid: Any = None, min_age_sec: float = None) -> int:
    psutil = _maybe_import_psutil()
    if psutil is None:
        return 0
    if _other_download_runner_active(current_pid=current_pid):
        if logger:
            logger.info("     [Drission] 다른 다운로드 runner가 활성 상태여서 stale browser 정리를 건너뜁니다.")
        return 0

    if min_age_sec is None:
        try:
            min_age_sec = float(os.getenv("PDF_STALE_BROWSER_MIN_AGE_SEC", "120"))
        except Exception:
            min_age_sec = 120.0
    min_age_sec = max(0.0, float(min_age_sec or 0.0))

    killed = 0
    now = time.time()
    for proc in psutil.process_iter(["pid", "ppid", "cmdline", "create_time"]):
        try:
            if int(proc.info.get("ppid") or 0) != 1:
                continue
            create_time = float(proc.info.get("create_time") or 0.0)
            if create_time and (now - create_time) < min_age_sec:
                continue
            cmdline = proc.info.get("cmdline") or []
            cmd = " ".join(str(part) for part in cmdline if part)
            if not _is_drission_browser_root_command(cmd):
                continue
            killed += _kill_process_tree(proc.info.get("pid"), logger=logger, reason="stale-orphan")
        except Exception:
            continue
    return killed


def _is_browser_disconnect_error(exc) -> bool:
    message = _exc_message(exc).strip().lower()
    if not message:
        return False
    needles = (
        "与页面的连接已断开",
        "connection to the page has been lost",
        "page disconnected",
        "browser disconnected",
        "disconnected from page",
        "disconnected from target",
        "target closed",
        "page crashed",
        "browser has been closed",
        "connection is closed",
        "connection closed",
        "not connected to devtools",
        "websocket is not open",
        "session closed",
        "target page, context or browser has been closed",
    )
    return any(token in message for token in needles)


def _raise_if_browser_disconnect(exc, logger=None, context: str = "") -> None:
    if not _is_browser_disconnect_error(exc):
        return
    message = _exc_message(exc).strip() or "browser_disconnected"
    if logger:
        prefix = f"{context}: " if context else ""
        logger.warning(f"     [Drission] {prefix}브라우저 연결 종료 감지 -> 세션 재생성")
    raise BrowserDisconnectedError(message)


def _resolve_best_browser_ua() -> str:
    override = os.getenv("PDF_BROWSER_UA", "").strip()
    if override:
        return override

    forced = os.getenv("PDF_BROWSER_UA_PLATFORM", "").strip().lower()
    if forced in ("mac", "macos", "darwin"):
        return BEST_BROWSER_UA_MAC
    if forced in ("linux", "server"):
        return BEST_BROWSER_UA_LINUX

    if sys.platform == "darwin":
        return BEST_BROWSER_UA_MAC
    return BEST_BROWSER_UA_LINUX


def _is_macos_app_browser_path(path: str) -> bool:
    normalized = str(path or "").strip().replace("\\", "/")
    if not normalized:
        return False
    return normalized.startswith("/Applications/") or ".app/Contents/MacOS/" in normalized


def _browser_path_allowed_for_execution_env(path: str, execution_env: str = "") -> bool:
    candidate = str(path or "").strip()
    if not candidate:
        return False
    if resolve_browser_execution_env(execution_env) != "linux_cli":
        return True
    return not _is_macos_app_browser_path(candidate)


def resolve_browser_executable(preferred_path: str = "", logger=None, execution_env: str = "") -> str:
    resolved_env = resolve_browser_execution_env(execution_env)
    candidates = []
    if preferred_path:
        candidates.append(preferred_path)
    env_path = str(os.environ.get("CHROME_PATH", "")).strip()
    if env_path:
        candidates.append(env_path)
    for name in ("google-chrome", "google-chrome-stable", "chromium-browser", "chromium", "chrome"):
        p = shutil.which(name)
        if p:
            candidates.append(p)
    candidates.extend(
        [
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/opt/google/chrome/chrome",
            "/usr/local/bin/chrome",
            "/home/yongyong0206/chrome-linux64/chrome",
        ]
    )
    if resolved_env != "linux_cli" and sys.platform == "darwin":
        candidates.append("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    seen = set()
    for path in candidates:
        p = str(path or "").strip()
        if not p or p in seen:
            continue
        seen.add(p)
        if not _browser_path_allowed_for_execution_env(p, resolved_env):
            if logger and _is_macos_app_browser_path(p):
                logger.info(f"     [Drission] linux_cli 환경이므로 macOS Chrome 경로 무시: {p}")
            continue
        if os.path.isfile(p) and os.access(p, os.X_OK):
            if logger:
                logger.info(f"     [Drission] 브라우저 실행 파일: {p}")
            return p
    return ""


def _find_system_chrome_user_data_dir(profile_name: str = "Default", execution_env: str = "") -> str:
    linux_candidates = [
        os.path.expanduser("~/.config/google-chrome"),
        os.path.expanduser("~/.config/google-chrome-beta"),
        os.path.expanduser("~/.config/chromium"),
    ]
    if resolve_browser_execution_env(execution_env) == "linux_cli":
        candidates = linux_candidates
    elif sys.platform == "darwin":
        candidates = [
            os.path.expanduser("~/Library/Application Support/Google/Chrome"),
            *linux_candidates,
        ]
    else:
        candidates = linux_candidates
    for base in candidates:
        if os.path.isdir(os.path.join(base, profile_name)):
            return base
    return ""


def _normalize_profile_mode(profile_mode: str = "") -> str:
    raw = str(profile_mode or os.getenv("PDF_BROWSER_PROFILE_MODE", "auto")).strip().lower()
    aliases = {
        "": "auto",
        "legacy": "auto",
        "default": "auto",
        "persist": "persistent",
        "persisted": "persistent",
        "stateful": "persistent",
        "ephemeral": "temp",
    }
    normalized = aliases.get(raw, raw)
    if normalized in ("auto", "temp", "persistent", "system"):
        return normalized
    return "auto"


def _normalize_execution_env(execution_env: str = "") -> str:
    raw = str(execution_env or os.getenv("PDF_BROWSER_EXECUTION_ENV", "auto")).strip().lower().replace("-", "_")
    aliases = {
        "": "auto",
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
    if normalized in ("auto", "desktop", "linux_cli"):
        return normalized
    return "auto"


def resolve_browser_execution_env(execution_env: str = "") -> str:
    normalized = _normalize_execution_env(execution_env)
    if normalized != "auto":
        return normalized
    if sys.platform.startswith("linux"):
        if any(os.environ.get(name, "").strip() for name in ("DISPLAY", "WAYLAND_DISPLAY", "MIR_SOCKET")):
            return "desktop"
        return "linux_cli"
    return "desktop"


def coerce_headless_for_execution_env(headless: bool, execution_env: str = "", logger=None, context: str = "browser") -> bool:
    requested = bool(headless)
    resolved_env = resolve_browser_execution_env(execution_env)
    if resolved_env == "linux_cli" and not requested:
        if logger:
            logger.info(f"     [Drission] {context}: linux_cli 환경이므로 headless=1로 강제")
        return True
    return requested


def _stateful_profile_requested(doi_url: str, profile_mode: str = "") -> bool:
    normalized_mode = _normalize_profile_mode(profile_mode)
    if normalized_mode == "temp":
        return False
    if normalized_mode in ("persistent", "system"):
        return True
    doi_norm = _doi_from_doi_url(doi_url)
    return doi_norm.startswith(AUTO_PROFILE_DOI_PREFIXES)


def _landing_stateful_profile_isolation_enabled() -> bool:
    raw = os.getenv("PDF_BROWSER_LANDING_ISOLATE_STATEFUL", "1").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _resolve_stateful_profile_source(profile_name: str, profile_mode: str, session_seed_root: str = "") -> tuple[str, str]:
    profile_name = str(profile_name or "Default").strip() or "Default"
    normalized_mode = _normalize_profile_mode(profile_mode)
    seed_root = os.path.abspath(str(session_seed_root or "").strip()) if str(session_seed_root or "").strip() else ""
    persistent_dir = os.getenv(
        "PDF_BROWSER_PERSISTENT_PROFILE_DIR",
        os.path.abspath(os.path.join("outputs", ".chrome_user_data")),
    ).strip() or os.path.abspath(os.path.join("outputs", ".chrome_user_data"))
    persistent_dir = os.path.abspath(persistent_dir)

    if seed_root and os.path.isdir(os.path.join(seed_root, profile_name)):
        return seed_root, "precheck_seed"

    if normalized_mode == "persistent":
        os.makedirs(persistent_dir, exist_ok=True)
        return persistent_dir, "persistent"

    system_dir = _find_system_chrome_user_data_dir(profile_name)
    if system_dir:
        return system_dir, "system"

    os.makedirs(persistent_dir, exist_ok=True)
    return persistent_dir, "persistent_fallback"


def _seed_profile_root_for_runtime(source_root: str, target_root: str, profile_name: str, logger=None) -> str:
    source_root = os.path.abspath(str(source_root or "").strip())
    target_root = os.path.abspath(str(target_root or "").strip())
    profile_name = str(profile_name or "Default").strip() or "Default"
    marker_path = os.path.join(target_root, ".codex_profile_seed_ready")
    target_profile_dir = os.path.join(target_root, profile_name)
    if os.path.isfile(marker_path) and os.path.isdir(target_profile_dir):
        return target_root

    if os.path.isdir(target_root):
        shutil.rmtree(target_root, ignore_errors=True)
    os.makedirs(target_root, exist_ok=True)

    for top_name in ("Local State", "First Run", "Last Version"):
        src_path = os.path.join(source_root, top_name)
        dst_path = os.path.join(target_root, top_name)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)

    src_profile_dir = os.path.join(source_root, profile_name)
    os.makedirs(target_profile_dir, exist_ok=True)
    if os.path.isdir(src_profile_dir):
        shutil.copytree(
            src_profile_dir,
            target_profile_dir,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns(
                "Singleton*",
                "LOCK",
                "lockfile",
                "Crashpad",
                "BrowserMetrics",
                "Code Cache",
                "GPUCache",
                "GrShaderCache",
                "ShaderCache",
                "DawnCache",
                "optimization_guide_model_store",
                "component_crx_cache",
                "segmentation_platform",
            ),
        )
    with open(marker_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"profile_name": profile_name, "source_root": source_root}, ensure_ascii=False))
    if logger:
        logger.info(f"     [Drission] 런타임 프로필 시드 완료: {target_root}/{profile_name}")
    return target_root


def build_landing_browser_session_plan(doi_url: str, worker_profile_root: str, worker_idx: int, logger=None) -> Dict[str, Any]:
    worker_profile_root = os.path.abspath(str(worker_profile_root or "").strip() or os.path.join("outputs", ".landing_profiles"))
    os.makedirs(worker_profile_root, exist_ok=True)

    profile_mode = _normalize_profile_mode()
    profile_name = os.getenv("PDF_BROWSER_PROFILE_NAME", "Default").strip() or "Default"
    temp_profile_name = f"worker_{int(worker_idx)}"
    temp_user_data_dir = os.path.join(worker_profile_root, temp_profile_name)

    plan: Dict[str, Any] = {
        "session_mode": "temp",
        "session_source": "temp",
        "profile_mode": profile_mode,
        "profile_name": temp_profile_name,
        "user_data_dir": temp_user_data_dir,
        "cache_key": "temp",
        "browser_identity": f"worker_{int(worker_idx)}:temp",
    }

    if not _stateful_profile_requested(doi_url, profile_mode):
        os.makedirs(temp_user_data_dir, exist_ok=True)
        return plan

    stateful_source, stateful_source_kind = _resolve_stateful_profile_source(profile_name, profile_mode)

    actual_user_data_dir = stateful_source
    actual_source_kind = stateful_source_kind
    if _landing_stateful_profile_isolation_enabled():
        isolated_root = os.path.join(worker_profile_root, f"stateful_worker_{int(worker_idx)}")
        try:
            actual_user_data_dir = _seed_profile_root_for_runtime(
                stateful_source,
                isolated_root,
                profile_name,
                logger=logger,
            )
            actual_source_kind = f"{stateful_source_kind}_clone"
        except Exception as exc:
            if logger:
                logger.warning(f"     [Drission] stateful 프로필 시드 실패, 원본 직접 사용: {exc}")

    return {
        "session_mode": "stateful",
        "session_source": actual_source_kind,
        "profile_mode": profile_mode,
        "profile_name": profile_name,
        "user_data_dir": actual_user_data_dir,
        "cache_key": f"stateful:{stateful_source_kind}",
        "browser_identity": f"worker_{int(worker_idx)}:stateful:{stateful_source_kind}",
    }


def _default_runtime_profile_root() -> str:
    explicit = os.getenv("PDF_BROWSER_RUNTIME_PROFILE_ROOT", "").strip()
    if explicit:
        return os.path.abspath(explicit)
    run_base = os.environ.get("SLURM_TMPDIR", "").strip() or os.path.join("/tmp", os.environ.get("USER", "user"))
    return os.path.abspath(os.path.join(run_base, "download_runtime_profiles"))


def _download_stateful_profile_isolation_enabled() -> bool:
    raw = os.getenv("PDF_BROWSER_DOWNLOAD_ISOLATE_STATEFUL", "1").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _shared_temp_profile_bucket_for_doi(doi_url: str) -> str:
    doi_norm = _doi_from_doi_url(doi_url)
    for prefix, bucket in SHARED_TEMP_PROFILE_BUCKETS:
        if doi_norm.startswith(prefix):
            return bucket
    return ""


def build_download_browser_session_plan(
    doi_url: str,
    runtime_profile_root: str = "",
    worker_label: str = "",
    logger=None,
) -> Dict[str, Any]:
    runtime_root = os.path.abspath(str(runtime_profile_root or _default_runtime_profile_root()).strip())
    os.makedirs(runtime_root, exist_ok=True)

    profile_mode = _normalize_profile_mode()
    profile_name = os.getenv("PDF_BROWSER_PROFILE_NAME", "Default").strip() or "Default"
    session_seed_root = os.getenv("PDF_BROWSER_SESSION_SEED_ROOT", "").strip()
    doi_norm = _doi_from_doi_url(doi_url)
    doi_key = _sanitize_doi_to_filename(doi_norm) or "unknown_doi"
    worker_key = _sanitize_doi_to_filename(worker_label or f"pid_{os.getpid()}") or f"pid_{os.getpid()}"

    if not _stateful_profile_requested(doi_url, profile_mode):
        shared_bucket = _shared_temp_profile_bucket_for_doi(doi_url)
        if shared_bucket:
            temp_root = os.path.join(runtime_root, worker_key, "temp_shared", shared_bucket)
            cleanup_on_close = False
            cleanup_dir = ""
            session_source = f"temp_shared:{shared_bucket}"
            cache_key = f"temp_shared:{worker_key}:{shared_bucket}"
            browser_identity = f"{worker_key}:temp_shared:{shared_bucket}"
        else:
            temp_root = os.path.join(runtime_root, worker_key, "temp", doi_key)
            cleanup_on_close = True
            cleanup_dir = temp_root
            session_source = "temp"
            cache_key = f"temp:{worker_key}:{doi_key}"
            browser_identity = f"{worker_key}:temp:{doi_key}"
        os.makedirs(temp_root, exist_ok=True)
        return {
            "session_mode": "temp",
            "session_source": session_source,
            "profile_mode": profile_mode,
            "profile_name": "Default",
            "user_data_dir": temp_root,
            "cache_key": cache_key,
            "browser_identity": browser_identity,
            "cleanup_on_close": cleanup_on_close,
            "cleanup_dir": cleanup_dir,
        }

    stateful_source, stateful_source_kind = _resolve_stateful_profile_source(
        profile_name,
        profile_mode,
        session_seed_root=session_seed_root,
    )

    actual_user_data_dir = stateful_source
    actual_source_kind = stateful_source_kind
    if _download_stateful_profile_isolation_enabled():
        isolated_root = os.path.join(runtime_root, worker_key, f"stateful_{stateful_source_kind}")
        try:
            actual_user_data_dir = _seed_profile_root_for_runtime(
                stateful_source,
                isolated_root,
                profile_name,
                logger=logger,
            )
            actual_source_kind = f"{stateful_source_kind}_clone"
        except Exception as exc:
            if logger:
                logger.warning(f"     [Drission] download stateful 프로필 시드 실패, 원본 직접 사용: {exc}")

    return {
        "session_mode": "stateful",
        "session_source": actual_source_kind,
        "profile_mode": profile_mode,
        "profile_name": profile_name,
        "user_data_dir": actual_user_data_dir,
        "cache_key": f"stateful:{worker_key}:{stateful_source_kind}",
        "browser_identity": f"{worker_key}:stateful:{stateful_source_kind}",
        "cleanup_on_close": False,
        "cleanup_dir": "",
    }


def _apply_browser_session_plan(co: ChromiumOptions, session_plan: Dict[str, Any], logger=None) -> bool:
    if not session_plan:
        return False
    user_data_dir = os.path.abspath(str(session_plan.get("user_data_dir") or "").strip())
    profile_name = str(session_plan.get("profile_name") or "").strip()
    if not user_data_dir or not profile_name:
        return False
    os.makedirs(user_data_dir, exist_ok=True)
    co.set_user_data_path(user_data_dir)
    co.set_user(profile_name)
    if logger:
        logger.info(
            "     [Drission] 브라우저 세션 적용: "
            f"mode={session_plan.get('session_mode')} source={session_plan.get('session_source')} "
            f"profile={profile_name} root={user_data_dir}"
        )
    return True


def _cleanup_browser_session_plan(session_plan: Dict[str, Any], logger=None) -> None:
    if not session_plan or not bool(session_plan.get("cleanup_on_close")):
        return
    cleanup_dir = os.path.abspath(str(session_plan.get("cleanup_dir") or session_plan.get("user_data_dir") or "").strip())
    if not cleanup_dir or not os.path.isdir(cleanup_dir):
        return
    try:
        shutil.rmtree(cleanup_dir, ignore_errors=True)
    except Exception as exc:
        if logger:
            logger.warning(f"     [Drission] 세션 정리 실패(무시): {exc}")


def _maybe_apply_system_chrome_profile(co: ChromiumOptions, doi_url: str, logger=None) -> bool:
    profile_mode = os.getenv("PDF_BROWSER_PROFILE_MODE", "auto").strip().lower()
    profile_name = os.getenv("PDF_BROWSER_PROFILE_NAME", "Default").strip() or "Default"
    doi_norm = _doi_from_doi_url(doi_url)
    fallback_dir = os.getenv(
        "PDF_BROWSER_PERSISTENT_PROFILE_DIR",
        os.path.abspath(os.path.join("outputs", ".chrome_user_data")),
    ).strip() or os.path.abspath(os.path.join("outputs", ".chrome_user_data"))

    if profile_mode == "temp":
        return False
    if profile_mode == "auto" and not doi_norm.startswith(AUTO_PROFILE_DOI_PREFIXES):
        return False

    user_data_dir = _find_system_chrome_user_data_dir(profile_name)
    if not user_data_dir:
        try:
            os.makedirs(fallback_dir, exist_ok=True)
            co.set_user_data_path(fallback_dir)
            co.set_user(profile_name)
            if logger:
                logger.info(f"     [Drission] 로컬 지속 프로필 사용: {fallback_dir}/{profile_name}")
            return True
        except Exception:
            return False

    try:
        co.set_user_data_path(user_data_dir)
        co.set_user(profile_name)
        if logger:
            logger.info(f"     [Drission] 시스템 Chrome 프로필 사용: {profile_name}")
        return True
    except Exception as e:
        if logger:
            logger.info(f"     [Drission] 시스템 Chrome 프로필 적용 실패(임시 프로필 유지): {e}")
        return False


def _apply_best_browser_profile(co: ChromiumOptions) -> None:
    execution_env = resolve_browser_execution_env()
    headless = os.getenv("PDF_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes")
    headless = coerce_headless_for_execution_env(headless, execution_env, context="browser_profile")
    no_sandbox = os.getenv("PDF_BROWSER_NO_SANDBOX", "0").strip().lower() in ("1", "true", "yes")
    server_tuned = os.getenv("PDF_BROWSER_SERVER_TUNED", "0").strip().lower() in ("1", "true", "yes")
    single_process = os.getenv("PDF_BROWSER_SINGLE_PROCESS", "0").strip().lower() in ("1", "true", "yes")
    humanized = os.getenv("PDF_BROWSER_HUMANIZED", "1").strip().lower() in ("1", "true", "yes")

    if headless:
        co.set_argument("--headless=new")
        co.set_argument("--disable-gpu")
    co.no_imgs(False)
    co.mute(False)
    co.set_argument("--disable-blink-features=AutomationControlled")
    co.set_argument("--disable-infobars")
    co.set_argument("--no-first-run")
    co.set_argument("--no-default-browser-check")
    co.set_argument(f"--window-size={BEST_BROWSER_WINDOW}")
    co.set_argument(f"--lang={BEST_BROWSER_LANG}")
    co.set_pref("intl.accept_languages", BEST_BROWSER_LANG_PREF)
    co.set_pref("credentials_enable_service", False)
    co.set_pref("profile.password_manager_enabled", False)
    co.set_argument("--start-maximized")
    co.set_argument("--password-store=basic")
    co.set_argument("--use-mock-keychain")
    if server_tuned:
        # 지문 일관성을 우선한다. 강한 disable 플래그는 opt-out일 때만 사용.
        co.set_argument("--disable-dev-shm-usage")
        if not humanized:
            co.set_argument("--disable-background-networking")
            co.set_argument("--disable-component-update")
            co.set_argument("--disable-domain-reliability")
            co.set_argument("--metrics-recording-only")
            co.set_argument("--disable-sync")
            co.set_argument("--disable-features=MediaRouter,OptimizationHints")
    if single_process:
        co.set_argument("--single-process")
        co.set_argument("--no-zygote")
    if no_sandbox:
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-dev-shm-usage")
    co.set_user_agent(_resolve_best_browser_ua())
    try:
        co.set_load_mode("eager")
    except Exception:
        pass


def _abort_on_landing_block() -> bool:
    return os.getenv("PDF_ABORT_ON_LANDING_BLOCK", "1").strip().lower() in ("1", "true", "yes", "on")
# =======================================================
# Logger
# =======================================================
def setup_logger(save_dir: str, filename: str) -> logging.Logger:
    filename = _sanitize_doi_to_filename(filename) if filename else "unknown"
    log_dir = os.path.join(save_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"download_log_{filename}.txt"

    logger = logging.getLogger("Paper_PDF_Downloader")
    logger.setLevel(logging.DEBUG)
    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(os.path.join(log_dir, log_filename), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(">> %(message)s"))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

# =======================================================
# Utilities
# =======================================================
def _sanitize_doi_to_filename(doi_url: str) -> str:
    clean = doi_url.strip().replace("https://doi.org/", "").replace("http://doi.org/", "")
    return clean.strip("/").replace("/", "_").replace(":", "-") + ".pdf"

def _get_current_files(download_dir: str) -> Set[str]:
    if not os.path.exists(download_dir): return set()
    return set(os.listdir(download_dir))


def _eles_quick(page, locator: str, timeout: float = 0.6):
    try:
        return page.eles(locator, timeout=timeout)
    except TypeError:
        try:
            return page.eles(locator)
        except Exception:
            return []
    except Exception:
        return []


def _ele_quick(page, locator: str, timeout: float = 0.6):
    try:
        return page.ele(locator, timeout=timeout)
    except TypeError:
        try:
            return page.ele(locator)
        except Exception:
            return None
    except Exception:
        return None


def _is_valid_pdf(file_path: str) -> bool:
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 1000:
            return False
        with open(file_path, 'rb') as f:
            header = f.read(4)
            return header.startswith(b'%PDF')
    except: return False

def _wait_for_new_file_diff(download_dir: str, initial_files: Set[str], timeout_s: int = 30, logger = None):
    timeout_s = max(1, min(int(timeout_s), MAX_ACTION_WAIT_S))
    if logger:
        logger.info(f"     파일 감지 및 유효성 검사 (최대 {timeout_s}초)...")
    t0 = time.time()
    while (time.time() - t0) < timeout_s:
        try:
            current_files = _get_current_files(download_dir)
            new_items = current_files - initial_files
            if not new_items:
                time.sleep(0.25)
                continue
            
            valid_pdfs = [f for f in new_items if f.lower().endswith(".pdf")]
            for pdf in valid_pdfs:
                full_path = os.path.join(download_dir, pdf)
                if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                    prev_size = -1
                    stable_count = 0
                    for _ in range(3):
                        curr = os.path.getsize(full_path)
                        if curr == prev_size: stable_count += 1
                        else: stable_count = 0
                        prev_size = curr
                        if stable_count >= 2:
                            if _is_valid_pdf(full_path):
                                if logger:
                                    logger.info(f"        정상 PDF 확인 완료 (크기: {curr} bytes): {pdf}")
                                return full_path
                            else:
                                pass
                        time.sleep(0.2)
            time.sleep(0.25)
        except Exception:
            time.sleep(0.25)
    if logger:
        logger.info("       파일 감지 타임아웃")
    return None

def _safe_screenshot(page, path: str, name: str, logger=None, full_page: bool = True):
    """
    DrissionPage의 get_screenshot 메서드를 사용하여 스크린샷을 저장합니다.
    path: 저장할 폴더 경로 (예: ./logs/screenshots)
    name: 파일명 (예: capture.png)
    """
    if page is None:
        return None
    try:
        # 폴더 생성
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        target_full_page = bool(full_page)
        try:
            current_url = str(getattr(page, "url", "") or "").lower()
        except Exception as e:
            _raise_if_browser_disconnect(e, logger=logger, context="screenshot-url-check")
            current_url = ""
        if target_full_page and any(
            token in current_url for token in ("sciencedirect.com", "linkinghub.elsevier.com")
        ):
            # Elsevier article pages are extremely tall; full-page capture has repeatedly
            # destabilized Chrome during active download flows on macOS.
            target_full_page = False

        # DrissionPage get_screenshot 호출
        saved_path = page.get_screenshot(path=path, name=name, full_page=target_full_page)
        
        if logger: 
            logger.info(f"  스크린샷 저장 성공: {saved_path}")
        return saved_path

    except Exception as e:
        if _is_browser_disconnect_error(e):
            if logger:
                logger.warning(f"  스크린샷 스킵: 브라우저 연결 종료 ({_exc_message(e)})")
            return None
        # 전체 페이지 캡처 실패 시 (메모리 부족, 무한 스크롤 등), 보이는 화면(Viewport)만 재시도
        try:
            if logger: 
                logger.warning(f"  전체 페이지 스크린샷 실패 ({e}), 보이는 화면만 캡처 시도...")
            
            # 파일명에 visible_ 접두사를 붙여 재시도
            retry_name = "visible_" + name
            page.get_screenshot(path=path, name=retry_name, full_page=False)
            return os.path.join(path, retry_name)
            
        except Exception as e2:
            if _is_browser_disconnect_error(e2) and logger:
                logger.warning(f"  스크린샷 재시도 스킵: 브라우저 연결 종료 ({_exc_message(e2)})")
            # 재시도마저 실패한 경우
            pass
            # if logger: logger.warning(f"  스크린샷 저장 최종 실패 : {e2}")
    return None


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _close_page_safely(page, logger=None, session_plan: Dict[str, Any] = None):
    if page is None:
        if session_plan:
            _kill_browser_processes_by_user_data_dir(
                str(session_plan.get("user_data_dir") or ""),
                logger=logger,
                only_orphans=False,
            )
        return

    hints = _collect_browser_cleanup_hints(page, session_plan=session_plan)
    browser_pid = hints.get("browser_pid")
    user_data_dir = str(hints.get("user_data_dir") or "").strip()
    close_errors = []

    try:
        quit_fn = getattr(page, "quit", None)
        if callable(quit_fn):
            quit_fn(timeout=3, force=True)
    except TypeError:
        try:
            quit_fn()
        except Exception as exc:
            close_errors.append(exc)
    except Exception as exc:
        close_errors.append(exc)

    if close_errors:
        try:
            browser = getattr(page, "browser", None)
            browser_quit = getattr(browser, "quit", None) if browser is not None else None
            if callable(browser_quit):
                browser_quit(timeout=3, force=True)
                close_errors.clear()
        except TypeError:
            try:
                browser_quit()
                close_errors.clear()
            except Exception as exc:
                close_errors.append(exc)
        except Exception as exc:
            close_errors.append(exc)

    if close_errors:
        try:
            close_fn = getattr(page, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception as exc:
            close_errors.append(exc)

    time.sleep(0.2)
    if browser_pid and _process_exists(browser_pid):
        _kill_process_tree(browser_pid, logger=logger, reason="post-quit-pid")
    if user_data_dir:
        _kill_browser_processes_by_user_data_dir(user_data_dir, logger=logger, only_orphans=False)

    if close_errors and logger:
        last_error = close_errors[-1]
        logger.warning(f"     [Drission] 브라우저 종료 fallback 후 정리: {_exc_message(last_error)}")


def _is_high_friction_domain(url_or_domain: str) -> bool:
    d = _extract_domain(url_or_domain) if "://" in str(url_or_domain) else str(url_or_domain or "").lower()
    return any(key in d for key in HIGH_FRICTION_DOMAINS)


def _finalize_downloaded_file(downloaded_path: str, target_path: str, logger=None) -> bool:
    if not downloaded_path or not os.path.exists(downloaded_path):
        return False
    if not _is_valid_pdf(downloaded_path):
        if logger:
            logger.warning(f"        유효하지 않은 PDF(스킵): {downloaded_path}")
        return False

    try:
        if os.path.abspath(downloaded_path) != os.path.abspath(target_path):
            if os.path.exists(target_path):
                os.remove(target_path)
            shutil.move(downloaded_path, target_path)
        return _is_valid_pdf(target_path)
    except Exception as e:
        if logger:
            logger.warning(f"        파일 정리 실패: {e}")
        return False


def _pick_valid_downloaded_pdf(
    download_dir: str,
    initial_files: Set[str] = None,
    tmp_target_path: str = "",
):
    if tmp_target_path and _is_valid_pdf(tmp_target_path):
        return tmp_target_path

    try:
        current_files = _get_current_files(download_dir)
    except Exception:
        return None

    candidate_groups = []
    if initial_files is not None:
        candidate_groups.append(current_files - initial_files)
    candidate_groups.append(current_files)

    for names in candidate_groups:
        pdf_candidates = []
        for name in names:
            if not str(name).lower().endswith(".pdf"):
                continue
            candidate = os.path.join(download_dir, name)
            if _is_valid_pdf(candidate):
                pdf_candidates.append(candidate)
        if pdf_candidates:
            pdf_candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return pdf_candidates[0]
    return None


def _capture_direct_downloaded_pdf(
    download_dir: str,
    initial_files: Set[str],
    tmp_target_path: str,
    final_target_path: str,
    logger=None,
    timeout_s: int = 6,
    context: str = "direct-download",
    downloaded_file_guard=None,
) -> bool:
    downloaded_path = _pick_valid_downloaded_pdf(
        download_dir=download_dir,
        initial_files=initial_files,
        tmp_target_path=tmp_target_path,
    )
    if not downloaded_path and timeout_s > 0:
        downloaded_path = _wait_for_new_file_diff(download_dir, initial_files, timeout_s=timeout_s, logger=logger)
    if not downloaded_path:
        downloaded_path = _pick_valid_downloaded_pdf(
            download_dir=download_dir,
            initial_files=initial_files,
            tmp_target_path=tmp_target_path,
        )

    if not downloaded_path:
        return False

    if downloaded_file_guard:
        try:
            if not downloaded_file_guard(downloaded_path):
                try:
                    os.remove(downloaded_path)
                except Exception as e:
                    _raise_if_browser_disconnect(e, logger=logger, context="unexpected-landing-retry")
                    pass
                if logger:
                    logger.info(f"        [{context}] 다운로드 파일이 대상 논문과 불일치하여 폐기")
                return False
        except Exception:
            return False

    if not _finalize_downloaded_file(downloaded_path, tmp_target_path, logger=logger):
        return False
    if not _finalize_downloaded_file(tmp_target_path, final_target_path, logger=logger):
        return False

    if logger:
        logger.info(f"        [{context}] DOI 직행 PDF 다운로드 성공")
    return True


def _finalize_downloadkit_result(
    download_result,
    tmp_target_path: str,
    final_target_path: str,
    logger=None,
    context: str = "downloadkit",
) -> bool:
    try:
        result, info = download_result
    except Exception:
        return False
    if result not in ("success", "skipped"):
        return False

    candidate_path = str(info or "").strip()
    if not candidate_path or not os.path.exists(candidate_path):
        return False
    if not _finalize_downloaded_file(candidate_path, tmp_target_path, logger=logger):
        return False
    if not _finalize_downloaded_file(tmp_target_path, final_target_path, logger=logger):
        return False
    if logger:
        logger.info(f"        [{context}] DownloadKit 결과를 최종 PDF로 확정")
    return True


def _is_supporting_info_blob(*parts) -> bool:
    blob = " ".join(str(part or "") for part in parts).lower()
    if not blob:
        return False
    markers = (
        "supporting information",
        "supporting info",
        "supplementary information",
        "supplementary info",
        "supplementary files",
        "electronic supplementary information",
        "supportinginformation",
        "suppinfo",
        "suppdata",
        "supplement",
    )
    return any(marker in blob for marker in markers)


def _is_rsc_article_pdf_url(url: str) -> bool:
    low = str(url or "").strip().lower()
    if not low:
        return False
    return ("pubs.rsc.org" in low and "/content/articlepdf/" in low and not _is_supporting_info_blob(low))


def _looks_like_pdf_link(url: str) -> bool:
    low = str(url or "").lower()
    if not low:
        return False
    if _is_supporting_info_blob(low):
        return False
    good_tokens = (
        ".pdf",
        "/doi/pdf",
        "/articlepdf",
        "/pdfft",
        "stamppdf/getpdf.jsp",
        "download=true",
        "article-pdf",
        "/server/api/core/bitstreams/",
        "/upload/pdf/",
        "/upload/article/",
    )
    bad_tokens = ("/proceedings", "/session", "/program", "/toc", "/contents")
    if any(b in low for b in bad_tokens):
        return False
    return any(g in low for g in good_tokens)


def _try_click_pdf_button_download(
    page,
    pdf_btn,
    save_dir: str,
    full_save_path: str,
    logger=None,
    wait_timeout_s: int = 25,
    return_page: bool = False,
):
    if page is None or pdf_btn is None:
        return (False, page) if return_page else False
    active_page = page
    try:
        try:
            btn_text = (pdf_btn.text or "").strip()
        except Exception:
            btn_text = ""
        try:
            btn_title = (pdf_btn.attr("title") or "").strip()
        except Exception:
            btn_title = ""
        try:
            btn_aria = (pdf_btn.attr("aria-label") or "").strip()
        except Exception:
            btn_aria = ""
        try:
            btn_href = (pdf_btn.attr("href") or "").strip()
        except Exception:
            btn_href = ""
        if _is_supporting_info_blob(btn_text, btn_title, btn_aria, btn_href):
            if logger:
                logger.warning("        [Drission] Supporting-information 버튼 후보를 클릭하지 않고 건너뜀")
            return (False, active_page) if return_page else False
        _dismiss_cookie_or_consent_banner(page, logger=logger)
        initial_files = _get_current_files(save_dir)
        try:
            before_tab_ids = set(getattr(page, "tab_ids", []) or [])
        except Exception:
            before_tab_ids = set()
        before_url = str(getattr(page, "url", "") or "")
        try:
            pdf_btn.click(by_js=True)
        except Exception:
            pdf_btn.click()
        time.sleep(0.9)

        try:
            after_tab_ids = set(getattr(page, "tab_ids", []) or [])
        except Exception:
            after_tab_ids = set()
        current_url = str(getattr(page, "url", "") or "")
        if (after_tab_ids - before_tab_ids) or (current_url and current_url != before_url):
            adopted = _adopt_latest_tab(page, logger=logger)
            if adopted is not None:
                active_page = adopted
                _dismiss_cookie_or_consent_banner(active_page, logger=logger)

        if _capture_direct_downloaded_pdf(
            download_dir=save_dir,
            initial_files=initial_files,
            tmp_target_path=full_save_path,
            final_target_path=full_save_path,
            logger=logger,
            timeout_s=wait_timeout_s,
            context="button-click-download",
        ):
            if logger:
                logger.info("        [Drission] 버튼 클릭 기반 다운로드 성공")
            return (True, active_page) if return_page else True

        click_candidates = _collect_pdf_candidate_urls_from_page(active_page, logger=logger)
        if _try_cookie_cffi_candidate_urls(
            active_page,
            click_candidates,
            full_save_path,
            logger=logger,
            timeout_s=min(max(8, wait_timeout_s), 18),
            context="button-click-candidate",
        ):
            if logger:
                logger.info("        [Drission] 버튼 클릭 후 후보 URL 직접 회수 성공")
            return (True, active_page) if return_page else True
    except Exception as e:
        if logger:
            logger.warning(f"        버튼 클릭 기반 다운로드 실패: {e}")
    return (False, active_page) if return_page else False


def _doi_from_doi_url(doi_url: str) -> str:
    raw = str(doi_url or "").strip()
    if "doi.org/" in raw:
        raw = raw.split("doi.org/", 1)[1]
    raw = raw.split("?", 1)[0].split("#", 1)[0].strip().lower()
    return raw


def _powdermat_entry_url(doi: str) -> str:
    doi_norm = _doi_from_doi_url(doi)
    if not doi_norm.startswith("10.4150/"):
        return ""
    return f"https://www.powdermat.org/journal/view.php?doi={quote(doi_norm, safe='/')}"


def _extract_powdermat_pdf_target_from_html(html: str) -> Dict[str, str]:
    raw = str(html or "")
    if not raw:
        return {}

    match = re.search(
        r"journal_download\(\s*['\"](pdf(?:_a)?)['\"]\s*,\s*['\"]\d+['\"]\s*,\s*['\"]([^'\"]+\.pdf)['\"]\s*\)",
        raw,
        flags=re.IGNORECASE,
    )
    if not match:
        return {}

    kind = str(match.group(1) or "").strip().lower()
    filename = os.path.basename(str(match.group(2) or "").strip())
    if not filename.lower().endswith(".pdf"):
        return {}

    subdir = "article" if kind == "pdf_a" else "pdf"
    return {
        "kind": kind,
        "filename": filename,
        "pdf_url": f"https://www.powdermat.org/upload/{subdir}/{quote(filename, safe='')}",
    }


def _resolve_powdermat_pdf_target(doi: str, current_url: str = "", current_html: str = "") -> Dict[str, str]:
    doi_norm = _doi_from_doi_url(doi)
    if not doi_norm.startswith("10.4150/"):
        return {}

    current_target = _extract_powdermat_pdf_target_from_html(current_html)
    if current_target:
        if current_url:
            current_target["article_url"] = current_url
        return current_target

    entry_url = _powdermat_entry_url(doi_norm)
    if not entry_url:
        return {}

    try:
        resp = requests.get(
            entry_url,
            timeout=15,
            headers={"User-Agent": _resolve_best_browser_ua(), "Referer": "https://www.powdermat.org/"},
        )
    except Exception:
        return {}

    resolved = _extract_powdermat_pdf_target_from_html(resp.text or "")
    if resolved:
        resolved["article_url"] = str(resp.url or entry_url).strip() or entry_url
    return resolved


def _ceramist_entry_url(doi: str) -> str:
    doi_norm = _doi_from_doi_url(doi)
    if not doi_norm.startswith("10.31613/"):
        return ""
    return f"https://www.ceramist.or.kr/journal/view.php?doi={quote(doi_norm, safe='/')}"


def _extract_ceramist_pdf_target_from_html(html: str) -> Dict[str, str]:
    raw = str(html or "")
    if not raw:
        return {}

    meta_match = re.search(
        r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+\.pdf[^"\']*)["\']',
        raw,
        flags=re.IGNORECASE,
    )
    if meta_match:
        pdf_url = str(meta_match.group(1) or "").strip()
        if pdf_url:
            return {
                "kind": "pdf",
                "filename": os.path.basename(urlparse(pdf_url).path or ""),
                "pdf_url": pdf_url,
            }

    match = re.search(
        r"journal_download\(\s*['\"]pdf(?:_a)?['\"]\s*,\s*['\"]\d+['\"]\s*,\s*['\"]([^'\"]+\.pdf)['\"]\s*\)",
        raw,
        flags=re.IGNORECASE,
    )
    if not match:
        return {}

    filename = os.path.basename(str(match.group(1) or "").strip())
    if not filename.lower().endswith(".pdf"):
        return {}
    return {
        "kind": "pdf",
        "filename": filename,
        "pdf_url": f"https://www.ceramist.or.kr/upload/pdf/{quote(filename, safe='')}",
    }


def _resolve_ceramist_pdf_target(doi: str, current_url: str = "", current_html: str = "") -> Dict[str, str]:
    doi_norm = _doi_from_doi_url(doi)
    if not doi_norm.startswith("10.31613/"):
        return {}

    current_target = _extract_ceramist_pdf_target_from_html(current_html)
    if current_target:
        if current_url:
            current_target["article_url"] = current_url
        return current_target

    entry_url = _ceramist_entry_url(doi_norm)
    if not entry_url:
        return {}

    try:
        resp = requests.get(
            entry_url,
            timeout=15,
            headers={"User-Agent": _resolve_best_browser_ua(), "Referer": "https://www.ceramist.or.kr/"},
        )
    except Exception:
        return {}

    resolved = _extract_ceramist_pdf_target_from_html(resp.text or "")
    if resolved:
        resolved["article_url"] = str(resp.url or entry_url).strip() or entry_url
    return resolved


def _resolve_kjmm_pdf_target(doi: str, current_url: str = "", current_html: str = "", logger=None) -> Dict[str, str]:
    doi_norm = _doi_from_doi_url(doi)
    if not doi_norm.startswith("10.3365/"):
        return {}

    headers = {"User-Agent": _resolve_best_browser_ua(), "Referer": "http://kjmm.org/"}

    article_url = str(current_url or "").strip()
    article_html = str(current_html or "")
    doi_router_url = f"http://kjmm.org/journal/journal-by-doi.cshtml?doi={quote(doi_norm, safe='/')}"

    if "/kjmm/articledetail/rd_r/" not in article_url.lower():
        try:
            router_resp = requests.get(doi_router_url, headers=headers, timeout=15)
            data = router_resp.json()
            idx = str(data.get("idx") or "").strip()
            if not idx:
                return {}
            article_url = f"http://kjmm.org/kjmm/ArticleDetail/RD_R/{idx}"
        except Exception as e:
            if logger:
                logger.info(f"        [KJMM] DOI router 해석 실패: {e}")
            return {}

    session = requests.Session()
    try:
        article_resp = session.get(article_url, headers=headers, timeout=15)
        article_html = str(article_resp.text or "")
    except Exception as e:
        if logger:
            logger.info(f"        [KJMM] article detail 진입 실패: {e}")
        return {}

    mm_match = re.search(
        r"MM_openBrWindow\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
        article_html,
        flags=re.IGNORECASE,
    )
    if not mm_match:
        return {"article_url": article_url}

    dbname = str(mm_match.group(1) or "").strip()
    dn = str(mm_match.group(2) or "").strip()
    if not dbname or not dn:
        return {"article_url": article_url}

    viewer_url = f"http://kjmm.org/Common/pdf_viewer?returnVal={quote(dbname, safe='')}&dn={quote(dn, safe='')}"
    try:
        viewer_resp = session.get(viewer_url, headers={**headers, "Referer": article_url}, timeout=15)
    except Exception as e:
        if logger:
            logger.info(f"        [KJMM] pdf_viewer 진입 실패: {e}")
        return {"article_url": article_url}

    viewer_html = str(viewer_resp.text or "")
    redirect_match = re.search(
        r'window\.location\.href\s*=\s*["\']([^"\']+)["\']',
        viewer_html,
        flags=re.IGNORECASE,
    )
    cart_url = urljoin(viewer_resp.url or viewer_url, redirect_match.group(1).strip()) if redirect_match else ""
    if not cart_url:
        return {"article_url": article_url}

    try:
        pdf_resp = session.get(cart_url, headers={**headers, "Referer": viewer_resp.url or viewer_url}, timeout=20, allow_redirects=True)
    except Exception as e:
        if logger:
            logger.info(f"        [KJMM] openAccess cart 진입 실패: {e}")
        return {"article_url": article_url}

    content_type = str(pdf_resp.headers.get("Content-Type") or "").lower()
    final_url = str(pdf_resp.url or cart_url).strip()
    body = str(pdf_resp.text or "")[:400].lower() if "text/" in content_type else ""
    if ("application/pdf" in content_type) or final_url.lower().endswith(".pdf"):
        return {
            "article_url": article_url,
            "pdf_url": final_url,
            "viewer_url": viewer_url,
        }
    if "you are not authorized user" in body:
        return {
            "article_url": article_url,
            "viewer_url": viewer_url,
            "access_issue": "FAIL_ACCESS_RIGHTS",
        }
    return {"article_url": article_url, "viewer_url": viewer_url}


def _extract_sciencedirect_pii_from_url(url: str) -> str:
    raw = str(url or "")
    m = re.search(r"/pii/([A-Z0-9]+)", raw, flags=re.IGNORECASE)
    return (m.group(1) if m else "").upper()


def _extract_sciencedirect_pii_from_text(text: str) -> str:
    raw = str(text or "")
    m = re.search(r"1-s2\.0-([A-Z0-9]+)-main\.pdf", raw, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"/papers\.cfm/([A-Z0-9]+)/pdfft", raw, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"/pii/([A-Z0-9]+)", raw, flags=re.IGNORECASE)
    return (m.group(1) if m else "").upper()


def _extract_sciencedirect_article_url_from_html(html: str) -> str:
    raw = str(html or "")
    patterns = [
        r'https?://(?:www\.)?sciencedirect\.com/science/article/pii/[A-Z0-9]+[^"\'<\s]*',
        r'//(?:www\.)?sciencedirect\.com/science/article/pii/[A-Z0-9]+[^"\'<\s]*',
        r'/science/article/pii/[A-Z0-9]+[^"\'<\s]*',
    ]
    for pat in patterns:
        m = re.search(pat, raw, flags=re.IGNORECASE)
        if not m:
            continue
        u = m.group(0)
        if u.startswith("//"):
            return "https:" + u
        if u.startswith("/"):
            return "https://www.sciencedirect.com" + u
        return u
    return ""


def _is_elsevier_retrieve_url(url: str) -> bool:
    low = str(url or "").lower()
    return "linkinghub.elsevier.com/retrieve/pii/" in low


def _extract_elsevier_retrieve_handoff_url(current_url: str, html: str) -> str:
    raw = str(html or "")
    if not raw:
        return ""
    try:
        pu = urlparse(str(current_url or "").strip())
        origin = f"{pu.scheme}://{pu.netloc}" if pu.scheme and pu.netloc else "https://linkinghub.elsevier.com"
    except Exception:
        origin = "https://linkinghub.elsevier.com"

    def _read_input_value(name_or_id: str) -> str:
        key = re.escape(str(name_or_id or ""))
        patterns = [
            rf'id=["\']{key}["\'][^>]*value=["\']([^"\']+)["\']',
            rf'name=["\']{key}["\'][^>]*value=["\']([^"\']+)["\']',
            rf'value=["\']([^"\']+)["\'][^>]*id=["\']{key}["\']',
            rf'value=["\']([^"\']+)["\'][^>]*name=["\']{key}["\']',
        ]
        for pat in patterns:
            m = re.search(pat, raw, flags=re.IGNORECASE)
            if m:
                return html_unescape(m.group(1).strip())
        return ""

    redirect_url = _read_input_value("redirectURL")
    handoff_key = _read_input_value("key")
    result_name = _read_input_value("resultName") or "articleSelectSinglePerm"
    if redirect_url and handoff_key and result_name:
        return f"{origin}/retrieve/{result_name}?Redirect={redirect_url}&key={handoff_key}"

    m = re.search(
        r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\']+)["\']',
        raw,
        flags=re.IGNORECASE,
    )
    if m:
        u = html_unescape(m.group(1).strip().strip("'\""))
        if u.startswith("/"):
            return origin + u
        if u.startswith("http"):
            return u
    return ""


def _click_elsevier_doi_link_in_retrieve(page, doi_norm: str, logger=None) -> bool:
    if page is None or not doi_norm:
        return False
    doi_norm = str(doi_norm).strip().lower()
    if not doi_norm:
        return False

    # 1) 빠른 DOM 셀렉터
    quick = _ele_quick(page, f"css:a[href*='doi.org/{doi_norm}']", timeout=0.7)
    if quick:
        try:
            try:
                quick.click()
            except Exception:
                quick.click(by_js=True)
            if logger:
                logger.info("        [Elsevier] retrieve 페이지 DOI 링크 클릭(quick)")
            return True
        except Exception:
            pass

    # 2) Shadow DOM / same-origin iframe까지 포함한 JS 클릭
    js = r"""
((doiNeedle) => {
  const needle = String(doiNeedle || '').toLowerCase();
  if (!needle) return false;

  function tryClick(el) {
    if (!el) return false;
    try { el.click(); return true; } catch(e) {}
    try { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); return true; } catch(e) {}
    return false;
  }

  function scan(root) {
    if (!root || !root.querySelectorAll) return false;
    const anchors = root.querySelectorAll('a[href]');
    for (const a of anchors) {
      const href = String(a.getAttribute('href') || '').toLowerCase();
      const txt = String((a.textContent || '') + ' ' + (a.getAttribute('title') || '') + ' ' + (a.getAttribute('aria-label') || '')).toLowerCase();
      if (href.includes('doi.org/' + needle) || txt.includes(needle)) {
        if (tryClick(a)) return true;
      }
    }
    const all = root.querySelectorAll('*');
    for (const el of all) {
      try {
        if (el.shadowRoot && scan(el.shadowRoot)) return true;
      } catch(e) {}
    }
    const iframes = root.querySelectorAll('iframe');
    for (const fr of iframes) {
      try {
        const d = fr.contentDocument || (fr.contentWindow && fr.contentWindow.document);
        if (d && scan(d)) return true;
      } catch(e) {}
    }
    return false;
  }
  return scan(document);
})(arguments[0]);
"""
    try:
        ok = page.run_js(js, doi_norm)
        if bool(ok):
            if logger:
                logger.info("        [Elsevier] retrieve 페이지 DOI 링크 클릭(JS)")
            return True
    except Exception:
        pass
    return False


def _extract_sciencedirect_pdfft_candidates_from_html(html: str) -> list:
    raw = str(html or "")
    if not raw:
        return []
    candidates = []
    patterns = [
        r'https?://(?:www\.)?sciencedirect\.com/[^"\'<\s]*pdfft[^"\'<\s]*',
        r'//(?:www\.)?sciencedirect\.com/[^"\'<\s]*pdfft[^"\'<\s]*',
        r'/[^"\'<\s]*pdfft[^"\'<\s]*',
    ]
    for pat in patterns:
        for m in re.findall(pat, raw, flags=re.IGNORECASE):
            u = html_unescape(str(m or "").strip())
            if not u:
                continue
            if "\\u002f" in u.lower():
                try:
                    u = u.encode("utf-8").decode("unicode_escape")
                except Exception:
                    pass
            if u.startswith("//"):
                u = "https:" + u
            elif u.startswith("/"):
                u = "https://www.sciencedirect.com" + u
            if "sciencedirect.com" in u.lower() and "/pdfft" in u.lower():
                candidates.append(u)
    uniq = []
    seen = set()
    for u in candidates:
        key = u.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(key)
    return uniq


def _extract_sciencedirect_pdfft_url_from_html(html: str, target_pii: str = "") -> str:
    raw = str(html or "")
    target_pii_norm = str(target_pii or "").strip().upper()
    if target_pii_norm:
        for u in _extract_sciencedirect_pdfft_candidates_from_html(raw):
            found = _extract_sciencedirect_pii_from_text(u)
            if found == target_pii_norm:
                return u
        pii = target_pii_norm
    else:
        pii = _extract_sciencedirect_pii_from_text(raw)
    if not pii:
        return ""
    md5 = ""
    pid = ""
    path = "science/article/pii"
    ext = "/pdfft"
    try:
        m = re.search(r'"md5":"([^"]+)"', raw, flags=re.IGNORECASE)
        if m:
            md5 = m.group(1).strip()
        m = re.search(r'"pid":"([^"]+)"', raw, flags=re.IGNORECASE)
        if m:
            pid = m.group(1).strip()
        m = re.search(r'"path":"([^"]+)"', raw, flags=re.IGNORECASE)
        if m:
            path = m.group(1).strip().strip("/")
        m = re.search(r'"pdfextension":"([^"]+)"', raw, flags=re.IGNORECASE)
        if m:
            ext = m.group(1).strip()
    except Exception:
        pass

    if not ext.startswith("/"):
        ext = "/" + ext
    base = f"https://www.sciencedirect.com/{path}/{pii}{ext}"
    q = {}
    if md5:
        q["md5"] = md5
    if pid:
        q["pid"] = pid
    if q:
        return base + "?" + urlencode(q)
    return base


def _looks_like_empty_rendered_page(title: str = "", html: str = "") -> bool:
    t = str(title or "").strip().lower()
    h = str(html or "")
    text = re.sub(r"<[^>]+>", " ", h)
    text = re.sub(r"\s+", " ", text).strip().lower()
    if t:
        return False
    if not text:
        return True
    if len(text) < 32 and not any(k in text for k in ("pdf", "article", "doi", "download", "view")):
        return True
    return False


def _resolve_doi_redirect_target(doi_url: str, logger=None) -> str:
    raw = str(doi_url or "").strip()
    if not raw:
        return ""
    headers = {
        "User-Agent": _resolve_best_browser_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": BEST_BROWSER_ACCEPT_LANGUAGE,
    }
    try:
        resp = requests.get(raw, headers=headers, allow_redirects=True, timeout=8)
        final_url = str(resp.url or "").strip()
        final_domain = _extract_domain(final_url)
        if final_url and final_url != raw and "doi.org" not in final_domain:
            return final_url
        if "sciencedirect.com/science/article/pii/" in final_url.lower():
            return final_url
        html = resp.text or ""
        if html:
            try:
                soup = BeautifulSoup(html, "html.parser")
                meta_locators = (
                    ("link", "rel", "canonical", "href"),
                    ("meta", "property", "og:url", "content"),
                    ("meta", "name", "citation_abstract_html_url", "content"),
                    ("meta", "name", "citation_fulltext_html_url", "content"),
                )
                for tag_name, attr_name, attr_value, value_key in meta_locators:
                    node = soup.find(tag_name, attrs={attr_name: attr_value})
                    if not node:
                        continue
                    cand = str(node.get(value_key) or "").strip()
                    if not cand:
                        continue
                    cand = urljoin(final_url or raw, cand)
                    cand_domain = _extract_domain(cand)
                    if cand_domain and "doi.org" not in cand_domain:
                        return cand
            except Exception:
                pass
            meta_refresh = re.search(
                r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\']+)["\']',
                html,
                flags=re.IGNORECASE,
            )
            if meta_refresh:
                cand = urljoin(final_url or raw, meta_refresh.group(1).strip())
                cand_domain = _extract_domain(cand)
                if cand_domain and "doi.org" not in cand_domain:
                    return cand
        return _extract_sciencedirect_article_url_from_html(resp.text or "")
    except Exception as e:
        if logger:
            logger.info(f"        [DOI Resolve] redirect 해석 실패: {e}")
        return ""


def _strip_challenge_markers_from_url(url: str, allowed_domains: tuple[str, ...] = ()) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    domain = _extract_domain(raw)
    if allowed_domains and not any(domain == host or domain.endswith(f".{host}") for host in allowed_domains):
        return ""
    low = raw.lower()
    if any(token in low for token in ("validate.perfdrive.com", "captcha.perfdrive.com", "challenges.cloudflare.com")):
        return ""
    if "__cf_chl_rt_tk=" not in low and "/cdn-cgi/challenge" not in low and "/cdn-cgi/l/chk_captcha" not in low:
        return raw
    cleaned = urlunparse(parsed._replace(query="", fragment=""))
    return cleaned if cleaned and cleaned != raw else ""


def _requests_first_party_entry_url(
    doi_url: str,
    allowed_domains: tuple[str, ...],
    prefer_tokens: tuple[str, ...] = (),
    reject_tokens: tuple[str, ...] = (),
) -> str:
    try:
        resp = requests.get(
            str(doi_url or "").strip(),
            allow_redirects=True,
            timeout=15,
            headers={"User-Agent": _resolve_best_browser_ua()},
        )
    except Exception:
        return ""

    candidates = []
    chain = [getattr(h, "url", "") for h in (resp.history or [])] + [resp.url]
    for candidate in chain:
        cand = str(candidate or "").strip()
        if not cand:
            continue
        domain = _extract_domain(cand)
        if allowed_domains and not any(domain == host or domain.endswith(f".{host}") for host in allowed_domains):
            continue
        low = cand.lower()
        if any(token in low for token in reject_tokens):
            continue
        candidates.append(cand)

    if not candidates:
        cleaned = _strip_challenge_markers_from_url(str(resp.url or ""), allowed_domains=allowed_domains)
        if cleaned:
            return cleaned
        return ""

    for token in prefer_tokens:
        low_token = str(token or "").lower()
        for candidate in reversed(candidates):
            if low_token and low_token in candidate.lower():
                cleaned = _strip_challenge_markers_from_url(candidate, allowed_domains=allowed_domains)
                return cleaned or candidate

    best = candidates[-1]
    cleaned = _strip_challenge_markers_from_url(best, allowed_domains=allowed_domains)
    return cleaned or best


def _build_aps_first_party_article_url(doi_norm: str) -> str:
    norm = str(doi_norm or "").strip().lower()
    if not norm.startswith("10.1103/"):
        return ""
    suffix = norm.split("/", 1)[1]
    journal = suffix.split(".", 1)[0].lower()
    slug = APS_JOURNAL_SLUGS.get(journal)
    if not slug:
        return ""
    return f"https://journals.aps.org/{slug}/abstract/{norm}"


def _resolve_aip_structural_entry_url(doi_url: str) -> str:
    return _requests_first_party_entry_url(
        doi_url=doi_url,
        allowed_domains=AIP_FIRST_PARTY_DOMAINS,
        prefer_tokens=("/article/", "/doi/abs/", "/doi/full/"),
        reject_tokens=("__cf_chl_rt_tk=", "/cdn-cgi/challenge", "challenges.cloudflare.com"),
    )


def _resolve_dspace_pdf_target(current_url: str = "", current_html: str = "", logger=None) -> Dict[str, str]:
    html = str(current_html or "")
    if not html:
        return {}
    current_url = str(current_url or "").strip()
    current_origin = ""
    try:
        parsed = urlparse(current_url)
        if parsed.scheme and parsed.netloc:
            current_origin = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        current_origin = ""

    candidates = []
    seen = set()

    def _push(url: str):
        cand = str(url or "").strip()
        if not cand or cand in seen:
            return
        seen.add(cand)
        candidates.append(cand)

    for match in re.finditer(
        r'https?://[^"\'\s>]+/server/api/core/bitstreams/[0-9a-f-]+/content(?:\?[^"\'\s>]*)?',
        html,
        flags=re.IGNORECASE,
    ):
        _push(match.group(0))

    for match in re.finditer(
        r'/server/api/core/bitstreams/[0-9a-f-]+/content(?:\?[^"\'\s>]*)?',
        html,
        flags=re.IGNORECASE,
    ):
        if current_origin:
            _push(urljoin(current_origin, match.group(0)))

    def _download_to_content_urls(download_url: str):
        match = re.search(r'/bitstreams/([0-9a-f-]+)/download', download_url, flags=re.IGNORECASE)
        if not match:
            return
        uuid = match.group(1)
        origins = []
        parsed_url = urlparse(download_url)
        if parsed_url.scheme and parsed_url.netloc:
            origins.append(f"{parsed_url.scheme}://{parsed_url.netloc}")
        if current_origin:
            origins.append(current_origin)
        for origin in origins:
            _push(f"{origin}/server/api/core/bitstreams/{uuid}/content")

    for match in re.finditer(
        r'https?://[^"\'\s>]+/bitstreams/[0-9a-f-]+/download(?:\?[^"\'\s>]*)?',
        html,
        flags=re.IGNORECASE,
    ):
        _download_to_content_urls(match.group(0))

    for match in re.finditer(
        r'/bitstreams/[0-9a-f-]+/download(?:\?[^"\'\s>]*)?',
        html,
        flags=re.IGNORECASE,
    ):
        if current_origin:
            _download_to_content_urls(urljoin(current_origin, match.group(0)))

    if not candidates:
        return {}

    headers = {
        "User-Agent": _resolve_best_browser_ua(),
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    }
    if current_url:
        headers["Referer"] = current_url

    for cand in candidates[:6]:
        try:
            resp = requests.get(cand, headers=headers, allow_redirects=True, timeout=12, stream=True)
            ct = str(resp.headers.get("Content-Type") or "").lower()
            cd = str(resp.headers.get("Content-Disposition") or "").lower()
            final_url = str(resp.url or cand)
            resp.close()
            if ("application/pdf" in ct) or (".pdf" in final_url.lower()) or ("filename=" in cd and ".pdf" in cd):
                if logger:
                    logger.info(f"        [DSpace] bitstream content PDF 후보 복구: {final_url}")
                return {
                    "pdf_url": cand,
                    "article_url": current_url,
                }
        except Exception as e:
            if logger:
                logger.info(f"        [DSpace] bitstream probe 실패: {cand} ({e})")
            continue
    return {}


def _extract_meta_content(page, meta_name: str) -> str:
    if page is None:
        return ""
    try:
        el = _ele_quick(page, f'css:meta[name="{meta_name}"]', timeout=0.5)
        if el:
            return str(el.attr("content") or "").strip()
    except Exception:
        pass
    return ""


def _extract_elsevier_target_pii(page) -> str:
    if page is None:
        return ""
    for candidate in (
        _extract_sciencedirect_pii_from_url(getattr(page, "url", "") or ""),
        _extract_sciencedirect_pii_from_text(_extract_meta_content(page, "citation_pdf_url")),
        _extract_sciencedirect_pii_from_text(_extract_meta_content(page, "citation_abstract_html_url")),
    ):
        if candidate:
            return candidate
    return ""


def _is_elsevier_target_page(page, target_doi: str, target_pii: str) -> bool:
    if page is None:
        return False
    current_url = str(getattr(page, "url", "") or "").lower()
    doi_norm = str(target_doi or "").strip().lower()
    pii_norm = str(target_pii or "").strip().upper()

    if pii_norm:
        pii_low = pii_norm.lower()
        if f"/pii/{pii_low}" in current_url:
            return True
        if f"/papers.cfm/{pii_low}/pdfft" in current_url:
            return True
        if f"1-s2.0-{pii_low}-main.pdf" in current_url:
            return True

    citation_doi = _extract_meta_content(page, "citation_doi").lower()
    if doi_norm and citation_doi and citation_doi == doi_norm:
        return True

    return False


def _is_recommended_or_related_blob(blob: str) -> bool:
    low = str(blob or "").lower()
    bad_tokens = (
        "recommended",
        "related",
        "suggested",
        "other users also viewed",
        "reading assistant",
        "questions you could ask",
        "actions you could take",
        "summarize this article",
        "summarize experiments",
        "similar article",
        "you may also like",
        "more like this",
    )
    return any(tok in low for tok in bad_tokens)


def _element_context_blob(el) -> str:
    if el is None:
        return ""
    try:
        raw = el.run_js(
            """let parts = [];
            let node = this;
            for (let i = 0; i < 6 && node; i += 1, node = node.parentElement) {
                try {
                    parts.push([
                        node.tagName || '',
                        node.id || '',
                        node.className || '',
                        node.getAttribute('role') || '',
                        node.getAttribute('aria-label') || '',
                        (node.innerText || '').slice(0, 300)
                    ].join(' '));
                } catch (e) {}
            }
            return parts.join(' | ');"""
        )
    except Exception:
        raw = ""
    return str(raw or "").lower()


def _is_elsevier_aux_overlay_blob(blob: str) -> bool:
    low = str(blob or "").lower()
    if not low:
        return False
    marker_tokens = (
        "other users also viewed",
        "reading assistant",
        "questions you could ask",
        "actions you could take",
        "summarize this article",
        "summarize experiments",
        "sign in to unlock the full response",
    )
    overlay_tokens = (
        " role dialog ",
        " aria-modal ",
        " modal ",
        " overlay ",
        " drawer ",
        " popover ",
        " backdrop ",
    )
    return any(tok in low for tok in marker_tokens) or any(tok in low for tok in overlay_tokens)


def _select_best_clickable_pdf_element(page, xpaths, logger=None, must_tokens=None, ban_tokens=None):
    candidates = []
    must_tokens = [str(t).lower() for t in (must_tokens or []) if str(t).strip()]
    ban_tokens = [str(t).lower() for t in (ban_tokens or []) if str(t).strip()]
    for xp in xpaths:
        for el in _eles_quick(page, f"xpath:{xp}", timeout=0.5):
            try:
                if not el.states.is_displayed:
                    continue
            except Exception:
                pass
            text = (el.text or "").strip()
            title = (el.attr("title") or "").strip()
            aria = (el.attr("aria-label") or "").strip()
            href = (el.attr("href") or "").strip()
            context_blob = _element_context_blob(el)
            blob = f"{text} {title} {aria} {href}".lower()
            full_blob = f"{blob} {context_blob}".strip()
            if _is_supporting_info_blob(full_blob) or any(
                k in full_blob
                for k in (
                    "figure",
                    "dataset",
                    "graphical abstract",
                    "citation",
                    "export",
                    "powerpoint",
                    "ms-power",
                    "ppt",
                    "bibtex",
                )
            ):
                continue
            if _is_recommended_or_related_blob(full_blob):
                continue
            if _is_elsevier_aux_overlay_blob(context_blob):
                continue
            if ban_tokens and any(tok in full_blob for tok in ban_tokens):
                continue
            if not any(k in blob for k in ("pdf", ".pdf", "/pdfft", "articlepdf", "open pdf", "view pdf")):
                continue
            if must_tokens and not any(tok in full_blob for tok in must_tokens):
                continue
            score = 0
            for k in ("open pdf", "view pdf", "download pdf", "/pdfft", ".pdf", "articlepdf", "pdf"):
                if k in blob:
                    score += 2
            if "accessbar" in context_blob:
                score += 8
            if "_blank" in blob or "opens in a new window" in full_blob:
                score += 2
            if href:
                score += 1
            candidates.append((score, el, blob))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    if logger:
        logger.info(f"        [ClickSelect] 후보 {len(candidates)}개 중 최고점={candidates[0][0]}")
    return candidates[0][1]


def _adopt_latest_tab(page, logger=None):
    if page is None:
        return page
    try:
        current_id = getattr(page, "tab_id", None)
        latest_obj = getattr(page, "latest_tab", None)
        latest_id = getattr(latest_obj, "tab_id", None) if latest_obj is not None else None
        if latest_id and current_id and latest_id != current_id:
            ntab = page.get_tab(latest_id)
            if ntab:
                if logger:
                    logger.info(f"        [Tab] 새 탭 전환: {current_id[:8]} -> {latest_id[:8]}")
                return ntab
    except Exception:
        pass
    return page


def _open_temporary_tab(page, start_url: str = "about:blank"):
    if page is None:
        return None
    try:
        temp_page = page.new_tab(start_url, background=False)
        time.sleep(0.2)
        return temp_page
    except Exception:
        return None


def _close_temporary_tab(page, temp_page) -> None:
    if page is None or temp_page is None or temp_page is page:
        return
    temp_tab_id = str(getattr(temp_page, "tab_id", "") or "")
    current_tab_id = str(getattr(page, "tab_id", "") or "")
    try:
        temp_page.close()
    except Exception:
        if temp_tab_id:
            try:
                page.close_tabs(temp_tab_id)
            except Exception:
                pass
    if current_tab_id:
        try:
            page.activate_tab(current_tab_id)
        except Exception:
            pass


def _close_new_tabs_since(page, baseline_tab_ids) -> None:
    if page is None:
        return
    baseline = {str(t or "") for t in (baseline_tab_ids or []) if str(t or "")}
    try:
        current_tab_id = str(getattr(page, "tab_id", "") or "")
        current_ids = [str(t or "") for t in (getattr(page, "tab_ids", []) or [])]
        for tab_id in current_ids:
            if not tab_id or tab_id in baseline:
                continue
            try:
                page.close_tabs(tab_id)
            except Exception:
                pass
        if current_tab_id:
            try:
                page.activate_tab(current_tab_id)
            except Exception:
                pass
    except Exception:
        pass


def _summarize_elsevier_pdf_control(el) -> str:
    if el is None:
        return "missing"
    try:
        text = " ".join(str(el.text or "").split())
        aria = str(el.attr("aria-label") or "").strip()
        href = str(el.attr("href") or "").strip()
        target = str(el.attr("target") or "").strip()
        el_id = str(el.attr("id") or "").strip()
        cls = " ".join(str(el.attr("class") or "").split()[:6])
        return (
            f"text={text[:80]!r}, aria={aria[:80]!r}, href={bool(href)}, "
            f"target={target[:20]!r}, id={el_id[:40]!r}, class={cls[:80]!r}"
        )
    except Exception:
        return "unavailable"


def _dismiss_elsevier_aux_overlays(page, logger=None) -> bool:
    if page is None:
        return False
    js = r"""
(() => {
  const markers = [
    'other users also viewed',
    'reading assistant',
    'questions you could ask',
    'actions you could take',
    'summarize this article',
    'summarize experiments',
    'sign in to unlock the full response'
  ];
  const lower = (v) => String(v || '').toLowerCase();
  const nodes = Array.from(document.querySelectorAll('dialog,[role="dialog"],aside,section,div'));
  const touched = new Set();
  let changed = false;

  function isTarget(el) {
    const blob = lower([
      el.tagName || '',
      el.id || '',
      el.className || '',
      el.getAttribute && el.getAttribute('role'),
      el.getAttribute && el.getAttribute('aria-label'),
      (el.innerText || '').slice(0, 1200)
    ].join(' '));
    return markers.some(m => blob.includes(m));
  }

  for (const el of nodes) {
    if (!el || touched.has(el) || !isTarget(el)) continue;
    touched.add(el);
    const buttons = Array.from(el.querySelectorAll('button,a,[role="button"]'));
    const closer = buttons.find(btn => {
      const blob = lower([
        btn.innerText || '',
        btn.getAttribute('aria-label') || '',
        btn.getAttribute('title') || '',
        btn.className || ''
      ].join(' '));
      return (
        blob.includes('close')
        || blob.includes('dismiss')
        || blob.includes('not now')
        || blob.includes('no thanks')
        || blob.includes('no thank you')
        || blob === '×'
        || blob === 'x'
      );
    });
    if (closer) {
      try {
        closer.click();
        changed = true;
        continue;
      } catch (e) {}
    }
    try {
      el.style.setProperty('display', 'none', 'important');
      el.style.setProperty('visibility', 'hidden', 'important');
      el.setAttribute('data-codex-hidden', '1');
      changed = true;
    } catch (e) {}
  }

  if (changed) {
    for (const back of Array.from(document.querySelectorAll('[class*="overlay"],[class*="backdrop"],[class*="modal-backdrop"]'))) {
      const text = lower((back.innerText || '').slice(0, 600));
      if (markers.some(m => text.includes(m)) || back.hasAttribute('data-codex-hidden')) {
        try {
          back.style.setProperty('display', 'none', 'important');
          back.style.setProperty('visibility', 'hidden', 'important');
        } catch (e) {}
      }
    }
  }
  return changed;
})();
"""
    try:
        changed = bool(page.run_js(js))
    except Exception:
        changed = False
    if changed and logger:
        logger.info("        [Elsevier] 보조 overlay/panel 정리")
    return changed


def _click_once_wait_file(
    page,
    el,
    tmp_dir: str,
    tmp_path: str,
    wait_s: int,
    logger=None,
    post_click_guard=None,
    downloaded_file_guard=None,
    fast_exit_on_new_tab: bool = False,
    allow_js_fallback: bool = True,
) -> bool:
    if page is None or el is None:
        return False
    try:
        before_tab_ids = set()
        try:
            before_tab_ids = set(getattr(page, "tab_ids", []) or [])
        except Exception:
            before_tab_ids = set()
        initial_files = _get_current_files(tmp_dir)
        try:
            try:
                el.scroll.to_see()
                time.sleep(0.25)
            except Exception:
                pass
            el.click()
        except Exception:
            if not allow_js_fallback:
                raise
            el.click(by_js=True)
        if post_click_guard:
            try:
                time.sleep(0.8)
                if not post_click_guard():
                    if logger:
                        logger.info("        [ClickGuard] 대상 논문 컨텍스트 불일치로 클릭 결과 무시")
                    return False
            except Exception:
                return False
        if fast_exit_on_new_tab and before_tab_ids:
            try:
                now_ids = set(getattr(page, "tab_ids", []) or [])
                if now_ids - before_tab_ids:
                    if logger:
                        logger.info("        [Elsevier][Tab] 새 탭 감지 -> 즉시 2단계로 전환")
                    return False
            except Exception:
                pass
        if _capture_direct_downloaded_pdf(
            download_dir=tmp_dir,
            initial_files=initial_files,
            tmp_target_path=tmp_path,
            final_target_path=tmp_path,
            logger=logger,
            timeout_s=wait_s,
            context="click-wait-download",
            downloaded_file_guard=downloaded_file_guard,
        ):
            return True
    except Exception as e:
        if logger:
            logger.warning(f"        클릭-대기 실패: {e}")
    return False


def _finalize_existing_downloads_in_dir(tmp_dir: str, tmp_path: str, logger=None, downloaded_file_guard=None) -> bool:
    try:
        candidates = []
        for name in sorted(os.listdir(tmp_dir)):
            path = os.path.join(tmp_dir, name)
            if not os.path.isfile(path):
                continue
            if not name.lower().endswith(".pdf"):
                continue
            if os.path.abspath(path) == os.path.abspath(tmp_path):
                continue
            if downloaded_file_guard and not downloaded_file_guard(path):
                continue
            if not _is_valid_pdf(path):
                continue
            candidates.append(path)
        if not candidates:
            return False
        chosen = candidates[0]
        if not _finalize_downloaded_file(chosen, tmp_path, logger=logger):
            return False
        for extra in candidates[1:]:
            try:
                os.remove(extra)
                if logger:
                    logger.info(f"        [Elsevier] 중복 다운로드 정리: {os.path.basename(extra)}")
            except Exception:
                pass
        return True
    except Exception:
        return False


def _is_low_trust_elsevier_session_source(session_source: str) -> bool:
    low = str(session_source or "").strip().lower()
    if not low:
        return True
    return low == "temp" or low.startswith("temp_shared") or low.startswith("persistent_fallback")


def _select_elsevier_pdf_control_snapshot(page) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "found": False,
        "disabled": False,
        "href": "",
        "onclick": "",
        "context_blob": "",
        "score": -999,
    }
    if page is None:
        return snapshot

    quick_locators = [
        'css:#viewpdf',
        'css:a#viewpdf',
        'css:button#viewpdf',
        'css:[id*="viewpdf"]',
        'css:[aria-label*="View PDF"]',
        'css:[aria-label*="view pdf"]',
    ]
    for loc in quick_locators:
        for el in _eles_quick(page, loc, timeout=0.25):
            context_blob = _element_context_blob(el)
            if _is_elsevier_aux_overlay_blob(context_blob) or _is_recommended_or_related_blob(context_blob):
                continue
            href = str(el.attr("href") or "").strip()
            onclick = str(el.attr("onclick") or "").strip()
            aria_label = str(el.attr("aria-label") or "").strip()
            class_name = str(el.attr("class") or "").strip()
            disabled = any(
                str(token or "").strip().lower() in ("true", "disabled")
                for token in (
                    el.attr("disabled"),
                    el.attr("aria-disabled"),
                )
            ) or ("disabled" in class_name.lower())
            blob = f"{(el.text or '').strip()} {aria_label} {class_name}".lower()
            score = 0
            if "accessbar" in context_blob:
                score += 8
            if "view pdf" in blob:
                score += 4
            if href:
                score += 3
            if onclick:
                score += 2
            if "_blank" in href.lower() or "opens in a new window" in context_blob:
                score += 1
            if disabled:
                score -= 20
            if score > int(snapshot.get("score") or -999):
                snapshot = {
                    "found": True,
                    "disabled": disabled,
                    "href": href,
                    "onclick": onclick,
                    "context_blob": context_blob,
                    "score": score,
                }
    return snapshot


def _inspect_elsevier_article_readiness(page, target_doi: str = "") -> Dict[str, Any]:
    if page is None:
        return {
            "ready": False,
            "issue": "",
            "evidence": [],
            "target_match": False,
            "citation_pdf_url": "",
            "citation_abs_url": "",
            "has_download_toolbar": False,
            "actionable_view_pdf": False,
            "looks_like_shell": False,
            "body_text_len": 0,
            "main_text_len": 0,
        }

    current_url = str(getattr(page, "url", "") or "")
    title = str(getattr(page, "title", "") or "")
    html = str(getattr(page, "html", "") or "")
    domain = _extract_domain(current_url)
    issue, evidence = detect_access_issue(title=title, html=html, url=current_url, domain=domain)
    citation_doi = _extract_meta_content(page, "citation_doi").lower()
    citation_pdf_url = _extract_meta_content(page, "citation_pdf_url").strip()
    citation_abs_url = _extract_meta_content(page, "citation_abstract_html_url").strip()
    target_pii = _extract_elsevier_target_pii(page)
    target_match = _is_elsevier_target_page(page, target_doi, target_pii)
    control_snapshot = _select_elsevier_pdf_control_snapshot(page)
    has_download_toolbar = bool(
        _ele_quick(page, 'css:button[aria-label*="Download"]', timeout=0.2)
        or _ele_quick(page, 'css:button[title*="Download"]', timeout=0.2)
        or _ele_quick(page, 'css:a[download]', timeout=0.2)
    )
    try:
        metrics = page.run_js(
            """const main = document.querySelector('main');
            return {
                body_text_len: ((document.body && document.body.innerText) || '').length,
                main_text_len: ((main && main.innerText) || '').length
            };"""
        ) or {}
    except Exception:
        metrics = {}
    body_text_len = int((metrics or {}).get("body_text_len") or 0)
    main_text_len = int((metrics or {}).get("main_text_len") or 0)
    looks_like_shell = _looks_like_elsevier_article_shell(page, doi_norm=str(target_doi or "").strip().lower(), target_pii=target_pii)
    actionable_view_pdf = bool(
        control_snapshot.get("found")
        and not bool(control_snapshot.get("disabled"))
        and (
            citation_pdf_url
            or control_snapshot.get("href")
            or control_snapshot.get("onclick")
            or int(control_snapshot.get("score") or 0) >= 8
        )
    )
    ready = bool(
        issue is None
        and target_match
        and (citation_pdf_url or actionable_view_pdf or has_download_toolbar)
        and (
            not looks_like_shell
            or bool(citation_pdf_url)
            or bool(has_download_toolbar)
            or bool(citation_abs_url)
            or body_text_len >= 450
            or main_text_len >= 120
        )
    )
    return {
        "ready": ready,
        "issue": issue or "",
        "evidence": list(evidence or []),
        "target_match": target_match,
        "citation_doi": citation_doi,
        "citation_pdf_url": citation_pdf_url,
        "citation_abs_url": citation_abs_url,
        "has_download_toolbar": has_download_toolbar,
        "actionable_view_pdf": actionable_view_pdf,
        "looks_like_shell": looks_like_shell,
        "body_text_len": body_text_len,
        "main_text_len": main_text_len,
        "control_score": int(control_snapshot.get("score") or 0),
        "current_url": current_url,
    }


def _wait_for_elsevier_article_ready(
    page,
    target_doi: str = "",
    logger=None,
    timeout_s: int = 8,
    strict: bool = False,
) -> Dict[str, Any]:
    if page is None:
        return {"ready": False, "issue": "", "evidence": [], "target_match": False}
    target_doi = str(target_doi or "").strip().lower()
    deadline = time.time() + max(1, int(timeout_s))
    ready_seen = 0
    required_ready_seen = 3 if strict else 2
    last_snapshot: Dict[str, Any] = {"ready": False, "issue": "", "evidence": [], "target_match": False}
    while time.time() < deadline:
        try:
            snapshot = _inspect_elsevier_article_readiness(page, target_doi)
            last_snapshot = snapshot
            if snapshot.get("issue") in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
                if logger:
                    logger.info(
                        "        [Elsevier] hydrate 대기 중 차단 감지: "
                        f"{snapshot.get('issue')} {snapshot.get('evidence')}"
                    )
                return snapshot
            if snapshot.get("ready"):
                ready_seen += 1
                if ready_seen >= required_ready_seen:
                    if logger:
                        logger.info(
                            "        [Elsevier] article page hydrate 대기 완료 "
                            f"(pdf_meta={int(bool(snapshot.get('citation_pdf_url')))}, "
                            f"actionable={int(bool(snapshot.get('actionable_view_pdf')))}, "
                            f"viewer={int(bool(snapshot.get('has_download_toolbar')))}, "
                            f"shell={int(bool(snapshot.get('looks_like_shell')))}, "
                            f"body={int(snapshot.get('body_text_len') or 0)})"
                        )
                    time.sleep(1.2 if strict else 1.0)
                    return snapshot
            else:
                ready_seen = 0
        except Exception:
            ready_seen = 0
        time.sleep(0.5)
    if logger:
        logger.info(
            "        [Elsevier] article page hydrate 대기 타임아웃 "
            f"(target={int(bool(last_snapshot.get('target_match')))}, "
            f"pdf_meta={int(bool(last_snapshot.get('citation_pdf_url')))}, "
            f"actionable={int(bool(last_snapshot.get('actionable_view_pdf')))}, "
            f"viewer={int(bool(last_snapshot.get('has_download_toolbar')))}, "
            f"shell={int(bool(last_snapshot.get('looks_like_shell')))}, "
            f"issue={last_snapshot.get('issue') or 'none'})"
        )
    return last_snapshot


def _warm_elsevier_article_hydration(
    page,
    doi_norm: str,
    target_pii: str,
    article_referer: str,
    logger=None,
    timeout_s: int = 12,
    strict: bool = True,
):
    current_url = str(getattr(page, "url", "") or "")
    last_snapshot = _inspect_elsevier_article_readiness(page, doi_norm)
    for idx, candidate in enumerate(_build_elsevier_article_candidates(target_pii, article_referer, current_url)[:2]):
        try:
            if logger:
                logger.info(f"        [Elsevier] hydration warm reload {idx + 1}: {candidate}")
            page.get(candidate, retry=0, interval=0.3, timeout=min(timeout_s, 12))
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            _dismiss_elsevier_aux_overlays(page, logger=logger)
            last_snapshot = _wait_for_elsevier_article_ready(
                page,
                doi_norm,
                logger=logger,
                timeout_s=timeout_s,
                strict=strict,
            )
            if last_snapshot.get("ready") or last_snapshot.get("issue") in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
                return page, last_snapshot
        except Exception as e:
            if logger:
                logger.info(f"        [Elsevier] hydration warm reload 실패(계속): {e}")
    return page, last_snapshot


def _looks_like_elsevier_signed_pdf_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    return host.endswith("pdf.sciencedirectassets.com") and path.endswith(".pdf")


def _wait_for_elsevier_viewer_ready(page, logger=None, timeout_s: int = 6) -> str:
    if page is None:
        return ""
    deadline = time.time() + max(1, int(timeout_s))
    while time.time() < deadline:
        try:
            current_url = str(getattr(page, "url", "") or "")
            if _looks_like_elsevier_signed_pdf_url(current_url):
                if logger:
                    logger.info("        [Elsevier] signed PDF viewer 도달")
                time.sleep(0.6)
                return "signed_pdf"
            has_download = bool(
                _ele_quick(page, 'css:button[aria-label*="Download"]', timeout=0.2)
                or _ele_quick(page, 'css:button[title*="Download"]', timeout=0.2)
                or _ele_quick(page, 'css:a[download]', timeout=0.2)
            )
            if has_download:
                if logger:
                    logger.info("        [Elsevier] viewer toolbar 준비 완료")
                time.sleep(0.6)
                return "viewer"
        except Exception:
            pass
        time.sleep(0.5)
    if logger:
        logger.info("        [Elsevier] viewer 준비 대기 타임아웃(계속 진행)")
    return ""


def _download_elsevier_signed_pdf_from_viewer(page, tmp_path: str, referer_url: str = "", logger=None) -> bool:
    current_url = str(getattr(page, "url", "") or "").strip()
    if not _looks_like_elsevier_signed_pdf_url(current_url):
        return False
    cookies = None
    try:
        cookies = {c.get("name"): c.get("value") for c in (page.cookies() or []) if c.get("name")}
    except Exception:
        cookies = None
    if not download_with_cffi(
        current_url,
        tmp_path,
        referer=referer_url or current_url,
        cookies=cookies,
        logger=logger,
    ):
        return False
    return _is_valid_pdf(tmp_path)


def _tab_looks_like_elsevier_target(tab, doi_norm: str, target_pii: str) -> bool:
    if tab is None:
        return False
    current_url = str(getattr(tab, "url", "") or "")
    if _looks_like_elsevier_signed_pdf_url(current_url):
        return True
    if target_pii and target_pii.lower() in current_url.lower() and "sciencedirect.com" in current_url.lower():
        return True
    return _is_elsevier_target_page(tab, doi_norm, target_pii)


def _adopt_elsevier_target_tab(page, doi_norm: str, target_pii: str, logger=None):
    if page is None:
        return page
    current_id = str(getattr(page, "tab_id", "") or "")
    try:
        tab_ids = [str(t or "") for t in (getattr(page, "tab_ids", []) or [])]
    except Exception:
        tab_ids = []
    if len(tab_ids) <= 1:
        return page

    chosen = None
    mismatched = []
    for tab_id in reversed(tab_ids):
        if not tab_id or tab_id == current_id:
            continue
        try:
            candidate = page.get_tab(tab_id)
        except Exception:
            continue
        if _tab_looks_like_elsevier_target(candidate, doi_norm=doi_norm, target_pii=target_pii):
            chosen = candidate
            break
        mismatched.append((tab_id, str(getattr(candidate, "url", "") or ""), str(getattr(candidate, "title", "") or "")))

    for tab_id, url, title in mismatched:
        low = url.lower()
        if low.startswith("http") or low.startswith("about:"):
            try:
                page.close_tabs(tab_id)
                if logger:
                    logger.info(
                        f"        [Elsevier][Tab] 타깃 불일치 탭 정리: url={url[:160]!r}, title={title[:100]!r}"
                    )
            except Exception:
                pass

    if chosen is not None:
        chosen_id = str(getattr(chosen, "tab_id", "") or "")
        if chosen_id and chosen_id != current_id and logger:
            logger.info(f"        [Elsevier][Tab] 타깃 탭 전환: {current_id[:8]} -> {chosen_id[:8]}")
        return chosen
    return page


def _build_elsevier_article_candidates(target_pii: str, *urls) -> list:
    candidates = []
    seen = set()

    def _push(raw_url: str) -> None:
        raw = str(raw_url or "").strip()
        if not raw:
            return
        try:
            parsed = urlparse(raw)
        except Exception:
            return
        low = raw.lower()
        if "sciencedirect.com/science/article/" not in low:
            return
        cleaned = urlunparse(parsed._replace(query="", fragment=""))
        for candidate in (cleaned, cleaned.replace("/science/article/pii/", "/science/article/abs/pii/")):
            cand = str(candidate or "").strip()
            key = cand.lower()
            if not cand or key in seen:
                continue
            seen.add(key)
            candidates.append(cand)

    for raw_url in urls:
        _push(raw_url)
    pii = str(target_pii or "").strip().upper()
    if pii:
        _push(f"https://www.sciencedirect.com/science/article/pii/{pii}")
        _push(f"https://www.sciencedirect.com/science/article/abs/pii/{pii}")
    return candidates


def _looks_like_elsevier_article_shell(page, doi_norm: str, target_pii: str) -> bool:
    if page is None:
        return False
    current_url = str(getattr(page, "url", "") or "").strip().lower()
    if "sciencedirect.com" not in current_url:
        return False
    if not any(token in current_url for token in ("/science/article/pii/", "/fulltext/")):
        return False
    title = str(getattr(page, "title", "") or "").strip()
    if len(title) < 18:
        return False
    citation_doi = _extract_meta_content(page, "citation_doi").lower()
    if doi_norm and citation_doi and citation_doi != doi_norm:
        return False
    try:
        metrics = page.run_js(
            """const main = document.querySelector('main');
            return {
                body_text_len: ((document.body && document.body.innerText) || '').length,
                main_text_len: ((main && main.innerText) || '').length
            };"""
        ) or {}
    except Exception:
        metrics = {}
    body_text_len = int((metrics or {}).get("body_text_len") or 0)
    main_text_len = int((metrics or {}).get("main_text_len") or 0)
    if body_text_len >= 450 or main_text_len >= 120:
        return False
    if target_pii and (target_pii.lower() not in current_url) and not citation_doi:
        return False
    return True


def _recover_elsevier_article_shell(page, doi_norm: str, target_pii: str, article_referer: str, logger=None) -> bool:
    if not _looks_like_elsevier_article_shell(page, doi_norm=doi_norm, target_pii=target_pii):
        return False
    current_url = str(getattr(page, "url", "") or "")
    for candidate in _build_elsevier_article_candidates(target_pii, article_referer, current_url):
        try:
            current_clean = urlunparse(urlparse(current_url)._replace(query="", fragment=""))
        except Exception:
            current_clean = current_url
        if candidate.lower() == str(current_clean or "").lower():
            continue
        if logger:
            logger.info(f"        [Elsevier] article shell reopen 시도: {candidate}")
        try:
            page.get(candidate, retry=0, interval=0.3, timeout=8)
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            return True
        except Exception as e:
            if logger:
                logger.info(f"        [Elsevier] article shell reopen 실패(계속): {e}")
    return False


def _recover_elsevier_interstitial_article(page, target_pii: str, article_referer: str, logger=None) -> bool:
    if page is None:
        return False
    interstitial_titles = {"redirecting", "redirecting...", "loading", "please wait", "just a moment"}
    title = str(getattr(page, "title", "") or "").strip().lower()
    current_url = str(getattr(page, "url", "") or "").strip().lower()
    if title not in interstitial_titles:
        return False
    if "sciencedirect.com" not in current_url:
        return False
    for candidate in _build_elsevier_article_candidates(target_pii, article_referer, current_url):
        if logger:
            logger.info(f"        [Elsevier] interstitial article reopen 시도: {candidate}")
        try:
            page.get(candidate, retry=0, interval=0.3, timeout=8)
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            reopened_title = str(getattr(page, "title", "") or "").strip().lower()
            if reopened_title not in interstitial_titles:
                return True
        except Exception as e:
            if logger:
                logger.info(f"        [Elsevier] interstitial article reopen 실패(계속): {e}")
    return False


def _attempt_elsevier_two_step_click_download(
    page,
    doi: str,
    tmp_dir: str,
    tmp_path: str,
    logger=None,
    allow_doi_reentry: bool = True,
    allow_headless_fresh_tab_recovery: bool = True,
    session_source: str = "",
    low_trust_session: bool = False,
    return_page: bool = False,
):
    def _ret(ok: bool, current_page, *, issue: str = "", evidence=None, readiness=None, stage: str = ""):
        detail = {
            "issue": str(issue or ""),
            "evidence": list(evidence or []),
            "readiness": readiness or {},
            "stage": str(stage or ""),
        }
        return (ok, current_page, detail) if return_page else ok

    if page is None:
        return _ret(False, page)
    _dismiss_cookie_or_consent_banner(page, logger=logger)
    doi_norm = str(doi or "").strip().lower()
    target_pii = _extract_elsevier_target_pii(page)
    if logger:
        logger.info(f"        [Elsevier] target_doi={doi_norm}, target_pii={target_pii or 'N/A'}")

    if not target_pii and not doi_norm:
        if logger:
            logger.info("        [Elsevier] 타겟 식별자 부족으로 클릭 플로우 스킵")
        return _ret(False, page)

    def _context_guard() -> bool:
        cur = str(getattr(page, "url", "") or "").lower()
        if target_pii and str(target_pii).lower() in cur and "sciencedirect.com" in cur:
            return True
        return _is_elsevier_target_page(page, doi_norm, target_pii)

    def _file_guard(downloaded_path: str) -> bool:
        if not target_pii:
            return True
        found_pii = _extract_sciencedirect_pii_from_text(os.path.basename(str(downloaded_path or "")))
        if not found_pii:
            return True
        return found_pii == target_pii

    token_candidates = []
    if target_pii:
        token_candidates.append(f"/pii/{target_pii.lower()}")
        token_candidates.append(target_pii.lower())

    hydration_timeout = 12 if low_trust_session else 8
    readiness = _wait_for_elsevier_article_ready(
        page,
        doi_norm,
        logger=logger,
        timeout_s=hydration_timeout,
        strict=low_trust_session,
    )
    if readiness.get("issue") in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
        return _ret(
            False,
            page,
            issue=str(readiness.get("issue") or ""),
            evidence=list(readiness.get("evidence") or []),
            readiness=readiness,
            stage="preclick-hydration",
        )
    time.sleep(0.6)
    article_referer = str(getattr(page, "url", "") or "")
    if _recover_elsevier_article_shell(page, doi_norm=doi_norm, target_pii=target_pii, article_referer=article_referer, logger=logger):
        readiness = _wait_for_elsevier_article_ready(
            page,
            doi_norm,
            logger=logger,
            timeout_s=hydration_timeout,
            strict=low_trust_session,
        )
        if readiness.get("issue") in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
            return _ret(
                False,
                page,
                issue=str(readiness.get("issue") or ""),
                evidence=list(readiness.get("evidence") or []),
                readiness=readiness,
                stage="preclick-hydration",
            )
        time.sleep(0.6)
        article_referer = str(getattr(page, "url", "") or article_referer)

    if low_trust_session and not readiness.get("ready"):
        if logger:
            logger.info(
                "        [Elsevier] low-trust session에서 hydration 미완료 -> warm reload 시도 "
                f"(source={session_source or 'unknown'})"
            )
        page, readiness = _warm_elsevier_article_hydration(
            page,
            doi_norm=doi_norm,
            target_pii=target_pii,
            article_referer=article_referer,
            logger=logger,
            timeout_s=12,
            strict=True,
        )
        article_referer = str(getattr(page, "url", "") or article_referer)
        if readiness.get("issue") in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
            return _ret(
                False,
                page,
                issue=str(readiness.get("issue") or ""),
                evidence=list(readiness.get("evidence") or []),
                readiness=readiness,
                stage="preclick-hydration",
            )
        if not readiness.get("ready"):
            evidence = [
                "elsevier_hydration_incomplete_low_trust",
                f"session_source={session_source or 'unknown'}",
                f"citation_pdf_url={int(bool(readiness.get('citation_pdf_url')))}",
                f"actionable_view_pdf={int(bool(readiness.get('actionable_view_pdf')))}",
                f"viewer_download={int(bool(readiness.get('has_download_toolbar')))}",
                f"looks_like_shell={int(bool(readiness.get('looks_like_shell')))}",
                f"target_match={int(bool(readiness.get('target_match')))}",
            ]
            if logger:
                logger.info(f"        [Elsevier] hydration 미완료로 클릭 플로우 중단: {evidence}")
            return _ret(
                False,
                page,
                issue="FAIL_PARSE",
                evidence=evidence,
                readiness=readiness,
                stage="preclick-hydration",
            )

    # 사용자 관찰 반영:
    # 복구(sciencedirect) 후 DOI 링크를 다시 타면 쿠키/버튼 플로우가 정상화되는 케이스가 있다.
    def _attempt_doi_reentry_after_dead_click(current_page):
        if not allow_doi_reentry:
            return current_page, False
        if "sciencedirect.com" not in str(getattr(current_page, "url", "") or "").lower():
            return current_page, False
        doi_link = _ele_quick(current_page, f"css:a[href*='doi.org/{doi_norm}']", timeout=0.6) if doi_norm else None
        if doi_link:
            try:
                href = str(doi_link.attr("href") or "").strip().lower()
                text_blob = (
                    f"{doi_link.text or ''} "
                    f"{doi_link.attr('title') or ''} "
                    f"{doi_link.attr('aria-label') or ''}"
                ).lower()
                if _is_elsevier_retrieve_url(href):
                    if logger:
                        logger.info("        [Elsevier] DOI 재진입 후보가 retrieve URL이라 스킵")
                    doi_link = None
                elif doi_norm and (doi_norm not in href) and (doi_norm not in text_blob):
                    if logger:
                        logger.info("        [Elsevier] DOI 재진입 후보가 타겟 DOI 불일치로 스킵")
                    doi_link = None
            except Exception:
                pass
        if not doi_link:
            return current_page, False

        origin_page = current_page
        origin_tab_id = str(getattr(current_page, "tab_id", "") or "")
        baseline_tab_ids = set(getattr(current_page, "tab_ids", []) or [])
        try:
            try:
                doi_link.click()
            except Exception:
                doi_link.click(by_js=True)
            time.sleep(0.8)
            current_page = _adopt_latest_tab(current_page, logger=logger)
            now_url = str(getattr(current_page, "url", "") or "")
            if _is_elsevier_retrieve_url(now_url) and target_pii:
                recover_url = f"https://www.sciencedirect.com/science/article/pii/{target_pii}"
                if logger:
                    logger.info(f"        [Elsevier] DOI 재진입 후 retrieve 감지 -> 복귀: {recover_url}")
                try:
                    current_page.get(recover_url, retry=0, interval=0.3, timeout=8)
                except Exception:
                    pass
            _dismiss_cookie_or_consent_banner(current_page, logger=logger)
            reentry_title = str(getattr(current_page, "title", "") or "")
            reentry_html = str(getattr(current_page, "html", "") or "")
            reentry_domain = _extract_domain(getattr(current_page, "url", "") or "")
            reentry_issue, reentry_evidence = detect_access_issue(
                title=reentry_title,
                html=reentry_html,
                url=getattr(current_page, "url", "") or "",
                domain=reentry_domain,
            )
            if reentry_issue in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                if logger:
                    logger.info(
                        "        [Elsevier] DOI 재진입 결과가 차단/캡차여서 원래 article context로 복귀 후 계속: "
                        f"{reentry_issue}, {reentry_evidence}"
                    )
                current_tab_id = str(getattr(current_page, "tab_id", "") or "")
                if current_tab_id and origin_tab_id and current_tab_id != origin_tab_id:
                    try:
                        origin_page.close_tabs(current_tab_id)
                    except Exception:
                        try:
                            current_page.close()
                        except Exception:
                            pass
                    try:
                        restored = origin_page.get_tab(origin_tab_id)
                        current_page = restored if restored is not None else origin_page
                    except Exception:
                        current_page = origin_page
                elif target_pii:
                    recover_candidates = _build_elsevier_article_candidates(
                        target_pii,
                        article_referer,
                        getattr(current_page, "url", "") or "",
                    )
                    for recover_url in recover_candidates:
                        try:
                            current_page.get(recover_url, retry=0, interval=0.3, timeout=8)
                            break
                        except Exception:
                            continue
                _dismiss_cookie_or_consent_banner(current_page, logger=logger)
                _dismiss_elsevier_aux_overlays(current_page, logger=logger)
                _close_new_tabs_since(current_page, baseline_tab_ids)
            if logger:
                logger.info("        [Elsevier] DOI 링크 재진입 전략 적용")
            return current_page, True
        except Exception:
            return current_page, False

    # ScienceDirect는 View PDF 버튼 id/aria 기반 렌더링이 많아 대표 버튼 1회만 누른다.
    quick_locators = [
        'css:#viewpdf',
        'css:a#viewpdf',
        'css:button#viewpdf',
        'css:[id*="viewpdf"]',
        'css:[aria-label*="View PDF"]',
        'css:[aria-label*="view pdf"]',
    ]
    step1 = None
    for loc in quick_locators:
        candidates = _eles_quick(page, loc, timeout=0.5)
        filtered = []
        for el in candidates:
            context_blob = _element_context_blob(el)
            if _is_elsevier_aux_overlay_blob(context_blob):
                continue
            blob = f"{(el.text or '').strip()} {(el.attr('aria-label') or '').strip()} {(el.attr('class') or '').strip()}".lower()
            score = 0
            if "accessbar" in context_blob:
                score += 8
            if "view pdf" in blob:
                score += 4
            if "_blank" in blob or "opens in a new window" in context_blob:
                score += 2
            filtered.append((score, el))
        if filtered:
            filtered.sort(key=lambda item: item[0], reverse=True)
            step1 = filtered[0][1]
            break

    # 1) Article page에서 Open/View/Download PDF 클릭
    article_xpaths = [
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
    ]
    if not step1:
        for _ in range(8):
            step1 = _select_best_clickable_pdf_element(
                page,
                article_xpaths,
                logger=logger,
                must_tokens=token_candidates,
            )
            if not step1:
                step1 = _select_best_clickable_pdf_element(page, article_xpaths, logger=logger)
            if step1:
                break
            time.sleep(0.5)
    if logger:
        logger.info(f"        [Elsevier] step1 control: {_summarize_elsevier_pdf_control(step1)}")

    if step1 and _click_once_wait_file(
        page,
        step1,
        tmp_dir,
        tmp_path,
        wait_s=8,
        logger=logger,
        post_click_guard=_context_guard,
        downloaded_file_guard=_file_guard,
        fast_exit_on_new_tab=True,
        allow_js_fallback=False,
    ):
        if logger:
            logger.info(f"        [Elsevier] 1단계 클릭으로 다운로드 성공: {doi}")
        return _ret(True, page)
    if _finalize_existing_downloads_in_dir(tmp_dir, tmp_path, logger=logger, downloaded_file_guard=_file_guard):
        if logger:
            logger.info(f"        [Elsevier] 1단계 지연 다운로드 정리 성공: {doi}")
        return _ret(True, page)

    # View PDF가 새 탭으로 열리는 케이스 대응
    page = _adopt_elsevier_target_tab(page, doi_norm=doi_norm, target_pii=target_pii, logger=logger)
    _dismiss_cookie_or_consent_banner(page, logger=logger)
    _dismiss_elsevier_aux_overlays(page, logger=logger)
    viewer_state = _wait_for_elsevier_viewer_ready(page, logger=logger, timeout_s=6)
    if (not viewer_state) and step1:
        page, doi_reentry_used = _attempt_doi_reentry_after_dead_click(page)
        if doi_reentry_used:
            article_referer = str(getattr(page, "url", "") or article_referer)
            page = _adopt_elsevier_target_tab(page, doi_norm=doi_norm, target_pii=target_pii, logger=logger)
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            _dismiss_elsevier_aux_overlays(page, logger=logger)
            viewer_state = _wait_for_elsevier_viewer_ready(page, logger=logger, timeout_s=4)
    if (not viewer_state) and _is_elsevier_target_page(page, doi_norm, target_pii):
        _dismiss_elsevier_aux_overlays(page, logger=logger)
        retry_step1 = _select_best_clickable_pdf_element(
            page,
            article_xpaths,
            logger=logger,
            must_tokens=token_candidates,
            ban_tokens=["recommended", "related", "reading assistant"],
        )
        if retry_step1 and _click_once_wait_file(
            page,
            retry_step1,
            tmp_dir,
            tmp_path,
            wait_s=8,
            logger=logger,
            post_click_guard=_context_guard,
            downloaded_file_guard=_file_guard,
            fast_exit_on_new_tab=True,
            allow_js_fallback=False,
        ):
            if logger:
                logger.info(f"        [Elsevier] overlay 정리 후 재클릭 성공: {doi}")
            return _ret(True, page)
        page = _adopt_elsevier_target_tab(page, doi_norm=doi_norm, target_pii=target_pii, logger=logger)
        _dismiss_cookie_or_consent_banner(page, logger=logger)
        _dismiss_elsevier_aux_overlays(page, logger=logger)
        viewer_state = _wait_for_elsevier_viewer_ready(page, logger=logger, timeout_s=4)
    if viewer_state == "signed_pdf" and _download_elsevier_signed_pdf_from_viewer(
        page,
        tmp_path,
        referer_url=article_referer,
        logger=logger,
    ):
        if logger:
            logger.info(f"        [Elsevier] signed PDF viewer URL 다운로드 성공: {doi}")
        return _ret(True, page)

    # 2) viewer/pdfft 상태라면 Download 버튼 한 번 더 클릭
    current_url = str(page.url or "").lower()
    is_viewer_context = (
        ("/pdfft" in current_url)
        or (".pdf" in current_url)
        or (("sciencedirect.com" in current_url) and (not _is_elsevier_target_page(page, doi_norm, target_pii)))
    )
    if is_viewer_context:
        viewer_xpaths = [
            "//button[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
            "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            "//a[contains(@download,'.pdf')]",
        ]
        step2 = _select_best_clickable_pdf_element(page, viewer_xpaths, logger=logger, must_tokens=token_candidates)
        if not step2:
            step2 = _select_best_clickable_pdf_element(page, viewer_xpaths, logger=logger)
        if step2 and _click_once_wait_file(
            page,
            step2,
            tmp_dir,
            tmp_path,
            wait_s=8,
            logger=logger,
            post_click_guard=_context_guard,
            downloaded_file_guard=_file_guard,
            allow_js_fallback=False,
        ):
            if logger:
                logger.info(f"        [Elsevier] 2단계 클릭으로 다운로드 성공: {doi}")
            return _ret(True, page)
        if _finalize_existing_downloads_in_dir(tmp_dir, tmp_path, logger=logger, downloaded_file_guard=_file_guard):
            if logger:
                logger.info(f"        [Elsevier] 2단계 지연 다운로드 정리 성공: {doi}")
            return _ret(True, page)

    # Headless에서는 article shell이 떠도 View PDF가 dead control로 남는 경우가 있다.
    # 이때는 현재 탭에서 pdfft fallback으로 바로 내려가면 403이 자주 발생하므로
    # 같은 article URL을 fresh tab에서 한 번 더 hydrate시켜 재시도한다.
    if (
        allow_headless_fresh_tab_recovery
        and os.getenv("PDF_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes")
    ):
        current_url = str(getattr(page, "url", "") or "")
        candidates = _build_elsevier_article_candidates(target_pii, article_referer, current_url)
        if candidates:
            baseline_tab_ids = set(getattr(page, "tab_ids", []) or [])
            if logger:
                logger.info(
                    f"        [Elsevier] headless fresh-tab recovery 시작: candidates={len(candidates)}"
                )
            for idx, candidate in enumerate(candidates):
                temp_page = _open_temporary_tab(page)
                if temp_page is None:
                    break
                try:
                    if logger:
                        logger.info(f"        [Elsevier] fresh-tab candidate {idx + 1}: {candidate}")
                    temp_page.get(candidate, retry=0, interval=0.3, timeout=10)
                    _dismiss_cookie_or_consent_banner(temp_page, logger=logger)
                    _dismiss_elsevier_aux_overlays(temp_page, logger=logger)
                    _wait_for_elsevier_article_ready(
                        temp_page,
                        doi_norm,
                        logger=logger,
                        timeout_s=12,
                        strict=low_trust_session,
                    )
                    nested_ok, temp_page, nested_detail = _attempt_elsevier_two_step_click_download(
                        temp_page,
                        doi_norm,
                        tmp_dir,
                        tmp_path,
                        logger=logger,
                        allow_doi_reentry=True,
                        allow_headless_fresh_tab_recovery=False,
                        session_source=session_source,
                        low_trust_session=low_trust_session,
                        return_page=True,
                    )
                    if nested_ok:
                        if logger:
                            logger.info(
                                f"        [Elsevier] fresh-tab recovery 성공: candidate {idx + 1}"
                            )
                        return _ret(True, temp_page)
                    if str((nested_detail or {}).get("issue") or "") in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
                        if logger:
                            logger.info(
                                "        [Elsevier] fresh-tab recovery 중 차단 감지 -> 후보 순회 중단 "
                                f"{(nested_detail or {}).get('evidence') or []}"
                            )
                        break
                except Exception as e:
                    if logger:
                        logger.info(f"        [Elsevier] fresh-tab recovery 실패(계속): {e}")
                finally:
                    _close_temporary_tab(page, temp_page)
                    _close_new_tabs_since(page, baseline_tab_ids)
    return _ret(False, page)


def _has_article_signal(title: str = "", html: str = "") -> bool:
    t = (title or "").lower()
    h = (html or "").lower()
    hard_block_title = (
        "just a moment",
        "attention required",
        "validate user",
        "verify you are human",
        "redirecting",
        "are you a robot",
    )
    markers = (
        "name=\"citation_title\"",
        "name='citation_title'",
        "name=\"citation_doi\"",
        "name='citation_doi'",
        "name=\"citation_pdf_url\"",
        "name='citation_pdf_url'",
        "name=\"dc.identifier\"",
        "name='dc.identifier'",
        "\"@type\":\"scholarlyarticle\"",
        "\"@type\": \"scholarlyarticle\"",
        "schema.org/scholarlyarticle",
        "og:type\" content=\"article\"",
        "article-header",
        "article-title",
        "/doi/pdf/",
        "/pdfft?",
    )
    if len(t) >= 35 and not any(k in t for k in hard_block_title):
        return True
    return any(m in h for m in markers)


def _has_pdf_action_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    markers = (
        "view pdf",
        "open pdf",
        "download pdf",
        "/pdfft",
        "citation_pdf_url",
        "article-pdf",
        "downloadarticlepdf",
        ".pdf",
    )
    return any(m in blob for m in markers)


def _has_cookie_or_consent_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    markers = (
        "cookie",
        "consent",
        "manage preferences",
        "managepreferences",
        "accept all",
        "acceptall",
        "reject non-essential",
        "reject all",
        "privacy policy",
        "onetrust",
        "continue with only essential cookies",
    )
    return any(m in blob for m in markers)


def _has_purchase_or_institutional_gate_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    hard_rights_markers = (
        "you do not currently have access to this content",
        "you do not have access to this content",
    )
    if any(marker in blob for marker in hard_rights_markers):
        return True
    institutional_markers = (
        "full text access may be available",
        "organizational sign in",
        "organizational username",
        "organizational password",
        "institutional sign in",
        "select your institution to access",
        "sign in with credentials provided by your organization",
        "sign in with username and password",
        "access the spie digital library",
        "provided by your organization",
        "access through your institution",
        "sign in through your institution",
        "institutional login",
        "shibboleth",
        "openathens",
    )
    purchase_markers = (
        "purchase this content",
        "purchase single article",
        "purchase article",
        "purchase instant access",
        "buy this article",
        "subscribe to digital library",
        "subscribe to this journal",
        "subscription required",
        "pay per view",
        "rent this article",
        "add to cart",
    )
    institutional_hits = sum(1 for m in institutional_markers if m in blob)
    purchase_hits = sum(1 for m in purchase_markers if m in blob)
    return institutional_hits >= 3 or (institutional_hits >= 2 and purchase_hits >= 1) or purchase_hits >= 2


def _has_bot_wall_text_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    if "pardon our interruption" in blob:
        return True

    support_markers = (
        "as you were browsing",
        "super-human speed",
        "third-party browser plugin",
        "ghostery or noscript",
        "preventing javascript from running",
        "cookies and javascript are enabled before reloading the page",
    )
    support_hits = sum(1 for m in support_markers if m in blob)
    if support_hits >= 2:
        return True

    other_bot_markers = (
        "validate user",
        "verify you are human",
        "are you a robot",
        "unusual traffic",
        "request blocked",
        "making sure you're not a bot",
        "making sure you are not a bot",
        "anubis",
    )
    return any(m in blob for m in other_bot_markers)


def _wait_for_spie_article_ready(page, logger=None, timeout_s: int = 10) -> bool:
    if page is None:
        return False
    try:
        current_url = str(getattr(page, "url", "") or "").lower()
    except Exception:
        return False
    if "spiedigitallibrary.org" not in current_url:
        return False

    start = time.time()
    last_title = ""
    while (time.time() - start) < max(2, min(int(timeout_s), 18)):
        try:
            title = str(getattr(page, "title", "") or "").strip()
            html = str(getattr(page, "html", "") or "")
        except Exception:
            return False
        last_title = title or last_title
        if _has_purchase_or_institutional_gate_signal(title=title, html=html):
            if logger:
                logger.info("        [SPIE] access-control modal 감지 -> rights gate로 계속 진행")
            return True
        if _has_bot_wall_text_signal(title=title, html=html):
            if logger:
                logger.info("        [SPIE] bot wall 감지 -> article ready 대기 종료")
            return False
        if (
            _has_article_signal(title=title, html=html)
            or _has_pdf_action_signal(title=title, html=html)
            or ("citation_pdf_url" in html.lower())
        ):
            if logger:
                logger.info("        [SPIE] article/PDF 시그널 확인")
            return True
        time.sleep(0.8)

    if logger and last_title:
        logger.info(f"        [SPIE] article ready 대기 타임아웃: title={last_title[:120]!r}")
    return False


def _has_auth_required_signal(title: str = "", html: str = "") -> bool:
    blob = f"{title or ''} {html or ''}".lower()
    safe_markers = (
        "open pdf",
        "download pdf",
        "author version (pdf)",
        "open access",
    )
    if any(m in blob for m in safe_markers):
        return False
    strong_markers = (
        "password required",
        "password to view",
        "please enter your password",
        "enter password",
        "password protected",
        "authenticated access only",
    )
    if any(m in blob for m in strong_markers):
        return True
    # 단독 authenticate 텍스트(스크립트/메타) 오탐 방지: password 문맥 동반 시에만 차단
    return ("authenticate" in blob or "authentication required" in blob) and ("password" in blob)


def _classify_access_gate(title: str = "", html: str = "") -> str:
    title_blob = (title or "").lower()
    html_blob = (html or "").lower()
    blob = f"{title_blob} {html_blob}"
    article_like = _has_article_signal(title=title, html=html)
    pdf_action_like = _has_pdf_action_signal(title=title, html=html)
    consent_like = _has_cookie_or_consent_signal(title=title, html=html)

    safe_markers = (
        "open pdf",
        "download pdf",
        "open access",
        "free access",
        "free full text",
        "author version (pdf)",
    )
    if any(m in blob for m in safe_markers):
        return "none"

    hard_rights_markers = (
        "401 - unauthorized",
        "access is denied due to invalid credentials",
        "do not have permission to view this directory or page",
        "you do not currently have access to this content",
        "you do not have access to this content",
        "password required",
        "password protected",
        "authenticated access only",
    )
    if any(m in blob for m in hard_rights_markers):
        return "hard_rights"

    weak_bot_like_markers = (
        "security check",
        "too many requests",
        "access denied",
        "forbidden",
    )
    if _has_bot_wall_text_signal(title=title, html=html):
        return "bot_like"
    weak_hits = [m for m in weak_bot_like_markers if m in blob]
    if weak_hits:
        if any(m in title_blob for m in weak_bot_like_markers):
            return "bot_like"
        if (article_like or pdf_action_like or consent_like) and len(weak_hits) < 2:
            return "none"
        return "bot_like"

    login_markers = (
        "institutional login",
        "institutional sign in",
        "organizational sign in",
        "organizational username",
        "organizational password",
        "sign in through your institution",
        "sign in with credentials provided by your organization",
        "log in to wiley online library",
        "access through your institution",
        "select your institution to access",
        "shibboleth",
        "openathens",
    )
    paywall_markers = (
        "purchase this content",
        "purchase instant access",
        "purchase article",
        "purchase single article",
        "buy this article",
        "get access to the full version of this article",
        "subscribe to digital library",
        "subscribe to this journal",
        "subscription required",
        "pay per view",
        "rent this article",
        "add to cart",
    )
    login_hit = any(m in blob for m in login_markers)
    paywall_hits = sum(1 for m in paywall_markers if m in blob)
    if _has_purchase_or_institutional_gate_signal(title=title, html=html):
        return "soft_gate"

    # 기사/DOI/PDF 시그널이 있는 정상 랜딩에서 네비 메뉴 문구만으로 권한 실패를 내지 않도록 보수 처리
    if article_like or pdf_action_like:
        if login_hit and paywall_hits >= 2:
            return "soft_gate"
        return "none"

    if login_hit and paywall_hits >= 1:
        return "soft_gate"
    if paywall_hits >= 2:
        return "soft_gate"
    return "none"


def _has_access_rights_required_signal(title: str = "", html: str = "") -> bool:
    return _classify_access_gate(title=title, html=html) in ("hard_rights", "soft_gate")


def _force_accept_cookie_banner(page, logger=None) -> bool:
    if page is None:
        return False
    locators = [
        "css:#onetrust-accept-btn-handler",
        "css:button[id*='onetrust-accept']",
        "css:button[aria-label*='Accept']",
        "css:button[title*='Accept']",
        "text:Accept all cookies",
        "text:Accept all",
        "text:I agree",
    ]
    for loc in locators:
        el = _ele_quick(page, loc, timeout=0.45)
        if not el:
            continue
        try:
            try:
                el.click(by_js=True)
            except Exception:
                el.click()
            if logger:
                logger.info("        [ConsentGate] 강제 쿠키 수락 버튼 클릭")
            time.sleep(0.5)
            return True
        except Exception:
            continue

    # Shadow DOM / same-origin iframe 안의 동의 버튼까지 탐색
    js = r"""
(() => {
  const textHits = ['accept all cookies', 'accept all', 'i agree', 'agree'];
  const idHits = ['onetrust-accept', 'accept-btn'];
  const clicked = new Set();
  function hit(el) {
    const id = ((el.id || '') + ' ' + (el.getAttribute('id') || '')).toLowerCase();
    const label = ((el.innerText || '') + ' ' + (el.value || '') + ' ' + (el.getAttribute('aria-label') || '') + ' ' + (el.getAttribute('title') || '')).toLowerCase();
    return idHits.some(k => id.includes(k)) || textHits.some(k => label.includes(k));
  }
  function clickIn(root) {
    if (!root) return false;
    const cand = root.querySelectorAll ? root.querySelectorAll('button,a,input[type="button"],input[type="submit"]') : [];
    for (const el of cand) {
      if (clicked.has(el)) continue;
      if (!hit(el)) continue;
      try { el.click(); clicked.add(el); return true; } catch(e) {}
    }
    const all = root.querySelectorAll ? root.querySelectorAll('*') : [];
    for (const el of all) {
      try {
        if (el.shadowRoot && clickIn(el.shadowRoot)) return true;
      } catch(e) {}
    }
    const ifr = root.querySelectorAll ? root.querySelectorAll('iframe') : [];
    for (const fr of ifr) {
      try {
        const d = fr.contentDocument || (fr.contentWindow && fr.contentWindow.document);
        if (d && clickIn(d)) return true;
      } catch(e) {}
    }
    return false;
  }
  return clickIn(document);
})();
"""
    try:
        ok = page.run_js(js)
        if ok:
            if logger:
                logger.info("        [ConsentGate] JS 쿠키 수락 버튼 클릭")
            time.sleep(0.5)
            return True
    except Exception:
        pass
    return False


def _dismiss_cookie_or_consent_banner(page, logger=None) -> bool:
    if page is None:
        return False
    if _force_accept_cookie_banner(page, logger=logger):
        return True
    xpaths = [
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'acceptall')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'i agree')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'agree')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'acceptall')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'reject non-essential')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'reject all')]",
        "//button[contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'onetrust-accept')]",
        # survey/feedback modal 닫기
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'no thanks')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'no thanks')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'no thank you')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'no thank you')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'not now')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'not now')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'maybe later')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'dismiss')]",
        "//button[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'close')]",
        "//button[contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'close')]",
        "//button[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'close')]",
        "//button[normalize-space(string(.))='×' or normalize-space(string(.))='x' or normalize-space(string(.))='X']",
    ]
    for xp in xpaths:
        elems = _eles_quick(page, f"xpath:{xp}", timeout=0.5)
        for el in elems:
            try:
                if not el.states.is_displayed:
                    continue
            except Exception:
                pass
            try:
                el.click(by_js=True)
            except Exception:
                try:
                    el.click()
                except Exception:
                    continue
            if logger:
                logger.info("        [Overlay] 쿠키/동의/설문 팝업 클릭 처리")
            time.sleep(0.8)
            return True
    return False


def _click_viewer_open_button(page, logger=None, return_detail: bool = False):
    if page is None:
        return {"clicked": False, "href": "", "label": ""} if return_detail else False
    current_url = str(getattr(page, "url", "") or "").lower()
    pdf_context = any(k in current_url for k in (".pdf", "/pdfft", "/doi/pdf", "/epdf", "/pdf/"))
    xpaths = [
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view pdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download pdf')]",
        "//button[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'pdf')]",
        "//a[contains(@href,'.pdf') or contains(@href,'/pdfft') or contains(@href,'/doi/pdf') or contains(@href,'articlepdf')]",
        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open')]",
        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'open')]",
    ]
    for xp in xpaths:
        elems = _eles_quick(page, f"xpath:{xp}", timeout=0.5)
        for el in elems:
            try:
                if not el.states.is_displayed:
                    continue
            except Exception:
                pass
            text = (el.text or "").strip().lower()
            title = (el.attr("title") or "").strip().lower()
            aria = (el.attr("aria-label") or "").strip().lower()
            href = (el.attr("href") or "").strip().lower()
            blob = f"{text} {title} {aria} {href}"
            if _is_supporting_info_blob(blob) or any(
                k in blob for k in ("figure", "dataset", "powerpoint", "citation", "export")
            ):
                continue
            has_pdf_signal = any(k in blob for k in ("pdf", ".pdf", "/pdfft", "/doi/pdf", "articlepdf", "download"))
            is_generic_open = ("open" in blob) and (not has_pdf_signal)
            if is_generic_open and (not pdf_context):
                continue
            try:
                try:
                    el.click(by_js=True)
                except Exception:
                    el.click()
                if logger:
                    logger.info("        [ViewerGate] Open/View 버튼 클릭 시도")
                time.sleep(1.0)
                if return_detail:
                    label = (text or title or aria or href).strip()
                    return {"clicked": True, "href": href, "label": label}
                return True
            except Exception:
                continue
    return {"clicked": False, "href": "", "label": ""} if return_detail else False


def _extract_rsc_article_pdf_url(page) -> str:
    if page is None:
        return ""
    current_url = str(getattr(page, "url", "") or "").strip()

    def normalize(url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            return ""
        if not raw.startswith("http"):
            try:
                raw = urljoin(current_url, raw)
            except Exception:
                return ""
        return raw

    try:
        meta_pdf = _ele_quick(page, 'css:meta[name="citation_pdf_url"]', timeout=0.2)
        if meta_pdf:
            content = normalize(meta_pdf.attr("content"))
            if _is_rsc_article_pdf_url(content):
                return content
    except Exception:
        pass

    try:
        links = _eles_quick(
            page,
            "xpath://a[contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'/content/articlepdf/') "
            "or contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article') "
            "or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article') "
            "or contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article') "
            "or contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'article pdf')]",
            timeout=0.3,
        )
        for link in links:
            href = normalize(link.attr("href"))
            text = (link.text or "").strip()
            title = (link.attr("title") or "").strip()
            aria = (link.attr("aria-label") or "").strip()
            if _is_supporting_info_blob(text, title, aria, href):
                continue
            if _is_rsc_article_pdf_url(href):
                return href
    except Exception:
        pass

    return ""


def _wait_for_rsc_article_pdf_ready(page, logger=None, timeout_s: int = 10) -> str:
    if page is None:
        return ""
    deadline = time.time() + max(0.5, float(timeout_s))
    while time.time() < deadline:
        article_pdf_url = _extract_rsc_article_pdf_url(page)
        if article_pdf_url:
            if logger:
                logger.info(f"        [RSC] article PDF 신호 확보: {article_pdf_url}")
            return article_pdf_url
        try:
            html_low = (page.html or "").lower()
        except Exception:
            html_low = ""
        if "download options" in html_low and "please wait" in html_low:
            time.sleep(0.6)
            continue
        time.sleep(0.35)
    return ""


def _collect_pdf_candidate_urls_from_page(page, logger=None) -> list:
    if page is None:
        return []

    current_url = str(getattr(page, "url", "") or "").strip()
    current_html = str(getattr(page, "html", "") or "")
    seen = set()
    candidates = []
    current_low = current_url.lower()

    def add(raw_url):
        url = str(raw_url or "").strip()
        if not url or url.startswith("javascript:") or url.startswith("blob:"):
            return
        if not url.startswith("http"):
            try:
                url = urljoin(current_url, url)
            except Exception:
                return
        low = url.lower()
        if not (
            _looks_like_pdf_link(url)
            or "silverchair.com/" in low
            or "watermark" in low
            or "stamp.jsp" in low
        ):
            return
        if url in seen:
            return
        seen.add(url)
        candidates.append(url)

    # Elsevier article pages often need a second-stage candidate expansion from
    # the article HTML itself, even when no explicit PDF href is present yet.
    if "sciencedirect.com" in current_low and "/science/article/" in current_low:
        add(current_url)
        try:
            target_pii = _extract_sciencedirect_pii_from_text(current_url) or _extract_sciencedirect_pii_from_text(current_html)
            pdfft_url = _extract_sciencedirect_pdfft_url_from_html(current_html, target_pii=target_pii)
            add(pdfft_url)
        except Exception:
            pass
    else:
        add(current_url)

    try:
        meta_pdf = _ele_quick(page, 'css:meta[name="citation_pdf_url"]', timeout=0.2)
        if meta_pdf:
            add(meta_pdf.attr("content"))
    except Exception:
        pass

    try:
        analyzed = _analyze_html_structure_drission(page, logger)
        add(analyzed)
    except Exception:
        pass

    try:
        links = _eles_quick(
            page,
            "xpath://a[contains(@href,'.pdf') or contains(@href,'/pdfft') or contains(@href,'/doi/pdf') or contains(@href,'article-pdf') or contains(@href,'stamp.jsp') or contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'stamppdf/getpdf.jsp')]",
            timeout=0.3,
        )
        for link in links[:12]:
            add(link.attr("href"))
    except Exception:
        pass

    try:
        frames = _eles_quick(page, 'css:iframe, embed, object', timeout=0.3)
        for frame in frames[:8]:
            add(frame.attr("src") or frame.attr("data"))
    except Exception:
        pass

    if logger and candidates:
        logger.info(f"        [ViewerGate] 후보 URL {len(candidates)}개 수집")
    return candidates


def _try_cookie_cffi_candidate_urls(page, candidate_urls, target_path: str, logger=None, timeout_s: int = 16, context: str = "candidate-cffi") -> bool:
    if page is None:
        return False

    urls = []
    seen = set()
    for raw in candidate_urls or []:
        url = str(raw or "").strip()
        if not url or url in seen:
            continue
        low = url.lower()
        if low.startswith(("javascript:", "blob:", "data:")):
            continue
        if "stamp.jsp" in low and "stamppdf/getpdf.jsp" not in low:
            recovered_stamp = _recover_ieee_stamp_pdf_url(url)
            if recovered_stamp:
                url = recovered_stamp
        seen.add(url)
        urls.append(url)

    if not urls:
        return False

    try:
        cookies = {c["name"]: c["value"] for c in (page.cookies() or [])}
    except Exception:
        cookies = None
    referer = str(getattr(page, "url", "") or "").strip() or None
    ua = _resolve_best_browser_ua()

    for url in urls:
        low = url.lower()
        if "stamp.jsp" in low:
            continue
        if logger:
            logger.info(f"        [{context}] 후보 직접 수집 시도: {url}")
        try:
            cffi_result = download_with_cffi(
                url,
                target_path,
                referer=referer,
                cookies=cookies,
                ua=ua,
                logger=logger,
                return_detail=True,
                timeout=timeout_s,
            )
            if cffi_result.get("ok"):
                return True
        except Exception:
            continue
    return False


def _should_soft_continue_issue(
    issue: str,
    evidence: list,
    title: str,
    html: str,
    domain: str,
) -> bool:
    if issue not in ("FAIL_BLOCK", "FAIL_CAPTCHA"):
        return False
    ev = [str(x).lower() for x in (evidence or [])]
    if any("url_marker=challenge_or_bot" in e for e in ev):
        return False
    if any("keyword=access_gate_bot_like" in e for e in ev):
        return False
    if _has_auth_required_signal(title=title, html=html):
        return False
    if not _is_high_friction_domain(domain):
        return False

    if _has_cookie_or_consent_signal(title=title, html=html) and (
        _has_article_signal(title=title, html=html) or _has_pdf_action_signal(title=title, html=html)
    ):
        return True

    t = (title or "").lower()
    hard_title_markers = (
        "just a moment",
        "attention required",
        "validate user",
        "verify you are human",
        "are you a robot",
        "access denied",
    )
    if any(k in t for k in hard_title_markers):
        return False

    soft_markers = ("keyword=too many requests", "keyword=/cdn-cgi/challenge")
    if any(m in ev_item for ev_item in ev for m in soft_markers):
        return _has_article_signal(title=title, html=html) or _has_pdf_action_signal(title=title, html=html)

    return False


def _looks_like_challenge_url(url: str = "") -> bool:
    low = str(url or "").strip().lower()
    if not low:
        return False
    markers = (
        "__cf_chl_rt_tk=",
        "/cdn-cgi/challenge",
        "/cdn-cgi/l/chk_captcha",
        "challenges.cloudflare.com",
        "validate.perfdrive.com",
        "/captcha/",
    )
    return any(marker in low for marker in markers)


def _publisher_bootstrap_url_for_doi(doi_norm: str = "") -> str:
    doi_norm = str(doi_norm or "").strip().lower()
    if doi_norm.startswith("10.1016"):
        return "https://www.sciencedirect.com/"
    if doi_norm.startswith("10.1021"):
        return "https://pubs.acs.org/"
    if doi_norm.startswith("10.1063") or doi_norm.startswith("10.1116"):
        return "https://pubs.aip.org/"
    if doi_norm.startswith(("10.1088", "10.1149", "10.7567")):
        return "https://iopscience.iop.org/"
    if doi_norm.startswith("10.1103"):
        return "https://journals.aps.org/"
    if doi_norm.startswith("10.1002"):
        return "https://onlinelibrary.wiley.com/"
    return ""


def _should_prebootstrap_low_trust_publisher(doi_norm: str = "", session_source: str = "") -> bool:
    doi_norm = str(doi_norm or "").strip().lower()
    source = str(session_source or "").strip().lower()
    if not doi_norm.startswith(
        ("10.1016", "10.1021", "10.1063", "10.1116", "10.1088", "10.1149", "10.7567", "10.1103", "10.1002")
    ):
        return False
    if not source:
        return True
    return source == "temp" or source.startswith("temp_shared") or source.startswith("persistent_fallback")


def _session_bootstrap_marker_path(session_plan: Dict[str, Any], bootstrap_key: str) -> str:
    user_data_dir = os.path.abspath(str((session_plan or {}).get("user_data_dir") or "").strip())
    key = _sanitize_doi_to_filename(str(bootstrap_key or "").strip().lower()) or "bootstrap"
    if not user_data_dir:
        return ""
    return os.path.join(user_data_dir, f".codex_bootstrap_{key}.ready")


def _maybe_bootstrap_low_trust_publisher_session(
    page,
    doi_norm: str,
    session_plan: Dict[str, Any],
    logger=None,
    timeout_s: int = 10,
) -> bool:
    if page is None:
        return False
    session_source = str((session_plan or {}).get("session_source") or "")
    if not _should_prebootstrap_low_trust_publisher(doi_norm, session_source=session_source):
        return False
    bootstrap_url = _publisher_bootstrap_url_for_doi(doi_norm)
    if not bootstrap_url:
        return False

    marker_path = _session_bootstrap_marker_path(session_plan or {}, bootstrap_url)
    if marker_path and os.path.exists(marker_path):
        return True

    try:
        if logger:
            logger.info(
                "     [Drission] low-trust publisher bootstrap 선행 방문: "
                f"{bootstrap_url} (source={session_source or 'unknown'})"
            )
        page.get(bootstrap_url, retry=0, interval=0.3, timeout=min(max(6, int(timeout_s)), 12))
        _dismiss_cookie_or_consent_banner(page, logger=logger)
        try:
            _dismiss_elsevier_aux_overlays(page, logger=logger)
        except Exception:
            pass
        time.sleep(1.0)
        if marker_path:
            try:
                with open(marker_path, "w", encoding="utf-8") as f:
                    f.write(f"{int(time.time())}\n{bootstrap_url}\n")
            except Exception:
                pass
        return True
    except Exception as e:
        if logger:
            logger.info(f"     [Drission] publisher bootstrap 실패(계속): {e}")
        return False


def _resolve_preferred_browser_entry_url(
    doi_url: str,
    doi_norm: str = "",
    session_source: str = "",
    logger=None,
) -> str:
    raw = str(doi_url or "").strip()
    if not raw:
        return ""
    if not _should_prebootstrap_low_trust_publisher(doi_norm, session_source=session_source):
        return raw
    if str(doi_norm or "").startswith("10.1103/"):
        aps_url = _build_aps_first_party_article_url(doi_norm)
        if aps_url:
            return aps_url
    if str(doi_norm or "").startswith(("10.1063/", "10.1116/")):
        aip_url = _resolve_aip_structural_entry_url(raw)
        if aip_url:
            return aip_url
    resolved_target = _resolve_doi_redirect_target(raw, logger=logger)
    if resolved_target and not _looks_like_challenge_url(resolved_target):
        return resolved_target
    return raw


def _snapshot_page_access_issue(page) -> Dict[str, Any]:
    if page is None:
        return {
            "issue": "FAIL_BLOCK",
            "evidence": ["page_missing"],
            "title": "",
            "html": "",
            "url": "",
            "domain": "",
        }
    try:
        title = str(getattr(page, "title", "") or "")
    except Exception:
        title = ""
    try:
        html = str(getattr(page, "html", "") or "")
    except Exception:
        html = ""
    try:
        url = str(getattr(page, "url", "") or "")
    except Exception:
        url = ""
    domain = _extract_domain(url)
    issue, evidence = detect_access_issue(title=title, html=html, url=url, domain=domain)
    return {
        "issue": issue,
        "evidence": list(evidence or []),
        "title": title,
        "html": html,
        "url": url,
        "domain": domain,
    }


def _wait_for_challenge_clear(page, logger=None, timeout_s: int = 8) -> Dict[str, Any]:
    deadline = time.time() + max(2, int(timeout_s))
    last_snapshot = _snapshot_page_access_issue(page)
    while time.time() < deadline:
        if last_snapshot.get("issue") not in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
            return last_snapshot
        time.sleep(1.0)
        _dismiss_cookie_or_consent_banner(page, logger=logger)
        try:
            _dismiss_elsevier_aux_overlays(page, logger=logger)
        except Exception:
            pass
        last_snapshot = _snapshot_page_access_issue(page)
    return last_snapshot


def _attempt_publisher_landing_challenge_recovery(
    page,
    doi_url: str,
    doi_norm: str,
    logger=None,
    timeout_s: int = 10,
    session_source: str = "",
) -> tuple[Any, Dict[str, Any], bool]:
    snapshot = _wait_for_challenge_clear(page, logger=logger, timeout_s=min(10, timeout_s))
    if snapshot.get("issue") not in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
        if logger:
            logger.info("        [LandingRecovery] challenge settle 후 landing 복구")
        return page, snapshot, True

    bootstrap_url = _publisher_bootstrap_url_for_doi(doi_norm)
    if bootstrap_url:
        try:
            if logger:
                logger.info(f"        [LandingRecovery] bootstrap 페이지 선행 방문: {bootstrap_url}")
            page.get(bootstrap_url, retry=0, interval=0.3, timeout=min(timeout_s, 12))
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            time.sleep(1.0)
        except Exception as e:
            if logger:
                logger.info(f"        [LandingRecovery] bootstrap 실패(계속): {e}")

    targets = []
    seen = set()

    def _push(url: str) -> None:
        raw = str(url or "").strip()
        if not raw:
            return
        if raw in seen:
            return
        seen.add(raw)
        targets.append(raw)

    current_url = str(snapshot.get("url") or "").strip()
    if str(doi_norm or "").startswith("10.1016"):
        target_pii = _extract_sciencedirect_pii_from_url(current_url) or _extract_sciencedirect_pii_from_text(snapshot.get("html") or "")
        for candidate in _build_elsevier_article_candidates(
            target_pii,
            current_url,
            _resolve_doi_redirect_target(doi_url, logger=logger),
        ):
            _push(candidate)
    else:
        preferred_target = _resolve_preferred_browser_entry_url(
            doi_url,
            doi_norm=doi_norm,
            session_source=session_source,
            logger=logger,
        )
        _push(preferred_target)
        resolved_target = _resolve_doi_redirect_target(doi_url, logger=logger)
        _push(resolved_target)
        stripped_current = _strip_challenge_markers_from_url(current_url)
        _push(stripped_current)
        if current_url and (not _looks_like_challenge_url(current_url)):
            _push(current_url)

    for target in targets:
        if _looks_like_challenge_url(target):
            continue
        try:
            if logger:
                logger.info(f"        [LandingRecovery] target 재진입: {target}")
            page.get(target, retry=0, interval=0.3, timeout=min(timeout_s, 12))
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            settled = _wait_for_challenge_clear(page, logger=logger, timeout_s=min(10, timeout_s))
            if settled.get("issue") not in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                if logger:
                    logger.info("        [LandingRecovery] target 재진입 성공")
                return page, settled, True
            snapshot = settled
        except Exception as e:
            if logger:
                logger.info(f"        [LandingRecovery] target 재진입 실패(계속): {e}")
            continue

    return page, snapshot, False


def _has_doi_not_found_signal(title: str = "", html: str = "", url: str = "", domain: str = "") -> bool:
    t = (title or "").lower()
    h = (html or "").lower()
    u = (url or "").lower()
    d = (domain or "").lower()
    if (not d) and u:
        d = _extract_domain(u)
    if "doi.org" not in d and "doi.org" not in u:
        return False

    markers = (
        "doi not found",
        "error: doi not found",
        "this doi cannot be found in the doi system",
        "the doi has not been activated yet",
        "report an error",
    )
    return any(marker in t or marker in h for marker in markers)


def detect_access_issue(title: str = "", html: str = "", http_status: int = None, url: str = "", domain: str = ""):
    """
    캡차/차단 신호를 감지해 (reason, evidence)를 반환.
    reason: FAIL_CAPTCHA | FAIL_BLOCK | FAIL_ACCESS_RIGHTS | FAIL_DOI_NOT_FOUND | None
    """
    t = (title or "").lower()
    h = (html or "").lower()
    u = (url or "").lower()
    d = (domain or "").lower()
    if (not d) and u:
        d = _extract_domain(u)
    evidence = []
    article_like = _has_article_signal(title=title, html=html)
    pdf_action_like = _has_pdf_action_signal(title=title, html=html)
    consent_like = _has_cookie_or_consent_signal(title=title, html=html)
    auth_required_like = _has_auth_required_signal(title=title, html=html)
    access_gate = _classify_access_gate(title=title, html=html)
    assume_inst_access = os.getenv("PDF_ASSUME_INSTITUTION_ACCESS", "0").strip().lower() in ("1", "true", "yes")
    rich_article_abstract = article_like and ("citation_doi" in h or "citation_title" in h) and ("abstract" in h)
    challenge_url_markers = (
        "__cf_chl_rt_tk=",
        "/cdn-cgi/challenge",
        "/cdn-cgi/l/chk_captcha",
        "challenges.cloudflare.com",
        "validate.perfdrive.com",
        "/captcha/",
    )
    if any(m in u for m in challenge_url_markers):
        evidence.append("url_marker=challenge_or_bot")
        return "FAIL_BLOCK", evidence
    if ("pubs.aip.org" in d) and ("__cf_chl_rt_tk=" in u):
        evidence.append("url_marker=aip_cloudflare_challenge")
        return "FAIL_BLOCK", evidence
    if _has_doi_not_found_signal(title=title, html=html, url=url, domain=d):
        evidence.append("keyword=doi_not_found")
        if "doi.org" in d or "doi.org" in u:
            evidence.append("domain=doi.org")
        return "FAIL_DOI_NOT_FOUND", evidence

    if auth_required_like:
        if rich_article_abstract or (article_like and pdf_action_like):
            evidence.append("soft=auth_header_present_on_article")
            return None, evidence
        evidence.append("keyword=auth_required")
        return "FAIL_ACCESS_RIGHTS", evidence
    if access_gate == "hard_rights":
        evidence.append("keyword=access_rights_required_hard")
        return "FAIL_ACCESS_RIGHTS", evidence
    if access_gate == "soft_gate":
        evidence.append("keyword=access_gate_soft")
        if assume_inst_access:
            evidence.append("policy=assume_institution_access")
            return "FAIL_BLOCK", evidence
        return "FAIL_ACCESS_RIGHTS", evidence
    if access_gate == "bot_like":
        evidence.append("keyword=access_gate_bot_like")
        return "FAIL_BLOCK", evidence

    # Cookie/consent overlay가 뜬 정상 페이지를 차단으로 오판하지 않도록 우선 예외처리
    if consent_like and (article_like or pdf_action_like):
        evidence.append("soft=consent_gate_detected")
        return None, evidence

    # Avoid false positives from normal pages that include analytics/captcha-related assets.
    title_captcha_keywords = [
        "pardon our interruption",
        "just a moment",
        "잠시만",
        "verify you are human",
        "are you human",
        "are you a robot",
        "i am not a robot",
        "validate user",
        "making sure you're not a bot",
        "making sure you are not a bot",
    ]
    title_block_keywords = [
        "attention required",
        "access denied",
        "forbidden",
        "request blocked",
        "too many requests",
        "security check",
    ]
    html_captcha_markers = [
        "pardon our interruption",
        "as you were browsing",
        "cf-turnstile",
        "challenge-form",
        "captcha-box",
        "validate user",
        "verify you are human",
        "are you a robot",
        "super-human speed",
        "ghostery or noscript",
        "third-party browser plugin",
        "making sure you're not a bot",
        "making sure you are not a bot",
        "anubis",
        "preventing javascript from running",
        "cookies and javascript are enabled before reloading the page",
    ]
    html_block_markers = [
        "error code 1020",
        "request blocked",
        "too many requests",
        "access denied",
        "forbidden",
        "cloudflare_error_1000",
        "/cdn-cgi/challenge",
        "unusual traffic",
    ]

    if http_status in (403, 429):
        evidence.append(f"http_status={http_status}")
        return "FAIL_BLOCK", evidence

    for kw in title_captcha_keywords:
        if kw in t or kw in h:
            evidence.append(f"keyword={kw}")
            return "FAIL_CAPTCHA", evidence

    for kw in title_block_keywords:
        if kw in t:
            if kw in ("attention required", "access denied") and _has_purchase_or_institutional_gate_signal(title=title, html=html):
                evidence.append(f"soft_keyword={kw}")
                continue
            if kw in ("forbidden", "too many requests", "security check") and (article_like or pdf_action_like or consent_like):
                evidence.append(f"soft_keyword={kw}")
                continue
            evidence.append(f"keyword={kw}")
            return "FAIL_BLOCK", evidence

    for kw in html_captcha_markers:
        if kw in h:
            evidence.append(f"keyword={kw}")
            return "FAIL_CAPTCHA", evidence

    for kw in html_block_markers:
        if kw in t or kw in h:
            if kw in ("forbidden", "too many requests", "/cdn-cgi/challenge", "access denied") and (
                article_like or pdf_action_like or consent_like
            ):
                evidence.append(f"soft_keyword={kw}")
                continue
            evidence.append(f"keyword={kw}")
            return "FAIL_BLOCK", evidence

    return None, evidence


def _resolve_pdf_pipeline_mode() -> str:
    env_mode = os.getenv("PDF_PIPELINE_MODE", "").strip().lower()
    if env_mode in ("baseline", "candidate"):
        return env_mode

    report_path = os.path.abspath(os.path.join("outputs", "benchmark_report.json"))
    if os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)
            if ((report.get("gate") or {}).get("passed")) is True:
                return "candidate"
        except Exception:
            pass

    return "baseline"


def _should_force_candidate_retry(url: str, final_url: str, reason: str) -> bool:
    if reason not in (REASON_FAIL_VIEWER_HTML, REASON_FAIL_WRONG_MIME):
        return False
    blob = f"{url or ''} {final_url or ''}".lower()
    candidate_markers = (
        "sciencedirect.com",
        "/pdfft",
        "ieeexplore.ieee.org",
        "stamppdf/getpdf.jsp",
        "stamp.jsp",
        "onlinelibrary.wiley.com",
        "advanced.onlinelibrary.wiley.com",
        "/doi/pdf",
        "/articlepdf",
    )
    return any(marker in blob for marker in candidate_markers)


# =======================================================
# Download Logics
# =======================================================
# 1. JS Injection (DrissionPage 버전)
# =======================================================

def download_pdf_via_js_injection(page, url, filename, save_dir, logger):
    """
    DrissionPage의 run_js를 사용하여 비동기 fetch 수행 후 Base64 데이터 반환
    """
    logger.info(f"  [Drission] JS Fetch & Base64 Return 시도: {url[:80]}...")
    
    # DrissionPage는 run_js에 인자를 전달하면 자동으로 함수로 래핑하여 실행합니다.
    # Promise를 리턴하면 Python에서 await되어 결과값을 받을 수 있습니다.
    js_script = """
        var targetUrl = arguments[0];
        
        // async 함수 정의 및 즉시 실행하여 Promise 반환
        return (async function(url) {
            // 뷰어 내부라면 src 사용 보정
            if (url === window.location.href) {
                var embed = document.querySelector('embed[type="application/pdf"]');
                if (embed && embed.src) url = embed.src;
            }

            try {
                const response = await fetch(url);
                if (!response.ok) throw new Error('Network response was not ok: ' + response.status);
                
                var ctype = response.headers.get('content-type');
                if (ctype && (ctype.includes('text/html') || ctype.includes('application/json'))) {
                    throw new Error('DETECTED_HTML_OR_JSON');
                }
                
                const blob = await response.blob();
                if (blob.size < 2000) throw new Error('TOO_SMALL');
                
                // Blob -> Base64 변환
                return await new Promise((resolve, reject) => {
                    var reader = new FileReader();
                    reader.readAsDataURL(blob); 
                    reader.onloadend = function() {
                        resolve(reader.result); // 성공 시 데이터 리턴
                    };
                    reader.onerror = function(err) {
                        reject("FAILED: " + err.message);
                    };
                });

            } catch (error) {
                if (error.message === 'DETECTED_HTML_OR_JSON') return "DETECTED_HTML_OR_JSON";
                return "FAILED: " + error.message;
            }
        })(targetUrl);
    """
    
    try:
        # 60초 타임아웃 설정은 DrissionPage 옵션이나 로직으로 처리 필요하지만, 
        # run_js 자체는 동기적으로 결과를 기다림 (내부적으로 CDP awaitPromise 사용)
        result = page.run_js(js_script, url)
        
        # 1. 실패/에러 케이스 처리
        if not result or str(result).startswith("FAILED"):
            logger.warning(f"     JS Fetch 실패: {result}")
            return False
        
        if str(result) == "DETECTED_HTML_OR_JSON":
            logger.warning("     JS HTML 감지됨")
            return False

        # 2. 성공 케이스 (Base64 데이터 수신)
        if str(result).startswith("data:"):
            # "data:application/pdf;base64," 헤더 제거
            try:
                header, encoded = str(result).split(",", 1)
                data = base64.b64decode(encoded)
                
                # 파일 저장
                file_path = os.path.join(save_dir, filename)
                with open(file_path, "wb") as f:
                    f.write(data)
                    
                logger.info(f"     JS 데이터 수신 및 파일 저장 완료: {file_path}")
                return True
            except Exception as e:
                logger.error(f"     Base64 디코딩/저장 실패: {e}")
                return False
            
        return False

    except Exception as e:
        logger.error(f"     JS 실행 중 파이썬 에러: {e}")
        return False


# =======================================================
# 2. Requests Force Download (DrissionPage 연동)
# =======================================================

def force_download_with_requests(page, pdf_url, referer_url, save_path, logger):
    """
    DrissionPage의 쿠키와 User-Agent를 가져와 requests로 다운로드 시도
    """
    try:
        logger.info(f"requests 시도 (Referer: {referer_url})")
        
        # DrissionPage에서 쿠키 가져오기 page.cookies -> [dict, list]
        cookies = page.cookies()[0]
        
        session = requests.Session()
        session.cookies.update(cookies)
        
        # User-Agent 가져오기
        user_agent = page.user_agent
        
        headers = {
            "User-Agent": user_agent,
            "Referer": referer_url,
            "Accept": "application/pdf,application/x-pdf,*/*",
        }
        
        response = session.get(pdf_url, headers=headers, stream=True, timeout=12)
        
        if response.status_code == 200:
            ctype = response.headers.get("Content-Type", "").lower()
            if "html" in ctype or "json" in ctype:
                logger.error(f"requests 실패: 서버가 PDF 대신 {ctype}을 보냈습니다.")
                return False

            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            if _is_valid_pdf(save_path): # _is_valid_pdf는 tools_exp.py 내부에 정의된 함수 사용
                logger.info("  requests 다운로드 성공 (유효한 PDF)")
                return True
            else:
                logger.error("  requests 실패: 파일 손상/HTML 감지")
                if os.path.exists(save_path):
                    os.remove(save_path)
                return False
        return False
    except Exception as e:
        _raise_if_browser_disconnect(e, logger=logger, context="requests-force-download")
        logger.error(f"requests 오류: {e}")
        return False


# =======================================================
# 3. Navigation Download (DrissionPage 버전)
# =======================================================

def download_pdf_via_navigation(page, url, download_dir, logger, timeout_s=30):
    """
    브라우저 네비게이션 -> (가능하면) 버튼 클릭으로 다운로드.
    download_dir 인자는 하위호환을 위해 유지하지만 실제로는 target file path로 사용한다.
    """
    if logger is None:
        import logging
        logger = logging.getLogger("SafetyLogger")

    target_path = download_dir
    target_dir = os.path.dirname(target_path) if os.path.splitext(target_path)[1] else target_path
    os.makedirs(target_dir, exist_ok=True)

    logger.info(f"     브라우저 네비게이션 다운로드 시도: {url}")

    try:
        initial_files = _get_current_files(target_dir)

        # 1) 페이지 이동 (무제한 대기 방지)
        try:
            nav_timeout = max(6, min(int(timeout_s), 12))
        except Exception:
            nav_timeout = 10
        page.get(url, retry=0, interval=0.5, timeout=nav_timeout)
        time.sleep(random.uniform(0.4, 0.9))
        _dismiss_cookie_or_consent_banner(page, logger=logger)
        pdf_like_navigation = any(k in (url or "").lower() for k in (".pdf", "/pdfft", "download=true"))

        # direct PDF URL이면 이동만으로 다운로드가 시작될 수 있으므로 먼저 짧게 확인
        if pdf_like_navigation:
            if _capture_direct_downloaded_pdf(
                download_dir=target_dir,
                initial_files=initial_files,
                tmp_target_path=target_path,
                final_target_path=target_path,
                logger=logger,
                timeout_s=min(timeout_s, 8),
                context="navigation-direct-pdf",
            ):
                logger.info("        자동 다운로드 감지/확정")
                return target_path
            # 일부 publisher(SPIE 등)는 .pdf endpoint가 별도 HTML gate를 거치므로
            # access-rights 판정보다 먼저 same-page cookie를 싣고 직접 회수해 본다.
            navigation_candidates = [url]
            navigation_candidates.extend(_collect_pdf_candidate_urls_from_page(page, logger=logger))
            if _try_cookie_cffi_candidate_urls(
                page,
                navigation_candidates,
                target_path,
                logger=logger,
                timeout_s=min(max(8, timeout_s), 16),
                context="navigation-preauth-cffi",
            ):
                logger.info("        navigation 전환 직후 cookie-aware 직접 회수 성공")
                return target_path

        if _has_auth_required_signal(title=page.title or "", html=page.html or "") or _has_access_rights_required_signal(
            title=page.title or "",
            html=page.html or "",
        ):
            logger.warning("        인증/비밀번호 요구 페이지 감지 -> 다운로드 포기")
            return "__ACCESS_RIGHTS_REQUIRED__"

        # viewer 페이지에서 Open 버튼만 누르면 내려오는 경우 대응
        viewer_detail = _click_viewer_open_button(page, logger=logger, return_detail=True)
        page = _adopt_latest_tab(page, logger=logger)
        if _capture_direct_downloaded_pdf(
            download_dir=target_dir,
            initial_files=initial_files,
            tmp_target_path=target_path,
            final_target_path=target_path,
            logger=logger,
            timeout_s=min(timeout_s, 6),
            context="navigation-viewer-open",
        ):
            logger.info("        viewer Open/View 후 다운로드 감지/확정")
            return target_path
        viewer_candidates = []
        if viewer_detail.get("href"):
            viewer_candidates.append(viewer_detail["href"])
        viewer_candidates.extend(_collect_pdf_candidate_urls_from_page(page, logger=logger))
        if _try_cookie_cffi_candidate_urls(
            page,
            viewer_candidates,
            target_path,
            logger=logger,
            timeout_s=min(max(8, timeout_s), 18),
            context="navigation-viewer-candidate",
        ):
            logger.info("        viewer 후보 URL 직접 수집 성공")
            return target_path

        # 2) 버튼 클릭
        try:
            button_xpath = """
                //a[contains(@class, 'pdf') or contains(@title, 'Download') or contains(text(), 'View PDF') or contains(text(), 'Download PDF')] |
                //button[contains(text(), 'View PDF') or contains(text(), 'Download')] |
                //span[contains(text(), 'View PDF') or contains(text(), 'Download')] |
                //a[contains(text(), 'Open')] | //button[contains(text(), 'Open')] |
                //a[contains(@href, '.pdf')] |
                //button[@aria-label='Download'] |
                //button[@aria-label='Download this article'] |
                //a[@title='Download this article'] |
                //button[@title='Download this article'] |
                //a[@aria-label='Download this article'] |
                //*[@id='pdf-download-icon'] |
                //a[contains(text(), '원문보기')] |
                //a[contains(text(), 'PDF 다운로드')] |
                //a[contains(@title, '원문보기')] |
                //img[contains(@alt, 'PDF')] |
                //a[contains(@href, 'down') and contains(@href, 'pdf')]
            """
            buttons = _eles_quick(page, f'xpath:{button_xpath}', timeout=0.8)
            clicked = False
            clicked_href = ""
            for btn in buttons:
                if not btn.states.is_displayed:
                    continue
                text = (btn.text or "").strip()
                title = (btn.attr("title") or "").strip()
                aria = (btn.attr("aria-label") or "").strip()
                href = (btn.attr("href") or "").strip()
                blob = f"{text} {title} {aria} {href}".lower()

                # 이미지/보조자료/고해상도 버튼 제외: 같은 DOI에서 다중 파일 생성 방지
                if _is_supporting_info_blob(blob) or any(
                    k in blob
                    for k in (
                        "hi-res",
                        "image",
                        "figure",
                        "graphical abstract",
                        "dataset",
                        "powerpoint",
                        "ms-power",
                        "ppt",
                        "citation",
                        "export",
                        "bibtex",
                        "ris",
                    )
                ):
                    continue
                # PDF 성격의 버튼만 허용
                if not any(k in blob for k in ("pdf", "view pdf", "open pdf", "/pdfft", ".pdf", "articlepdf")):
                    continue

                btn_info = text or title or aria or "ICON"
                clicked_href = href
                logger.info(f"         버튼 발견: {btn_info[:20]}... 클릭 시도")
                try:
                    btn.click()
                    logger.info("        [Plan A] GUI 클릭 성공")
                except Exception:
                    logger.warning("        GUI 클릭 실패 -> [Plan B] JS 클릭 시도")
                    btn.click(by_js=True)
                clicked = True
                time.sleep(0.7)
                break

            if not clicked:
                logger.warning("        클릭할 PDF 버튼을 못 찾음 (이동만으로 다운로드됐을 수 있음)")
                _dismiss_cookie_or_consent_banner(page, logger=logger)
                _click_viewer_open_button(page, logger=logger)
            else:
                page = _adopt_latest_tab(page, logger=logger)
        except Exception as e:
            _raise_if_browser_disconnect(e, logger=logger, context="navigation-button-click")
            logger.warning(f"        버튼 클릭 로직 에러 (무시): {e}")

        # 3) 파일 생성 대기 및 확정
        if _capture_direct_downloaded_pdf(
            download_dir=target_dir,
            initial_files=initial_files,
            tmp_target_path=target_path,
            final_target_path=target_path,
            logger=logger,
            timeout_s=timeout_s,
            context="navigation-click-download",
        ):
            logger.info(f"        파일명/유효성 확인 완료: {target_path}")
            return target_path

        click_candidates = []
        if clicked_href:
            click_candidates.append(clicked_href)
        click_candidates.extend(_collect_pdf_candidate_urls_from_page(page, logger=logger))
        if _try_cookie_cffi_candidate_urls(
            page,
            click_candidates,
            target_path,
            logger=logger,
            timeout_s=min(max(8, timeout_s), 18),
            context="navigation-click-candidate",
        ):
            logger.info("        클릭 후 후보 URL 직접 수집 성공")
            return target_path

        page_src = (page.html or "")
        if "Forbidden" in page_src or "Access Denied" in page_src:
            logger.warning("        403 Forbidden 감지됨")
        elif "challenge" in page_src:
            logger.warning("        캡차 화면 감지됨")
        else:
            logger.warning("        파일 생성 안됨 (타임아웃)")
        return None
    except Exception as e:
        _raise_if_browser_disconnect(e, logger=logger, context="navigation-download")
        logger.error(f"        네비게이션 다운로드 중 에러: {e}")
        return None

# =======================================================
# 4. CFFI 다운로더
# =======================================================
def download_with_cffi(
    url,
    save_path,
    referer=None,
    cookies=None,
    ua=None,
    logger=None,
    return_detail=False,
    timeout=20,
    metrics_extra=None,
):
    if os.path.isdir(save_path):
        try: shutil.rmtree(save_path)
        except: pass

    try:
        timeout = max(2, min(int(timeout), MAX_ACTION_WAIT_S))
        if not ua:
            ua = _resolve_best_browser_ua()

        headers = {
            "User-Agent": ua,
            "Referer": referer if referer else "https://www.google.com",
            "Accept": "application/pdf,application/x-pdf,*/*",
            "Accept-Language": BEST_BROWSER_ACCEPT_LANGUAGE,
        }

        cookie_count = 0
        if cookies:
            if isinstance(cookies, dict): cookie_count = len(cookies)
            else: cookie_count = len(cookies)

        if logger:
            logger.info(f"        [CFFI] 다운로드 시도 (쿠키: {cookie_count}개)")

        pipeline_mode = _resolve_pdf_pipeline_mode()

        attempt = download_pdf(
            url,
            save_path,
            strategy_mode=pipeline_mode,
            timeout=timeout,
            min_size=1024,
            headers=headers,
            cookies=cookies,
            strategy_name=f"cffi_{pipeline_mode}",
            phase="direct",
        )

        # IEEE/Elsevier/Wiley의 direct PDF wrapper는 baseline에서 viewer HTML로
        # 되돌아오는 경우가 많아, 고마찰 URL에 한해 candidate expansion을 1회 더 시도한다.
        if pipeline_mode == "baseline" and _should_force_candidate_retry(url, attempt.final_url, attempt.reason):
            if logger:
                logger.info(
                    "        [CFFI] viewer HTML/wrong mime 감지 -> candidate recovery 재시도 "
                    f"(url={attempt.final_url or url})"
                )
            candidate_attempt = download_pdf(
                url,
                save_path,
                strategy_mode="candidate",
                timeout=timeout,
                min_size=1024,
                headers=headers,
                cookies=cookies,
                strategy_name="cffi_candidate",
                phase="direct_candidate_recovery",
                max_viewer_hops=2,
            )
            candidate_finished_at_ms = int(time.time() * 1000)
            candidate_started_at_ms = max(0, candidate_finished_at_ms - int(candidate_attempt.elapsed_ms or 0))
            append_metrics_jsonl(
                os.path.abspath(
                    os.environ.get("PDF_ATTEMPTS_JSONL", os.path.join("outputs", "download_attempts.jsonl"))
                ),
                candidate_attempt,
                extra={
                    "timestamp_ms": candidate_finished_at_ms,
                    "started_at_ms": candidate_started_at_ms,
                    "finished_at_ms": candidate_finished_at_ms,
                    "recovered_from_reason": attempt.reason,
                    **(metrics_extra or {}),
                },
            )
            if candidate_attempt.success or candidate_attempt.reason != attempt.reason:
                attempt = candidate_attempt

        # 계측 누적 (append-only)
        metrics_path = os.path.abspath(
            os.environ.get("PDF_ATTEMPTS_JSONL", os.path.join("outputs", "download_attempts.jsonl"))
        )
        finished_at_ms = int(time.time() * 1000)
        started_at_ms = max(0, finished_at_ms - int(attempt.elapsed_ms or 0))
        append_metrics_jsonl(
            metrics_path,
            attempt,
            extra={
                "timestamp_ms": finished_at_ms,
                "started_at_ms": started_at_ms,
                "finished_at_ms": finished_at_ms,
                **(metrics_extra or {}),
            },
        )

        if logger:
            if attempt.success:
                logger.info(
                    f"        [CFFI] 다운로드 성공! "
                    f"(status={attempt.status_code}, elapsed={attempt.elapsed_ms}ms, mode={pipeline_mode})"
                )
            else:
                logger.warning(
                    f"        [CFFI] 실패 reason={attempt.reason}, "
                    f"status={attempt.status_code}, elapsed={attempt.elapsed_ms}ms, mode={pipeline_mode}"
                )

        if return_detail:
            evidence = [
                f"status_code={attempt.status_code}",
                f"content_type={attempt.content_type}",
                f"content_disposition={attempt.content_disposition}",
                f"content_length={attempt.content_length}",
                f"final_url={attempt.final_url}",
                f"redirect_chain={' -> '.join(attempt.redirect_chain)}",
                f"first_bytes={attempt.first_bytes}",
                f"elapsed_ms={attempt.elapsed_ms}",
                f"strategy={attempt.strategy}",
                f"phase={attempt.phase}",
            ]
            # 429 Retry-After 보강
            if attempt.reason == REASON_FAIL_HTTP_STATUS and attempt.status_code == 429:
                retry_after = attempt.evidence.get("retry_after") if isinstance(attempt.evidence, dict) else None
                if retry_after:
                    evidence.append(f"retry_after={retry_after}")

            return {
                "ok": attempt.success,
                "reason": attempt.reason if not attempt.success else "SUCCESS",
                "evidence": evidence,
                "http_status": attempt.status_code,
            }

        return attempt.success

    except Exception as e:
        if logger:
            logger.warning(f"        [CFFI] 에러: {e}")
        if return_detail:
            reason = REASON_FAIL_TIMEOUT_NETWORK
            if "redirect" in str(e).lower():
                reason = REASON_FAIL_REDIRECT_LOOP
            return {"ok": False, "reason": reason, "evidence": [str(e)], "http_status": None}
        return False

# =======================================================
# DrissionPage 크롤러
# =======================================================
def download_with_drission(
    doi_url,
    save_dir,
    filename,
    chrome_path,
    max_attempts=2,
    logger=None,
    mode="first",
    return_detail=False,
    hard_timeout_s=None,
    artifact_root=None,
):
    # 폴더 생성
    os.makedirs(save_dir, exist_ok=True)
    full_save_path = os.path.join(save_dir, filename)
    artifact_root = artifact_root or save_dir
    os.makedirs(artifact_root, exist_ok=True)
    browser_tmp_root = os.path.join(artifact_root, ".browser_tmp")
    browser_tmp_dir = os.path.join(browser_tmp_root, os.path.splitext(filename)[0])
    tmp_save_path = os.path.join(browser_tmp_dir, filename)
    
    # 기존 파일 정리
    if os.path.exists(full_save_path):
        try: os.remove(full_save_path)
        except: pass
    try:
        if os.path.exists(browser_tmp_dir):
            shutil.rmtree(browser_tmp_dir)
        os.makedirs(browser_tmp_dir, exist_ok=True)
    except Exception:
        pass

    doi_norm_preview = _doi_from_doi_url(doi_url)
    is_elsevier_preview = doi_norm_preview.startswith("10.1016")
    resolved_browser = resolve_browser_executable(chrome_path, logger=logger)
    session_plan = build_download_browser_session_plan(
        doi_url,
        runtime_profile_root=os.getenv("PDF_BROWSER_RUNTIME_PROFILE_ROOT", "").strip(),
        worker_label=f"pid_{os.getpid()}",
        logger=logger,
    )
    elsevier_session_source = str(session_plan.get("session_source") or "")
    elsevier_low_trust_session = _is_low_trust_elsevier_session_source(elsevier_session_source)
    if not resolved_browser:
        if return_detail:
            return {
                "ok": False,
                "reason": "FAIL_NETWORK",
                "evidence": ["browser_executable_not_found"],
                "stage": "drission-init",
                "domain": _extract_domain(doi_url),
                "http_status": None,
                "browser_session_mode": str(session_plan.get("session_mode") or ""),
                "browser_session_source": str(session_plan.get("session_source") or ""),
                "browser_profile_name": str(session_plan.get("profile_name") or ""),
                "browser_user_data_dir": str(session_plan.get("user_data_dir") or ""),
            }
        return False

    # --- 옵션 설정 ---
    co = ChromiumOptions()
    co.set_browser_path(resolved_browser)
    _apply_browser_session_plan(co, session_plan, logger=logger)
    co.auto_port()
    _apply_best_browser_profile(co)
    if is_elsevier_preview:
        try:
            co.set_load_mode("normal")
            if logger:
                logger.info("     [Drission] Elsevier는 normal load mode 사용")
                if elsevier_low_trust_session:
                    logger.info(
                        "     [Drission] Elsevier low-trust session 감지 -> strict hydration 사용 "
                        f"(source={elsevier_session_source or 'unknown'})"
                    )
        except Exception:
            pass
    
    # 다운로드 설정
    co.set_pref('download.default_directory', browser_tmp_dir) # 다운로드 경로 지정(doi 단위 임시 디렉터리)
    co.set_pref('download.prompt_for_download', False)  # 저장 여부 묻지 않기
    co.set_pref('plugins.always_open_pdf_externally', not is_elsevier_preview) # Elsevier는 viewer-first 경로 유지
    co.set_pref('profile.default_content_settings.popups', 0) # 팝업 차단 해제

    page = None
    for init_attempt in range(3): # 최대 3번 브라우저 실행 시도
        try:
            page = ChromiumPage(co)
            break # 성공하면 루프 탈출
        except Exception as e:
            if logger: logger.warning(f"     [Drission] 브라우저 실행 실패({init_attempt+1}/3): {e} -> 재시도 중...")
            time.sleep(2) # 2초 대기 후 재시도
            
    if page is None:
        if logger: logger.error(f"     [Drission] 브라우저 초기화 최종 실패. 이 논문은 스킵합니다.")
        if return_detail:
            return {
                "ok": False,
                "reason": "FAIL_NETWORK",
                "evidence": ["browser_init_failed"],
                "stage": "drission-init",
                "domain": _extract_domain(doi_url),
                "http_status": None,
                "browser_session_mode": str(session_plan.get("session_mode") or ""),
                "browser_session_source": str(session_plan.get("session_source") or ""),
                "browser_profile_name": str(session_plan.get("profile_name") or ""),
                "browser_user_data_dir": str(session_plan.get("user_data_dir") or ""),
            }
        return False

    _maybe_bootstrap_low_trust_publisher_session(
        page,
        doi_norm_preview,
        session_plan=session_plan,
        logger=logger,
        timeout_s=10 if mode == "deep" else 8,
    )
    preferred_entry_url = _resolve_preferred_browser_entry_url(
        doi_url,
        doi_norm=doi_norm_preview,
        session_source=elsevier_session_source,
        logger=logger,
    ) or doi_url
    
    if mode == "first":
        # 요청 반영: first pass는 항상 1회만 시도한다.
        max_attempts = 1
    per_attempt_timeout = 24 if mode == "deep" else (20 if is_elsevier_preview else 12)
    per_attempt_sleep = 2 if mode == "deep" else 0
    landing_attempted = False
    landing_success = False
    landing_state = "not_attempted"
    landing_url = ""
    landing_title = ""

    def _set_landing_state(state: str, success: bool, page_obj=None):
        nonlocal landing_attempted, landing_success, landing_state, landing_url, landing_title
        landing_attempted = True
        landing_success = bool(success)
        landing_state = state
        target_page = page_obj or page
        if target_page is not None:
            try:
                landing_url = target_page.url or landing_url
            except Exception:
                pass
            try:
                landing_title = target_page.title or landing_title
            except Exception:
                pass

    def _detail(ok, reason, evidence=None, stage="drission", http_status=None):
        payload = {
            "ok": ok,
            "reason": reason,
            "evidence": evidence or [],
            "stage": stage,
            "domain": _extract_domain(doi_url),
            "http_status": http_status,
            "landing_attempted": landing_attempted,
            "landing_success": landing_success,
            "landing_state": landing_state,
            "landing_url": landing_url,
            "landing_title": landing_title,
            "browser_session_mode": str(session_plan.get("session_mode") or ""),
            "browser_session_source": str(session_plan.get("session_source") or ""),
            "browser_profile_name": str(session_plan.get("profile_name") or ""),
            "browser_user_data_dir": str(session_plan.get("user_data_dir") or ""),
        }
        return payload if return_detail else ok

    def _ret(ok, reason, evidence=None, stage="drission", http_status=None):
        if not ok and page:
            try:
                    _safe_screenshot(
                        page,
                        os.path.join(artifact_root, "logs", "screenshots"),
                        f"final_fail_capture_{filename}.png",
                        logger,
                    )
            except Exception:
                pass
        payload = _detail(ok, reason, evidence=evidence, stage=stage, http_status=http_status)
        try:
            if os.path.exists(browser_tmp_dir):
                shutil.rmtree(browser_tmp_dir)
            if os.path.isdir(browser_tmp_root) and not os.listdir(browser_tmp_root):
                os.rmdir(browser_tmp_root)
        except Exception:
            pass
        _close_page_safely(page, logger, session_plan=session_plan)
        _cleanup_browser_session_plan(session_plan, logger=logger)
        return payload
    
    for attempt in range(1, max_attempts + 1):
        try:
            def _over_budget() -> bool:
                return False

            if page is None:
                for init_try in range(3):
                    try:
                        page = ChromiumPage(co)
                        break
                    except Exception as e:
                        time.sleep(2)
                
                if page is None:
                    if logger: logger.error(f"     [Drission] 브라우저 생성 실패 (재시도 {attempt}). 다음 시도로 넘어갑니다.")
                    continue
            
            
            current_entry_url = preferred_entry_url or doi_url
            logger.info(f"     [Drission] 접속 시도 ({attempt}/{max_attempts}): {current_entry_url}")
            
            # 페이지 접속
            landing_initial_files = _get_current_files(browser_tmp_dir)
            page.get(current_entry_url, retry=0, interval=0.5, timeout=min(per_attempt_timeout, MAX_ACTION_WAIT_S))
            _dismiss_cookie_or_consent_banner(page, logger=logger)
            current_domain = _extract_domain(page.url)
            referer_url = page.url
            page_title = page.title or ""
            page_html = page.html or ""
            if "spiedigitallibrary.org" in current_domain:
                _wait_for_spie_article_ready(page, logger=logger, timeout_s=10 if mode == "deep" else 7)
                current_domain = _extract_domain(page.url)
                referer_url = page.url
                page_title = page.title or ""
                page_html = page.html or ""
            if _capture_direct_downloaded_pdf(
                download_dir=browser_tmp_dir,
                initial_files=landing_initial_files,
                tmp_target_path=tmp_save_path,
                final_target_path=full_save_path,
                logger=logger,
                timeout_s=0,
                context="doi-direct-download-immediate",
            ):
                _set_landing_state("direct_pdf_handoff", True)
                return _ret(True, "SUCCESS", stage="doi-direct-download")
            unexpected_landing = (
                (not current_domain)
                or ("google." in current_domain)
                or page.url.startswith("chrome://")
                or page.url.startswith("about:blank")
            )
            if unexpected_landing:
                direct_wait_s = 10 if mode == "deep" else 6
                if _capture_direct_downloaded_pdf(
                    download_dir=browser_tmp_dir,
                    initial_files=landing_initial_files,
                    tmp_target_path=tmp_save_path,
                    final_target_path=full_save_path,
                    logger=logger,
                    timeout_s=direct_wait_s,
                    context="doi-direct-download",
                ):
                    _set_landing_state("direct_pdf_handoff", True)
                    return _ret(True, "SUCCESS", stage="doi-direct-download")
                logger.info(f"        [Drission] 예상외 랜딩({page.url}) 감지 -> DOI 재요청 1회")
                try:
                    retry_initial_files = _get_current_files(browser_tmp_dir)
                    page.get(current_entry_url, retry=0, interval=0.5, timeout=min(per_attempt_timeout, MAX_ACTION_WAIT_S))
                    _dismiss_cookie_or_consent_banner(page, logger=logger)
                    current_domain = _extract_domain(page.url)
                    referer_url = page.url
                    page_title = page.title or ""
                    page_html = page.html or ""
                    if "spiedigitallibrary.org" in current_domain:
                        _wait_for_spie_article_ready(page, logger=logger, timeout_s=10 if mode == "deep" else 7)
                        current_domain = _extract_domain(page.url)
                        referer_url = page.url
                        page_title = page.title or ""
                        page_html = page.html or ""
                    if _capture_direct_downloaded_pdf(
                        download_dir=browser_tmp_dir,
                        initial_files=retry_initial_files,
                        tmp_target_path=tmp_save_path,
                        final_target_path=full_save_path,
                        logger=logger,
                        timeout_s=0,
                        context="doi-direct-download-retry-immediate",
                    ):
                        _set_landing_state("direct_pdf_handoff", True)
                        return _ret(True, "SUCCESS", stage="doi-direct-download")
                    retry_unexpected = (
                        (not current_domain)
                        or ("google." in current_domain)
                        or page.url.startswith("chrome://")
                        or page.url.startswith("about:blank")
                    )
                    if retry_unexpected and _capture_direct_downloaded_pdf(
                        download_dir=browser_tmp_dir,
                        initial_files=retry_initial_files,
                        tmp_target_path=tmp_save_path,
                        final_target_path=full_save_path,
                        logger=logger,
                        timeout_s=direct_wait_s,
                        context="doi-direct-download-retry",
                    ):
                        _set_landing_state("direct_pdf_handoff", True)
                        return _ret(True, "SUCCESS", stage="doi-direct-download")
                except Exception:
                    pass
            doi_norm = _doi_from_doi_url(doi_url)
            is_elsevier_doi = doi_norm.startswith("10.1016")

            # Elsevier 일부 DOI는 doi.org 랜딩 직후 빈 화면으로 남는 케이스가 있어
            # 응답 html/redirect 정보를 이용해 scienceDirect article URL로 보정한다.
            if is_elsevier_doi and "sciencedirect.com" not in current_domain:
                # 요청 반영: 진입 후 자동 이동 대기는 1회(3초)만 수행한다.
                if current_domain.endswith("doi.org") or ("linkinghub.elsevier.com" in current_domain):
                    if logger:
                        logger.info("        [Elsevier] 자동 이동 대기 3초(1회)")
                    time.sleep(3.0)
                    _dismiss_cookie_or_consent_banner(page, logger=logger)
                    current_domain = _extract_domain(page.url)
                    referer_url = page.url
                    page_title = page.title or ""
                    page_html = page.html or ""
                    if logger:
                        logger.info(f"        [Elsevier] 대기 후 URL: {page.url}")
            if "sciencedirect.com" in current_domain and (not _is_elsevier_retrieve_url(page.url)):
                if logger:
                    logger.info("        [Elsevier] 자동 이동 감지 -> 복구 단계 스킵")

                if logger:
                    logger.info(
                        f"        [Elsevier] landing domain={current_domain or 'N/A'}, "
                        f"url={page.url}, title_len={len(page_title.strip())}, html_len={len(page_html)}"
                    )
                need_recover = current_domain.endswith("doi.org") or _looks_like_empty_rendered_page(page_title, page_html)
                if "linkinghub.elsevier.com" in current_domain:
                    # retrieve에서는 강제 article URL 복구 대신 DOI 클릭을 우선한다.
                    need_recover = True
                    if _is_elsevier_retrieve_url(page.url):
                        clicked = _click_elsevier_doi_link_in_retrieve(page, doi_norm, logger=logger)
                        if clicked:
                            try:
                                time.sleep(0.8)
                                page = _adopt_latest_tab(page, logger=logger)
                                _dismiss_cookie_or_consent_banner(page, logger=logger)
                                current_domain = _extract_domain(page.url)
                                referer_url = page.url
                                page_title = page.title or ""
                                page_html = page.html or ""
                                if logger:
                                    logger.info(f"        [Elsevier] retrieve→DOI 클릭 이동: {page.url}")
                                if ("sciencedirect.com" in current_domain) and (not _is_elsevier_retrieve_url(page.url)):
                                    need_recover = False
                            except Exception as e:
                                _raise_if_browser_disconnect(e, logger=logger, context="elsevier-retrieve-doi-click")
                                if logger:
                                    logger.info(f"        [Elsevier] retrieve→DOI 클릭 후 전환 실패: {e}")
                        if need_recover:
                            handoff_url = _extract_elsevier_retrieve_handoff_url(page.url, page_html)
                            if handoff_url:
                                try:
                                    if logger:
                                        logger.info(f"        [Elsevier] retrieve handoff 이동: {handoff_url}")
                                    page.get(
                                        handoff_url,
                                        retry=0,
                                        interval=0.5,
                                        timeout=min(per_attempt_timeout, MAX_ACTION_WAIT_S),
                                    )
                                    _dismiss_cookie_or_consent_banner(page, logger=logger)
                                    current_domain = _extract_domain(page.url)
                                    referer_url = page.url
                                    page_title = page.title or ""
                                    page_html = page.html or ""
                                    if logger:
                                        logger.info(f"        [Elsevier] handoff 후 URL: {page.url}")
                                    _wait_for_elsevier_article_ready(page, doi_norm, logger=logger, timeout_s=8)
                                    if ("sciencedirect.com" in current_domain) and (not _is_elsevier_retrieve_url(page.url)):
                                        need_recover = False
                                except Exception as e:
                                    _raise_if_browser_disconnect(e, logger=logger, context="elsevier-retrieve-handoff")
                                    if logger:
                                        logger.info(f"        [Elsevier] retrieve handoff 이동 실패: {e}")
                if need_recover:
                    # 핵심 수정: retrieve에 머문 상태에서는 강제 page.get 복구를 하지 않는다.
                    if "linkinghub.elsevier.com" in current_domain and _is_elsevier_retrieve_url(page.url):
                        if logger:
                            logger.warning("        [Elsevier] retrieve 고착: DOI/handoff 전환 실패로 종료")
                        _set_landing_state("interstitial_or_retrieve", False)
                        return _ret(False, "FAIL_BLOCK", ["elsevier_retrieve_stuck_no_handoff"], stage="landing")

                    recovered_article_url = _extract_sciencedirect_article_url_from_html(page_html)
                    if (not recovered_article_url) and ("linkinghub.elsevier.com" in current_domain):
                        pii = _extract_sciencedirect_pii_from_url(page.url)
                        if pii:
                            recovered_article_url = f"https://www.sciencedirect.com/science/article/pii/{pii}"
                            if logger:
                                logger.info(f"        [Elsevier] linkinghub PII 전환: {pii}")
                    if not recovered_article_url:
                        recovered_article_url = _resolve_doi_redirect_target(doi_url, logger=logger)
                    if recovered_article_url and recovered_article_url != (page.url or ""):
                        try:
                            logger.info(f"        [Elsevier] article URL 복구 이동: {recovered_article_url}")
                            page.get(
                                recovered_article_url,
                                retry=0,
                                interval=0.5,
                                timeout=min(per_attempt_timeout, MAX_ACTION_WAIT_S),
                            )
                            _dismiss_cookie_or_consent_banner(page, logger=logger)
                            current_domain = _extract_domain(page.url)
                            referer_url = page.url
                            page_title = page.title or ""
                            page_html = page.html or ""
                        except Exception as e:
                            _raise_if_browser_disconnect(e, logger=logger, context="elsevier-article-recover")
                            logger.info(f"        [Elsevier] article URL 복구 이동 실패(계속 진행): {e}")

            if (not current_domain) or ("google." in current_domain) or page.url.startswith("chrome://") or page.url.startswith("about:blank"):
                if _capture_direct_downloaded_pdf(
                    download_dir=browser_tmp_dir,
                    initial_files=set(),
                    tmp_target_path=tmp_save_path,
                    final_target_path=full_save_path,
                    logger=logger,
                    timeout_s=3,
                    context="doi-direct-download-final-check",
                ):
                    _set_landing_state("direct_pdf_handoff", True)
                    return _ret(True, "SUCCESS", stage="doi-direct-download")
                _set_landing_state("blank_or_incomplete", False)
                return _ret(False, "FAIL_NETWORK", [f"unexpected_landing_page={page.url}"], stage="landing")

            if current_domain.endswith("doi.org"):
                resolved_target = _resolve_doi_redirect_target(doi_url, logger=logger)
                if resolved_target and resolved_target != (page.url or ""):
                    try:
                        if logger:
                            logger.info(f"        [DOI Resolve] doi landing -> 최종 URL 복구 이동: {resolved_target}")
                        page.get(
                            resolved_target,
                            retry=0,
                            interval=0.5,
                            timeout=min(per_attempt_timeout, MAX_ACTION_WAIT_S),
                        )
                        _dismiss_cookie_or_consent_banner(page, logger=logger)
                        if "spiedigitallibrary.org" in _extract_domain(page.url):
                            _wait_for_spie_article_ready(page, logger=logger, timeout_s=10 if mode == "deep" else 7)
                        current_domain = _extract_domain(page.url)
                        referer_url = page.url
                        page_title = page.title or ""
                        page_html = page.html or ""
                    except Exception as e:
                        _raise_if_browser_disconnect(e, logger=logger, context="doi-unresolved-recover")
                        if logger:
                            logger.info(f"        [DOI Resolve] doi landing recover 실패(계속): {e}")
                title_blob = str(page_title or "").strip().lower()
                article_like = _has_article_signal(title=page_title, html=page_html)
                pdf_action_like = _has_pdf_action_signal(title=page_title, html=page_html)
                if current_domain.endswith("doi.org") and (title_blob in {"new tab", "새 탭", "google"} or (not article_like and not pdf_action_like)):
                    _set_landing_state("blank_or_incomplete", False)
                    return _ret(
                        False,
                        "FAIL_NETWORK",
                        [f"unresolved_doi_landing={page.url}", f"title={page_title[:120]}"],
                        stage="landing",
                    )

            if is_elsevier_doi and ("sciencedirect.com" in current_domain):
                _recover_elsevier_interstitial_article(
                    page,
                    target_pii=_extract_elsevier_target_pii(page),
                    article_referer=str(getattr(page, "url", "") or ""),
                    logger=logger,
                )
                current_domain = _extract_domain(page.url)
                page_title = page.title or page_title
                page_html = page.html or page_html

            issue, evidence = detect_access_issue(
                title=page_title,
                html=page_html,
                url=page.url or "",
                domain=current_domain,
            )
            if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK") and current_domain and (not current_domain.endswith("doi.org")):
                page, recovery_snapshot, recovered = _attempt_publisher_landing_challenge_recovery(
                    page,
                    doi_url=doi_url,
                    doi_norm=doi_norm,
                    logger=logger,
                    timeout_s=14 if is_elsevier_doi else 10,
                    session_source=elsevier_session_source,
                )
                if recovered:
                    current_domain = _extract_domain(page.url)
                    referer_url = page.url
                    page_title = page.title or page_title
                    page_html = page.html or page_html
                    issue, evidence = detect_access_issue(
                        title=page_title,
                        html=page_html,
                        url=page.url or "",
                        domain=current_domain,
                    )
                else:
                    page_title = str(recovery_snapshot.get("title") or page_title)
                    page_html = str(recovery_snapshot.get("html") or page_html)
                    current_domain = str(recovery_snapshot.get("domain") or current_domain)
                    issue = str(recovery_snapshot.get("issue") or issue or "")
                    evidence = list(recovery_snapshot.get("evidence") or evidence or [])
            # 요청사항 반영:
            # hard-fail 판단은 landing 단계에서만 수행하고, 이후 단계는 다운로드 시도까지 진행한다.
            if issue == "FAIL_DOI_NOT_FOUND":
                if logger:
                    logger.warning(f"        landing 단계 DOI 미등록 감지로 중단: {evidence}")
                _set_landing_state("doi_not_found", False)
                return _ret(False, issue, evidence, stage="landing")
            if issue == "FAIL_ACCESS_RIGHTS":
                if logger:
                    logger.warning(f"        landing 단계 접근권한 필요 감지로 중단: {evidence}")
                _set_landing_state("access_rights_block", False)
                return _ret(False, issue, evidence, stage="landing")
            if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                if not _abort_on_landing_block():
                    if logger:
                        logger.info(f"        landing 차단/캡차 감지 but parser override로 계속 진행: {evidence}")
                elif current_domain.endswith("doi.org"):
                    if logger:
                        logger.info(f"        [Drission] landing issue on doi.org, publisher 이동 시도 계속: {evidence}")
                elif _should_soft_continue_issue(issue, evidence, page_title, page_html, current_domain):
                    if logger:
                        logger.info(f"        [Drission] soft-continue (landing {issue}): {evidence}")
                else:
                    if logger:
                        logger.warning(f"        landing 단계 차단/캡차 감지로 중단: {evidence}")
                    _set_landing_state("challenge_or_block", False)
                    return _ret(False, issue, evidence, stage="landing")
            _set_landing_state("success_landing", True)

            high_friction = _is_high_friction_domain(current_domain)
            is_sciencedirect = "sciencedirect.com" in current_domain
            is_elsevier_landing = is_sciencedirect or ("linkinghub.elsevier.com" in current_domain)
            is_acs = "acs.org" in current_domain
            is_rsc = "rsc.org" in current_domain

            if is_elsevier_landing:
                logger.info(f"        [Elsevier] 2단계 클릭 다운로드 우선 시도: {doi_norm}")
                elsevier_click_ok, page, elsevier_click_detail = _attempt_elsevier_two_step_click_download(
                    page=page,
                    doi=doi_norm,
                    tmp_dir=browser_tmp_dir,
                    tmp_path=tmp_save_path,
                    logger=logger,
                    allow_doi_reentry=True,
                    session_source=elsevier_session_source,
                    low_trust_session=elsevier_low_trust_session,
                    return_page=True,
                )
                current_domain = _extract_domain(page.url)
                referer_url = page.url
                page_title = page.title or ""
                page_html = page.html or ""
                if elsevier_click_ok:
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="elsevier-two-step-click")
                elsevier_issue = str((elsevier_click_detail or {}).get("issue") or "")
                elsevier_evidence = list((elsevier_click_detail or {}).get("evidence") or [])
                elsevier_stage = str((elsevier_click_detail or {}).get("stage") or "")
                if elsevier_stage == "preclick-hydration" and elsevier_issue:
                    return _ret(False, elsevier_issue, elsevier_evidence, stage="elsevier-hydration")
                logger.info("        [Elsevier] 클릭 플로우 실패, 기존 다운로드 경로로 계속")

            # --- PDF 링크 탐색 ---
            pdf_url = None
            pdf_btn = None
            if is_rsc:
                pdf_url = _wait_for_rsc_article_pdf_ready(page, logger=logger, timeout_s=12 if mode == "deep" else 8)
                pdf_btn = _select_best_clickable_pdf_element(
                    page,
                    [
                        "//a[contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'/content/articlepdf/')]",
                        "//a[contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article')]",
                        "//button[contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article')]",
                        "//a[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article')]",
                        "//button[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article')]",
                        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article')]",
                        "//button[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download this article')]",
                        "//a[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'article pdf')]",
                    ],
                    logger=logger,
                    ban_tokens=[
                        "supporting information",
                        "supporting info",
                        "supplementary information",
                        "supplementary files",
                        "electronic supplementary information",
                        "suppdata",
                    ],
                )
            else:
                pdf_btn = _ele_quick(page, 'text:Download PDF', timeout=0.5) or \
                          _ele_quick(page, 'text:View PDF', timeout=0.5) or \
                          _ele_quick(page, 'css:#viewpdf', timeout=0.5) or \
                          _ele_quick(page, 'css:[id*="viewpdf"]', timeout=0.5) or \
                          _ele_quick(page, 'css:[aria-label*="View PDF"]', timeout=0.5) or \
                          _ele_quick(page, 'text:PDF', timeout=0.5) or \
                          _ele_quick(page, 'tag:a@@title:PDF', timeout=0.5) or \
                          _ele_quick(page, 'css:a[href*=".pdf"]', timeout=0.5)
            
            # 1. Meta 태그
            if not pdf_url:
                meta = _ele_quick(page, 'xpath://meta[@name="citation_pdf_url"]', timeout=0.5)
                if meta:
                    meta_content = meta.attr('content')
                    if not _is_supporting_info_blob(meta_content):
                        pdf_url = meta_content
            
            # 2. 버튼/링크 패턴 매칭
            if not pdf_url:
                if pdf_btn:
                    btn_href = pdf_btn.attr('href')
                    if _looks_like_pdf_link(btn_href):
                        pdf_url = btn_href
                    elif btn_href and logger:
                        logger.info(f"        [LinkFilter] PDF 링크 후보 제외(weak): {btn_href}")
            # 3. analyze_html
                if not pdf_url:
                    pdf_url = _analyze_html_structure_drission(page, logger)
                if pdf_url and "stamp.jsp" in pdf_url:
                    logger.info("        [IEEE] Stamp 링크 감지 -> 실제 PDF 주소 추출 시도")
                    
                    # 1. 해당 뷰어 페이지(stamp.jsp)로 이동
                    page.get(pdf_url, retry=0, interval=0.5, timeout=8 if mode == "deep" else 5)
                    time.sleep(0.6) # 로딩 대기
                    
                    # 2.   _analyze_html_structure_drission 재호출
                    real_url = _analyze_html_structure_drission(page, logger)
                    
                    if real_url and "stamp.jsp" not in real_url:
                        pdf_url = real_url
                        logger.info(f"        [IEEE] Real URL 교체 완료: {pdf_url}")
                    else:
                        recovered_ieee_pdf = _recover_ieee_stamp_pdf_url(real_url, pdf_url, page.url, page.html)
                        if recovered_ieee_pdf:
                            pdf_url = recovered_ieee_pdf
                            logger.info(f"        [IEEE] stampPDF 경로 강제 복구: {pdf_url}")
                        else:
                            logger.warning("        [IEEE] Real URL 추출 실패 (기본 링크 사용)")

            page_title = page.title or ""
            page_html = page.html or ""
            issue, evidence = detect_access_issue(
                title=page_title,
                html=page_html,
                url=page.url or "",
                domain=current_domain,
            )
            if issue == "FAIL_DOI_NOT_FOUND":
                return _ret(False, issue, evidence, stage="pdf-discovery")
            if issue == "FAIL_ACCESS_RIGHTS":
                return _ret(False, issue, evidence, stage="pdf-discovery")
            if issue in ("FAIL_CAPTCHA", "FAIL_BLOCK") and logger:
                logger.info(f"        [Drission] pdf-discovery issue 관측(계속 진행): {issue}, {evidence}")

            # 4. Iframe
            if not pdf_url:
                iframe = _ele_quick(page, 'tag:iframe@@src:.pdf', timeout=0.5)
                if iframe: pdf_url = iframe.attr('src')
            if (not pdf_url) and is_sciencedirect:
                target_pii_now = _extract_elsevier_target_pii(page)
                pdf_url = _extract_sciencedirect_pdfft_url_from_html(page_html, target_pii=target_pii_now)
                if pdf_url and logger:
                    logger.info(f"        [Elsevier] html 메타 기반 pdfft URL 복구: {pdf_url}")
            if (not pdf_url) and doi_norm.startswith("10.4150/"):
                powdermat_target = _resolve_powdermat_pdf_target(
                    doi=doi_norm,
                    current_url=page.url or "",
                    current_html=page_html,
                )
                if powdermat_target:
                    pdf_url = powdermat_target.get("pdf_url") or pdf_url
                    powdermat_article_url = str(powdermat_target.get("article_url") or "").strip()
                    if powdermat_article_url:
                        referer_url = powdermat_article_url
                        current_domain = _extract_domain(powdermat_article_url) or current_domain
                    if pdf_url and logger:
                        logger.info(f"        [Powdermat] DOI 기반 direct PDF URL 복구: {pdf_url}")
            if (not pdf_url) and doi_norm.startswith("10.31613/"):
                ceramist_target = _resolve_ceramist_pdf_target(
                    doi=doi_norm,
                    current_url=page.url or "",
                    current_html=page_html,
                )
                if ceramist_target:
                    pdf_url = ceramist_target.get("pdf_url") or pdf_url
                    ceramist_article_url = str(ceramist_target.get("article_url") or "").strip()
                    if ceramist_article_url:
                        referer_url = ceramist_article_url
                        current_domain = _extract_domain(ceramist_article_url) or current_domain
                    if pdf_url and logger:
                        logger.info(f"        [Ceramist] DOI 기반 direct PDF URL 복구: {pdf_url}")
            if (not pdf_url) and doi_norm.startswith("10.3365/"):
                kjmm_target = _resolve_kjmm_pdf_target(
                    doi=doi_norm,
                    current_url=page.url or "",
                    current_html=page_html,
                    logger=logger,
                )
                kjmm_issue = str(kjmm_target.get("access_issue") or "").strip()
                if kjmm_issue:
                    return _ret(False, kjmm_issue, ["kjmm_pdf_access_issue"], stage="pdf-discovery")
                if kjmm_target:
                    pdf_url = kjmm_target.get("pdf_url") or pdf_url
                    kjmm_article_url = str(kjmm_target.get("article_url") or "").strip()
                    if kjmm_article_url:
                        referer_url = kjmm_article_url
                        current_domain = _extract_domain(kjmm_article_url) or current_domain
                    if pdf_url and logger:
                        logger.info(f"        [KJMM] DOI 기반 PDF URL 복구: {pdf_url}")
            if (not pdf_url) or ("/bitstreams/" in str(pdf_url or "").lower() and "/download" in str(pdf_url or "").lower()):
                dspace_target = _resolve_dspace_pdf_target(
                    current_url=page.url or "",
                    current_html=page_html,
                    logger=logger,
                )
                if dspace_target:
                    pdf_url = dspace_target.get("pdf_url") or pdf_url
                    dspace_article_url = str(dspace_target.get("article_url") or "").strip()
                    if dspace_article_url:
                        referer_url = dspace_article_url
                        current_domain = _extract_domain(dspace_article_url) or current_domain
                    if pdf_url and logger:
                        logger.info(f"        [DSpace] article bitstream PDF URL 복구: {pdf_url}")

            ieee_fastpath_url_ready = False
            pdf_url_domain = _extract_domain(pdf_url) if pdf_url else ""
            if pdf_url and (
                "ieeexplore.ieee.org" in str(current_domain or "").lower()
                or "ieeexplore.ieee.org" in pdf_url_domain
            ):
                pdf_low = str(pdf_url or "").lower()
                ieee_fastpath_url_ready = any(
                    token in pdf_low
                    for token in (
                        "stamppdf/getpdf.jsp",
                        "/ielx",
                        ".pdf",
                        "arnumber=",
                    )
                )
                if ieee_fastpath_url_ready and logger:
                    logger.info("        [IEEE] real PDF URL 확보 -> 버튼 클릭 우선 시도 생략")
            
            # 고차단 도메인은 실제 사용자 행동과 유사하게 버튼 클릭 다운로드를 우선 시도
            if high_friction and pdf_btn and (not is_sciencedirect) and (not ieee_fastpath_url_ready):
                if is_rsc and pdf_url and _is_rsc_article_pdf_url(pdf_url):
                    logger.info("        [RSC] articlepdf URL 확보 -> generic 버튼 클릭 우선 시도 생략")
                else:
                    btn_wait_s = 18 if mode == "deep" else (6 if is_sciencedirect else 12)
                    logger.info(f"        [Drission] 고차단 도메인({current_domain}) 버튼 클릭 다운로드 우선 시도")
                    click_ok, click_page = _try_click_pdf_button_download(
                        page=page,
                        pdf_btn=pdf_btn,
                        save_dir=browser_tmp_dir,
                        full_save_path=tmp_save_path,
                        logger=logger,
                        wait_timeout_s=btn_wait_s,
                        return_page=True,
                    )
                    if click_page is not None:
                        page = click_page
                        current_domain = _extract_domain(getattr(page, "url", "") or "") or current_domain
                    if click_ok:
                        if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                            return _ret(True, "SUCCESS", stage="button-click-download")

            # 일반/비지원 도메인도 js 기반 버튼 케이스가 있어 1회 클릭 시도
            if (not high_friction) and pdf_btn and (not is_sciencedirect):
                logger.info(f"        [Drission] 일반 도메인({current_domain}) 버튼 클릭 다운로드 1회 시도")
                click_ok, click_page = _try_click_pdf_button_download(
                    page=page,
                    pdf_btn=pdf_btn,
                    save_dir=browser_tmp_dir,
                    full_save_path=tmp_save_path,
                    logger=logger,
                    wait_timeout_s=8 if mode == "first" else 16,
                    return_page=True,
                )
                if click_page is not None:
                    page = click_page
                    current_domain = _extract_domain(getattr(page, "url", "") or "") or current_domain
                if click_ok:
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="button-click-download")

            # --- 다운로드 실행 ---
            if pdf_url:
                # 상대 경로를 절대 경로로 변환
                if not pdf_url.startswith('http'):
                    pdf_url = urljoin(page.url, pdf_url)
                if _is_supporting_info_blob(pdf_url):
                    if logger:
                        logger.warning(f"        [LinkFilter] supporting-information PDF 후보 폐기: {pdf_url}")
                    pdf_url = None
                if is_sciencedirect:
                    target_pii_now = _extract_elsevier_target_pii(page)
                    found_pii = _extract_sciencedirect_pii_from_text(pdf_url)
                    if target_pii_now and found_pii and (found_pii != target_pii_now):
                        if logger:
                            logger.warning(
                                f"        [Elsevier] pdfft 후보 PII 불일치로 폐기: found={found_pii}, target={target_pii_now}"
                            )
                        pdf_url = None
                if not pdf_url:
                    logger.warning("        [Elsevier] 유효한 PDF 후보를 찾지 못함(타깃 불일치)")
                    continue
                
                logger.info(f"        PDF 링크 발견: {pdf_url}")
                if is_sciencedirect:
                    if os.getenv("PDF_DEBUG_PRE_PDF_SCREENSHOT", "0").strip().lower() in ("1", "true", "yes", "on"):
                        try:
                            _safe_screenshot(
                                page,
                                os.path.join(artifact_root, "logs", "screenshots"),
                                f"pre_pdf_attempt_{filename}.png",
                                logger,
                                full_page=False,
                            )
                        except Exception as e:
                            _raise_if_browser_disconnect(e, logger=logger, context="pre-pdf-screenshot")
                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_download"], stage="drission")

                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="already-downloaded")

                if ieee_fastpath_url_ready:
                    try:
                        cookies_list = page.cookies()
                        current_cookies = {c['name']: c['value'] for c in cookies_list}
                    except Exception:
                        current_cookies = {}
                    logger.info("        [IEEE] fastpath cookie-aware 직접 회수 시도")
                    ieee_cffi_result = download_with_cffi(
                        pdf_url,
                        full_save_path,
                        referer=page.url,
                        cookies=current_cookies,
                        ua=_resolve_best_browser_ua(),
                        logger=logger,
                        return_detail=True,
                        timeout=8,
                    )
                    if ieee_cffi_result.get("ok"):
                        return _ret(True, "SUCCESS", stage="ieee-fastpath-cffi")
                    if logger:
                        logger.info(
                            "        [IEEE] fastpath 직접 회수 실패 "
                            f"(reason={ieee_cffi_result.get('reason') or 'unknown'}) -> 기본 다운로드 경로 계속"
                        )

                # DrissionPage 자체 다운로드 먼저 시도
                logger.info("        1. Drission 자체 다운로드 시도")
                try:
                    # [수정] path=폴더경로, rename=파일명 (확장자 포함 가능)
                    # file_exists='overwrite'로 중복 시 덮어쓰기
                    initial_files = _get_current_files(browser_tmp_dir)
                    clean_name = filename # 파일명 그대로 사용
                    download_result = page.download(
                        pdf_url,
                        goal_path=browser_tmp_dir,
                        rename=clean_name,
                        file_exists='overwrite',
                        show_msg=False,
                    )

                    if _finalize_downloadkit_result(
                        download_result=download_result,
                        tmp_target_path=tmp_save_path,
                        final_target_path=full_save_path,
                        logger=logger,
                        context="drission-downloadkit",
                    ):
                        return _ret(True, "SUCCESS", stage="drission-download")

                    if mode == "deep":
                        download_wait_s = 18
                    else:
                        download_wait_s = 4 if is_sciencedirect else (8 if is_acs else 6)
                    if _capture_direct_downloaded_pdf(
                        download_dir=browser_tmp_dir,
                        initial_files=initial_files,
                        tmp_target_path=tmp_save_path,
                        final_target_path=tmp_save_path,
                        logger=logger,
                        timeout_s=download_wait_s,
                        context="drission-download",
                    ):
                        logger.info(f"        [Drission] 다운로드 성공")
                        if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                            return _ret(True, "SUCCESS", stage="drission-download")
                    if _is_valid_pdf(tmp_save_path):
                        logger.info(f"        [Drission] 다운로드 성공(파일명 정규화 경유)")
                        if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                            return _ret(True, "SUCCESS", stage="drission-download")
                    logger.info("        자체 다운로드 타임아웃")

                except Exception as e:
                    _raise_if_browser_disconnect(e, logger=logger, context="drission-download")
                    logger.warning(f"        자체 다운로드 실패: {e}")
                    pass

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_navigation"], stage="drission")

                if is_sciencedirect and mode == "first" and _looks_like_elsevier_signed_pdf_url(pdf_url):
                    try:
                        nav_result = download_pdf_via_navigation(page, pdf_url, tmp_save_path, logger, timeout_s=8)
                        if nav_result == "__ACCESS_RIGHTS_REQUIRED__":
                            return _ret(False, "FAIL_ACCESS_RIGHTS", ["navigation_access_rights_required"], stage="navigation-download")
                        if nav_result:
                            if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                                return _ret(True, "SUCCESS", stage="navigation-download")
                    except Exception as e:
                        _raise_if_browser_disconnect(e, logger=logger, context="signed-navigation-download")
                        pass

                if not is_sciencedirect:
                    try :
                        nav_timeout = 8 if high_friction else 6
                        nav_result = download_pdf_via_navigation(page, pdf_url, tmp_save_path, logger, timeout_s=nav_timeout)
                        if nav_result == "__ACCESS_RIGHTS_REQUIRED__":
                            return _ret(False, "FAIL_ACCESS_RIGHTS", ["navigation_access_rights_required"], stage="navigation-download")
                        if nav_result:
                            if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                                return _ret(True, "SUCCESS", stage="navigation-download")
                    except Exception as e:
                        _raise_if_browser_disconnect(e, logger=logger, context="navigation-download")
                        pass

                if _is_valid_pdf(tmp_save_path):
                    if _finalize_downloaded_file(tmp_save_path, full_save_path, logger=logger):
                        return _ret(True, "SUCCESS", stage="post-navigation-file-check")
                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="post-navigation-file-check")

                # Elsevier는 navigation/requests/js 연쇄 시도가 대부분 병목으로만 작동하므로 CFFI cookie 시도로 바로 전환.
                if is_sciencedirect and mode == "first":
                    if _over_budget():
                        return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_cffi"], stage="drission")
                    cookies_list = page.cookies()
                    current_cookies = {c['name']: c['value'] for c in cookies_list}
                    cffi_result = download_with_cffi(
                        pdf_url,
                        full_save_path,
                        referer=page.url,
                        cookies=current_cookies,
                        ua=_resolve_best_browser_ua(),
                        logger=logger,
                        return_detail=True,
                        timeout=12,
                    )
                    if cffi_result.get("ok"):
                        return _ret(True, "SUCCESS", stage="cffi-download")
                    return _ret(False, "FAIL_PARSE", ["sciencedirect_pdf_not_downloadable"], stage="drission")

                if mode == "first":
                    return _ret(False, "FAIL_PARSE", ["first_mode_fastpath_exhausted"], stage="drission")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_requests"], stage="drission")

                try :
                    if force_download_with_requests(page, pdf_url, referer_url, full_save_path, logger):
                        return _ret(True, "SUCCESS", stage="requests-download")
                except Exception as e:
                    _raise_if_browser_disconnect(e, logger=logger, context="requests-download")
                    pass

                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="post-requests-file-check")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_js"], stage="drission")

                # ACS/Elsevier는 JS 주입 성공률이 낮고 오탐 HTML이 많아 1차 패스에서는 생략한다.
                if not (is_sciencedirect or is_acs):
                    try :
                        if download_pdf_via_js_injection(page, pdf_url, filename, save_dir, logger):
                            return _ret(True, "SUCCESS", stage="js-download")
                    except Exception as e:
                        _raise_if_browser_disconnect(e, logger=logger, context="js-download")
                        pass

                if _is_valid_pdf(full_save_path):
                    return _ret(True, "SUCCESS", stage="post-js-file-check")

                if _over_budget():
                    return _ret(False, "FAIL_PARSE", ["budget_exceeded_before_cffi"], stage="drission")

                # 고차단 도메인도 마지막에는 쿠키 포함 CFFI를 시도 (직접 링크 접근은 후순위)
                cookies_list = page.cookies()
                current_cookies = {c['name']: c['value'] for c in cookies_list}
                try : 
                    cffi_result = download_with_cffi(
                        pdf_url,
                        full_save_path,
                        referer=page.url,
                        cookies=current_cookies,
                        ua=_resolve_best_browser_ua(),
                        logger=logger,
                        return_detail=True,
                        timeout=120 if mode == "deep" else 60,
                    )
                    if cffi_result.get("ok"):
                        return _ret(True, "SUCCESS", stage="cffi-download")
                    if cffi_result.get("reason") in ("FAIL_CAPTCHA", "FAIL_BLOCK"):
                        return _ret(
                            False,
                            cffi_result.get("reason"),
                            cffi_result.get("evidence", []),
                            stage="cffi-download",
                            http_status=cffi_result.get("http_status"),
                        )
                except Exception as e:
                    _raise_if_browser_disconnect(e, logger=logger, context="cffi-download")
                    pass
                
            else :
                current_page_url = ""
                try:
                    current_page_url = page.url or ""
                except Exception:
                    current_page_url = ""
                logger.warning(
                    "        pdf 링크 미발견 : "
                    f"doi={doi_url}, current_url={current_page_url}, "
                    f"title={(page_title or '')[:160]}, domain={current_domain}"
                )

        except BrowserDisconnectedError as e:
            logger.warning(f"        시도 {attempt} 브라우저 연결 종료: {e}")
            if page:
                _close_page_safely(page, logger, session_plan=session_plan)
                page = None
            if attempt >= max_attempts:
                return _ret(False, "FAIL_TIMEOUT/NETWORK", [f"browser_disconnected: {e}"], stage="drission")
            time.sleep(min(2 + attempt, 5))
            continue
        except Exception as e:
            logger.warning(f"        시도 {attempt} 에러: {e}")
            # 에러 발생 시 브라우저 닫고 초기화 (다음 시도에서 재생성)
            if page:
                _close_page_safely(page, logger, session_plan=session_plan)
                page = None
            if attempt >= max_attempts:
                return _ret(False, "FAIL_NETWORK", [str(e)], stage="drission")
        
        time.sleep(per_attempt_sleep) # 재시도 전 대기

    return _ret(False, "FAIL_PARSE", ["pdf_link_not_found_or_download_failed"], stage="drission")


# =======================================================
# [보조] IEEE stamp.jsp URL 복구
# =======================================================
def _extract_ieee_arnumber(*candidates: str) -> Optional[str]:
    for candidate in candidates:
        text = str(candidate or "")
        if not text:
            continue
        parsed = urlparse(text)
        try:
            arnumber_values = parse_qs(parsed.query).get("arnumber") or []
        except Exception:
            arnumber_values = []
        if arnumber_values and str(arnumber_values[0]).strip():
            return str(arnumber_values[0]).strip()
        match = re.search(r"[?&]arnumber=(\d+)", text, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"/document/(\d+)", text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _recover_ieee_stamp_pdf_url(*candidates: str) -> Optional[str]:
    arnumber = _extract_ieee_arnumber(*candidates)
    if not arnumber:
        return None
    return f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={arnumber}&ref="


# =======================================================
# [핵심] 일반론적 HTML 구조 분석 (IEEE 로직 대폭 강화)
# =======================================================
def _analyze_html_structure_drission(page, logger):
    """
    DrissionPage 객체를 받아 HTML 구조를 분석하여 PDF 링크를 추출하는 함수
    (기존 Selenium analyze_html_structure의 DrissionPage 이식 버전)
    """
    current_url = page.url
    page_source = page.html
    logger.info("     [Drission] HTML 구조 정밀 분석 중...")

    # -------------------------------------------------------
    # 1. [IEEE 전용] stamp.jsp 페이지 처리
    # -------------------------------------------------------
    if "ieeexplore.ieee.org" in current_url and "stamp.jsp" in current_url:
        frame_wait_s = int(os.getenv("PDF_IEEE_IFRAME_WAIT_S", "8"))
        logger.info(f"        IEEE Stamp 페이지 감지. Iframe 로딩 대기중 (최대 {frame_wait_s}초)...")
        
        start_time = time.time()
        found_src = None
        
        while time.time() - start_time < frame_wait_s:
            try:
                # iframe 태그들 찾기
                frames = _eles_quick(page, 'tag:iframe', timeout=0.5)
                for f in frames:
                    s = f.attr('src')
                    if s:
                        # 조건: ielx7(전형적 패턴), .pdf, 또는 pdf가 포함된 긴 주소
                        if ("ielx7" in s or ".pdf" in s.lower() or "pdf" in s.lower()):
                            found_src = s
                            break
                
                if found_src:
                    break
                
                time.sleep(1) # 1초 대기 후 재시도
            except Exception:
                time.sleep(1)

        if found_src:
            if not found_src.startswith("http"):
                found_src = urljoin(current_url, found_src)
            logger.info(f"        IEEE Iframe SRC 발견: {found_src}")
            return found_src
        else:
            # 타임아웃 시 디버깅용 로그
            frames = _eles_quick(page, 'tag:iframe', timeout=0.5)
            src_list = [f.attr('src') for f in frames]
            logger.warning(f"        IEEE Iframe 로딩 실패. 발견된 iframe들: {src_list}")
            stamp_pdf_url = _recover_ieee_stamp_pdf_url(current_url, page_source)
            if stamp_pdf_url:
                logger.info(f"        [IEEE] arnumber 기반 stampPDF URL 복구: {stamp_pdf_url}")
                return stamp_pdf_url

    # # # -------------------------------------------------------
    # # # 2. ScienceDirect 전용 로직 -> new 시도
    # # # -------------------------------------------------------
    # if "sciencedirect.com" in page.url:
    #     import re
    #     current_url = page.url
    #     # URL에서 PII 추출 (예: /pii/S002195172030005X)
    #     match = re.search(r'/pii/([A-Z0-9]+)', current_url, re.IGNORECASE)
        
    #     if match:
    #         pii_code = match.group(1)
    #         # 이 URL 패턴이 403을 가장 잘 우회하는 "순수 PDF API" 형식입니다.
    #         # download=true 파라미터가 핵심입니다.
    #         pdf_heuristic_url = f"https://www.sciencedirect.com/science/article/pii/{pii_code}/pdfft?isDTM=true&download=true"
    #         logger.info(f"        [ScienceDirect] trying new url : {pdf_heuristic_url}")
    #         return pdf_heuristic_url

    # -------------------------------------------------------
    # 2. [ScienceDirect 전용] article shell -> pdfft 복구
    # -------------------------------------------------------
    if "sciencedirect.com" in current_url.lower() and "/science/article/" in current_url.lower():
        try:
            target_pii = _extract_sciencedirect_pii_from_text(current_url) or _extract_sciencedirect_pii_from_text(page_source)
            pdfft_url = _extract_sciencedirect_pdfft_url_from_html(page_source, target_pii=target_pii)
            if pdfft_url:
                logger.info(f"        [ScienceDirect] HTML 기반 pdfft 후보 복구: {pdfft_url}")
                return pdfft_url
        except Exception:
            pass

    # -------------------------------------------------------
    # 3. Iframe / Embed / Object (일반)
    # -------------------------------------------------------
    try:
        # css selector로 여러 태그 동시 검색
        frames = _eles_quick(page, 'css:iframe, embed, object', timeout=0.5)
        for frame in frames:
            src = frame.attr("src")
            if not src:
                # object 태그의 경우 data 속성을 사용하기도 함
                src = frame.attr("data")
            
            if src:
                src_lower = src.lower()
                if (".pdf" in src_lower or "pdfdirect" in src_lower or "ielx7" in src_lower or "blob:" in src_lower):
                    if not src.startswith("http") and not src.startswith("blob:"):
                        src = urljoin(current_url, src)
                    logger.info(f"        [Frame/Embed] 발견: {src}")
                    return src
    except Exception: 
        pass

    # -------------------------------------------------------
    # 4. Meta Tag
    # -------------------------------------------------------
    try:
        # citation_pdf_url 메타 태그 검색
        meta_pdf = _ele_quick(page, 'css:meta[name="citation_pdf_url"]', timeout=0.5)
        if meta_pdf:
            content = meta_pdf.attr("content")
            if content and content != current_url and not _is_supporting_info_blob(content):
                logger.info(f"        [Meta Tag] 발견: {content}")
                return content
    except Exception: 
        pass

    # -------------------------------------------------------
    # 5. Regex (페이지 소스 텍스트 검색)
    # -------------------------------------------------------
    patterns = [r'"pdfUrl":"([^"]+)"', r'"pdfPath":"([^"]+)"', r'content="([^"]+\.pdf)"', r'src="([^"]+\.pdf)"']
    for pat in patterns:
        match = re.search(pat, page_source, re.IGNORECASE)
        if match:
            url = match.group(1)
            # 유니코드 이스케이프 (\u002F -> /) 처리
            if "\\" in url:
                try: url = url.encode().decode('unicode-escape')
                except: pass
            
            if not url.startswith("http"): 
                url = urljoin(current_url, url)
                
            if _is_supporting_info_blob(url):
                continue
            if len(url) > 10 and url != current_url:
                logger.info(f"        [Regex] 발견: {url}")
                return url

    # -------------------------------------------------------
    # 6. Links (XPath 활용)
    # -------------------------------------------------------
    try:
        xpath_query = "//a[contains(translate(text(), 'PDF', 'pdf'), 'pdf') or contains(@href, '/pdf') or contains(@href, 'download=true')]"
        links = _eles_quick(page, f'xpath:{xpath_query}', timeout=0.5)
        best = None
        best_score = -10**9
        for link in links:
            href = link.attr("href")
            text = (link.text or "").strip().lower()
            if not href:
                continue
            href_low = href.lower()
            # javascript: 링크나 현재 페이지 링크 제외
            if "javascript" in href_low or href == current_url:
                continue

            abs_href = href if href.startswith("http") else urljoin(current_url, href)
            abs_low = abs_href.lower()
            if _is_supporting_info_blob(text, abs_low):
                continue

            score = 0
            if ".pdf" in abs_low:
                score += 8
            if any(k in abs_low for k in ("/doi/pdf", "/articlepdf", "/pdfft", "download=true")):
                score += 6
            if any(k in text for k in ("download pdf", "view pdf", "open pdf")):
                score += 4
            elif "pdf" in text:
                score += 2

            # 페이지/목차/프로시딩 링크는 우선순위를 강하게 낮춤
            if any(k in abs_low for k in ("/proceedings", "/session", "/program", "/contents", "/toc")):
                score -= 7
            if any(k in abs_low for k in (".jpg", ".jpeg", ".png", ".gif", ".svg")):
                score -= 10

            if score > best_score:
                best_score = score
                best = abs_href

        if best and best_score > 0:
            logger.info(f"        [Link] 발견(score={best_score}): {best}")
            return best
    except Exception:
        pass

    return None

# ======================================================
# sci-hub download
def try_manual_scihub(doi: str, pdf_dir: str, logger=None, max_total_s: int = 15) -> bool:
    mirrors = [
                "https://sci-hub.al",
                "https://sci-hub.mk",
                "https://sci-hub.ee",
                "https://sci-hub.vg",
                "https://sci-hub.kr",
               "https://sci-hub.st", 
               "https://sci-hub.red",
               "https://sci-hub.box", 
               "https://sci-hub.ru", 
               "https://sci-hub.in",
                # "https://sci-hub.se", 
               ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    filename = _sanitize_doi_to_filename(doi)
    filepath = os.path.join(pdf_dir, filename)

    if os.path.exists(filepath):
        logger.info(f"  - 이미 파일이 존재합니다: {filename}")
        return True

    max_total_s = max(3, min(int(max_total_s), MAX_ACTION_WAIT_S))
    started_at = time.monotonic()
    for mirror in mirrors:
        if (time.monotonic() - started_at) >= max_total_s:
            if logger:
                logger.info(f"Sci-hub 시간 예산 초과({max_total_s}s)로 중단")
            break
        try:
            target_url = f"{mirror}/{doi}"
            # print(f"  - Sci-Hub 접속 시도: {target_url}")
            resp = requests.get(target_url, headers=headers, timeout=(4, 6), verify=False)
            
            if resp.status_code != 200: continue
            
            content_type = resp.headers.get('Content-Type', '').lower()
            if 'application/pdf' in content_type or resp.content.startswith(b'%PDF'):
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                if logger: 
                    logger.info("Sci-Hub로 다운로드 성공!!!")
                return True
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            pdf_url = None

            # 1. Iframe 
            iframe = soup.select_one('iframe#pdf')
            if iframe:
                pdf_url = iframe.get('src')
            
            # 2. Embed 태그
            if not pdf_url:
                embed = soup.select_one('embed[type="application/pdf"]')
                if embed:
                    pdf_url = embed.get('src')
            
            # 3. 'save' 버튼 또는 링크
            if not pdf_url:
                save_btn = soup.select_one('div#buttons a[onclick]')
                if save_btn:
                    onclick_text = save_btn.get('onclick', '')
                    if "location.href" in onclick_text:
                        # location.href='url' 파싱
                        pdf_url = onclick_text.split("'")[1]
            
            # 4. div.download
            if not pdf_url:
                download_div = soup.find('div', class_='download')
                if download_div and download_div.find('a'):
                    pdf_url = download_div.find('a').get('href')

            if pdf_url:
                # URL 정규화
                if pdf_url.startswith('//'): pdf_url = 'https:' + pdf_url
                else: pdf_url = urljoin(mirror,pdf_url)
                pdf_url = pdf_url.split('#')[0]

                logger.info(f"  - PDF 주소 추출 성공: {pdf_url}")
                
                # 실제 파일 다운로드
                left = max_total_s - (time.monotonic() - started_at)
                if left <= 0:
                    break
                pdf_content = requests.get(pdf_url, headers=headers, timeout=(4, min(8, max(2, int(left)))), verify=False)
                if pdf_content.status_code == 200 and b'%PDF' in pdf_content.content[:1024]:
                    with open(filepath, 'wb') as f:
                        f.write(pdf_content.content)
                    logger.info("Sci-Hub로 다운로드 성공!!!")
                    return True
        except Exception as e:
            logger.warning(f"  - 미러 {mirror} 시도 중 오류: {e}")
            time.sleep(0.2)
            continue
    
    logger.warning("Sci-hub 방법 전부 실패")
    return False

    
    
# =======================================================
import re
from typing import Optional, Dict
import requests

PREFIX_EXACT_MAP: Dict[str, str] = {
    "10.1002": "WILEY",
    "10.1007": "Springer",
    "10.1016": "ELSEVIER",
    "10.1021": "ACS",
    "10.1038": "Nature",
    "10.1039": "RSC",
    "10.1063": "AIP",
    "10.1088": "IOP",
    "10.1109": "IEEE",
    "10.1111": "WILEY",
    "10.1117": "SPIE",
    "10.3390": "MDPI",
    # CELL은 DOI prefix만으로 ELSEVIER(10.1016)와 분리가 어려움
}

# 2) Crossref가 돌려주는 registrant(스튜어드) name의 변형들을 "원하는 라벨"로 통일
def normalize_publisher_label(raw_name: str, prefix: Optional[str] = None) -> Optional[str]:
    """
    raw_name: Crossref /prefixes/{prefix} 응답의 message.name (등록자/스튜어드 이름)
    prefix: (선택) prefix를 같이 주면 보조 규칙에 활용
    """
    prefix_key = extract_doi_prefix(prefix or "")
    if prefix_key in PREFIX_EXACT_MAP:
        return PREFIX_EXACT_MAP[prefix_key]

    if not raw_name or str(raw_name) == 'non':
        return None

    n = raw_name.lower().strip()

    # Nature/Springer 계열은 DOI prefix가 더 신뢰할 수 있다.
    if prefix_key == "10.1038":
        return "Nature"
    if prefix_key == "10.1007":
        return "Springer"
    if ("nature" in n) or ("npg" in n):
        return "Nature"
    if "springer" in n:
        if "springer nature" in n:
            return "Nature"
        return "Springer"

    # ACS
    if ("american chemical society" in n) or ('acs' in n) or re.search(r"\bacs\b", n):
        return "ACS"

    # RSC
    if ("royal society of chemistry" in n) or re.search(r"\brsc\b", n):
        return "RSC"

    # AIP (AIP Publishing / American Institute of Physics 등 변형 흡수)
    if ("aip" in n) or ("american institute of physics" in n) or re.search(r"\baip\b", n):
        return "AIP"

    # IOP (IOP Publishing / Institute of Physics 등 변형 흡수)
    if ("iop publishing" in n) or ("institute of physics" in n) or re.search(r"\biop\b", n):
        return "IOP"

    # IEEE
    if ("institute of electrical and electronics engineers" in n) or re.search(r"\bieee\b", n):
        return "IEEE"

    # MDPI
    if ("multidisciplinary digital publishing institute" in n) or re.search(r"\bmdpi\b", n):
        return "MDPI"

    # SPIE
    if ("spie" in n) or ("society of photo-optical instrumentation engineers" in n):
        return "SPIE"

    # ELSEVIER
    if "elsevier" in n:
        return "ELSEVIER"

    # WILEY
    if ("wiley" in n) or ("advanced materials" in n):
        return "WILEY"

    # CELL (Cell Press 등)
    if ("cell press" in n) or re.search(r"\bcell\b", n):
        return "CELL"

    return None


def extract_doi_prefix(prefix_or_doi: str) -> Optional[str]:
    """
    입력이 '10.1016' 같은 prefix일 수도 있고, '10.1016/j.xxx...' 같은 DOI일 수도 있으니 prefix만 추출.
    """
    if not prefix_or_doi:
        return None
    m = re.search(r"(10\.\d{4,9})", prefix_or_doi.strip())
    return m.group(1) if m else None


def get_publisher_from_doi_prefix(
    prefix_or_doi: str,
    *,
    mailto: Optional[str] = None,
    timeout: float = 20.0,
    return_raw_if_unmapped: bool = False,
) -> Optional[str]:
    """
    Crossref REST API /prefixes/{prefix}를 이용해 prefix의 steward(등록자) 이름을 받고,
    이를 사용자가 원하는 퍼블리셔 라벨로 정규화해 반환.

    - 반환 예: "Nature", "ACS", "RSC", "AIP", "IOP", "IEEE", "ELSEVIER", "WILEY", "CELL"
    - 매핑 실패 시: None (혹은 return_raw_if_unmapped=True면 raw registrant name)
    """
    prefix = extract_doi_prefix(prefix_or_doi)
    if not prefix:
        return None

    # 1) prefix만으로 확정 가능한 경우 즉시 반환
    if prefix in PREFIX_EXACT_MAP:
        return PREFIX_EXACT_MAP[prefix]

    # 2) Crossref /prefixes/{prefix} 호출
    #    이 엔드포인트는 steward name과 member ID를 돌려줍니다. :contentReference[oaicite:1]{index=1}
    url = f"https://api.crossref.org/prefixes/{prefix}"
    params = {}
    if mailto:
        params["mailto"] = mailto  # polite pool 사용 권장 패턴에 부합 :contentReference[oaicite:2]{index=2}

    try:
        r = requests.get(url, params=params, timeout=timeout, headers={"Accept": "application/json"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
    except requests.RequestException:
        return None
    except ValueError:
        return None

    msg = data.get("message") or {}
    raw_name = msg.get("name") or ""

    # 3) raw registrant name -> 원하는 라벨로 정규화
    label = normalize_publisher_label(raw_name, prefix=prefix)
    if label:
        return label

    return raw_name if (return_raw_if_unmapped and raw_name) else None

import re
from typing import Optional



def download_via_acspdf(doi: str, output_path: str, logger = None, metrics_extra=None) -> bool:
    pdf_url = f"https://pubs.acs.org/doi/pdf/{doi}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://pubs.acs.org/doi/{doi}",
    }
    referer = headers["Referer"]
    return bool(download_with_cffi(pdf_url, output_path, referer, logger=logger, metrics_extra=metrics_extra))


def download_via_aippdf(doi: str, output_path: str, logger = None, metrics_extra=None) -> bool:
    # AIP의 aip.scitation/avs.scitation doi/pdf 엔드포인트는 브라우저 없이 접근하면
    # JS gate HTML로 떨어지는 경우가 많아 불필요한 실패와 시도 횟수만 늘린다.
    # 여기서는 API/direct 단계에서 건너뛰고 브라우저 landing에서 실제 article-pdf 경로를 찾는다.
    if logger:
        logger.info(f"        [AIP] direct doi/pdf wrapper 스킵 -> browser landing 우선: {doi}")
    return False


def download_via_ioppdf(doi: str, output_path: str, logger = None, metrics_extra=None) -> bool:
    pdf_url = f"https://iopscience.iop.org/article/{doi}/pdf"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://iopscience.iop.org/article/{doi}",
    }
    referer = headers["Referer"]
    return bool(download_with_cffi(pdf_url, output_path, referer, logger=logger, metrics_extra=metrics_extra))


def download_via_wiley(doi: str, output_path: str, logger = None, metrics_extra=None):
    """
    Download the PDF of a Wiley article via the Wiley TDM API.
    Requires a Wiley API key.
    """
    api_key = WILEY_API_KEY
    if not api_key:
        if logger:
            logger.info("WILEY_API_KEY 미설정 -> cookie-less direct fallback 생략, browser landing으로 이관")
        return False
    base_url = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"
    url = base_url + doi
    headers = {"Wiley-TDM-Client-Token": api_key}
    response = requests.get(url, headers=headers, timeout=20)
    try:
        if response.status_code == 200:
            with open(output_path, 'wb') as file:
                file.write(response.content)
            if logger:
                logger.info(f"{doi} downloaded successfully via Wiley API")
            return True
        if logger:
            logger.warning(
                f"Wiley API failed status={response.status_code} for doi={doi}, "
                "cookie-less direct fallback 생략 -> browser landing으로 이관"
            )
        return False
    except Exception as e:
        # Provide a more specific hint on failure
        raise Exception(
            f"Wiley API download failed: {e}. Ensure your API key is correct and you have access rights.")
def _download_direct_pdf_candidates(
    doi: str,
    output_path: str,
    candidates: list[tuple[str, str]],
    logger = None,
    metrics_extra=None,
) -> bool:
    for idx, (pdf_url, referer) in enumerate(candidates):
        if logger:
            logger.info(f"        [DirectPDF] 후보 시도: {pdf_url}")
        result = download_with_cffi(
            pdf_url,
            output_path,
            referer=referer,
            logger=logger,
            return_detail=True,
            metrics_extra={
                "candidate_index": idx,
                "candidate_url": pdf_url,
                **(metrics_extra or {}),
            },
        )
        if result.get("ok"):
            return True
        if logger:
            logger.info(
                f"        [DirectPDF] 후보 실패 reason={result.get('reason') or 'unknown'} "
                f"status={result.get('http_status')}"
            )
    return False


def _nature_article_id_from_doi(doi: str) -> str:
    norm = str(doi or "").strip()
    if "/" not in norm:
        return norm
    return norm.split("/", 1)[1]


def download_via_naturepdf(doi: str, output_path: str, logger = None, metrics_extra=None):
    """
    Download the PDF of a Nature article by constructing the direct Nature PDF URL.
    """
    article_id = _nature_article_id_from_doi(doi)
    candidates = [
        (
            f"https://www.nature.com/articles/{article_id}.pdf",
            f"https://www.nature.com/articles/{article_id}",
        ),
        (
            f"https://www.nature.com/articles/{article_id}_reference.pdf",
            f"https://www.nature.com/articles/{article_id}",
        ),
        (
            f"https://www.nature.com/articles/{article_id}.pdf?download=1",
            f"https://www.nature.com/articles/{article_id}",
        ),
    ]
    return _download_direct_pdf_candidates(doi, output_path, candidates, logger=logger, metrics_extra=metrics_extra)


def download_via_springerpdf(doi: str, output_path: str, logger = None, metrics_extra=None):
    """
    Download the PDF of a Springer article by constructing the direct SpringerLink PDF URL.
    """
    candidates = [
        (
            f"https://link.springer.com/content/pdf/{doi}.pdf",
            f"https://link.springer.com/article/{doi}",
        ),
        (
            f"https://link.springer.com/content/pdf/{doi}.pdf?download=1",
            f"https://link.springer.com/article/{doi}",
        ),
    ]
    return _download_direct_pdf_candidates(doi, output_path, candidates, logger=logger, metrics_extra=metrics_extra)
    
def download_using_api(doi: str, output_path: str, publisher: str, logger = None, metrics_extra=None):
    """
    Attempt to download the article PDF using publisher-specific API methods.
    Raises an Exception if no suitable method is found or if the download fails.
    """
    TOOL_FUNCTIONS = {
        "wiley": download_via_wiley,
        "nature": download_via_naturepdf,
        "springer": download_via_springerpdf,
        "acs": download_via_acspdf,
        "aip": download_via_aippdf,
        "iop": download_via_ioppdf,
    }
    
    filename = _sanitize_doi_to_filename(doi)
    filepath = os.path.join(output_path, filename)

    if not publisher :
        logger.warning("Publisher is Not recognized or Not supported, cannot use API method.")
        raise Exception("Publisher is Not recognized or Not supported, cannot use API method.")
    
    publisher_key = publisher.lower()
    if publisher_key in TOOL_FUNCTIONS:
        download_func = TOOL_FUNCTIONS[publisher_key]
        logger.info(f"Trying download using api or url for publisher : {publisher_key}, doi : {doi}")
        return download_func(doi, filepath, logger, metrics_extra=metrics_extra)
    else:
        logger.warning(f"No download method available for publisher: {publisher}")
        raise Exception(f"No download method available for publisher: {publisher}")

MOUSE_PATCH_JS =  """
function getRandomInt(min, max) {
return Math.floor(Math.random() * (max - min + 1)) + min;
}
let screenX = getRandomInt(800, 1200);
let screenY = getRandomInt(400, 600);
Object.defineProperty(MouseEvent.prototype, 'screenX', { value: screenX });
Object.defineProperty(MouseEvent.prototype, 'screenY', { value: screenY });
"""
