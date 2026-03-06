import argparse
import csv
import json
import os
import re
import shutil
import socket
import subprocess
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from statistics import median
from typing import Dict, List
from urllib.parse import urlparse
from urllib.request import urlopen

from DrissionPage import ChromiumOptions, ChromiumPage

from tools_exp import (
    _apply_best_browser_profile,
    _extract_elsevier_retrieve_handoff_url,
    _has_article_signal,
    _has_cookie_or_consent_signal,
    _has_pdf_action_signal,
    _is_elsevier_retrieve_url,
    _sanitize_doi_to_filename,
    detect_access_issue,
)

OUT_SUCCESS_ACCESS = "SUCCESS_ACCESS"
OUT_FAIL_CAPTCHA = "FAIL_CAPTCHA"
OUT_FAIL_BLOCK = "FAIL_BLOCK"
OUT_FAIL_ACCESS_RIGHTS = "FAIL_ACCESS_RIGHTS"
OUT_FAIL_NETWORK = "FAIL_NETWORK"


def _valid_doi(doi: str) -> bool:
    return bool(re.match(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$", doi))


def load_dois(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    seen = set()
    out = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        doi_col = None
        for c in (reader.fieldnames or []):
            if c and c.strip().lower() == "doi":
                doi_col = c
                break
        if doi_col is None:
            raise ValueError("CSV DOI column not found")

        for row in reader:
            raw = str(row.get(doi_col, "")).strip().lower()
            if not raw:
                continue
            doi = raw.replace("https://doi.org/", "").replace("http://doi.org/", "").strip()
            if doi in seen:
                continue
            if not _valid_doi(doi):
                continue
            seen.add(doi)
            out.append(doi)
    return out


def _browser_for_worker(
    chrome_path: str,
    worker_idx: int,
    worker_profile_root: str,
    startup_retries: int = 3,
    retry_sleep_sec: float = 1.5,
) -> ChromiumPage:
    startup_retries = max(1, int(startup_retries))
    worker_profile_root = os.path.abspath(worker_profile_root)
    os.makedirs(worker_profile_root, exist_ok=True)
    worker_profile_name = f"worker_{int(worker_idx)}"

    last_err = None
    for _ in range(startup_retries):
        co = ChromiumOptions()
        if chrome_path and os.path.exists(chrome_path):
            co.set_browser_path(chrome_path)
        co.set_user_data_path(worker_profile_root)
        co.set_user(worker_profile_name)
        co.auto_port()
        _apply_best_browser_profile(co)
        try:
            return ChromiumPage(co)
        except Exception as e:
            last_err = e
            time.sleep(max(0.3, float(retry_sleep_sec)))

    raise RuntimeError(
        f"browser_init_failed(worker={worker_idx}, chrome_path={chrome_path}, "
        f"profile_root={worker_profile_root}, profile={worker_profile_name}): {last_err}"
    )


def _resolve_browser_path(preferred_path: str) -> str:
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

    seen = set()
    for path in candidates:
        p = str(path or "").strip()
        if not p or p in seen:
            continue
        seen.add(p)
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    return ""


def _pick_free_local_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _run_chrome_smoke(chrome_path: str, profile_root: str, no_sandbox: bool, single_process: bool) -> Dict[str, str]:
    smoke_dir = os.path.join(profile_root, "_smoke")
    shutil.rmtree(smoke_dir, ignore_errors=True)
    os.makedirs(smoke_dir, exist_ok=True)
    port = _pick_free_local_port()

    cmd = [
        chrome_path,
        "--headless=new",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-domain-reliability",
        "--disable-sync",
        "--disable-extensions",
        "--metrics-recording-only",
        "--disable-features=MediaRouter,OptimizationHints",
        f"--user-data-dir={smoke_dir}",
        f"--remote-debugging-port={port}",
        "--remote-debugging-address=127.0.0.1",
        "about:blank",
    ]
    if no_sandbox:
        cmd.append("--no-sandbox")
    if single_process:
        cmd.extend(["--single-process", "--no-zygote"])

    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.time() + 25
        version_body = ""
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            try:
                with urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1.2) as resp:
                    version_body = (resp.read() or b"").decode("utf-8", errors="ignore")
                if version_body:
                    return {
                        "ok": "1",
                        "stderr": "",
                        "stdout": version_body[-500:],
                        "mode": "single" if single_process else "normal",
                        "returncode": "0",
                    }
            except Exception:
                time.sleep(0.5)
    except Exception as e:
        return {"ok": "0", "stderr": str(e), "stdout": "", "mode": "single" if single_process else "normal"}
    finally:
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    out = ""
    err = ""
    if proc is not None:
        try:
            out, err = proc.communicate(timeout=1)
        except Exception:
            pass
    return {
        "ok": "0",
        "stderr": (err or "")[-2000:],
        "stdout": (out or "")[-500:],
        "mode": "single" if single_process else "normal",
        "returncode": str(proc.returncode if proc is not None and proc.returncode is not None else -1),
    }


def _build_artifact_zip(records: List[Dict], artifact_dir: str, zip_path: str, target: str) -> str:
    target = str(target or "").strip().lower()
    if target == "success":
        target_records = [r for r in records if (r.get("outcome") == OUT_SUCCESS_ACCESS)]
    else:
        target = "fail"
        target_records = [r for r in records if (r.get("outcome") != OUT_SUCCESS_ACCESS)]

    if not target_records:
        return ""

    abs_artifact_dir = os.path.abspath(artifact_dir)
    zip_path = os.path.abspath(zip_path)
    os.makedirs(os.path.dirname(zip_path) or ".", exist_ok=True)

    manifest = []
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rec in target_records:
            row = {
                "doi": rec.get("doi", ""),
                "outcome": rec.get("outcome", ""),
                "resolved_url": rec.get("resolved_url", ""),
                "evidence": rec.get("evidence", []),
                "screenshot_path": rec.get("screenshot_path", ""),
                "html_path": rec.get("html_path", ""),
                "meta_path": rec.get("meta_path", ""),
            }
            manifest.append(row)

            for key in ("screenshot_path", "html_path", "meta_path"):
                p = str(rec.get(key, "") or "").strip()
                if not p or (not os.path.isfile(p)):
                    continue
                if p.startswith(abs_artifact_dir):
                    rel = os.path.relpath(p, start=abs_artifact_dir)
                    arc = os.path.join("artifacts", rel)
                else:
                    arc = os.path.join("artifacts", os.path.basename(p))
                zf.write(p, arcname=arc)

        zf.writestr(f"manifest_{target}.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    return zip_path


def _verify_landing_success(
    doi: str,
    url: str,
    domain: str,
    title: str,
    html: str,
    article_signal: bool,
    pdf_action_signal: bool,
) -> bool:
    low_url = str(url or "").lower()
    low_domain = str(domain or "").lower()
    low_title = str(title or "").strip().lower()
    low_html = str(html or "").lower()

    if (not low_domain) or low_domain.endswith("doi.org"):
        return False
    if _is_elsevier_retrieve_url(low_url):
        return False
    challenge_markers = (
        "__cf_chl_rt_tk=",
        "/cdn-cgi/challenge",
        "/cdn-cgi/l/chk_captcha",
        "challenges.cloudflare.com",
        "validate.perfdrive.com",
        "cf-turnstile",
        "__cf_chl_opt",
        "checking your browser before accessing",
    )
    if any(m in low_url or m in low_html or m in low_title for m in challenge_markers):
        return False
    if ("pubs.aip.org" in low_domain) and ("__cf_chl_rt_tk=" in low_url):
        return False
    doi_norm = str(doi or "").strip().lower()
    if doi_norm.startswith("10.1016") and ("sciencedirect.com" not in low_domain):
        return False
    if article_signal or pdf_action_signal:
        return True
    if low_title in ("redirecting", "redirecting...", "redirect"):
        return False
    if len(low_title) < 12:
        return False
    if len(low_html) < 600:
        return False
    return True


def _save_probe_artifacts(
    page: ChromiumPage,
    doi: str,
    artifact_dir: str,
    title: str,
    html: str,
    final_url: str,
    outcome: str,
    include_html: bool,
) -> Dict[str, str]:
    out = {"screenshot": "", "html": "", "meta": ""}
    if page is None or not artifact_dir:
        return out
    bucket = "success" if outcome == OUT_SUCCESS_ACCESS else "fail"
    artifact_dir = os.path.abspath(os.path.join(artifact_dir, bucket))
    os.makedirs(artifact_dir, exist_ok=True)
    safe = _sanitize_doi_to_filename(doi).replace(".pdf", "")
    ts = int(time.time() * 1000)
    prefix = "landing_success" if outcome == OUT_SUCCESS_ACCESS else "landing_fail"
    ss_name = f"{prefix}_{safe}_{ts}.png"
    html_name = f"{prefix}_{safe}_{ts}.html"
    meta_name = f"{prefix}_{safe}_{ts}.json"

    try:
        page.get_screenshot(path=artifact_dir, name=ss_name, full_page=False)
        out["screenshot"] = os.path.abspath(os.path.join(artifact_dir, ss_name))
    except Exception:
        pass
    if include_html:
        try:
            html_path = os.path.abspath(os.path.join(artifact_dir, html_name))
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html or "")
            out["html"] = html_path
        except Exception:
            pass
    try:
        meta_path = os.path.abspath(os.path.join(artifact_dir, meta_name))
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "doi": doi,
                    "captured_at_ms": ts,
                    "outcome": outcome,
                    "final_url": final_url,
                    "title": (title or "")[:400],
                    "html_len": len(html or ""),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        out["meta"] = meta_path
    except Exception:
        pass
    return out


def _probe_one(
    page: ChromiumPage,
    doi: str,
    timeout_sec: float,
    capture_fail_artifacts: bool,
    capture_success_artifacts: bool,
    capture_success_html: bool,
    artifact_dir: str,
) -> Dict:
    started = time.perf_counter()
    doi_url = f"https://doi.org/{doi}"
    final_url = ""
    domain = ""
    title = ""
    html = ""
    issue = None
    evidence = []
    article_signal = False
    pdf_action_signal = False
    consent_signal = False
    verified_success = False

    try:
        page.get(doi_url, retry=1, interval=1, timeout=timeout_sec)
        final_url = page.url or doi_url
        domain = (urlparse(final_url).netloc or "").lower()
        title = page.title or ""
        html = page.html or ""

        # Elsevier는 retrieve 랜딩에서 "Redirecting" 상태로 멈출 수 있어 1회 handoff 보정
        if _is_elsevier_retrieve_url(final_url):
            time.sleep(2.2)
            final_url = page.url or final_url
            domain = (urlparse(final_url).netloc or "").lower()
            title = page.title or ""
            html = page.html or ""
            if _is_elsevier_retrieve_url(final_url):
                handoff = _extract_elsevier_retrieve_handoff_url(final_url, html)
                if handoff:
                    try:
                        page.get(handoff, retry=0, interval=0.5, timeout=min(timeout_sec, 12))
                        time.sleep(1.0)
                    except Exception:
                        pass
                final_url = page.url or final_url
                domain = (urlparse(final_url).netloc or "").lower()
                title = page.title or ""
                html = page.html or ""

        article_signal = bool(_has_article_signal(title=title, html=html))
        pdf_action_signal = bool(_has_pdf_action_signal(title=title, html=html))
        consent_signal = bool(_has_cookie_or_consent_signal(title=title, html=html))
        issue, evidence = detect_access_issue(title=title, html=html, url=final_url, domain=domain)
        unexpected_landing = (
            (not domain)
            or ("google." in domain)
            or final_url.startswith("chrome://")
            or final_url.startswith("about:blank")
        )
        if unexpected_landing:
            outcome = OUT_FAIL_NETWORK
            evidence = (evidence or []) + [f"unexpected_landing={final_url}"]
        elif issue == OUT_FAIL_CAPTCHA:
            outcome = OUT_FAIL_CAPTCHA
        elif issue == OUT_FAIL_BLOCK:
            outcome = OUT_FAIL_BLOCK
        elif issue == OUT_FAIL_ACCESS_RIGHTS:
            outcome = OUT_FAIL_ACCESS_RIGHTS
        else:
            verified_success = _verify_landing_success(
                doi=doi,
                url=final_url,
                domain=domain,
                title=title,
                html=html,
                article_signal=article_signal,
                pdf_action_signal=pdf_action_signal,
            )
            if verified_success:
                outcome = OUT_SUCCESS_ACCESS
            else:
                outcome = OUT_FAIL_BLOCK
                evidence = (evidence or []) + ["unverified_landing"]
                low_url = str(final_url or "").lower()
                low_title = str(title or "").lower()
                low_html = str(html or "").lower()
                if any(
                    m in low_url or m in low_title or m in low_html
                    for m in (
                        "__cf_chl_rt_tk=",
                        "/cdn-cgi/challenge",
                        "/cdn-cgi/l/chk_captcha",
                        "challenges.cloudflare.com",
                        "validate.perfdrive.com",
                        "cf-turnstile",
                        "__cf_chl_opt",
                        "checking your browser before accessing",
                    )
                ):
                    evidence.append("verify=challenge_marker")
                if str(doi or "").strip().lower().startswith("10.1016") and ("sciencedirect.com" not in str(domain or "").lower()):
                    evidence.append("verify=elsevier_not_sciencedirect")
                if str(domain or "").lower().endswith("doi.org"):
                    evidence.append("verify=still_on_doi_domain")
                if (len(low_html) < 600) and (not article_signal) and (not pdf_action_signal):
                    evidence.append("verify=thin_html_no_article_signal")
                if _is_elsevier_retrieve_url(final_url):
                    evidence.append("elsevier_retrieve_stuck")
    except Exception as e:
        outcome = OUT_FAIL_NETWORK
        evidence = [str(e)[:300]]
        final_url = final_url or doi_url
        domain = domain or (urlparse(final_url).netloc or "").lower()

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    artifacts = {"screenshot": "", "html": "", "meta": ""}
    should_capture = (
        (capture_fail_artifacts and outcome != OUT_SUCCESS_ACCESS)
        or (capture_success_artifacts and outcome == OUT_SUCCESS_ACCESS)
    )
    if should_capture:
        artifacts = _save_probe_artifacts(
            page=page,
            doi=doi,
            artifact_dir=artifact_dir,
            title=title,
            html=html,
            final_url=final_url,
            outcome=outcome,
            include_html=(outcome != OUT_SUCCESS_ACCESS) or bool(capture_success_html),
        )

    verification_level = "n/a"
    if outcome == OUT_SUCCESS_ACCESS:
        verification_level = "high" if (article_signal or pdf_action_signal) else "low"

    return {
        "doi": doi,
        "doi_url": doi_url,
        "resolved_url": final_url,
        "domain": domain,
        "title": title[:240],
        "outcome": outcome,
        "issue": issue or "",
        "evidence": evidence,
        "article_signal": article_signal,
        "pdf_action_signal": pdf_action_signal,
        "consent_signal": consent_signal,
        "verification_level": verification_level,
        "verified_success": bool(verified_success),
        "is_elsevier_retrieve": bool(_is_elsevier_retrieve_url(final_url)),
        "screenshot_path": artifacts.get("screenshot", ""),
        "html_path": artifacts.get("html", ""),
        "meta_path": artifacts.get("meta", ""),
        "html_len": len(html or ""),
        "elapsed_ms": elapsed_ms,
        "timestamp_ms": int(time.time() * 1000),
    }


def _worker_run(
    worker_idx: int,
    dois: List[str],
    out_jsonl: str,
    chrome_path: str,
    worker_profile_root: str,
    startup_retries: int,
    timeout_sec: float,
    progress_every: int,
    capture_fail_artifacts: bool,
    capture_success_artifacts: bool,
    capture_success_html: bool,
    artifact_dir: str,
) -> Dict:
    page = _browser_for_worker(
        chrome_path=chrome_path,
        worker_idx=worker_idx,
        worker_profile_root=worker_profile_root,
        startup_retries=startup_retries,
    )
    records = []
    done = 0
    success = 0
    try:
        with open(out_jsonl, "w", encoding="utf-8") as f:
            for doi in dois:
                rec = _probe_one(
                    page,
                    doi,
                    timeout_sec=timeout_sec,
                    capture_fail_artifacts=capture_fail_artifacts,
                    capture_success_artifacts=capture_success_artifacts,
                    capture_success_html=capture_success_html,
                    artifact_dir=artifact_dir,
                )
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.append(rec)
                done += 1
                if rec["outcome"] == OUT_SUCCESS_ACCESS:
                    success += 1
                if progress_every > 0 and (done % progress_every == 0 or done == len(dois)):
                    print(
                        f"[landing_repro|worker={worker_idx}] progress {done}/{len(dois)} (success={success})",
                        flush=True,
                    )
    finally:
        try:
            page.quit()
        except Exception:
            pass
    return {"worker": worker_idx, "done": done, "success": success, "records": records}


def _summarize(records: List[Dict]) -> Dict:
    total = len(records)
    counts = Counter(r.get("outcome") for r in records)
    elapsed = sorted(int(r.get("elapsed_ms", 0)) for r in records)

    def pct(v: float) -> float:
        if not elapsed:
            return 0.0
        if len(elapsed) == 1:
            return float(elapsed[0])
        idx = (len(elapsed) - 1) * v
        lo = int(idx)
        hi = min(lo + 1, len(elapsed) - 1)
        frac = idx - lo
        return float(elapsed[lo] * (1 - frac) + elapsed[hi] * frac)

    by_domain = defaultdict(
        lambda: {"n": 0, "success": 0, "captcha": 0, "block": 0, "access_rights": 0, "network": 0, "elapsed": []}
    )
    for r in records:
        d = r.get("domain") or ""
        o = r.get("outcome")
        m = by_domain[d]
        m["n"] += 1
        if o == OUT_SUCCESS_ACCESS:
            m["success"] += 1
        elif o == OUT_FAIL_CAPTCHA:
            m["captcha"] += 1
        elif o == OUT_FAIL_BLOCK:
            m["block"] += 1
        elif o == OUT_FAIL_ACCESS_RIGHTS:
            m["access_rights"] += 1
        else:
            m["network"] += 1
        m["elapsed"].append(int(r.get("elapsed_ms", 0)))

    domain_rows = []
    for d, m in by_domain.items():
        n = max(1, m["n"])
        vals = sorted(m["elapsed"])
        domain_rows.append(
            {
                "domain": d,
                "n": m["n"],
                "success_rate": round(m["success"] / n, 4),
                "captcha_rate": round(m["captcha"] / n, 4),
                "block_rate": round(m["block"] / n, 4),
                "access_rights_rate": round(m["access_rights"] / n, 4),
                "network_rate": round(m["network"] / n, 4),
                "p50_elapsed_ms": round(float(median(vals)), 1) if vals else 0.0,
                "p90_elapsed_ms": round(float(vals[int((len(vals) - 1) * 0.9)]), 1) if vals else 0.0,
            }
        )
    domain_rows.sort(key=lambda x: (x["captcha_rate"] + x["block_rate"], x["n"]), reverse=True)

    success = counts.get(OUT_SUCCESS_ACCESS, 0)
    blocked = counts.get(OUT_FAIL_CAPTCHA, 0) + counts.get(OUT_FAIL_BLOCK, 0)
    access_rights = counts.get(OUT_FAIL_ACCESS_RIGHTS, 0)
    return {
        "total_valid": total,
        "success_ratio": round(success / total, 4) if total else 0.0,
        "block_captcha_rate": round(blocked / total, 4) if total else 0.0,
        "access_rights_rate": round(access_rights / total, 4) if total else 0.0,
        "outcome_counts": dict(counts),
        "p50_elapsed_ms": round(pct(0.5), 1),
        "p90_elapsed_ms": round(pct(0.9), 1),
        "domain_breakdown_top20": domain_rows[:20],
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Reproduce landing-page access success using current download browser settings")
    p.add_argument("--input", type=str, default="ready_to_download.csv")
    p.add_argument("--max-dois", type=int, default=0)
    p.add_argument("--workers", type=int, default=5)
    p.add_argument("--startup-retries", type=int, default=3)
    p.add_argument("--timeout-sec", type=float, default=20.0)
    p.add_argument("--progress-every", type=int, default=50)
    p.add_argument("--chrome-path", type=str, default=os.environ.get("CHROME_PATH", ""))
    p.add_argument("--headless", type=int, default=1, choices=[0, 1])
    p.add_argument("--no-sandbox", type=int, default=1, choices=[0, 1])
    p.add_argument("--server-tuned", type=int, default=1, choices=[0, 1])
    p.add_argument("--single-process", type=int, default=0, choices=[0, 1])
    p.add_argument("--humanized-browser", type=int, default=1, choices=[0, 1])
    p.add_argument("--assume-institution-access", type=int, default=1, choices=[0, 1])
    p.add_argument("--profile-mode", type=str, default="auto")
    p.add_argument("--profile-name", type=str, default="Default")
    p.add_argument("--persistent-profile-dir", type=str, default="outputs/.chrome_user_data")
    p.add_argument("--worker-profile-root", type=str, default="")
    p.add_argument("--clean-worker-profiles", type=int, default=1, choices=[0, 1])
    p.add_argument("--capture-fail-artifacts", type=int, default=1, choices=[0, 1])
    p.add_argument("--capture-success-artifacts", type=int, default=1, choices=[0, 1])
    p.add_argument("--capture-success-html", type=int, default=0, choices=[0, 1])
    p.add_argument("--artifact-dir", type=str, default="outputs/landing_access_artifacts")
    p.add_argument("--zip-fail-artifacts", type=int, default=1, choices=[0, 1])
    p.add_argument("--artifact-zip", type=str, default="outputs/landing_access_failures.zip")
    p.add_argument("--zip-success-artifacts", type=int, default=1, choices=[0, 1])
    p.add_argument("--success-artifact-zip", type=str, default="outputs/landing_access_successes.zip")
    p.add_argument("--output-jsonl", type=str, default="outputs/landing_access_repro.jsonl")
    p.add_argument("--report", type=str, default="outputs/landing_access_repro_report.json")
    args = p.parse_args()

    dois = load_dois(args.input)
    if args.max_dois and args.max_dois > 0:
        dois = dois[: args.max_dois]
    if not dois:
        raise RuntimeError("No valid DOI found.")

    workers = max(1, int(args.workers))
    chunks = [[] for _ in range(workers)]
    for i, doi in enumerate(dois):
        chunks[i % workers].append(doi)

    os.environ["PDF_BROWSER_HEADLESS"] = "1" if int(args.headless) == 1 else "0"
    os.environ["PDF_BROWSER_NO_SANDBOX"] = "1" if int(args.no_sandbox) == 1 else "0"
    os.environ["PDF_BROWSER_SERVER_TUNED"] = "1" if int(args.server_tuned) == 1 else "0"
    os.environ["PDF_BROWSER_SINGLE_PROCESS"] = "1" if int(args.single_process) == 1 else "0"
    os.environ["PDF_BROWSER_HUMANIZED"] = "1" if int(args.humanized_browser) == 1 else "0"
    os.environ["PDF_ASSUME_INSTITUTION_ACCESS"] = "1" if int(args.assume_institution_access) == 1 else "0"
    os.environ["PDF_BROWSER_PROFILE_MODE"] = str(args.profile_mode or "auto").strip()
    os.environ["PDF_BROWSER_PROFILE_NAME"] = str(args.profile_name or "Default").strip() or "Default"
    os.environ["PDF_BROWSER_PERSISTENT_PROFILE_DIR"] = os.path.abspath(str(args.persistent_profile_dir))
    worker_profile_root = str(args.worker_profile_root or "").strip()
    if not worker_profile_root:
        run_base = os.environ.get("SLURM_TMPDIR", "").strip()
        if not run_base:
            run_base = os.path.join("/tmp", os.environ.get("USER", "user"))
        worker_profile_root = os.path.join(run_base, "landing_worker_profiles")
    worker_profile_root = os.path.abspath(worker_profile_root)
    if int(args.clean_worker_profiles) == 1 and os.path.isdir(worker_profile_root):
        shutil.rmtree(worker_profile_root, ignore_errors=True)
    os.makedirs(worker_profile_root, exist_ok=True)

    os.makedirs(os.path.dirname(args.output_jsonl) or ".", exist_ok=True)
    if int(args.capture_fail_artifacts) == 1:
        os.makedirs(args.artifact_dir, exist_ok=True)
    worker_files = [f"{args.output_jsonl}.worker{i}.jsonl" for i in range(workers)]
    for wf in worker_files:
        try:
            os.remove(wf)
        except FileNotFoundError:
            pass

    chrome_path = _resolve_browser_path(str(args.chrome_path or "").strip())
    if not chrome_path:
        raise RuntimeError(
            "Chrome/Chromium executable not found. "
            "Set --chrome-path or CHROME_PATH. "
            "Tried commands: google-chrome, google-chrome-stable, chromium-browser, chromium, chrome."
        )

    print(json.dumps({"resolved_chrome_path": chrome_path}, ensure_ascii=False), flush=True)
    print(json.dumps({"worker_profile_root": worker_profile_root}, ensure_ascii=False), flush=True)

    smoke_normal = _run_chrome_smoke(
        chrome_path=chrome_path,
        profile_root=worker_profile_root,
        no_sandbox=bool(int(args.no_sandbox)),
        single_process=False,
    )
    if smoke_normal.get("ok") != "1":
        smoke_single = _run_chrome_smoke(
            chrome_path=chrome_path,
            profile_root=worker_profile_root,
            no_sandbox=bool(int(args.no_sandbox)),
            single_process=True,
        )
        if smoke_single.get("ok") == "1":
            os.environ["PDF_BROWSER_SINGLE_PROCESS"] = "1"
            print(json.dumps({"chrome_smoke": "single-process-fallback-ok"}, ensure_ascii=False), flush=True)
        else:
            artifact_smoke = os.path.abspath(os.path.join(args.artifact_dir, "chrome_smoke_fail.json"))
            os.makedirs(args.artifact_dir, exist_ok=True)
            with open(artifact_smoke, "w", encoding="utf-8") as f:
                json.dump(
                    {"normal": smoke_normal, "single": smoke_single, "chrome_path": chrome_path},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            raise RuntimeError(f"chrome_smoke_failed: {artifact_smoke}")
    else:
        print(json.dumps({"chrome_smoke": "ok"}, ensure_ascii=False), flush=True)

    all_records = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = []
        for i, chunk in enumerate(chunks):
            futs.append(
                ex.submit(
                    _worker_run,
                    i,
                    chunk,
                    worker_files[i],
                    chrome_path,
                    worker_profile_root,
                    int(args.startup_retries),
                    float(args.timeout_sec),
                    int(args.progress_every),
                    bool(int(args.capture_fail_artifacts)),
                    bool(int(args.capture_success_artifacts)),
                    bool(int(args.capture_success_html)),
                    os.path.abspath(args.artifact_dir),
                )
            )
        for fut in as_completed(futs):
            res = fut.result()
            all_records.extend(res["records"])
            print(
                f"[landing_repro|worker={res['worker']}] done {res['done']} success={res['success']}",
                flush=True,
            )

    with open(args.output_jsonl, "w", encoding="utf-8") as out_f:
        for wf in worker_files:
            if not os.path.exists(wf):
                continue
            with open(wf, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    out_f.write(line)

    summary = _summarize(all_records)
    failure_artifact_zip = ""
    success_artifact_zip = ""
    if int(args.capture_fail_artifacts) == 1 and int(args.zip_fail_artifacts) == 1:
        failure_artifact_zip = _build_artifact_zip(
            records=all_records,
            artifact_dir=args.artifact_dir,
            zip_path=args.artifact_zip,
            target="fail",
        )
    if int(args.capture_success_artifacts) == 1 and int(args.zip_success_artifacts) == 1:
        success_artifact_zip = _build_artifact_zip(
            records=all_records,
            artifact_dir=args.artifact_dir,
            zip_path=args.success_artifact_zip,
            target="success",
        )

    success_artifact_count = sum(
        1 for r in all_records if (r.get("outcome") == OUT_SUCCESS_ACCESS and str(r.get("screenshot_path", "")).strip())
    )
    fail_artifact_count = sum(
        1 for r in all_records if (r.get("outcome") != OUT_SUCCESS_ACCESS and str(r.get("screenshot_path", "")).strip())
    )

    report = {
        "generated_at": int(time.time()),
        "input_csv": os.path.abspath(args.input),
        "workers": workers,
        "headless": bool(int(args.headless)),
        "no_sandbox": bool(int(args.no_sandbox)),
        "server_tuned": bool(int(args.server_tuned)),
        "single_process": os.environ.get("PDF_BROWSER_SINGLE_PROCESS", "0"),
        "humanized_browser": os.environ.get("PDF_BROWSER_HUMANIZED", "1"),
        "assume_institution_access": bool(int(args.assume_institution_access)),
        "capture_success_artifacts": bool(int(args.capture_success_artifacts)),
        "capture_success_html": bool(int(args.capture_success_html)),
        "startup_retries": int(args.startup_retries),
        "profile_mode": os.environ.get("PDF_BROWSER_PROFILE_MODE", ""),
        "profile_name": os.environ.get("PDF_BROWSER_PROFILE_NAME", ""),
        "persistent_profile_dir": os.environ.get("PDF_BROWSER_PERSISTENT_PROFILE_DIR", ""),
        "worker_profile_root": worker_profile_root,
        "timeout_sec": float(args.timeout_sec),
        "total_valid": len(dois),
        "criteria": "SUCCESS_ACCESS if no access issue and landing verification passes (non-challenge URL/domain/title/html)",
        "summary": summary,
        "output_jsonl": os.path.abspath(args.output_jsonl),
        "artifact_dir": os.path.abspath(args.artifact_dir) if int(args.capture_fail_artifacts) == 1 else "",
        "failure_artifact_zip": failure_artifact_zip,
        "success_artifact_zip": success_artifact_zip,
        "artifact_counts": {
            "success_screenshots": success_artifact_count,
            "fail_screenshots": fail_artifact_count,
        },
    }

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(
        json.dumps(
            {
                "total_valid": len(dois),
                "workers": workers,
                "success_ratio": summary["success_ratio"],
                "block_captcha_rate": summary["block_captcha_rate"],
                "report": os.path.abspath(args.report),
                "failure_artifact_zip": failure_artifact_zip,
                "success_artifact_zip": success_artifact_zip,
                "artifact_counts": {
                    "success_screenshots": success_artifact_count,
                    "fail_screenshots": fail_artifact_count,
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
