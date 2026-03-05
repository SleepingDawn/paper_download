import argparse
import copy
import csv
import glob
import json
import multiprocessing as mp
import os
import random
import re
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import median
from typing import Deque, Dict, List, Optional, Tuple
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

OUT_SUCCESS = "SUCCESS"
OUT_FAIL_CAPTCHA = "FAIL_CAPTCHA"
OUT_FAIL_BLOCK = "FAIL_BLOCK"
OUT_FAIL_HTTP_STATUS = "FAIL_HTTP_STATUS"
OUT_FAIL_TIMEOUT_NETWORK = "FAIL_TIMEOUT/NETWORK"
OUT_FAIL_REDIRECT_LOOP = "FAIL_REDIRECT_LOOP"
OUT_FAIL_OTHER = "FAIL_OTHER"
OUT_FAIL_INVALID_DOI = "FAIL_INVALID_DOI"
OUT_SKIP_DUPLICATE = "SKIP_DUPLICATE"
OUT_SKIP_CIRCUIT_OPEN = "SKIP_CIRCUIT_OPEN"

FAIL_OR_SKIP_OUTCOMES = {
    OUT_FAIL_CAPTCHA,
    OUT_FAIL_BLOCK,
    OUT_FAIL_HTTP_STATUS,
    OUT_FAIL_TIMEOUT_NETWORK,
    OUT_FAIL_REDIRECT_LOOP,
    OUT_FAIL_OTHER,
    OUT_FAIL_INVALID_DOI,
    OUT_SKIP_CIRCUIT_OPEN,
}

FAMILY_CLOUDFLARE = "Cloudflare"
FAMILY_RADWARE = "Radware"
FAMILY_ACCESS_DENIED = "AccessDenied"
FAMILY_RATE_LIMIT_429 = "RateLimit429"
FAMILY_HTTP_ERROR = "HttpError"
FAMILY_CAPTCHA_GENERIC = "CaptchaGeneric"
FAMILY_NETWORK = "Network"
FAMILY_UNKNOWN = "Unknown"
FAMILY_CIRCUIT = "CircuitBreaker"

CAPTCHA_KEYWORDS = [
    "turnstile",
    "hcaptcha",
    "recaptcha",
    "captcha",
    "are you human",
    "verify you are human",
    "robot check",
    "i am human",
    "challenge",
]

BLOCK_KEYWORDS = [
    "access denied",
    "request blocked",
    "forbidden",
    "too many requests",
    "bot detected",
    "security check",
    "attention required",
]

HEADER_KEYS = ["Server", "Via", "CF-RAY", "Retry-After", "Content-Type", "Content-Length"]

CONTENT_MARKERS = [
    "citation_title",
    "citation_author",
    "citation_journal_title",
    "citation_doi",
    "citation_abstract",
    "citation_keywords",
    "dc.title",
    "dc.creator",
    "dc.identifier",
    "dc.description",
    "dc.source",
    "prism.publicationname",
    "prism.issn",
    "article:published_time",
    "og:title",
    "og:description",
    "article-title",
    "article-title-main",
    "article metadata",
    "article info",
    "journal article",
    "abstract",
    "articlemeta",
    "publication-title",
    "journal-title",
    "sciencedirect",
    "ieeexplore",
    "springerlink",
    "article-details",
    "doi",
]

GENERIC_NON_CONTENT_TITLE_MARKERS = [
    "just a moment",
    "verify you are human",
    "access denied",
    "forbidden",
    "security check",
    "bot manager",
    "error",
]


@dataclass
class PolicyConfig:
    name: str
    max_workers: int
    timeout_sec: float
    resolve_prefetch_bytes: int
    final_prefetch_bytes: int
    defaults: Dict
    domain_policy: Dict
    resolve_strategy: str  # full_probe | redirect_only


class DomainTrafficPolicy:
    def __init__(self, cfg: PolicyConfig):
        self.cfg = cfg
        self._lock = threading.Lock()
        self._state: Dict[str, Dict] = {}

    def _rule(self, domain: str) -> Dict:
        rule = dict(self.cfg.defaults)
        domain_overrides = (self.cfg.domain_policy.get("domains") or {}).get(domain, {})
        for k, v in domain_overrides.items():
            if k == "circuit_breaker_threshold":
                merged = dict(rule.get("circuit_breaker_threshold", {}))
                merged.update(v)
                rule[k] = merged
            else:
                rule[k] = v
        return rule

    def _ensure_state(self, domain: str) -> Dict:
        st = self._state.get(domain)
        if st is None:
            st = {
                "inflight": 0,
                "next_allowed": 0.0,
                "attempts": 0,
                "recent_bad": deque(maxlen=50),
                "circuit_open_until": 0.0,
                "circuit_reason": "",
            }
            self._state[domain] = st
        return st

    def check_circuit(self, domain: str) -> Tuple[bool, str]:
        if not domain:
            return False, ""
        with self._lock:
            st = self._ensure_state(domain)
            rule = self._rule(domain)
            now = time.time()

            if now < st["circuit_open_until"]:
                left = int(st["circuit_open_until"] - now)
                return True, f"circuit_open({left}s):{st['circuit_reason']}"

            max_attempts = int(rule.get("max_attempts_per_domain_per_run", 10_000))
            if st["attempts"] >= max_attempts:
                return True, f"max_attempts_reached:{max_attempts}"

            return False, ""

    def acquire(self, domain: str) -> Tuple[bool, str]:
        if not domain:
            return True, ""

        while True:
            with self._lock:
                st = self._ensure_state(domain)
                rule = self._rule(domain)
                now = time.time()

                if now < st["circuit_open_until"]:
                    left = int(st["circuit_open_until"] - now)
                    return False, f"circuit_open({left}s):{st['circuit_reason']}"

                max_attempts = int(rule.get("max_attempts_per_domain_per_run", 10_000))
                if st["attempts"] >= max_attempts:
                    return False, f"max_attempts_reached:{max_attempts}"

                concurrency = int(rule.get("concurrency", 1))
                if st["inflight"] < concurrency and now >= st["next_allowed"]:
                    st["inflight"] += 1
                    st["attempts"] += 1
                    return True, ""

                sleep_for = max(0.01, st["next_allowed"] - now)
            time.sleep(min(0.2, sleep_for))

    def release(self, domain: str, outcome: str, status_code: Optional[int], retry_after: Optional[float]) -> None:
        if not domain:
            return

        with self._lock:
            st = self._ensure_state(domain)
            rule = self._rule(domain)
            st["inflight"] = max(0, st["inflight"] - 1)

            now = time.time()
            delay = float(rule.get("base_delay", 0.0)) + random.uniform(0.0, float(rule.get("jitter", 0.0)))

            if status_code == 429:
                delay = max(delay, retry_after if retry_after is not None else float(rule.get("cooldown_429", 2.0)))

            if outcome == OUT_FAIL_BLOCK:
                delay = max(delay, float(rule.get("cooldown_block", 0.5)))
            elif outcome == OUT_FAIL_CAPTCHA:
                delay = max(delay, float(rule.get("cooldown_captcha", 0.8)))

            st["next_allowed"] = max(st["next_allowed"], now + delay)

            bad = outcome in (OUT_FAIL_BLOCK, OUT_FAIL_CAPTCHA)
            st["recent_bad"].append(1 if bad else 0)

            cbt = rule.get("circuit_breaker_threshold", {})
            window = int(cbt.get("window", 8))
            min_samples = int(cbt.get("min_samples", 6))
            bad_ratio = float(cbt.get("bad_ratio", 0.8))
            open_for = float(cbt.get("open_for_sec", 1800))

            if len(st["recent_bad"]) >= min_samples:
                recent = list(st["recent_bad"])[-window:]
                if recent:
                    ratio = sum(recent) / len(recent)
                    if ratio >= bad_ratio:
                        st["circuit_open_until"] = max(st["circuit_open_until"], now + open_for)
                        st["circuit_reason"] = f"bad_ratio={ratio:.2f},window={len(recent)}"


def _merge_domain_rule(defaults: Dict, domain_policy: Dict, domain: str) -> Dict:
    rule = dict(defaults or {})
    domain_overrides = (domain_policy.get("domains") or {}).get(domain, {}) if isinstance(domain_policy, dict) else {}
    for k, v in domain_overrides.items():
        if k == "circuit_breaker_threshold":
            merged = dict(rule.get("circuit_breaker_threshold", {}))
            merged.update(v)
            rule[k] = merged
        else:
            rule[k] = v
    return rule


def _default_shared_domain_state() -> Dict:
    return {
        "inflight": 0,
        "next_allowed": 0.0,
        "attempts": 0,
        "recent_bad": [],
        "circuit_open_until": 0.0,
        "circuit_reason": "",
    }


def _shared_check_circuit(domain: str, defaults: Dict, domain_policy: Dict, shared_state, shared_lock) -> Tuple[bool, str]:
    if not domain:
        return False, ""

    with shared_lock:
        st = dict(shared_state.get(domain) or _default_shared_domain_state())
        rule = _merge_domain_rule(defaults, domain_policy, domain)
        now = time.time()

        if now < float(st.get("circuit_open_until", 0.0)):
            left = int(float(st.get("circuit_open_until", 0.0)) - now)
            return True, f"circuit_open({left}s):{st.get('circuit_reason', '')}"

        max_attempts = int(rule.get("max_attempts_per_domain_per_run", 10_000))
        if int(st.get("attempts", 0)) >= max_attempts:
            return True, f"max_attempts_reached:{max_attempts}"

        shared_state[domain] = st
        return False, ""


def _shared_acquire(domain: str, defaults: Dict, domain_policy: Dict, shared_state, shared_lock) -> Tuple[bool, str]:
    if not domain:
        return True, ""

    while True:
        with shared_lock:
            st = dict(shared_state.get(domain) or _default_shared_domain_state())
            rule = _merge_domain_rule(defaults, domain_policy, domain)
            now = time.time()

            circuit_open_until = float(st.get("circuit_open_until", 0.0))
            if now < circuit_open_until:
                left = int(circuit_open_until - now)
                return False, f"circuit_open({left}s):{st.get('circuit_reason', '')}"

            max_attempts = int(rule.get("max_attempts_per_domain_per_run", 10_000))
            if int(st.get("attempts", 0)) >= max_attempts:
                return False, f"max_attempts_reached:{max_attempts}"

            inflight = int(st.get("inflight", 0))
            next_allowed = float(st.get("next_allowed", 0.0))
            concurrency = int(rule.get("concurrency", 1))

            if inflight < concurrency and now >= next_allowed:
                st["inflight"] = inflight + 1
                st["attempts"] = int(st.get("attempts", 0)) + 1
                shared_state[domain] = st
                return True, ""

            sleep_for = max(0.01, next_allowed - now)

        time.sleep(min(0.2, sleep_for))


def _shared_release(
    domain: str,
    outcome: str,
    status_code: Optional[int],
    retry_after: Optional[float],
    defaults: Dict,
    domain_policy: Dict,
    shared_state,
    shared_lock,
) -> None:
    if not domain:
        return

    with shared_lock:
        st = dict(shared_state.get(domain) or _default_shared_domain_state())
        rule = _merge_domain_rule(defaults, domain_policy, domain)

        st["inflight"] = max(0, int(st.get("inflight", 0)) - 1)

        now = time.time()
        delay = float(rule.get("base_delay", 0.0)) + random.uniform(0.0, float(rule.get("jitter", 0.0)))

        if status_code == 429:
            delay = max(delay, retry_after if retry_after is not None else float(rule.get("cooldown_429", 2.0)))

        if outcome == OUT_FAIL_BLOCK:
            delay = max(delay, float(rule.get("cooldown_block", 0.5)))
        elif outcome == OUT_FAIL_CAPTCHA:
            delay = max(delay, float(rule.get("cooldown_captcha", 0.8)))

        st["next_allowed"] = max(float(st.get("next_allowed", 0.0)), now + delay)

        bad = outcome in (OUT_FAIL_BLOCK, OUT_FAIL_CAPTCHA)
        recent = list(st.get("recent_bad", []))
        recent.append(1 if bad else 0)
        if len(recent) > 50:
            recent = recent[-50:]
        st["recent_bad"] = recent

        cbt = rule.get("circuit_breaker_threshold", {})
        window = int(cbt.get("window", 8))
        min_samples = int(cbt.get("min_samples", 6))
        bad_ratio = float(cbt.get("bad_ratio", 0.8))
        open_for = float(cbt.get("open_for_sec", 1800))

        if len(recent) >= min_samples:
            recent_window = recent[-window:]
            if recent_window:
                ratio = sum(recent_window) / len(recent_window)
                if ratio >= bad_ratio:
                    st["circuit_open_until"] = max(float(st.get("circuit_open_until", 0.0)), now + open_for)
                    st["circuit_reason"] = f"bad_ratio={ratio:.2f},window={len(recent_window)}"

        shared_state[domain] = st


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    v = value.strip()
    return float(v) if v.isdigit() else None


def _safe_url_parts(url: str, max_len: int = 256) -> Tuple[str, str]:
    try:
        p = urllib_parse.urlparse(url)
        path = (p.path or "")[:max_len]
        query = (p.query or "")[:max_len]
        return path, query
    except Exception:
        return "", ""


def _extract_title(text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()[:300]


def _extract_meta_refresh_url(text: str, base_url: str) -> str:
    # e.g. <meta http-equiv="refresh" content="2; url='/path?...'">
    m = re.search(
        r"<meta[^>]*http-equiv\s*=\s*['\"]?\s*refresh\s*['\"]?[^>]*content\s*=\s*\"([^\"]+)\"",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r"<meta[^>]*http-equiv\s*=\s*['\"]?\s*refresh\s*['\"]?[^>]*content\s*=\s*'([^']+)'",
            text,
            flags=re.IGNORECASE,
        )
    if not m:
        return ""

    content = m.group(1)
    u = re.search(r"url\s*=\s*['\"]?([^'\";]+)", content, flags=re.IGNORECASE)
    if not u:
        return ""
    target = (u.group(1) or "").strip()
    if not target:
        return ""
    return urllib_parse.urljoin(base_url, target)


def _first_keyword(text: str, candidates: List[str]) -> str:
    low = text.lower()
    for k in candidates:
        if k in low:
            return k
    return ""


def _has_content_signal(title: str, snippet: str) -> Tuple[bool, str]:
    text = f"{title}\n{snippet}".lower()
    marker = _first_keyword(text, CONTENT_MARKERS)
    if marker:
        return True, marker

    t = (title or "").strip()
    low_t = t.lower()
    if t and len(t) >= 20 and not any(g in low_t for g in GENERIC_NON_CONTENT_TITLE_MARKERS):
        return True, "title-length"
    return False, "no-content-signal"


def _detect_signature(
    status_code: Optional[int],
    headers_subset: Dict,
    title: str,
    snippet: str,
    domain: str,
    require_content_signal: bool = True,
) -> Tuple[str, str, str, str]:
    text = f"{title}\n{snippet}".lower()
    server = str(headers_subset.get("server", "")).lower()
    via = str(headers_subset.get("via", "")).lower()
    cf_ray = str(headers_subset.get("cf-ray", "")).lower()
    cookie_names = ",".join(headers_subset.get("set-cookie-names", [])).lower()

    captcha_kw = _first_keyword(text, CAPTCHA_KEYWORDS)
    block_kw = _first_keyword(text, BLOCK_KEYWORDS)

    is_429 = status_code == 429
    is_cloudflare = (
        "cloudflare" in text
        or "just a moment" in text
        or bool(cf_ray)
        or "cloudflare" in server
        or "cloudflare" in via
    )
    is_radware = (
        "radware" in text
        or "perfdrive" in domain
        or "__uzdbm" in text
        or "__uzdbm" in cookie_names
        or "bot manager captcha" in text
    )
    is_access_denied = "access denied" in text

    if is_429:
        return OUT_FAIL_BLOCK, FAMILY_RATE_LIMIT_429, "429", "status=429"

    if is_radware:
        outcome = OUT_FAIL_CAPTCHA if captcha_kw or "captcha" in text else OUT_FAIL_BLOCK
        sig = f"radware|{captcha_kw or block_kw or 'bot-manager'}"
        return outcome, FAMILY_RADWARE, sig, captcha_kw or block_kw or "radware"

    if is_cloudflare:
        outcome = OUT_FAIL_CAPTCHA if (captcha_kw or "challenge" in text or "just a moment" in text) else OUT_FAIL_BLOCK
        sig = f"cloudflare|{captcha_kw or block_kw or 'just-a-moment'}"
        return outcome, FAMILY_CLOUDFLARE, sig, captcha_kw or block_kw or "cloudflare"

    if is_access_denied:
        return OUT_FAIL_BLOCK, FAMILY_ACCESS_DENIED, "access-denied", "access denied"

    if captcha_kw:
        return OUT_FAIL_CAPTCHA, FAMILY_CAPTCHA_GENERIC, f"captcha|{captcha_kw}", captcha_kw

    if status_code in (401, 403, 451):
        return OUT_FAIL_BLOCK, FAMILY_ACCESS_DENIED, f"http-{status_code}", f"status={status_code}"

    if status_code is not None and status_code >= 400:
        return OUT_FAIL_HTTP_STATUS, FAMILY_HTTP_ERROR, f"http-{status_code}", f"status={status_code}"

    if require_content_signal:
        ok, marker = _has_content_signal(title, snippet)
        if not ok:
            return OUT_FAIL_OTHER, FAMILY_UNKNOWN, "no-content-signal", marker

    return OUT_SUCCESS, "None", "ok", "ok"


def _extract_cookie_names(headers_obj, header_map: Dict[str, str]) -> List[str]:
    raw_list = []
    try:
        if headers_obj is not None and hasattr(headers_obj, "get_all"):
            raw_list = headers_obj.get_all("Set-Cookie") or []
    except Exception:
        raw_list = []

    if not raw_list:
        one = header_map.get("Set-Cookie") or header_map.get("set-cookie") or ""
        if one:
            raw_list = [one]

    names = []
    for line in raw_list:
        first = str(line).split(";", 1)[0]
        name = first.split("=", 1)[0].strip()
        if name:
            names.append(name)

    uniq = []
    seen = set()
    for n in names:
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq[:10]


def _headers_subset(headers_obj, header_map: Dict[str, str]) -> Dict:
    out = {}
    for k in HEADER_KEYS:
        v = header_map.get(k) or header_map.get(k.lower())
        if v:
            out[k.lower()] = str(v)[:256]
    out["set-cookie-names"] = _extract_cookie_names(headers_obj, header_map)
    return out


def _probe_url(
    url: str,
    timeout_sec: float,
    prefetch_bytes: int,
    user_agent: str,
    require_content_signal: bool = True,
    _meta_hops: int = 0,
    _visited: Optional[set] = None,
) -> Dict:
    redirect_chain = [url]
    if _visited is None:
        _visited = {url}
    else:
        _visited = set(_visited)
        _visited.add(url)

    class RedirectTracker(urllib_request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, hdrs, newurl):
            redirect_chain.append(newurl)
            if len(redirect_chain) > 20:
                raise urllib_error.URLError("too many redirects")
            return super().redirect_request(req, fp, code, msg, hdrs, newurl)

    opener = urllib_request.build_opener(RedirectTracker)
    req_headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }

    started = time.perf_counter()
    try:
        req = urllib_request.Request(url=url, headers=req_headers, method="GET")
        with opener.open(req, timeout=timeout_sec) as resp:
            status_code = resp.getcode()
            header_map = dict(resp.headers.items())
            headers_subset = _headers_subset(resp.headers, header_map)
            body = resp.read(prefetch_bytes)
            final_url = getattr(resp, "url", url)

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        snippet = body.decode("utf-8", errors="ignore")
        title = _extract_title(snippet)
        domain = (urllib_parse.urlparse(final_url).netloc or "").lower()

        if len(set(redirect_chain)) != len(redirect_chain):
            return {
                "status_code": status_code,
                "final_url": final_url,
                "redirect_chain": redirect_chain,
                "headers_subset": headers_subset,
                "title": title,
                "snippet": snippet,
                "elapsed_ms": elapsed_ms,
                "outcome": OUT_FAIL_REDIRECT_LOOP,
                "signature_family": FAMILY_NETWORK,
                "signature_key": "redirect-loop",
                "evidence": "redirect_loop_detected",
            }

        outcome, family, sig_key, evidence = _detect_signature(
            status_code,
            headers_subset,
            title,
            snippet[:256],
            domain,
            require_content_signal=require_content_signal,
        )

        # Some publisher landing pages (e.g., linkinghub) use HTML meta refresh.
        # Follow at most 2 extra hops in normal mode to reach the actual landing page.
        if (
            require_content_signal
            and outcome == OUT_FAIL_OTHER
            and sig_key == "no-content-signal"
            and _meta_hops < 2
        ):
            nxt = _extract_meta_refresh_url(snippet, final_url)
            if nxt and nxt not in _visited:
                child = _probe_url(
                    nxt,
                    timeout_sec=timeout_sec,
                    prefetch_bytes=prefetch_bytes,
                    user_agent=user_agent,
                    require_content_signal=require_content_signal,
                    _meta_hops=_meta_hops + 1,
                    _visited=_visited,
                )
                child_chain = child.get("redirect_chain", []) or []
                merged_chain = list(redirect_chain)
                if child_chain and child_chain[0] == nxt:
                    merged_chain.extend(child_chain)
                else:
                    merged_chain.append(nxt)
                    merged_chain.extend(child_chain)
                child["redirect_chain"] = merged_chain
                child["elapsed_ms"] = int(elapsed_ms + int(child.get("elapsed_ms", 0) or 0))
                return child

        return {
            "status_code": status_code,
            "final_url": final_url,
            "redirect_chain": redirect_chain,
            "headers_subset": headers_subset,
            "title": title,
            "snippet": snippet,
            "elapsed_ms": elapsed_ms,
            "outcome": outcome,
            "signature_family": family,
            "signature_key": sig_key,
            "evidence": evidence,
        }
    except urllib_error.HTTPError as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        raw = e.read(prefetch_bytes) if hasattr(e, "read") else b""
        snippet = raw.decode("utf-8", errors="ignore")
        status_code = getattr(e, "code", None)
        final_url = getattr(e, "url", url)
        header_map = dict(e.headers.items()) if getattr(e, "headers", None) else {}
        headers_subset = _headers_subset(getattr(e, "headers", None), header_map)
        title = _extract_title(snippet)
        domain = (urllib_parse.urlparse(final_url).netloc or "").lower()

        outcome, family, sig_key, evidence = _detect_signature(
            status_code,
            headers_subset,
            title,
            snippet[:256],
            domain,
            require_content_signal=require_content_signal,
        )
        return {
            "status_code": status_code,
            "final_url": final_url,
            "redirect_chain": redirect_chain + [final_url],
            "headers_subset": headers_subset,
            "title": title,
            "snippet": snippet,
            "elapsed_ms": elapsed_ms,
            "outcome": outcome,
            "signature_family": family,
            "signature_key": sig_key,
            "evidence": evidence,
        }
    except urllib_error.URLError as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        msg = str(e)
        low = msg.lower()
        out = OUT_FAIL_REDIRECT_LOOP if "redirect" in low or "infinite" in low else OUT_FAIL_TIMEOUT_NETWORK
        fam = FAMILY_NETWORK
        sig = "url-error-redirect" if out == OUT_FAIL_REDIRECT_LOOP else "url-error-network"
        return {
            "status_code": None,
            "final_url": url,
            "redirect_chain": redirect_chain,
            "headers_subset": {},
            "title": "",
            "snippet": "",
            "elapsed_ms": elapsed_ms,
            "outcome": out,
            "signature_family": fam,
            "signature_key": sig,
            "evidence": f"url_error={msg[:200]}",
        }
    except TimeoutError as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "status_code": None,
            "final_url": url,
            "redirect_chain": redirect_chain,
            "headers_subset": {},
            "title": "",
            "snippet": "",
            "elapsed_ms": elapsed_ms,
            "outcome": OUT_FAIL_TIMEOUT_NETWORK,
            "signature_family": FAMILY_NETWORK,
            "signature_key": "timeout",
            "evidence": f"timeout={str(e)[:200]}",
        }
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "status_code": None,
            "final_url": url,
            "redirect_chain": redirect_chain,
            "headers_subset": {},
            "title": "",
            "snippet": "",
            "elapsed_ms": elapsed_ms,
            "outcome": OUT_FAIL_TIMEOUT_NETWORK,
            "signature_family": FAMILY_NETWORK,
            "signature_key": "network-or-unknown",
            "evidence": f"network_or_unknown={str(e)[:200]}",
        }


def _resolve_redirect_only(doi_url: str, timeout_sec: float, user_agent: str, max_hops: int = 10) -> Dict:
    """
    doi.org 리졸브 전용: 리다이렉트 체인만 수집하고, 최종 타깃 본문은 읽지 않는다.
    """

    class NoRedirect(urllib_request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, hdrs, newurl):
            return None

    opener = urllib_request.build_opener(NoRedirect)
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }

    started = time.perf_counter()
    chain = [doi_url]
    current = doi_url
    last_status = None
    last_headers_subset = {}

    try:
        for _ in range(max_hops):
            req = urllib_request.Request(url=current, headers=headers, method="GET")
            try:
                with opener.open(req, timeout=timeout_sec) as resp:
                    status = resp.getcode()
                    hdrs = dict(resp.headers.items())
                    hsub = _headers_subset(resp.headers, hdrs)
                    final_url = getattr(resp, "url", current)
                    elapsed = int((time.perf_counter() - started) * 1000)
                    domain = (urllib_parse.urlparse(final_url).netloc or "").lower()

                    out, fam, sig, ev = _detect_signature(status, hsub, "", "", domain, require_content_signal=False)
                    if out in (OUT_FAIL_BLOCK, OUT_FAIL_CAPTCHA, OUT_FAIL_HTTP_STATUS):
                        return {
                            "status_code": status,
                            "final_url": final_url,
                            "redirect_chain": chain,
                            "headers_subset": hsub,
                            "title": "",
                            "snippet": "",
                            "elapsed_ms": elapsed,
                            "outcome": out,
                            "signature_family": fam,
                            "signature_key": sig,
                            "evidence": ev,
                        }

                    return {
                        "status_code": status,
                        "final_url": final_url,
                        "redirect_chain": chain,
                        "headers_subset": hsub,
                        "title": "",
                        "snippet": "",
                        "elapsed_ms": elapsed,
                        "outcome": OUT_SUCCESS,
                        "signature_family": "None",
                        "signature_key": "resolve-ok",
                        "evidence": "resolve_ok",
                    }
            except urllib_error.HTTPError as e:
                status = getattr(e, "code", None)
                hdrs = dict(e.headers.items()) if getattr(e, "headers", None) else {}
                hsub = _headers_subset(getattr(e, "headers", None), hdrs)
                location = hdrs.get("Location") or hdrs.get("location")
                last_status = status
                last_headers_subset = hsub

                if status in (301, 302, 303, 307, 308) and location:
                    nxt = urllib_parse.urljoin(current, location)
                    chain.append(nxt)
                    if len(chain) != len(set(chain)):
                        elapsed = int((time.perf_counter() - started) * 1000)
                        return {
                            "status_code": status,
                            "final_url": current,
                            "redirect_chain": chain,
                            "headers_subset": hsub,
                            "title": "",
                            "snippet": "",
                            "elapsed_ms": elapsed,
                            "outcome": OUT_FAIL_REDIRECT_LOOP,
                            "signature_family": FAMILY_NETWORK,
                            "signature_key": "redirect-loop",
                            "evidence": "redirect_loop_detected",
                        }
                    nxt_domain = (urllib_parse.urlparse(nxt).netloc or "").lower()
                    curr_domain = (urllib_parse.urlparse(current).netloc or "").lower()
                    # doi.org -> 외부 도메인으로 넘어가는 첫 Location에서 리졸브 종료.
                    # 외부 도메인 본문 요청은 이후 정책(서킷브레이커) 적용 후 수행한다.
                    if curr_domain.endswith("doi.org") and not nxt_domain.endswith("doi.org"):
                        elapsed = int((time.perf_counter() - started) * 1000)
                        return {
                            "status_code": status,
                            "final_url": nxt,
                            "redirect_chain": chain,
                            "headers_subset": hsub,
                            "title": "",
                            "snippet": "",
                            "elapsed_ms": elapsed,
                            "outcome": OUT_SUCCESS,
                            "signature_family": "None",
                            "signature_key": "resolved-location",
                            "evidence": "resolved_via_location",
                        }
                    current = nxt
                    continue

                elapsed = int((time.perf_counter() - started) * 1000)
                domain = (urllib_parse.urlparse(current).netloc or "").lower()
                out, fam, sig, ev = _detect_signature(status, hsub, "", "", domain, require_content_signal=False)
                return {
                    "status_code": status,
                    "final_url": current,
                    "redirect_chain": chain,
                    "headers_subset": hsub,
                    "title": "",
                    "snippet": "",
                    "elapsed_ms": elapsed,
                    "outcome": out,
                    "signature_family": fam,
                    "signature_key": sig,
                    "evidence": ev,
                }

        elapsed = int((time.perf_counter() - started) * 1000)
        return {
            "status_code": last_status,
            "final_url": current,
            "redirect_chain": chain,
            "headers_subset": last_headers_subset,
            "title": "",
            "snippet": "",
            "elapsed_ms": elapsed,
            "outcome": OUT_FAIL_REDIRECT_LOOP,
            "signature_family": FAMILY_NETWORK,
            "signature_key": "redirect-hop-limit",
            "evidence": "redirect_hop_limit",
        }
    except urllib_error.URLError as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        msg = str(e)
        low = msg.lower()
        out = OUT_FAIL_REDIRECT_LOOP if "redirect" in low or "infinite" in low else OUT_FAIL_TIMEOUT_NETWORK
        return {
            "status_code": None,
            "final_url": current,
            "redirect_chain": chain,
            "headers_subset": {},
            "title": "",
            "snippet": "",
            "elapsed_ms": elapsed,
            "outcome": out,
            "signature_family": FAMILY_NETWORK,
            "signature_key": "resolve-url-error",
            "evidence": f"url_error={msg[:200]}",
        }
    except Exception as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        return {
            "status_code": None,
            "final_url": current,
            "redirect_chain": chain,
            "headers_subset": {},
            "title": "",
            "snippet": "",
            "elapsed_ms": elapsed,
            "outcome": OUT_FAIL_TIMEOUT_NETWORK,
            "signature_family": FAMILY_NETWORK,
            "signature_key": "resolve-network-or-unknown",
            "evidence": f"network_or_unknown={str(e)[:200]}",
        }


def _valid_doi(doi: str) -> bool:
    return bool(re.match(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$", doi))


def load_dois(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path) and csv_path.endswith("ready_to_downlaod.csv"):
        alt = csv_path.replace("ready_to_downlaod.csv", "ready_to_download.csv")
        if os.path.exists(alt):
            csv_path = alt

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    seen = set()
    dois = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        doi_col = None
        for c in (reader.fieldnames or []):
            if c and c.strip().lower() == "doi":
                doi_col = c
                break
        if doi_col is None:
            raise ValueError("CSV에 DOI 컬럼이 없습니다.")

        for row in reader:
            raw = str(row.get(doi_col, "")).strip()
            if not raw:
                continue
            doi = raw.replace("https://doi.org/", "").replace("http://doi.org/", "").strip().lower()
            if doi in seen:
                continue
            if not _valid_doi(doi):
                continue
            seen.add(doi)
            dois.append(doi)

    return dois


def _p50_p90(values: List[int]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    arr = sorted(values)
    p50 = float(median(arr))
    p90_idx = min(len(arr) - 1, int(len(arr) * 0.9))
    return p50, float(arr[p90_idx])


def _summarize(records: List[Dict]) -> Dict:
    total = len(records)
    if total == 0:
        return {
            "total": 0,
            "block_captcha_rate": 0.0,
            "p50_elapsed_ms": 0.0,
            "p90_elapsed_ms": 0.0,
            "outcome_counts": {},
            "by_domain": {},
        }

    outcome_counts = Counter(r["outcome"] for r in records)
    elapsed_all = [int(r.get("elapsed_ms", 0)) for r in records]

    by_domain: Dict[str, Dict] = defaultdict(lambda: {
        "total": 0,
        "outcome_counts": Counter(),
        "family_counts": Counter(),
        "elapsed_ms": [],
        "signature_counts": Counter(),
    })

    for r in records:
        d = r.get("domain") or ""
        m = by_domain[d]
        m["total"] += 1
        m["outcome_counts"][r["outcome"]] += 1
        m["family_counts"][r.get("signature_family") or FAMILY_UNKNOWN] += 1
        m["signature_counts"][r.get("signature_key") or ""] += 1
        m["elapsed_ms"].append(int(r.get("elapsed_ms", 0)))

    by_domain_out = {}
    for d, m in by_domain.items():
        bad = m["outcome_counts"].get(OUT_FAIL_CAPTCHA, 0) + m["outcome_counts"].get(OUT_FAIL_BLOCK, 0)
        p50, p90 = _p50_p90(m["elapsed_ms"])
        by_domain_out[d] = {
            "total": m["total"],
            "outcome_counts": dict(m["outcome_counts"]),
            "family_counts": dict(m["family_counts"]),
            "top_signatures": m["signature_counts"].most_common(5),
            "block_captcha_rate": round(bad / m["total"], 4) if m["total"] else 0.0,
            "p50_elapsed_ms": p50,
            "p90_elapsed_ms": p90,
        }

    bad_total = outcome_counts.get(OUT_FAIL_CAPTCHA, 0) + outcome_counts.get(OUT_FAIL_BLOCK, 0)
    p50_all, p90_all = _p50_p90(elapsed_all)
    return {
        "total": total,
        "block_captcha_rate": round(bad_total / total, 4),
        "p50_elapsed_ms": p50_all,
        "p90_elapsed_ms": p90_all,
        "outcome_counts": dict(outcome_counts),
        "by_domain": by_domain_out,
    }


def _summarize_parallel(records: List[Dict]) -> Dict:
    valid_records = [r for r in records if r.get("outcome") != OUT_FAIL_INVALID_DOI]
    total_valid = len(valid_records)
    if total_valid == 0:
        return {
            "total_valid": 0,
            "success_rate": 0.0,
            "block_rate": 0.0,
            "captcha_rate": 0.0,
            "block_captcha_rate": 0.0,
            "p50_elapsed_ms": 0.0,
            "p90_elapsed_ms": 0.0,
            "outcome_counts": {},
            "domain_top20": [],
        }

    outcome_counts = Counter(r.get("outcome") for r in valid_records)
    success = outcome_counts.get(OUT_SUCCESS, 0)
    block = outcome_counts.get(OUT_FAIL_BLOCK, 0)
    captcha = outcome_counts.get(OUT_FAIL_CAPTCHA, 0)
    timeouts = outcome_counts.get(OUT_FAIL_TIMEOUT_NETWORK, 0)
    p50, p90 = _p50_p90([int(r.get("elapsed_ms", 0)) for r in valid_records])

    by_domain: Dict[str, Dict] = defaultdict(lambda: {"total": 0, "success": 0, "block": 0, "captcha": 0, "timeout": 0, "elapsed": []})
    for r in valid_records:
        d = r.get("domain") or ""
        m = by_domain[d]
        m["total"] += 1
        outcome = r.get("outcome")
        if outcome == OUT_SUCCESS:
            m["success"] += 1
        elif outcome == OUT_FAIL_BLOCK:
            m["block"] += 1
        elif outcome == OUT_FAIL_CAPTCHA:
            m["captcha"] += 1
        elif outcome == OUT_FAIL_TIMEOUT_NETWORK:
            m["timeout"] += 1
        m["elapsed"].append(int(r.get("elapsed_ms", 0)))

    domain_rows = []
    for d, m in by_domain.items():
        dp50, dp90 = _p50_p90(m["elapsed"])
        domain_rows.append(
            {
                "domain": d,
                "total": m["total"],
                "success": m["success"],
                "block": m["block"],
                "captcha": m["captcha"],
                "timeout": m["timeout"],
                "success_rate": round(m["success"] / m["total"], 4) if m["total"] else 0.0,
                "block_captcha_rate": round((m["block"] + m["captcha"]) / m["total"], 4) if m["total"] else 0.0,
                "p50_elapsed_ms": dp50,
                "p90_elapsed_ms": dp90,
            }
        )
    domain_rows.sort(key=lambda x: x["total"], reverse=True)

    fail_other = max(0, total_valid - success - block - captcha)
    return {
        "total_valid": total_valid,
        "success_ratio": round(success / total_valid, 4),
        "success_rate": round(success / total_valid, 4),
        "block_rate": round(block / total_valid, 4),
        "captcha_rate": round(captcha / total_valid, 4),
        "block_captcha_rate": round((block + captcha) / total_valid, 4),
        "other_fail_rate": round(fail_other / total_valid, 4),
        "timeout_rate": round(timeouts / total_valid, 4),
        "p50_elapsed_ms": p50,
        "p90_elapsed_ms": p90,
        "outcome_counts": dict(outcome_counts),
        "fail_counts_by_reason": {
            "FAIL_BLOCK": int(block),
            "FAIL_CAPTCHA": int(captcha),
            "FAIL_OTHER": int(fail_other),
        },
        "domain_top20": domain_rows[:20],
    }


def _domain_breakdown_from_records(records: List[Dict]) -> List[Dict]:
    valid = [r for r in records if r.get("outcome") != OUT_FAIL_INVALID_DOI]
    by_domain: Dict[str, Dict] = defaultdict(
        lambda: {"n": 0, "success": 0, "block": 0, "captcha": 0, "other": 0, "timeout": 0, "elapsed": [], "sig": Counter()}
    )

    for r in valid:
        d = r.get("domain") or ""
        m = by_domain[d]
        m["n"] += 1
        out = r.get("outcome")
        if out == OUT_SUCCESS:
            m["success"] += 1
        elif out == OUT_FAIL_BLOCK:
            m["block"] += 1
        elif out == OUT_FAIL_CAPTCHA:
            m["captcha"] += 1
        else:
            m["other"] += 1
        if out == OUT_FAIL_TIMEOUT_NETWORK:
            m["timeout"] += 1
        m["elapsed"].append(int(r.get("elapsed_ms", 0)))
        sig = r.get("signature_key") or r.get("classification_evidence") or ""
        if sig:
            m["sig"][sig] += 1

    rows = []
    for d, m in by_domain.items():
        p50, p90 = _p50_p90(m["elapsed"])
        n = m["n"] or 1
        rows.append(
            {
                "domain": d,
                "n": m["n"],
                "success_rate": round(m["success"] / n, 4),
                "block_rate": round(m["block"] / n, 4),
                "captcha_rate": round(m["captcha"] / n, 4),
                "other_fail_rate": round(m["other"] / n, 4),
                "p50_elapsed_ms": p50,
                "p90_elapsed_ms": p90,
                "block_captcha_count": int(m["block"] + m["captcha"]),
                "top_evidence_signatures": m["sig"].most_common(5),
            }
        )
    rows.sort(key=lambda x: (x["block_captcha_count"], x["n"]), reverse=True)
    return rows


def _top_domains_by_failures(domain_rows: List[Dict], topn: int = 20) -> List[Dict]:
    return [
        {
            "domain": r["domain"],
            "n": r["n"],
            "success_rate": r["success_rate"],
            "block_rate": r["block_rate"],
            "captcha_rate": r["captcha_rate"],
            "other_fail_rate": r["other_fail_rate"],
            "block_captcha_count": r["block_captcha_count"],
            "top_evidence_signatures": r.get("top_evidence_signatures", []),
        }
        for r in domain_rows[:topn]
    ]


def _merge_worker_logs(worker_paths: List[str], merged_path: str) -> None:
    os.makedirs(os.path.dirname(merged_path), exist_ok=True)
    with open(merged_path, "w", encoding="utf-8") as out_f:
        for p in sorted(set(worker_paths)):
            if not os.path.exists(p):
                continue
            with open(p, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    out_f.write(line)


def _print_overall_markdown_table(parallel_report: Dict) -> None:
    rs = parallel_report["run_sequential"]
    rp = parallel_report["run_parallel"]
    print("| mode | total_valid | success_ratio | block_captcha_rate | p50_elapsed_ms | p90_elapsed_ms |")
    print("|---|---:|---:|---:|---:|---:|")
    print(
        f"| sequential(w=1) | {rs['total_valid']} | {rs['success_ratio']:.4f} | {rs['block_captcha_rate']:.4f} | {rs['p50_elapsed_ms']:.1f} | {rs['p90_elapsed_ms']:.1f} |"
    )
    print(
        f"| parallel(w=4) | {rp['total_valid']} | {rp['success_ratio']:.4f} | {rp['block_captcha_rate']:.4f} | {rp['p50_elapsed_ms']:.1f} | {rp['p90_elapsed_ms']:.1f} |"
    )


def _print_domain_markdown_table(seq_rows: List[Dict], par_rows: List[Dict], topn: int = 20) -> None:
    seq_map = {r["domain"]: r for r in seq_rows}
    par_map = {r["domain"]: r for r in par_rows}
    domains = set(seq_map.keys()) | set(par_map.keys())
    rank = []
    for d in domains:
        s = seq_map.get(d, {})
        p = par_map.get(d, {})
        s_bad = float(s.get("block_rate", 0.0)) + float(s.get("captcha_rate", 0.0))
        p_bad = float(p.get("block_rate", 0.0)) + float(p.get("captcha_rate", 0.0))
        score = max(s_bad, p_bad) * max(int(s.get("n", 0)), int(p.get("n", 0)))
        rank.append((score, d))
    rank.sort(reverse=True)

    print("| domain | n_seq | block_rate_seq | captcha_rate_seq | other_fail_rate_seq | n_par | block_rate_par | captcha_rate_par | other_fail_rate_par |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for _, d in rank[:topn]:
        s = seq_map.get(d, {})
        p = par_map.get(d, {})
        print(
            f"| {d} | {int(s.get('n', 0))} | {float(s.get('block_rate', 0.0)):.4f} | {float(s.get('captcha_rate', 0.0)):.4f} | {float(s.get('other_fail_rate', 0.0)):.4f} | "
            f"{int(p.get('n', 0))} | {float(p.get('block_rate', 0.0)):.4f} | {float(p.get('captcha_rate', 0.0)):.4f} | {float(p.get('other_fail_rate', 0.0)):.4f} |"
        )


def _tune_policy_for_low_block_captcha(current_policy: Dict, seq_records: List[Dict]) -> Tuple[Dict, Dict]:
    tuned = copy.deepcopy(current_policy if isinstance(current_policy, dict) else {})
    if "default" not in tuned:
        tuned["default"] = _default_policy_defaults()
    if "domains" not in tuned:
        tuned["domains"] = {}

    default_rule = tuned["default"]
    default_rule["concurrency"] = 1
    default_rule["base_delay"] = max(float(default_rule.get("base_delay", 0.0)), 0.08)
    default_rule["jitter"] = max(float(default_rule.get("jitter", 0.0)), 0.06)
    default_rule["cooldown_block"] = max(float(default_rule.get("cooldown_block", 0.2)), 1.0)
    default_rule["cooldown_captcha"] = max(float(default_rule.get("cooldown_captcha", 0.2)), 1.2)
    default_rule["cooldown_429"] = max(float(default_rule.get("cooldown_429", 1.0)), 4.0)
    cbt = dict(default_rule.get("circuit_breaker_threshold", {}))
    cbt["window"] = min(int(cbt.get("window", 8)), 4)
    cbt["min_samples"] = min(int(cbt.get("min_samples", 6)), 3)
    cbt["bad_ratio"] = min(float(cbt.get("bad_ratio", 0.8)), 0.67)
    cbt["open_for_sec"] = max(float(cbt.get("open_for_sec", 1800)), 7200.0)
    default_rule["circuit_breaker_threshold"] = cbt

    by_domain_fail = defaultdict(lambda: {"total": 0, "bad": 0})
    for r in seq_records:
        out = r.get("outcome")
        if out == OUT_FAIL_INVALID_DOI:
            continue
        d = r.get("domain") or ""
        by_domain_fail[d]["total"] += 1
        if out in (OUT_FAIL_BLOCK, OUT_FAIL_CAPTCHA):
            by_domain_fail[d]["bad"] += 1

    tuned_domains = 0
    for d, m in by_domain_fail.items():
        if m["bad"] <= 0:
            continue
        rule = dict((tuned.get("domains") or {}).get(d, {}))
        rule["concurrency"] = 1
        rule["base_delay"] = max(float(rule.get("base_delay", default_rule["base_delay"])), 0.12)
        rule["jitter"] = max(float(rule.get("jitter", default_rule["jitter"])), 0.08)
        rule["cooldown_block"] = max(float(rule.get("cooldown_block", default_rule["cooldown_block"])), 1.5)
        rule["cooldown_captcha"] = max(float(rule.get("cooldown_captcha", default_rule["cooldown_captcha"])), 2.0)
        rule["cooldown_429"] = max(float(rule.get("cooldown_429", default_rule["cooldown_429"])), 8.0)
        existing_max_attempts = int(rule.get("max_attempts_per_domain_per_run", 9999))
        rule["max_attempts_per_domain_per_run"] = min(existing_max_attempts, 2)
        rcbt = dict(rule.get("circuit_breaker_threshold", {}))
        rcbt["window"] = 3
        rcbt["min_samples"] = 2
        rcbt["bad_ratio"] = 0.5
        rcbt["open_for_sec"] = max(float(rcbt.get("open_for_sec", 1800)), 12_000.0)
        rule["circuit_breaker_threshold"] = rcbt
        rule["reason"] = "tuned_for_low_block_captcha_rate"
        tuned["domains"][d] = rule
        tuned_domains += 1

    note = {
        "tuned_domains": tuned_domains,
        "default_rule": tuned["default"],
    }
    return tuned, note


def _build_rootcause_for_run(records: List[Dict]) -> Dict:
    domain_stats = _summarize(records)["by_domain"]

    family_domains: Dict[str, Counter] = defaultdict(Counter)
    family_signatures: Dict[str, Counter] = defaultdict(Counter)
    family_status: Dict[str, Counter] = defaultdict(Counter)

    for r in records:
        out = r.get("outcome")
        if out not in (OUT_FAIL_CAPTCHA, OUT_FAIL_BLOCK):
            continue
        fam = r.get("signature_family") or FAMILY_UNKNOWN
        dom = r.get("domain") or ""
        family_domains[fam][dom] += 1
        family_signatures[fam][r.get("signature_key") or ""] += 1
        family_status[fam][str(r.get("status_code"))] += 1

    families = {}
    for fam in sorted(family_domains.keys()):
        families[fam] = {
            "total": int(sum(family_domains[fam].values())),
            "domains": family_domains[fam].most_common(20),
            "top_signatures": family_signatures[fam].most_common(10),
            "status_codes": family_status[fam].most_common(10),
        }

    return {
        "domain_stats": domain_stats,
        "families": families,
    }


def _generate_domain_policy_v2(rootcause_baseline: Dict, out_path: str) -> Dict:
    defaults = {
        "concurrency": 1,
        "base_delay": 0.05,
        "jitter": 0.05,
        "cooldown_block": 0.6,
        "cooldown_captcha": 0.8,
        "cooldown_429": 2.0,
        "max_attempts_per_domain_per_run": 9999,
        "circuit_breaker_threshold": {
            "window": 10,
            "min_samples": 8,
            "bad_ratio": 0.85,
            "open_for_sec": 1200,
        },
    }

    domains_cfg = {}
    for domain, ds in rootcause_baseline.get("domain_stats", {}).items():
        total = int(ds.get("total", 0))
        rate = float(ds.get("block_captcha_rate", 0.0))
        fam_counts = ds.get("family_counts", {})
        top_family = None
        top_count = 0
        for f, c in fam_counts.items():
            if c > top_count:
                top_family = f
                top_count = c

        if total < 6:
            continue

        rule = {}
        reason = ""

        if top_family in (FAMILY_CLOUDFLARE, FAMILY_RADWARE, FAMILY_ACCESS_DENIED) and rate >= 0.8:
            # 강한 차단 계열은 느리게 계속 때리는 대신 빨리 중단해 노출률 감소
            rule = {
                "concurrency": 1,
                "base_delay": 0.08,
                "jitter": 0.08,
                "cooldown_block": 1.0,
                "cooldown_captcha": 1.2,
                "cooldown_429": 3.0,
                "max_attempts_per_domain_per_run": max(4, min(12, int(total * 0.08))),
                "circuit_breaker_threshold": {
                    "window": 6,
                    "min_samples": 4,
                    "bad_ratio": 0.67,
                    "open_for_sec": 3600,
                },
            }
            reason = f"high bad rate ({rate:.2f}) on {top_family}"
        elif top_family == FAMILY_RATE_LIMIT_429 and rate >= 0.4:
            rule = {
                "concurrency": 1,
                "base_delay": 0.1,
                "jitter": 0.1,
                "cooldown_block": 1.2,
                "cooldown_captcha": 1.2,
                "cooldown_429": 8.0,
                "max_attempts_per_domain_per_run": max(10, min(40, int(total * 0.5))),
                "circuit_breaker_threshold": {
                    "window": 10,
                    "min_samples": 6,
                    "bad_ratio": 0.75,
                    "open_for_sec": 1800,
                },
            }
            reason = f"rate-limit dominant ({rate:.2f})"
        elif rate >= 0.6:
            rule = {
                "concurrency": 1,
                "base_delay": 0.08,
                "jitter": 0.06,
                "cooldown_block": 0.9,
                "cooldown_captcha": 1.0,
                "cooldown_429": 3.0,
                "max_attempts_per_domain_per_run": max(12, min(60, int(total * 0.4))),
                "circuit_breaker_threshold": {
                    "window": 10,
                    "min_samples": 6,
                    "bad_ratio": 0.8,
                    "open_for_sec": 1800,
                },
            }
            reason = f"elevated bad rate ({rate:.2f})"

        if rule:
            rule["reason"] = reason
            domains_cfg[domain] = rule

    payload = {
        "version": "v2",
        "generated_at": int(time.time()),
        "default": defaults,
        "domains": domains_cfg,
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def _gate_v2(baseline: Dict, improved: Dict) -> Dict:
    b_rate = float(baseline.get("block_captcha_rate", 0.0))
    i_rate = float(improved.get("block_captcha_rate", 0.0))
    b_p90 = float(baseline.get("p90_elapsed_ms", 0.0))
    i_p90 = float(improved.get("p90_elapsed_ms", 0.0))

    rel_reduction = (b_rate - i_rate) / b_rate if b_rate > 0 else 0.0
    cond_a = rel_reduction >= 0.10
    cond_b = (i_rate <= b_rate) and (i_p90 <= b_p90)
    passed = cond_a or cond_b

    return {
        "relative_reduction_block_captcha": round(rel_reduction, 4),
        "A_block_captcha_rate_reduced_10pct": cond_a,
        "B_non_worse_rate_and_non_worse_p90": cond_b,
        "passed": passed,
    }


def _parallel_gate(single_summary: Dict, parallel_summary: Dict) -> Dict:
    s_succ = float(single_summary.get("success_rate", 0.0))
    p_succ = float(parallel_summary.get("success_rate", 0.0))
    s_bad = float(single_summary.get("block_captcha_rate", 0.0))
    p_bad = float(parallel_summary.get("block_captcha_rate", 0.0))
    delta_bad = p_bad - s_bad

    cond_1 = s_succ >= 0.95
    cond_2 = p_succ >= 0.95
    cond_3 = delta_bad <= 0.01
    passed = cond_1 and cond_2 and cond_3

    return {
        "single_success_rate_ge_95pct": cond_1,
        "parallel_success_rate_ge_95pct": cond_2,
        "parallel_block_captcha_not_worse_than_1pp": cond_3,
        "single_success_rate": round(s_succ, 4),
        "parallel_success_rate": round(p_succ, 4),
        "single_block_captcha_rate": round(s_bad, 4),
        "parallel_block_captcha_rate": round(p_bad, 4),
        "delta_block_captcha_rate": round(delta_bad, 4),
        "passed": passed,
    }


def _build_audit_record(
    doi: str,
    started: float,
    probe: Dict,
    run_policy: str,
    run_id: str,
    evidence_max_bytes: int,
    forced_outcome: Optional[str] = None,
    forced_family: Optional[str] = None,
    forced_sig: str = "",
) -> Dict:
    final_url = probe.get("final_url") or ""
    domain = (urllib_parse.urlparse(final_url).netloc or "").lower()
    path, query = _safe_url_parts(final_url)
    snippet = (probe.get("title", "") + "\n" + probe.get("snippet", ""))[:evidence_max_bytes]

    return {
        "run_id": run_id,
        "policy": run_policy,
        "timestamp_ms": int(time.time() * 1000),
        "doi": doi,
        "resolved_url": final_url,
        "domain": domain,
        "status_code": probe.get("status_code"),
        "redirect_chain": probe.get("redirect_chain", []),
        "redirect_count": len(probe.get("redirect_chain", [])),
        "headers_subset": probe.get("headers_subset", {}),
        "content_type": (probe.get("headers_subset", {}) or {}).get("content-type", ""),
        "content_length": (probe.get("headers_subset", {}) or {}).get("content-length", ""),
        "final_url_path": path,
        "final_url_query": query,
        "evidence_snippet": snippet,
        "outcome": forced_outcome or probe.get("outcome") or OUT_FAIL_TIMEOUT_NETWORK,
        "signature_family": forced_family or probe.get("signature_family") or FAMILY_UNKNOWN,
        "signature_key": forced_sig or probe.get("signature_key") or "",
        "classification_evidence": probe.get("evidence") or "",
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
    }


def _process_one_multiprocess(
    doi: str,
    cfg_payload: Dict,
    shared_state,
    shared_lock,
    shared_resolve_cache,
    shared_final_cache,
    shared_cache_lock,
    evidence_max_bytes: int,
    run_id: str,
) -> Dict:
    try:
        worker_pid = os.getpid()

        def _finalize(rec: Dict) -> Dict:
            rec["worker_pid"] = worker_pid
            return rec

        if not _valid_doi(doi):
            return _finalize({
                "run_id": run_id,
                "policy": cfg_payload["name"],
                "timestamp_ms": int(time.time() * 1000),
                "doi": doi,
                "resolved_url": "",
                "domain": "",
                "status_code": None,
                "redirect_chain": [],
                "redirect_count": 0,
                "headers_subset": {},
                "content_type": "",
                "content_length": "",
                "final_url_path": "",
                "final_url_query": "",
                "evidence_snippet": "invalid_doi",
                "outcome": OUT_FAIL_INVALID_DOI,
                "signature_family": FAMILY_UNKNOWN,
                "signature_key": "invalid_doi",
                "classification_evidence": "invalid_doi",
                "elapsed_ms": 0,
            })

        started = time.perf_counter()
        doi_url = f"https://doi.org/{doi}"
        user_agent = cfg_payload["user_agent"]

        with shared_cache_lock:
            r1_cached = shared_resolve_cache.get(doi)

        if r1_cached is None:
            if cfg_payload.get("resolve_strategy") == "redirect_only":
                r1 = _resolve_redirect_only(doi_url, cfg_payload["timeout_sec"], user_agent)
            else:
                r1 = _probe_url(
                    doi_url,
                    cfg_payload["timeout_sec"],
                    cfg_payload["resolve_prefetch_bytes"],
                    user_agent,
                    require_content_signal=False,
                )
            with shared_cache_lock:
                shared_resolve_cache[doi] = r1
        else:
            r1 = dict(r1_cached)

        if r1.get("outcome") != OUT_SUCCESS:
            return _finalize(_build_audit_record(doi, started, r1, cfg_payload["name"], run_id, evidence_max_bytes))

        resolved_url = r1.get("final_url") or doi_url
        domain = (urllib_parse.urlparse(resolved_url).netloc or "").lower()
        final_key = f"{domain}|{resolved_url}"

        with shared_cache_lock:
            r2_cached = shared_final_cache.get(final_key)
        if r2_cached is not None:
            r2 = dict(r2_cached)
            rec = _build_audit_record(doi, started, r2, cfg_payload["name"], run_id, evidence_max_bytes)
            rec["redirect_chain"] = (r1.get("redirect_chain", []) or []) + (r2.get("redirect_chain", []) or [])
            rec["redirect_count"] = len(rec["redirect_chain"])
            return _finalize(rec)

        is_open, reason = _shared_check_circuit(
            domain=domain,
            defaults=cfg_payload["defaults"],
            domain_policy=cfg_payload["domain_policy"],
            shared_state=shared_state,
            shared_lock=shared_lock,
        )
        if is_open:
            probe_stub = {
                "final_url": resolved_url,
                "redirect_chain": r1.get("redirect_chain", []),
                "headers_subset": r1.get("headers_subset", {}),
                "status_code": r1.get("status_code"),
                "title": "",
                "snippet": "",
                "evidence": reason,
            }
            rec = _build_audit_record(
                doi,
                started,
                probe_stub,
                cfg_payload["name"],
                run_id,
                evidence_max_bytes,
                forced_outcome=OUT_SKIP_CIRCUIT_OPEN,
                forced_family=FAMILY_CIRCUIT,
                forced_sig=reason,
            )
            return _finalize(rec)

        ok, reason = _shared_acquire(
            domain=domain,
            defaults=cfg_payload["defaults"],
            domain_policy=cfg_payload["domain_policy"],
            shared_state=shared_state,
            shared_lock=shared_lock,
        )
        if not ok:
            probe_stub = {
                "final_url": resolved_url,
                "redirect_chain": r1.get("redirect_chain", []),
                "headers_subset": r1.get("headers_subset", {}),
                "status_code": r1.get("status_code"),
                "title": "",
                "snippet": "",
                "evidence": reason,
            }
            rec = _build_audit_record(
                doi,
                started,
                probe_stub,
                cfg_payload["name"],
                run_id,
                evidence_max_bytes,
                forced_outcome=OUT_SKIP_CIRCUIT_OPEN,
                forced_family=FAMILY_CIRCUIT,
                forced_sig=reason,
            )
            return _finalize(rec)

        r2 = _probe_url(
            resolved_url,
            cfg_payload["timeout_sec"],
            cfg_payload["final_prefetch_bytes"],
            user_agent,
            require_content_signal=True,
        )
        retry_after = _parse_retry_after((r2.get("headers_subset") or {}).get("retry-after"))
        _shared_release(
            domain=domain,
            outcome=r2.get("outcome", OUT_FAIL_TIMEOUT_NETWORK),
            status_code=r2.get("status_code"),
            retry_after=retry_after,
            defaults=cfg_payload["defaults"],
            domain_policy=cfg_payload["domain_policy"],
            shared_state=shared_state,
            shared_lock=shared_lock,
        )
        with shared_cache_lock:
            shared_final_cache[final_key] = r2

        rec = _build_audit_record(doi, started, r2, cfg_payload["name"], run_id, evidence_max_bytes)
        rec["redirect_chain"] = (r1.get("redirect_chain", []) or []) + (r2.get("redirect_chain", []) or [])
        rec["redirect_count"] = len(rec["redirect_chain"])
        return _finalize(rec)
    except Exception as e:
        return {
            "run_id": run_id,
            "policy": cfg_payload.get("name", ""),
            "timestamp_ms": int(time.time() * 1000),
            "doi": doi,
            "resolved_url": "",
            "domain": "",
            "status_code": None,
            "redirect_chain": [],
            "redirect_count": 0,
            "headers_subset": {},
            "content_type": "",
            "content_length": "",
            "final_url_path": "",
            "final_url_query": "",
            "evidence_snippet": f"worker_exception={str(e)[:300]}",
            "outcome": OUT_FAIL_TIMEOUT_NETWORK,
            "signature_family": FAMILY_NETWORK,
            "signature_key": "worker_exception",
            "classification_evidence": "worker_exception",
            "elapsed_ms": 0,
            "worker_pid": os.getpid(),
        }


def run_audit_multiprocess(
    dois: List[str],
    cfg: PolicyConfig,
    jsonl_path: str,
    evidence_max_bytes: int,
    run_id: str,
    workers: int,
    progress_every: int = 100,
) -> List[Dict]:
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

    manager = mp.Manager()
    shared_state = manager.dict()
    shared_lock = manager.Lock()
    shared_resolve_cache = manager.dict()
    shared_final_cache = manager.dict()
    shared_cache_lock = manager.Lock()

    cfg_payload = {
        "name": cfg.name,
        "timeout_sec": cfg.timeout_sec,
        "resolve_prefetch_bytes": cfg.resolve_prefetch_bytes,
        "final_prefetch_bytes": cfg.final_prefetch_bytes,
        "defaults": cfg.defaults,
        "domain_policy": cfg.domain_policy,
        "resolve_strategy": cfg.resolve_strategy,
        "user_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    results: List[Dict] = []
    done = 0
    progress_step = max(1, int(progress_every))

    worker_prefix = os.path.splitext(jsonl_path)[0]
    old_worker_logs = glob.glob(f"{worker_prefix}.worker*.jsonl")
    for p in old_worker_logs:
        try:
            os.remove(p)
        except OSError:
            pass
    worker_log_paths: List[str] = []

    with ProcessPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [
            ex.submit(
                _process_one_multiprocess,
                doi,
                cfg_payload,
                shared_state,
                shared_lock,
                shared_resolve_cache,
                shared_final_cache,
                shared_cache_lock,
                evidence_max_bytes,
                run_id,
            )
            for doi in dois
        ]
        for fut in as_completed(futures):
            rec = fut.result()
            worker_pid = rec.pop("worker_pid", "0")
            worker_path = f"{worker_prefix}.worker{worker_pid}.jsonl"
            with open(worker_path, "a", encoding="utf-8") as wf:
                wf.write(json.dumps(rec, ensure_ascii=False) + "\n")
            worker_log_paths.append(worker_path)

            results.append(rec)
            done += 1
            if done % progress_step == 0 or done == len(dois):
                print(f"[{cfg.name}|mp={workers}] progress {done}/{len(dois)}", flush=True)

    _merge_worker_logs(worker_log_paths, jsonl_path)

    manager.shutdown()
    return results


def run_audit(
    dois: List[str],
    cfg: PolicyConfig,
    jsonl_path: str,
    evidence_max_bytes: int,
    run_id: str,
    progress_every: int = 100,
) -> List[Dict]:
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

    lock_write = threading.Lock()
    lock_cache = threading.Lock()
    progress_lock = threading.Lock()

    seen_doi: set = set()
    resolve_cache: Dict[str, Dict] = {}
    final_cache: Dict[str, Dict] = {}

    policy = DomainTrafficPolicy(cfg)
    results: List[Dict] = []

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    progress_done = {"n": 0}

    def _append_jsonl(rec: Dict):
        with lock_write:
            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def _tick():
        if progress_every <= 0:
            return
        with progress_lock:
            progress_done["n"] += 1
            n = progress_done["n"]
            if n % progress_every == 0 or n == len(dois):
                print(f"[{cfg.name}] progress {n}/{len(dois)}", flush=True)

    def _build_record(doi: str, started: float, probe: Dict, run_policy: str, forced_outcome: Optional[str] = None, forced_family: Optional[str] = None, forced_sig: str = "") -> Dict:
        final_url = probe.get("final_url") or ""
        domain = (urllib_parse.urlparse(final_url).netloc or "").lower()
        path, query = _safe_url_parts(final_url)
        snippet = (probe.get("title", "") + "\n" + probe.get("snippet", ""))[:evidence_max_bytes]

        rec = {
            "run_id": run_id,
            "policy": run_policy,
            "timestamp_ms": int(time.time() * 1000),
            "doi": doi,
            "resolved_url": final_url,
            "domain": domain,
            "status_code": probe.get("status_code"),
            "redirect_chain": probe.get("redirect_chain", []),
            "redirect_count": len(probe.get("redirect_chain", [])),
            "headers_subset": probe.get("headers_subset", {}),
            "content_type": (probe.get("headers_subset", {}) or {}).get("content-type", ""),
            "content_length": (probe.get("headers_subset", {}) or {}).get("content-length", ""),
            "final_url_path": path,
            "final_url_query": query,
            "evidence_snippet": snippet,
            "outcome": forced_outcome or probe.get("outcome") or OUT_FAIL_TIMEOUT_NETWORK,
            "signature_family": forced_family or probe.get("signature_family") or FAMILY_UNKNOWN,
            "signature_key": forced_sig or probe.get("signature_key") or "",
            "classification_evidence": probe.get("evidence") or "",
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
        }
        return rec

    def _process_one(doi: str) -> Dict:
        with lock_cache:
            if doi in seen_doi:
                rec = {
                    "run_id": run_id,
                    "policy": cfg.name,
                    "timestamp_ms": int(time.time() * 1000),
                    "doi": doi,
                    "resolved_url": "",
                    "domain": "",
                    "status_code": None,
                    "redirect_chain": [],
                    "redirect_count": 0,
                    "headers_subset": {},
                    "content_type": "",
                    "content_length": "",
                    "final_url_path": "",
                    "final_url_query": "",
                    "evidence_snippet": "duplicate_doi_skipped",
                    "outcome": OUT_SKIP_DUPLICATE,
                    "signature_family": FAMILY_UNKNOWN,
                    "signature_key": "duplicate",
                    "classification_evidence": "duplicate",
                    "elapsed_ms": 0,
                }
                _append_jsonl(rec)
                _tick()
                return rec
            seen_doi.add(doi)

        if not _valid_doi(doi):
            rec = {
                "run_id": run_id,
                "policy": cfg.name,
                "timestamp_ms": int(time.time() * 1000),
                "doi": doi,
                "resolved_url": "",
                "domain": "",
                "status_code": None,
                "redirect_chain": [],
                "redirect_count": 0,
                "headers_subset": {},
                "content_type": "",
                "content_length": "",
                "final_url_path": "",
                "final_url_query": "",
                "evidence_snippet": "invalid_doi",
                "outcome": OUT_FAIL_INVALID_DOI,
                "signature_family": FAMILY_UNKNOWN,
                "signature_key": "invalid_doi",
                "classification_evidence": "invalid_doi",
                "elapsed_ms": 0,
            }
            _append_jsonl(rec)
            _tick()
            return rec

        started = time.perf_counter()
        doi_url = f"https://doi.org/{doi}"

        with lock_cache:
            r1_cached = resolve_cache.get(doi)

        if r1_cached is None:
            if cfg.resolve_strategy == "redirect_only":
                r1 = _resolve_redirect_only(doi_url, cfg.timeout_sec, user_agent)
            else:
                r1 = _probe_url(
                    doi_url,
                    cfg.timeout_sec,
                    cfg.resolve_prefetch_bytes,
                    user_agent,
                    require_content_signal=False,
                )
            with lock_cache:
                resolve_cache[doi] = r1
        else:
            r1 = r1_cached

        # resolve 단계에서 실패/차단/캡차면 즉시 종료
        if r1.get("outcome") != OUT_SUCCESS:
            rec = _build_record(doi, started, r1, cfg.name)
            _append_jsonl(rec)
            _tick()
            return rec

        resolved_url = r1.get("final_url") or doi_url
        domain = (urllib_parse.urlparse(resolved_url).netloc or "").lower()

        is_open, reason = policy.check_circuit(domain)
        if is_open:
            probe_stub = {
                "final_url": resolved_url,
                "redirect_chain": r1.get("redirect_chain", []),
                "headers_subset": r1.get("headers_subset", {}),
                "status_code": r1.get("status_code"),
                "title": "",
                "snippet": "",
                "evidence": reason,
            }
            rec = _build_record(
                doi,
                started,
                probe_stub,
                cfg.name,
                forced_outcome=OUT_SKIP_CIRCUIT_OPEN,
                forced_family=FAMILY_CIRCUIT,
                forced_sig=reason,
            )
            _append_jsonl(rec)
            _tick()
            return rec

        final_key = f"{domain}|{resolved_url}"
        with lock_cache:
            r2_cached = final_cache.get(final_key)

        if r2_cached is None:
            ok, reason = policy.acquire(domain)
            if not ok:
                probe_stub = {
                    "final_url": resolved_url,
                    "redirect_chain": r1.get("redirect_chain", []),
                    "headers_subset": r1.get("headers_subset", {}),
                    "status_code": r1.get("status_code"),
                    "title": "",
                    "snippet": "",
                    "evidence": reason,
                }
                rec = _build_record(
                    doi,
                    started,
                    probe_stub,
                    cfg.name,
                    forced_outcome=OUT_SKIP_CIRCUIT_OPEN,
                    forced_family=FAMILY_CIRCUIT,
                    forced_sig=reason,
                )
                _append_jsonl(rec)
                _tick()
                return rec

            r2 = _probe_url(
                resolved_url,
                cfg.timeout_sec,
                cfg.final_prefetch_bytes,
                user_agent,
                require_content_signal=True,
            )
            retry_after = _parse_retry_after((r2.get("headers_subset") or {}).get("retry-after"))
            policy.release(domain, r2.get("outcome", OUT_FAIL_TIMEOUT_NETWORK), r2.get("status_code"), retry_after)

            with lock_cache:
                final_cache[final_key] = r2
        else:
            r2 = r2_cached

        # 캡차/차단 감지 시 즉시 종료 정책: 이미 single probe만 수행하므로 그대로 종료
        rec = _build_record(doi, started, r2, cfg.name)

        # resolve + final redirect chain 합치기
        rec["redirect_chain"] = (r1.get("redirect_chain", []) or []) + (r2.get("redirect_chain", []) or [])
        rec["redirect_count"] = len(rec["redirect_chain"])

        _append_jsonl(rec)
        _tick()
        return rec

    with ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        futures = [ex.submit(_process_one, doi) for doi in dois]
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                rec = {
                    "run_id": run_id,
                    "policy": cfg.name,
                    "timestamp_ms": int(time.time() * 1000),
                    "doi": "",
                    "resolved_url": "",
                    "domain": "",
                    "status_code": None,
                    "redirect_chain": [],
                    "redirect_count": 0,
                    "headers_subset": {},
                    "content_type": "",
                    "content_length": "",
                    "final_url_path": "",
                    "final_url_query": "",
                    "evidence_snippet": f"worker_exception={str(e)[:300]}",
                    "outcome": OUT_FAIL_TIMEOUT_NETWORK,
                    "signature_family": FAMILY_NETWORK,
                    "signature_key": "worker_exception",
                    "classification_evidence": "worker_exception",
                    "elapsed_ms": 0,
                }
                _append_jsonl(rec)
                results.append(rec)
                _tick()

    return results


def _load_policy_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _default_policy_defaults() -> Dict:
    return {
        "concurrency": 1,
        "base_delay": 0.0,
        "jitter": 0.02,
        "cooldown_block": 0.2,
        "cooldown_captcha": 0.2,
        "cooldown_429": 1.0,
        "max_attempts_per_domain_per_run": 10000,
        "circuit_breaker_threshold": {
            "window": 100,
            "min_samples": 100,
            "bad_ratio": 1.1,
            "open_for_sec": 0,
        },
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DOI Access Audit with root-cause and policy v2")
    p.add_argument("--csv", type=str, default=None, help="입력 CSV 경로 (하위 호환)")
    p.add_argument("--input", type=str, default=None, help="입력 CSV 경로")
    p.add_argument("--jsonl", type=str, default="outputs/access_audit.jsonl")
    p.add_argument("--report", type=str, default="outputs/audit_report.json")
    p.add_argument("--parallel-report", type=str, default="outputs/parallel_compare_report.json")
    p.add_argument("--domain-breakdown", type=str, default="outputs/domain_breakdown.json")
    p.add_argument("--rootcause", type=str, default="outputs/audit_rootcause.json")
    p.add_argument("--domain-policy-v2", type=str, default="outputs/domain_policy_v2.json")
    p.add_argument("--policy-file", type=str, default="outputs/audit_policy.json")
    p.add_argument("--evidence-max-bytes", type=int, default=1024)
    p.add_argument("--mode", choices=["compare", "baseline", "improved_v2", "compare_parallel"], default="compare")
    p.add_argument("--max-dois", type=int, default=0)
    p.add_argument("--progress-every", type=int, default=100)
    p.add_argument("--workers", type=int, default=1, help="단일 실행 시 멀티프로세스 워커 수")
    p.add_argument("--workers-baseline", type=int, default=1, help="compare_parallel에서 단일 기준 워커 수")
    p.add_argument("--workers-parallel", type=int, default=4, help="compare_parallel에서 병렬 워커 수")

    p.add_argument("--baseline-workers", type=int, default=12)
    p.add_argument("--baseline-timeout", type=float, default=4.0)
    p.add_argument("--improved-workers", type=int, default=8)
    p.add_argument("--improved-timeout", type=float, default=5.0)
    return p


def main():
    args = build_arg_parser().parse_args()

    input_csv = args.input or args.csv or "ready_to_downlaod.csv"
    dois = load_dois(input_csv)
    if args.max_dois and args.max_dois > 0:
        dois = dois[: args.max_dois]
    if not dois:
        raise RuntimeError("감사 대상 DOI가 없습니다.")

    run_stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())

    if args.mode == "compare_parallel":
        wb = max(1, int(args.workers_baseline))
        wp = max(1, int(args.workers_parallel))
        if wp < 4:
            raise ValueError("workers-parallel은 최소 4 이상이어야 합니다.")

        # compare_parallel은 stale 튜닝 정책의 영향을 제거하고 기본 정책에서 시작한다.
        loaded_policy = {}
        current_policy = {
            "version": "v2",
            "default": dict(_default_policy_defaults()),
            "domains": {},
        }

        jsonl_base, jsonl_ext = os.path.splitext(args.jsonl)
        jsonl_ext = jsonl_ext or ".jsonl"

        attempts = []
        tuning_notes = []
        max_trials = 3
        final_attempt = None
        final_policy = copy.deepcopy(current_policy)

        for trial in range(1, max_trials + 1):
            trial_tag = f"{run_stamp}_trial{trial}"
            single_jsonl = f"{jsonl_base}_{trial_tag}_w{wb}{jsonl_ext}"
            parallel_jsonl = f"{jsonl_base}_{trial_tag}_w{wp}{jsonl_ext}"
            single_run_id = f"single_w{wb}_{trial_tag}"
            parallel_run_id = f"parallel_w{wp}_{trial_tag}"

            single_cfg = PolicyConfig(
                name=f"single_w{wb}_trial{trial}",
                max_workers=wb,
                timeout_sec=args.improved_timeout,
                resolve_prefetch_bytes=1024,
                final_prefetch_bytes=32768,
                defaults=current_policy["default"],
                domain_policy=current_policy,
                resolve_strategy="redirect_only",
            )
            parallel_cfg = PolicyConfig(
                name=f"parallel_w{wp}_trial{trial}",
                max_workers=wp,
                timeout_sec=args.improved_timeout,
                resolve_prefetch_bytes=1024,
                final_prefetch_bytes=32768,
                defaults=current_policy["default"],
                domain_policy=current_policy,
                resolve_strategy="redirect_only",
            )

            single_records = run_audit_multiprocess(
                dois=dois,
                cfg=single_cfg,
                jsonl_path=single_jsonl,
                evidence_max_bytes=args.evidence_max_bytes,
                run_id=single_run_id,
                workers=wb,
                progress_every=args.progress_every,
            )
            parallel_records = run_audit_multiprocess(
                dois=dois,
                cfg=parallel_cfg,
                jsonl_path=parallel_jsonl,
                evidence_max_bytes=args.evidence_max_bytes,
                run_id=parallel_run_id,
                workers=wp,
                progress_every=args.progress_every,
            )

            seq_summary = _summarize_parallel(single_records)
            par_summary = _summarize_parallel(parallel_records)
            seq_breakdown = _domain_breakdown_from_records(single_records)
            par_breakdown = _domain_breakdown_from_records(parallel_records)

            goal = (
                float(seq_summary.get("success_ratio", 0.0)) >= 0.95
                and float(par_summary.get("success_ratio", 0.0)) >= 0.95
                and
                float(seq_summary.get("block_captcha_rate", 1.0)) < 0.05
                and float(par_summary.get("block_captcha_rate", 1.0)) < 0.05
            )

            attempt_payload = {
                "trial": trial,
                "policy_snapshot": {
                    "default": current_policy["default"],
                    "domains_tuned": len(current_policy.get("domains", {})),
                },
                "run_sequential": {
                    "workers": wb,
                    "run_id": single_run_id,
                    "jsonl": os.path.abspath(single_jsonl),
                    "summary": seq_summary,
                    "top_domains_by_failures": _top_domains_by_failures(seq_breakdown, topn=20),
                },
                "run_parallel": {
                    "workers": wp,
                    "run_id": parallel_run_id,
                    "jsonl": os.path.abspath(parallel_jsonl),
                    "summary": par_summary,
                    "top_domains_by_failures": _top_domains_by_failures(par_breakdown, topn=20),
                },
                "delta": {
                    "block_captcha_rate_pp": round((par_summary["block_captcha_rate"] - seq_summary["block_captcha_rate"]) * 100.0, 4),
                    "success_ratio_pp": round((par_summary["success_ratio"] - seq_summary["success_ratio"]) * 100.0, 4),
                },
                "goal_passed": goal,
                "domain_breakdown": {
                    "run_sequential": seq_breakdown,
                    "run_parallel": par_breakdown,
                },
            }
            attempts.append(attempt_payload)
            final_attempt = attempt_payload
            final_policy = copy.deepcopy(current_policy)

            if goal:
                break

            if trial < max_trials:
                # block/captcha가 이미 목표 이하면 더 공격적 튜닝으로 성공률을 해치지 않는다.
                if (
                    float(seq_summary.get("block_captcha_rate", 1.0)) < 0.05
                    and float(par_summary.get("block_captcha_rate", 1.0)) < 0.05
                ):
                    break
                tuned_policy, tune_note = _tune_policy_for_low_block_captcha(current_policy, single_records)
                tune_note["trial"] = trial
                tune_note["before_sequential_block_captcha_rate"] = seq_summary["block_captcha_rate"]
                tune_note["before_parallel_block_captcha_rate"] = par_summary["block_captcha_rate"]
                tuning_notes.append(tune_note)
                current_policy = tuned_policy

        if final_attempt is None:
            raise RuntimeError("compare_parallel 실행 결과가 비어 있습니다.")

        final_seq_summary = final_attempt["run_sequential"]["summary"]
        final_par_summary = final_attempt["run_parallel"]["summary"]
        gate = {
            "sequential_success_ratio_ge_95pct": final_seq_summary["success_ratio"] >= 0.95,
            "parallel_success_ratio_ge_95pct": final_par_summary["success_ratio"] >= 0.95,
            "sequential_block_captcha_lt_5pct": final_seq_summary["block_captcha_rate"] < 0.05,
            "parallel_block_captcha_lt_5pct": final_par_summary["block_captcha_rate"] < 0.05,
            "passed": bool(final_attempt["goal_passed"]),
        }

        # 최종 선택 run의 sequential+parallel 로그를 하나로 병합
        with open(args.jsonl, "w", encoding="utf-8") as out_f:
            for src in (final_attempt["run_sequential"]["jsonl"], final_attempt["run_parallel"]["jsonl"]):
                with open(src, "r", encoding="utf-8") as in_f:
                    for line in in_f:
                        out_f.write(line)

        parallel_report = {
            "generated_at": int(time.time()),
            "input_csv": os.path.abspath(input_csv),
            "total_valid": len(dois),
            "goal": "sequential/parallel success_ratio >= 0.95 and block_captcha_rate < 0.05",
            "run_sequential": {
                "total_valid": final_seq_summary["total_valid"],
                "success_ratio": final_seq_summary["success_ratio"],
                "block_captcha_rate": final_seq_summary["block_captcha_rate"],
                "p50_elapsed_ms": final_seq_summary["p50_elapsed_ms"],
                "p90_elapsed_ms": final_seq_summary["p90_elapsed_ms"],
                "fail_counts_by_reason": final_seq_summary["fail_counts_by_reason"],
                "top_domains_by_failures": final_attempt["run_sequential"]["top_domains_by_failures"],
                "jsonl": final_attempt["run_sequential"]["jsonl"],
            },
            "run_parallel": {
                "total_valid": final_par_summary["total_valid"],
                "success_ratio": final_par_summary["success_ratio"],
                "block_captcha_rate": final_par_summary["block_captcha_rate"],
                "p50_elapsed_ms": final_par_summary["p50_elapsed_ms"],
                "p90_elapsed_ms": final_par_summary["p90_elapsed_ms"],
                "fail_counts_by_reason": final_par_summary["fail_counts_by_reason"],
                "top_domains_by_failures": final_attempt["run_parallel"]["top_domains_by_failures"],
                "jsonl": final_attempt["run_parallel"]["jsonl"],
            },
            "delta": final_attempt["delta"],
            "gate": gate,
            "tuning_notes": tuning_notes,
            "attempts": attempts,
            "merged_jsonl": os.path.abspath(args.jsonl),
        }

        os.makedirs(os.path.dirname(args.parallel_report), exist_ok=True)
        with open(args.parallel_report, "w", encoding="utf-8") as f:
            json.dump(parallel_report, f, ensure_ascii=False, indent=2)

        os.makedirs(os.path.dirname(args.domain_breakdown), exist_ok=True)
        with open(args.domain_breakdown, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "generated_at": int(time.time()),
                    "input_csv": os.path.abspath(input_csv),
                    "run_sequential": final_attempt["domain_breakdown"]["run_sequential"],
                    "run_parallel": final_attempt["domain_breakdown"]["run_parallel"],
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

        if gate["passed"]:
            final_policy["generated_at"] = int(time.time())
            with open(args.domain_policy_v2, "w", encoding="utf-8") as f:
                json.dump(final_policy, f, ensure_ascii=False, indent=2)

            policy_payload = {
                "generated_at": int(time.time()),
                "selected_default_policy": "improved_v2",
                "parallel_validation": {
                    "workers_baseline": wb,
                    "workers_parallel": wp,
                    "gate": gate,
                    "report_path": os.path.abspath(args.parallel_report),
                },
                "domain_policy_v2": os.path.abspath(args.domain_policy_v2),
            }
            os.makedirs(os.path.dirname(args.policy_file), exist_ok=True)
            with open(args.policy_file, "w", encoding="utf-8") as f:
                json.dump(policy_payload, f, ensure_ascii=False, indent=2)

        print("\n[Overall Summary]")
        _print_overall_markdown_table(parallel_report)
        print("\n[Domain Breakdown Top 20]")
        _print_domain_markdown_table(
            final_attempt["domain_breakdown"]["run_sequential"],
            final_attempt["domain_breakdown"]["run_parallel"],
            topn=20,
        )

        print(
            json.dumps(
                {
                    "mode": args.mode,
                    "total_valid": len(dois),
                    "workers_baseline": wb,
                    "workers_parallel": wp,
                    "gate_passed": gate["passed"],
                    "parallel_compare_report": os.path.abspath(args.parallel_report),
                    "domain_breakdown": os.path.abspath(args.domain_breakdown),
                    "merged_jsonl": os.path.abspath(args.jsonl),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    report = {
        "generated_at": int(time.time()),
        "input_csv": os.path.abspath(input_csv),
        "total_dois": len(dois),
        "runs": {},
        "gate": None,
        "selected_default_policy": None,
    }

    baseline_cfg = PolicyConfig(
        name="baseline",
        max_workers=args.baseline_workers,
        timeout_sec=args.baseline_timeout,
        resolve_prefetch_bytes=2048,
        final_prefetch_bytes=4096,
        defaults=_default_policy_defaults(),
        domain_policy={"domains": {}},
        resolve_strategy="full_probe",
    )

    improved_cfg = PolicyConfig(
        name="improved_v2",
        max_workers=args.improved_workers,
        timeout_sec=args.improved_timeout,
        resolve_prefetch_bytes=2048,
        final_prefetch_bytes=8192,
        defaults=_default_policy_defaults(),
        domain_policy={},
        resolve_strategy="redirect_only",
    )

    baseline_records: List[Dict] = []
    improved_records: List[Dict] = []

    def _run_with_config(run_dois: List[str], run_cfg: PolicyConfig, run_id: str) -> List[Dict]:
        if int(args.workers) > 1:
            return run_audit_multiprocess(
                dois=run_dois,
                cfg=run_cfg,
                jsonl_path=args.jsonl,
                evidence_max_bytes=args.evidence_max_bytes,
                run_id=run_id,
                workers=int(args.workers),
                progress_every=args.progress_every,
            )
        return run_audit(
            dois=run_dois,
            cfg=run_cfg,
            jsonl_path=args.jsonl,
            evidence_max_bytes=args.evidence_max_bytes,
            run_id=run_id,
            progress_every=args.progress_every,
        )

    if args.mode in ("compare", "baseline"):
        run_id = f"baseline_{run_stamp}"
        baseline_records = _run_with_config(dois, baseline_cfg, run_id)
        report["runs"]["baseline"] = {
            "run_id": run_id,
            "summary": _summarize(baseline_records),
        }

        rc_base = _build_rootcause_for_run(baseline_records)
        domain_policy_v2 = _generate_domain_policy_v2(rc_base, args.domain_policy_v2)
    else:
        domain_policy_v2 = _load_policy_json(args.domain_policy_v2)

    improved_cfg.domain_policy = domain_policy_v2
    if isinstance(domain_policy_v2, dict) and domain_policy_v2.get("default"):
        improved_cfg.defaults = dict(_default_policy_defaults())
        # v2 default overrides
        for k, v in domain_policy_v2.get("default", {}).items():
            if k == "circuit_breaker_threshold":
                merged = dict(improved_cfg.defaults.get("circuit_breaker_threshold", {}))
                merged.update(v)
                improved_cfg.defaults[k] = merged
            else:
                improved_cfg.defaults[k] = v

    if args.mode in ("compare", "improved_v2"):
        run_id = f"improved_v2_{run_stamp}"
        improved_records = _run_with_config(dois, improved_cfg, run_id)
        report["runs"]["improved_v2"] = {
            "run_id": run_id,
            "summary": _summarize(improved_records),
        }

    rootcause_report = {
        "generated_at": int(time.time()),
        "input_csv": os.path.abspath(input_csv),
        "total_dois": len(dois),
        "runs": {},
        "domain_policy_v2_path": os.path.abspath(args.domain_policy_v2),
    }

    if baseline_records:
        rootcause_report["runs"]["baseline"] = _build_rootcause_for_run(baseline_records)
    if improved_records:
        rootcause_report["runs"]["improved_v2"] = _build_rootcause_for_run(improved_records)

    os.makedirs(os.path.dirname(args.rootcause), exist_ok=True)
    with open(args.rootcause, "w", encoding="utf-8") as f:
        json.dump(rootcause_report, f, ensure_ascii=False, indent=2)

    if args.mode == "compare":
        gate = _gate_v2(report["runs"]["baseline"]["summary"], report["runs"]["improved_v2"]["summary"])
        report["gate"] = gate
        report["selected_default_policy"] = "improved_v2" if gate["passed"] else "baseline"
    elif args.mode == "baseline":
        report["selected_default_policy"] = "baseline"
    else:
        report["selected_default_policy"] = "improved_v2"

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    policy_payload = {
        "generated_at": int(time.time()),
        "selected_default_policy": report["selected_default_policy"],
        "gate": report.get("gate"),
        "domain_policy_v2": os.path.abspath(args.domain_policy_v2),
    }
    os.makedirs(os.path.dirname(args.policy_file), exist_ok=True)
    with open(args.policy_file, "w", encoding="utf-8") as f:
        json.dump(policy_payload, f, ensure_ascii=False, indent=2)

    print(
        json.dumps(
            {
                "total_dois": len(dois),
                "mode": args.mode,
                "selected_default_policy": report["selected_default_policy"],
                "report": os.path.abspath(args.report),
                "rootcause": os.path.abspath(args.rootcause),
                "domain_policy_v2": os.path.abspath(args.domain_policy_v2),
                "jsonl": os.path.abspath(args.jsonl),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
