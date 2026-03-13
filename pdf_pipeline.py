import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlencode, urljoin, urlparse

REASON_FAIL_WRONG_MIME = "FAIL_WRONG_MIME"
REASON_FAIL_VIEWER_HTML = "FAIL_VIEWER_HTML"
REASON_FAIL_HTTP_STATUS = "FAIL_HTTP_STATUS"
REASON_FAIL_TIMEOUT_NETWORK = "FAIL_TIMEOUT/NETWORK"
REASON_FAIL_PDF_MAGIC = "FAIL_PDF_MAGIC"
REASON_FAIL_TOO_SMALL = "FAIL_TOO_SMALL"
REASON_FAIL_NO_CANDIDATE = "FAIL_NO_CANDIDATE"
REASON_FAIL_REDIRECT_LOOP = "FAIL_REDIRECT_LOOP"
REASON_FAIL_UNKNOWN = "FAIL_UNKNOWN"
REASON_SUCCESS = "SUCCESS"

PDF_MAGIC = b"%PDF"


@dataclass
class DownloadAttempt:
    success: bool
    reason: str
    strategy: str
    phase: str
    elapsed_ms: int
    url: str
    final_url: str
    domain: str
    status_code: Optional[int]
    content_type: str
    content_disposition: str
    content_length: Optional[int]
    redirect_chain: List[str]
    first_bytes: str
    evidence: Dict
    file_path: Optional[str] = None


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _first_bytes(data: bytes, n: int = 64) -> str:
    if not data:
        return ""
    return data[:n].hex()


def _is_pdf_magic(data: bytes) -> bool:
    return bool(data and data.startswith(PDF_MAGIC))


def _looks_like_html(data: bytes) -> bool:
    if not data:
        return False
    head = data[:256].lower()
    return b"<html" in head or b"<!doctype html" in head


def _extract_pdf_candidates(base_url: str, html: str) -> List[str]:
    candidates: List[str] = []

    patterns = [
        r"""citation_pdf_url["']?\s+content=["']([^"']+)["']""",
        r"""og:pdf["']?\s+content=["']([^"']+)["']""",
        r"""<a[^>]+href=["']([^"']+)["']""",
        r"""<iframe[^>]+src=["']([^"']+)["']""",
        r"""<embed[^>]+src=["']([^"']+)["']""",
    ]

    for pat in patterns:
        for m in re.finditer(pat, html, flags=re.IGNORECASE):
            raw = (m.group(1) or "").strip()
            if not raw or "javascript:" in raw.lower():
                continue
            resolved = urljoin(base_url, raw)
            resolved_low = resolved.lower()
            raw_low = raw.lower()
            if any(
                token in resolved_low or token in raw_low
                for token in (
                    ".pdf",
                    "/pdfft",
                    "/articlepdf",
                    "/doi/pdf",
                    "stamppdf/getpdf.jsp",
                    "stamp.jsp",
                    "viewpdf",
                    "download=true",
                    "pdf",
                )
            ):
                candidates.append(resolved)

    base_low = (base_url or "").lower()
    arnumber = ""
    m = re.search(r"[?&]arnumber=(\d+)", base_low)
    if not m:
        m = re.search(r"/document/(\d+)", base_low)
    if not m:
        m = re.search(r'"arnumber"\s*:\s*"?(\\d+)"?', html, flags=re.IGNORECASE)
    if m:
        arnumber = str(m.group(1) or "").strip()
    if arnumber:
        candidates.append(f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={arnumber}&ref=")
        candidates.append(f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={arnumber}")

    pii = ""
    m = re.search(r"/pii/([A-Z0-9]+)", base_url, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"/pii/([A-Z0-9]+)", html, flags=re.IGNORECASE)
    if m:
        pii = str(m.group(1) or "").strip().upper()
    if pii:
        md5 = ""
        pid = ""
        path = "science/article/pii"
        ext = "/pdfft"
        m = re.search(r'"md5":"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            md5 = str(m.group(1) or "").strip()
        m = re.search(r'"pid":"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            pid = str(m.group(1) or "").strip()
        m = re.search(r'"path":"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            path = str(m.group(1) or "").strip().strip("/") or path
        m = re.search(r'"pdfextension":"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            ext = str(m.group(1) or "").strip() or ext
        if not ext.startswith("/"):
            ext = "/" + ext
        pdfft_url = f"https://www.sciencedirect.com/{path}/{pii}{ext}"
        query = {}
        if md5:
            query["md5"] = md5
        if pid:
            query["pid"] = pid
        if query:
            pdfft_url += "?" + urlencode(query)
        candidates.append(pdfft_url)

    uniq = []
    seen = set()
    for c in candidates:
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq[:8]


def _classify_non_pdf(content_type: str, body: bytes) -> Optional[str]:
    ctype = (content_type or "").lower()
    if _is_pdf_magic(body):
        return None
    if "application/pdf" in ctype and not _looks_like_html(body):
        return None
    if "text/html" in ctype or "application/xhtml" in ctype or _looks_like_html(body):
        text = body[:20000].decode("utf-8", errors="ignore").lower()
        viewer_keywords = [
            "pdf viewer",
            "citation_pdf_url",
            "download pdf",
            "view pdf",
            "iframe",
            "embed",
        ]
        if any(k in text for k in viewer_keywords):
            return REASON_FAIL_VIEWER_HTML
    return REASON_FAIL_WRONG_MIME


def _build_attempt(
    success: bool,
    reason: str,
    strategy: str,
    phase: str,
    elapsed_ms: int,
    url: str,
    final_url: str,
    status_code: Optional[int],
    headers: Dict,
    body: bytes,
    redirect_chain: List[str],
    evidence: Dict,
    file_path: Optional[str] = None,
) -> DownloadAttempt:
    return DownloadAttempt(
        success=success,
        reason=reason,
        strategy=strategy,
        phase=phase,
        elapsed_ms=elapsed_ms,
        url=url,
        final_url=final_url,
        domain=_domain(final_url or url),
        status_code=status_code,
        content_type=headers.get("Content-Type", "") if headers else "",
        content_disposition=headers.get("Content-Disposition", "") if headers else "",
        content_length=int(headers.get("Content-Length", 0)) if headers and str(headers.get("Content-Length", "")).isdigit() else None,
        redirect_chain=redirect_chain,
        first_bytes=_first_bytes(body),
        evidence=evidence,
        file_path=file_path,
    )


def _save_pdf_and_verify(path: str, body: bytes, min_size: int) -> Tuple[bool, str]:
    if len(body) < min_size:
        return False, REASON_FAIL_TOO_SMALL
    if not _is_pdf_magic(body):
        return False, REASON_FAIL_PDF_MAGIC

    with open(path, "wb") as f:
        f.write(body)

    # File-level verification for PDF-only guarantee
    with open(path, "rb") as f:
        first = f.read(5)
    if not first.startswith(PDF_MAGIC):
        return False, REASON_FAIL_PDF_MAGIC

    if os.path.getsize(path) < min_size:
        return False, REASON_FAIL_TOO_SMALL

    return True, REASON_SUCCESS


def _do_get(url: str, timeout: int, headers: Dict, cookies=None):
    redirect_chain: List[str] = [url]

    class _TrackingRedirect(urllib_request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, hdrs, newurl):
            redirect_chain.append(newurl)
            return super().redirect_request(req, fp, code, msg, hdrs, newurl)

    opener = urllib_request.build_opener(_TrackingRedirect)
    req_headers = dict(headers or {})
    if cookies:
        if isinstance(cookies, dict):
            req_headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
        elif isinstance(cookies, list):
            req_headers["Cookie"] = "; ".join(
                f"{c.get('name')}={c.get('value')}" for c in cookies if isinstance(c, dict)
            )

    req = urllib_request.Request(url=url, headers=req_headers, method="GET")
    resp = opener.open(req, timeout=timeout)
    body = resp.read()
    status = resp.getcode()
    headers_resp = dict(resp.headers.items())
    final_url = getattr(resp, "url", url)
    return {
        "status_code": status,
        "headers": headers_resp,
        "content": body,
        "url": final_url,
        "redirect_chain": redirect_chain if redirect_chain else [url, final_url],
    }


def download_pdf(
    url: str,
    save_path: str,
    *,
    strategy_mode: str = "baseline",  # baseline | candidate
    timeout: int = 60,
    min_size: int = 1024,
    headers: Optional[Dict] = None,
    cookies=None,
    strategy_name: str = "cffi",
    phase: str = "direct",
    max_viewer_hops: int = 1,
    fetcher=None,
) -> DownloadAttempt:
    started = time.perf_counter()
    headers = headers or {}

    if "User-Agent" not in headers:
        headers["User-Agent"] = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    try:
        fetch_fn = fetcher or _do_get
        response = fetch_fn(url, timeout=timeout, headers=headers, cookies=cookies)
        elapsed = int((time.perf_counter() - started) * 1000)

        redirect_chain = response.get("redirect_chain", [url, response.get("url", url)])
        if len(redirect_chain) != len(set(redirect_chain)):
            return _build_attempt(
                False,
                REASON_FAIL_REDIRECT_LOOP,
                strategy_name,
                phase,
                elapsed,
                url,
                response.get("url", url),
                response.get("status_code"),
                response.get("headers", {}),
                response.get("content") or b"",
                redirect_chain,
                {"loop_detected": True},
            )

        status_code = response.get("status_code")
        body = response.get("content") or b""
        headers_resp = response.get("headers", {})

        if status_code != 200:
            retry_after = headers_resp.get("Retry-After")
            return _build_attempt(
                False,
                REASON_FAIL_HTTP_STATUS,
                strategy_name,
                phase,
                elapsed,
                url,
                response.get("url", url),
                status_code,
                headers_resp,
                body,
                redirect_chain,
                {"status_code": status_code, "retry_after": retry_after},
            )

        content_type = headers_resp.get("Content-Type", "")
        non_pdf_reason = _classify_non_pdf(content_type, body)

        if non_pdf_reason in (REASON_FAIL_VIEWER_HTML, REASON_FAIL_WRONG_MIME):
            # candidate mode: viewer HTML에서 PDF 후보 확장
            if strategy_mode == "candidate" and non_pdf_reason == REASON_FAIL_VIEWER_HTML and max_viewer_hops > 0:
                html_text = body.decode("utf-8", errors="ignore")
                candidates = _extract_pdf_candidates(response.get("url", url), html_text)
                if not candidates:
                    return _build_attempt(
                        False,
                        REASON_FAIL_NO_CANDIDATE,
                        strategy_name,
                        phase,
                        elapsed,
                        url,
                        response.get("url", url),
                        status_code,
                        headers_resp,
                        body,
                        redirect_chain,
                        {"viewer_html": True, "candidates": 0},
                    )

                last_attempt = None
                for idx, cand in enumerate(candidates):
                    cand_attempt = download_pdf(
                        cand,
                        save_path,
                        strategy_mode=strategy_mode,
                        timeout=timeout,
                        min_size=min_size,
                        headers=headers,
                        cookies=cookies,
                        strategy_name=strategy_name,
                        phase=f"viewer_candidate_{idx}",
                        max_viewer_hops=max_viewer_hops - 1,
                        fetcher=fetch_fn,
                    )
                    if cand_attempt.success:
                        return cand_attempt
                    last_attempt = cand_attempt

                return last_attempt if last_attempt else _build_attempt(
                    False,
                    REASON_FAIL_NO_CANDIDATE,
                    strategy_name,
                    phase,
                    elapsed,
                    url,
                    response.get("url", url),
                    status_code,
                    headers_resp,
                    body,
                    redirect_chain,
                    {"viewer_html": True, "candidate_attempts": len(candidates)},
                )

            return _build_attempt(
                False,
                non_pdf_reason,
                strategy_name,
                phase,
                elapsed,
                url,
                response.get("url", url),
                status_code,
                headers_resp,
                body,
                redirect_chain,
                {"content_type": content_type},
            )

        ok, reason = _save_pdf_and_verify(save_path, body, min_size=min_size)
        return _build_attempt(
            ok,
            reason,
            strategy_name,
            phase,
            elapsed,
            url,
            response.get("url", url),
            status_code,
            headers_resp,
            body,
            redirect_chain,
            {"validated": ok},
            file_path=save_path if ok else None,
        )

    except (TimeoutError, OSError) as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        if "timed out" not in str(e).lower() and "timeout" not in str(e).lower():
            # Non-timeout OS errors are treated as network failures in taxonomy.
            pass
        return _build_attempt(
            False,
            REASON_FAIL_TIMEOUT_NETWORK,
            strategy_name,
            phase,
            elapsed,
            url,
            url,
            None,
            {},
            b"",
            [url],
            {"error": str(e), "kind": "timeout"},
        )
    except urllib_error.HTTPError as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        body = e.read() if hasattr(e, "read") else b""
        headers_resp = dict(e.headers.items()) if getattr(e, "headers", None) else {}
        return _build_attempt(
            False,
            REASON_FAIL_HTTP_STATUS if 400 <= int(getattr(e, "code", 0) or 0) < 600 else REASON_FAIL_UNKNOWN,
            strategy_name,
            phase,
            elapsed,
            url,
            getattr(e, "url", url),
            getattr(e, "code", None),
            headers_resp,
            body,
            [url, getattr(e, "url", url)],
            {"error": str(e), "kind": "http_error"},
        )
    except urllib_error.URLError as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        msg = str(e).lower()
        reason = REASON_FAIL_REDIRECT_LOOP if "redirect" in msg or "infinite loop" in msg else REASON_FAIL_TIMEOUT_NETWORK
        return _build_attempt(
            False,
            reason,
            strategy_name,
            phase,
            elapsed,
            url,
            url,
            None,
            {},
            b"",
            [url],
            {"error": str(e), "kind": "url_error"},
        )
    except Exception as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        return _build_attempt(
            False,
            REASON_FAIL_TIMEOUT_NETWORK,
            strategy_name,
            phase,
            elapsed,
            url,
            url,
            None,
            {},
            b"",
            [url],
            {"error": str(e), "kind": "network_or_unknown"},
        )


def append_metrics_jsonl(path: str, attempt: Any, extra: Optional[Dict[str, Any]] = None) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    if isinstance(attempt, DownloadAttempt):
        payload = asdict(attempt)
    elif isinstance(attempt, dict):
        payload = dict(attempt)
    else:
        raise TypeError(f"Unsupported attempt payload type: {type(attempt)!r}")
    if extra:
        payload.update({str(k): v for k, v in extra.items()})
    payload.setdefault("record_type", "attempt_result")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
