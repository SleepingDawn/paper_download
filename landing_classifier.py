import csv
import json
import os
import random
import re
import time
from collections import Counter, defaultdict, deque
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from tools_exp import normalize_publisher_label

STATE_SUCCESS_LANDING = "success_landing"
STATE_DIRECT_PDF_HANDOFF = "direct_pdf_handoff"
STATE_CHALLENGE_DETECTED = "challenge_detected"
STATE_BLANK_OR_INCOMPLETE = "blank_or_incomplete"
STATE_CONSENT_OR_INTERSTITIAL_BLOCK = "consent_or_interstitial_block"
STATE_BROKEN_JS_SHELL = "broken_js_shell"
STATE_DOMAIN_MISMATCH = "domain_mismatch"
STATE_PUBLISHER_ERROR = "publisher_error"
STATE_TIMEOUT = "timeout"
STATE_NETWORK_ERROR = "network_error"
STATE_UNKNOWN_NON_SUCCESS = "unknown_non_success"

SUCCESS_STATES = {STATE_SUCCESS_LANDING, STATE_DIRECT_PDF_HANDOFF}
NON_SUCCESS_STATES = {
    STATE_CHALLENGE_DETECTED,
    STATE_BLANK_OR_INCOMPLETE,
    STATE_CONSENT_OR_INTERSTITIAL_BLOCK,
    STATE_BROKEN_JS_SHELL,
    STATE_DOMAIN_MISMATCH,
    STATE_PUBLISHER_ERROR,
    STATE_TIMEOUT,
    STATE_NETWORK_ERROR,
    STATE_UNKNOWN_NON_SUCCESS,
}

SAFE_LANDING_MAX_WORKERS = 2
DEFAULT_PER_PUBLISHER_COOLDOWN_SEC = float(os.getenv("LANDING_PER_PUBLISHER_COOLDOWN_SEC", "7"))
DEFAULT_GLOBAL_START_SPACING_SEC = float(os.getenv("LANDING_GLOBAL_START_SPACING_SEC", "1.5"))
DEFAULT_PER_DOI_DEADLINE_SEC = float(os.getenv("LANDING_PER_DOI_DEADLINE_SEC", "75"))
DEFAULT_MAX_NAV_ATTEMPTS = int(os.getenv("LANDING_MAX_NAV_ATTEMPTS", "2"))
DEFAULT_JITTER_MIN_SEC = float(os.getenv("LANDING_JITTER_MIN_SEC", "0.7"))
DEFAULT_JITTER_MAX_SEC = float(os.getenv("LANDING_JITTER_MAX_SEC", "1.8"))
DEFAULT_SETTLE_WAIT_SEC = float(os.getenv("LANDING_SETTLE_WAIT_SEC", "1.2"))
DEFAULT_STABILIZE_POLLS = int(os.getenv("LANDING_STABILIZE_POLLS", "2"))
DEFAULT_CHALLENGE_COOLDOWN_MULTIPLIER = float(os.getenv("LANDING_CHALLENGE_COOLDOWN_MULTIPLIER", "2.2"))
DEFAULT_CHALLENGE_MIN_HOLDOFF_SEC = float(os.getenv("LANDING_CHALLENGE_MIN_HOLDOFF_SEC", "18"))

DOI_RE = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$")
DOI_TEXT_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", re.IGNORECASE)
SPACE_RE = re.compile(r"\s+")
URLISH_TITLE_RE = re.compile(r"^(?:https?://)?[a-z0-9.-]+(?:/[^\s]*)?$", re.IGNORECASE)
MAIN_HTML_SELECTORS = (
    "main",
    "article",
    '[role="main"]',
    "#main-content",
    "#maincontent",
    "#main_container",
    "#sub_container2",
    ".main-content",
    ".article",
    ".article-content",
    ".article__body",
)

DOI_PREFIX_TO_PUBLISHER = {
    "10.1016": "elsevier",
    "10.1021": "acs",
    "10.1038": "nature",
    "10.1039": "rsc",
    "10.1063": "aip",
    "10.1088": "iop",
    "10.1109": "ieee",
    "10.1103": "aps",
    "10.1002": "wiley",
    "10.1111": "wiley",
    "10.1116": "aip",
    "10.7150": "ivyspring",
    "10.3390": "mdpi",
    "10.3389": "frontiers",
    "10.1364": "optica",
    "10.1117": "spie",
    "10.1080": "taylor_and_francis",
}

PUBLISHER_DOMAIN_HINTS = {
    "acs": ("pubs.acs.org",),
    "aip": ("pubs.aip.org", "avs.scitation.org", "aip.scitation.org"),
    "aps": ("journals.aps.org",),
    "elsevier": ("sciencedirect.com", "linkinghub.elsevier.com", "elsevier.com", "cell.com", "thelancet.com"),
    "frontiers": ("frontiersin.org",),
    "ieee": ("ieeexplore.ieee.org",),
    "iop": ("iopscience.iop.org",),
    "ivyspring": ("thno.org", "www.thno.org"),
    "mdpi": ("mdpi.com",),
    "nature": ("nature.com", "link.springer.com", "springer.com"),
    "optica": ("opg.optica.org", "optica.org"),
    "powdermat": ("powdermat.org",),
    "rsc": ("pubs.rsc.org",),
    "spie": ("spiedigitallibrary.org",),
    "taylor_and_francis": ("tandfonline.com",),
    "wiley": ("onlinelibrary.wiley.com", "advanced.onlinelibrary.wiley.com"),
}

PUBLISHER_HTML_MARKERS = {
    "acs": ("article_header-title", "article_abstract", "loa__article", "citation_pdf_url"),
    "aip": ("publicationcontenttitle", "citation_abstract_html_url", "article__headline", "abstract"),
    "aps": ("aps-article", "citation_pdf_url", "physrev", "abstract"),
    "elsevier": ("sciencedirect", "\"pii\"", "citation_abstract_html_url", "abstract", "article"),
    "frontiers": ("journalfulltext", "articlereference", "frontiers", "citation_pdf_url"),
    "ieee": ("xplore-document-title", "stats-document-abstract", "\"publicationtitle\""),
    "iop": ("wd-jnl-art-title", "article-text", "abstract"),
    "ivyspring": ("citation_pdf_url", "citation_journal_title", "theranostics", "headinga1"),
    "mdpi": ("art-title", "html-abstract", "pubhistory"),
    "nature": ("c-article-header", "c-article-body", "article-item__title"),
    "optica": ("articlebody", "abstract", "opg"),
    "rsc": ("article_header", "article_info", "capsule__title", "abstract"),
    "spie": ("articlemeta", "abstract", "citation_title"),
    "taylor_and_francis": ("citation__title", "abstractsection", "hlfld-abstract"),
    "wiley": ("article-header__title", "article-section__abstract", "citation_journal_title"),
}

INTERSTITIAL_TITLES = {
    "redirecting",
    "redirecting...",
    "loading",
    "please wait",
    "just a moment",
}

CHALLENGE_URL_MARKERS = (
    "__cf_chl_rt_tk=",
    "/cdn-cgi/challenge",
    "/cdn-cgi/l/chk_captcha",
    "challenges.cloudflare.com",
    "cf-turnstile",
    "/captcha/",
)

CHALLENGE_TEXT_MARKERS = (
    "verify you are human",
    "are you a robot",
    "are you human",
    "security check",
    "unusual traffic",
    "request blocked",
    "attention required",
    "captcha",
)

CHALLENGE_HTML_MARKERS = (
    "challenge-form",
    "captcha-box",
    "cf-turnstile",
    "/cdn-cgi/challenge",
)

BROKEN_JS_SHELL_TITLE_MARKERS = (
    "failed to fetch dynamically imported module",
    "chunkloaderror",
    "chunk load error",
)

BROKEN_JS_SHELL_TEXT_MARKERS = (
    "failed to fetch dynamically imported module",
    "access the old version here",
    "unexpected application error",
    "loading chunk",
)

BROKEN_JS_SHELL_HTML_MARKERS = (
    "failed to fetch dynamically imported module",
    "chunkloaderror",
    "__nuxt_error",
)

SOFT_CHALLENGE_VENDOR_MARKERS = (
    "validate.perfdrive.com",
    "stormcaster.js",
)

CONSENT_MARKERS = (
    "cookie",
    "consent",
    "manage preferences",
    "accept all",
    "accept cookies",
    "privacy preference",
    "privacy choices",
    "onetrust",
    "continue with only essential cookies",
    "before you continue",
)

INTERSTITIAL_MARKERS = (
    "redirecting",
    "retrieve/pii",
    "please wait",
    "loading article",
    "openurl",
    "xref?genre=article",
    "tdm-reservation",
    "enable javascript to continue",
)

ERROR_MARKERS = (
    "404",
    "not found",
    "page not found",
    "temporarily unavailable",
    "service unavailable",
    "internal server error",
    "bad gateway",
    "gateway timeout",
    "this site can't be reached",
    "problem loading page",
    "unable to locate",
    "article not found",
)

ACCESS_GATE_LOGIN_MARKERS = (
    "sign in through your institution",
    "institutional access",
    "institutional login",
    "access through your institution",
    "openathens",
    "shibboleth",
)

ACCESS_GATE_PAYWALL_MARKERS = (
    "purchase instant access",
    "subscribe to this journal",
    "buy this article",
    "rent this article",
)


def _clean_space(value: str) -> str:
    return SPACE_RE.sub(" ", str(value or "")).strip()


def _extract_domain(url: str) -> str:
    try:
        return (urlparse(str(url or "")).netloc or "").lower()
    except Exception:
        return ""


def _normalize_doi(doi: str) -> str:
    raw = str(doi or "").strip().lower()
    return raw.replace("https://doi.org/", "").replace("http://doi.org/", "").strip()


def _doi_prefix(doi: str) -> str:
    norm = _normalize_doi(doi)
    if "/" not in norm:
        return ""
    return norm.split("/", 1)[0]


def _publisher_key_from_label(raw_label: str) -> str:
    raw = _clean_space(raw_label).lower()
    if not raw:
        return ""
    if "advanced materials" in raw:
        return "wiley"
    if "american chemical society" in raw or raw == "acs":
        return "acs"
    if "american institute of physics" in raw or raw == "aip":
        return "aip"
    if "american physical society" in raw or raw == "aps":
        return "aps"
    if "elsevier" in raw:
        return "elsevier"
    if "wiley" in raw:
        return "wiley"
    if "royal society of chemistry" in raw or raw == "rsc":
        return "rsc"
    if "nature" in raw or "springer" in raw:
        return "nature"
    if "multidisciplinary digital publishing institute" in raw or raw == "mdpi":
        return "mdpi"
    if "frontiers" in raw:
        return "frontiers"
    if "ivyspring" in raw or "theranostics" in raw:
        return "ivyspring"
    if "institute of physics" in raw or raw == "iop" or "iop publishing" in raw:
        return "iop"
    if "institute of electrical and electronics engineers" in raw or raw == "ieee":
        return "ieee"
    if "taylor & francis" in raw or "taylor and francis" in raw:
        return "taylor_and_francis"
    if "optica" in raw:
        return "optica"
    if "spie" in raw:
        return "spie"
    if "hanguk bunmal jaeryo hakoeji" in raw or "journal of powder materials" in raw or "powder materials" in raw:
        return "powdermat"
    return ""


def estimate_publisher_key(doi: str, input_publisher: str = "", pdf_url: str = "") -> str:
    direct = _publisher_key_from_label(input_publisher)
    if direct:
        return direct

    normalized = normalize_publisher_label(str(input_publisher or ""))
    mapped = _publisher_key_from_label(str(normalized or ""))
    if mapped:
        return mapped

    domain = _extract_domain(pdf_url)
    for key, domains in PUBLISHER_DOMAIN_HINTS.items():
        if any(domain == d or domain.endswith(f".{d}") for d in domains):
            return key

    prefix = _doi_prefix(doi)
    if prefix in DOI_PREFIX_TO_PUBLISHER:
        return DOI_PREFIX_TO_PUBLISHER[prefix]
    return prefix or "unknown"


def expected_domains_for_record(record: Dict[str, Any]) -> Tuple[str, ...]:
    return tuple(PUBLISHER_DOMAIN_HINTS.get(str(record.get("scheduler_publisher") or ""), ()))


def load_landing_inputs(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    out: List[Dict[str, Any]] = []
    seen = set()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        field_map = {str(name or "").strip().lower(): name for name in (reader.fieldnames or [])}
        doi_col = field_map.get("doi")
        if not doi_col:
            raise ValueError("CSV DOI column not found")

        for row in reader:
            doi = _normalize_doi(row.get(doi_col, ""))
            if doi in seen or not DOI_RE.match(doi):
                continue
            seen.add(doi)
            publisher = str(row.get(field_map.get("publisher", ""), "") or "").strip()
            title = str(row.get(field_map.get("title", ""), "") or "").strip()
            pdf_url = str(row.get(field_map.get("pdf_url", ""), "") or "").strip()
            open_access = str(row.get(field_map.get("open_access", ""), "") or "").strip()
            scheduler_publisher = estimate_publisher_key(doi, input_publisher=publisher, pdf_url=pdf_url)
            out.append(
                {
                    "doi": doi,
                    "input_publisher": publisher,
                    "input_title": title,
                    "input_pdf_url": pdf_url,
                    "open_access": open_access,
                    "scheduler_publisher": scheduler_publisher,
                }
            )
    return out


def reorder_inputs_for_pacing(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, deque] = defaultdict(deque)
    for rec in records:
        groups[str(rec.get("scheduler_publisher") or "unknown")].append(rec)

    ordered: List[Dict[str, Any]] = []
    last_key = ""
    while groups:
        candidates = sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)
        chosen_key = ""
        for key, items in candidates:
            if key != last_key:
                chosen_key = key
                break
        if not chosen_key:
            chosen_key = candidates[0][0]
        ordered.append(groups[chosen_key].popleft())
        if not groups[chosen_key]:
            groups.pop(chosen_key, None)
        last_key = chosen_key
    return ordered


def chunk_inputs_round_robin(records: Sequence[Dict[str, Any]], workers: int) -> List[List[Dict[str, Any]]]:
    workers = max(1, int(workers))
    chunks: List[List[Dict[str, Any]]] = [[] for _ in range(workers)]
    for idx, rec in enumerate(records):
        chunks[idx % workers].append(rec)
    return chunks


def reserve_pacing_slot(
    shared_state,
    shared_lock,
    publisher_key: str,
    cooldown_sec: float,
    global_spacing_sec: float,
    jitter_min_sec: float,
    jitter_max_sec: float,
) -> Dict[str, Any]:
    pub = str(publisher_key or "unknown")
    requested_wall = time.time()
    requested_mono = time.monotonic()
    jitter = random.uniform(max(0.0, jitter_min_sec), max(jitter_min_sec, jitter_max_sec))
    while True:
        wait_sec = 0.0
        with shared_lock:
            now = time.monotonic()
            active_key = f"active::{pub}"
            last_finish_key = f"last_finish::{pub}"
            penalty_until_key = f"penalty_until::{pub}"
            active_count = int(shared_state.get(active_key, 0) or 0)
            last_finish = float(shared_state.get(last_finish_key, 0.0) or 0.0)
            penalty_until = float(shared_state.get(penalty_until_key, 0.0) or 0.0)
            last_global = float(shared_state.get("last_global_start", 0.0) or 0.0)
            wait_pub = (last_finish + max(0.0, cooldown_sec) + jitter) - now
            wait_penalty = penalty_until - now
            wait_global = (last_global + max(0.0, global_spacing_sec)) - now
            wait_sec = max(0.0, wait_pub, wait_penalty, wait_global)
            if active_count <= 0 and wait_sec <= 0.0:
                shared_state[active_key] = 1
                shared_state["last_global_start"] = now
                shared_state["last_global_publisher"] = pub
                return {
                    "requested_start_ms": int(requested_wall * 1000),
                    "actual_start_ms": int(time.time() * 1000),
                    "wait_ms": int(max(0.0, time.monotonic() - requested_mono) * 1000),
                    "publisher_key": pub,
                    "jitter_sec": round(float(jitter), 3),
                    "penalty_wait_ms": int(max(0.0, wait_penalty) * 1000),
                }
        time.sleep(min(max(wait_sec, 0.25), 1.0))


def release_pacing_slot(
    shared_state,
    shared_lock,
    publisher_key: str,
    classifier_state: str = "",
    reason_codes: Sequence[str] = (),
) -> None:
    pub = str(publisher_key or "unknown")
    with shared_lock:
        active_key = f"active::{pub}"
        current = int(shared_state.get(active_key, 0) or 0)
        now = time.monotonic()
        shared_state[active_key] = max(0, current - 1)
        shared_state[f"last_finish::{pub}"] = now
        if str(classifier_state or "") == STATE_CHALLENGE_DETECTED:
            challenge_holdoff = max(
                DEFAULT_CHALLENGE_MIN_HOLDOFF_SEC,
                max(0.0, DEFAULT_PER_PUBLISHER_COOLDOWN_SEC) * max(1.0, DEFAULT_CHALLENGE_COOLDOWN_MULTIPLIER),
            )
            shared_state[f"penalty_until::{pub}"] = now + float(challenge_holdoff)


def _strip_visible_text(html: str) -> str:
    soup = BeautifulSoup(str(html or ""), "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    return _clean_space(" ".join(soup.stripped_strings))


def _extract_main_like_text(html: str) -> str:
    soup = BeautifulSoup(str(html or ""), "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    for selector in MAIN_HTML_SELECTORS:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if not node:
            continue
        text = _clean_space(" ".join(node.stripped_strings))
        if text:
            return text
    return ""


def collect_page_snapshot(page, title: str = "", html: str = "") -> Dict[str, Any]:
    js_snapshot = {}
    script = r"""
return (() => {
  function clean(value) {
    return String(value || '').replace(/\s+/g, ' ').trim();
  }
  const body = document.body;
  const bodyText = clean(body ? (body.innerText || '') : '');
  const main = document.querySelector('main, article, [role="main"], #main-content, #maincontent, #main_container, #sub_container2, .main-content, .article, .article-content, .article__body');
  const abstractNode = document.querySelector('[class*="abstract"], #abstract, section.abstract, [data-testid*="abstract"]');
  const h1 = document.querySelector('h1');
  const metaKeys = ['citation_doi', 'citation_title', 'citation_pdf_url', 'citation_abstract_html_url', 'citation_fulltext_html_url', 'citation_journal_title', 'citation_author', 'dc.identifier', 'dc.title', 'prism.doi', 'og:type', 'og:url'];
  const meta = {};
  for (const key of metaKeys) {
    const node = document.querySelector(`meta[name="${key}"], meta[property="${key}"]`);
    meta[key] = clean(node ? (node.getAttribute('content') || '') : '');
  }
  return {
    ready_state: document.readyState || '',
    body_child_count: body ? body.children.length : 0,
    body_text_len: bodyText.length,
    body_text_excerpt: bodyText.slice(0, 480),
    main_text_len: clean(main ? (main.innerText || '') : '').length,
    abstract_text_len: clean(abstractNode ? (abstractNode.innerText || '') : '').length,
    h1_text: clean(h1 ? (h1.innerText || '') : ''),
    meta: meta,
    has_main: Boolean(main),
    has_article_tag: Boolean(document.querySelector('article')),
    has_abstract_node: Boolean(abstractNode),
    spinner_count: document.querySelectorAll('[aria-busy="true"], .spinner, .loading, .loader').length,
    iframe_count: document.querySelectorAll('iframe').length,
    canonical_url: clean((document.querySelector('link[rel="canonical"]') || {}).href || ''),
  };
})();
"""
    try:
        js_snapshot = page.run_js(script) or {}
        if not isinstance(js_snapshot, dict):
            js_snapshot = {}
    except Exception:
        js_snapshot = {}

    parsed_text = _strip_visible_text(html)
    parsed_main_text = _extract_main_like_text(html)
    if not js_snapshot.get("body_text_excerpt"):
        js_snapshot["body_text_excerpt"] = parsed_text[:480]
    if not js_snapshot.get("body_text_len"):
        js_snapshot["body_text_len"] = len(parsed_text)
    if (int(js_snapshot.get("main_text_len", 0) or 0) < 120) and parsed_main_text:
        js_snapshot["main_text_len"] = len(parsed_main_text)
        js_snapshot["has_main"] = True
    if "meta" not in js_snapshot or not isinstance(js_snapshot.get("meta"), dict):
        js_snapshot["meta"] = {}
    js_snapshot["parsed_text_excerpt"] = parsed_text[:480]
    js_snapshot["parsed_text_len"] = len(parsed_text)
    js_snapshot["parsed_main_text_len"] = len(parsed_main_text)
    js_snapshot["title"] = _clean_space(title)
    js_snapshot["html_len"] = len(str(html or ""))
    return js_snapshot


def stabilize_page_state(
    page,
    title: str,
    html: str,
    deadline_monotonic: float,
    settle_wait_sec: float = DEFAULT_SETTLE_WAIT_SEC,
    stabilize_polls: int = DEFAULT_STABILIZE_POLLS,
) -> Tuple[str, str, Dict[str, Any]]:
    best_title = str(title or "")
    best_html = str(html or "")
    best_snapshot = collect_page_snapshot(page, title=best_title, html=best_html)

    for _ in range(max(0, int(stabilize_polls))):
        if time.monotonic() + max(0.0, settle_wait_sec) >= deadline_monotonic:
            break
        if (
            int(best_snapshot.get("body_text_len", 0) or 0) >= 220
            or int(best_snapshot.get("main_text_len", 0) or 0) >= 120
        ):
            break
        time.sleep(max(0.0, settle_wait_sec))
        current_title = page.title or best_title
        current_html = page.html or best_html
        current_snapshot = collect_page_snapshot(page, title=current_title, html=current_html)
        score_now = (
            int(best_snapshot.get("body_text_len", 0) or 0)
            + int(best_snapshot.get("main_text_len", 0) or 0)
            + len(best_title.strip())
        )
        score_new = (
            int(current_snapshot.get("body_text_len", 0) or 0)
            + int(current_snapshot.get("main_text_len", 0) or 0)
            + len(str(current_title or "").strip())
        )
        if score_new >= score_now:
            best_title = current_title
            best_html = current_html
            best_snapshot = current_snapshot

    return best_title, best_html, best_snapshot


def _urlish_title(title: str, final_url: str) -> bool:
    clean_title = _clean_space(title).lower()
    if not clean_title:
        return False
    normalized_url = str(final_url or "").strip().lower().replace("https://", "").replace("http://", "")
    if clean_title == normalized_url:
        return True
    return bool(URLISH_TITLE_RE.match(clean_title) and "." in clean_title and "/" in clean_title)


def _domain_matches_expected(domain: str, expected_domains: Sequence[str]) -> bool:
    low_domain = str(domain or "").lower()
    if not expected_domains:
        return True
    for expected in expected_domains:
        candidate = str(expected or "").lower()
        if not candidate:
            continue
        if low_domain == candidate or low_domain.endswith(f".{candidate}") or candidate in low_domain:
            return True
    return False


def _contains_any(blob: str, markers: Sequence[str]) -> bool:
    low_blob = str(blob or "").lower()
    return any(str(marker or "").lower() in low_blob for marker in markers)


def _marker_hits(blob: str, markers: Sequence[str]) -> List[str]:
    low_blob = str(blob or "").lower()
    return [str(marker) for marker in markers if str(marker or "").lower() in low_blob]


def _collect_article_markers(
    doi: str,
    final_url: str,
    title: str,
    html: str,
    snapshot: Dict[str, Any],
    scheduler_publisher: str,
) -> Tuple[List[str], List[str]]:
    markers: List[str] = []
    publisher_markers: List[str] = []
    doi_norm = _normalize_doi(doi)
    low_title = str(title or "").lower()
    low_html = str(html or "").lower()
    low_url = str(final_url or "").lower()
    text_excerpt = str(snapshot.get("body_text_excerpt") or snapshot.get("parsed_text_excerpt") or "").lower()
    meta = {str(k or "").lower(): str(v or "") for k, v in dict(snapshot.get("meta") or {}).items()}

    if meta.get("citation_title") or "citation_title" in low_html:
        markers.append("citation_title")
    if meta.get("citation_doi") or meta.get("prism.doi") or "citation_doi" in low_html:
        markers.append("citation_doi")
    if meta.get("citation_journal_title") or "citation_journal_title" in low_html:
        markers.append("journal_meta")
    if "\"@type\":\"scholarlyarticle\"" in low_html or "schema.org/scholarlyarticle" in low_html:
        markers.append("schema_article")
    if bool(snapshot.get("has_abstract_node")) or "citation_abstract" in low_html:
        markers.append("abstract_node")
    if bool(snapshot.get("has_main")) or bool(snapshot.get("has_article_tag")):
        markers.append("article_container")
    if int(snapshot.get("main_text_len", 0) or 0) >= 120:
        markers.append("main_text")
    if int(snapshot.get("abstract_text_len", 0) or 0) >= 60:
        markers.append("abstract_text")
    if doi_norm and (doi_norm in low_html or doi_norm in text_excerpt or doi_norm in low_url):
        markers.append("doi_match")
    if len(_clean_space(title)) >= 20 and not _urlish_title(title, final_url):
        markers.append("title_populated")

    for marker in PUBLISHER_HTML_MARKERS.get(str(scheduler_publisher or ""), ()):
        if marker.lower() in low_html:
            publisher_markers.append(marker)

    return sorted(set(markers)), sorted(set(publisher_markers))


def _has_active_challenge(
    final_url: str,
    title: str,
    html: str,
    visible_text: str,
    article_payload_evidence: bool,
) -> bool:
    low_url = str(final_url or "").lower()
    low_title = _clean_space(title).lower()
    low_text = str(visible_text or "").lower()
    low_html = str(html or "").lower()[:25000]
    title_hits = _marker_hits(low_title, CHALLENGE_TEXT_MARKERS)
    text_hits = _marker_hits(low_text, CHALLENGE_TEXT_MARKERS)
    html_hits = _marker_hits(low_html, CHALLENGE_HTML_MARKERS)

    if _contains_any(low_url, CHALLENGE_URL_MARKERS):
        return True
    if title_hits:
        return True
    if len(text_hits) >= 2:
        return True
    if text_hits and not article_payload_evidence:
        return True
    if html_hits and not article_payload_evidence:
        return True
    if _contains_any(low_url, SOFT_CHALLENGE_VENDOR_MARKERS):
        return True
    if _contains_any(low_html, SOFT_CHALLENGE_VENDOR_MARKERS):
        return (not article_payload_evidence) and bool(title_hits or text_hits)
    return False


def _has_blocking_access_gate(
    title: str,
    visible_text: str,
    strong_article_evidence: bool,
    main_text_len: int,
) -> bool:
    visible_blob = " ".join([_clean_space(title).lower(), str(visible_text or "").lower()])
    login_hits = _marker_hits(visible_blob, ACCESS_GATE_LOGIN_MARKERS)
    paywall_hits = _marker_hits(visible_blob, ACCESS_GATE_PAYWALL_MARKERS)

    if paywall_hits and (not strong_article_evidence):
        return True
    if login_hits and paywall_hits:
        return not strong_article_evidence
    if login_hits and main_text_len < 120 and not strong_article_evidence:
        return True
    return False


def _has_broken_js_shell(
    title: str,
    html: str,
    visible_text: str,
    main_text_len: int,
    body_text_len: int,
) -> bool:
    low_title = _clean_space(title).lower()
    low_text = str(visible_text or "").lower()
    low_html = str(html or "").lower()
    title_hit = any(marker in low_title for marker in BROKEN_JS_SHELL_TITLE_MARKERS)
    text_hit = any(marker in low_text for marker in BROKEN_JS_SHELL_TEXT_MARKERS)
    html_hit = any(marker in low_html for marker in BROKEN_JS_SHELL_HTML_MARKERS)

    if title_hit:
        return True
    if text_hit and body_text_len < 6000:
        return True
    if html_hit and main_text_len < 80 and 180 <= body_text_len < 4000:
        return True
    return False


def _classify_non_success(
    final_url: str,
    domain: str,
    title: str,
    html: str,
    snapshot: Dict[str, Any],
    expected_domains: Sequence[str],
    reason_codes: List[str],
    strong_article_evidence: bool,
    article_payload_evidence: bool,
    expected_domain_match: bool,
) -> str:
    low_url = str(final_url or "").lower()
    low_domain = str(domain or "").lower()
    low_title = _clean_space(title).lower()
    text = str(snapshot.get("body_text_excerpt") or snapshot.get("parsed_text_excerpt") or "").lower()
    body_text_len = int(snapshot.get("body_text_len", 0) or 0)
    main_text_len = int(snapshot.get("main_text_len", 0) or 0)
    spinner_count = int(snapshot.get("spinner_count", 0) or 0)
    body_child_count = int(snapshot.get("body_child_count", 0) or 0)
    iframe_count = int(snapshot.get("iframe_count", 0) or 0)
    ready_state = str(snapshot.get("ready_state") or "").lower()
    html_len = int(snapshot.get("html_len", 0) or 0)
    urlish_title = _urlish_title(title, final_url)
    structured_body_candidate = body_text_len >= 1200 and bool(article_payload_evidence or strong_article_evidence)

    if _has_active_challenge(
        final_url=final_url,
        title=title,
        html=html,
        visible_text=text,
        article_payload_evidence=article_payload_evidence,
    ):
        reason_codes.append("challenge_marker")
        return STATE_CHALLENGE_DETECTED

    error_blob = " ".join([low_title, text])
    if any(marker in error_blob for marker in ERROR_MARKERS):
        reason_codes.append("publisher_error_marker")
        return STATE_PUBLISHER_ERROR

    if _has_broken_js_shell(
        title=title,
        html=html,
        visible_text=text,
        main_text_len=main_text_len,
        body_text_len=body_text_len,
    ):
        reason_codes.append("broken_js_shell_marker")
        return STATE_BROKEN_JS_SHELL

    interstitial_blob = " ".join([low_url, low_title, text, str(html or "").lower()])
    if _has_blocking_access_gate(
        title=title,
        visible_text=text,
        strong_article_evidence=strong_article_evidence,
        main_text_len=main_text_len,
    ):
        reason_codes.append("access_gate_marker")
        return STATE_CONSENT_OR_INTERSTITIAL_BLOCK
    if any(marker in interstitial_blob for marker in CONSENT_MARKERS) and main_text_len < 120:
        reason_codes.append("consent_overlay_marker")
        return STATE_CONSENT_OR_INTERSTITIAL_BLOCK
    if low_domain.endswith("doi.org") or low_title in INTERSTITIAL_TITLES:
        reason_codes.append("redirect_or_doi_domain")
        return STATE_CONSENT_OR_INTERSTITIAL_BLOCK
    if "linkinghub.elsevier.com/retrieve/" in low_url:
        reason_codes.append("elsevier_retrieve_interstitial")
        return STATE_CONSENT_OR_INTERSTITIAL_BLOCK
    if any(marker in interstitial_blob for marker in INTERSTITIAL_MARKERS) and main_text_len < 140:
        reason_codes.append("interstitial_marker")
        return STATE_CONSENT_OR_INTERSTITIAL_BLOCK

    if expected_domains and (not expected_domain_match) and article_payload_evidence:
        reason_codes.append("domain_mismatch_article_like")
        return STATE_DOMAIN_MISMATCH

    blank_signals = 0
    if not low_domain or low_url.startswith("about:blank") or low_url.startswith("chrome://"):
        blank_signals += 2
        reason_codes.append("no_final_domain")
    if body_text_len < 80:
        blank_signals += 1
        reason_codes.append("low_visible_text")
    if main_text_len < 40 and not structured_body_candidate:
        blank_signals += 1
        reason_codes.append("low_main_text")
    if body_child_count <= 2 and not structured_body_candidate:
        blank_signals += 1
        reason_codes.append("minimal_body_children")
    if spinner_count > 0 and body_text_len < 180:
        blank_signals += 1
        reason_codes.append("spinner_without_content")
    if ready_state and ready_state != "complete" and body_text_len < 200:
        blank_signals += 1
        reason_codes.append("incomplete_ready_state")
    if html_len < 400:
        blank_signals += 1
        reason_codes.append("tiny_html")
    if urlish_title:
        blank_signals += 1
        reason_codes.append("urlish_title")
    if iframe_count > 0 and body_text_len < 100:
        blank_signals += 1
        reason_codes.append("empty_iframe_shell")
    if blank_signals >= 2:
        return STATE_BLANK_OR_INCOMPLETE

    if expected_domains and (not _domain_matches_expected(low_domain, expected_domains)):
        reason_codes.append("expected_domain_mismatch")
    return STATE_UNKNOWN_NON_SUCCESS


def classify_landing(
    doi: str,
    input_publisher: str,
    scheduler_publisher: str,
    final_url: str,
    title: str,
    html: str,
    snapshot: Dict[str, Any],
    issue: str = "",
    issue_evidence: Sequence[str] = (),
    exception_kind: str = "",
    expected_domains: Sequence[str] = (),
) -> Dict[str, Any]:
    reason_codes: List[str] = []
    domain = _extract_domain(final_url)
    meta = dict(snapshot.get("meta") or {})
    article_markers, publisher_markers = _collect_article_markers(
        doi=doi,
        final_url=final_url,
        title=title,
        html=html,
        snapshot=snapshot,
        scheduler_publisher=scheduler_publisher,
    )

    if exception_kind == "timeout":
        return {
            "classifier_state": STATE_TIMEOUT,
            "reason_codes": ["navigation_timeout"],
            "domain": domain,
            "expected_domains": list(expected_domains),
            "article_markers": article_markers,
            "publisher_markers": publisher_markers,
            "signal_summary": {
                "doi_present": "doi_match" in article_markers,
                "expected_domain_match": _domain_matches_expected(domain, expected_domains),
                "title_urlish": _urlish_title(title, final_url),
                "meta_keys": sorted(k for k, v in meta.items() if str(v or "").strip()),
                "body_text_len": int(snapshot.get("body_text_len", 0) or 0),
                "main_text_len": int(snapshot.get("main_text_len", 0) or 0),
            },
        }
    if exception_kind == "network":
        return {
            "classifier_state": STATE_NETWORK_ERROR,
            "reason_codes": ["navigation_network_error"],
            "domain": domain,
            "expected_domains": list(expected_domains),
            "article_markers": article_markers,
            "publisher_markers": publisher_markers,
            "signal_summary": {
                "doi_present": "doi_match" in article_markers,
                "expected_domain_match": _domain_matches_expected(domain, expected_domains),
                "title_urlish": _urlish_title(title, final_url),
                "meta_keys": sorted(k for k, v in meta.items() if str(v or "").strip()),
                "body_text_len": int(snapshot.get("body_text_len", 0) or 0),
                "main_text_len": int(snapshot.get("main_text_len", 0) or 0),
            },
        }

    expected_domain_match = _domain_matches_expected(domain, expected_domains)
    title_urlish = _urlish_title(title, final_url)
    body_text_len = int(snapshot.get("body_text_len", 0) or 0)
    main_text_len = int(snapshot.get("main_text_len", 0) or 0)
    abstract_text_len = int(snapshot.get("abstract_text_len", 0) or 0)
    title_clean = _clean_space(title)
    meta_keys = sorted(k for k, v in meta.items() if str(v or "").strip())
    doi_present = "doi_match" in article_markers
    has_strong_meta = any(k in meta_keys for k in ("citation_doi", "citation_title", "prism.doi", "citation_journal_title"))
    has_structured_article = any(
        marker in article_markers
        for marker in ("citation_title", "citation_doi", "schema_article", "abstract_node", "journal_meta", "main_text")
    )
    content_populated = body_text_len >= 220 or main_text_len >= 120
    title_populated = len(title_clean) >= 18 and not title_urlish
    article_body_evidence = (
        main_text_len >= 120
        or abstract_text_len >= 60
        or (
            body_text_len >= 1500
            and doi_present
            and has_strong_meta
            and title_populated
            and (publisher_markers or has_structured_article)
        )
    )
    article_payload_evidence = bool(
        content_populated
        and doi_present
        and article_body_evidence
        and (has_strong_meta or title_populated)
        and (publisher_markers or has_structured_article)
    )
    strong_article_evidence = bool(expected_domain_match and article_payload_evidence)
    signal_summary = {
        "doi_present": doi_present,
        "expected_domain_match": expected_domain_match,
        "title_urlish": title_urlish,
        "meta_keys": meta_keys,
        "body_text_len": body_text_len,
        "main_text_len": main_text_len,
        "article_payload_evidence": article_payload_evidence,
        "strong_article_evidence": strong_article_evidence,
    }

    if issue:
        state = ""
        issue_blob = " ".join(str(ev or "").lower() for ev in (issue_evidence or []))
        if issue == "FAIL_CAPTCHA":
            reason_codes.append("fail_captcha")
            state = STATE_CHALLENGE_DETECTED
        elif issue == "FAIL_ACCESS_RIGHTS":
            reason_codes.append("access_rights_gate")
            state = STATE_CONSENT_OR_INTERSTITIAL_BLOCK
        elif issue == "FAIL_BLOCK":
            if any(
                token in issue_blob
                for token in (
                    "challenge",
                    "captcha",
                    "too many requests",
                    "url_marker=",
                    "http_status=429",
                    "verify you are human",
                    "unusual traffic",
                    "access denied",
                    "security check",
                )
            ):
                reason_codes.append("fail_block_bot_signal")
                state = STATE_CHALLENGE_DETECTED
            elif ("bot_like" in issue_blob) and (not article_payload_evidence):
                reason_codes.append("fail_block_bot_signal")
                state = STATE_CHALLENGE_DETECTED
            elif any(
                token in issue_blob
                for token in ("access_gate_soft", "policy=assume_institution_access", "access_rights", "institution")
            ):
                reason_codes.append("soft_access_gate")
                state = STATE_CONSENT_OR_INTERSTITIAL_BLOCK
        reason_codes.extend(str(ev or "") for ev in (issue_evidence or [])[:5] if str(ev or "").strip())
        if state:
            return {
                "classifier_state": state,
                "reason_codes": sorted(set(reason_codes)),
                "domain": domain,
                "expected_domains": list(expected_domains),
                "article_markers": article_markers,
                "publisher_markers": publisher_markers,
                "signal_summary": signal_summary,
            }

    if domain:
        reason_codes.append("expected_domain_match" if expected_domain_match else "expected_domain_mismatch")
    if has_strong_meta:
        reason_codes.append("strong_meta_present")
    if doi_present:
        reason_codes.append("doi_match")
    if publisher_markers:
        reason_codes.append("publisher_marker_present")
    if content_populated:
        reason_codes.append("content_populated")

    non_success_state = _classify_non_success(
        final_url=final_url,
        domain=domain,
        title=title,
        html=html,
        snapshot=snapshot,
        expected_domains=expected_domains,
        reason_codes=reason_codes,
        strong_article_evidence=strong_article_evidence,
        article_payload_evidence=article_payload_evidence,
        expected_domain_match=expected_domain_match,
    )
    if non_success_state != STATE_UNKNOWN_NON_SUCCESS:
        return {
            "classifier_state": non_success_state,
            "reason_codes": sorted(set(reason_codes)),
            "domain": domain,
            "expected_domains": list(expected_domains),
            "article_markers": article_markers,
            "publisher_markers": publisher_markers,
            "signal_summary": signal_summary,
        }

    success_conditions = [
        bool(domain) and not domain.endswith("doi.org"),
        expected_domain_match,
        content_populated,
        title_populated or has_strong_meta,
        has_structured_article or (doi_present and publisher_markers) or (doi_present and has_strong_meta),
    ]
    if all(success_conditions):
        return {
            "classifier_state": STATE_SUCCESS_LANDING,
            "reason_codes": sorted(set(reason_codes)),
            "domain": domain,
            "expected_domains": list(expected_domains),
            "article_markers": article_markers,
            "publisher_markers": publisher_markers,
            "signal_summary": signal_summary,
        }

    reason_codes.append("insufficient_article_signals")
    return {
        "classifier_state": STATE_UNKNOWN_NON_SUCCESS,
        "reason_codes": sorted(set(reason_codes)),
        "domain": domain,
        "expected_domains": list(expected_domains),
        "article_markers": article_markers,
        "publisher_markers": publisher_markers,
        "signal_summary": signal_summary,
    }


def summarize_classifier_states(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    classifier_counts = Counter(str(rec.get("classifier_state") or "") for rec in records)
    reason_counts = Counter()
    by_publisher: Dict[str, Counter] = defaultdict(Counter)
    reclassified = []

    for rec in records:
        publisher = str(rec.get("scheduler_publisher") or "unknown")
        state = str(rec.get("classifier_state") or "")
        by_publisher[publisher][state] += 1
        for code in rec.get("reason_codes", []) or []:
            reason_counts[str(code)] += 1
        if rec.get("legacy_success_like") and state != STATE_SUCCESS_LANDING:
            reclassified.append(
                {
                    "doi": rec.get("doi"),
                    "publisher": rec.get("input_publisher") or publisher,
                    "final_url": rec.get("resolved_url"),
                    "title": rec.get("title"),
                    "classifier_state": state,
                    "reason_codes": rec.get("reason_codes", [])[:6],
                }
            )

    publisher_rows = []
    for publisher, counts in by_publisher.items():
        total = sum(counts.values())
        publisher_rows.append(
            {
                "publisher": publisher,
                "sample_size": total,
                "counts": dict(counts),
            }
        )
    publisher_rows.sort(key=lambda row: (-row["sample_size"], row["publisher"]))

    return {
        "classifier_counts": dict(classifier_counts),
        "top_reason_codes": reason_counts.most_common(15),
        "publisher_breakdown": publisher_rows,
        "legacy_success_like_count": sum(1 for rec in records if rec.get("legacy_success_like")),
        "legacy_reclassified_non_success": reclassified[:12],
    }


def render_experiment_markdown(report: Dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    classifier_counts = dict(summary.get("classifier_counts") or {})
    reason_rows = list(summary.get("top_reason_codes") or [])
    publishers = [str(item) for item in (report.get("publishers_covered") or []) if str(item).strip()]
    if not publishers:
        publishers = [row.get("publisher") for row in summary.get("publisher_breakdown", []) if row.get("publisher")]

    remaining = list(report.get("remaining_weak_spots") or [])
    if not remaining:
        if classifier_counts.get(STATE_UNKNOWN_NON_SUCCESS):
            remaining.append("`unknown_non_success` remains when a publisher page loads but exposes too little article metadata to prove a valid landing.")
        if classifier_counts.get(STATE_CONSENT_OR_INTERSTITIAL_BLOCK):
            remaining.append("Consent/login/interstitial handling remains conservative; pages that still block article metadata are intentionally counted as non-success.")
        if classifier_counts.get(STATE_BROKEN_JS_SHELL):
            remaining.append("Broken JS shells are now split out from consent/interstitial blocks; they still count as non-success until the publisher renders usable article content.")
        if classifier_counts.get(STATE_DOMAIN_MISMATCH):
            remaining.append("Domain mismatch now stays separate from unknown failures so publisher-metadata inconsistencies can be tracked without relaxing success rules.")
        if classifier_counts.get(STATE_CHALLENGE_DETECTED):
            remaining.append("Challenge pages are detected and stopped, but no anti-bot bypass is implemented by design.")

    lines = [
        "# Landing Experiment Summary",
        "",
        f"- Sample size: {report.get('sample_size', 0)}",
        f"- Publishers covered: {', '.join(publishers) if publishers else 'n/a'}",
        f"- Legacy success-like count: {summary.get('legacy_success_like_count', 0)}",
        f"- Legacy reclassified as non-success: {len(summary.get('legacy_reclassified_non_success', []))}",
        "",
        "## Counts by Classifier State",
    ]
    for key in sorted(classifier_counts):
        lines.append(f"- {key}: {classifier_counts[key]}")

    lines.extend(["", "## Representative Failure Reasons"])
    if reason_rows:
        for key, count in reason_rows[:8]:
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")

    lines.extend(["", "## Remaining Weak Spots"])
    if remaining:
        for item in remaining:
            lines.append(f"- {item}")
    else:
        lines.append("- none observed in this sample")

    return "\n".join(lines) + "\n"


def suggest_remaining_weak_spots(summary: Dict[str, Any]) -> List[str]:
    counts = dict(summary.get("classifier_counts") or {})
    weak_spots: List[str] = []
    if counts.get(STATE_UNKNOWN_NON_SUCCESS):
        weak_spots.append(
            "`unknown_non_success` still appears on pages that render some content but do not expose stable DOI/article metadata."
        )
    if counts.get(STATE_BROKEN_JS_SHELL):
        weak_spots.append(
            "A few pages fail before article content renders because the publisher JS shell breaks; these are kept separate from consent/interstitial blocks."
        )
    if counts.get(STATE_DOMAIN_MISMATCH):
        weak_spots.append(
            "Some article-like pages still land on a domain family that disagrees with the expected publisher metadata; these are tracked separately instead of being folded into success."
        )
    if counts.get(STATE_BLANK_OR_INCOMPLETE):
        weak_spots.append(
            "A few publisher shells still finish with too little visible text; the classifier now rejects them, but the browser-side recovery options stay intentionally conservative."
        )
    if counts.get(STATE_CHALLENGE_DETECTED):
        weak_spots.append(
            "Challenge pages remain a hard stop. [blocked] No CAPTCHA, Turnstile, or Cloudflare bypass is implemented."
        )
    return weak_spots


def compact_text_signature(snapshot: Dict[str, Any]) -> str:
    excerpt = _clean_space(snapshot.get("body_text_excerpt") or snapshot.get("parsed_text_excerpt") or "")
    if not excerpt:
        return ""
    return excerpt[:240]

