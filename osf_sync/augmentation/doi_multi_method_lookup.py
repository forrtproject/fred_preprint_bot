from __future__ import annotations
from osf_sync.dynamo.preprints_repo import PreprintsRepo
from osf_sync.dynamo.api_cache_repo import ApiCacheRepo
from osf_sync.augmentation.matching_crossref import (
    _safe_get_issued_year as _cr_safe_get_issued_year,
    _query_crossref,
    _query_crossref_biblio,
)
from osf_sync.augmentation.doi_check_openalex import (
    OPENALEX_MAILTO,
    _fetch_candidates,
    _norm,
    _safe_publication_year as _oa_safe_publication_year,
)

import argparse
import csv
import logging
import os
import re
import sys
import time
import html
import random
from typing import Any, Dict, Iterator, List, Optional, Tuple
import functools
import json
from pathlib import Path

import requests
from dotenv import load_dotenv

try:
    from rapidfuzz import fuzz as rf_fuzz
except Exception:
    rf_fuzz = None

load_dotenv()


# Ensure UTF-8 stdout to avoid Windows cp1252 logging issues.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
logger.setLevel(logging.INFO)

# Default cache TTL: one week.
DOI_MULTI_METHOD_CACHE_TTL_SECS = float(os.environ.get(
    "DOI_MULTI_METHOD_CACHE_TTL_SECS", 7 * 24 * 3600))
YEAR_MAX_DIFF = 1
TOP_N_PER_STRATEGY = int(os.environ.get("DOI_MULTI_METHOD_TOP_N", 5))
STRUCTURED_THRESHOLD_DEFAULT = float(os.environ.get(
    "DOI_MULTI_METHOD_STRUCTURED_THRESHOLD", 70.0))
UNPARSED_THRESHOLD_DEFAULT = float(os.environ.get(
    "DOI_MULTI_METHOD_UNPARSED_THRESHOLD", 60.0))
TITLE_FUZZ_THRESHOLD = float(os.environ.get(
    "DOI_MULTI_METHOD_TITLE_FUZZ_THRESHOLD", 90.0))
PARSED_WEIGHTS = {
    "title": 0.6,
    "year": 0.2,
    "journal": 0.1,
    "authors": 0.1,
}
METADATA_PRIORITY = ["crossref_title", "crossref_raw", "openalex_title"]


_api_cache = ApiCacheRepo()
_mem_cache: Dict[str, Any] = {}  # in-memory L1 cache for the current run
_MEM_CACHE_SENTINEL = object()  # distinguishes cached None from cache miss
MOJIBAKE_MARKERS = (
    "Ã",
    "Â",
    "â",
    "ï¿½",
    "�",
)
MOJIBAKE_REPLACEMENTS = {
    "\xa0": " ",
    "Â ": " ",
    "â": "–",
    "â": "—",
    "â": "‐",
    "â": "’",
    "â": "‘",
    "â": "“",
    "â": "”",
    "â¦": "…",
}
TRANSCODE_PRENORMALIZE = str.maketrans(
    {
        "’": "'",
        "‘": "'",
        "“": '"',
        "”": '"',
    }
)


def _cache_key(prefix: str, payload: Any) -> str:
    """
    Build a stable cache key from a JSON-ish payload.
    """
    try:
        blob = json.dumps(payload, ensure_ascii=True,
                          sort_keys=True, default=str)
    except Exception:
        blob = repr(payload)
    return f"{prefix}::{blob}"


def _cache_get(key: str) -> Tuple[bool, Optional[Any]]:
    # L1: in-memory
    mem_val = _mem_cache.get(key, _MEM_CACHE_SENTINEL)
    if mem_val is not _MEM_CACHE_SENTINEL:
        return True, mem_val
    # L2: DynamoDB
    item = _api_cache.get(key)
    if not item or not _api_cache.is_fresh(item, ttl_seconds=int(DOI_MULTI_METHOD_CACHE_TTL_SECS)):
        return False, None
    payload = item.get("payload")
    if isinstance(payload, dict) and payload.get("_none") is True:
        _mem_cache[key] = None
        return True, None
    _mem_cache[key] = payload
    return True, payload


def _cache_set(key: str, value: Optional[Any]) -> None:
    _mem_cache[key] = value
    payload = {"_none": True} if value is None else value
    _api_cache.put(
        key,
        payload,
        source="doi_multi_method",
        ttl_seconds=int(DOI_MULTI_METHOD_CACHE_TTL_SECS),
        status=value is not None,
    )


def _cached(key: str, fn, *args, **kwargs):
    found, hit = _cache_get(key)
    if found:
        return hit
    val = fn(*args, **kwargs)
    _cache_set(key, val)
    return val


def _decode_response_text(resp: requests.Response) -> str:
    raw = resp.content or b""
    if not raw:
        return ""
    for enc in ("utf-8", (resp.encoding or "").strip(), getattr(resp, "apparent_encoding", "")):
        if not enc:
            continue
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("latin-1", errors="replace")


def _repair_common_mojibake(text: str) -> str:
    s = " ".join(str(html.unescape(text or "")).split()).strip()
    if not s:
        return s

    def _apply_replacements(candidate: str) -> str:
        out = candidate
        for bad, good in MOJIBAKE_REPLACEMENTS.items():
            out = out.replace(bad, good)
        return " ".join(out.split()).strip()

    def _score(candidate: str) -> int:
        score = 0
        for ch in candidate:
            code = ord(ch)
            if code == 0xFFFD:
                score += 12
            elif 0x80 <= code <= 0x9F:
                score += 8
        for marker in MOJIBAKE_MARKERS:
            score += candidate.count(marker) * 3
        return score

    base_clean = _apply_replacements(s)
    candidates = {base_clean}
    frontier = [s, base_clean] if base_clean != s else [s]
    for _ in range(2):
        next_frontier: List[str] = []
        for base in frontier:
            for source in {base, base.translate(TRANSCODE_PRENORMALIZE)}:
                for enc in ("latin-1", "cp1252"):
                    try:
                        fixed = " ".join(source.encode(enc).decode("utf-8").split()).strip()
                    except Exception:
                        continue
                    fixed = _apply_replacements(fixed)
                    if fixed and fixed not in candidates:
                        candidates.add(fixed)
                        next_frontier.append(fixed)
        if not next_frontier:
            break
        frontier = next_frontier

    ranked = sorted(candidates, key=lambda c: (_score(c), -len(c)))
    return ranked[0] if ranked else s


def _mojibake_penalty(text: str) -> int:
    if not text:
        return 0
    score = 0
    for ch in text:
        code = ord(ch)
        if code == 0xFFFD:
            score += 12
        elif 0x80 <= code <= 0x9F:
            score += 8
    for marker in MOJIBAKE_MARKERS:
        score += text.count(marker) * 3
    return score


METHOD_PRIORITY = ["crossref_raw", "crossref_title", "openalex_title"]
CSV_FIELDS = [
    "raw_citation",
    "title",
    "raw_citation_doi",
    "raw_citation_score",
    "raw_citation_rank_source",
    "raw_citation_rank_score",
    "column_exists_raw_citation",
    "title_doi",
    "title_score",
    "title_rank_source",
    "title_rank_score",
    "openalex_doi",
    "openalex_score",
    "openalex_rank_source",
    "openalex_rank_score",
    "crossref_citation",
    "crossref_title_citation",
    "openalex_raw_citation",
    "crossref_citation_string_distance",
    "crossref_title_citation_string_distance",
    "openalex_raw_citation_string_distance",
    "final_citation_string_distance",
    "final_method",
    "final_candidate_rank",
    "final_candidate_rank_score",
    "final_score",
    "status",
    "conflict_reason",
]
LOG_EVERY = 100


def _iter_all_refs(
    repo: PreprintsRepo,
    limit: int,
    ref_id: Optional[str],
    include_existing: bool,
) -> Iterator[Dict[str, Any]]:
    """
    Yield references from Dynamo across the entire preprint_references table.
    Applies ref_id and include_existing filtering while respecting the limit.
    """
    emitted = 0
    last_key = None
    while emitted < limit:
        remaining = limit - emitted
        if remaining <= 0:
            break
        kwargs: Dict[str, Any] = {"Limit": min(1000, remaining)}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_refs.scan(**kwargs)
        chunk = resp.get("Items", []) or []
        for it in chunk:
            if not it:
                continue
            if ref_id and it.get("ref_id") != ref_id:
                continue
            if not include_existing:
                doi_val = (it.get("doi") or "").strip()
                if doi_val:
                    continue
            yield it
            emitted += 1
            if emitted >= limit:
                break
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break


def _maybe_log_progress(processed: int, log_every: int, out_file) -> None:
    if log_every and processed % log_every == 0:
        logger.info("Processed %s references", processed)
        try:
            out_file.flush()
        except Exception:
            pass


def _normalize_doi(doi: Optional[str], *, source: str = "text") -> Optional[str]:
    if not doi:
        return None
    d = html.unescape(str(doi)).strip().lower()
    if not d:
        return None
    d = d.replace("\u200b", "").replace("\ufeff", "")
    for pref in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "dx.doi.org/",
        "doi.org/",
        "doi:",
    ):
        if d.startswith(pref):
            d = d[len(pref):]
            break
    first_doi = d.find("10.")
    if first_doi == -1:
        return None
    d = d[first_doi:]
    m = _DOI_RE.search(d)
    if m:
        d = m.group(0)
    d = d.split("?", 1)[0].split("#", 1)[0].strip()
    d = d.strip(" \t\r\n\"'<>[]{}")
    if source != "api":
        d = d.strip(".,;:")
        while d.endswith(")") and d.count("(") < d.count(")"):
            d = d[:-1].rstrip()
    if not d:
        return None
    if not d.startswith("10."):
        return None
    if not _DOI_RE.fullmatch(d):
        return None
    return d


def normalize_doi(doi: Optional[str], *, source: str = "text") -> Optional[str]:
    """Public DOI normalizer for ingestion/update code paths."""
    return _normalize_doi(doi, source=source)


def doi_resolves(doi: str, timeout: int = 10) -> Optional[bool]:
    """Check whether a DOI resolves via the doi.org Handle API.

    Returns True if the handle exists and has a URL, False if the handle is
    not found or has no URL, and None on network/parse errors.
    """
    doi = _normalize_doi(doi)
    if not doi or not doi.startswith("10."):
        return False

    key = _cache_key("doi_resolve", {"doi": doi})
    found, hit = _cache_get(key)
    if found:
        return hit

    import urllib.parse
    url = f"https://doi.org/api/handles/{urllib.parse.quote(doi, safe='')}?type=URL"
    try:
        r = requests.get(url, timeout=timeout)
    except Exception:
        return None

    try:
        body = r.json()
    except Exception:
        return None

    code = body.get("responseCode")
    if code is None:
        return None

    if code == 1:
        result: Optional[bool] = True
    elif code in (100, 200):
        result = False
    elif code == 2:
        result = None
    else:
        result = None

    # Only cache definitive results (True/False), not transient errors (None)
    if result is not None:
        _cache_set(key, result)

    return result


def _ascii_sanitize(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    try:
        return val.encode("ascii", "ignore").decode("ascii")
    except Exception:
        return val


_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_DOI_RE = re.compile(r"10\.[0-9]{4,9}/\S+", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(18|19|20)\d{2}\b")
_TAG_RE = re.compile(r"<[^>]+>")
_RAW_SCREEN_RE = re.compile(
    r"\b(18|19|20)\d{2}[a-z]?\b|\bn\.\s*d\.($|[^A-Za-z])|\bunder review\b|\bin press\b|\bin preparation\b|\baccepted in principle\b|\baccepted\b",
    re.IGNORECASE,
)


def _strip_urls(val: Optional[str]) -> Optional[str]:
    """Remove URLs from a string while preserving surrounding text/spacing."""
    if val is None:
        return None
    cleaned = _URL_RE.sub("", val)
    return " ".join(cleaned.split()).strip()


def _clean_title_text(text: str) -> str:
    if not text:
        return ""
    unescaped = html.unescape(text)
    cleaned = _TAG_RE.sub(" ", unescaped)
    return " ".join(cleaned.split()).strip()


def _levenshtein_distance(a: str, b: str) -> int:
    """Simple Levenshtein distance for modest-length citation strings."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    # Ensure a is the shorter string to minimize memory
    if len(a) > len(b):
        a, b = b, a
    previous = list(range(len(a) + 1))
    for i, ch_b in enumerate(b, 1):
        current = [i]
        for j, ch_a in enumerate(a, 1):
            cost = 0 if ch_a == ch_b else 1
            current.append(
                min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost))
        previous = current
    return previous[-1]


def _prep_for_distance(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    cleaned = _strip_urls(_ascii_sanitize(val))
    if cleaned is None:
        return None
    return cleaned.lower()


def _string_distance(a: Optional[str], b: Optional[str]) -> Optional[int]:
    """Compute Levenshtein distance between two citations after cleaning."""
    pa = _prep_for_distance(a)
    pb = _prep_for_distance(b)
    if pa is None or pb is None:
        return None
    return _levenshtein_distance(pa, pb)


def _normalized_string_distance(a: Optional[str], b: Optional[str]) -> float:
    """Normalized distance in [0,1]; 1.0 means maximally different or missing."""
    pa = _prep_for_distance(a)
    pb = _prep_for_distance(b)
    if not pa or not pb:
        return 1.0
    max_len = max(len(pa), len(pb))
    if max_len == 0:
        return 0.0
    return min(1.0, _levenshtein_distance(pa, pb) / max_len)


def _raw_citation_has_year(raw: str) -> bool:
    """Check if a raw citation string appears to include a 4-digit year."""
    if not raw:
        return False
    return bool(_YEAR_RE.search(raw))


def _raw_citation_passes_screen(raw: str) -> bool:
    if not raw:
        return False
    cleaned = _strip_urls(raw)
    return bool(_RAW_SCREEN_RE.search(cleaned or ""))


def _extract_year_from_raw(raw: str) -> Optional[int]:
    """Extract the last 4-digit year from a raw citation string."""
    if not raw:
        return None
    for match in re.finditer(r"\((19|20)\d{2}\)", raw):
        try:
            return int(match.group(0).strip("()"))
        except Exception:
            break
    last_year = None
    for match in _YEAR_RE.finditer(raw):
        last_year = match.group(0)
    if not last_year:
        return None
    try:
        return int(last_year)
    except Exception:
        return None


def _tokenize_simple(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())



def _subset_inflation_penalty_core(
    ref_text: str, cand_text: str, min_token_set_ratio: float
) -> Tuple[float, Dict[str, Any]]:
    if rf_fuzz is None or not ref_text or not cand_text:
        return 0.0, {"reason": "disabled_or_empty"}

    try:
        token_set_ratio = rf_fuzz.token_set_ratio(ref_text, cand_text)
        if token_set_ratio < min_token_set_ratio:
            return 0.0, {"token_set_ratio": token_set_ratio, "reason": "low_token_set"}
        token_sort_ratio = rf_fuzz.token_sort_ratio(ref_text, cand_text)
    except Exception:
        return 0.0, {"reason": "fuzz_error"}

    ref_tokens = set(_tokenize_simple(ref_text))
    cand_tokens = set(_tokenize_simple(cand_text))
    if not ref_tokens or not cand_tokens:
        return 0.0, {"reason": "no_tokens"}

    ref_subset = ref_tokens < cand_tokens
    cand_subset = cand_tokens < ref_tokens
    size_ratio = min(len(ref_tokens), len(cand_tokens)) / \
        max(len(ref_tokens), len(cand_tokens))
    penalty = 0.0

    if ref_subset and size_ratio < 0.80 and token_sort_ratio < 95:
        penalty += min(18.0, (0.80 - size_ratio) * 60.0)

    if cand_subset and size_ratio < 0.70 and token_sort_ratio < 92:
        penalty += min(12.0, (0.70 - size_ratio) * 50.0)

    GENERIC = {
        "proceedings",
        "conference",
        "symposium",
        "workshop",
        "meeting",
        "international",
        "annual",
        "edition",
        "volume",
        "vol",
        "series",
        "lecture",
        "notes",
        "springer",
        "acm",
        "ieee",
    }
    extra_tokens = []
    if ref_subset:
        extra_tokens = list(cand_tokens - ref_tokens)
    elif cand_subset:
        extra_tokens = list(ref_tokens - cand_tokens)

    generic_extra = [t for t in extra_tokens if t in GENERIC]
    if extra_tokens:
        generic_ratio = len(generic_extra) / max(1, len(extra_tokens))
        if len(extra_tokens) >= 3 and generic_ratio >= 0.40:
            penalty += 6.0
    else:
        generic_ratio = 0.0

    meta = {
        "token_set_ratio": token_set_ratio,
        "token_sort_ratio": token_sort_ratio,
        "ref_tokens": len(ref_tokens),
        "cand_tokens": len(cand_tokens),
        "size_ratio": round(size_ratio, 3),
        "ref_subset": ref_subset,
        "cand_subset": cand_subset,
        "extra_tokens": sorted(extra_tokens)[:12],
        "generic_extra_ratio": round(generic_ratio, 3),
    }
    return penalty, meta


def _subset_inflation_penalty(ref_text: str, cand_text: str) -> Tuple[float, Dict[str, Any]]:
    return _subset_inflation_penalty_core(ref_text, cand_text, min_token_set_ratio=99.0)



def _subset_title_hard_cap(ref_text: str, cand_text: str, max_score: float) -> Tuple[Optional[float], Dict[str, Any]]:
    if rf_fuzz is None or not ref_text or not cand_text:
        return None, {"reason": "disabled_or_empty"}
    try:
        token_set_ratio = rf_fuzz.token_set_ratio(ref_text, cand_text)
        if token_set_ratio < 99:
            return None, {"token_set_ratio": token_set_ratio, "reason": "low_token_set"}
        token_sort_ratio = rf_fuzz.token_sort_ratio(ref_text, cand_text)
    except Exception:
        return None, {"reason": "fuzz_error"}

    ref_tokens = set(_tokenize_simple(ref_text))
    cand_tokens = set(_tokenize_simple(cand_text))
    if not ref_tokens or not cand_tokens:
        return None, {"reason": "no_tokens"}
    ref_subset = ref_tokens < cand_tokens
    size_ratio = min(len(ref_tokens), len(cand_tokens)) / \
        max(len(ref_tokens), len(cand_tokens))

    cap = None
    if ref_subset and size_ratio < 0.85 and token_sort_ratio < 95:
        cap = max_score
    meta = {
        "token_set_ratio": token_set_ratio,
        "token_sort_ratio": token_sort_ratio,
        "ref_tokens": len(ref_tokens),
        "cand_tokens": len(cand_tokens),
        "size_ratio": round(size_ratio, 3),
        "ref_subset": ref_subset,
        "cap": cap,
    }
    return cap, meta


def _distance_tiebreak(
    raw_citation: str,
    bests: List[Dict[str, Any]],
    method_to_citation: Dict[str, Optional[str]],
    epsilon: float = 1.0,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not raw_citation or len(bests) < 2:
        return None, None
    max_score = max(c["score"] for c in bests)
    tied = [c for c in bests if (max_score - c["score"]) <= epsilon]
    if len(tied) < 2:
        return None, None

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for cand in tied:
        method = cand.get("method")
        cite = method_to_citation.get(method)
        dist = _normalized_string_distance(raw_citation, cite)
        scored.append((dist, cand))

    scored.sort(key=lambda x: (x[0], METHOD_PRIORITY.index(x[1]["method"])))
    return scored[0][1], "distance-tiebreak"


def _distance_tiebreak_deduped(
    raw_citation: str, candidates: List[Dict[str, Any]], epsilon: float = 1.0
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not raw_citation or len(candidates) < 2:
        return None, None
    max_score = max(c["score"] for c in candidates)
    tied = [c for c in candidates if (max_score - c["score"]) <= epsilon]
    if len(tied) < 2:
        return None, None

    def _method_rank(method: Optional[str]) -> int:
        if method in METHOD_PRIORITY:
            return METHOD_PRIORITY.index(method)
        return len(METHOD_PRIORITY)

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for cand in tied:
        doi = cand.get("doi")
        citation = (
            _cached(_cache_key("doi_cite", {
                    "doi": doi, "style": "apa"}), _fetch_citation_for_doi, doi)
            if doi
            else None
        )
        dist = _normalized_string_distance(raw_citation, citation)
        scored.append((dist, cand))

    scored.sort(key=lambda x: (x[0], _method_rank(x[1].get("method"))))
    return scored[0][1], "distance-tiebreak"


def _raw_citation_validity(raw: str) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    if not raw:
        return False, ["empty"]
    txt = " ".join(raw.split())
    length = len(txt)
    if length < 20:
        reasons.append("too_short")
    if length > 600:
        reasons.append("too_long")
    if not _YEAR_RE.search(txt):
        reasons.append("missing_year")
    if not re.search(r"\b[A-Za-z]+,\s*[A-Za-z]", txt):
        reasons.append("missing_author_pattern")
    tokens = _tokenize_simple(txt)
    if tokens:
        numeric_tokens = [t for t in tokens if t.isdigit()]
        if len(numeric_tokens) / max(1, len(tokens)) > 0.35:
            reasons.append("too_many_numbers")
    if txt.count("(") + txt.count(")") > 20:
        reasons.append("too_many_parentheses")
    return (len(reasons) == 0), reasons


def _all_name_tokens_from_strings(authors: List[Any]) -> List[str]:
    tokens: List[str] = []
    for raw in authors:
        value = str(raw or "")
        if not value:
            continue
        cleaned = re.sub(r"[,.;]", " ", value)
        parts = [p for p in cleaned.split() if p]
        for p in parts:
            norm = p.lower()
            if norm:
                tokens.append(norm)
    return tokens


def _candidate_author_names(cand: Dict[str, Any], method: str) -> List[str]:
    names: List[str] = []
    if method.startswith("crossref"):
        for au in cand.get("author") or []:
            if isinstance(au, dict):
                given = (au.get("given") or "").strip()
                family = (au.get("family") or "").strip()
                if given or family:
                    names.append(f"{given} {family}".strip())
                else:
                    literal = (au.get("literal") or "").strip()
                    if literal:
                        names.append(literal)
            else:
                val = str(au or "").strip()
                if val:
                    names.append(val)
    elif method.startswith("openalex"):
        for au in cand.get("authorships") or []:
            try:
                dn = (au.get("author") or {}).get("display_name") or ""
                if dn:
                    names.append(dn)
            except Exception:
                continue
    return names


def _author_overlap_details(ref_authors: List[Any], cand: Dict[str, Any], method: str) -> Dict[str, Any]:
    ref_tokens = _all_name_tokens_from_strings(ref_authors)
    cand_names = _candidate_author_names(cand, method)
    cand_tokens = _all_name_tokens_from_strings(cand_names)
    overlap_tokens = sorted(set(ref_tokens) & set(cand_tokens))
    if not ref_tokens:
        overlap_pct = None
    else:
        overlap = len(set(ref_tokens) & set(cand_tokens))
        overlap_pct = round(100.0 * overlap / max(1, len(set(ref_tokens))), 1)
    return {
        "ref_authors": ref_authors,
        "cand_authors": cand_names,
        "ref_name_tokens": ref_tokens,
        "cand_name_tokens": cand_tokens,
        "overlap_name_tokens": overlap_tokens,
        "author_overlap_pct": overlap_pct,
    }


def _candidate_core_fields(cand: Dict[str, Any], method: str) -> Dict[str, Any]:
    if method.startswith("crossref"):
        title_list = cand.get("title") or []
        ctitle = title_list[0] if title_list else ""
        cjour = (cand.get("container-title") or [""])[0]
        cyear = _cr_safe_get_issued_year(cand)
        return {
            "title": ctitle,
            "journal": cjour,
            "year": cyear,
            "volume": cand.get("volume"),
            "issue": cand.get("issue"),
            "page": cand.get("page"),
        }
    if method.startswith("openalex"):
        ctitle = cand.get("title") or ""
        cjour = ""
        try:
            pl = cand.get("primary_location") or {}
            src = pl.get("source") or {}
            cjour = src.get("display_name") or ""
        except Exception:
            cjour = ""
        cyear = _oa_safe_publication_year(cand)
        biblio = cand.get("biblio") or {}
        return {
            "title": ctitle,
            "journal": cjour,
            "year": cyear,
            "volume": biblio.get("volume"),
            "issue": biblio.get("issue"),
            "page": biblio.get("first_page") or biblio.get("page"),
        }
    return {"title": "", "journal": "", "year": None, "volume": None, "issue": None, "page": None}


def _normalize_simple(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def _year_within_window(ref_year: Optional[int], cand_year: Optional[int], max_diff: int) -> Tuple[bool, Optional[int]]:
    if ref_year is None or cand_year is None:
        return True, None
    try:
        diff = abs(int(ref_year) - int(cand_year))
    except Exception:
        return True, None
    return diff <= max_diff, diff



def _fuzzy_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if rf_fuzz is not None:
        try:
            return float(rf_fuzz.token_set_ratio(a, b))
        except Exception:
            return 0.0
    na = _normalize_simple(a)
    nb = _normalize_simple(b)
    if not na or not nb:
        return 0.0
    dist = _levenshtein_distance(na, nb)
    return max(0.0, 100.0 * (1.0 - dist / max(len(na), len(nb))))


def _title_match_score(ref_title: str, cand_title: str) -> Tuple[bool, float, Dict[str, Any]]:
    ref_clean = _clean_title_text(ref_title)
    cand_clean = _clean_title_text(cand_title)
    if not ref_clean or not cand_clean:
        return False, 0.0, {"reason": "empty"}
    nref = _normalize_simple(ref_clean)
    ncand = _normalize_simple(cand_clean)
    contained = False
    if nref and ncand:
        contained = nref in ncand or ncand in nref
    ratio = _fuzzy_ratio(ref_clean, cand_clean)
    matched = contained or ratio >= TITLE_FUZZ_THRESHOLD
    score = 100.0 if contained else ratio

    ref_tokens = set(_tokenize_simple(ref_clean))
    cand_tokens = set(_tokenize_simple(cand_clean))
    size_ratio = min(len(ref_tokens), len(cand_tokens)) / max(len(ref_tokens),
                                                              len(cand_tokens)) if ref_tokens and cand_tokens else 0.0
    cand_subset = cand_tokens < ref_tokens if ref_tokens and cand_tokens else False
    if matched and cand_subset and len(cand_tokens) <= 2 and size_ratio < 0.5:
        return False, 0.0, {
            "reason": "candidate_too_short_subset",
            "ratio": round(ratio, 2),
            "ref_tokens": len(ref_tokens),
            "cand_tokens": len(cand_tokens),
            "size_ratio": round(size_ratio, 3),
            "threshold": TITLE_FUZZ_THRESHOLD,
        }

    meta = {
        "contained": contained,
        "ratio": round(ratio, 2),
        "threshold": TITLE_FUZZ_THRESHOLD,
        "ref_tokens": len(ref_tokens),
        "cand_tokens": len(cand_tokens),
        "size_ratio": round(size_ratio, 3),
        "cand_subset": cand_subset,
    }
    return matched, score, meta


def _journal_score(ref_journal: Optional[str], cand_journal: Optional[str]) -> Optional[float]:
    if not ref_journal or not cand_journal:
        return None
    return _fuzzy_ratio(ref_journal, cand_journal)


def _author_score(ref_authors: List[Any], cand_authors: List[str]) -> Optional[float]:
    if not ref_authors:
        return None
    ref_tokens = set(_all_name_tokens_from_strings(ref_authors))
    cand_tokens = set(_all_name_tokens_from_strings(cand_authors))
    if not ref_tokens or not cand_tokens:
        return None
    overlap = len(ref_tokens & cand_tokens)
    return 100.0 * overlap / max(1, len(ref_tokens))


def _asymmetric_subsequence_score(raw: str, citation: str) -> float:
    raw_tokens = _tokenize_simple(raw or "")
    cand_tokens = _tokenize_simple(citation or "")
    if not raw_tokens or not cand_tokens:
        return 0.0
    j = 0
    matched = 0
    for token in raw_tokens:
        while j < len(cand_tokens) and cand_tokens[j] != token:
            j += 1
        if j < len(cand_tokens):
            matched += 1
            j += 1
    return 100.0 * matched / max(1, len(raw_tokens))



def _candidate_scoring_fields(cand: Dict[str, Any], method: str) -> Dict[str, Any]:
    core = _candidate_core_fields(cand, method)
    return {
        "title": core.get("title") or "",
        "journal": core.get("journal") or "",
        "year": core.get("year"),
        "authors": _candidate_author_names(cand, method),
    }


def _score_candidate_parsed(
    *,
    ref_title: str,
    ref_journal: Optional[str],
    ref_year: Optional[int],
    ref_authors: List[Any],
    cand: Dict[str, Any],
    method: str,
    debug: bool = False,
    merged_fields: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[float], Dict[str, Any]]:
    fields = merged_fields if merged_fields is not None else _candidate_scoring_fields(
        cand, method)
    cand_title = fields["title"]
    cand_journal = fields["journal"]
    cand_year = fields["year"]
    cand_authors = fields["authors"]

    ref_title_clean = _clean_title_text(ref_title)
    cand_title_clean = _clean_title_text(cand_title)

    year_ok, year_diff = _year_within_window(
        ref_year, cand_year, YEAR_MAX_DIFF)
    if not year_ok:
        return None, {"reason": "year_out_of_range", "cand_year": cand_year, "ref_year": ref_year, "diff": year_diff}

    title_match, title_score, title_meta = _title_match_score(
        ref_title_clean, cand_title_clean)
    if not title_match:
        return None, {"reason": "title_mismatch", "title_meta": title_meta}

    ref_author_tokens = set(_all_name_tokens_from_strings(ref_authors or []))
    cand_author_tokens = set(_all_name_tokens_from_strings(cand_authors or []))
    if ref_author_tokens and cand_author_tokens and not (ref_author_tokens & cand_author_tokens):
        return None, {"reason": "author_mismatch"}

    scores: Dict[str, float] = {"title": float(title_score)}
    if ref_year is not None and cand_year is not None:
        if year_diff == 0:
            scores["year"] = 100.0
        elif year_diff == 1:
            scores["year"] = 80.0
    journal_score = _journal_score(ref_journal, cand_journal)
    if journal_score is not None:
        scores["journal"] = journal_score
    author_score = _author_score(ref_authors, cand_authors)
    if author_score is not None:
        scores["authors"] = author_score

    total_weight = 0.0
    weighted = 0.0
    for key, weight in PARSED_WEIGHTS.items():
        if key in scores:
            total_weight += weight
            weighted += weight * scores[key]
    if total_weight <= 0.0:
        return None, {"reason": "no_scores"}
    calc_score = weighted / total_weight

    penalty, penalty_meta = _subset_inflation_penalty(
        ref_title_clean, cand_title_clean)
    if penalty:
        calc_score = float(calc_score) - penalty
    cap, cap_meta = _subset_title_hard_cap(
        ref_title_clean, cand_title_clean, max_score=88.0)
    if cap is not None and calc_score > cap:
        calc_score = float(cap)

    meta = {
        "scores": scores,
        "title_meta": title_meta,
        "year_diff": year_diff,
        "penalty": penalty,
        "penalty_meta": penalty_meta,
        "cap": cap,
        "cap_meta": cap_meta,
    }
    return float(calc_score), meta


def _score_candidate_unparsed(raw_citation: str, doi: str) -> Tuple[Optional[float], Dict[str, Any]]:
    citation = _cached(_cache_key(
        "doi_cite", {"doi": doi, "style": "apa"}), _fetch_citation_for_doi, doi)
    if not citation:
        return None, {"reason": "no_citation"}
    score = _asymmetric_subsequence_score(raw_citation, citation)
    return float(score), {"citation": citation}


def _build_method_candidates(items: List[Dict[str, Any]], method: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for idx, cand in enumerate(items or [], 1):
        doi = _normalize_doi(cand.get("DOI") or cand.get("doi"), source="api")
        if not doi:
            continue
        entry = {
            "doi": doi,
            "method": method,
            "candidate": cand,
            "candidate_number": idx,
        }
        if method.startswith("crossref"):
            entry["query_score"] = cand.get("score")
        elif method.startswith("openalex"):
            entry["query_score"] = cand.get("relevance_score")
        entries.append(entry)
    return entries


def _pick_canonical_entry(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _priority(entry: Dict[str, Any]) -> int:
        method = entry.get("method") or ""
        if method in METADATA_PRIORITY:
            return METADATA_PRIORITY.index(method)
        return len(METADATA_PRIORITY)

    return sorted(entries, key=_priority)[0]


def _merge_candidate_fields(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _priority(entry: Dict[str, Any]) -> int:
        method = entry.get("method") or ""
        if method in METADATA_PRIORITY:
            return METADATA_PRIORITY.index(method)
        return len(METADATA_PRIORITY)

    ordered = sorted(entries, key=_priority)
    merged = {"title": "", "journal": "", "year": None, "authors": []}
    for entry in ordered:
        fields = _candidate_scoring_fields(entry["candidate"], entry["method"])
        if not merged["title"] and fields.get("title"):
            merged["title"] = fields["title"]
        if not merged["journal"] and fields.get("journal"):
            merged["journal"] = fields["journal"]
        if merged["year"] is None and fields.get("year") is not None:
            merged["year"] = fields["year"]
        if not merged["authors"] and fields.get("authors"):
            merged["authors"] = fields["authors"]
    return merged


def _dedupe_candidates(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_doi: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        doi = entry["doi"]
        rec = by_doi.setdefault(
            doi,
            {"doi": doi, "sources": [], "entries": []},
        )
        rec["sources"].append(entry["method"])
        rec["entries"].append(entry)
    merged: List[Dict[str, Any]] = []
    for doi, rec in by_doi.items():
        canonical = _pick_canonical_entry(rec["entries"])
        merged_fields = _merge_candidate_fields(rec["entries"])
        merged.append(
            {
                "doi": doi,
                "sources": sorted(set(rec["sources"])),
                "canonical": canonical,
                "entries": rec["entries"],
                "merged_fields": merged_fields,
            }
        )
    return merged



@functools.lru_cache(maxsize=256)
def _fetch_citation_for_doi(doi: Optional[str], style: str = "apa") -> Optional[str]:
    """Resolve a DOI into a formatted citation text."""
    if not doi:
        return None
    best_body: Optional[str] = None
    best_penalty = 10**9
    try:
        r = requests.get(
            f"https://doi.org/{doi}",
            headers={"Accept": f"text/x-bibliography; style={style}"},
            timeout=20,
        )
        body = _repair_common_mojibake(_decode_response_text(r).strip())
        if r.status_code == 200 and body:
            penalty = _mojibake_penalty(body)
            best_body, best_penalty = body, penalty
            if penalty == 0:
                return body
    except Exception:
        pass
    try:
        r = requests.get(
            f"https://api.crossref.org/works/{doi}/transform",
            headers={"Accept": f"text/x-bibliography; style={style}"},
            timeout=20,
        )
        body = _repair_common_mojibake(_decode_response_text(r).strip())
        if r.status_code == 200 and body:
            penalty = _mojibake_penalty(body)
            if penalty < best_penalty:
                best_body, best_penalty = body, penalty
            if penalty == 0:
                return body
    except Exception:
        pass
    return best_body



def _fetch_openalex_candidates(title: str, year: Optional[int], mailto: str, debug: bool) -> List[Dict[str, Any]]:
    if not title:
        return []

    sess = requests.Session()
    nt = _norm(title)
    use_mailto = mailto or OPENALEX_MAILTO

    # Stage 1: title + year
    cands = []
    year_key = _cache_key(
        "oa_with_year", {"title": nt, "year": year, "mailto": use_mailto,
                         "per_page": TOP_N_PER_STRATEGY})
    try:
        cands = _cached(year_key, _fetch_candidates,
                        sess, nt, year, use_mailto, TOP_N_PER_STRATEGY, debug)
    except Exception:
        cands = []

    # Stage 2: title only (no year filter)
    if not cands:
        no_year_key = _cache_key(
            "oa_no_year", {"title": nt, "year": None, "mailto": use_mailto,
                           "per_page": TOP_N_PER_STRATEGY})
        cands = _cached(no_year_key, _fetch_candidates,
                        sess, nt, None, use_mailto, TOP_N_PER_STRATEGY)

    return cands or []



def process_reference(
    ref: Dict[str, Any],
    *,
    threshold: int,
    mailto: str,
    screen_raw: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    raw_citation = (ref.get("raw_citation") or "").strip()
    title = (ref.get("title") or "").strip()
    journal = (ref.get("journal") or "").strip() or None
    authors = ref.get("authors") or []
    volume = (ref.get("volume") or "").strip() or None
    issue = (ref.get("issue") or "").strip() or None
    page = (ref.get("page") or "").strip() or None
    year = ref.get("year")
    try:
        year = int(year) if year is not None else None
    except Exception:
        year = None

    raw_year = _extract_year_from_raw(raw_citation)
    match_year = raw_year if raw_year is not None else year
    query_year = match_year

    parsed_available = bool(title)

    screened_out = False
    if screen_raw and raw_citation and not _raw_citation_passes_screen(raw_citation):
        screened_out = True
        if debug:
            logger.info(
                "Raw citation screened out ref_id=%s osf_id=%s raw_snippet=%s",
                ref.get("ref_id"),
                ref.get("osf_id"),
                raw_citation[:160],
            )

    if raw_citation and not screened_out:
        raw_key = _cache_key(
            "cr_biblio", {"blob": raw_citation.strip(), "rows": TOP_N_PER_STRATEGY})
        raw_items = _cached(
            raw_key,
            _query_crossref_biblio,
            blob=raw_citation,
            rows=TOP_N_PER_STRATEGY,
            debug=debug,
        )
    else:
        raw_items = []

    if title and not screened_out:
        # matching_crossref._build_params only uses the first author string, but include all for safety.
        title_key = _cache_key(
            "cr_title",
            {
                "title": title.strip(),
                "year": query_year,
                "journal": journal,
                "authors": [a for a in (authors or []) if a],
                "rows": TOP_N_PER_STRATEGY,
            },
        )
        title_items = _cached(
            title_key,
            _query_crossref,
            title=title,
            year=query_year,
            journal=journal,
            authors=authors,
            rows=TOP_N_PER_STRATEGY,
            debug=debug,
        )
    else:
        title_items = []

    oa_items = _fetch_openalex_candidates(
        title, query_year, mailto, debug) if title and not screened_out else []

    raw_items = (raw_items or [])[:TOP_N_PER_STRATEGY]
    title_items = (title_items or [])[:TOP_N_PER_STRATEGY]
    oa_items = (oa_items or [])[:TOP_N_PER_STRATEGY]

    raw_entries = _build_method_candidates(raw_items, "crossref_raw")
    title_entries = _build_method_candidates(title_items, "crossref_title")
    oa_entries = _build_method_candidates(oa_items, "openalex_title")

    def _score_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        scored: List[Dict[str, Any]] = []

        def _short(val: Optional[str], limit: int = 140) -> str:
            if not val:
                return ""
            return val if len(val) <= limit else f"{val[:limit]}..."

        for entry in entries:
            if parsed_available:
                merged_fields = entry.get("merged_fields")
                score, meta = _score_candidate_parsed(
                    ref_title=title,
                    ref_journal=journal,
                    ref_year=match_year,
                    ref_authors=authors,
                    cand=entry["candidate"],
                    method=entry["method"],
                    debug=debug,
                    merged_fields=merged_fields,
                )
            else:
                if not raw_citation:
                    continue
                score, meta = _score_candidate_unparsed(
                    raw_citation, entry["doi"])

            if score is None:
                if debug:
                    if parsed_available:
                        cand_fields = _candidate_scoring_fields(
                            entry["candidate"], entry["method"])
                        logger.info(
                            "Candidate rejected doi=%s method=%s reason=%s cand_title=%s cand_journal=%s cand_year=%s ref_year=%s year_diff=%s title_meta=%s query_score=%s",
                            entry.get("doi"),
                            entry.get("method"),
                            meta.get("reason"),
                            _short(cand_fields.get("title")),
                            _short(cand_fields.get("journal")),
                            cand_fields.get("year"),
                            match_year,
                            meta.get("year_diff"),
                            meta.get("title_meta"),
                            entry.get("query_score"),
                        )
                        if entry.get("method") == "openalex_title":
                            logger.info(
                                "OpenAlex candidate rejected doi=%s reason=%s title=%s journal=%s year=%s ref_year=%s year_diff=%s title_meta=%s relevance=%s",
                                entry.get("doi"),
                                meta.get("reason"),
                                _short(cand_fields.get("title")),
                                _short(cand_fields.get("journal")),
                                cand_fields.get("year"),
                                match_year,
                                meta.get("year_diff"),
                                meta.get("title_meta"),
                                entry.get("query_score"),
                            )
                    else:
                        logger.info(
                            "Candidate rejected doi=%s method=%s reason=%s query_score=%s",
                            entry.get("doi"),
                            entry.get("method"),
                            meta.get("reason"),
                            entry.get("query_score"),
                        )
                continue

            scored_entry = dict(entry)
            scored_entry["score"] = float(score)
            scored_entry["score_meta"] = meta
            scored.append(scored_entry)

            if debug and parsed_available:
                logger.info(
                    "Candidate accepted doi=%s method=%s score=%s scores=%s year_diff=%s title_meta=%s query_score=%s",
                    entry.get("doi"),
                    entry.get("method"),
                    score,
                    meta.get("scores"),
                    meta.get("year_diff"),
                    meta.get("title_meta"),
                    entry.get("query_score"),
                )
                if entry.get("method") == "openalex_title":
                    cand_fields = _candidate_scoring_fields(
                        entry["candidate"], entry["method"])
                    logger.info(
                        "OpenAlex candidate accepted doi=%s score=%s scores=%s title=%s journal=%s year=%s ref_year=%s year_diff=%s title_meta=%s relevance=%s",
                        entry.get("doi"),
                        score,
                        meta.get("scores"),
                        _short(cand_fields.get("title")),
                        _short(cand_fields.get("journal")),
                        cand_fields.get("year"),
                        match_year,
                        meta.get("year_diff"),
                        meta.get("title_meta"),
                        entry.get("query_score"),
                    )
                penalty = meta.get("penalty") or 0.0
                if penalty:
                    logger.info(
                        "Candidate subset penalty doi=%s method=%s penalty=%s meta=%s",
                        entry.get("doi"),
                        entry.get("method"),
                        penalty,
                        meta.get("penalty_meta"),
                    )
                cap = meta.get("cap")
                if cap is not None:
                    logger.info(
                        "Candidate hard cap doi=%s method=%s cap=%s meta=%s",
                        entry.get("doi"),
                        entry.get("method"),
                        cap,
                        meta.get("cap_meta"),
                    )
            elif debug:
                logger.info(
                    "Candidate accepted doi=%s method=%s score=%s query_score=%s",
                    entry.get("doi"),
                    entry.get("method"),
                    score,
                    entry.get("query_score"),
                )

        scored.sort(key=lambda x: x["score"], reverse=True)
        for idx, entry in enumerate(scored, 1):
            entry["rank"] = idx
        return scored

    raw_scored = _score_entries(raw_entries)
    title_scored = _score_entries(title_entries)
    oa_scored = _score_entries(oa_entries)

    # Best per strategy (pre-dedup), for evaluation only.
    best_raw = raw_scored[0] if raw_scored else None
    best_title = title_scored[0] if title_scored else None
    best_oa = oa_scored[0] if oa_scored else None

    all_entries = raw_entries + title_entries + oa_entries
    deduped = _dedupe_candidates(all_entries)
    canonical_entries: List[Dict[str, Any]] = []
    for rec in deduped:
        entry = dict(rec["canonical"])
        entry["sources"] = rec["sources"]
        entry["merged_fields"] = rec.get("merged_fields")
        canonical_entries.append(entry)

    deduped_scored = _score_entries(canonical_entries)

    structured_threshold = float(threshold)
    unparsed_threshold = float(UNPARSED_THRESHOLD_DEFAULT)
    threshold_used = structured_threshold if parsed_available else unparsed_threshold

    final_choice: Optional[Dict[str, Any]] = None
    status = "no_match"
    conflict_reason: Optional[str] = None

    if deduped_scored:
        max_score = deduped_scored[0]["score"]
        if max_score >= threshold_used:
            final_choice = deduped_scored[0]
            status = "matched"
            tied = [c for c in deduped_scored if abs(
                c["score"] - max_score) < 1e-6]
            if len(tied) > 1:
                status = "conflict"
                conflict_reason = "multiple-dois-same-score"
            score_gap = max_score - \
                deduped_scored[1]["score"] if len(deduped_scored) > 1 else None
            if status == "conflict" or (score_gap is not None and score_gap <= 1.0):
                tiebreak_choice, reason = _distance_tiebreak_deduped(
                    raw_citation, deduped_scored, epsilon=1.0)
                if tiebreak_choice:
                    final_choice = tiebreak_choice
                    status = "matched"
                    conflict_reason = reason
                elif status == "conflict":
                    final_choice = None
    if screened_out and status == "no_match" and conflict_reason is None:
        conflict_reason = "screened_out_raw_citation"

    doi_raw = best_raw.get("doi") if best_raw else None
    doi_title = best_title.get("doi") if best_title else None
    doi_oa = best_oa.get("doi") if best_oa else None

    crossref_citation_raw = _cached(_cache_key("doi_cite", {
                                    "doi": doi_raw, "style": "apa"}), _fetch_citation_for_doi, doi_raw) if doi_raw else None
    crossref_title_citation_raw = _cached(_cache_key("doi_cite", {
                                          "doi": doi_title, "style": "apa"}), _fetch_citation_for_doi, doi_title) if doi_title else None
    openalex_raw_citation_raw = _cached(_cache_key("doi_cite", {
                                        "doi": doi_oa, "style": "apa"}), _fetch_citation_for_doi, doi_oa) if doi_oa else None

    crossref_citation = _ascii_sanitize(_strip_urls(crossref_citation_raw))
    crossref_title_citation = _ascii_sanitize(
        _strip_urls(crossref_title_citation_raw))
    openalex_raw_citation = _ascii_sanitize(
        _strip_urls(openalex_raw_citation_raw))

    crossref_citation_string_distance = _string_distance(
        raw_citation, crossref_citation)
    crossref_title_citation_string_distance = _string_distance(
        raw_citation, crossref_title_citation)
    openalex_raw_citation_string_distance = _string_distance(
        raw_citation, openalex_raw_citation)

    final_citation_raw = None
    if final_choice:
        final_citation_raw = _cached(
            _cache_key(
                "doi_cite", {"doi": final_choice.get("doi"), "style": "apa"}),
            _fetch_citation_for_doi,
            final_choice.get("doi"),
        )
    final_citation = _ascii_sanitize(_strip_urls(final_citation_raw))
    final_citation_string_distance = _string_distance(
        raw_citation, final_citation)

    crossref_raw_query_score = best_raw.get(
        "query_score") if best_raw else None
    crossref_title_query_score = best_title.get(
        "query_score") if best_title else None

    row = {
        "raw_citation": _ascii_sanitize(raw_citation),
        "title": _ascii_sanitize(title),
        "raw_citation_doi": best_raw.get("doi") if best_raw else None,
        "raw_citation_score": best_raw.get("score") if best_raw else None,
        "raw_citation_rank_source": best_raw.get("candidate_number") if best_raw else None,
        # Rank score now reflects the Crossref response score (solr score)
        "raw_citation_rank_score": crossref_raw_query_score,
        "column_exists_raw_citation": _raw_citation_has_year(raw_citation),
        "title_doi": best_title.get("doi") if best_title else None,
        "title_score": best_title.get("score") if best_title else None,
        "title_rank_source": best_title.get("candidate_number") if best_title else None,
        "title_rank_score": crossref_title_query_score,
        "openalex_doi": best_oa.get("doi") if best_oa else None,
        "openalex_score": best_oa.get("score") if best_oa else None,
        "openalex_rank_source": best_oa.get("candidate_number") if best_oa else None,
        # OpenAlex relevance score from the API response
        "openalex_rank_score": (best_oa.get("candidate") or {}).get("relevance_score") if best_oa else None,
        "crossref_citation": crossref_citation,
        "crossref_title_citation": crossref_title_citation,
        "openalex_raw_citation": openalex_raw_citation,
        "crossref_citation_string_distance": crossref_citation_string_distance,
        "crossref_title_citation_string_distance": crossref_title_citation_string_distance,
        "openalex_raw_citation_string_distance": openalex_raw_citation_string_distance,
        "final_citation_string_distance": final_citation_string_distance,
        "final_method": final_choice.get("method") if final_choice else None,
        # candidate_number reflects the original position returned by the source API; useful for auditing
        "final_candidate_rank": final_choice.get("candidate_number") if final_choice else None,
        # rank is the score-based ordering within the chosen method
        "final_candidate_rank_score": final_choice.get("rank") if final_choice else None,
        "final_score": final_choice.get("score") if final_choice else None,
        "status": status,
        "conflict_reason": conflict_reason,
    }

    if debug:
        if final_choice:
            cand = final_choice.get("candidate") or {}
            author_detail = _author_overlap_details(
                authors, cand, final_choice.get("method") or "")
            _method = final_choice.get("method") or ""
            _cf = _candidate_core_fields(cand, _method)
            _cand_title = _cf.get("title") or ""
            _cand_journal = _cf.get("journal") or ""
            _cand_year = _cf.get("year")
            _cand_volume = _cf.get("volume")
            _cand_issue = _cf.get("issue")
            _cand_page = _cf.get("page")
            match_detail = {
                "ref_title": title,
                "cand_title": _cand_title,
                "title_match": _fuzzy_ratio(_cand_title, title) >= TITLE_FUZZ_THRESHOLD if title else None,
                "ref_journal": journal,
                "cand_journal": _cand_journal,
                "journal_match": _fuzzy_ratio(_cand_journal, journal) >= TITLE_FUZZ_THRESHOLD if journal and _cand_journal else None,
                "ref_year": match_year,
                "cand_year": _cand_year,
                "year_match": int(match_year) == int(_cand_year) if match_year is not None and _cand_year is not None else None,
                "ref_volume": volume,
                "cand_volume": _cand_volume,
                "volume_match": _normalize_simple(volume) == _normalize_simple(_cand_volume) if volume and _cand_volume else None,
                "ref_issue": issue,
                "cand_issue": _cand_issue,
                "issue_match": _normalize_simple(issue) == _normalize_simple(_cand_issue) if issue and _cand_issue else None,
                "ref_page": page,
                "cand_page": _cand_page,
                "page_match": _normalize_simple(page) == _normalize_simple(_cand_page) if page and _cand_page else None,
                "authors_match": author_detail.get("author_overlap_pct") is not None and author_detail["author_overlap_pct"] > 0,
            }
        else:
            author_detail = {
                "ref_authors": authors,
                "cand_authors": [],
                "ref_name_tokens": _all_name_tokens_from_strings(authors),
                "cand_name_tokens": [],
                "overlap_name_tokens": [],
                "author_overlap_pct": None,
            }
            match_detail = {
                "ref_title": title,
                "cand_title": None,
                "title_match": None,
                "ref_journal": journal,
                "cand_journal": None,
                "journal_match": None,
                "ref_year": year,
                "cand_year": None,
                "year_match": None,
                "ref_volume": volume,
                "cand_volume": None,
                "volume_match": None,
                "ref_issue": issue,
                "cand_issue": None,
                "issue_match": None,
                "ref_page": page,
                "cand_page": None,
                "page_match": None,
                "authors_match": None,
            }
        logger.info(
            "Debug scored candidates",
            extra={
                "osf_id": ref.get("osf_id"),
                "ref_id": ref.get("ref_id"),
                "raw_scored": [(c["doi"], c["score"]) for c in raw_scored[:5]],
                "title_scored": [(c["doi"], c["score"]) for c in title_scored[:5]],
                "oa_scored": [(c["doi"], c["score"]) for c in oa_scored[:5]],
                "final": row["final_method"],
                "final_doi": final_choice.get("doi") if final_choice else None,
                "final_score": final_choice.get("score") if final_choice else None,
                "final_rank_source": final_choice.get("candidate_number") if final_choice else None,
                "final_rank_score": final_choice.get("rank") if final_choice else None,
                "final_status": status,
                "final_conflict_reason": conflict_reason,
                "ref_title": title,
                "ref_raw_citation": raw_citation,
                "ref_journal": journal,
                "ref_year": match_year,
                "ref_volume": volume,
                "ref_issue": issue,
                "ref_page": page,
                "ref_authors": author_detail["ref_authors"],
                "cand_authors": author_detail["cand_authors"],
                "ref_name_tokens": author_detail["ref_name_tokens"],
                "cand_name_tokens": author_detail["cand_name_tokens"],
                "author_overlap_pct": author_detail["author_overlap_pct"],
                "title_match": match_detail["title_match"],
                "journal_match": match_detail["journal_match"],
                "year_match": match_detail["year_match"],
                "volume_match": match_detail["volume_match"],
                "issue_match": match_detail["issue_match"],
                "page_match": match_detail["page_match"],
                "authors_match": match_detail["authors_match"],
            },
        )
        # Emit a human-readable line to the terminal (extras may not render with default formatter).
        logger.info(
            "Selected DOI=%s method=%s score=%s overlap_pct=%s",
            final_choice.get("doi") if final_choice else None,
            final_choice.get("method") if final_choice else None,
            final_choice.get("score") if final_choice else None,
            author_detail["author_overlap_pct"],
        )
        logger.info(
            "Author overlap details ref_authors=%s cand_authors=%s ref_tokens=%s cand_tokens=%s overlap_tokens=%s",
            author_detail["ref_authors"],
            author_detail["cand_authors"],
            author_detail["ref_name_tokens"],
            author_detail["cand_name_tokens"],
            author_detail["overlap_name_tokens"],
        )
        logger.info(
            "Match details title(ref=%s cand=%s match=%s) journal(ref=%s cand=%s match=%s) "
            "year(ref=%s cand=%s match=%s) volume(ref=%s cand=%s match=%s) "
            "issue(ref=%s cand=%s match=%s) page(ref=%s cand=%s match=%s) authors_match=%s",
            match_detail["ref_title"],
            match_detail["cand_title"],
            match_detail["title_match"],
            match_detail["ref_journal"],
            match_detail["cand_journal"],
            match_detail["journal_match"],
            match_detail["ref_year"],
            match_detail["cand_year"],
            match_detail["year_match"],
            match_detail["ref_volume"],
            match_detail["cand_volume"],
            match_detail["volume_match"],
            match_detail["ref_issue"],
            match_detail["cand_issue"],
            match_detail["issue_match"],
            match_detail["ref_page"],
            match_detail["cand_page"],
            match_detail["page_match"],
            match_detail["authors_match"],
        )

    return row


def main():
    ap = argparse.ArgumentParser(
        description=(
            "Run DOI augmentation across Crossref raw, Crossref title, and OpenAlex title searches. "
            "Uses SBMV scoring, resolves conflicts, and emits a CSV summary."
        )
    )
    ap.add_argument("--osf-id", help="OSF preprint id to process")
    ap.add_argument(
        "--ref-id", help="Optional ref_id filter when using --osf-id")
    ap.add_argument("--limit", type=int, default=400,
                    help="Max references to fetch when using --osf-id")
    ap.add_argument("--output", default="doi_multi_method.csv",
                    help="CSV output path")
    ap.add_argument(
        "--threshold",
        type=int,
        default=int(STRUCTURED_THRESHOLD_DEFAULT),
        help="Structured score threshold (parsed metadata available)",
    )
    ap.add_argument("--title", help="Title to search (standalone mode)")
    ap.add_argument("--raw", help="Raw citation to search (standalone mode)")
    ap.add_argument(
        "--raw-stdin",
        action="store_true",
        help="Read raw citation from stdin (useful for pasting multi-line citations)",
    )
    ap.add_argument("--year", type=int, help="Year hint (standalone mode)")
    ap.add_argument("--journal", help="Journal hint (standalone mode)")
    ap.add_argument("--author", action="append", dest="authors",
                    help="Repeatable author (standalone mode)")
    ap.add_argument("--mailto", default=None,
                    help="Override OPENALEX_MAILTO/OPENALEX_EMAIL for OpenAlex requests")
    ap.add_argument("--from-db", action="store_true",
                    help="Read references from Dynamo preprint_references")
    ap.add_argument("--include-existing", action="store_true",
                    help="Include refs that already have a DOI (useful for re-checking)")
    ap.add_argument("--no-screen-raw", dest="screen_raw",
                    action="store_false", help="Disable raw citation screening")
    ap.add_argument("--random-sample", action="store_true",
                    help="Process a random sample of refs (limit still applies)")
    ap.add_argument("--debug", action="store_true")
    ap.set_defaults(screen_raw=True)
    args = ap.parse_args()

    if args.debug:
        logger.info(
            "Debug environment executable=%s rapidfuzz_available=%s",
            sys.executable,
            rf_fuzz is not None,
        )

    mailto = args.mailto or os.environ.get("OPENALEX_MAILTO") or os.environ.get(
        "OPENALEX_EMAIL") or OPENALEX_MAILTO
    out_path = Path(args.output)

    if args.raw_stdin:
        args.raw = sys.stdin.read().strip()

    processed = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

        if args.from_db:
            repo = PreprintsRepo()
            if not args.osf_id:
                refs = _iter_all_refs(
                    repo, args.limit, args.ref_id, args.include_existing)
            else:
                refs = repo.select_refs_missing_doi(
                    limit=args.limit,
                    osf_id=args.osf_id,
                    ref_id=args.ref_id,
                    include_existing=args.include_existing,
                )
            if args.random_sample:
                if not isinstance(refs, list):
                    refs = list(refs)
                random.shuffle(refs)
            for ref in refs:
                row = process_reference(
                    ref,
                    threshold=args.threshold,
                    mailto=mailto,
                    screen_raw=args.screen_raw,
                    debug=args.debug,
                )
                writer.writerow(row)
                processed += 1
                _maybe_log_progress(processed, LOG_EVERY, f)
        elif args.osf_id:
            repo = PreprintsRepo()
            refs = repo.select_refs_missing_doi(
                limit=args.limit,
                osf_id=args.osf_id,
                ref_id=args.ref_id,
                include_existing=True,
            )
            if args.random_sample:
                random.shuffle(refs)
            for ref in refs:
                row = process_reference(
                    ref,
                    threshold=args.threshold,
                    mailto=mailto,
                    screen_raw=args.screen_raw,
                    debug=args.debug,
                )
                writer.writerow(row)
                processed += 1
                _maybe_log_progress(processed, LOG_EVERY, f)
        else:
            if not (args.title or args.raw):
                ap.error("Provide --osf-id or at least one of --title/--raw")
            ref = {
                "osf_id": None,
                "ref_id": None,
                "raw_citation": args.raw or "",
                "title": args.title or "",
                "year": args.year,
                "journal": args.journal,
                "authors": args.authors or [],
                "volume": None,
                "issue": None,
                "page": None,
            }
            row = process_reference(
                ref,
                threshold=args.threshold,
                mailto=mailto,
                screen_raw=args.screen_raw,
                debug=args.debug,
            )
            writer.writerow(row)
            processed = 1

    print(f"Wrote {processed} rows to {out_path}", flush=True)


if __name__ == "__main__":
    main()
