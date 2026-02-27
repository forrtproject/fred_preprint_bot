from __future__ import annotations

import os
import html
import time
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from thefuzz import fuzz
from ..dynamo.preprints_repo import PreprintsRepo
from ..logging_setup import get_logger, with_extras

logger = get_logger(__name__)


def _info(msg: str, **extras):
    if extras:
        with_extras(logger, **extras).info(msg)
    else:
        logger.info(msg)


def _warn(msg: str, **extras):
    if extras:
        with_extras(logger, **extras).warning(msg)
    else:
        logger.warning(msg)


def _exception(msg: str, **extras):
    if extras:
        with_extras(logger, **extras).exception(msg)
    else:
        logger.exception(msg)


def _debug(msg: str, **extras):
    if extras:
        with_extras(logger, **extras).debug(msg)
    else:
        logger.debug(msg)

CROSSREF_BASE = "https://api.crossref.org/works"
RAW_MIN_FUZZ = 70
RAW_MIN_JACCARD = float(os.environ.get("RAW_MIN_JACCARD", 0.12))
RAW_MIN_SOLR_SCORE = 30.0
STRUCTURED_MIN_SOLR_SCORE = 52.0
TITLE_MIN_RATIO = 88
SBMV_THRESHOLD_DEFAULT = 75.0  # composite score cutoff for SBMV-style scoring
YEAR_MAX_DIFF = 1

_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_TAG_RE = re.compile(r"<[^>]+>")

# -----------------------
# Utilities
# -----------------------

def _mailto() -> str:
    # Prefer dedicated CROSSREF_MAILTO; fallback to OPENALEX_EMAIL
    return os.environ.get("CROSSREF_MAILTO") or os.environ.get("OPENALEX_EMAIL") or "devnull@example.com"


def _year_window(year: Optional[int]) -> Optional[Tuple[str, str]]:
    """Return (from, until) YYYY-MM-DD tuple if year is an int, else None."""
    if year is None:
        return None
    try:
        y = int(year)
        return (f"{y}-01-01", f"{y}-12-31")
    except Exception:
        return None


def _year_within_window(ref_year: Optional[int], cand_year: Optional[int], max_diff: int) -> Tuple[bool, Optional[int]]:
    if ref_year is None or cand_year is None:
        return True, None
    try:
        diff = abs(int(ref_year) - int(cand_year))
    except Exception:
        return True, None
    return diff <= max_diff, diff


def _extract_year_from_raw(raw: str) -> Optional[int]:
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


def _clean_title_text(text: str) -> str:
    if not text:
        return ""
    unescaped = html.unescape(text)
    cleaned = _TAG_RE.sub(" ", unescaped)
    return " ".join(cleaned.split()).strip()


def _build_params(title: str,
                  year: Optional[int],
                  journal: Optional[str],
                  authors: List[str],
                  rows: int = 30) -> Dict[str, str]:
    """
    Build Crossref query params. We bias toward title search and add year window.
    We keep it simple to avoid 400s from unsupported params.
    """
    params = {
        "query.title": title,
        "rows": str(rows),
        # Include score so consumers can surface the Crossref relevance score.
        "select": ",".join([
            "DOI", "title", "issued", "author", "container-title", "score",
            "volume", "issue", "page",
            "published-print", "published-online",
        ]),
        "mailto": _mailto(),
    }

    ywin = _year_window(year)
    if ywin:
        params["filter"] = f"from-pub-date:{ywin[0]},until-pub-date:{ywin[1]}"

    # If we have a journal, add as an additional cue (Crossref supports query.container-title)
    if journal:
        params["query.container-title"] = journal

    # If we have at least one author, add one. (Multiple authors as multiple query.author are OK)
    if authors:
        # Use the first non-empty author string
        a = next((a for a in authors if a and a.strip()), None)
        if a:
            params["query.author"] = a

    return params


def _first_year_from_date_parts(blob: Optional[dict]) -> Optional[int]:
    if not blob:
        return None
    try:
        parts = blob.get("date-parts") or []
        if parts and isinstance(parts[0], list) and parts[0]:
            return int(parts[0][0])
    except Exception:
        return None
    return None


def _safe_get_issued_year(item: dict) -> Optional[int]:
    """
    Prefer the journal issue's published-print year when available; fall back to Crossref's
    canonical issued/published dates.
    """
    journal_issue = item.get("journal-issue") or {}
    year = _first_year_from_date_parts(journal_issue.get("published-print"))
    if year is not None:
        return year
    year = _first_year_from_date_parts(journal_issue.get("published-online"))
    if year is not None:
        return year
    year = _first_year_from_date_parts(item.get("published-print"))
    if year is not None:
        return year
    return _first_year_from_date_parts(item.get("issued"))


def _score_candidate_sbmv(
    cand: dict,
    title: Optional[str],
    journal: Optional[str],
    year: Optional[int],
    authors: Optional[List[str]],
    volume: Optional[str] = None,
    issue: Optional[str] = None,
    page: Optional[str] = None,
) -> float:
    """
    Crossref-style SBMV-inspired scoring: weighted blend of title, authors, journal,
    year, and volume/issue/page where available.
    """
    weights = {
        "title": 0.45,
        "authors": 0.2,
        "journal": 0.15,
        "year": 0.1,
        "vip": 0.1,  # volume/issue/page bucket
    }

    scores: Dict[str, float] = {}

    # Title
    ctitles = cand.get("title") or []
    ctitle_raw = ctitles[0] if ctitles else ""
    ctitle = _clean_title_text(ctitle_raw)
    rtitle = _clean_title_text(title or "")
    if ctitle and rtitle:
        scores["title"] = fuzz.token_set_ratio(ctitle, rtitle)

    # Authors (overlap ratio of name tokens)
    if authors:
        ref_tokens = set(_all_name_tokens_from_strings(authors))
        cand_tokens = set(_candidate_author_name_tokens(cand))
        if ref_tokens and cand_tokens:
            overlap = len(ref_tokens & cand_tokens)
            scores["authors"] = 100.0 * overlap / max(1, len(ref_tokens))

    # Journal
    cjour = (cand.get("container-title") or [""])[0]
    if journal and cjour:
        if _journal_matches(cjour, journal):
            scores["journal"] = 100.0
        else:
            scores["journal"] = float(fuzz.token_set_ratio(cjour.lower(), journal.lower()))

    # Year
    cyear = _safe_get_issued_year(cand)
    if year is not None and cyear is not None:
        diff = abs(int(year) - int(cyear))
        if diff == 0:
            scores["year"] = 100.0
        elif diff == 1:
            scores["year"] = 80.0
        else:
            scores["year"] = 0.0

    # Volume/Issue/Page
    vip_score = 0.0
    vip_checks = 0
    if volume:
        cv = str(cand.get("volume") or "")
        if cv:
            vip_checks += 1
            vip_score += 100.0 if _normalize_text(cv) == _normalize_text(volume) else 0.0
    if issue:
        ci = str(cand.get("issue") or "")
        if ci:
            vip_checks += 1
            vip_score += 100.0 if _normalize_text(ci) == _normalize_text(issue) else 0.0
    if page:
        cp = str(cand.get("page") or "")
        if cp:
            vip_checks += 1
            vip_score += 100.0 if _normalize_text(cp) == _normalize_text(page) else 0.0
    if vip_checks:
        scores["vip"] = vip_score / vip_checks

    # Weighted average over available components
    total_weight = 0.0
    total_score = 0.0
    for key, val in scores.items():
        w = weights.get(key, 0.0)
        if val is None:
            continue
        total_weight += w
        total_score += w * float(val)
    if total_weight == 0:
        return 0.0
    score = total_score / total_weight

    penalty, _ = _subset_inflation_penalty(rtitle, ctitle)
    if penalty:
        score = float(score) - penalty
    cap, _ = _subset_title_hard_cap(rtitle, ctitle, max_score=88.0)
    if cap is not None and score > cap:
        score = float(cap)

    return float(score)


def _score_candidate_structured(cand: dict,
                                title: str,
                                year: Optional[int],
                                journal: Optional[str],
                                authors: Optional[List[str]] = None,
                                volume: Optional[str] = None,
                                issue: Optional[str] = None,
                                page: Optional[str] = None) -> float:
    return _score_candidate_sbmv(cand, title, journal, year, authors, volume, issue, page)


def _build_raw_candidate_doc(cand: dict, authors: List[str]) -> str:
    parts: List[str] = []
    parts.extend([_clean_title_text(t) for t in (cand.get("title") or []) if t])
    parts.extend([_clean_title_text(t) for t in (cand.get("short-container-title") or []) if t])
    parts.extend([_clean_title_text(t) for t in (cand.get("container-title") or []) if t])
    parts.extend([f"{a.get('given', '')} {a.get('family', '')}".strip()
                  for a in (cand.get("author") or []) if a])
    # Add reversed/different orderings to capture initial vs expanded names
    for a in cand.get("author") or []:
        given = (a.get("given") or "").strip()
        family = (a.get("family") or "").strip()
        if given and family:
            parts.append(f"{family} {given}")
            parts.append(f"{given[0]}. {family}" if given else family)
    parts.extend(authors or [])
    for key in ("volume", "issue", "page"):
        val = cand.get(key)
        if isinstance(val, list):
            parts.extend([str(x) for x in val if x])
        elif val:
            parts.append(str(val))
    year = _safe_get_issued_year(cand)
    if year:
        parts.append(str(year))
    return " ".join(p for p in parts if p)


def _score_candidate_raw(cand: dict, raw_blob: str, authors: List[str]) -> int:
    if not raw_blob:
        return 0
    doc = _build_raw_candidate_doc(cand, authors)
    if not doc:
        return 0
    return fuzz.token_set_ratio(doc, raw_blob)


def _normalize_text(value: str) -> str:
    if value is None:
        return ""
    # Decode HTML entities (e.g., &amp;) before stripping punctuation/case
    unescaped = html.unescape(value)
    return re.sub(r"[^a-z0-9]+", "", unescaped.lower())


def _tokenize_simple(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def _subset_inflation_penalty_core(ref_text: str, cand_text: str, min_token_set_ratio: float) -> Tuple[float, Dict[str, Any]]:
    if not ref_text or not cand_text:
        return 0.0, {"reason": "empty"}
    try:
        token_set_ratio = fuzz.token_set_ratio(ref_text, cand_text)
        if token_set_ratio < min_token_set_ratio:
            return 0.0, {"token_set_ratio": token_set_ratio, "reason": "low_token_set"}
        token_sort_ratio = fuzz.token_sort_ratio(ref_text, cand_text)
    except Exception:
        return 0.0, {"reason": "fuzz_error"}

    ref_tokens = set(_tokenize_simple(ref_text))
    cand_tokens = set(_tokenize_simple(cand_text))
    if not ref_tokens or not cand_tokens:
        return 0.0, {"reason": "no_tokens"}

    ref_subset = ref_tokens < cand_tokens
    cand_subset = cand_tokens < ref_tokens
    size_ratio = min(len(ref_tokens), len(cand_tokens)) / max(len(ref_tokens), len(cand_tokens))
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


def _subset_inflation_penalty_raw(ref_text: str, cand_text: str) -> Tuple[float, Dict[str, Any]]:
    return _subset_inflation_penalty_core(ref_text, cand_text, min_token_set_ratio=0.0)


def _subset_title_hard_cap(ref_text: str, cand_text: str, max_score: float) -> Tuple[Optional[float], Dict[str, Any]]:
    if not ref_text or not cand_text:
        return None, {"reason": "empty"}
    try:
        token_set_ratio = fuzz.token_set_ratio(ref_text, cand_text)
        if token_set_ratio < 99:
            return None, {"token_set_ratio": token_set_ratio, "reason": "low_token_set"}
        token_sort_ratio = fuzz.token_sort_ratio(ref_text, cand_text)
    except Exception:
        return None, {"reason": "fuzz_error"}

    ref_tokens = set(_tokenize_simple(ref_text))
    cand_tokens = set(_tokenize_simple(cand_text))
    if not ref_tokens or not cand_tokens:
        return None, {"reason": "no_tokens"}
    ref_subset = ref_tokens < cand_tokens
    size_ratio = min(len(ref_tokens), len(cand_tokens)) / max(len(ref_tokens), len(cand_tokens))

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


def _jaccard_similarity(a: str, b: str) -> float:
    aset = set(_tokenize_simple(a))
    bset = set(_tokenize_simple(b))
    if not aset and not bset:
        return 0.0
    return len(aset & bset) / max(1, len(aset | bset))


def _journal_matches(cand_journal: str, ref_journal: str, fuzz_threshold: int = 94) -> bool:
    """
    Consider journals matching if normalized strings are equal, or if fuzzy token_set_ratio
    clears a high bar to allow minor spelling variants (e.g., Specialities vs Specialties).
    """
    if not cand_journal or not ref_journal:
        return False
    if _normalize_text(cand_journal) == _normalize_text(ref_journal):
        return True
    ratio = fuzz.token_set_ratio(cand_journal.lower(), ref_journal.lower())
    return ratio >= fuzz_threshold


def _title_matches(cand_title: Optional[str], ref_title: Optional[str]) -> bool:
    if not cand_title or not ref_title:
        return False if ref_title else True
    ref_clean = _clean_title_text(ref_title)
    cand_clean = _clean_title_text(cand_title)
    if not ref_clean or not cand_clean:
        return False if ref_title else True
    ref_tokens = set(_tokenize_simple(ref_clean))
    cand_tokens = set(_tokenize_simple(cand_clean))
    size_ratio = min(len(ref_tokens), len(cand_tokens)) / max(len(ref_tokens), len(cand_tokens)) if ref_tokens and cand_tokens else 0.0
    cand_subset = cand_tokens < ref_tokens if ref_tokens and cand_tokens else False
    if cand_subset and len(cand_tokens) <= 2 and size_ratio < 0.5:
        return False
    ratio = fuzz.token_set_ratio(cand_clean, ref_clean)
    return ratio >= TITLE_MIN_RATIO


def _coerce_author_string(author) -> str:
    if isinstance(author, str):
        return author
    if isinstance(author, dict):
        for key in ("family", "name", "text", "literal", "S"):
            val = author.get(key)
            if val:
                return str(val)
        given = author.get("given")
        family = author.get("family")
        if given or family:
            return f"{given or ''} {family or ''}".strip()
    return str(author or "")


def _all_name_tokens_from_strings(authors: List) -> List[str]:
    tokens: List[str] = []
    for raw in authors:
        value = _coerce_author_string(raw)
        if not value:
            continue
        cleaned = re.sub(r"[,\.;]", " ", value)
        parts = [p for p in cleaned.split() if p]
        for p in parts:
            norm = _normalize_text(p)
            if norm:
                tokens.append(norm)
    return tokens


def _candidate_author_name_tokens(cand: dict) -> List[str]:
    tokens: List[str] = []
    for a in cand.get("author") or []:
        if not isinstance(a, dict):
            norm = _normalize_text(str(a))
            if norm:
                tokens.append(norm)
            continue
        for key in ("given", "family"):
            val = a.get(key)
            if val:
                for p in re.sub(r"[,\.;]", " ", val).split():
                    norm = _normalize_text(p)
                    if norm:
                        tokens.append(norm)
        if not a.get("given") and not a.get("family"):
            literal = a.get("name") or a.get("literal")
            if literal:
                for p in re.sub(r"[,\.;]", " ", literal).split():
                    norm = _normalize_text(p)
                    if norm:
                        tokens.append(norm)
    return tokens


def _authors_overlap(cand: dict, ref_authors: List) -> bool:
    ref_tokens = _all_name_tokens_from_strings(ref_authors)
    cand_tokens = _candidate_author_name_tokens(cand)
    if not ref_tokens or not cand_tokens:
        return True
    return bool(set(ref_tokens) & set(cand_tokens))


def _raw_candidate_valid(
    cand: dict,
    raw_blob: str,
    authors: List[str],
    journal: Optional[str],
    year: Optional[int],
) -> bool:
    raw_score = _score_candidate_raw(cand, raw_blob, authors)
    if raw_score < RAW_MIN_FUZZ:
        doc = _build_raw_candidate_doc(cand, authors)
        _debug(
            "Raw score too low",
            raw_score=raw_score,
            doi=cand.get("DOI"),
            raw_excerpt=raw_blob[:200],
            doc_excerpt=doc[:200],
            raw_len=len(raw_blob or ""),
            doc_len=len(doc or ""),
        )
        return False
    doc = _build_raw_candidate_doc(cand, authors)
    jacc = _jaccard_similarity(doc, raw_blob)
    if jacc < RAW_MIN_JACCARD:
        _debug(
            "Raw jaccard too low",
            jaccard=jacc,
            doi=cand.get("DOI"),
            raw_excerpt=raw_blob[:200],
            doc_excerpt=doc[:200],
        )
        return False
    cyear = _safe_get_issued_year(cand)
    year_ok, year_diff = _year_within_window(year, cyear, YEAR_MAX_DIFF)
    if not year_ok:
        _debug(
            "Year mismatch in raw candidate",
            candidate_year=cyear,
            ref_year=year,
            year_diff=year_diff,
            doi=cand.get("DOI"),
        )
        return False
    cjour = (cand.get("container-title") or [""])[0]
    if journal and cjour:
        if not _journal_matches(cjour, journal):
            _debug("Journal mismatch in raw candidate", doi=cand.get("DOI"), candidate_journal=cjour, ref_journal=journal)
            return False
    if authors:
        if not _authors_overlap(cand, authors):
            _debug("Author mismatch in raw candidate", doi=cand.get("DOI"), authors=authors, candidate_authors=cand.get("author"))
            return False
    return True


def _structured_candidate_valid(
    cand: dict,
    title: Optional[str],
    journal: Optional[str],
    year: Optional[int],
    authors: Optional[List[str]],
) -> bool:
    ctitles = cand.get("title") or []
    ctitle = ctitles[0] if ctitles else ""
    if title:
        if not _title_matches(ctitle, title):
            return False
    cjour = (cand.get("container-title") or [""])[0]
    if journal and cjour:
        if not _journal_matches(cjour, journal):
            return False
    cyear = _safe_get_issued_year(cand)
    year_ok, _ = _year_within_window(year, cyear, YEAR_MAX_DIFF)
    if not year_ok:
        return False
    if authors:
        if not _authors_overlap(cand, authors):
            return False
    return True


def _pick_best(cands: List[dict],
               title: str,
               year: Optional[int],
               journal: Optional[str],
               threshold: int,
               debug: bool = False,
               raw_blob: Optional[str] = None,
               structured_authors: Optional[List[str]] = None,
               ref_volume: Optional[str] = None,
               ref_issue: Optional[str] = None,
               ref_page: Optional[str] = None,
               raw_search: bool = False) -> Tuple[Optional[dict], Optional[str]]:
    best = None
    best_score = -1
    last_reason: Optional[str] = None

    for c in cands:
        solr_score = c.get("score")
        if raw_blob and solr_score is not None and solr_score < RAW_MIN_SOLR_SCORE:
            if debug:
                doi = c.get("DOI")
                print(f"SKIP low-solr-score DOI={doi} SOLR={solr_score:.1f}")
            last_reason = "low-solr-score"
            continue
        if raw_blob and not _raw_candidate_valid(
            c,
            raw_blob,
            structured_authors or [],
            journal,
            year,
        ):
            if debug:
                doi = c.get("DOI")
                print(f"SKIP raw-invalid DOI={doi}")
            last_reason = "raw-validation-failed"
            continue
        if not raw_blob:
            if solr_score is not None and solr_score < STRUCTURED_MIN_SOLR_SCORE:
                if debug:
                    doi = c.get("DOI")
                    print(f"SKIP low-structured-score DOI={doi} SOLR={solr_score:.1f}")
                last_reason = "low-structured-score"
                continue
            if not _structured_candidate_valid(
                c,
                title,
                journal,
                year,
                structured_authors or [],
            ):
                if debug:
                    doi = c.get("DOI")
                    print(f"SKIP structured-invalid DOI={doi}")
                last_reason = "structured-validation-failed"
                continue
        sc = solr_score
        if sc is None:
            if raw_blob:
                sc = _score_candidate_raw(c, raw_blob, structured_authors or [])
            else:
                sc = _score_candidate_structured(c, title, year, journal, structured_authors, ref_volume, ref_issue, ref_page)
        if raw_blob:
            doc = _build_raw_candidate_doc(c, structured_authors or [])
            penalty, _ = _subset_inflation_penalty_raw(raw_blob, doc)
            if penalty:
                sc = float(sc) - min(10.0, penalty)
        if debug:
            doi = c.get("DOI")
            cyear = _safe_get_issued_year(c)
            ctitles = c.get("title") or []
            ctitle = ctitles[0] if ctitles else ""
            if isinstance(sc, (int, float)):
                score_str = f"{sc:6.2f}"
            else:
                score_str = str(sc)
            print(f"SCORE={score_str}  YEAR={cyear!s:>4}  DOI={doi}  TITLE={ctitle}")

        if sc > best_score:
            best_score = sc
            best = c
            last_reason = None

    effective_threshold = threshold if not raw_search else max(15, threshold // 2)
    if best and best_score >= effective_threshold:
        return best, None
    return None, last_reason or "below-threshold"


def _crossref_request(params: Dict[str, str],
                      max_attempts: int = 6,
                      debug: bool = False) -> Optional[dict]:
    """
    GET Crossref with retries. Log detailed errors.
    """
    url = CROSSREF_BASE
    if debug:
        _debug("Crossref request start", params=params)
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=25)
            if r.status_code == 200:
                if debug:
                    _debug("Crossref request success", url=r.url, attempt=attempt)
                return r.json()
            else:
                body = r.text[:600]
                # Log both via logger and console to ensure visibility
                _warn("Crossref HTTP error", status=r.status_code, url=r.url, body=body)
                if debug:
                    print("[Crossref HTTP error] Attempt {}/{}".format(attempt, max_attempts))
                    print("  Status:", r.status_code)
                    print("  URL:", r.url)
                    print("  Body:", body)
        except requests.RequestException as e:
            _warn("Crossref network error", error=str(e))
            if debug:
                print("[Crossref network error] Attempt {}/{}".format(attempt, max_attempts))
                print("  Error:", repr(e))
        time.sleep(1.2 * attempt)
    _warn("Crossref request exhausted", attempts=max_attempts, params=params)
    return None


def _query_crossref(title: str,
                    year: Optional[int],
                    journal: Optional[str],
                    authors: List[str],
                    rows: int,
                    debug: bool) -> List[dict]:
    """
    Execute Crossref query; return list of items (may be empty).
    """
    params = _build_params(title, year, journal, authors, rows=rows)
    res = _crossref_request(params, debug=debug)
    if not res:
        return []
    try:
        items = (res.get("message") or {}).get("items") or []
        return items
    except Exception:
        return []


def _query_crossref_biblio(blob: str, rows: int, debug: bool) -> List[dict]:
    params = {
        "query": blob,
        "rows": str(rows),
        "select": ",".join([
            "DOI", "title", "issued", "author", "container-title", "score",
            "volume", "issue", "page",
            "published-print", "published-online",
        ]),
        "mailto": _mailto(),
    }
    if debug:
        _debug("Crossref bibliographic query", blob_excerpt=blob[:200], rows=rows)
    res = _crossref_request(params, debug=debug)
    if not res:
        return []
    try:
        items = (res.get("message") or {}).get("items") or []
        return items
    except Exception:
        return []


# -----------------------
# Public entry
# -----------------------

def enrich_missing_with_crossref(limit: int = 300,
                                 threshold: int = 78,
                                 ua_email: Optional[str] = None,
                                 osf_id: Optional[str] = None,
                                 ref_id: Optional[str] = None,
                                 debug: bool = False,
                                 dump_misses: Optional[str] = None) -> Dict[str, int]:
    """
    For references missing a DOI (or weakly populated), try Crossref.
    If osf_id/ref_id supplied, only process that reference.

    Returns: {"checked": n, "updated": u, "failed": f}
    """
    logger.info("Crossref lookup start")

    checked = updated = failed = 0
    misses: List[Dict[str, str]] = []
    repo = PreprintsRepo()
    rows = repo.select_refs_missing_doi(
        limit=limit,
        osf_id=osf_id,
        ref_id=ref_id,
        include_existing=bool(ref_id and osf_id),
    )

    for r in rows:
        if ref_id and r.get("ref_id") != ref_id:
            continue
        checked += 1
        title = (r.get("title") or "").strip()
        raw_citation = (r.get("raw_citation") or "").strip()
        use_raw_only = False
        if not title and raw_citation:
            use_raw_only = True
            _debug("Crossref search using raw citation only", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"))
        if not title and not raw_citation:
            _debug("Skipping Crossref search: missing title and raw citation", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"))
            misses.append({
                "osf_id": r.get("osf_id"),
                "ref_id": r.get("ref_id"),
                "raw_citation": raw_citation,
                "reason": "missing-title-and-raw",
            })
            continue
        raw_year = _extract_year_from_raw(raw_citation)
        if debug and raw_citation:
            _debug("Crossref raw citation", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"), raw_excerpt=raw_citation[:240])
        if raw_year is not None:
            year = raw_year
        else:
            raw_year = r.get("year")
            year = int(raw_year) if raw_year is not None and str(raw_year).isdigit() else None
        journal = (r.get("journal") or "").strip() or None
        authors = r.get("authors") or []
        volume = (r.get("volume") or "").strip() or None
        issue = (r.get("issue") or "").strip() or None
        page = (r.get("page") or "").strip() or None

        meta = {
            "osf_id": r.get("osf_id"),
            "ref_id": r.get("ref_id"),
            "title_excerpt": title[:160],
            "authors": authors[:3],
            "year": year,
            "journal": journal,
            "using_raw_only": use_raw_only,
        }
        raw_mode = use_raw_only
        if use_raw_only:
            _debug("Crossref raw-only search", **meta)
            items = _query_crossref_biblio(raw_citation, rows=30, debug=debug)
        else:
            _debug("Crossref structured search", **meta)
            items = _query_crossref(title, year, journal, authors, rows=30, debug=debug)
            if not items and raw_citation:
                _debug("Crossref fallback to raw citation", **meta)
                items = _query_crossref_biblio(raw_citation, rows=30, debug=debug)
                raw_mode = True
        if not items:
            _debug("No Crossref candidates", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"))
            misses.append({
                "osf_id": r.get("osf_id"),
                "ref_id": r.get("ref_id"),
                "raw_citation": raw_citation,
                "reason": "no-candidates",
            })
            continue

        best, miss_reason = _pick_best(
            items,
            title,
            year,
            journal,
            threshold=threshold,
            debug=debug,
            raw_blob=raw_citation if raw_mode else None,
            structured_authors=authors,
            ref_volume=volume,
            ref_issue=issue,
            ref_page=page,
            raw_search=raw_mode,
        )
        if not best:
            _debug("No good Crossref match", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"), candidates=len(items))
            misses.append({
                "osf_id": r.get("osf_id"),
                "ref_id": r.get("ref_id"),
                "raw_citation": raw_citation,
                "reason": miss_reason or "no-good-match",
            })
            continue

        doi = best.get("DOI")
        if not doi:
            _debug("Best Crossref candidate missing DOI", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"))
            continue

        # Update DB
        try:
            ok = repo.update_reference_doi(r.get("osf_id"), r.get("ref_id"), doi, source="crossref")
            updated += 1 if ok else 0
        except Exception:
            failed += 1
            _exception("Dynamo update error in Crossref enrichment", osf_id=r.get("osf_id"), ref_id=r.get("ref_id"), doi=doi)

    logger.info("Crossref enrichment complete")
    if dump_misses and misses:
        import csv
        try:
            with open(dump_misses, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["osf_id", "ref_id", "raw_citation", "reason"])
                writer.writeheader()
                writer.writerows(misses)
            _info("Crossref misses written", path=dump_misses, count=len(misses))
        except Exception as e:
            _warn("Failed to write Crossref misses CSV", path=dump_misses, error=str(e))
    return {"checked": checked, "updated": updated, "failed": failed}
