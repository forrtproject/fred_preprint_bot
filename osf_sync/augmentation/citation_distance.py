"""Citation distance utilities for FLORA validation.

Compares a raw GROBID-extracted citation against the APA-formatted
citation resolved from doi.org using an ASCII-compact normalised
Levenshtein distance:

1. Strip URLs and DOI strings (including bare ``doi: 10.…`` prefixes).
2. NFKD-decompose Unicode (é → e + combining accent, etc.).
3. Drop everything except alphanumeric characters and lowercase.
4. Compute Levenshtein edit distance / max(len(a), len(b)).

This is intentionally different from the tiebreak distance in
``doi_multi_method_lookup`` which retains spacing/punctuation.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

from .doi_multi_method_lookup import _fetch_citation_for_doi

# Default distance threshold: references above this are flagged for review.
DISTANCE_THRESHOLD = 0.29

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_DOI_PREFIX_RE = re.compile(r"\bdoi:\s*10\.[0-9]{4,9}/\S+", re.IGNORECASE)
_BARE_DOI_RE = re.compile(r"10\.[0-9]{4,9}/\S+", re.IGNORECASE)


def _ascii_compact(s: str) -> str:
    """Reduce a citation to lowercase ASCII alphanumerics only."""
    s = _URL_RE.sub("", s)
    s = _DOI_PREFIX_RE.sub("", s)
    s = _BARE_DOI_RE.sub("", s)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]", "", s)
    return s.lower()


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) > len(b):
        a, b = b, a
    prev = list(range(len(a) + 1))
    for i, ch_b in enumerate(b, 1):
        curr = [i]
        for j, ch_a in enumerate(a, 1):
            cost = 0 if ch_a == ch_b else 1
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def fetch_apa_citation(doi: str) -> Optional[str]:
    """Fetch the APA-formatted citation for a DOI via doi.org / Crossref."""
    return _fetch_citation_for_doi(doi, style="apa")


def compute_citation_distance(raw_citation: str, apa_citation: str) -> float:
    """Return normalised edit distance in [0, 1] between two citation strings.

    Both strings are reduced to ASCII-compact form before comparison.
    """
    a = _ascii_compact(raw_citation or "")
    b = _ascii_compact(apa_citation or "")
    max_len = max(len(a), len(b))
    if max_len == 0:
        return 0.0
    return min(1.0, _levenshtein(a, b) / max_len)


def needs_validation(distance: float, threshold: Optional[float] = None) -> bool:
    """Return True when the distance exceeds the threshold."""
    if threshold is None:
        threshold = DISTANCE_THRESHOLD
    return distance > threshold
