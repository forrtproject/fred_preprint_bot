"""Citation distance utilities for FLORA validation.

Compares a raw GROBID-extracted citation against the APA-formatted
citation resolved from doi.org.  Uses ASCII-normalised Levenshtein
distance (same implementation as the DOI tiebreak in
doi_multi_method_lookup) divided by the length of the longer string.
"""
from __future__ import annotations

from typing import Optional

from .doi_multi_method_lookup import (
    _fetch_citation_for_doi,
    _normalized_string_distance,
)

# Default distance threshold: references above this are flagged for review.
DISTANCE_THRESHOLD = 0.29


def fetch_apa_citation(doi: str) -> Optional[str]:
    """Fetch the APA-formatted citation for a DOI via doi.org / Crossref."""
    return _fetch_citation_for_doi(doi, style="apa")


def compute_citation_distance(raw_citation: str, apa_citation: str) -> float:
    """Return normalised edit distance in [0, 1] between two citation strings."""
    return _normalized_string_distance(raw_citation, apa_citation)


def needs_validation(distance: float, threshold: Optional[float] = None) -> bool:
    """Return True when the distance exceeds the threshold."""
    if threshold is None:
        threshold = DISTANCE_THRESHOLD
    return distance > threshold
