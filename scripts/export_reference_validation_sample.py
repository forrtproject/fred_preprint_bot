#!/usr/bin/env python3
"""
Export a DOI/reference validation sample from DynamoDB.

Key properties:
- Targets exactly N rows with distinct raw GROBID references.
- Uses many preprints (base + extra) to improve diversity.
- Resolves APA citations defensively (cache/reuse/checkpoint/retry/backoff/rate-limit).
- Writes CSV as UTF-8 with BOM (`utf-8-sig`) for Excel.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import threading
import time
import html
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote

import boto3
import requests
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from osf_sync.dynamo.api_cache_repo import ApiCacheRepo


DOI_RE = re.compile(r"10\.[0-9]{4,9}/\S+", re.IGNORECASE)
WS_RE = re.compile(r"\s+")
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


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return WS_RE.sub(" ", text).strip()


def _decode_response_text(resp: requests.Response) -> str:
    """
    Decode response bytes robustly (UTF-8 first) to avoid mojibake.
    """
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
    """
    Repair common UTF-8-decoded-as-latin-1/cp1252 artifacts in legacy cache rows.
    """
    s = _clean_text(html.unescape(text))
    if not s:
        return s

    def _apply_replacements(candidate: str) -> str:
        out = candidate
        for bad, good in MOJIBAKE_REPLACEMENTS.items():
            out = out.replace(bad, good)
        return _clean_text(out)

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
    # Try up to two rounds of latin-1/cp1252 -> utf-8 recovery.
    for _ in range(2):
        next_frontier: List[str] = []
        for base in frontier:
            for source in {base, base.translate(TRANSCODE_PRENORMALIZE)}:
                for enc in ("latin-1", "cp1252"):
                    try:
                        fixed = _clean_text(source.encode(enc).decode("utf-8"))
                    except Exception:
                        continue
                    fixed = _apply_replacements(fixed)
                    if fixed and fixed not in candidates:
                        candidates.add(fixed)
                        next_frontier.append(fixed)
        if not next_frontier:
            break
        frontier = next_frontier

    # Prefer lowest mojibake score; tie-break on longer text to avoid lossy fixes.
    ranked = sorted(candidates, key=lambda c: (_score(c), -len(c)))
    return ranked[0] if ranked else s


def _mojibake_penalty(text: str) -> int:
    if not text:
        return 0
    candidate = _clean_text(text)
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


def _normalize_raw_key(raw: Any) -> str:
    return _clean_text(raw).lower()


def _normalize_doi(doi: Any, source: str = "") -> str:
    value = _clean_text(doi).lower()
    if not value:
        return ""
    value = value.replace("\u200b", "").replace("\ufeff", "")
    for prefix in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "dx.doi.org/",
        "doi.org/",
        "doi:",
    ):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break

    first = value.find("10.")
    if first == -1:
        return ""
    value = value[first:]

    match = DOI_RE.search(value)
    if match:
        value = match.group(0)
    value = value.split("?", 1)[0].split("#", 1)[0].strip()
    value = value.strip(" \t\r\n\"'<>[]{}")
    if source.lower() not in {"crossref", "openalex"}:
        value = value.strip(".,;:")
        while value.endswith(")") and value.count("(") < value.count(")"):
            value = value[:-1].rstrip()
    if not value or not value.startswith("10."):
        return ""
    if not DOI_RE.fullmatch(value):
        return ""
    return value


def _doi_cite_cache_key(doi: str, style: str = "apa") -> str:
    payload = {"doi": doi, "style": style}
    blob = json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str)
    return f"doi_cite::{blob}"


def _extract_flora_apa_for_doi(row: Dict[str, Any], doi: str) -> Tuple[str, str]:
    pairs = row.get("flora_ref_pairs")
    if not isinstance(pairs, list):
        return "", ""
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        doi_o = _normalize_doi(pair.get("doi_o"))
        doi_r = _normalize_doi(pair.get("doi_r"))
        if doi == doi_o:
            apa = _clean_text(pair.get("apa_ref_o"))
            if apa:
                return apa, "flora"
        if doi == doi_r:
            apa = _clean_text(pair.get("apa_ref_r"))
            if apa:
                return apa, "flora"
    return "", ""


def _scan_eligible_reference_rows(
    table,
    projection_expression: str,
    *,
    exclude_tei_dois: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    filter_expr = (
        Attr("doi").exists()
        & Attr("doi").ne("")
        & Attr("raw_citation").exists()
        & Attr("raw_citation").ne("")
    )
    if exclude_tei_dois:
        filter_expr = filter_expr & Attr("doi_source").ne("tei")

    last = None
    while True:
        kwargs: Dict[str, Any] = {
            "ProjectionExpression": projection_expression,
            "FilterExpression": filter_expr,
        }
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        out.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
    return out


def _select_preprints(
    by_osf: Dict[str, List[Dict[str, Any]]],
    *,
    preprint_count: int,
    sample_size: int,
    seed: int,
) -> List[str]:
    all_ids = list(by_osf.keys())
    if len(all_ids) < preprint_count:
        raise ValueError(f"Need {preprint_count} preprints, but only {len(all_ids)} available.")

    rng = random.Random(seed)
    per_preprint_quota = max(1, sample_size // preprint_count)
    ids_with_quota = [osf_id for osf_id, rows in by_osf.items() if len(rows) >= per_preprint_quota]
    if len(ids_with_quota) >= preprint_count:
        return rng.sample(ids_with_quota, preprint_count)

    ranked = sorted(all_ids, key=lambda osf_id: len(by_osf[osf_id]), reverse=True)
    max_possible = sum(len(by_osf[osf_id]) for osf_id in ranked[:preprint_count])
    if max_possible < sample_size:
        raise ValueError(
            f"Cannot sample {sample_size} rows across {preprint_count} preprints. "
            f"Max possible is {max_possible}."
        )

    pool_size = min(len(ranked), max(preprint_count * 3, preprint_count))
    pool = ranked[:pool_size]
    chosen = rng.sample(pool, preprint_count) if len(pool) > preprint_count else list(pool)
    if sum(len(by_osf[x]) for x in chosen) < sample_size:
        chosen = ranked[:preprint_count]
    return chosen


def _round_robin_rows(
    by_osf: Dict[str, List[Dict[str, Any]]],
    osf_ids: Sequence[str],
    *,
    seed: int,
) -> Iterator[Dict[str, Any]]:
    rng = random.Random(seed)
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    active: List[str] = []
    for osf_id in osf_ids:
        rows = list(by_osf.get(osf_id, []))
        if not rows:
            continue
        rng.shuffle(rows)
        buckets[osf_id] = rows
        active.append(osf_id)

    while active:
        rng.shuffle(active)
        next_active: List[str] = []
        for osf_id in active:
            bucket = buckets[osf_id]
            if not bucket:
                continue
            yield bucket.pop()
            if bucket:
                next_active.append(osf_id)
        active = next_active


def _distinct_by_raw(
    rows: Iterator[Dict[str, Any]],
    *,
    sample_size: int,
    seen_raw: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    out: List[Dict[str, Any]] = []
    seen = set(seen_raw or set())
    for row in rows:
        key = _normalize_raw_key(row.get("raw_citation"))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row)
        if len(out) >= sample_size:
            break
    return out, seen


def _build_distinct_raw_sample(
    by_osf: Dict[str, List[Dict[str, Any]]],
    *,
    preprint_count: int,
    extra_preprints: int,
    sample_size: int,
    seed: int,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    total_preprints = preprint_count + extra_preprints
    selected = _select_preprints(
        by_osf,
        preprint_count=total_preprints,
        sample_size=sample_size,
        seed=seed,
    )

    primary_iter = _round_robin_rows(by_osf, selected, seed=seed + 1)
    sampled, seen_raw = _distinct_by_raw(primary_iter, sample_size=sample_size)

    if len(sampled) < sample_size:
        remaining_ids = [osf_id for osf_id in by_osf.keys() if osf_id not in set(selected)]
        random.Random(seed + 2).shuffle(remaining_ids)
        remaining_iter = _round_robin_rows(by_osf, remaining_ids, seed=seed + 3)
        extra_needed = sample_size - len(sampled)
        extra_rows, seen_raw = _distinct_by_raw(
            remaining_iter,
            sample_size=extra_needed,
            seen_raw=seen_raw,
        )
        sampled.extend(extra_rows)

    if len(sampled) < sample_size:
        raise ValueError(
            f"Only found {len(sampled)} distinct raw references; need {sample_size}."
        )

    random.Random(seed + 4).shuffle(sampled)
    return sampled[:sample_size], selected


def _lookup_cached_apa_for_dois(
    cache_repo: ApiCacheRepo,
    doi_list: Sequence[str],
    *,
    ttl_seconds: int,
) -> Tuple[Dict[str, str], Set[str]]:
    cached_hits: Dict[str, str] = {}
    cached_negative: Set[str] = set()
    for doi in doi_list:
        item = cache_repo.get(_doi_cite_cache_key(doi))
        if not item:
            continue
        if not cache_repo.is_fresh(item, ttl_seconds=ttl_seconds):
            continue
        payload = item.get("payload")
        if isinstance(payload, dict) and payload.get("_none") is True:
            cached_negative.add(doi)
            continue
        if payload is None:
            cached_negative.add(doi)
            continue
        citation = _clean_text(payload)
        if citation:
            cached_hits[doi] = citation
        else:
            cached_negative.add(doi)
    return cached_hits, cached_negative


def _load_reuse_csv(path: Optional[str]) -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    if not path:
        return out
    p = Path(path)
    if not p.exists():
        return out
    try:
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                doi = _normalize_doi(row.get("doi"))
                apa = _repair_common_mojibake(_clean_text(row.get("apa_reference")))
                src = _clean_text(row.get("apa_source")) or "reused_csv"
                if doi and apa:
                    out[doi] = (apa, src)
    except Exception:
        return {}
    return out


def _load_checkpoint(path: Optional[str]) -> Dict[str, Tuple[str, str]]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: Dict[str, Tuple[str, str]] = {}
    if not isinstance(raw, dict):
        return out
    for doi, rec in raw.items():
        if not isinstance(rec, dict):
            continue
        apa = _repair_common_mojibake(_clean_text(rec.get("apa_reference")))
        src = _clean_text(rec.get("apa_source")) or "checkpoint"
        norm = _normalize_doi(doi)
        if norm and apa:
            out[norm] = (apa, src)
    return out


def _save_checkpoint(path: Optional[str], mapping: Dict[str, Tuple[str, str]]) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        doi: {"apa_reference": apa, "apa_source": src}
        for doi, (apa, src) in mapping.items()
        if doi and apa
    }
    p.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


class DefensiveCitationResolver:
    def __init__(
        self,
        *,
        connect_timeout: float,
        read_timeout: float,
        max_attempts: int,
        max_rps: float,
    ) -> None:
        self.timeout = (max(1.0, connect_timeout), max(2.0, read_timeout))
        self.max_attempts = max(1, int(max_attempts))
        self.min_interval = 1.0 / max(max_rps, 0.1)
        self._lock = threading.Lock()
        self._next_allowed = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "flora-preprint-notifier/validation-export",
                "Accept-Language": "en",
            }
        )

    def _throttle(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
            self._next_allowed = time.monotonic() + self.min_interval

    def _request(
        self,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, int]:
        last_status = 0
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            try:
                resp = self.session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout,
                )
                last_status = int(resp.status_code)
                body = _repair_common_mojibake(_decode_response_text(resp))
                if resp.status_code == 200 and body:
                    return body, 200

                transient = resp.status_code in {408, 425, 429, 500, 502, 503, 504}
                if not transient:
                    return "", last_status

                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = min(30.0, float(retry_after))
                else:
                    delay = min(30.0, 0.5 * (2 ** (attempt - 1)))
                delay = delay + random.uniform(0.0, 0.35)
                time.sleep(delay)
            except (requests.Timeout, requests.ConnectionError):
                delay = min(30.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35)
                time.sleep(delay)
            except requests.RequestException:
                return "", last_status
        return "", last_status

    def resolve(self, doi: str) -> Tuple[str, str]:
        if not doi:
            return "", "missing"
        safe = quote(doi, safe="/")
        best_text = ""
        best_source = "missing"
        best_penalty = 10**9

        text, status = self._request(
            f"https://doi.org/{safe}",
            headers={"Accept": "text/x-bibliography; style=apa"},
        )
        if text:
            penalty = _mojibake_penalty(text)
            best_text, best_source, best_penalty = text, "doi.org", penalty
            if penalty == 0:
                return text, "doi.org"

        # Crossref fallback helps for many (but not all) registrations.
        text, _ = self._request(
            f"https://api.crossref.org/works/{safe}/transform",
            headers={"Accept": "text/x-bibliography; style=apa"},
        )
        if text:
            penalty = _mojibake_penalty(text)
            if penalty < best_penalty:
                best_text, best_source, best_penalty = text, "crossref", penalty
            if penalty == 0:
                return text, "crossref"

        if best_text:
            return best_text, best_source

        if status in {408, 425, 429, 500, 502, 503, 504}:
            return "", "missing_transient"
        return "", "missing"


def main() -> int:
    load_dotenv(".env")

    ap = argparse.ArgumentParser(description="Export DOI-reference validation sample CSV.")
    ap.add_argument("--table", default=os.environ.get("DDB_TABLE_REFERENCES", "prod_preprint_references"))
    ap.add_argument("--cache-table", default=os.environ.get("DDB_TABLE_API_CACHE", "prod_api_cache"))
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))
    ap.add_argument("--sample-size", type=int, default=2000)
    ap.add_argument("--preprint-count", type=int, default=200)
    ap.add_argument("--extra-preprints", type=int, default=40)
    ap.add_argument("--seed", type=int, default=20260221)
    ap.add_argument("--citation-workers", type=int, default=4)
    ap.add_argument("--connect-timeout", type=float, default=4.0)
    ap.add_argument("--read-timeout", type=float, default=18.0)
    ap.add_argument("--max-attempts", type=int, default=6)
    ap.add_argument("--max-rps", type=float, default=3.0)
    ap.add_argument(
        "--cache-ttl-seconds",
        type=int,
        default=int(os.environ.get("DOI_MULTI_METHOD_CACHE_TTL_SECS", str(7 * 24 * 3600))),
    )
    ap.add_argument("--negative-cache-ttl-seconds", type=int, default=24 * 3600)
    ap.add_argument("--checkpoint", default="validation/doi_citation_checkpoint.json")
    ap.add_argument("--reuse-csv", default="validation/reference_validation_sample_2000.csv")
    ap.add_argument(
        "--output",
        default="validation/reference_validation_sample_2000_distinct_raw.csv",
        help="Output CSV path (UTF-8 with BOM for Excel via utf-8-sig).",
    )
    ap.add_argument("--exclude-tei-dois", action="store_true")
    args = ap.parse_args()

    ddb = boto3.resource(
        "dynamodb",
        region_name=args.region,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
    table = ddb.Table(args.table)
    os.environ["DDB_TABLE_API_CACHE"] = args.cache_table
    cache_repo = ApiCacheRepo()
    cache_enabled = True

    print(f"Reading references from {args.table} in {args.region} ...")
    projection = "osf_id, ref_id, raw_citation, doi, doi_source, flora_ref_pairs"
    all_rows = _scan_eligible_reference_rows(
        table,
        projection,
        exclude_tei_dois=bool(args.exclude_tei_dois),
    )
    print(f"Eligible rows scanned: {len(all_rows)}")

    source_counts = Counter()
    by_osf: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        source = _clean_text(row.get("doi_source"))
        doi = _normalize_doi(row.get("doi"), source=source)
        raw = _clean_text(row.get("raw_citation"))
        osf_id = _clean_text(row.get("osf_id"))
        if not doi or not raw or not osf_id:
            continue
        row["doi"] = doi
        row["raw_citation"] = raw
        source = source or "(missing)"
        source_counts[source] += 1
        by_osf[osf_id].append(row)

    print(f"Eligible distinct preprints: {len(by_osf)}")
    print(f"Eligible source distribution: {dict(source_counts)}")

    sampled, selected_preprints = _build_distinct_raw_sample(
        by_osf,
        preprint_count=args.preprint_count,
        extra_preprints=args.extra_preprints,
        sample_size=args.sample_size,
        seed=args.seed,
    )
    sampled_preprints = len({_clean_text(r.get("osf_id")) for r in sampled})
    distinct_raw = len({_normalize_raw_key(r.get("raw_citation")) for r in sampled})
    print(
        f"Sampled rows: {len(sampled)} with {distinct_raw} distinct raw refs "
        f"across {sampled_preprints} represented preprints "
        f"(selected pool {len(selected_preprints)} = {args.preprint_count}+{args.extra_preprints})"
    )

    unique_dois = sorted(
        {
            _normalize_doi(r.get("doi"), source=_clean_text(r.get("doi_source")))
            for r in sampled
            if _normalize_doi(r.get("doi"), source=_clean_text(r.get("doi_source")))
        }
    )
    print(f"Unique DOIs in sample: {len(unique_dois)}")

    citation_by_doi: Dict[str, str] = {}
    citation_source_by_doi: Dict[str, str] = {}

    # 1) FLORA on-row
    flora_hits = 0
    for row in sampled:
        doi = _normalize_doi(row.get("doi"), source=_clean_text(row.get("doi_source")))
        if not doi or doi in citation_by_doi:
            continue
        apa_ref, apa_source = _extract_flora_apa_for_doi(row, doi)
        if apa_ref:
            citation_by_doi[doi] = apa_ref
            citation_source_by_doi[doi] = apa_source
            flora_hits += 1

    # 2) Reuse previous CSV results
    reused = _load_reuse_csv(args.reuse_csv)
    reuse_hits = 0
    for doi in unique_dois:
        if doi in citation_by_doi:
            continue
        rec = reused.get(doi)
        if rec:
            citation_by_doi[doi], citation_source_by_doi[doi] = rec
            reuse_hits += 1

    # 3) Cache table
    unresolved = [doi for doi in unique_dois if doi not in citation_by_doi]
    try:
        cached_hits, cached_negative = _lookup_cached_apa_for_dois(
            cache_repo,
            unresolved,
            ttl_seconds=int(args.cache_ttl_seconds),
        )
    except Exception as e:
        cache_enabled = False
        cached_hits, cached_negative = {}, set()
        print(f"Warning: API cache unavailable, skipping cache reads ({e})")

    for doi, citation in cached_hits.items():
        citation_by_doi[doi] = citation
        citation_source_by_doi[doi] = "api_cache"

    # 4) Checkpoint from previous partial runs
    checkpoint_hits = 0
    checkpoint_data = _load_checkpoint(args.checkpoint)
    for doi, (citation, source) in checkpoint_data.items():
        if doi in citation_by_doi:
            continue
        citation_by_doi[doi] = citation
        citation_source_by_doi[doi] = source or "checkpoint"
        checkpoint_hits += 1

    # Refresh only suspiciously encoded citations instead of trusting stale cache rows.
    suspicious = {
        doi
        for doi, citation in list(citation_by_doi.items())
        if _mojibake_penalty(citation) > 0
    }
    if suspicious:
        for doi in suspicious:
            citation_by_doi.pop(doi, None)
            citation_source_by_doi.pop(doi, None)
        print(f"Suspicious cached/reused APA rows to refresh: {len(suspicious)}")

    external_dois = [doi for doi in unique_dois if doi not in citation_by_doi and doi not in cached_negative]
    print(
        "APA resolution plan: "
        f"flora={flora_hits}, reuse_csv={reuse_hits}, cache_hits={len(cached_hits)}, "
        f"checkpoint_hits={checkpoint_hits}, cache_negative={len(cached_negative)}, "
        f"external_needed={len(external_dois)}"
    )

    resolver = DefensiveCitationResolver(
        connect_timeout=float(args.connect_timeout),
        read_timeout=float(args.read_timeout),
        max_attempts=int(args.max_attempts),
        max_rps=float(args.max_rps),
    )

    checkpoint_mut: Dict[str, Tuple[str, str]] = dict(checkpoint_data)
    external_success = 0
    if external_dois:
        workers = max(1, int(args.citation_workers))
        print(
            f"Resolving {len(external_dois)} DOI citations externally "
            f"with {workers} workers, max_rps={args.max_rps}, attempts={args.max_attempts} ..."
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(resolver.resolve, doi): doi for doi in external_dois}
            done = 0
            for fut in as_completed(futures):
                doi = futures[fut]
                try:
                    citation, source = fut.result()
                except Exception:
                    citation, source = "", "missing_exception"

                citation = _clean_text(citation)
                if citation:
                    citation_by_doi[doi] = citation
                    citation_source_by_doi[doi] = source
                    checkpoint_mut[doi] = (citation, source)
                    external_success += 1
                else:
                    citation_source_by_doi.setdefault(doi, source or "missing")

                done += 1
                if done % 100 == 0:
                    _save_checkpoint(args.checkpoint, checkpoint_mut)
                    print(f"Resolved {done}/{len(external_dois)} external DOI citations ...")

    _save_checkpoint(args.checkpoint, checkpoint_mut)

    # Optional cache writeback
    if cache_enabled:
        for doi in external_dois:
            if doi in citation_by_doi:
                try:
                    cache_repo.put(
                        _doi_cite_cache_key(doi),
                        citation_by_doi[doi],
                        source="doi_validation_export",
                        ttl_seconds=int(args.cache_ttl_seconds),
                        status=True,
                    )
                except Exception as e:
                    cache_enabled = False
                    print(f"Warning: API cache unavailable, skipping further cache writes ({e})")
                    break
            else:
                try:
                    cache_repo.put(
                        _doi_cite_cache_key(doi),
                        None,
                        source="doi_validation_export",
                        ttl_seconds=int(args.negative_cache_ttl_seconds),
                        status=False,
                    )
                except Exception as e:
                    cache_enabled = False
                    print(f"Warning: API cache unavailable, skipping further cache writes ({e})")
                    break

    out_rows: List[Dict[str, Any]] = []
    for row in sampled:
        doi = _normalize_doi(row.get("doi"), source=_clean_text(row.get("doi_source")))
        out_rows.append(
            {
                "osf_id": _clean_text(row.get("osf_id")),
                "ref_id": _clean_text(row.get("ref_id")),
                "doi": doi,
                "doi_source": _clean_text(row.get("doi_source")) or "(missing)",
                "raw_reference_grobid": _clean_text(row.get("raw_citation")),
                "apa_reference": citation_by_doi.get(doi, ""),
                "apa_source": citation_source_by_doi.get(doi, "missing"),
            }
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "osf_id",
        "ref_id",
        "doi",
        "doi_source",
        "raw_reference_grobid",
        "apa_reference",
        "apa_source",
    ]
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    distinct_raw_final = len({_normalize_raw_key(r.get("raw_reference_grobid")) for r in out_rows})
    print(f"Wrote {len(out_rows)} rows to {out_path}")
    print(f"Distinct raw refs in output: {distinct_raw_final}/{len(out_rows)}")
    print(f"Distinct represented preprints in output: {len({r['osf_id'] for r in out_rows if r['osf_id']})}")
    print(f"Unique DOIs with APA available: {sum(1 for d in unique_dois if citation_by_doi.get(d))}/{len(unique_dois)}")
    print(f"External resolution success: {external_success}/{len(external_dois)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
