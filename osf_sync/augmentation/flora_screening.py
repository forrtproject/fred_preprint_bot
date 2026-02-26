from __future__ import annotations
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

from ..dynamo.preprints_repo import PreprintsRepo
from .flora_original_lookup import normalize_doi, lookup_originals_with_flora
from ..logging_setup import get_logger, with_extras
from .citation_distance import (
    compute_citation_distance,
    fetch_apa_citation,
    needs_validation,
)

logger = get_logger(__name__)


def _info(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).info(msg)
    else:
        logger.info(msg)


def _warn(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).warning(msg)
    else:
        logger.warning(msg)


def _query_all_refs(repo: PreprintsRepo, osf_id: str) -> List[Dict[str, Any]]:
    """Fetch all references for a preprint in a single unfiltered query."""
    items: List[Dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": "osf_id = :oid",
            "ExpressionAttributeValues": {":oid": osf_id},
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_refs.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def _extract_dois(refs: List[Dict[str, Any]]) -> Set[str]:
    """Extract and normalize all DOIs from a list of reference items."""
    dois: Set[str] = set()
    for r in refs:
        d = normalize_doi((r or {}).get("doi"))
        if d:
            dois.add(d)
    return dois


def _validate_eligible_ref_distances(
    pid: str,
    eligible_refs: List[Dict[str, Any]],
    repo: PreprintsRepo,
) -> None:
    """Compute citation distance for each eligible ref and flag high-distance ones.

    Distance and APA citation are always stored on the reference.

    Email gating (``flora_citation_validation_pending``) is only activated
    when ``validation.reviewer_email`` is configured.  Without a reviewer,
    high-distance refs are logged as warnings but do not block sending —
    there is no mechanism to resolve the pending state without a reviewer.

    Network failures (doi.org unreachable) are logged and silently passed
    through so they never block screening.
    """
    from ..runtime_config import RUNTIME_CONFIG

    reviewer_configured = bool(RUNTIME_CONFIG.validation.reviewer_email)
    flagged: List[Dict[str, Any]] = []

    for ref in eligible_refs:
        ref_doi = ref.get("original_doi")
        ref_id = ref.get("ref_id")
        if not ref_doi or not ref_id:
            continue

        # Skip refs already processed (may happen on re-runs)
        if ref.get("citation_validation_status") in ("pass", "pending_review", "approved", "rejected"):
            continue

        try:
            apa = fetch_apa_citation(ref_doi)
        except Exception as exc:
            _warn("APA citation fetch failed; skipping validation",
                  osf_id=pid, ref_id=ref_id, doi=ref_doi, error=str(exc))
            continue

        if not apa:
            _warn("No APA citation returned; skipping validation",
                  osf_id=pid, ref_id=ref_id, doi=ref_doi)
            continue

        raw = ref.get("raw_citation", "")
        distance = compute_citation_distance(raw, apa)
        validation_needed = needs_validation(distance, RUNTIME_CONFIG.validation.distance_threshold)
        # Only mark as pending_review when a reviewer can actually act on it
        status = "pending_review" if (validation_needed and reviewer_configured) else "pass"

        # Store result on the reference
        ref["citation_apa_resolved"] = apa
        ref["citation_distance"] = distance
        ref["citation_validation_status"] = status

        try:
            repo.update_reference_citation_distance(
                pid, ref_id,
                distance=distance,
                apa_citation=apa,
                validation_status=status,
            )
        except Exception as exc:
            _warn("Failed to persist citation distance",
                  osf_id=pid, ref_id=ref_id, error=str(exc))

        if validation_needed and not reviewer_configured:
            _warn(
                "High citation distance but no reviewer configured; proceeding without gate",
                osf_id=pid, ref_id=ref_id, doi=ref_doi,
                distance=f"{distance:.1%}",
            )
        elif validation_needed:
            flagged.append(ref)

    if not flagged:
        return

    review_id = f"review-{pid}-{uuid.uuid4().hex[:8]}"
    try:
        repo.set_citation_validation_pending(pid, pending=True, review_id=review_id)
    except Exception as exc:
        _warn("Failed to set citation validation pending flag",
              osf_id=pid, review_id=review_id, error=str(exc))
        return

    _info(
        "Citation validation pending",
        osf_id=pid,
        flagged_count=len(flagged),
        review_id=review_id,
    )

    try:
        from ..email.citation_review import send_citation_review_email
        send_citation_review_email(pid, flagged, review_id)
    except Exception as exc:
        _warn("Failed to send citation review email",
              osf_id=pid, review_id=review_id, error=str(exc))


def screen_flora_replications(
    *,
    limit: int = 500,
    osf_id: Optional[str] = None,
    osf_ids: Optional[List[str]] = None,
    ref_id: Optional[str] = None,
    persist_flags: bool = True,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    For each preprint, evaluate cited reference DOIs against FLORA original DOIs (doi_o).
    A reference is a baseline target only when its own DOI is an original with at least one
    linked replication DOI (doi_r). Then check whether any of those linked replications are
    already cited in the same preprint reference list.
    """
    repo = PreprintsRepo()
    preprint_dois: Dict[str, Set[str]] = {}

    if osf_ids or osf_id:
        # Combined fetch: read each partition once, split into flora rows + DOI set.
        pids_to_fetch = osf_ids if osf_ids else [osf_id]
        rows: List[Dict[str, Any]] = []
        for pid in pids_to_fetch:
            all_refs = _query_all_refs(repo, pid)
            preprint_dois[pid] = _extract_dois(all_refs)
            rows.extend(
                r for r in all_refs
                if r and r.get("flora_lookup_status") is not None
                and (not ref_id or r.get("ref_id") == ref_id)
            )
        if limit:
            rows = rows[:limit]
    else:
        # Scan path — DOIs will be pre-fetched after grouping.
        rows = repo.select_refs_with_flora_original(
            limit=limit,
            ref_id=ref_id,
            include_missing_original=True,
        )

    candidate_ids = sorted({(r or {}).get("osf_id") for r in rows if (r or {}).get("osf_id")})
    allowed_ids = repo.filter_osf_ids_without_sent_email(candidate_ids)
    rows = [r for r in rows if (r or {}).get("osf_id") in allowed_ids]

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        osfid = r.get("osf_id")
        if osfid:
            grouped[osfid].append(r)

    # Pre-fetch DOIs for preprints not covered by the combined fetch (scan path).
    missing_pids = [pid for pid in grouped if pid not in preprint_dois]
    if missing_pids:
        fetch_workers = min(20, len(missing_pids))
        with ThreadPoolExecutor(max_workers=fetch_workers) as pool:
            futures = {
                pool.submit(_query_all_refs, repo, pid): pid
                for pid in missing_pids
            }
            for future in as_completed(futures):
                pid = futures[future]
                try:
                    preprint_dois[pid] = _extract_dois(future.result())
                except Exception as e:
                    _warn("Failed to pre-fetch DOIs", osf_id=pid, error=str(e))
                    preprint_dois[pid] = set()

    def _process_preprint(pid: str, refs: List[Dict[str, Any]]) -> Dict[str, Any]:
        # All DOIs for this preprint were pre-fetched into preprint_dois.
        all_dois: Set[str] = set(preprint_dois.get(pid, set()))

        eligible_refs: List[Dict[str, Any]] = []
        retained_refs: List[Dict[str, Any]] = []

        for r in refs:
            refid = r.get("ref_id")
            ref_doi = normalize_doi(r.get("doi"))

            # Keep only FLORA pairs where the current cited reference is an original DOI.
            ref_objs = r.get("flora_ref_pairs") or []
            matching_pairs = []
            for obj in ref_objs:
                doi_o = normalize_doi(obj.get("doi_o"))
                doi_r = normalize_doi(obj.get("doi_r"))
                if ref_doi and doi_o and ref_doi == doi_o:
                    matching_pairs.append({"doi_o": doi_o, "doi_r": doi_r})

            replication_dois: List[str] = []
            seen_replication_dois: Set[str] = set()
            for p in matching_pairs:
                doi_r = p.get("doi_r")
                if not doi_r or doi_r in seen_replication_dois:
                    continue
                seen_replication_dois.add(doi_r)
                replication_dois.append(doi_r)

            # Not a baseline target: skip without writing — no downstream consumer
            # needs a flag on refs that aren't FLORA originals.
            if not replication_dois:
                continue

            replication_cited = any(
                (d in all_dois) for d in replication_dois)

            if persist_flags:
                try:
                    repo.update_reference_flora_screening(
                        pid,
                        refid,
                        replication_cited=replication_cited,
                    )
                except Exception as e:
                    _warn("Failed to persist FLORA screening flag",
                          osf_id=pid, ref_id=refid, error=str(e))

            payload = {
                "osf_id": pid,
                "ref_id": refid,
                "original_doi": ref_doi,
                "matching_replication_dois": replication_dois,
                "replication_cited": replication_cited,
                # raw_citation carried for citation distance validation below
                "raw_citation": r.get("raw_citation", ""),
            }
            retained_refs.append(payload)
            if not replication_cited:
                eligible_refs.append(payload)

        # --- Citation distance validation for eligible refs ---
        if persist_flags and eligible_refs:
            _validate_eligible_ref_distances(pid, eligible_refs, repo)

        if persist_flags and hasattr(repo, "update_preprint_flora_eligibility"):
            try:
                repo.update_preprint_flora_eligibility(
                    pid,
                    eligible=bool(eligible_refs),
                    eligible_count=len(eligible_refs),
                )
            except Exception as e:
                _warn("Failed to persist preprint FLORA eligibility", osf_id=pid, error=str(e))

        if debug:
            _info("FLORA screening", osf_id=pid, eligible_count=len(
                eligible_refs), total=len(retained_refs))

        if eligible_refs:
            return {
                "osf_id": pid,
                "eligible": True,
                "eligible_count": len(eligible_refs),
                "replication_refs": retained_refs,
            }
        return {
            "osf_id": pid,
            "eligible": False,
            "eligible_count": 0,
            "replication_refs": retained_refs,
        }

    results: List[Dict[str, Any]] = []
    max_workers = min(20, len(grouped)) if grouped else 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_process_preprint, pid, refs): pid
            for pid, refs in grouped.items()
        }
        for future in as_completed(futures):
            pid = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                _warn("FLORA screening failed for preprint", osf_id=pid, error=str(e))

    return results


def lookup_and_screen_flora(
    *,
    limit_lookup: int = 0,
    limit_screen: int = 0,
    osf_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    cache_ttl_hours: Optional[int] = None,
    ignore_cache: bool = False,
    persist_flags: bool = True,
    only_unchecked: bool = True,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Convenience wrapper: first run FLORA local CSV lookup to populate original DOIs, then screen.
    Returns {"lookup": {...stats...}, "screen": [...results...]}.
    """
    lookup_stats = lookup_originals_with_flora(
        limit=limit_lookup,
        osf_id=osf_id,
        ref_id=ref_id,
        only_unchecked=only_unchecked,
        cache_ttl_hours=cache_ttl_hours,
        ignore_cache=ignore_cache,
        debug=debug,
    )
    scoped_osf_ids: Optional[List[str]] = None
    if not osf_id and not ref_id and only_unchecked:
        touched = lookup_stats.get("processed_osf_ids")
        if isinstance(touched, list):
            scoped_osf_ids = [str(v) for v in touched if v]
    screen_results = screen_flora_replications(
        limit=limit_screen,
        osf_id=osf_id,
        osf_ids=scoped_osf_ids,
        ref_id=ref_id,
        persist_flags=persist_flags,
        debug=debug,
    )
    return {"lookup": lookup_stats, "screen": screen_results}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Screen replication DOIs via FLORA local CSV comparison.")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--limit-lookup", type=int, default=0,
                    help="How many rows to run through FLORA local CSV lookup before screening (0 = unlimited)")
    ap.add_argument("--osf_id", default=None)
    ap.add_argument("--only-osf-id", dest="osf_id", default=None,
                    help="Alias for --osf_id to process a single OSF id")
    ap.add_argument("--ref_id", default=None)
    ap.add_argument("--no-persist", action="store_true",
                    help="Do not write screening flags back to Dynamo.")
    ap.add_argument("--no-lookup-first", action="store_true",
                    help="Skip the lookup stage and only screen existing FLORA results")
    ap.add_argument("--include-checked", action="store_true",
                    help="Re-run lookup even for rows with prior FLORA status")
    ap.add_argument("--cache-ttl-hours", type=int, default=None)
    ap.add_argument("--ignore-cache", action="store_true",
                    help="Deprecated; ignored for local FLORA CSV lookup")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.no_lookup_first:
        out = screen_flora_replications(
            limit=args.limit,
            osf_id=args.osf_id,
            ref_id=args.ref_id,
            persist_flags=not args.no_persist,
            debug=args.debug,
        )
        print(out)
    else:
        out = lookup_and_screen_flora(
            limit_lookup=args.limit_lookup,
            limit_screen=args.limit,
            osf_id=args.osf_id,
            ref_id=args.ref_id,
            cache_ttl_hours=args.cache_ttl_hours,
            ignore_cache=args.ignore_cache,
            persist_flags=not args.no_persist,
            only_unchecked=not args.include_checked,
            debug=args.debug,
        )
        print(out)
