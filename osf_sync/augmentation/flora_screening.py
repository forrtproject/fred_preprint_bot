from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from ..dynamo.preprints_repo import PreprintsRepo
from .flora_original_lookup import normalize_doi, lookup_originals_with_flora
from ..logging_setup import get_logger, with_extras

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
    if osf_ids:
        rows: List[Dict[str, Any]] = []
        for pid in osf_ids:
            rows.extend(
                repo.select_refs_with_flora_original(
                    limit=0,
                    osf_id=pid,
                    ref_id=ref_id,
                    include_missing_original=True,
                )
            )
        if limit:
            rows = rows[:limit]
    else:
        rows = repo.select_refs_with_flora_original(
            limit=limit,
            osf_id=osf_id,
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

    results: List[Dict[str, Any]] = []

    for pid, refs in grouped.items():
        # Collect all DOIs cited in this preprint (normalize)
        all_dois: Set[str] = set()
        for r in refs:
            d = normalize_doi(r.get("doi"))
            if d:
                all_dois.add(d)
        # Also consider other references without FLORA mapping (so fetch all DOI refs for this preprint).
        try:
            extra = repo.select_refs_with_doi(
                limit=0, osf_id=pid, only_unchecked=False)
        except TypeError:
            extra = repo.select_refs_with_doi(
                limit=0, osf_id=pid)  # fallback for older signature
        for r in extra:
            d = normalize_doi(r.get("doi"))
            if d:
                all_dois.add(d)

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

            # Not a baseline target: the cited DOI is not an original with known linked replications.
            if not replication_dois:
                if persist_flags:
                    try:
                        repo.update_reference_flora_screening(
                            pid,
                            refid,
                            original_cited=False,
                        )
                    except Exception as e:
                        _warn("Failed to persist FLORA screening flag",
                              osf_id=pid, ref_id=refid, error=str(e))
                continue

            replication_cited = any(
                (d in all_dois) for d in replication_dois)

            if persist_flags:
                try:
                    repo.update_reference_flora_screening(
                        pid,
                        refid,
                        original_cited=replication_cited,
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
            }
            retained_refs.append(payload)
            if not replication_cited:
                eligible_refs.append(payload)

        if eligible_refs:
            results.append({
                "osf_id": pid,
                "eligible": True,
                "eligible_count": len(eligible_refs),
                "replication_refs": retained_refs,
            })
        else:
            results.append({
                "osf_id": pid,
                "eligible": False,
                "eligible_count": 0,
                "replication_refs": retained_refs,
            })

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
