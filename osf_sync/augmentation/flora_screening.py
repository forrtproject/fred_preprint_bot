from __future__ import annotations
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

from ..dynamo.preprints_repo import PreprintsRepo
from .flora_original_lookup import (
    normalize_doi,
    _ensure_fresh_flora_csv,
    _load_flora_pairs_by_original,
    _resolve_flora_csv_path,
)
from ..logging_setup import get_logger, with_extras
from ..version_utils import base_id, sibling_ids
from .citation_distance import (
    compute_citation_distance,
    fetch_apa_citation,
    needs_validation,
)

_OSF_DOI_RE = re.compile(r"^(?P<prefix>10\.[\d]+/osf\.io/)(?P<id>[a-z0-9]+(?:_v\d+)?)$")

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


def _build_self_dois(preprint: Optional[Dict[str, Any]]) -> Set[str]:
    """Return the set of DOIs that identify the preprint itself.

    Used to drop FLoRA pairs that list the preprint as the replication of one
    of its own references — without this, a preprint that is itself a
    replication study gets emailed urging its authors to cite their own paper.

    Includes:
    - The preprint's linked version-of-record DOI (``doi`` attribute), if set.
    - The OSF preprint DOI of the current version, plus all sibling versions
      and the unversioned base, derived from ``links.preprint_doi``.
    """
    out: Set[str] = set()
    if not preprint:
        return out

    own_doi = normalize_doi(preprint.get("doi"))
    if own_doi:
        out.add(own_doi)

    links = preprint.get("links") or {}
    own_preprint_doi = normalize_doi(links.get("preprint_doi"))
    if not own_preprint_doi:
        return out
    out.add(own_preprint_doi)

    m = _OSF_DOI_RE.match(own_preprint_doi)
    if not m:
        return out
    prefix, variant_id = m.group("prefix"), m.group("id")
    base = base_id(variant_id)
    candidates = {base, variant_id, *sibling_ids(variant_id)}
    # If the preprint id is unversioned, also include _vN variants — FLoRA may
    # register the same paper under a versioned form even when our record is
    # unversioned.
    if base == variant_id:
        candidates.update(f"{base}_v{n}" for n in range(1, 31))
    for cid in candidates:
        d = normalize_doi(prefix + cid)
        if d:
            out.add(d)
    return out


def _validate_eligible_ref_distances(
    pid: str,
    eligible_refs: List[Dict[str, Any]],
    repo: PreprintsRepo,
) -> List[Dict[str, Any]]:
    """Compute citation distance for each eligible ref and flag high-distance ones.

    Returns the list of flagged refs (high distance, reviewer configured).

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

    if flagged:
        # Set pending flag for safety gating (review_id assigned later at batch level)
        try:
            repo.set_citation_validation_pending(pid, pending=True)
        except Exception as exc:
            _warn("Failed to set citation validation pending flag",
                  osf_id=pid, error=str(exc))

    return flagged


def lookup_and_screen_flora(
    *,
    limit: int = 0,
    osf_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    cache_path: Optional[str] = None,
    persist_flags: bool = True,
    only_unchecked: bool = True,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Merged FLORA lookup + screening in one pass per preprint.

    For each preprint: query all refs, match DOIs against FLORA CSV dict,
    run screening, write results. Writes per-preprint (flora_last_checked)
    and per-matched-ref only (flora_ref_pairs + flora_replication_cited).
    """
    repo = PreprintsRepo()

    # 1. Load FLORA CSV
    flora_path = _resolve_flora_csv_path(cache_path)
    refresh_meta = _ensure_fresh_flora_csv(flora_path, debug=debug)
    flora_pairs = _load_flora_pairs_by_original(flora_path)

    # 2. Select preprints to check
    preprint_ids = repo.select_preprints_for_flora_check(
        limit=limit,
        osf_id=osf_id,
        only_unchecked=only_unchecked,
    )

    # 3. Filter out preprints with sent emails (select_preprints_for_flora_check
    #    already excludes email_sent=True, but when osf_id is specified we need
    #    to apply it here)
    if osf_id:
        allowed = repo.filter_osf_ids_without_sent_email(preprint_ids)
        preprint_ids = [pid for pid in preprint_ids if pid in allowed]

    skipped_sent = 0  # tracked for stats compatibility

    def _process_preprint(pid: str) -> Dict[str, Any]:
        preprint = repo.t_preprints.get_item(Key={"osf_id": pid}).get("Item")
        self_dois = _build_self_dois(preprint)
        all_refs = _query_all_refs(repo, pid)
        all_dois = _extract_dois(all_refs)

        eligible_refs: List[Dict[str, Any]] = []
        retained_refs: List[Dict[str, Any]] = []

        for r in all_refs:
            refid = r.get("ref_id")
            if ref_id and refid != ref_id:
                continue
            ref_doi = normalize_doi(r.get("doi"))
            if not ref_doi:
                continue

            ref_pairs_for_doi = flora_pairs.get(ref_doi) or []
            if not ref_pairs_for_doi:
                continue

            # Drop FLoRA pairs whose replication DOI is the preprint itself —
            # those represent the preprint being listed as a replication of
            # one of its own references, and we must not email authors urging
            # them to cite their own paper.
            non_self_pairs = [
                obj for obj in ref_pairs_for_doi
                if normalize_doi(obj.get("doi_r")) not in self_dois
            ]
            self_pair_count = len(ref_pairs_for_doi) - len(non_self_pairs)
            if self_pair_count and debug:
                _info("Dropped self-replication FLoRA pairs",
                      osf_id=pid, ref_id=refid, doi=ref_doi,
                      dropped=self_pair_count)
            if not non_self_pairs:
                # Every replication for this ref is self. Treat as "cited" so
                # the ref drops out of the eligible set; on a re-run this also
                # overwrites previously-persisted self-only state.
                if persist_flags and self_pair_count:
                    try:
                        repo.update_reference_flora_result(
                            pid,
                            refid,
                            ref_pairs=[],
                            replication_cited=True,
                        )
                    except Exception as e:
                        _warn("Failed to clear self-only FLORA result",
                              osf_id=pid, ref_id=refid, error=str(e))
                continue

            # Filter to pairs where this ref's DOI is the original DOI
            matching_pairs = []
            for obj in non_self_pairs:
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

            if not replication_dois:
                continue

            replication_cited = any(d in all_dois for d in replication_dois)

            # Write combined lookup + screening result for this matched ref
            if persist_flags:
                try:
                    repo.update_reference_flora_result(
                        pid,
                        refid,
                        ref_pairs=non_self_pairs,
                        replication_cited=replication_cited,
                    )
                except Exception as e:
                    _warn("Failed to persist FLORA result",
                          osf_id=pid, ref_id=refid, error=str(e))

            payload = {
                "osf_id": pid,
                "ref_id": refid,
                "original_doi": ref_doi,
                "matching_replication_dois": replication_dois,
                "replication_cited": replication_cited,
                "raw_citation": r.get("raw_citation", ""),
            }
            retained_refs.append(payload)
            if not replication_cited:
                eligible_refs.append(payload)

        # Citation distance validation for eligible refs
        flagged_for_review: List[Dict[str, Any]] = []
        if persist_flags and eligible_refs:
            flagged_for_review = _validate_eligible_ref_distances(pid, eligible_refs, repo)

        # Update preprint: flora_last_checked + eligibility
        if persist_flags and hasattr(repo, "update_preprint_flora_eligibility"):
            try:
                repo.update_preprint_flora_eligibility(
                    pid,
                    eligible=bool(eligible_refs),
                    eligible_count=len(eligible_refs),
                )
            except Exception as e:
                _warn("Failed to persist preprint FLORA eligibility",
                      osf_id=pid, error=str(e))

        if debug:
            _info("FLORA check", osf_id=pid,
                  eligible_count=len(eligible_refs), total=len(retained_refs))

        return {
            "osf_id": pid,
            "eligible": bool(eligible_refs),
            "eligible_count": len(eligible_refs),
            "replication_refs": retained_refs,
            "flagged_for_review": flagged_for_review,
        }

    # Parallel processing
    results: List[Dict[str, Any]] = []
    workers = min(20, len(preprint_ids)) if preprint_ids else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_process_preprint, pid): pid
            for pid in preprint_ids
        }
        for future in as_completed(futures):
            pid = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                _warn("FLORA check failed for preprint", osf_id=pid, error=str(e))

    # Collect all flagged refs across preprints and send one batch review email
    all_flagged: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        flagged = r.get("flagged_for_review", [])
        if flagged:
            all_flagged[r["osf_id"]] = flagged

    if all_flagged:
        from ..runtime_config import RUNTIME_CONFIG
        if RUNTIME_CONFIG.validation.reviewer_email:
            review_id = f"review-batch-{uuid.uuid4().hex[:8]}"
            # Store batch review_id on each affected preprint
            for pid in all_flagged:
                try:
                    repo.set_citation_validation_pending(pid, pending=True, review_id=review_id)
                except Exception as exc:
                    _warn("Failed to set batch review_id",
                          osf_id=pid, review_id=review_id, error=str(exc))

            _info("Sending batch citation review email",
                  review_id=review_id,
                  preprints=len(all_flagged),
                  total_refs=sum(len(v) for v in all_flagged.values()))
            try:
                from ..email.citation_review import send_batch_citation_review_email
                send_batch_citation_review_email(all_flagged, review_id)
            except Exception as exc:
                _warn("Failed to send batch citation review email",
                      review_id=review_id, error=str(exc))

    lookup_stats = {
        "preprints_checked": len(results),
        "preprints_eligible": sum(1 for r in results if r.get("eligible")),
        "skipped_sent_preprint": skipped_sent,
        "csv_downloaded": 1 if refresh_meta.get("downloaded") else 0,
    }
    return {"lookup": lookup_stats, "screen": results}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Lookup + screen replication DOIs via FLORA local CSV (preprint-level).")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--osf_id", default=None)
    ap.add_argument("--only-osf-id", dest="osf_id", default=None,
                    help="Alias for --osf_id to process a single OSF id")
    ap.add_argument("--ref_id", default=None)
    ap.add_argument("--no-persist", action="store_true",
                    help="Do not write flags back to Dynamo.")
    ap.add_argument("--include-checked", action="store_true",
                    help="Re-run even for preprints recently checked")
    ap.add_argument("--cache-path", default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out = lookup_and_screen_flora(
        limit=args.limit,
        osf_id=args.osf_id,
        ref_id=args.ref_id,
        cache_path=args.cache_path,
        persist_flags=not args.no_persist,
        only_unchecked=not args.include_checked,
        debug=args.debug,
    )
    print(out)
