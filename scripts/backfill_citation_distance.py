#!/usr/bin/env python3
"""Backfill citation distance for existing FLORA-matched eligible references.

Scans for references where:
  - flora_replication_cited = False  (replication NOT cited — eligible for email)
  - flora_ref_pairs exists        (FLORA-matched reference)
  - doi exists (non-empty)
  - citation_distance does not yet exist (not yet validated)

For each such reference, fetches the APA citation from doi.org and
computes the normalised Levenshtein distance against the raw GROBID
citation.  Results are stored on the reference.

After processing all refs for a preprint, if any were flagged (distance
above threshold) and the preprint has not yet been emailed, the
``flora_citation_validation_pending`` flag is set and (optionally) a
review email is dispatched to the configured reviewer.

Usage::

    python scripts/backfill_citation_distance.py \\
        --limit 200 \\
        --rate-limit 1.0 \\
        --send-reviews \\
        --dry-run

Flags::

    --limit N         Stop after processing N references (0 = unlimited).
    --rate-limit S    Minimum seconds between doi.org requests (default 1.0).
    --threshold T     Distance threshold (default from config, normally 0.29).
    --send-reviews    Send review emails for flagged preprints.
    --dry-run         Compute distances but do not write to DB or send emails.
    --osf-id ID       Restrict to a single preprint.
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from boto3.dynamodb.conditions import Attr
from osf_sync.dynamo.preprints_repo import PreprintsRepo
from osf_sync.augmentation.citation_distance import (
    compute_citation_distance,
    fetch_apa_citation,
    needs_validation,
    DISTANCE_THRESHOLD,
)
from osf_sync.runtime_config import RUNTIME_CONFIG


def _scan_eligible_refs(
    repo: PreprintsRepo,
    osf_id: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Scan refs with flora_replication_cited=False, flora_ref_pairs set, doi set, citation_distance absent."""
    filter_expr = (
        Attr("flora_replication_cited").eq(False)
        & Attr("flora_ref_pairs").exists()
        & Attr("doi").exists()
        & Attr("doi").ne("")
        & Attr("citation_distance").not_exists()
    )
    if osf_id:
        filter_expr = filter_expr & Attr("osf_id").eq(osf_id)

    rows: List[Dict[str, Any]] = []
    last_key = None
    page_size = min(100, limit) if limit else 100

    while True:
        kwargs: Dict[str, Any] = {
            "FilterExpression": filter_expr,
            "Limit": page_size,
            "ProjectionExpression": (
                "osf_id, ref_id, doi, raw_citation, citation_validation_status"
            ),
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_refs.scan(**kwargs)
        rows.extend(resp.get("Items", []))
        if limit and len(rows) >= limit:
            return rows[:limit]
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return rows


def _preprint_already_emailed(repo: PreprintsRepo, osf_id: str) -> bool:
    item = repo.t_preprints.get_item(
        Key={"osf_id": osf_id},
        ProjectionExpression="email_sent",
    ).get("Item") or {}
    return bool(item.get("email_sent"))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Backfill citation distance for FLORA-matched eligible references."
    )
    ap.add_argument("--limit", type=int, default=0,
                    help="Max references to process (0 = unlimited)")
    ap.add_argument("--rate-limit", type=float, default=1.0,
                    help="Seconds between doi.org requests")
    ap.add_argument("--threshold", type=float, default=None,
                    help="Distance threshold (default from runtime config)")
    ap.add_argument("--send-reviews", action="store_true",
                    help="Send review emails for newly flagged preprints")
    ap.add_argument("--dry-run", action="store_true",
                    help="Compute distances but do not persist or send emails")
    ap.add_argument("--osf-id", default=None,
                    help="Restrict to a single preprint osf_id")
    args = ap.parse_args()

    threshold = args.threshold if args.threshold is not None else RUNTIME_CONFIG.validation.distance_threshold

    repo = PreprintsRepo()
    rows = _scan_eligible_refs(repo, args.osf_id, args.limit)
    print(f"Found {len(rows)} references to process (threshold={threshold:.0%})", flush=True)

    # Group by osf_id so we can send one review email per preprint
    by_preprint: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_preprint[r["osf_id"]].append(r)

    total_checked = 0
    total_pass = 0
    total_flagged = 0
    total_errors = 0
    last_request_time = 0.0

    for pid, refs in by_preprint.items():
        flagged_for_preprint: List[Dict[str, Any]] = []
        already_emailed = _preprint_already_emailed(repo, pid)

        for ref in refs:
            # Rate limiting
            elapsed = time.monotonic() - last_request_time
            if elapsed < args.rate_limit:
                time.sleep(args.rate_limit - elapsed)

            doi = ref.get("doi", "")
            ref_id = ref.get("ref_id", "")
            raw = ref.get("raw_citation", "")
            total_checked += 1

            try:
                apa = fetch_apa_citation(doi)
                last_request_time = time.monotonic()
            except Exception as exc:
                print(f"  ERROR fetching APA for {pid}/{ref_id} ({doi}): {exc}", flush=True)
                total_errors += 1
                continue

            if not apa:
                print(f"  SKIP {pid}/{ref_id}: no APA citation for {doi}", flush=True)
                total_errors += 1
                continue

            distance = compute_citation_distance(raw, apa)
            validation_needed = needs_validation(distance, threshold)
            status = "pending_review" if validation_needed else "pass"

            flag = "FLAG" if validation_needed else "pass"
            print(f"  {flag} {pid}/{ref_id}: distance={distance:.1%} doi={doi}", flush=True)

            if not args.dry_run:
                try:
                    repo.update_reference_citation_distance(
                        pid, ref_id,
                        distance=distance,
                        apa_citation=apa,
                        validation_status=status,
                    )
                except Exception as exc:
                    print(f"  ERROR persisting {pid}/{ref_id}: {exc}", flush=True)
                    total_errors += 1
                    continue

            reviewer_configured = bool(RUNTIME_CONFIG.validation.reviewer_email)
            if validation_needed and reviewer_configured:
                ref["citation_distance"] = distance
                ref["citation_apa_resolved"] = apa
                ref["citation_validation_status"] = status
                flagged_for_preprint.append(ref)
                total_flagged += 1
            elif validation_needed:
                print(f"  WARN {pid}/{ref_id}: distance={distance:.1%} but no reviewer configured — stored as pass", flush=True)
                total_pass += 1
            else:
                total_pass += 1

        # Preprint-level: set pending flag and optionally send review email
        if flagged_for_preprint and not already_emailed:
            import uuid
            review_id = f"review-{pid}-{uuid.uuid4().hex[:8]}"
            print(f"  -> {pid}: {len(flagged_for_preprint)} flagged ref(s), review_id={review_id}", flush=True)
            if not args.dry_run:
                try:
                    repo.set_citation_validation_pending(pid, pending=True, review_id=review_id)
                except Exception as exc:
                    print(f"  ERROR setting pending flag for {pid}: {exc}", flush=True)

                if args.send_reviews:
                    try:
                        from osf_sync.email.citation_review import send_citation_review_email
                        msg_id = send_citation_review_email(pid, flagged_for_preprint, review_id)
                        if msg_id:
                            print(f"  -> Review email sent for {pid} (msg_id={msg_id})", flush=True)
                        else:
                            print(f"  -> Review email skipped for {pid} (no reviewer configured?)", flush=True)
                    except Exception as exc:
                        print(f"  ERROR sending review email for {pid}: {exc}", flush=True)
        elif flagged_for_preprint and already_emailed:
            print(f"  -> {pid}: {len(flagged_for_preprint)} flagged ref(s), but email already sent — skipping", flush=True)

    print(
        f"\nDone. checked={total_checked} pass={total_pass} "
        f"flagged={total_flagged} errors={total_errors} "
        f"{'(dry-run)' if args.dry_run else ''}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
