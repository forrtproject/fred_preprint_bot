#!/usr/bin/env python3
"""Remediation: clean self-replication FLoRA pairs on specific preprints
and mark them excluded for protocol-deviation accounting.

The standard FLoRA stage skips any preprint with ``email_sent=True``. For
preprints that were emailed before the self-replication fix landed, this
script invokes the same screening logic with that guard bypassed, so the
persisted ``flora_ref_pairs`` and ``flora_eligible`` state is brought in
line with the new rules. It then calls ``mark_preprint_excluded`` so the
preprint is recorded as a protocol deviation (the email was already sent
and cannot be unsent).

It does NOT send emails or unsend anything; it only updates DB state.

Usage:
    python scripts/clean_flora_self_replications.py --ids n9kgz_v2 ydgh9_v2 x394e_v4
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from osf_sync.augmentation.flora_screening import (
    lookup_and_screen_flora,
    _build_self_dois,
)
from osf_sync.augmentation.flora_original_lookup import normalize_doi
from osf_sync.dynamo.preprints_repo import PreprintsRepo

EXCLUSION_REASON = "self_replication_email_deviation"
EXCLUSION_STAGE = "post_email_review"


def _collect_self_pair_details(repo: PreprintsRepo, osf_id: str) -> dict:
    preprint = repo.t_preprints.get_item(Key={"osf_id": osf_id}).get("Item") or {}
    self_dois = _build_self_dois(preprint)
    refs_with_self: list[dict] = []
    last_key = None
    while True:
        kw = {"KeyConditionExpression": "osf_id = :oid",
              "ExpressionAttributeValues": {":oid": osf_id}}
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = repo.t_refs.query(**kw)
        for r in resp.get("Items", []):
            pairs = r.get("flora_ref_pairs") or []
            self_doi_rs = sorted({
                d for p in pairs
                if (d := normalize_doi(p.get("doi_r"))) and d in self_dois
            })
            if self_doi_rs:
                refs_with_self.append({
                    "ref_id": r.get("ref_id"),
                    "ref_doi": r.get("doi"),
                    "self_doi_rs": self_doi_rs,
                })
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return {
        "email_message_id": preprint.get("email_message_id"),
        "email_sent_at": preprint.get("email_sent_at"),
        "email_recipient": preprint.get("email_recipient"),
        "trial_arm": preprint.get("trial_arm"),
        "refs_with_self_doi_r": refs_with_self,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", nargs="+", required=True, help="OSF ids to clean")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run without persisting (smoke test only)")
    ap.add_argument("--skip-mark-excluded", action="store_true",
                    help="Only re-screen; do not mark protocol deviation")
    args = ap.parse_args()

    # Bypass the email_sent guard inside lookup_and_screen_flora so already-
    # emailed preprints are re-screened. The bypass is local to this script.
    PreprintsRepo.filter_osf_ids_without_sent_email = (
        lambda self, osf_ids: {oid for oid in osf_ids if oid}
    )

    repo = PreprintsRepo()

    for osf_id in args.ids:
        print(f"\n=== {osf_id} ===")

        # Snapshot self-pair details BEFORE re-screen overwrites them.
        details = _collect_self_pair_details(repo, osf_id)
        details["remediation_note"] = (
            "Email already sent before self-replication fix landed; "
            "marked as protocol deviation for analysis exclusion."
        )
        print(f"  pre-cleanup self-pair refs: {len(details['refs_with_self_doi_r'])}")

        # Re-screen with the new logic (drops/clears self pairs).
        result = lookup_and_screen_flora(
            osf_id=osf_id,
            persist_flags=not args.dry_run,
            only_unchecked=False,
            debug=True,
        )
        screen = result.get("screen") or []
        if not screen:
            print(f"  no screen result returned (preprint may not exist)")
            continue
        s = screen[0]
        print(f"  post-cleanup eligible={s.get('eligible')} "
              f"eligible_count={s.get('eligible_count')}")

        if args.skip_mark_excluded or args.dry_run:
            continue

        marked = repo.mark_preprint_excluded(
            osf_id=osf_id,
            reason=EXCLUSION_REASON,
            stage=EXCLUSION_STAGE,
            details=details,
        )
        if marked:
            print(f"  marked excluded: reason={EXCLUSION_REASON}")
        else:
            print(f"  already had an excluded_preprints row; preprint flag updated")


if __name__ == "__main__":
    main()
