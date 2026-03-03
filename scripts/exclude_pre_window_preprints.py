#!/usr/bin/env python3
"""Exclude preprints whose creation date falls before the ingest window start.

With anchor_date=2026-03-05 and window_months=6, the window starts on
2025-09-05. Any preprint with date_created earlier than that was ingested
under a wider window and should be removed before the experiment begins.

Usage:
    # Dry run (default) — shows count and sample IDs:
    python scripts/exclude_pre_window_preprints.py

    # Actually exclude:
    python scripts/exclude_pre_window_preprints.py --execute
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import boto3
from boto3.dynamodb.conditions import Attr

from osf_sync.dynamo.preprints_repo import PreprintsRepo

# Derived from anchor_date=2026-03-05, window_months=6
WINDOW_START = "2025-09-05"
REASON = "ingest_date_window"


def _scan_pre_window(table) -> list[str]:
    """Return osf_ids of non-excluded preprints created before WINDOW_START."""
    filter_expr = (
        Attr("date_created").lt(WINDOW_START)
        & (Attr("excluded").not_exists() | Attr("excluded").eq(False))
    )
    osf_ids = []
    kwargs = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": "osf_id, date_created",
    }
    resp = table.scan(**kwargs)
    osf_ids.extend((item["osf_id"], item.get("date_created", "")) for item in resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        resp = table.scan(**kwargs)
        osf_ids.extend((item["osf_id"], item.get("date_created", "")) for item in resp.get("Items", []))
    return osf_ids


def main():
    parser = argparse.ArgumentParser(
        description=f"Exclude preprints created before {WINDOW_START} (anchor 2026-03-05 ± 6 months)"
    )
    parser.add_argument("--execute", action="store_true", help="Actually exclude (default is dry-run)")
    args = parser.parse_args()

    repo = PreprintsRepo()
    items = _scan_pre_window(repo.t_preprints)

    print(f"Found {len(items)} non-excluded preprints with date_created < {WINDOW_START}")
    if not items:
        return

    if not args.execute:
        print(f"Dry run — pass --execute to exclude these {len(items)} preprints")
        for osf_id, created in sorted(items, key=lambda x: x[1])[:20]:
            print(f"  {osf_id}  ({created})")
        if len(items) > 20:
            print(f"  ... and {len(items) - 20} more")
        return

    excluded = 0
    failed = 0
    for osf_id, created in items:
        try:
            repo.mark_preprint_excluded(
                osf_id=osf_id,
                reason=REASON,
                stage="cleanup",
                details={"date_created": created, "window_start": WINDOW_START},
            )
            excluded += 1
        except Exception as exc:
            print(f"  Failed to exclude {osf_id}: {exc}")
            failed += 1
        if (excluded + failed) % 100 == 0:
            print(f"  Progress: {excluded + failed}/{len(items)} (excluded={excluded}, failed={failed})")

    print(f"Done: excluded {excluded}, failed {failed} out of {len(items)} total")


if __name__ == "__main__":
    main()
