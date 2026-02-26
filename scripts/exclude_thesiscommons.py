#!/usr/bin/env python3
"""One-off script: mark ThesisCommons preprints as excluded.

ThesisCommons is a thesis repository, not a preprint server. This script
finds any existing ThesisCommons items in the preprints table that are not
yet excluded and marks them excluded via the standard repo method (which
also removes them from all processing queues).

Usage:
    # Dry run (default):
    python scripts/exclude_thesiscommons.py

    # Actually exclude:
    python scripts/exclude_thesiscommons.py --execute
"""

import argparse
import sys
import os

# Allow running from the repo root without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import boto3
from boto3.dynamodb.conditions import Attr

from osf_sync.dynamo.preprints_repo import PreprintsRepo

PROVIDER = "thesiscommons"
REASON = "ingest_excluded_provider"


def main():
    parser = argparse.ArgumentParser(description="Exclude ThesisCommons preprints from the pipeline")
    parser.add_argument("--execute", action="store_true", help="Actually exclude (default is dry-run)")
    args = parser.parse_args()

    repo = PreprintsRepo()
    table = repo.t_preprints

    # Scan for thesiscommons items that are not yet excluded.
    filter_expr = Attr("provider_id").eq(PROVIDER) & (
        Attr("excluded").not_exists() | Attr("excluded").eq(False)
    )

    osf_ids = []
    scan_kwargs = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": "osf_id",
    }
    resp = table.scan(**scan_kwargs)
    osf_ids.extend(item["osf_id"] for item in resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        resp = table.scan(**scan_kwargs)
        osf_ids.extend(item["osf_id"] for item in resp.get("Items", []))

    print(f"Found {len(osf_ids)} non-excluded ThesisCommons preprints")

    if not osf_ids:
        return

    if not args.execute:
        print("Dry run — pass --execute to mark these preprints excluded")
        for osf_id in osf_ids[:20]:
            print(f"  {osf_id}")
        if len(osf_ids) > 20:
            print(f"  ... and {len(osf_ids) - 20} more")
        return

    excluded = 0
    for osf_id in osf_ids:
        try:
            repo.mark_preprint_excluded(
                osf_id=osf_id,
                reason=REASON,
                stage="cleanup",
                details={"provider_id": PROVIDER},
            )
            excluded += 1
        except Exception as exc:
            print(f"  Failed to exclude {osf_id}: {exc}")

    print(f"Excluded {excluded}/{len(osf_ids)} ThesisCommons preprints")


if __name__ == "__main__":
    main()
