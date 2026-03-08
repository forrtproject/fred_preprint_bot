#!/usr/bin/env python3
"""Remove duplicate trial assignments for preprints already excluded as old versions.

The version exclusion backfill excluded old-version preprints but left their
trial_preprint_assignments rows in place. This script deletes assignment rows
whose preprint_id is in the excluded_preprints table, cleaning up the duplicates.

Must run AFTER backfill_version_exclusions.py.

Usage::

    python scripts/backfill_dedup_assignments.py --dry-run
    python scripts/backfill_dedup_assignments.py --no-dry-run
"""
from __future__ import annotations

import argparse
import logging
import os

import boto3
from botocore.config import Config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def _get_dynamo_resource():
    local_url = os.getenv("DYNAMO_LOCAL_URL")
    region = os.getenv("AWS_REGION", "eu-central-1")
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})
    if local_url:
        return boto3.resource(
            "dynamodb",
            region_name=region,
            endpoint_url=local_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
            config=cfg,
        )
    return boto3.resource("dynamodb", region_name=region, config=cfg)


def _scan_all(table, **kwargs):
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp["Items"])
    return items


_ORPHAN_FIELDS = [
    "trial_cluster_id",
    "trial_assigned_at",
    "trial_assignment_run_id",
    "trial_matched_cluster_ids",
    "trial_assignment_reason",
]


def _clean_orphaned_fields(excluded_table, preprints_table, *, dry_run=True):
    """Remove leftover trial fields from excluded preprints."""
    log.info("Scanning excluded preprints for orphaned trial fields...")
    all_excluded = _scan_all(excluded_table, ProjectionExpression="osf_id")
    excluded_ids = [item["osf_id"] for item in all_excluded]
    log.info("Excluded preprints: %d", len(excluded_ids))

    to_clean = []
    for osf_id in excluded_ids:
        try:
            resp = preprints_table.get_item(
                Key={"osf_id": osf_id},
                ProjectionExpression=", ".join(["osf_id"] + _ORPHAN_FIELDS),
            )
        except Exception:
            continue
        item = resp.get("Item")
        if not item:
            continue
        present = [f for f in _ORPHAN_FIELDS if f in item]
        if present:
            to_clean.append((osf_id, present))

    log.info("Excluded preprints with orphaned trial fields: %d", len(to_clean))
    if not to_clean:
        log.info("Nothing to do.")
        return

    if dry_run:
        for osf_id, fields in to_clean[:15]:
            log.info("  would clean %s: %s", osf_id, ", ".join(fields))
        if len(to_clean) > 15:
            log.info("  ... and %d more", len(to_clean) - 15)
        log.info("Dry run -- no changes made. Use --no-dry-run to apply.")
        return

    cleaned = 0
    for osf_id, fields in to_clean:
        try:
            preprints_table.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression="REMOVE " + ", ".join(fields),
                ConditionExpression="attribute_exists(osf_id)",
            )
            cleaned += 1
        except Exception as e:
            log.warning("Failed to clean %s: %s", osf_id, e)
    log.info("Cleaned orphaned trial fields on %d records", cleaned)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_false", dest="dry_run")
    parser.add_argument(
        "--orphaned-fields-only",
        action="store_true",
        help="Only clean orphaned trial fields on excluded preprints (skip assignment deletion)",
    )
    args = parser.parse_args()

    ddb = _get_dynamo_resource()
    assignments_table = ddb.Table(os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "trial_preprint_assignments"))
    excluded_table = ddb.Table(os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "excluded_preprints"))
    preprints_table = ddb.Table(os.environ.get("DDB_TABLE_PREPRINTS", "preprints"))

    if args.orphaned_fields_only:
        _clean_orphaned_fields(excluded_table, preprints_table, dry_run=args.dry_run)
        return

    log.info("Scanning assignments...")
    all_assignments = _scan_all(
        assignments_table,
        ProjectionExpression="preprint_id, arm, #s",
        ExpressionAttributeNames={"#s": "status"},
    )
    log.info("Total assignments: %d", len(all_assignments))

    log.info("Scanning excluded preprints...")
    all_excluded = _scan_all(excluded_table, ProjectionExpression="osf_id, exclusion_reason")
    excluded_ids = {item["osf_id"]: item.get("exclusion_reason", "") for item in all_excluded}
    log.info("Excluded preprints: %d", len(excluded_ids))

    to_delete = [a for a in all_assignments if a["preprint_id"] in excluded_ids]
    log.info("Assignments to delete (preprint is excluded): %d", len(to_delete))

    if not to_delete:
        log.info("Nothing to do.")
        return

    # Breakdown
    by_reason = {}
    for a in to_delete:
        reason = excluded_ids.get(a["preprint_id"], "unknown")
        by_reason[reason] = by_reason.get(reason, 0) + 1
    for reason, count in sorted(by_reason.items()):
        log.info("  reason=%s: %d", reason, count)

    if args.dry_run:
        for a in to_delete[:15]:
            log.info("  would delete assignment for %s (arm=%s)", a["preprint_id"], a.get("arm"))
        if len(to_delete) > 15:
            log.info("  ... and %d more", len(to_delete) - 15)
        log.info("Dry run -- no changes made. Use --no-dry-run to apply.")
        return

    deleted = 0
    failed = 0
    email_cleared = 0
    for a in to_delete:
        pid = a["preprint_id"]
        try:
            assignments_table.delete_item(Key={"preprint_id": pid})
            deleted += 1
        except Exception as e:
            log.warning("Failed to delete assignment %s: %s", pid, e)
            failed += 1

        # Clear queue_email/trial_assignment_status on the excluded preprint
        # so the email stage doesn't try to send to old versions
        try:
            preprints_table.update_item(
                Key={"osf_id": pid},
                UpdateExpression=(
                    "REMOVE queue_email, trial_assignment_status, trial_arm, "
                    "trial_cluster_id, trial_assigned_at, trial_assignment_run_id, "
                    "trial_matched_cluster_ids, trial_assignment_reason"
                ),
                ConditionExpression="attribute_exists(osf_id)",
            )
            email_cleared += 1
        except Exception:
            pass  # preprint record may not exist or already clean

    log.info("Deleted assignments: %d, Failed: %d", deleted, failed)
    log.info("Cleared email/assignment state on preprint records: %d", email_cleared)


if __name__ == "__main__":
    main()
