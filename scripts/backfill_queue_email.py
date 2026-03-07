#!/usr/bin/env python3
"""Backfill trial assignment state and queue_email for preprints wiped by upsert_preprints.

upsert_preprints used put_item (full overwrite) instead of update_item, so
pipeline-computed fields (trial_assignment_status, trial_arm, queue_email, etc.)
were wiped whenever OSF re-ingested a preprint. This script reads the
trial_assignments table and restores the missing fields on preprints.

Usage::

    python scripts/backfill_queue_email.py --dry-run
    python scripts/backfill_queue_email.py --no-dry-run
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_false", dest="dry_run")
    args = parser.parse_args()

    ddb = _get_dynamo_resource()
    preprints_table = ddb.Table(os.environ.get("DDB_TABLE_PREPRINTS", "preprints"))
    assignments_table = ddb.Table(os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "trial_preprint_assignments"))

    # Get all assignments
    assignment_rows = _scan_all(
        assignments_table,
        ProjectionExpression="preprint_id, #s, arm, cluster_id, assigned_at, run_id, reason, matched_cluster_ids",
        ExpressionAttributeNames={"#s": "status"},
    )
    log.info("Found %d assignment rows", len(assignment_rows))

    # Get preprints that already have trial_assignment_status
    Attr = boto3.dynamodb.conditions.Attr
    already_assigned = _scan_all(
        preprints_table,
        FilterExpression=Attr("trial_assignment_status").exists(),
        ProjectionExpression="osf_id",
    )
    already_set = {i["osf_id"] for i in already_assigned}
    log.info("Preprints already having trial_assignment_status: %d", len(already_set))

    # Find assignments missing from preprints table
    missing = [r for r in assignment_rows if r["preprint_id"] not in already_set]
    log.info("Assignments missing trial_assignment_status on preprint: %d", len(missing))

    if not missing:
        log.info("Nothing to backfill.")
        return

    treatment_count = sum(1 for r in missing if r.get("arm") == "treatment")
    control_count = sum(1 for r in missing if r.get("arm") == "control")
    log.info("  treatment: %d, control: %d, other: %d",
             treatment_count, control_count, len(missing) - treatment_count - control_count)

    if args.dry_run:
        for r in missing[:10]:
            log.info("  would restore %s (arm=%s, status=%s)",
                     r["preprint_id"], r.get("arm"), r.get("status"))
        if len(missing) > 10:
            log.info("  ... and %d more", len(missing) - 10)
        log.info("Dry run — no changes made. Use --no-dry-run to apply.")
        return

    restored = 0
    email_queued = 0
    for r in missing:
        osf_id = r["preprint_id"]
        now = dt.datetime.utcnow().isoformat()

        set_exprs = [
            "trial_assignment_status=:status",
            "updated_at=:t",
        ]
        eav = {
            ":status": r.get("status", "assigned"),
            ":t": now,
        }

        if r.get("arm"):
            set_exprs.append("trial_arm=:arm")
            eav[":arm"] = r["arm"]
        if r.get("cluster_id"):
            set_exprs.append("trial_cluster_id=:cluster")
            eav[":cluster"] = r["cluster_id"]
        if r.get("assigned_at"):
            set_exprs.append("trial_assigned_at=:assigned_at")
            eav[":assigned_at"] = r["assigned_at"]
        if r.get("run_id"):
            set_exprs.append("trial_assignment_run_id=:run_id")
            eav[":run_id"] = r["run_id"]
        if r.get("reason"):
            set_exprs.append("trial_assignment_reason=:reason")
            eav[":reason"] = r["reason"]
        if r.get("matched_cluster_ids"):
            set_exprs.append("trial_matched_cluster_ids=:matched")
            eav[":matched"] = r["matched_cluster_ids"]

        try:
            preprints_table.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression="SET " + ", ".join(set_exprs),
                ConditionExpression="attribute_exists(osf_id)",
                ExpressionAttributeValues=eav,
            )
            restored += 1

            # Set queue_email for treatment preprints
            if r.get("status") == "assigned" and r.get("arm") == "treatment":
                try:
                    preprints_table.update_item(
                        Key={"osf_id": osf_id},
                        UpdateExpression="SET queue_email=:pending, updated_at=:t",
                        ConditionExpression=(
                            "attribute_not_exists(queue_email) OR queue_email <> :done"
                        ),
                        ExpressionAttributeValues={
                            ":pending": "pending",
                            ":t": now,
                            ":done": "done",
                        },
                    )
                    email_queued += 1
                except Exception:
                    pass  # already done or other issue

        except Exception as e:
            log.warning("Failed to update %s: %s", osf_id, e)

    log.info("Restored trial assignment state for %d / %d preprints", restored, len(missing))
    log.info("Set queue_email=pending for %d treatment preprints", email_queued)


if __name__ == "__main__":
    main()
