#!/usr/bin/env python3
"""Backfill exclusions for multi-version preprints.

For each base preprint with multiple non-excluded versions in the DB:
- If any version has trial_assignment_status -> that version wins, exclude others
- If no version is randomized -> keep the highest version number, exclude others

Run backfill_queue_email.py FIRST to restore trial assignment state.

Usage::

    python scripts/backfill_version_exclusions.py --dry-run
    python scripts/backfill_version_exclusions.py --no-dry-run
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import logging
import os
import re
import sys

import boto3
from botocore.config import Config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

_VERSION_RE = re.compile(r"^(.+)_v(\d+)$")


def _parse_version(osf_id: str):
    m = _VERSION_RE.match(osf_id)
    if m:
        return m.group(1), int(m.group(2))
    return osf_id, None


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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_false", dest="dry_run")
    args = parser.parse_args()

    ddb = _get_dynamo_resource()
    preprints_table = ddb.Table(os.environ.get("DDB_TABLE_PREPRINTS", "preprints"))
    excluded_table = ddb.Table(os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "excluded_preprints"))

    log.info("Scanning preprints table...")
    all_preprints = _scan_all(
        preprints_table,
        ProjectionExpression="osf_id, version, is_latest_version, trial_assignment_status, excluded",
    )
    log.info("Total preprints: %d", len(all_preprints))

    # Fetch already-excluded IDs
    all_excluded = _scan_all(excluded_table, ProjectionExpression="osf_id")
    excluded_ids = {item["osf_id"] for item in all_excluded}
    log.info("Already excluded preprints: %d", len(excluded_ids))

    # Group by base_id
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for p in all_preprints:
        osf_id = p["osf_id"]
        base, ver = _parse_version(osf_id)
        if ver is not None:
            groups[base].append(p)

    multi_version_groups = {base: versions for base, versions in groups.items() if len(versions) > 1}
    log.info("Multi-version groups: %d", len(multi_version_groups))

    to_exclude: list[tuple[str, str, dict]] = []  # (osf_id, reason, details)
    kept_randomized = 0
    kept_highest = 0

    for base, versions in sorted(multi_version_groups.items()):
        # Filter to non-excluded versions
        active = [v for v in versions if v["osf_id"] not in excluded_ids and not v.get("excluded")]
        if len(active) <= 1:
            continue

        # Check if any version is randomized
        randomized = [v for v in active if v.get("trial_assignment_status")]
        if randomized:
            # Keep the randomized one (first if multiple — shouldn't happen)
            winner = randomized[0]
            kept_randomized += 1
        else:
            # Keep the highest version number
            active_sorted = sorted(active, key=lambda v: _parse_version(v["osf_id"])[1] or 0, reverse=True)
            winner = active_sorted[0]
            kept_highest += 1

        winner_id = winner["osf_id"]
        for v in active:
            if v["osf_id"] != winner_id:
                to_exclude.append((
                    v["osf_id"],
                    "superseded_by_newer_version",
                    {"superseded_by": winner_id},
                ))

    log.info("=== Summary ===")
    log.info("Groups with multiple active versions: %d", kept_randomized + kept_highest)
    log.info("  Kept randomized version: %d", kept_randomized)
    log.info("  Kept highest version (no randomization): %d", kept_highest)
    log.info("Versions to exclude: %d", len(to_exclude))

    if not to_exclude:
        log.info("Nothing to do.")
        return

    if args.dry_run:
        for osf_id, reason, details in to_exclude[:20]:
            log.info("  would exclude %s (reason=%s, %s)", osf_id, reason, details)
        if len(to_exclude) > 20:
            log.info("  ... and %d more", len(to_exclude) - 20)
        log.info("Dry run -- no changes made. Use --no-dry-run to apply.")
        return

    applied = 0
    skipped = 0
    now_iso = dt.datetime.utcnow().isoformat()
    now_date = dt.date.today().isoformat()

    for osf_id, reason, details in to_exclude:
        try:
            excluded_table.put_item(
                Item={
                    "osf_id": osf_id,
                    "excluded_at": now_iso,
                    "exclusion_date": now_date,
                    "exclusion_reason": reason,
                    "exclusion_stage": "backfill",
                    "exclusion_details": details,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                },
                ConditionExpression="attribute_not_exists(osf_id)",
            )
            # Also set excluded flag on preprints record
            try:
                preprints_table.update_item(
                    Key={"osf_id": osf_id},
                    UpdateExpression="SET excluded = :true, exclusion_reason = :reason, updated_at = :t",
                    ExpressionAttributeValues={":true": True, ":reason": reason, ":t": now_iso},
                )
            except Exception:
                pass
            applied += 1
        except ddb.meta.client.exceptions.ConditionalCheckFailedException:
            skipped += 1
        except Exception as e:
            log.warning("Failed to exclude %s: %s", osf_id, e)
            skipped += 1

    log.info("Applied: %d, Skipped (already excluded): %d", applied, skipped)


if __name__ == "__main__":
    main()
