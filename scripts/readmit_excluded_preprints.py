#!/usr/bin/env python3
"""Readmit preprints that were excluded with reason 'no_author_contacts_extracted'.

Reverses the exclusion by:
  1. Setting excluded=False on the preprints table
  2. Restoring queue_pdf/queue_grobid/queue_extract to 'done'
  3. Clearing author_email_candidates so the author stage re-processes them
  4. Removing the exclusion record from the excluded_preprints table

Usage:
    # Dry run (default) — shows what would be readmitted
    python scripts/readmit_excluded_preprints.py

    # Actually readmit
    python scripts/readmit_excluded_preprints.py --commit

    # Readmit specific IDs only
    python scripts/readmit_excluded_preprints.py --commit --ids abc123 def456

    # Limit to N preprints
    python scripts/readmit_excluded_preprints.py --commit --limit 100
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError


def get_ddb():
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


def get_excluded_ids(ddb, limit=None):
    """Get all preprints excluded with reason 'no_author_contacts_extracted'."""
    table = ddb.Table(os.getenv("DDB_TABLE_EXCLUDED_PREPRINTS", "prod_excluded_preprints"))
    ids = []
    last_key = None
    while True:
        kwargs = {
            "IndexName": "by_reason",
            "KeyConditionExpression": Key("exclusion_reason").eq("no_author_contacts_extracted"),
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        if limit:
            kwargs["Limit"] = limit - len(ids)
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            ids.append(item["osf_id"])
            if limit and len(ids) >= limit:
                return ids
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return ids


def readmit_preprint(ddb, osf_id, dry_run=True):
    """Readmit a single preprint."""
    t_preprints = ddb.Table(os.getenv("DDB_TABLE_PREPRINTS", "prod_preprints"))
    t_excluded = ddb.Table(os.getenv("DDB_TABLE_EXCLUDED_PREPRINTS", "prod_excluded_preprints"))

    if dry_run:
        return True

    # 1. Update the preprint: clear exclusion, restore queues, clear candidates
    try:
        t_preprints.update_item(
            Key={"osf_id": osf_id},
            ConditionExpression="attribute_exists(osf_id)",
            UpdateExpression=(
                "SET excluded = :false, "
                "    queue_pdf = :done, "
                "    queue_grobid = :done, "
                "    queue_extract = :done "
                "REMOVE excluded_reason, excluded_at, excluded_date, excluded_stage, "
                "       author_email_candidates"
            ),
            ExpressionAttributeValues={
                ":false": False,
                ":done": "done",
            },
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            print(f"  [skip] {osf_id}: preprint record not found")
            return False
        raise

    # 2. Remove the exclusion record
    try:
        t_excluded.delete_item(Key={"osf_id": osf_id})
    except ClientError:
        # Non-fatal: the preprint is already readmitted
        print(f"  [warn] {osf_id}: could not delete exclusion record")

    return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Readmit excluded preprints")
    parser.add_argument("--commit", action="store_true", help="Actually perform readmission (default: dry run)")
    parser.add_argument("--ids", nargs="+", help="Specific OSF IDs to readmit")
    parser.add_argument("--limit", type=int, default=None, help="Max preprints to readmit")
    args = parser.parse_args()

    dry_run = not args.commit
    ddb = get_ddb()

    if args.ids:
        ids = args.ids
    else:
        ids = get_excluded_ids(ddb, limit=args.limit)

    print(f"{'[DRY RUN] ' if dry_run else ''}Found {len(ids)} preprints to readmit")

    success = 0
    failed = 0
    for i, osf_id in enumerate(ids):
        ok = readmit_preprint(ddb, osf_id, dry_run=dry_run)
        if ok:
            success += 1
        else:
            failed += 1
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{len(ids)} (success={success}, failed={failed})")

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Done: {success} readmitted, {failed} failed out of {len(ids)} total")
    if dry_run:
        print("\nRun with --commit to actually readmit these preprints.")


if __name__ == "__main__":
    main()
