#!/usr/bin/env python3
"""Diagnose and repair preprints missing the flora_eligible attribute.

Items with queue_extract=done but no flora_eligible attribute appear as
"FLoRA Screening pending" in the dashboard. If they have a stale
flora_last_checked (from before flora_eligible was introduced),
select_preprints_for_flora_check skips them — they're permanently stuck.

Fix: clear flora_last_checked so the pipeline re-screens them on its
next run, setting both flora_eligible and flora_last_checked.

Also diagnoses missing author_email_candidates (author extraction pending).

Usage:
    python scripts/backfill_flora_eligible.py              # dry-run
    python scripts/backfill_flora_eligible.py --commit     # apply
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
from collections import Counter
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from dotenv import dotenv_values

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
env = dotenv_values(ENV_FILE)

REGION = env.get("AWS_REGION", "eu-central-1")
TABLE_NAME = "prod_preprints"

session = boto3.Session(
    aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
    region_name=REGION,
)
ddb = session.resource("dynamodb")
table = ddb.Table(TABLE_NAME)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def scan_extract_done_no_flora():
    """Find items with queue_extract=done, no flora_eligible, and not excluded."""
    items = []
    filter_expr = (
        Attr("queue_extract").eq("done")
        & Attr("flora_eligible").not_exists()
        & (Attr("excluded").not_exists() | Attr("excluded").eq(False))
    )
    kwargs = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": "osf_id, flora_last_checked, flora_screened_at, email_sent, queue_extract",
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def scan_extract_done_no_author():
    """Find items with queue_extract=done, no author_email_candidates, and not excluded."""
    items = []
    filter_expr = (
        Attr("queue_extract").eq("done")
        & Attr("author_email_candidates").not_exists()
        & (Attr("excluded").not_exists() | Attr("excluded").eq(False))
    )
    kwargs = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": "osf_id, flora_eligible, email_sent, queue_extract",
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# Repair
# ---------------------------------------------------------------------------
def clear_flora_last_checked(osf_id, dry_run=True):
    """Remove flora_last_checked so select_preprints_for_flora_check re-selects the item."""
    if dry_run:
        return True

    now = dt.datetime.utcnow().isoformat()
    try:
        table.update_item(
            Key={"osf_id": osf_id},
            ConditionExpression="attribute_exists(osf_id) AND attribute_exists(flora_last_checked)",
            UpdateExpression="SET updated_at = :t REMOVE flora_last_checked",
            ExpressionAttributeValues={":t": now},
        )
        return True
    except Exception as e:
        print(f"    FAILED {osf_id}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Diagnose and repair missing flora_eligible / author_email_candidates")
    parser.add_argument("--commit", action="store_true", help="Apply repairs (default is dry-run)")
    args = parser.parse_args()
    dry_run = not args.commit

    print(f"Table:  {TABLE_NAME}")
    print(f"Region: {REGION}")
    print(f"Mode:   {'DRY RUN' if dry_run else 'COMMIT'}")
    print()

    # -----------------------------------------------------------------------
    # Part 1: FLoRA screening
    # -----------------------------------------------------------------------
    print("=" * 60)
    print("FLoRA SCREENING: items with queue_extract=done but no flora_eligible")
    print("=" * 60)

    flora_items = scan_extract_done_no_flora()
    print(f"\nFound {len(flora_items)} items\n")

    has_last_checked = []
    has_screened_at_only = []
    no_flora_attrs = []
    has_email_sent = []

    for item in flora_items:
        osf_id = item["osf_id"]
        if item.get("email_sent") is True:
            has_email_sent.append(item)
        elif item.get("flora_last_checked"):
            has_last_checked.append(item)
        elif item.get("flora_screened_at"):
            has_screened_at_only.append(item)
        else:
            no_flora_attrs.append(item)

    print(f"  Has flora_last_checked (stuck — needs clearing): {len(has_last_checked)}")
    for it in has_last_checked[:5]:
        print(f"    {it['osf_id']}  flora_last_checked={it.get('flora_last_checked')}")
    if len(has_last_checked) > 5:
        print(f"    ... and {len(has_last_checked) - 5} more")

    print(f"\n  Has flora_screened_at only (old format — needs clearing): {len(has_screened_at_only)}")
    for it in has_screened_at_only[:5]:
        print(f"    {it['osf_id']}  flora_screened_at={it.get('flora_screened_at')}")
    if len(has_screened_at_only) > 5:
        print(f"    ... and {len(has_screened_at_only) - 5} more")

    print(f"\n  No flora attrs (should be picked up by pipeline): {len(no_flora_attrs)}")
    for it in no_flora_attrs[:5]:
        print(f"    {it['osf_id']}")
    if len(no_flora_attrs) > 5:
        print(f"    ... and {len(no_flora_attrs) - 5} more")

    print(f"\n  Has email_sent=True (skipped by design): {len(has_email_sent)}")
    for it in has_email_sent[:5]:
        print(f"    {it['osf_id']}")

    # Repair: clear flora_last_checked for stuck items
    fixable_flora = has_last_checked
    if fixable_flora:
        print(f"\n{'WOULD CLEAR' if dry_run else 'CLEARING'} flora_last_checked on {len(fixable_flora)} items:")
        success = failed = 0
        for item in fixable_flora:
            ok = clear_flora_last_checked(item["osf_id"], dry_run=dry_run)
            if ok:
                success += 1
            else:
                failed += 1
        print(f"  {'Would clear' if dry_run else 'Cleared'}: {success}, Failed: {failed}")

    # -----------------------------------------------------------------------
    # Part 2: Author extraction
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("AUTHOR EXTRACTION: items with queue_extract=done but no author_email_candidates")
    print("=" * 60)

    author_items = scan_extract_done_no_author()
    print(f"\nFound {len(author_items)} items\n")

    author_has_flora = []
    author_no_flora = []
    author_email_sent = []

    for item in author_items:
        if item.get("email_sent") is True:
            author_email_sent.append(item)
        elif item.get("flora_eligible") is not None:
            author_has_flora.append(item)
        else:
            author_no_flora.append(item)

    print(f"  Has flora_eligible (was screened, author extraction pending): {len(author_has_flora)}")
    for it in author_has_flora[:5]:
        print(f"    {it['osf_id']}  flora_eligible={it.get('flora_eligible')}")

    print(f"\n  No flora_eligible (needs flora screening first): {len(author_no_flora)}")
    for it in author_no_flora[:5]:
        print(f"    {it['osf_id']}")

    print(f"\n  Has email_sent=True: {len(author_email_sent)}")

    # Note: author extraction repair is handled by fixing flora first,
    # then letting the pipeline run flora → author in sequence.
    # Items that have flora_eligible but no author_email_candidates
    # should be picked up by the author stage automatically.

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"  FLoRA pending: {len(flora_items)}")
    print(f"    Fixable (has stale flora_last_checked): {len(fixable_flora)}")
    print(f"    Already selectable by pipeline (no flora attrs): {len(no_flora_attrs)}")
    print(f"    Skipped by design (email_sent): {len(has_email_sent)}")
    print(f"  Author pending: {len(author_items)}")
    print(f"    Needs flora screening first: {len(author_no_flora)}")
    print(f"    Author extraction pending (has flora): {len(author_has_flora)}")

    if dry_run and fixable_flora:
        print(f"\n  Use --commit to clear flora_last_checked on {len(fixable_flora)} stuck items.")
        print(f"  Then trigger a pipeline run to re-screen them.")


if __name__ == "__main__":
    main()
