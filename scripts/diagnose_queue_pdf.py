#!/usr/bin/env python3
"""Diagnose and repair preprints stuck with queue_pdf=pending.

Queries the by_queue_pdf GSI for all items with queue_pdf=pending,
classifies them by actual state, and optionally repairs fixable categories.

Usage:
    python scripts/diagnose_queue_pdf.py              # dry-run (report only)
    python scripts/diagnose_queue_pdf.py --commit     # apply repairs
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
from collections import Counter, defaultdict
from pathlib import Path

import boto3
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
# Query
# ---------------------------------------------------------------------------
def query_pending_pdf_items():
    """Query by_queue_pdf GSI for all items with queue_pdf=pending."""
    items = []
    kwargs = {
        "IndexName": "by_queue_pdf",
        "KeyConditionExpression": "queue_pdf = :q",
        "ExpressionAttributeValues": {":q": "pending"},
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------
# Categories:
#   A - pdf_downloaded=True (bool), not excluded  → queue_pdf should be "done"
#   B - excluded=True (bool)                      → queue_pdf should be removed
#   C - type mismatch on pdf_downloaded or excluded (non-bool truthy)
#   D - legitimately pending (pdf_downloaded falsy, not excluded)
#   E - unexpected state

def _is_bool_true(val):
    """Check if value is boolean True (not string 'true' etc.)."""
    return val is True and isinstance(val, bool)


def _is_truthy_non_bool(val):
    """Check if value is truthy but not a proper bool True."""
    if val is None or val is False or val is True:
        return False
    return bool(val)


def classify_item(item):
    osf_id = item.get("osf_id", "?")
    pdf_downloaded = item.get("pdf_downloaded")
    excluded = item.get("excluded")
    tei_generated = item.get("tei_generated")
    queue_grobid = item.get("queue_grobid")

    info = {
        "osf_id": osf_id,
        "pdf_downloaded": pdf_downloaded,
        "pdf_downloaded_type": type(pdf_downloaded).__name__,
        "excluded": excluded,
        "excluded_type": type(excluded).__name__,
        "tei_generated": tei_generated,
        "queue_grobid": queue_grobid,
        "queue_extract": item.get("queue_extract"),
    }

    # Check for type mismatches first
    if _is_truthy_non_bool(pdf_downloaded) or _is_truthy_non_bool(excluded):
        return "C", info

    if _is_bool_true(excluded):
        return "B", info

    if _is_bool_true(pdf_downloaded):
        return "A", info

    # pdf_downloaded is falsy (False, None, or missing) and not excluded
    if not pdf_downloaded and not excluded:
        return "D", info

    return "E", info


# ---------------------------------------------------------------------------
# Repairs
# ---------------------------------------------------------------------------
def repair_a(osf_id, info, dry_run=True):
    """Fix: pdf_downloaded=True but queue_pdf=pending → set queue_pdf=done."""
    tei_generated = info.get("tei_generated")
    queue_grobid = info.get("queue_grobid")

    # Determine if we also need to advance the grobid queue
    need_grobid = not _is_bool_true(tei_generated) and queue_grobid != "pending"

    if dry_run:
        extra = " + SET queue_grobid=pending" if need_grobid else ""
        print(f"  [A] would fix {osf_id}: SET queue_pdf=done{extra}")
        return True

    now = dt.datetime.utcnow().isoformat()
    set_parts = ["queue_pdf = :done", "updated_at = :t"]
    eav = {":done": "done", ":t": now, ":pending_q": "pending", ":true": True}

    if need_grobid:
        set_parts.append("queue_grobid = :grobid_pending")
        eav[":grobid_pending"] = "pending"

    try:
        table.update_item(
            Key={"osf_id": osf_id},
            ConditionExpression="queue_pdf = :pending_q AND pdf_downloaded = :true",
            UpdateExpression="SET " + ", ".join(set_parts),
            ExpressionAttributeValues=eav,
        )
        return True
    except Exception as e:
        print(f"  [A] FAILED {osf_id}: {e}")
        return False


def repair_b(osf_id, info, dry_run=True):
    """Fix: excluded=True but queue_pdf still present → remove queue fields."""
    if dry_run:
        print(f"  [B] would fix {osf_id}: REMOVE queue_pdf (+ queue_grobid, queue_extract if present)")
        return True

    remove_parts = ["queue_pdf"]
    if info.get("queue_grobid"):
        remove_parts.append("queue_grobid")
    if info.get("queue_extract"):
        remove_parts.append("queue_extract")

    now = dt.datetime.utcnow().isoformat()
    try:
        table.update_item(
            Key={"osf_id": osf_id},
            ConditionExpression="queue_pdf = :pending_q AND excluded = :true",
            UpdateExpression="SET updated_at = :t REMOVE " + ", ".join(remove_parts),
            ExpressionAttributeValues={":pending_q": "pending", ":true": True, ":t": now},
        )
        return True
    except Exception as e:
        print(f"  [B] FAILED {osf_id}: {e}")
        return False


def repair_c(osf_id, info, dry_run=True):
    """Fix: type mismatch on pdf_downloaded or excluded."""
    pdf_val = info.get("pdf_downloaded")
    excl_val = info.get("excluded")

    # Determine correct boolean values
    pdf_bool = bool(pdf_val) if pdf_val is not None else False
    excl_bool = bool(excl_val) if excl_val is not None else False

    if dry_run:
        fixes = []
        if _is_truthy_non_bool(pdf_val):
            fixes.append(f"pdf_downloaded: {pdf_val!r} ({type(pdf_val).__name__}) → {pdf_bool}")
        if _is_truthy_non_bool(excl_val):
            fixes.append(f"excluded: {excl_val!r} ({type(excl_val).__name__}) → {excl_bool}")
        print(f"  [C] would fix {osf_id}: {'; '.join(fixes)}")
        return True

    now = dt.datetime.utcnow().isoformat()
    set_parts = ["updated_at = :t"]
    eav = {":t": now, ":pending_q": "pending"}

    if _is_truthy_non_bool(pdf_val):
        set_parts.append("pdf_downloaded = :pdf_bool")
        eav[":pdf_bool"] = pdf_bool
    if _is_truthy_non_bool(excl_val):
        set_parts.append("excluded = :excl_bool")
        eav[":excl_bool"] = excl_bool

    try:
        table.update_item(
            Key={"osf_id": osf_id},
            ConditionExpression="queue_pdf = :pending_q",
            UpdateExpression="SET " + ", ".join(set_parts),
            ExpressionAttributeValues=eav,
        )
        # After fixing types, the item may now fall into category A, B, or D.
        # If it's A or B, re-classify and repair in a second pass.
        if pdf_bool and not excl_bool:
            return repair_a(osf_id, {**info, "pdf_downloaded": pdf_bool, "excluded": excl_bool}, dry_run=False)
        elif excl_bool:
            return repair_b(osf_id, {**info, "pdf_downloaded": pdf_bool, "excluded": excl_bool}, dry_run=False)
        return True
    except Exception as e:
        print(f"  [C] FAILED {osf_id}: {e}")
        return False


REPAIR_FNS = {
    "A": repair_a,
    "B": repair_b,
    "C": repair_c,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Diagnose and repair stuck queue_pdf=pending items")
    parser.add_argument("--commit", action="store_true", help="Apply repairs (default is dry-run)")
    args = parser.parse_args()
    dry_run = not args.commit

    print(f"Table:  {TABLE_NAME}")
    print(f"Region: {REGION}")
    print(f"Mode:   {'DRY RUN' if dry_run else 'COMMIT'}")
    print()

    # 1. Query
    items = query_pending_pdf_items()
    print(f"Found {len(items)} items with queue_pdf=pending\n")

    if not items:
        print("Nothing to do.")
        return

    # 2. Classify
    categories = defaultdict(list)
    for i, item in enumerate(items):
        cat, info = classify_item(item)
        categories[cat].append(info)
        if (i + 1) % 200 == 0:
            print(f"  classified {i + 1}/{len(items)} ...")

    # 3. Report
    print("\n" + "=" * 60)
    print("DIAGNOSTIC BREAKDOWN")
    print("=" * 60)

    labels = {
        "A": "downloaded but queue stuck (pdf_downloaded=True, queue_pdf=pending)",
        "B": "excluded but queue not cleaned (excluded=True, queue_pdf=pending)",
        "C": "type mismatch (non-bool truthy value)",
        "D": "legitimately pending (pdf_downloaded=False, not excluded)",
        "E": "other / unexpected state",
    }

    for cat in sorted(categories):
        items_in_cat = categories[cat]
        print(f"\n  [{cat}] {labels.get(cat, 'unknown')}: {len(items_in_cat)}")
        for info in items_in_cat[:5]:
            print(f"      {info['osf_id']}  pdf_downloaded={info['pdf_downloaded']!r} ({info['pdf_downloaded_type']})  "
                  f"excluded={info['excluded']!r} ({info['excluded_type']})  "
                  f"tei_generated={info['tei_generated']!r}  queue_grobid={info['queue_grobid']!r}")
        if len(items_in_cat) > 5:
            print(f"      ... and {len(items_in_cat) - 5} more")

    fixable = sum(len(v) for k, v in categories.items() if k in REPAIR_FNS)
    not_fixable = sum(len(v) for k, v in categories.items() if k not in REPAIR_FNS)
    print(f"\n  Total: {sum(len(v) for v in categories.values())}")
    print(f"  Fixable (A+B+C): {fixable}")
    print(f"  Report-only (D+E): {not_fixable}")

    # 4. Repair (if not dry-run) or report what would be done
    if fixable == 0:
        print("\nNo fixable items found.")
        return

    print(f"\n{'=' * 60}")
    print("REPAIRS" if not dry_run else "WOULD REPAIR (dry run)")
    print("=" * 60)

    success = 0
    failed = 0
    for cat in sorted(categories):
        if cat not in REPAIR_FNS:
            continue
        fn = REPAIR_FNS[cat]
        for info in categories[cat]:
            ok = fn(info["osf_id"], info, dry_run=dry_run)
            if ok:
                success += 1
            else:
                failed += 1
            if not dry_run and (success + failed) % 50 == 0:
                print(f"  progress: {success + failed}/{fixable} (success={success}, failed={failed})")

    print(f"\n  {'Would repair' if dry_run else 'Repaired'}: {success}")
    if failed:
        print(f"  Failed: {failed}")
    if dry_run:
        print(f"\n  Use --commit to apply these repairs.")


if __name__ == "__main__":
    main()
