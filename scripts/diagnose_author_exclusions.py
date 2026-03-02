#!/usr/bin/env python3
"""Diagnose why preprints were excluded with reason 'no_author_contacts_extracted'.

Samples excluded preprints from different dates, cross-references with the
preprints table, and reports on patterns.

Usage:
    python scripts/diagnose_author_exclusions.py [--limit N]
"""

import json
import os
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config


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


def sample_excluded_across_dates(ddb, target=50):
    """Get excluded preprints spread across different dates by scanning with
    ScanIndexForward=False (newest first) and True (oldest first), deduplicating."""
    table = ddb.Table(os.getenv("DDB_TABLE_EXCLUDED_PREPRINTS", "prod_excluded_preprints"))
    items_by_id = {}

    # Get newest first
    resp = table.query(
        IndexName="by_reason",
        KeyConditionExpression=Key("exclusion_reason").eq("no_author_contacts_extracted"),
        ScanIndexForward=False,
        Limit=target,
    )
    for item in resp.get("Items", []):
        items_by_id[item["osf_id"]] = item

    # Get oldest first
    resp = table.query(
        IndexName="by_reason",
        KeyConditionExpression=Key("exclusion_reason").eq("no_author_contacts_extracted"),
        ScanIndexForward=True,
        Limit=target,
    )
    for item in resp.get("Items", []):
        items_by_id[item["osf_id"]] = item

    # Take a spread: sort by date, pick evenly
    all_items = sorted(items_by_id.values(), key=lambda x: x.get("excluded_at", ""))
    if len(all_items) <= target:
        return all_items
    step = max(1, len(all_items) // target)
    return [all_items[i] for i in range(0, len(all_items), step)][:target]


def get_preprint(ddb, osf_id):
    """Get the full preprint record."""
    table = ddb.Table(os.getenv("DDB_TABLE_PREPRINTS", "prod_preprints"))
    resp = table.get_item(Key={"osf_id": osf_id})
    return resp.get("Item")


def _json_default(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main():
    target = 50
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        target = int(sys.argv[idx + 1])

    ddb = get_ddb()
    excluded_items = sample_excluded_across_dates(ddb, target)
    print(f"Sampled {len(excluded_items)} excluded preprints (reason=no_author_contacts_extracted)\n")

    # Counters for patterns
    patterns = Counter()
    detail_rows = Counter()
    providers = Counter()
    candidates_status = Counter()
    candidate_email_domains = Counter()
    interesting_cases = []
    excluded_dates = Counter()
    rows_zero_count = 0
    rows_nonzero_count = 0

    for excl in excluded_items:
        osf_id = excl["osf_id"]
        details = excl.get("exclusion_details") or {}
        rows_count = int(details.get("rows", -1))
        provider = details.get("provider_id", "unknown")
        excl_date = excl.get("exclusion_date", "unknown")

        providers[provider] += 1
        excluded_dates[excl_date] += 1

        if rows_count == 0:
            rows_zero_count += 1
            detail_rows["0 (no rows at all)"] += 1
        elif rows_count > 0:
            rows_nonzero_count += 1
            detail_rows[f"rows>0 (had {rows_count} rows)"] += 1
        else:
            detail_rows["unknown"] += 1

        # Look up the preprint record
        preprint = get_preprint(ddb, osf_id)
        if not preprint:
            patterns["preprint_record_missing"] += 1
            continue

        # Check for author_email_candidates — distinguish missing vs empty
        has_attr = "author_email_candidates" in preprint
        candidates = preprint.get("author_email_candidates")
        is_excluded = preprint.get("excluded", False)
        has_queues = bool(preprint.get("queue_pdf"))

        if not has_attr:
            candidates_status["attribute_MISSING"] += 1
        elif candidates is None:
            candidates_status["attribute_NULL"] += 1
        elif isinstance(candidates, list) and len(candidates) == 0:
            candidates_status["empty_list_[]"] += 1
        elif isinstance(candidates, list):
            candidates_status[f"has_{len(candidates)}_candidates"] += 1
            # Check if any have emails
            with_emails = [c for c in candidates if c.get("email")]
            without_emails = [c for c in candidates if not c.get("email")]
            if with_emails:
                patterns["has_stored_candidates_WITH_emails"] += 1
                for c in with_emails:
                    domain = c["email"].split("@")[-1].lower() if "@" in c["email"] else "unknown"
                    candidate_email_domains[domain] += 1
            else:
                patterns["has_stored_candidates_NO_emails"] += 1

            interesting_cases.append({
                "osf_id": osf_id,
                "provider": provider,
                "extraction_rows": rows_count,
                "candidates": candidates,
                "excluded": is_excluded,
                "has_queues": has_queues,
                "title": (preprint.get("title") or "")[:100],
                "excl_date": excl_date,
            })
        else:
            candidates_status[f"unexpected_type_{type(candidates).__name__}"] += 1

        if is_excluded and not has_queues:
            patterns["state:excluded_no_queues"] += 1
        elif is_excluded and has_queues:
            patterns["state:excluded_BUT_has_queues"] += 1
        elif not is_excluded:
            patterns["state:NOT_marked_excluded"] += 1

    # Report
    print("=" * 70)
    print("EXCLUSION DATE DISTRIBUTION")
    print("=" * 70)
    for date, count in sorted(excluded_dates.items()):
        print(f"  {date}: {count}")

    print(f"\n{'=' * 70}")
    print("EXTRACTION ROWS (from exclusion_details)")
    print("=" * 70)
    print(f"  rows=0 (no author rows extracted at all): {rows_zero_count}")
    print(f"  rows>0 (had rows but no contactable email): {rows_nonzero_count}")

    print(f"\n{'=' * 70}")
    print("AUTHOR_EMAIL_CANDIDATES ATTRIBUTE STATUS IN PREPRINTS TABLE")
    print("=" * 70)
    for status, count in candidates_status.most_common():
        print(f"  {status}: {count}")

    print(f"\n{'=' * 70}")
    print("PREPRINT STATE")
    print("=" * 70)
    for pattern, count in sorted(patterns.items()):
        print(f"  {pattern}: {count}")

    print(f"\n{'=' * 70}")
    print("PROVIDERS")
    print("=" * 70)
    for prov, count in providers.most_common():
        print(f"  {prov}: {count}")

    if candidate_email_domains:
        print(f"\n{'=' * 70}")
        print("EMAIL DOMAINS IN STORED CANDIDATES (of excluded preprints)")
        print("=" * 70)
        for domain, count in candidate_email_domains.most_common(20):
            print(f"  {domain}: {count}")

    if interesting_cases:
        print(f"\n{'=' * 70}")
        print(f"EXCLUDED PREPRINTS WITH STORED CANDIDATES ({len(interesting_cases)})")
        print("=" * 70)
        for p in interesting_cases[:25]:
            print(f"\n  osf_id: {p['osf_id']}  [{p['excl_date']}]")
            print(f"  title: {p['title']}")
            print(f"  provider: {p['provider']}, extraction_rows: {p['extraction_rows']}")
            print(f"  excluded={p['excluded']}, has_queues={p['has_queues']}")
            print(f"  candidates ({len(p['candidates'])}):")
            for c in p["candidates"]:
                email = c.get("email", "")
                name = c.get("name", "N/A")
                pos = c.get("position", "?")
                source = c.get("source", "?")
                print(f"    - pos={pos} {name} <{email}> source={source}")
    else:
        print(f"\n(No excluded preprints with stored candidates found in sample)")

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print("=" * 70)
    total = len(excluded_items)
    print(f"  Total sampled: {total}")
    print(f"  Had extraction rows (emails should have been findable): {rows_nonzero_count} ({100*rows_nonzero_count/total:.0f}%)")
    print(f"  Had zero rows (PDF/OSF extraction failed): {rows_zero_count} ({100*rows_zero_count/total:.0f}%)")
    stored = sum(1 for s, c in candidates_status.items() if s.startswith("has_"))
    print(f"  Have stored candidates in preprints table: {stored}")
    print(f"  Candidates attribute missing entirely: {candidates_status.get('attribute_MISSING', 0)}")
    print(f"  Candidates stored as empty list: {candidates_status.get('empty_list_[]', 0)}")


if __name__ == "__main__":
    main()
