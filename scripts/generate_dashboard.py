#!/usr/bin/env python3
"""Generate a Markdown dashboard summarising DynamoDB pipeline stats.

Usage:
    python scripts/generate_dashboard.py [OUTPUT_PATH]

Requires env vars for table names (DDB_TABLE_PREPRINTS, etc.) and AWS
credentials (AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or a
local DynamoDB endpoint (DYNAMO_LOCAL_URL).
"""

import os
import sys
from collections import Counter
from datetime import datetime, timezone
from statistics import median

import boto3
from botocore.config import Config


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

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


def _count_by_gsi(table, index_name, key_name, key_value):
    """Count items on a GSI using Select='COUNT' (no data transferred)."""
    resp = table.query(
        IndexName=index_name,
        KeyConditionExpression=boto3.dynamodb.conditions.Key(key_name).eq(key_value),
        Select="COUNT",
    )
    total = resp["Count"]
    while resp.get("LastEvaluatedKey"):
        resp = table.query(
            IndexName=index_name,
            KeyConditionExpression=boto3.dynamodb.conditions.Key(key_name).eq(key_value),
            Select="COUNT",
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        total += resp["Count"]
    return total


def _query_all_items(table, index_name, key_name, key_value):
    """Return all items from a GSI query (paginated)."""
    items = []
    resp = table.query(
        IndexName=index_name,
        KeyConditionExpression=boto3.dynamodb.conditions.Key(key_name).eq(key_value),
    )
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        resp = table.query(
            IndexName=index_name,
            KeyConditionExpression=boto3.dynamodb.conditions.Key(key_name).eq(key_value),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp["Items"])
    return items


def _scan_all(table, projection_expression):
    """Full paginated scan returning only projected attributes (small tables only)."""
    items = []
    resp = table.scan(ProjectionExpression=projection_expression)
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            ProjectionExpression=projection_expression,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp["Items"])
    return items


def _scan_count_with_filter(table, filter_expression):
    """Count table items matching a filter using paginated scans."""
    total = 0
    resp = table.scan(FilterExpression=filter_expression, Select="COUNT")
    total += resp.get("Count", 0)
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            FilterExpression=filter_expression,
            Select="COUNT",
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        total += resp.get("Count", 0)
    return total


def _scan_with_filter(table, filter_expression, projection_expression):
    """Full paginated scan returning projected attributes for items matching a filter."""
    items = []
    resp = table.scan(
        FilterExpression=filter_expression,
        ProjectionExpression=projection_expression,
    )
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            FilterExpression=filter_expression,
            ProjectionExpression=projection_expression,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp["Items"])
    return items


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def collect_stats():
    ddb = _get_dynamo_resource()

    preprints_name = os.environ.get("DDB_TABLE_PREPRINTS", "preprints")
    excluded_name = os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "excluded_preprints")
    assignments_name = os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "trial_preprint_assignments")
    suppression_name = os.environ.get("DDB_TABLE_EMAIL_SUPPRESSION", "email_suppression")

    preprints = ddb.Table(preprints_name)
    excluded = ddb.Table(excluded_name)
    assignments = ddb.Table(assignments_name)
    suppression = ddb.Table(suppression_name)

    # Pipeline funnel — 8 GSI count queries
    queues = ["queue_pdf", "queue_grobid", "queue_extract", "queue_email"]
    funnel = {}
    for q in queues:
        index = f"by_{q}"
        pending = _count_by_gsi(preprints, index, q, "pending")
        done = _count_by_gsi(preprints, index, q, "done")
        funnel[q] = {"pending": pending, "done": done, "total": pending + done}

    total_preprints = funnel["queue_pdf"]["total"]

    # Exclusions — small table scan
    excl_items = _scan_all(excluded, "exclusion_reason")
    excl_counts = Counter(item.get("exclusion_reason", "unknown") for item in excl_items)
    total_excluded = sum(excl_counts.values())

    # Trial assignments — GSI query for assigned (need arm breakdown)
    assigned_items = _query_all_items(assignments, "by_status", "status", "assigned")
    arm_counts = Counter(item.get("arm", "unknown") for item in assigned_items)
    total_assigned = len(assigned_items)

    # Randomization-excluded count
    randomization_excluded = _count_by_gsi(assignments, "by_status", "status", "excluded")

    # Email suppression and current send-error backlog
    suppression_items = _scan_all(suppression, "reason")
    suppression_counts = Counter(item.get("reason", "unknown") for item in suppression_items)
    total_suppressed = sum(suppression_counts.values())
    email_error_open = _scan_count_with_filter(
        preprints,
        boto3.dynamodb.conditions.Attr("email_error").exists(),
    )

    # FLoRA screening and email extraction coverage
    Attr = boto3.dynamodb.conditions.Attr
    flora_screened = _scan_count_with_filter(
        preprints, Attr("flora_eligible").exists(),
    )
    email_extracted = _scan_count_with_filter(
        preprints, Attr("author_email_candidates").exists(),
    )

    # FLoRA matching — preprints with flora_eligible = True
    flora_eligible_items = _scan_with_filter(
        preprints,
        Attr("flora_eligible").eq(True),
        "osf_id, flora_eligible_count, flora_citation_validation_pending, "
        "author_email_candidates, trial_assignment_status, excluded",
    )
    flora_total = len(flora_eligible_items)

    # Distribution of eligible reference counts
    eligible_counts = [
        int(item.get("flora_eligible_count", 0))
        for item in flora_eligible_items
    ]
    flora_median_refs = median(eligible_counts) if eligible_counts else 0
    flora_max_refs = max(eligible_counts) if eligible_counts else 0
    flora_multi_ref = sum(1 for c in eligible_counts if c > 1)
    flora_multi_ref_pct = (flora_multi_ref / flora_total * 100) if flora_total else 0

    # Preprints with citation validation pending (refs needing confirmation)
    flora_validation_pending = sum(
        1 for item in flora_eligible_items
        if item.get("flora_citation_validation_pending") is True
    )

    # Preprints missing author emails
    flora_missing_email = sum(
        1 for item in flora_eligible_items
        if not item.get("author_email_candidates")
    )

    # Total assignable: flora_eligible, has email, no pending validation (regardless
    # of assignment status — the effective sample size)
    flora_total_assignable = sum(
        1 for item in flora_eligible_items
        if not item.get("excluded")
        and item.get("author_email_candidates")
        and not item.get("flora_citation_validation_pending")
    )

    # Assignment pending: assignable but not yet assigned
    flora_assignment_pending = sum(
        1 for item in flora_eligible_items
        if not item.get("trial_assignment_status")
        and not item.get("excluded")
        and item.get("author_email_candidates")
        and not item.get("flora_citation_validation_pending")
    )

    return {
        "funnel": funnel,
        "total_preprints": total_preprints,
        "excl_counts": excl_counts,
        "total_excluded": total_excluded,
        "arm_counts": arm_counts,
        "total_assigned": total_assigned,
        "randomization_excluded": randomization_excluded,
        "suppression_counts": suppression_counts,
        "total_suppressed": total_suppressed,
        "email_error_open": email_error_open,
        "flora_screened": flora_screened,
        "email_extracted": email_extracted,
        "flora_total": flora_total,
        "flora_median_refs": flora_median_refs,
        "flora_max_refs": flora_max_refs,
        "flora_multi_ref": flora_multi_ref,
        "flora_multi_ref_pct": flora_multi_ref_pct,
        "flora_validation_pending": flora_validation_pending,
        "flora_missing_email": flora_missing_email,
        "flora_total_assignable": flora_total_assignable,
        "flora_assignment_pending": flora_assignment_pending,
    }


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

STAGE_LABELS = {
    "queue_pdf": "PDF Download",
    "queue_grobid": "GROBID Processing",
    "queue_extract": "Reference Extraction",
    "queue_email": "Email",
}


def render_markdown(stats):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_identified = stats["total_preprints"] + stats["total_excluded"]
    lines = [
        "# FLoRA Preprint Notifier — Dashboard",
        f"*Updated: {now} · [Source](https://github.com/forrtproject/flora_preprint_notifier)*",
        "",
        "## Preprint Flow",
        "| | Count |",
        "|---|---:|",
        f"| **Preprints identified (OSF)** | **{total_identified}** |",
        f"| − Excluded | {stats['total_excluded']} |",
        f"| = Active in pipeline | {stats['total_preprints']} |",
    ]

    # Exclusion breakdown sorted by frequency
    lines.extend(["", "### Exclusion Breakdown", "| Reason | Count |", "|--------|------:|"])
    for reason, count in stats["excl_counts"].most_common():
        lines.append(f"| {reason} | {count} |")

    # Pipeline funnel
    lines.extend([
        "",
        "## Pipeline Funnel",
        "| Stage | Pending | Done | Total |",
        "|-------|--------:|-----:|------:|",
    ])

    for q in ["queue_pdf", "queue_grobid", "queue_extract", "queue_email"]:
        s = stats["funnel"][q]
        lines.append(f"| {STAGE_LABELS[q]} | {s['pending']} | {s['done']} | {s['total']} |")

    # FLoRA matching
    lines.extend([
        "",
        "## FLoRA Matching",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Screened against FLoRA | {stats['flora_screened']} / {stats['total_preprints']} |",
        f"| Email extraction completed | {stats['email_extracted']} / {stats['total_preprints']} |",
        f"| Preprints with FLoRA matches | {stats['flora_total']} |",
        f"| Pending citation confirmation | {stats['flora_validation_pending']} |",
        f"| Missing author emails | {stats['flora_missing_email']} |",
        "",
        "**Matched reference distribution:**",
        f" Median: {stats['flora_median_refs']}, "
        f"Max: {stats['flora_max_refs']}, "
        f">1 match: {stats['flora_multi_ref']}/{stats['flora_total']}"
        f" ({stats['flora_multi_ref_pct']:.0f}%)",
        "",
        f"**Total assignable (eligible, has email, no pending validation):** "
        f"{stats['flora_total_assignable']}",
        f"**Assignment pending:** {stats['flora_assignment_pending']}",
    ])

    # Trial assignments
    lines.extend(["", "## Trial Assignment", "| Arm | Count |", "|-----|-------|"])
    for arm in sorted(stats["arm_counts"]):
        lines.append(f"| {arm.replace('_', ' ').title()} | {stats['arm_counts'][arm]} |")
    lines.append(f"| Randomization-excluded | {stats['randomization_excluded']} |")
    lines.append(f"**Total assigned:** {stats['total_assigned']}")

    # Email summary (from funnel)
    email = stats["funnel"]["queue_email"]
    lines.extend([
        "",
        "## Emails",
        "| Status | Count |",
        "|--------|-------|",
        f"| Sent (queue done) | {email['done']} |",
        f"| Pending | {email['pending']} |",
    ])

    lines.extend([
        "",
        "## Email Health",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Open send errors | {stats['email_error_open']} |",
        f"| Total suppressions | {stats['total_suppressed']} |",
    ])
    for reason in sorted(stats["suppression_counts"]):
        lines.append(f"| Suppressed ({reason}) | {stats['suppression_counts'][reason]} |")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else "dashboard.md"
    stats = collect_stats()
    md = render_markdown(stats)

    if output_path == "/dev/stdout":
        sys.stdout.write(md)
    else:
        with open(output_path, "w") as f:
            f.write(md)
        print(f"Dashboard written to {output_path}")


if __name__ == "__main__":
    main()
