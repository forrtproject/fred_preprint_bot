#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from typing import Any, Dict, Optional

import boto3
from boto3.dynamodb.conditions import Attr


def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts:
        return None
    raw = str(ts).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def _compute_recheck_due(now: dt.datetime, checked_at: Optional[str], recheck_days: int) -> dt.datetime:
    base = _parse_iso(checked_at) or now
    return base + dt.timedelta(days=max(1, recheck_days))


def _queue_values(
    *,
    now: dt.datetime,
    osf_id: str,
    ref_id: str,
    flora_lookup_status: Any,
    flora_checked_at: Optional[str],
    pending_value: str,
    recheck_value: str,
    recheck_days: int,
) -> Optional[Dict[str, str]]:
    status = flora_lookup_status
    if status is True:
        # Matched rows should not be in queue.
        return None
    if status is False:
        due = _compute_recheck_due(now, flora_checked_at, recheck_days).isoformat()
        return {
            "pk": recheck_value,
            "sk": f"{due}#{osf_id}#{ref_id}",
        }
    return {
        "pk": pending_value,
        "sk": f"{now.isoformat()}#{osf_id}#{ref_id}",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill flora_queue_pk/sk for existing DOI references.")
    ap.add_argument("--table", required=True, help="DynamoDB references table name")
    ap.add_argument("--region", required=True, help="AWS region")
    ap.add_argument("--pending-value", default="PENDING")
    ap.add_argument("--recheck-value", default="RECHECK")
    ap.add_argument("--recheck-days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=0, help="Max rows to update (0=unlimited)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ddb = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    projection = (
        "osf_id, ref_id, doi, flora_lookup_status, flora_checked_at, "
        "flora_queue_pk, flora_queue_sk"
    )
    filter_expr = Attr("doi").exists() & Attr("doi").ne("")
    last_key = None
    scanned = 0
    updated = 0
    skipped_has_queue = 0
    skipped_status_true = 0

    while True:
        scan_kwargs: Dict[str, Any] = {
            "ProjectionExpression": projection,
            "FilterExpression": filter_expr,
        }
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])
        for item in items:
            scanned += 1
            osf_id = item.get("osf_id")
            ref_id = item.get("ref_id")
            if not osf_id or not ref_id:
                continue

            if item.get("flora_queue_pk") and item.get("flora_queue_sk"):
                skipped_has_queue += 1
                continue

            values = _queue_values(
                now=dt.datetime.utcnow(),
                osf_id=str(osf_id),
                ref_id=str(ref_id),
                flora_lookup_status=item.get("flora_lookup_status"),
                flora_checked_at=item.get("flora_checked_at"),
                pending_value=args.pending_value,
                recheck_value=args.recheck_value,
                recheck_days=args.recheck_days,
            )
            if values is None:
                skipped_status_true += 1
                continue

            if not args.dry_run:
                table.update_item(
                    Key={"osf_id": osf_id, "ref_id": ref_id},
                    UpdateExpression="SET flora_queue_pk=:pk, flora_queue_sk=:sk",
                    ExpressionAttributeValues={
                        ":pk": values["pk"],
                        ":sk": values["sk"],
                    },
                )
            updated += 1

            if args.limit and updated >= args.limit:
                print(
                    {
                        "table": args.table,
                        "region": args.region,
                        "scanned": scanned,
                        "updated": updated,
                        "skipped_has_queue": skipped_has_queue,
                        "skipped_status_true": skipped_status_true,
                        "limit_reached": True,
                        "dry_run": args.dry_run,
                    }
                )
                return 0

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    print(
        {
            "table": args.table,
            "region": args.region,
            "scanned": scanned,
            "updated": updated,
            "skipped_has_queue": skipped_has_queue,
            "skipped_status_true": skipped_status_true,
            "limit_reached": False,
            "dry_run": args.dry_run,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
