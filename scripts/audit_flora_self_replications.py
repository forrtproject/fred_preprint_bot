#!/usr/bin/env python3
"""Audit references whose FLoRA-matched replication DOI is the preprint itself.

Scans the references table for items with ``flora_ref_pairs`` set and reports
how many preprints have at least one self-replication pair — i.e. a FLoRA
pair whose ``doi_r`` matches one of the preprint's own DOIs (any OSF version
or the linked version-of-record).

Outputs three populations:
- ``emailed``: preprint already received the FLoRA-Notify email
- ``eligible_pending``: flora_eligible=True but not yet emailed
- ``other``: matched ref exists but preprint not eligible / other state

Usage:
    python scripts/audit_flora_self_replications.py [--out path.json]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config

from osf_sync.augmentation.flora_original_lookup import normalize_doi
from osf_sync.augmentation.flora_screening import _build_self_dois


def get_ddb():
    region = os.getenv("AWS_REGION", "eu-north-1")
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})
    return boto3.resource("dynamodb", region_name=region, config=cfg)


def scan_refs_with_flora_pairs(ddb) -> List[Dict[str, Any]]:
    """Scan the references table for items that carry flora_ref_pairs."""
    table = ddb.Table(os.getenv("DDB_TABLE_REFERENCES", "prod_preprint_references"))
    items: List[Dict[str, Any]] = []
    last_key = None
    page = 0
    while True:
        kwargs: Dict[str, Any] = {
            "FilterExpression": Attr("flora_ref_pairs").exists(),
            "ProjectionExpression": "osf_id, ref_id, doi, flora_ref_pairs, flora_replication_cited",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        page += 1
        if page % 5 == 0:
            print(f"  scanned {len(items)} matched refs so far...", file=sys.stderr)
        if not last_key:
            break
    return items


def fetch_preprints(ddb, osf_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
    table_name = os.getenv("DDB_TABLE_PREPRINTS", "prod_preprints")
    out: Dict[str, Dict[str, Any]] = {}
    if not osf_ids:
        return out
    ids = list(osf_ids)
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        keys = [{"osf_id": x} for x in chunk]
        resp = ddb.batch_get_item(
            RequestItems={
                table_name: {
                    "Keys": keys,
                    "ProjectionExpression": (
                        "osf_id, doi, links, flora_eligible, email_sent, "
                        "email_sent_at, email_recipient, email_message_id, "
                        "trial_assignment_status, trial_arm"
                    ),
                }
            }
        )
        for it in resp.get("Responses", {}).get(table_name, []):
            out[it["osf_id"]] = it
        unprocessed = resp.get("UnprocessedKeys") or {}
        while unprocessed:
            resp = ddb.batch_get_item(RequestItems=unprocessed)
            for it in resp.get("Responses", {}).get(table_name, []):
                out[it["osf_id"]] = it
            unprocessed = resp.get("UnprocessedKeys") or {}
    return out


def classify(preprint: Dict[str, Any]) -> str:
    if preprint.get("email_sent") is True:
        return "emailed"
    if preprint.get("flora_eligible") is True:
        return "eligible_pending"
    return "other"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="Write JSON detail to this path")
    args = ap.parse_args()

    ddb = get_ddb()

    print("Scanning references table for items with flora_ref_pairs...", file=sys.stderr)
    matched_refs = scan_refs_with_flora_pairs(ddb)
    print(f"Found {len(matched_refs)} refs with flora_ref_pairs.", file=sys.stderr)

    osf_ids = {r["osf_id"] for r in matched_refs}
    print(f"Fetching {len(osf_ids)} preprint records...", file=sys.stderr)
    preprints = fetch_preprints(ddb, osf_ids)

    affected_refs: List[Dict[str, Any]] = []
    affected_preprints: Dict[str, Dict[str, Any]] = {}

    for ref in matched_refs:
        pid = ref["osf_id"]
        preprint = preprints.get(pid)
        if not preprint:
            continue
        self_dois = _build_self_dois(preprint)
        pairs = ref.get("flora_ref_pairs") or []
        self_pair_doi_rs: List[str] = []
        non_self_pair_doi_rs: List[str] = []
        for pair in pairs:
            doi_r = normalize_doi(pair.get("doi_r"))
            if not doi_r:
                continue
            (self_pair_doi_rs if doi_r in self_dois else non_self_pair_doi_rs).append(doi_r)
        if not self_pair_doi_rs:
            continue
        record = {
            "osf_id": pid,
            "ref_id": ref.get("ref_id"),
            "ref_doi": ref.get("doi"),
            "self_doi_rs": self_pair_doi_rs,
            "non_self_doi_rs": non_self_pair_doi_rs,
            "flora_replication_cited": ref.get("flora_replication_cited"),
            "preprint_email_sent": preprint.get("email_sent") is True,
            "preprint_flora_eligible": preprint.get("flora_eligible") is True,
        }
        affected_refs.append(record)
        if pid not in affected_preprints:
            cls = classify(preprint)
            affected_preprints[pid] = {
                "osf_id": pid,
                "class": cls,
                "email_sent": preprint.get("email_sent") is True,
                "email_sent_at": preprint.get("email_sent_at"),
                "email_recipient": preprint.get("email_recipient"),
                "email_message_id": preprint.get("email_message_id"),
                "trial_arm": preprint.get("trial_arm"),
                "flora_eligible": preprint.get("flora_eligible"),
                "self_doi_rs": set(),
                "had_only_self_replications": True,
                "ref_count": 0,
            }
        agg = affected_preprints[pid]
        agg["ref_count"] += 1
        agg["self_doi_rs"].update(self_pair_doi_rs)
        if non_self_pair_doi_rs:
            agg["had_only_self_replications"] = False

    by_class: Dict[str, List[Dict[str, Any]]] = {"emailed": [], "eligible_pending": [], "other": []}
    only_self_by_class: Dict[str, int] = {"emailed": 0, "eligible_pending": 0, "other": 0}
    for pid, agg in affected_preprints.items():
        agg["self_doi_rs"] = sorted(agg["self_doi_rs"])
        by_class[agg["class"]].append(agg)
        if agg["had_only_self_replications"]:
            only_self_by_class[agg["class"]] += 1

    print()
    print("=" * 70)
    print("FLoRA self-replication audit")
    print("=" * 70)
    print(f"  Total refs with FLoRA pairs scanned:     {len(matched_refs)}")
    print(f"  Refs with at least one self doi_r:       {len(affected_refs)}")
    print(f"  Distinct preprints affected:             {len(affected_preprints)}")
    print()
    print(f"  By preprint state:")
    for cls in ("emailed", "eligible_pending", "other"):
        n = len(by_class[cls])
        only_self = only_self_by_class[cls]
        print(f"    {cls:18}: {n:5d}   (of which only-self replications: {only_self})")
    print()
    print("  'only-self replications' = every FLoRA pair on every matched ref is self;")
    print("  these preprints would have been screened ineligible under the new fix.")
    print()

    if by_class["emailed"]:
        print("Emailed preprints with self-only replications (priority for follow-up):")
        for agg in by_class["emailed"]:
            if agg["had_only_self_replications"]:
                print(f"  - {agg['osf_id']:14}  arm={agg.get('trial_arm','?'):10}  "
                      f"sent={agg.get('email_sent_at','?')}  to={agg.get('email_recipient','?')}")
        print()

    if args.out:
        out = {
            "summary": {
                "matched_refs": len(matched_refs),
                "affected_refs": len(affected_refs),
                "affected_preprints": len(affected_preprints),
                "by_class": {k: len(v) for k, v in by_class.items()},
                "only_self_by_class": only_self_by_class,
            },
            "preprints": list(affected_preprints.values()),
            "refs": affected_refs,
        }
        Path(args.out).write_text(json.dumps(out, default=str, indent=2))
        print(f"Wrote detail to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
