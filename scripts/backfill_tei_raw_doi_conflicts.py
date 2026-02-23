#!/usr/bin/env python3
"""
Backfill TEI DOI conflicts using DOI(s) found in raw citation text.

Safe behavior:
- Only touches rows with doi_source in {"tei", "tei_raw", "tei_raw_backfill"}.
- If exactly one DOI is present in raw_citation and differs from stored DOI, replace with raw DOI.
- If multiple DOIs are present and stored DOI is not among them, clear stored DOI as ambiguous.
- Supports dry-run (default) and apply mode.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from osf_sync.augmentation.doi_multi_method_lookup import doi_resolves, normalize_doi


RAW_DOI_RE = re.compile(r"10\.[0-9]{4,9}/\S+", re.IGNORECASE)
EXPLICIT_DOI_RE = re.compile(
    r"(?:https?://(?:dx\.)?doi\.org/|doi\s*:\s*)(10\.[0-9]{4,9}/\S+)",
    re.IGNORECASE,
)


def _is_plausible_doi(doi: Optional[str]) -> bool:
    d = str(doi or "").strip().lower()
    if not d or "/" not in d:
        return False
    _, suffix = d.split("/", 1)
    if len(suffix) < 6:
        return False
    if suffix.endswith(("-", ".", "/")):
        return False
    if not suffix[-1].isalnum():
        return False
    return True


def _extract_dois_from_raw(raw_citation: Optional[str], *, allow_generic: bool = False) -> List[str]:
    txt = str(raw_citation or "").strip()
    if not txt:
        return []
    patterns = [EXPLICIT_DOI_RE]
    if allow_generic:
        patterns.append(RAW_DOI_RE)
    out: List[str] = []
    seen = set()
    for pat in patterns:
        for m in pat.finditer(txt):
            val = m.group(1) if m.lastindex else m.group(0)
            d = normalize_doi(val, source="text")
            if not d or d in seen:
                continue
            if not _is_plausible_doi(d):
                continue
            seen.add(d)
            out.append(d)
    return out


@lru_cache(maxsize=65536)
def _resolve_cached(doi: str) -> Optional[bool]:
    try:
        return doi_resolves(doi)
    except Exception:
        return None


@lru_cache(maxsize=65536)
def _repair_doi_suffix_if_needed(doi: str, max_trim: int = 8) -> Optional[str]:
    candidate = str(doi or "").strip().lower()
    if not candidate:
        return None
    trimmed = candidate
    for _ in range(max_trim):
        if not trimmed or not trimmed[-1].isdigit():
            break
        trimmed = trimmed[:-1]
        if len(trimmed) < 8:
            break
        if _resolve_cached(trimmed) is True:
            return trimmed
    return None


def _iter_rows(table, *, osf_id: Optional[str]) -> List[Dict[str, Any]]:
    if osf_id:
        out: List[Dict[str, Any]] = []
        last = None
        while True:
            kwargs: Dict[str, Any] = {
                "KeyConditionExpression": "osf_id = :o",
                "ExpressionAttributeValues": {":o": osf_id},
                "ProjectionExpression": "osf_id, ref_id, doi, doi_source, raw_citation",
                "ConsistentRead": True,
            }
            if last:
                kwargs["ExclusiveStartKey"] = last
            resp = table.query(**kwargs)
            out.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
        return out

    out = []
    last = None
    while True:
        kwargs = {
            "ProjectionExpression": "osf_id, ref_id, doi, doi_source, raw_citation",
            "ConsistentRead": True,
        }
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        out.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
    return out


def main() -> int:
    load_dotenv(".env")

    ap = argparse.ArgumentParser(description="Backfill TEI DOI conflicts from raw citation DOI.")
    ap.add_argument("--table", default=os.environ.get("DDB_TABLE_REFERENCES", "prod_preprint_references"))
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))
    ap.add_argument("--osf-id", default=None, help="Optional single-preprint scope for safe rollout.")
    ap.add_argument("--limit", type=int, default=0, help="Optional max number of updates (0 = no cap).")
    ap.add_argument(
        "--validate-resolves",
        action="store_true",
        default=False,
        help="Require raw DOI to resolve via doi.org handle API (strict, slower).",
    )
    ap.add_argument(
        "--no-validate-resolves",
        dest="validate_resolves",
        action="store_false",
        help="Skip resolve validation (default).",
    )
    ap.add_argument(
        "--allow-generic",
        action="store_true",
        help="Also parse bare 10.x DOIs not preceded by doi.org/doi: (less strict).",
    )
    ap.add_argument(
        "--no-clear-multi-mismatch",
        action="store_true",
        help="Do not clear rows where multiple raw DOIs exist and stored DOI is not among them.",
    )
    ap.add_argument("--apply", action="store_true", help="Persist updates (default: dry-run).")
    args = ap.parse_args()

    ddb = boto3.resource(
        "dynamodb",
        region_name=args.region,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
    table = ddb.Table(args.table)

    rows = _iter_rows(table, osf_id=args.osf_id)
    print(f"Scanned rows: {len(rows)} from table={args.table} region={args.region}")

    touched = 0
    candidates = 0
    candidates_replace_single = 0
    candidates_clear_multi = 0
    skipped_missing_raw_doi = 0
    skipped_resolve = 0
    skipped_condition = 0
    skipped_truncated_prefix = 0
    errors = 0
    samples: List[Tuple[str, str, str, str, str]] = []

    now = dt.datetime.now(dt.timezone.utc).isoformat()
    for row in rows:
        src = (row.get("doi_source") or "").strip().lower()
        if src not in {"tei", "tei_raw", "tei_raw_backfill"}:
            continue

        old_doi = normalize_doi(row.get("doi"), source="text")
        raw_dois = _extract_dois_from_raw(
            row.get("raw_citation"), allow_generic=bool(args.allow_generic)
        )
        if not raw_dois:
            skipped_missing_raw_doi += 1
            continue
        if not old_doi:
            continue
        if old_doi in raw_dois:
            continue

        action = ""
        new_doi = ""
        if len(raw_dois) == 1:
            raw_doi = raw_dois[0]
            # Safety: prefix relations are often line-wrap truncation artifacts.
            if old_doi.startswith(raw_doi) or raw_doi.startswith(old_doi):
                skipped_truncated_prefix += 1
                continue
            action = "replace_single"
            new_doi = raw_doi
            if args.validate_resolves:
                state = _resolve_cached(new_doi)
                if state is False:
                    repaired = _repair_doi_suffix_if_needed(new_doi)
                    if repaired:
                        new_doi = repaired
                        state = True
                # Mirror extraction behavior: only reject definitive non-resolving DOIs.
                if state is False:
                    skipped_resolve += 1
                    continue
        else:
            if args.no_clear_multi_mismatch:
                continue
            action = "clear_multi_ambiguous"

        candidates += 1
        if action == "replace_single":
            candidates_replace_single += 1
        else:
            candidates_clear_multi += 1
        if len(samples) < 15:
            samples.append((row.get("osf_id", ""), row.get("ref_id", ""), old_doi, (new_doi or "-"), action))

        if args.limit and touched >= args.limit:
            continue
        if not args.apply:
            touched += 1
            continue

        osf_id = row.get("osf_id")
        ref_id = row.get("ref_id")
        if not osf_id or not ref_id:
            continue
        try:
            if action == "replace_single":
                table.update_item(
                    Key={"osf_id": osf_id, "ref_id": ref_id},
                    UpdateExpression="SET doi=:d, has_doi=:hd, doi_source=:src_new, updated_at=:t",
                    ExpressionAttributeValues={
                        ":d": new_doi,
                        ":hd": True,
                        ":src_new": "tei_raw_backfill",
                        ":t": now,
                        ":src_old": row.get("doi_source"),
                        ":old": row.get("doi"),
                    },
                    ConditionExpression="doi_source=:src_old AND doi=:old",
                )
            else:
                table.update_item(
                    Key={"osf_id": osf_id, "ref_id": ref_id},
                    UpdateExpression=(
                        "SET has_doi=:hd, tei_doi_conflict_multi=:flag, updated_at=:t "
                        "REMOVE doi, doi_source, doi_checked_at"
                    ),
                    ExpressionAttributeValues={
                        ":hd": False,
                        ":flag": True,
                        ":t": now,
                        ":src_old": row.get("doi_source"),
                        ":old": row.get("doi"),
                    },
                    ConditionExpression="doi_source=:src_old AND doi=:old",
                )
            touched += 1
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                skipped_condition += 1
                continue
            errors += 1
        except Exception:
            errors += 1

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Mode: {mode}")
    print(f"Candidates after filters: {candidates}")
    print(f"  - replace_single: {candidates_replace_single}")
    print(f"  - clear_multi_ambiguous: {candidates_clear_multi}")
    print(f"Rows updated/that would update: {touched}")
    print(f"Skipped (raw DOI missing): {skipped_missing_raw_doi}")
    print(f"Skipped (truncated prefix/suffix): {skipped_truncated_prefix}")
    print(f"Skipped (resolve validation failed): {skipped_resolve}")
    print(f"Skipped (conditional changed row): {skipped_condition}")
    print(f"Errors: {errors}")
    print("Sample changes (osf_id, ref_id, old_doi, new_doi_or_dash, action):")
    for s in samples:
        print("\t".join(s))
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
