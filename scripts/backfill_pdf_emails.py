#!/usr/bin/env python3
"""Backfill PDF-extracted emails for preprints processed before the _assign_pdf_emails fix.

When Grobid fails (no TEI), the old _assign_pdf_emails silently dropped
PDF-extracted emails because it only checked TEI names (name.given/name.surname),
which are None when Grobid fails.  This script re-extracts PDF emails and
re-matches them against each candidate's name using _best_email_for_author.

Usage::

    python scripts/backfill_pdf_emails.py --dry-run --limit 10
    python scripts/backfill_pdf_emails.py --dry-run --ids 5u8br_v3
    python scripts/backfill_pdf_emails.py --no-dry-run --limit 0

Flags::

    --dry-run / --no-dry-run   Report only vs apply updates (default: dry-run).
    --limit N                  Max preprints to process (0 = all).
    --ids ID [ID ...]          Restrict to specific OSF IDs.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EXTRACTION_DIR = ROOT / "osf_sync" / "extraction"
if str(EXTRACTION_DIR) not in sys.path:
    sys.path.insert(0, str(EXTRACTION_DIR))

from boto3.dynamodb.types import TypeDeserializer
from osf_sync.dynamo.preprints_repo import PreprintsRepo
from osf_sync.pdf import ensure_pdf_available_or_skip
from osf_sync.email.blacklist import load_blacklist
from get_mail_from_pdf import get_mail_from_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Re-used helpers from extract_author_list.py (copied to avoid pulling in
# ORCID/OpenAlex dependencies that this script does not need).
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,15}$", re.IGNORECASE)


def _clean_email(raw: Optional[str], *, allow_blacklist: bool = False) -> Optional[str]:
    if not raw:
        return None
    txt = raw.strip().strip("<>").strip().strip(";").strip(",")
    if txt.lower().startswith("mailto:"):
        txt = txt[7:].strip()
    if not txt:
        return None
    if not EMAIL_RE.match(txt):
        return None
    local, _, domain = txt.rpartition("@")
    if not local or not domain:
        return None
    local_l = local.lower()
    domain_l = domain.lower()
    if not allow_blacklist:
        blacklist = load_blacklist()
        if domain_l in blacklist["domains"]:
            return None
        if local_l in blacklist["locals"]:
            return None
        if txt.lower() in blacklist["emails"]:
            return None
    return txt


def _filter_emails(emails: List[str]) -> List[str]:
    valid: List[str] = []
    for raw in emails:
        val = _clean_email(raw)
        if val:
            valid.append(val)
    return list(dict.fromkeys(valid))


def _normalize_name(text: str) -> str:
    if not text:
        return ""
    txt = unicodedata.normalize("NFKD", text)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.lower()
    txt = re.sub(r"[^a-z]", "", txt)
    return txt


def _first_name(text: str) -> str:
    if not text:
        return ""
    return text.split()[0]


def _strip_middle_initials(text: str) -> str:
    if not text:
        return ""
    parts = [p for p in re.split(r"\s+", text.strip()) if p]
    cleaned = []
    for p in parts:
        p_clean = p.strip(".")
        if len(p_clean) == 1:
            continue
        cleaned.append(p)
    return " ".join(cleaned)


def _initials_only(text: str) -> str:
    if not text:
        return ""
    parts = [p for p in re.split(r"\s+", text.strip()) if p]
    initials = []
    for p in parts:
        p_clean = p.strip(".")
        if p_clean:
            initials.append(p_clean[0])
    return "".join(initials)


def _levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            insert = curr[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            curr.append(min(insert, delete, sub))
        prev = curr
    return prev[-1]


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    dist = _levenshtein_distance(a, b)
    denom = max(len(a), len(b))
    return 1.0 - (dist / denom if denom else 0.0)


def _best_email_for_author(
    given: str,
    surname: str,
    emails: List[str],
) -> Tuple[Optional[str], float]:
    if not emails:
        return None, 0.0
    best_email = None
    best_sim = 0.0
    given_n = _normalize_name(given)
    surname_n = _normalize_name(surname)
    given_no_mid = _strip_middle_initials(given)
    given_no_mid_n = _normalize_name(given_no_mid)
    full_gs = _normalize_name(f"{given} {surname}".strip())
    full_sg = _normalize_name(f"{surname} {given}".strip())
    full_gs_no_mid = _normalize_name(f"{given_no_mid} {surname}".strip())
    init_s = _normalize_name(f"{_first_name(given_no_mid)[:1]} {surname}".strip()) if given_no_mid else _normalize_name(surname)
    variants = [v for v in {full_gs, full_sg, full_gs_no_mid, init_s, given_n, given_no_mid_n, surname_n} if v]
    initials_raw = _initials_only(f"{given_no_mid} {surname}".strip())
    initials_norm = _normalize_name(initials_raw)
    initials_raw_full = _initials_only(f"{given} {surname}".strip())
    initials_norm_full = _normalize_name(initials_raw_full)
    for email in emails:
        local_raw = email.split("@", 1)[0].lower()
        local = _normalize_name(local_raw)
        if not local:
            continue
        sims: List[float] = []
        for v in variants:
            sims.append(_similarity(v, local))
            if v.startswith(local):
                sims.append(len(local) / len(v))
            if local in v:
                sims.append(len(local) / len(v))
            if v in local:
                sims.append(len(v) / len(local))
        sim = max(sims) if sims else 0.0
        if given_n and surname_n:
            if given_n in local and surname_n in local:
                sim += 0.20
            elif local == surname_n:
                sim -= 0.10
        if initials_norm or initials_norm_full:
            local_alpha = _normalize_name(re.sub(r"\d+", "", local_raw))
            for init in [initials_norm_full, initials_norm]:
                if not init:
                    continue
                if local_alpha == init:
                    sim = max(sim, 0.90)
                    break
                if local_alpha.startswith(init):
                    sim = max(sim, 0.75)
                    break
                if init in local_alpha:
                    sim = max(sim, 0.65)
                    break
        if sim > 1.0:
            sim = 1.0
        if sim > best_sim:
            best_sim = sim
            best_email = email
    return best_email, best_sim


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _deserialize_raw(raw: Any) -> Optional[dict]:
    if raw is None:
        return None
    if isinstance(raw, dict) and set(raw.keys()) == {"M"}:
        des = TypeDeserializer()
        return des.deserialize(raw)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    if isinstance(raw, dict):
        return raw
    return None


def _provider_id_from_raw(raw: dict) -> Optional[str]:
    rel = (raw.get("relationships") or {}).get("provider") or {}
    data = rel.get("data") or {}
    return data.get("id")


# ---------------------------------------------------------------------------
# Name splitting for stored candidates
# ---------------------------------------------------------------------------

BACKFILL_THRESHOLD = 0.90


def _split_candidate_name(name: str) -> Tuple[str, str]:
    """Split a 'Full Name' string into (given, surname).

    Uses the same last-token-is-surname heuristic used elsewhere.
    """
    parts = name.strip().split()
    if len(parts) >= 2:
        return " ".join(parts[:-1]), parts[-1]
    if len(parts) == 1:
        return "", parts[0]
    return "", ""


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _scan_preprints_with_candidates(
    repo: PreprintsRepo,
    *,
    limit: int,
    ids: Optional[List[str]],
) -> List[dict]:
    """Return preprint items that have author_email_candidates."""
    if ids:
        items = []
        for osf_id in ids:
            it = repo.t_preprints.get_item(Key={"osf_id": osf_id}).get("Item")
            if it:
                items.append(it)
        return items

    results: List[dict] = []
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "FilterExpression": "attribute_exists(author_email_candidates)",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_preprints.scan(**kwargs)
        for it in resp.get("Items", []):
            results.append(it)
            if limit and len(results) >= limit:
                return results
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return results


def process_preprint(
    item: dict,
    repo: PreprintsRepo,
    pdf_root: str,
    *,
    dry_run: bool,
) -> Optional[Dict[str, Any]]:
    """Process a single preprint.  Returns a change report or None."""
    osf_id = item["osf_id"]
    candidates = item.get("author_email_candidates")
    if not candidates:
        return None

    raw = _deserialize_raw(item.get("raw"))
    if not raw:
        logger.debug("[%s] no raw field, skipping", osf_id)
        return None

    provider_id = _provider_id_from_raw(raw) or item.get("provider_id") or "unknown"

    # Download PDF
    try:
        kind, pdf_path, reason = ensure_pdf_available_or_skip(
            osf_id=osf_id,
            provider_id=provider_id,
            raw=raw,
            dest_root=pdf_root,
        )
    except Exception as exc:
        logger.warning("[%s] PDF download failed: %s", osf_id, exc)
        return None

    if kind == "skipped" or not pdf_path:
        logger.debug("[%s] PDF skipped: %s", osf_id, reason)
        return None

    # Extract emails from PDF
    try:
        raw_pdf_emails = get_mail_from_pdf(pdf_path)
    except Exception as exc:
        logger.warning("[%s] PDF email extraction failed: %s", osf_id, exc)
        return None

    pdf_emails = _filter_emails(raw_pdf_emails)
    if not pdf_emails:
        return None

    # Check if any PDF email already present in candidates
    existing_emails = {
        (c.get("email") or "").lower()
        for c in candidates
        if c.get("email")
    }
    pdf_email_set = {e.lower() for e in pdf_emails}
    if pdf_email_set & existing_emails:
        # At least one PDF email already in candidates — already correct
        return None

    # Try to match PDF emails to candidates by name
    changes: List[Dict[str, str]] = []
    updated_candidates = []
    for cand in candidates:
        name = cand.get("name") or ""
        email = cand.get("email") or ""
        given, surname = _split_candidate_name(name)
        if not given and not surname:
            updated_candidates.append(cand)
            continue

        best_email, sim = _best_email_for_author(given, surname, pdf_emails)
        if best_email and sim >= BACKFILL_THRESHOLD:
            changes.append({
                "name": name,
                "old_email": email,
                "new_email": best_email,
                "similarity": f"{sim:.3f}",
            })
            new_cand = dict(cand)
            new_cand["email"] = best_email
            updated_candidates.append(new_cand)
        else:
            updated_candidates.append(cand)

    if not changes:
        return None

    # Apply update
    if not dry_run:
        try:
            repo.update_preprint_author_email_candidates(osf_id, updated_candidates)
        except Exception as exc:
            logger.error("[%s] update failed: %s", osf_id, exc)
            return None

    return {
        "osf_id": osf_id,
        "changes": changes,
        "applied": not dry_run,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill PDF emails for existing preprints")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Report only, no updates (default)")
    parser.add_argument("--no-dry-run", action="store_false", dest="dry_run",
                        help="Apply updates to DynamoDB")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max preprints to process (0 = all)")
    parser.add_argument("--ids", nargs="+", default=None,
                        help="Specific OSF IDs to process")
    args = parser.parse_args()

    pdf_root = os.environ.get("PDF_DEST_ROOT", "/tmp/preprints")
    repo = PreprintsRepo()

    logger.info("Scanning preprints (limit=%s, ids=%s, dry_run=%s)",
                args.limit or "all", args.ids or "none", args.dry_run)

    items = _scan_preprints_with_candidates(
        repo,
        limit=args.limit,
        ids=args.ids,
    )
    logger.info("Found %d preprints with author_email_candidates", len(items))

    total_affected = 0
    total_changes = 0
    all_results: List[Dict[str, Any]] = []

    for i, item in enumerate(items):
        osf_id = item["osf_id"]
        result = process_preprint(item, repo, pdf_root, dry_run=args.dry_run)
        if result:
            total_affected += 1
            total_changes += len(result["changes"])
            all_results.append(result)
            for ch in result["changes"]:
                action = "WOULD REPLACE" if args.dry_run else "REPLACED"
                logger.info("[%s] %s %s: %s -> %s (sim=%s)",
                            osf_id, action, ch["name"],
                            ch["old_email"], ch["new_email"], ch["similarity"])

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d preprints processed, %d affected",
                        i + 1, len(items), total_affected)

    logger.info("=" * 60)
    logger.info("Done. Processed %d preprints, %d affected, %d email changes%s",
                len(items), total_affected, total_changes,
                " (dry run)" if args.dry_run else "")

    if all_results:
        print(json.dumps(all_results, indent=2))


if __name__ == "__main__":
    main()
