#!/usr/bin/env python3
"""Deep-dive into a few excluded preprints to understand what emails
were found in TEI/PDF but rejected by the contactability filter.

Usage:
    python scripts/diagnose_email_rejection.py [--ids ID1 ID2 ...]
"""

import json
import os
import re
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
from lxml import etree

# Email validation imports (same as extraction code)
try:
    from email_validator import validate_email, EmailNotValidError
except ImportError:
    validate_email = None
    EmailNotValidError = Exception

TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,15}", re.IGNORECASE)


def get_ddb():
    local_url = os.getenv("DYNAMO_LOCAL_URL")
    region = os.getenv("AWS_REGION", "eu-central-1")
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})
    if local_url:
        return boto3.resource(
            "dynamodb", region_name=region, endpoint_url=local_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
            config=cfg,
        )
    return boto3.resource("dynamodb", region_name=region, config=cfg)


def get_excluded_sample(ddb, n=10):
    table = ddb.Table(os.getenv("DDB_TABLE_EXCLUDED_PREPRINTS", "prod_excluded_preprints"))
    # Get most recent exclusions
    resp = table.query(
        IndexName="by_reason",
        KeyConditionExpression=Key("exclusion_reason").eq("no_author_contacts_extracted"),
        ScanIndexForward=False,
        Limit=n,
    )
    return [item["osf_id"] for item in resp.get("Items", [])]


def get_preprint(ddb, osf_id):
    table = ddb.Table(os.getenv("DDB_TABLE_PREPRINTS", "prod_preprints"))
    return table.get_item(Key={"osf_id": osf_id}).get("Item")


def get_tei(ddb, osf_id):
    table = ddb.Table(os.getenv("DDB_TABLE_TEI", "prod_preprint_tei"))
    item = table.get_item(Key={"osf_id": osf_id}).get("Item")
    if item:
        return item.get("tei_xml") or item.get("tei")
    return None


def extract_emails_from_tei(tei_xml):
    """Extract email addresses from TEI XML."""
    emails = []
    try:
        tree = etree.fromstring(tei_xml.encode() if isinstance(tei_xml, str) else tei_xml)
        # Check <email> elements
        for email_el in tree.iter("{http://www.tei-c.org/ns/1.0}email"):
            text = (email_el.text or "").strip()
            if text and "@" in text:
                emails.append(("tei_element", text))
        # Also check for emails in the full text
        text_content = etree.tostring(tree, method="text", encoding="unicode")
        for match in EMAIL_RE.findall(text_content):
            if match not in [e[1] for e in emails]:
                emails.append(("tei_text_regex", match))
    except Exception as exc:
        emails.append(("parse_error", str(exc)))
    return emails


def extract_authors_from_tei(tei_xml):
    """Extract author names from TEI XML."""
    authors = []
    try:
        tree = etree.fromstring(tei_xml.encode() if isinstance(tei_xml, str) else tei_xml)
        for author in tree.findall(".//{http://www.tei-c.org/ns/1.0}author"):
            persname = author.find("{http://www.tei-c.org/ns/1.0}persName")
            if persname is not None:
                forename = persname.findtext("{http://www.tei-c.org/ns/1.0}forename", "")
                surname = persname.findtext("{http://www.tei-c.org/ns/1.0}surname", "")
                name = f"{forename} {surname}".strip()
                email_el = author.find("{http://www.tei-c.org/ns/1.0}email")
                email = (email_el.text or "").strip() if email_el is not None else ""
                authors.append({"name": name, "email": email})
    except Exception:
        pass
    return authors


def get_cached_orcid_data(ddb, osf_id):
    """Check API cache for ORCID and OSF contributor data."""
    table = ddb.Table(os.getenv("DDB_TABLE_API_CACHE", "prod_api_cache"))
    results = {}

    # Check OSF contributors cache
    key = f"osf_contributors::{osf_id}"
    item = table.get_item(Key={"cache_key": key}).get("Item")
    if item:
        payload = item.get("payload", [])
        results["osf_contributors"] = len(payload) if isinstance(payload, list) else "non-list"
        # Extract ORCIDs from contributors
        orcids = []
        for contrib in (payload if isinstance(payload, list) else []):
            attrs = ((contrib.get("relationships") or {}).get("users") or {}).get("data", {})
            user_id = attrs.get("id", "")
            if user_id:
                user_key = f"osf_user::{user_id}"
                # Could look up user cache here but that's N queries per preprint
                pass
        results["osf_contributors_cached"] = True
    else:
        results["osf_contributors_cached"] = False

    return results


def validate_single_email(email):
    """Validate an email using the same logic as the pipeline."""
    if not validate_email:
        return "email_validator not installed"
    try:
        result = validate_email(email, check_deliverability=True)
        return f"VALID -> {result.normalized}"
    except EmailNotValidError as e:
        return f"INVALID: {e}"
    except Exception as e:
        return f"ERROR: {e}"


def main():
    ddb = get_ddb()

    if "--ids" in sys.argv:
        idx = sys.argv.index("--ids")
        ids = sys.argv[idx + 1:]
    else:
        ids = get_excluded_sample(ddb, n=10)

    print(f"Diagnosing {len(ids)} excluded preprints\n")

    for osf_id in ids:
        print(f"\n{'=' * 70}")
        print(f"OSF ID: {osf_id}")
        print(f"{'=' * 70}")

        preprint = get_preprint(ddb, osf_id)
        if not preprint:
            print("  [!] Preprint record NOT FOUND in preprints table")
            continue

        print(f"  Title: {(preprint.get('title') or 'N/A')[:100]}")
        print(f"  Provider: {preprint.get('provider_id', 'N/A')}")
        print(f"  Excluded: {preprint.get('excluded', False)}")
        print(f"  Has queue_pdf: {bool(preprint.get('queue_pdf'))}")
        candidates = preprint.get("author_email_candidates")
        print(f"  author_email_candidates: {candidates}")

        # Check TEI
        tei_xml = get_tei(ddb, osf_id)
        if tei_xml:
            print(f"\n  --- TEI Analysis ---")
            authors = extract_authors_from_tei(tei_xml)
            print(f"  TEI authors found: {len(authors)}")
            for i, a in enumerate(authors):
                email_note = f" <{a['email']}>" if a["email"] else " (no email)"
                print(f"    [{i}] {a['name']}{email_note}")

            emails = extract_emails_from_tei(tei_xml)
            print(f"  All emails in TEI: {len(emails)}")
            for source, email in emails:
                if validate_email:
                    status = validate_single_email(email)
                    print(f"    [{source}] {email} -> {status}")
                else:
                    print(f"    [{source}] {email}")
        else:
            print(f"\n  [!] No TEI XML found in DynamoDB")

        # Check API cache
        cache_info = get_cached_orcid_data(ddb, osf_id)
        print(f"\n  --- API Cache ---")
        print(f"  OSF contributors cached: {cache_info.get('osf_contributors_cached', False)}")
        if cache_info.get("osf_contributors"):
            print(f"  Contributor count: {cache_info['osf_contributors']}")

        print()


if __name__ == "__main__":
    main()
