from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from ..dynamo.preprints_repo import PreprintsRepo
from ..runtime_config import RUNTIME_CONFIG

logger = logging.getLogger(__name__)

# Map OSF provider IDs to display names
_PROVIDER_NAMES = {
    "osf": "OSF Preprints",
    "psyarxiv": "PsyArXiv",
    "socarxiv": "SocArXiv",
    "edarxiv": "EdArXiv",
    "metaarxiv": "MetaArXiv",
    "mediarxiv": "MediarXiv",
    "africarxiv": "AfricArXiv",
    "arabixiv": "Arabixiv",
    "biohackrxiv": "BioHackrXiv",
    "eartharxiv": "EarthArXiv",
    "ecoevorxiv": "EcoEvoRxiv",
    "frenxiv": "Frenxiv",
    "inarxiv": "INArxiv",
    "marxiv": "MarXiv",
    "sportrxiv": "SportRxiv",
    "thesiscommons": "Thesis Commons",
}


def _provider_display_name(provider_id: str | None) -> str:
    if not provider_id:
        return "OSF Preprints"
    return _PROVIDER_NAMES.get(provider_id.lower(), provider_id)


def _build_greeting(first_names: List[str]) -> str:
    """Build a greeting addressing recipients by first name.

    Single recipient: "Dear Alice,"
    Multiple: "Dear Alice, dear Bob,"
    """
    if not first_names:
        return "Dear Author,"
    parts = [f"Dear {first_names[0]}"] + [f"dear {n}" for n in first_names[1:]]
    return ", ".join(parts) + ","


def assemble_email_context(osf_id: str, repo: PreprintsRepo | None = None) -> Optional[Dict[str, Any]]:
    """Gather preprint + FLORA data into a template context dict.

    Returns None if the preprint is not eligible for emailing.
    """
    repo = repo or PreprintsRepo()
    cfg = RUNTIME_CONFIG.email

    # Fetch preprint record
    preprint = repo.t_preprints.get_item(Key={"osf_id": osf_id}).get("Item")
    if not preprint:
        logger.warning("Preprint not found", extra={"osf_id": osf_id})
        return None

    title = preprint.get("title") or "(untitled)"
    provider_id = preprint.get("provider_id") or "osf"
    doi = preprint.get("doi") or ""
    candidates = preprint.get("author_email_candidates") or []
    if not candidates:
        logger.info("No email candidates", extra={"osf_id": osf_id})
        return None

    # Collect all candidates with valid-looking emails
    recipients: List[Dict[str, str]] = []
    for cand in candidates:
        email = (cand.get("email") or "").strip()
        if not email:
            continue
        full_name = cand.get("name", "")
        parts = full_name.rsplit(" ", 1) if full_name else ["", ""]
        recipients.append({
            "email": email,
            "first_name": parts[0] if len(parts) > 1 else full_name,
            "last_name": parts[1] if len(parts) > 1 else "",
        })

    if not recipients:
        logger.info("No email candidates with addresses", extra={"osf_id": osf_id})
        return None

    # Build greeting addressing all recipients by first name
    author_greeting = _build_greeting([r["first_name"] for r in recipients])

    # Fetch all references for this preprint
    all_refs = _fetch_all_refs(osf_id, repo)

    # Safety check: skip preprints still awaiting citation distance validation
    if preprint.get("flora_citation_validation_pending"):
        logger.info("Preprint has pending citation validation; skipping email", extra={"osf_id": osf_id})
        return None

    # Separate into eligible (replication NOT cited → notify author)
    # and already-cited (replication IS cited — author is aware).
    # Exclude refs whose DOI match was rejected by the reviewer.
    eligible_refs = [
        r for r in all_refs
        if r.get("flora_replication_cited") is False
        and r.get("flora_ref_pairs")
        and r.get("citation_validation_status") != "rejected"
    ]
    cited_refs = [r for r in all_refs if r.get("flora_replication_cited") is True]

    if not eligible_refs:
        logger.info("No eligible references for email", extra={"osf_id": osf_id})
        return None

    some_replications_cited = len(cited_refs) > 0
    cited_replication_count = len(cited_refs)

    # Build originals list
    originals: List[Dict[str, Any]] = []
    for ref in eligible_refs:
        ref_pairs = ref.get("flora_ref_pairs") or []
        if not ref_pairs:
            continue

        # Each ref_pair has original + replication info
        # Group replications by original DOI
        original_info = _build_original_entry(ref, ref_pairs)
        if original_info:
            originals.append(original_info)

    if not originals:
        return None

    # Build URLs
    unsubscribe_mailto = (
        f"mailto:{cfg.sender_address}"
        f"?subject={quote('Unsubscribe')}"
        f"&body={quote('Please unsubscribe me from FLoRA-Notify emails')}"
    )

    feedback_base = cfg.feedback_base_url.rstrip("/")
    report_url = f"{cfg.report_base_url.rstrip('/')}/{osf_id}"

    # Singular vs multiple: one original study cited → singular, otherwise multiple.
    # Also compute total replication count for subject-line pluralisation in the template.
    cardinality = "singular" if len(originals) == 1 else "multiple"
    total_replication_count = sum(len(o["replications"]) for o in originals)

    context = {
        "preprint_title": title,
        "server_name": _provider_display_name(provider_id),
        "author_greeting": author_greeting,
        "originals": originals,
        "total_replication_count": total_replication_count,
        "some_replications_cited": some_replications_cited,
        "cited_replication_count": cited_replication_count,
        "flora_learn_more_url": cfg.flora_learn_more_url,
        "report_url": report_url,
        "feedback_helpful_url": f"{feedback_base}?email_choice=helpful_{cardinality}&server_id={provider_id}",
        "feedback_not_helpful_url": f"{feedback_base}?email_choice=not_helpful_{cardinality}&server_id={provider_id}",
        "feedback_already_aware_url": f"{feedback_base}?email_choice=already_aware&server_id={provider_id}",
        "feedback_report_error_url": f"{feedback_base}?email_choice=report_error_{cardinality}&server_id={provider_id}",
        "unsubscribe_mailto": unsubscribe_mailto,
        "_recipients": recipients,
        "_osf_id": osf_id,
    }

    return context


def _fetch_all_refs(osf_id: str, repo: PreprintsRepo) -> List[Dict[str, Any]]:
    """Fetch all references for a preprint that have been through FLORA screening."""
    items: List[Dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": "osf_id = :oid",
            "ExpressionAttributeValues": {":oid": osf_id},
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_refs.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    # Only return refs that have been through FLORA screening
    return [r for r in items if r.get("flora_replication_cited") is not None]


def _strip_doi_from_ref(text: str, doi: str) -> str:
    """Remove DOI URLs/text already embedded in a reference string."""
    if not doi or not text:
        return text
    # Remove any doi.org URL — catches case variations, spacing artifacts, and URL encoding
    text = re.sub(r"https?://(?:dx\.)?doi\.org/\S*", "", text, flags=re.IGNORECASE)
    # Also strip textual "DOI: xxx" labels (some databases embed these in the reference string)
    doi_pat = re.escape(doi).replace("/", r"/\s*")
    text = re.sub(r"\bDOI:\s*" + doi_pat, "", text, flags=re.IGNORECASE)
    # Strip trailing whitespace/punctuation artifacts left behind
    return text.rstrip(". ").rstrip()


def _build_original_entry(ref: Dict[str, Any], ref_pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Build a single original entry with its replications from FLORA ref_pairs."""
    if not ref_pairs:
        return None

    # The ref itself is the "original" that was cited in the preprint
    original_doi = ref.get("doi") or ""
    original_citation = ref.get("raw_citation") or ref.get("title") or "(unknown reference)"
    if original_doi:
        original_citation = _strip_doi_from_ref(original_citation, original_doi)
    original_doi_url = f"https://doi.org/{original_doi}" if original_doi else ""

    replications: List[Dict[str, Any]] = []
    for pair in ref_pairs:
        rep = pair.get("replication") or pair
        rep_doi = (
            pair.get("doi_r")
            or rep.get("doi")
            or pair.get("replication_doi")
            or ""
        )
        rep_ref = (
            pair.get("apa_ref_r")
            or rep.get("reference")
            or rep.get("title")
            or rep_doi
            or "(unknown)"
        )
        rep_outcome = (
            pair.get("replication_outcome")
            or pair.get("outcome")
            or rep.get("outcome")
            or "unknown"
        )
        rep_oa_url = pair.get("oa_url_r") or rep.get("oa_url") or ""

        if rep_doi:
            rep_ref = _strip_doi_from_ref(rep_ref, rep_doi)

        replications.append({
            "full_reference": rep_ref,
            "doi": rep_doi,
            "doi_url": f"https://doi.org/{rep_doi}" if rep_doi else "",
            "oa_url": rep_oa_url,
            "outcome": rep_outcome,
        })

    if not replications:
        return None

    return {
        "full_reference": original_citation,
        "doi": original_doi,
        "doi_url": original_doi_url,
        "replications": replications,
    }
