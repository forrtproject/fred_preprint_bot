"""Send batch citation-distance review emails and build mailto: action links."""
from __future__ import annotations

import logging
import os
import urllib.parse
from typing import Any, Dict, List, Optional

from .gmail import send_email
from ..runtime_config import RUNTIME_CONFIG

log = logging.getLogger(__name__)

_SENDER = "flora@replications.forrt.org"


def _mailto_link(subject: str, body: str) -> str:
    """Build a mailto: link that opens the user's email client with a pre-filled reply."""
    sender = os.environ.get("GMAIL_SENDER_ADDRESS", _SENDER)
    params = urllib.parse.urlencode({"subject": subject, "body": body}, quote_via=urllib.parse.quote)
    return f"mailto:{sender}?{params}"


def _build_batch_html(all_flagged: Dict[str, List[Dict[str, Any]]], review_id: str) -> str:
    """Build the HTML body for a consolidated batch citation distance review email."""
    total_refs = sum(len(refs) for refs in all_flagged.values())
    n_preprints = len(all_flagged)
    subject = "Re: Citation Distance Review — Batch"
    lines: list[str] = []

    lines.append(f"<p>{total_refs} reference(s) across {n_preprints} preprint(s) have a citation distance "
                 f"above {RUNTIME_CONFIG.validation.distance_threshold:.0%} and need your review "
                 f"before notification emails are sent.</p>")
    lines.append("<p>The distance measures how different the raw GROBID citation "
                 "(extracted from the PDF) is from the APA citation resolved via doi.org. "
                 "A high distance may indicate the DOI was matched to the wrong paper.</p>")

    for pid, refs in all_flagged.items():
        lines.append(f"<h2>Preprint: {pid}</h2>")
        for i, ref in enumerate(refs, 1):
            ref_id = ref.get("ref_id", "unknown")
            distance = ref.get("citation_distance", 0)
            doi = ref.get("original_doi") or ref.get("doi", "")
            raw = ref.get("raw_citation", "")
            apa = ref.get("citation_apa_resolved", "")
            target = f"{pid}/{ref_id}"

            lines.append(f"<h3>Reference {i}: {ref_id} — Distance: {distance:.1%}</h3>")
            lines.append(f"<p><b>DOI:</b> {doi}</p>")
            lines.append(f"<p><b>Raw citation (GROBID):</b></p><blockquote>{raw}</blockquote>")
            lines.append(f"<p><b>Resolved APA citation (doi.org):</b></p><blockquote>{apa}</blockquote>")

            approve_link = _mailto_link(subject, f"APPROVE {target}\n\nReview ID: {review_id}")
            approve_apa_link = _mailto_link(subject, f"APPROVE {target} USE APA\n\nReview ID: {review_id}")
            reject_link = _mailto_link(subject, f"REJECT {target}\n\nReview ID: {review_id}")
            lines.append(f'<p><a href="{approve_link}">APPROVE {target}</a> '
                         f'| <a href="{approve_apa_link}">APPROVE {target} USE APA</a> '
                         f'| <a href="{reject_link}">REJECT {target}</a></p>')
            lines.append("<hr>")

    # Remainder actions at the bottom
    approve_rem_link = _mailto_link(subject, f"APPROVE REMAINDER\n\nReview ID: {review_id}")
    approve_rem_apa_link = _mailto_link(subject, f"APPROVE REMAINDER USE APA\n\nReview ID: {review_id}")
    reject_rem_link = _mailto_link(subject, f"REJECT REMAINDER\n\nReview ID: {review_id}")

    lines.append("<p><b>Bulk actions</b> (apply to all refs not individually decided above):</p>")
    lines.append(f'<p><a href="{approve_rem_link}">APPROVE REMAINDER</a> — All DOI matches are correct; use raw citations</p>')
    lines.append(f'<p><a href="{approve_rem_apa_link}">APPROVE REMAINDER USE APA</a> — All DOI matches are correct; use clean APA citations</p>')
    lines.append(f'<p><a href="{reject_rem_link}">REJECT REMAINDER</a> — All DOI matches are incorrect</p>')

    lines.append(f"<p><small>Review ID: {review_id}</small></p>")
    return "\n".join(lines)


def _build_batch_plain(all_flagged: Dict[str, List[Dict[str, Any]]], review_id: str) -> str:
    """Build a plain-text fallback for the batch review email."""
    total_refs = sum(len(refs) for refs in all_flagged.values())
    n_preprints = len(all_flagged)
    parts: list[str] = []

    parts.append(f"{total_refs} reference(s) across {n_preprints} preprint(s) have a citation distance "
                 f"above {RUNTIME_CONFIG.validation.distance_threshold:.0%} and need your review "
                 f"before notification emails are sent.")
    parts.append("")
    parts.append("Reply with per-reference decisions (APPROVE/REJECT {osf_id}/{ref_id}) "
                 "or APPROVE/REJECT REMAINDER for bulk action.")
    parts.append("")

    for pid, refs in all_flagged.items():
        parts.append(f"=== Preprint: {pid} ===")
        parts.append("")
        for i, ref in enumerate(refs, 1):
            ref_id = ref.get("ref_id", "unknown")
            distance = ref.get("citation_distance", 0)
            doi = ref.get("original_doi") or ref.get("doi", "")
            raw = ref.get("raw_citation", "")
            apa = ref.get("citation_apa_resolved", "")
            target = f"{pid}/{ref_id}"

            parts.append(f"--- Reference {i}: {ref_id} — Distance: {distance:.1%} ---")
            parts.append(f"DOI: {doi}")
            parts.append(f"Raw citation (GROBID): {raw}")
            parts.append(f"Resolved APA citation (doi.org): {apa}")
            parts.append(f"Reply: APPROVE {target} / APPROVE {target} USE APA / REJECT {target}")
            parts.append("")

    parts.append(f"Review ID: {review_id}")
    return "\n".join(parts)


def send_batch_citation_review_email(
    all_flagged: Dict[str, List[Dict[str, Any]]],
    review_id: str,
) -> Optional[str]:
    """Send a consolidated batch citation distance review email to the configured reviewer.

    Returns the message ID if sent, or None if no reviewer is configured.
    """
    reviewer = RUNTIME_CONFIG.validation.reviewer_email
    if not reviewer:
        log.warning("No reviewer_email configured; skipping batch review email")
        return None

    total_refs = sum(len(refs) for refs in all_flagged.values())
    n_preprints = len(all_flagged)
    subject = f"Citation Distance Review — {total_refs} ref(s) across {n_preprints} preprint(s)"
    html_body = _build_batch_html(all_flagged, review_id)
    plain_body = _build_batch_plain(all_flagged, review_id)

    result = send_email(
        to=reviewer,
        subject=subject,
        html_body=html_body,
        plain_body=plain_body,
    )
    msg_id = result.get("id")
    log.info("Batch citation review email sent",
             extra={"review_id": review_id, "msg_id": msg_id,
                    "preprints": n_preprints, "refs": total_refs})
    return msg_id
