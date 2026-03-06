"""Send citation-distance review emails and build mailto: action links."""
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


def _build_html(pid: str, flagged: List[Dict[str, Any]], review_id: str) -> str:
    """Build the HTML body for a citation distance review email."""
    n = len(flagged)
    lines: list[str] = []

    lines.append(f"<p>{n} reference(s) for preprint <b>{pid}</b> have a citation distance "
                 f"above {RUNTIME_CONFIG.validation.distance_threshold:.0%} and need your review "
                 f"before the notification email is sent.</p>")
    lines.append("<p>The distance measures how different the raw GROBID citation "
                 "(extracted from the PDF) is from the APA citation resolved via doi.org. "
                 "A high distance may indicate the DOI was matched to the wrong paper.</p>")

    # Quick actions
    approve_all_link = _mailto_link(
        f"Re: Citation Distance Review — {pid}",
        f"APPROVE ALL\n\nReview ID: {review_id}",
    )
    approve_all_apa_link = _mailto_link(
        f"Re: Citation Distance Review — {pid}",
        f"APPROVE ALL USE APA\n\nReview ID: {review_id}",
    )
    reject_all_link = _mailto_link(
        f"Re: Citation Distance Review — {pid}",
        f"REJECT ALL\n\nReview ID: {review_id}",
    )
    lines.append("<p><b>Quick actions</b> (click to open your email client with a pre-filled reply):</p>")
    lines.append(f'<p><a href="{approve_all_link}">APPROVE ALL</a> — All DOI matches are correct; use raw citations in email</p>')
    lines.append(f'<p><a href="{approve_all_apa_link}">APPROVE ALL USE APA</a> — All DOI matches are correct; use clean APA citations in email</p>')
    lines.append(f'<p><a href="{reject_all_link}">REJECT ALL</a> — All DOI matches are incorrect</p>')

    lines.append("<hr>")

    for i, ref in enumerate(flagged, 1):
        ref_id = ref.get("ref_id", "unknown")
        distance = ref.get("citation_distance", 0)
        doi = ref.get("original_doi") or ref.get("doi", "")
        raw = ref.get("raw_citation", "")
        apa = ref.get("citation_apa_resolved", "")

        lines.append(f"<h3>Reference {i}: {ref_id} — Distance: {distance:.1%}</h3>")
        lines.append(f"<p><b>DOI:</b> {doi}</p>")
        lines.append(f"<p><b>Raw citation (GROBID):</b></p><blockquote>{raw}</blockquote>")
        lines.append(f"<p><b>Resolved APA citation (doi.org):</b></p><blockquote>{apa}</blockquote>")

        approve_link = _mailto_link(
            f"Re: Citation Distance Review — {pid}",
            f"APPROVE {ref_id}\n\nReview ID: {review_id}",
        )
        approve_apa_link = _mailto_link(
            f"Re: Citation Distance Review — {pid}",
            f"APPROVE {ref_id} USE APA\n\nReview ID: {review_id}",
        )
        reject_link = _mailto_link(
            f"Re: Citation Distance Review — {pid}",
            f"REJECT {ref_id}\n\nReview ID: {review_id}",
        )
        lines.append(f'<p><a href="{approve_link}">APPROVE {ref_id}</a> '
                     f'| <a href="{approve_apa_link}">APPROVE {ref_id} USE APA</a> '
                     f'| <a href="{reject_link}">REJECT {ref_id}</a></p>')
        lines.append("<hr>")

    lines.append(f"<p><small>Review ID: {review_id}</small></p>")

    return "\n".join(lines)


def _build_plain(pid: str, flagged: List[Dict[str, Any]], review_id: str) -> str:
    """Build a plain-text fallback for the review email."""
    n = len(flagged)
    parts: list[str] = []

    parts.append(f"{n} reference(s) for preprint {pid} have a citation distance "
                 f"above {RUNTIME_CONFIG.validation.distance_threshold:.0%} and need your review "
                 f"before the notification email is sent.")
    parts.append("")
    parts.append("Reply with APPROVE ALL, APPROVE ALL USE APA, or REJECT ALL, or per-reference decisions below.")
    parts.append("")

    for i, ref in enumerate(flagged, 1):
        ref_id = ref.get("ref_id", "unknown")
        distance = ref.get("citation_distance", 0)
        doi = ref.get("original_doi") or ref.get("doi", "")
        raw = ref.get("raw_citation", "")
        apa = ref.get("citation_apa_resolved", "")

        parts.append(f"--- Reference {i}: {ref_id} — Distance: {distance:.1%} ---")
        parts.append(f"DOI: {doi}")
        parts.append(f"Raw citation (GROBID): {raw}")
        parts.append(f"Resolved APA citation (doi.org): {apa}")
        parts.append(f"Reply: APPROVE {ref_id} / APPROVE {ref_id} USE APA / REJECT {ref_id}")
        parts.append("")

    parts.append(f"Review ID: {review_id}")
    return "\n".join(parts)


def send_citation_review_email(
    pid: str,
    flagged: List[Dict[str, Any]],
    review_id: str,
) -> Optional[str]:
    """Send a citation distance review email to the configured reviewer.

    Returns the message ID if sent, or None if no reviewer is configured.
    """
    reviewer = RUNTIME_CONFIG.validation.reviewer_email
    if not reviewer:
        log.warning("No reviewer_email configured; skipping review email for %s", pid)
        return None

    subject = f"Citation Distance Review — {pid}"
    html_body = _build_html(pid, flagged, review_id)
    plain_body = _build_plain(pid, flagged, review_id)

    result = send_email(
        to=reviewer,
        subject=subject,
        html_body=html_body,
        plain_body=plain_body,
    )
    msg_id = result.get("id")
    log.info("Citation review email sent", extra={"pid": pid, "review_id": review_id, "msg_id": msg_id})
    return msg_id
