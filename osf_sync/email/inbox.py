"""Scan Gmail inbox via IMAP for bounces and unsubscribe replies.

Detected addresses are stored in the DynamoDB suppression table so they
are excluded from future email sends.
"""
from __future__ import annotations

import email
import email.policy
import imaplib
import logging
import os
import re
from typing import Any, Dict, Optional, Set

log = logging.getLogger(__name__)

# RFC 5322 simplified email regex — good enough for bounce/unsub extraction
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _sender_address() -> str:
    return os.environ.get("GMAIL_SENDER_ADDRESS", "flora@replications.forrt.org")


def _is_prod() -> bool:
    return (os.environ.get("PIPELINE_ENV", "dev") or "dev").strip().lower() == "prod"


def _extract_bounce_addresses(msg: email.message.Message) -> Set[str]:
    """Extract failed recipient addresses from a bounce (DSN) message."""
    addresses: Set[str] = set()
    self_addr = _sender_address().lower()

    # Walk MIME parts looking for message/delivery-status (RFC 3464)
    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "message/delivery-status":
            payload = part.get_payload()
            if isinstance(payload, list):
                for dsn_part in payload:
                    text = str(dsn_part)
                    for line in text.splitlines():
                        line_lower = line.lower().strip()
                        if line_lower.startswith("final-recipient:") or line_lower.startswith("original-recipient:"):
                            found = _EMAIL_RE.findall(line)
                            addresses.update(a.lower() for a in found if a.lower() != self_addr)
            elif isinstance(payload, str):
                for line in payload.splitlines():
                    line_lower = line.lower().strip()
                    if line_lower.startswith("final-recipient:") or line_lower.startswith("original-recipient:"):
                        found = _EMAIL_RE.findall(line)
                        addresses.update(a.lower() for a in found if a.lower() != self_addr)

    # Fallback: scan plain-text body for emails if DSN parsing found nothing
    if not addresses:
        for part in msg.walk():
            ct = part.get_content_type()
            if ct in ("text/plain", "text/html"):
                try:
                    body = part.get_payload(decode=True)
                    if body:
                        text = body.decode("utf-8", errors="replace")
                        found = _EMAIL_RE.findall(text)
                        addresses.update(a.lower() for a in found if a.lower() != self_addr)
                except Exception:
                    continue

    return addresses


def _extract_unsub_sender(msg: email.message.Message) -> Optional[str]:
    """Extract the sender address from an unsubscribe reply."""
    self_addr = _sender_address().lower()
    from_header = msg.get("From", "")
    found = _EMAIL_RE.findall(from_header)
    for addr in found:
        if addr.lower() != self_addr:
            return addr.lower()
    return None


def process_inbox(
    *,
    max_messages: int = 200,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Scan Gmail inbox for bounces and unsubscribe replies.

    Returns a stats dict with counts of what was found and processed.
    """
    from ..dynamo.suppression_repo import SuppressionRepo

    sender = _sender_address()
    app_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not app_password:
        log.warning("GMAIL_APP_PASSWORD not set, skipping inbox processing")
        return {"bounces_found": 0, "unsubscribes_found": 0, "already_suppressed": 0, "errors": 0, "dry_run": dry_run, "skipped": "no credentials"}

    repo = SuppressionRepo()
    is_prod = _is_prod()

    bounces_found = 0
    unsubscribes_found = 0
    already_suppressed = 0
    errors = 0

    try:
        imap = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        imap.login(sender, app_password)
        imap.select("INBOX")
    except Exception:
        log.warning("IMAP connection failed", exc_info=True)
        return {"bounces_found": 0, "unsubscribes_found": 0, "already_suppressed": 0, "errors": 1, "dry_run": dry_run}

    try:
        # --- Process bounces ---
        bounce_ids = _search_unseen(imap, 'FROM "mailer-daemon"', max_messages)
        for msg_id in bounce_ids:
            try:
                msg = _fetch_message(imap, msg_id)
                if msg is None:
                    continue
                addrs = _extract_bounce_addresses(msg)
                for addr in addrs:
                    bounces_found += 1
                    if not dry_run:
                        added = repo.add_suppression(addr, "bounce")
                        if not added:
                            already_suppressed += 1
                if addrs and is_prod and not dry_run:
                    imap.store(msg_id, "+FLAGS", "\\Seen")
            except Exception:
                errors += 1
                log.warning("Error processing bounce message", exc_info=True)

        # --- Process unsubscribes ---
        unsub_ids = _search_unseen(imap, 'SUBJECT "Unsubscribe"', max_messages)
        for msg_id in unsub_ids:
            try:
                msg = _fetch_message(imap, msg_id)
                if msg is None:
                    continue
                addr = _extract_unsub_sender(msg)
                if addr:
                    unsubscribes_found += 1
                    if not dry_run:
                        added = repo.add_suppression(addr, "unsubscribe")
                        if not added:
                            already_suppressed += 1
                    if is_prod and not dry_run:
                        imap.store(msg_id, "+FLAGS", "\\Seen")
            except Exception:
                errors += 1
                log.warning("Error processing unsubscribe message", exc_info=True)
    finally:
        try:
            imap.close()
            imap.logout()
        except Exception:
            pass

    return {
        "bounces_found": bounces_found,
        "unsubscribes_found": unsubscribes_found,
        "already_suppressed": already_suppressed,
        "errors": errors,
        "dry_run": dry_run,
    }


def _search_unseen(imap: imaplib.IMAP4_SSL, criteria: str, max_messages: int) -> list:
    """Search for UNSEEN messages matching criteria, return up to max_messages IDs."""
    try:
        status, data = imap.search(None, "UNSEEN", criteria)
        if status != "OK" or not data or not data[0]:
            return []
        ids = data[0].split()
        return ids[:max_messages]
    except Exception:
        log.warning("IMAP search failed", exc_info=True)
        return []


def _fetch_message(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> Optional[email.message.Message]:
    """Fetch and parse a single message by ID."""
    try:
        status, data = imap.fetch(msg_id, "(BODY.PEEK[])")
        if status != "OK" or not data or not data[0]:
            return None
        raw = data[0][1]
        if isinstance(raw, bytes):
            return email.message_from_bytes(raw, policy=email.policy.default)
        return None
    except Exception:
        log.warning("IMAP fetch failed for message %s", msg_id, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Citation-distance validation responses
# ---------------------------------------------------------------------------

_REVIEW_ID_RE = re.compile(r"Review\s*ID:\s*(review-\S+)", re.IGNORECASE)
_DECISION_RE = re.compile(
    r"(APPROVE|REJECT)\s+(ALL|[A-Za-z0-9_]+)(?:\s+USE\s+APA)?",
    re.IGNORECASE,
)
# Separate pattern to detect USE APA suffix
_USE_APA_RE = re.compile(
    r"(APPROVE)\s+(ALL|[A-Za-z0-9_]+)\s+USE\s+APA",
    re.IGNORECASE,
)


def _parse_validation_body(body: str) -> tuple[Optional[str], list[tuple[str, str, bool]]]:
    """Extract review_id and list of (decision, ref_id_or_ALL, use_apa) from reply text."""
    review_id_match = _REVIEW_ID_RE.search(body)
    review_id = review_id_match.group(1) if review_id_match else None

    # Collect USE APA refs
    use_apa_targets: set[str] = set()
    for m in _USE_APA_RE.finditer(body):
        use_apa_targets.add(m.group(2).upper())

    decisions = [
        (m.group(1).lower(), m.group(2), m.group(2).upper() in use_apa_targets)
        for m in _DECISION_RE.finditer(body)
    ]
    return review_id, decisions


def _resolve_osf_id_from_review_id(review_id: str) -> Optional[str]:
    """Extract the osf_id encoded in a review ID (review-{osf_id}-{hex})."""
    parts = review_id.split("-")
    # Format: review-{osf_id}-{hex8}
    if len(parts) >= 3 and parts[0] == "review":
        return "-".join(parts[1:-1])
    return None


def process_validation_responses(
    *,
    max_messages: int = 50,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Scan inbox for citation-distance review replies and apply decisions.

    Looks for replies to "Citation Distance Review" emails. Parses APPROVE/REJECT
    decisions and updates reference validation status + clears the preprint pending flag.
    """
    from ..dynamo.preprints_repo import PreprintsRepo

    sender = _sender_address()
    app_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not app_password:
        log.warning("GMAIL_APP_PASSWORD not set, skipping validation response processing")
        return {"responses_processed": 0, "decisions_applied": 0, "errors": 0, "dry_run": dry_run, "skipped": "no credentials"}

    repo = PreprintsRepo()
    is_prod = _is_prod()

    responses_processed = 0
    decisions_applied = 0
    errors = 0

    try:
        imap = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        imap.login(sender, app_password)
        # Gmail labels appear as IMAP folders
        status, _ = imap.select("FLoRA-Validation")
        if status != "OK":
            log.warning("Could not select FLoRA-Validation label, falling back to INBOX subject search")
            imap.select("INBOX")
            use_label = False
        else:
            use_label = True
    except Exception:
        log.warning("IMAP connection failed for validation responses", exc_info=True)
        return {"responses_processed": 0, "decisions_applied": 0, "errors": 1, "dry_run": dry_run}

    try:
        if use_label:
            # Only process unread messages; mark as read after processing
            msg_ids = _search_unseen(imap, "ALL", max_messages)
        else:
            msg_ids = _search_unseen(imap, 'SUBJECT "Citation Distance Review"', max_messages)
        for msg_id in msg_ids:
            try:
                msg = _fetch_message(imap, msg_id)
                if msg is None:
                    continue

                # Skip outgoing review emails (sent from our address)
                from_header = msg.get("From", "")
                if sender.lower() in from_header.lower():
                    if not dry_run:
                        imap.store(msg_id, "+FLAGS", "\\Seen")
                    continue

                # Extract plain-text body
                body = ""
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode("utf-8", errors="replace")
                            break
                if not body:
                    # Fall back to HTML stripped of tags
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            payload = part.get_payload(decode=True)
                            if payload:
                                body = re.sub(r"<[^>]+>", " ", payload.decode("utf-8", errors="replace"))
                                break
                if not body:
                    continue

                review_id, decisions = _parse_validation_body(body)
                if not review_id or not decisions:
                    log.info("Skipping message — no review ID or decisions found")
                    continue

                osf_id = _resolve_osf_id_from_review_id(review_id)
                if not osf_id:
                    log.warning("Could not extract osf_id from review_id %s", review_id)
                    errors += 1
                    continue

                # Verify this review_id matches the stored one
                preprint = repo.t_preprints.get_item(
                    Key={"osf_id": osf_id},
                    ProjectionExpression="osf_id, citation_validation_review_id",
                ).get("Item")
                if not preprint:
                    log.warning("Preprint %s not found for review %s", osf_id, review_id)
                    errors += 1
                    continue
                stored_review_id = preprint.get("citation_validation_review_id")
                if stored_review_id and stored_review_id != review_id:
                    log.warning("Review ID mismatch for %s: expected %s, got %s",
                                osf_id, stored_review_id, review_id)
                    errors += 1
                    continue

                # Resolve "ALL" to actual ref IDs
                refs_for_preprint = None
                expanded_decisions: list[tuple[str, str, bool]] = []
                for decision, ref_id_or_all, use_apa in decisions:
                    if ref_id_or_all.upper() == "ALL":
                        if refs_for_preprint is None:
                            refs_for_preprint = _get_pending_refs(repo, osf_id)
                        for rid in refs_for_preprint:
                            expanded_decisions.append((decision, rid, use_apa))
                    else:
                        expanded_decisions.append((decision, ref_id_or_all, use_apa))

                if not dry_run:
                    for decision, ref_id, use_apa in expanded_decisions:
                        status = "approved" if decision == "approve" else "rejected"
                        try:
                            repo.update_reference_validation_decision(
                                osf_id, ref_id, decision=status,
                            )
                            if use_apa and status == "approved":
                                _replace_raw_with_apa(repo, osf_id, ref_id)
                            decisions_applied += 1
                        except Exception:
                            log.warning("Failed to apply decision %s for %s/%s",
                                        status, osf_id, ref_id, exc_info=True)
                            errors += 1

                    # Clear the preprint-level pending flag
                    try:
                        repo.set_citation_validation_pending(osf_id, pending=False)
                    except Exception:
                        log.warning("Failed to clear pending flag for %s", osf_id, exc_info=True)
                        errors += 1
                else:
                    decisions_applied += len(expanded_decisions)

                responses_processed += 1
                log.info("Processed validation response",
                         extra={"osf_id": osf_id, "review_id": review_id,
                                "decisions": len(expanded_decisions)})

                if not dry_run:
                    imap.store(msg_id, "+FLAGS", "\\Seen")

            except Exception:
                errors += 1
                log.warning("Error processing validation response", exc_info=True)
    finally:
        try:
            imap.close()
            imap.logout()
        except Exception:
            pass

    return {
        "responses_processed": responses_processed,
        "decisions_applied": decisions_applied,
        "errors": errors,
        "dry_run": dry_run,
    }


def _replace_raw_with_apa(repo, osf_id: str, ref_id: str) -> None:
    """Overwrite raw_citation with citation_apa_resolved so the author email uses the clean APA version."""
    import datetime as dt
    try:
        item = repo.t_refs.get_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            ProjectionExpression="citation_apa_resolved",
        ).get("Item")
        apa = (item or {}).get("citation_apa_resolved")
        if not apa:
            log.info("No APA citation to substitute for %s/%s", osf_id, ref_id)
            return
        now = dt.datetime.utcnow().isoformat()
        repo.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression="SET raw_citation=:apa, raw_citation_source=:src, updated_at=:t",
            ExpressionAttributeValues={":apa": apa, ":src": "apa_override", ":t": now},
        )
        log.info("Replaced raw_citation with APA for %s/%s", osf_id, ref_id)
    except Exception:
        log.warning("Failed to replace raw_citation with APA for %s/%s", osf_id, ref_id, exc_info=True)


def _get_pending_refs(repo, osf_id: str) -> list[str]:
    """Return ref_ids with citation_validation_status == 'pending_review' for a preprint."""
    ref_ids = []
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": "osf_id = :oid",
            "FilterExpression": "citation_validation_status = :s",
            "ExpressionAttributeValues": {":oid": osf_id, ":s": "pending_review"},
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_refs.query(**kwargs)
        for item in resp.get("Items", []):
            rid = item.get("ref_id")
            if rid:
                ref_ids.append(rid)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return ref_ids
