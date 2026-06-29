"""Scan Gmail inbox via IMAP for bounces and unsubscribe replies.

Detected addresses are stored in the DynamoDB suppression table so they
are excluded from future email sends.
"""
from __future__ import annotations

import email
import email.policy
import html
import imaplib
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from botocore.exceptions import ClientError

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
    r"(APPROVE|REJECT)\s+(REMAINDER|[A-Za-z0-9_]+/[A-Za-z0-9_]+)(?:\s+USE\s+APA)?",
    re.IGNORECASE,
)
# Separate pattern to detect USE APA suffix
_USE_APA_RE = re.compile(
    r"(APPROVE)\s+(REMAINDER|[A-Za-z0-9_]+/[A-Za-z0-9_]+)\s+USE\s+APA",
    re.IGNORECASE,
)


def _parse_validation_body(body: str) -> tuple[Optional[str], list[tuple[str, str, bool]]]:
    """Extract review_id and list of (decision, target, use_apa) from reply text.

    Target is either 'REMAINDER' or '{osf_id}/{ref_id}'.
    """
    review_id_match = _REVIEW_ID_RE.search(body)
    review_id = review_id_match.group(1) if review_id_match else None

    # Collect USE APA targets
    use_apa_targets: set[str] = set()
    for m in _USE_APA_RE.finditer(body):
        use_apa_targets.add(m.group(2).upper())

    decisions = [
        (m.group(1).lower(), m.group(2), m.group(2).upper() in use_apa_targets)
        for m in _DECISION_RE.finditer(body)
    ]
    return review_id, decisions


def process_validation_responses(
    *,
    max_messages: int = 50,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Scan inbox for citation-distance review replies and apply decisions.

    Supports batch review format where targets are '{osf_id}/{ref_id}' or 'REMAINDER'.
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
    batch_member_cache: Dict[str, List[Tuple[str, str]]] = {}

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
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            payload = part.get_payload(decode=True)
                            if payload:
                                body = _html_to_text(payload.decode("utf-8", errors="replace"))
                                break
                if not body:
                    continue

                # Parse only the reviewer's own (top-posted) text, never the quoted
                # original review email — whose instructional lines would otherwise be
                # misread as decisions.
                reply_text = _top_posted_region(body)
                review_id, decisions = _parse_validation_body(reply_text)
                if not review_id or not decisions:
                    log.info("Skipping message — no review ID or decisions found")
                    continue

                # Preprints still linked to this batch via citation_validation_review_id.
                # Re-screening can overwrite/clear that link before a reply arrives, so
                # this scan is frequently empty.  Explicit per-ref targets carry
                # osf_id/ref_id directly and are applied regardless; REMAINDER is scoped
                # from the reply's own Targets list (or the outgoing review email).
                affected_osf_ids = _get_preprints_for_batch_review(repo, review_id)

                decided_pairs: Set[Tuple[str, str]] = set()
                # Only preprints we actually write a decision to are "touched" and thus
                # eligible to have their email gate released — never a stale target.
                touched_osf_ids: Set[str] = set()
                remainder_decision: Optional[Tuple[str, bool]] = None
                applied_here = 0
                resolved = False
                had_failure = False  # transient write/read error → leave unread for retry

                def _apply(osf_id: str, ref_id: str, decision: str, use_apa: bool) -> str:
                    """Record one decision.

                    Returns 'applied' on a real write, 'skipped' for a non-retryable
                    no-op (unknown target), or 'failed' for a retryable error.  Only an
                    'applied' result means the preprint should be treated as touched.
                    """
                    nonlocal decisions_applied, applied_here, errors
                    if dry_run:
                        decisions_applied += 1
                        applied_here += 1
                        return "applied"
                    status_val = "approved" if decision == "approve" else "rejected"
                    try:
                        repo.update_reference_validation_decision(osf_id, ref_id, decision=status_val)
                    except ClientError as exc:
                        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                            # Target reference does not exist (stale/typo) — not retryable.
                            log.warning("Skipping decision for unknown reference %s/%s", osf_id, ref_id)
                            errors += 1
                            return "skipped"
                        log.warning("Failed to apply decision %s for %s/%s",
                                    status_val, osf_id, ref_id, exc_info=True)
                        errors += 1
                        return "failed"
                    except Exception:
                        log.warning("Failed to apply decision %s for %s/%s",
                                    status_val, osf_id, ref_id, exc_info=True)
                        errors += 1
                        return "failed"
                    if use_apa and status_val == "approved":
                        if not _replace_raw_with_apa(repo, osf_id, ref_id):
                            return "failed"  # APA substitution failed — retry next run
                    decisions_applied += 1
                    applied_here += 1
                    return "applied"

                # 1) Explicit per-ref decisions ({osf_id}/{ref_id}) — applied directly,
                #    independent of the (often broken) review_id link.
                for decision, target, use_apa in decisions:
                    if target.upper() == "REMAINDER":
                        remainder_decision = (decision, use_apa)
                        continue
                    parts = target.split("/", 1)
                    if len(parts) != 2:
                        log.warning("Invalid target format: %s", target)
                        errors += 1
                        continue
                    osf_id, ref_id = parts
                    outcome = _apply(osf_id, ref_id, decision, use_apa)
                    if outcome == "applied":
                        touched_osf_ids.add(osf_id)  # only real writes touch the preprint
                    elif outcome == "failed":
                        had_failure = True
                    decided_pairs.add((osf_id, ref_id))
                    resolved = True

                # 2) REMAINDER — every batch ref not individually decided and still
                #    pending_review.  Membership is taken from the reply's own Targets
                #    list first, then the live review_id link, then the outgoing email.
                if remainder_decision:
                    rem_decision, rem_use_apa = remainder_decision
                    embedded = _parse_remainder_targets(reply_text)
                    if embedded:
                        candidate_pairs: List[Tuple[str, str]] = embedded
                        resolved = True
                    elif affected_osf_ids:
                        candidate_pairs = [
                            (oid, rid)
                            for oid in affected_osf_ids
                            for rid in _get_pending_refs(repo, oid)
                        ]
                        resolved = True
                    else:
                        if review_id not in batch_member_cache:
                            batch_member_cache[review_id] = list(
                                _get_batch_members_from_outgoing(imap, review_id, sender)
                            )
                        candidate_pairs = list(batch_member_cache[review_id])
                        if candidate_pairs:
                            resolved = True
                        else:
                            log.warning("Could not determine batch membership for REMAINDER on %s",
                                        review_id)
                    for osf_id, ref_id in candidate_pairs:
                        if (osf_id, ref_id) in decided_pairs:
                            continue
                        pending = _ref_is_pending(repo, osf_id, ref_id)
                        if pending is None:  # read failure — retry later
                            had_failure = True
                            continue
                        if not pending:  # decided elsewhere or never pending
                            continue
                        outcome = _apply(osf_id, ref_id, rem_decision, rem_use_apa)
                        if outcome == "applied":
                            touched_osf_ids.add(osf_id)
                        elif outcome == "failed":
                            had_failure = True
                        decided_pairs.add((osf_id, ref_id))

                if not resolved:
                    # Nothing matched (e.g. a REMAINDER-only reply whose outgoing review
                    # email is no longer available).  Leave unread so a later run can retry.
                    log.warning("No preprints or batch members found for review_id %s", review_id)
                    errors += 1
                    continue

                # Release the preprint-level email gate only once no references remain
                # pending_review — a partial reply must not unlock a preprint that still
                # has unreviewed high-distance citations.  A consistent read ensures the
                # just-written decisions are visible; a failure here leaves the message
                # unread so the gate is retried rather than stranded.
                if not dry_run and not had_failure:
                    for osf_id in touched_osf_ids:
                        try:
                            if not _get_pending_refs(repo, osf_id, consistent=True):
                                repo.set_citation_validation_pending(osf_id, pending=False)
                        except ClientError as exc:
                            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                                # Preprint row does not exist (orphan ref) — nothing to
                                # clear and not retryable; do not create a phantom item.
                                log.warning("Preprint %s missing; skipping gate clear", osf_id)
                                continue
                            log.warning("Failed to clear pending flag for %s", osf_id, exc_info=True)
                            errors += 1
                            had_failure = True
                        except Exception:
                            log.warning("Failed to clear pending flag for %s", osf_id, exc_info=True)
                            errors += 1
                            had_failure = True

                responses_processed += 1
                log.info("Processed batch validation response",
                         extra={"review_id": review_id,
                                "affected_preprints": len(touched_osf_ids),
                                "decisions_applied": applied_here,
                                "had_failure": had_failure})

                # Only mark read when every intended write succeeded; otherwise leave
                # the reply unread so the next run retries it.
                if not dry_run and not had_failure:
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


def _replace_raw_with_apa(repo, osf_id: str, ref_id: str) -> bool:
    """Overwrite raw_citation with citation_apa_resolved for the author email.

    Returns False only on an actual read/write error (so the caller can retry);
    a missing APA citation is a no-op success.
    """
    import datetime as dt
    try:
        item = repo.t_refs.get_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            ProjectionExpression="citation_apa_resolved",
        ).get("Item")
        apa = (item or {}).get("citation_apa_resolved")
        if not apa:
            log.info("No APA citation to substitute for %s/%s", osf_id, ref_id)
            return True
        now = dt.datetime.utcnow().isoformat()
        repo.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression="SET raw_citation=:apa, raw_citation_source=:src, updated_at=:t",
            ExpressionAttributeValues={":apa": apa, ":src": "apa_override", ":t": now},
        )
        log.info("Replaced raw_citation with APA for %s/%s", osf_id, ref_id)
        return True
    except Exception:
        log.warning("Failed to replace raw_citation with APA for %s/%s", osf_id, ref_id, exc_info=True)
        return False


def _get_preprints_for_batch_review(repo, review_id: str) -> list[str]:
    """Return osf_ids of preprints whose citation_validation_review_id matches review_id."""
    osf_ids: list[str] = []
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "FilterExpression": "citation_validation_review_id = :rid",
            "ExpressionAttributeValues": {":rid": review_id},
            "ProjectionExpression": "osf_id",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = repo.t_preprints.scan(**kwargs)
        for item in resp.get("Items", []):
            oid = item.get("osf_id")
            if oid:
                osf_ids.append(oid)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return osf_ids


def _get_pending_refs(repo, osf_id: str, consistent: bool = False) -> list[str]:
    """Return ref_ids with citation_validation_status == 'pending_review' for a preprint.

    Pass consistent=True for a strongly consistent read (used right after writing
    decisions, so the email-gate clear sees the just-written statuses).
    """
    ref_ids = []
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": "osf_id = :oid",
            "FilterExpression": "citation_validation_status = :s",
            "ExpressionAttributeValues": {":oid": osf_id, ":s": "pending_review"},
            "ConsistentRead": consistent,
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


def _ref_is_pending(repo, osf_id: str, ref_id: str) -> Optional[bool]:
    """Whether the reference still has citation_validation_status == 'pending_review'.

    Returns None on a read error so callers can distinguish "not pending" from
    "could not determine" and avoid silently dropping a decision.
    """
    try:
        item = repo.t_refs.get_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            ProjectionExpression="citation_validation_status",
        ).get("Item")
    except Exception:
        log.warning("Failed to read status for %s/%s", osf_id, ref_id, exc_info=True)
        return None
    return bool(item) and item.get("citation_validation_status") == "pending_review"


_QUOTE_MARKERS = (
    re.compile(r"^\s*>"),
    re.compile(r"^\s*On\b.+\bwrote:\s*$", re.IGNORECASE),
    re.compile(r"^\s*-{2,}\s*Original Message\s*-{2,}", re.IGNORECASE),
    re.compile(r"^\s*From:\s", re.IGNORECASE),
    re.compile(r"^\s*_{5,}\s*$"),
    re.compile(r"^\s*Sent from\b", re.IGNORECASE),
)


def _html_to_text(html_body: str) -> str:
    """Flatten an HTML reply to text while preserving line structure.

    Quoted-reply containers are dropped first so the original review email's
    instructional lines cannot be parsed as decisions, and block boundaries become
    newlines so _top_posted_region's line-based quote markers still apply.
    """
    html_body = re.split(r"<blockquote|<div[^>]*gmail_quote", html_body, maxsplit=1, flags=re.IGNORECASE)[0]
    html_body = re.sub(r"(?i)<\s*br\s*/?>", "\n", html_body)
    html_body = re.sub(r"(?i)</\s*(p|div|tr|li|h[1-6]|blockquote)\s*>", "\n", html_body)
    return html.unescape(re.sub(r"<[^>]+>", " ", html_body))


def _top_posted_region(body: str) -> str:
    """Return the reviewer's own text, dropping any quoted original/signature.

    Reviewers top-post their decision above the quoted review email; parsing the
    whole body would re-read the original's instructional lines (e.g. the
    'APPROVE REMAINDER' action text) as if they were decisions.
    """
    kept: list[str] = []
    for line in body.splitlines():
        if any(p.match(line) for p in _QUOTE_MARKERS):
            break
        kept.append(line)
    return "\n".join(kept)


# Matches an explicit "{osf_id}/{ref_id}" target embedded in review text.
_TARGET_PAIR_RE = re.compile(r"\b([A-Za-z0-9]+(?:_v\d+)?)/([A-Za-z0-9_]+)\b")


def _parse_remainder_targets(body: str) -> list[tuple[str, str]]:
    """Extract the batch member list a self-contained REMAINDER reply carries.

    Newer review emails embed a ``Targets: a/b, c/d`` line in the REMAINDER action so
    the reply no longer depends on the server-side review_id link.  Returns an empty
    list for older replies that lack the line.
    """
    pairs: list[tuple[str, str]] = []
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("targets:"):
            for m in _TARGET_PAIR_RE.finditer(stripped[len("targets:"):]):
                pairs.append((m.group(1), m.group(2)))
    return pairs


def _get_batch_members_from_outgoing(imap, review_id: str, sender: str) -> set[tuple[str, str]]:
    """Reconstruct a batch's (osf_id, ref_id) members from its outgoing review email.

    Used as a fallback for older REMAINDER replies that pre-date the embedded
    ``Targets:`` line and whose review_id link has since been cleared.  The original
    outgoing email (still in the label) lists every flagged reference.
    """
    members: set[tuple[str, str]] = set()
    try:
        status, data = imap.search(None, 'BODY "%s"' % review_id)
        if status != "OK" or not data or not data[0]:
            return members
        for mid in data[0].split():
            msg = _fetch_message(imap, mid)
            if msg is None:
                continue
            if sender.lower() not in msg.get("From", "").lower():
                continue  # only the outgoing review email enumerates all members
            body = ""
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="replace")
                        break
            if not body or review_id not in body:
                continue
            for m in re.finditer(
                r"(?:APPROVE|REJECT)\s+([A-Za-z0-9]+(?:_v\d+)?)/([A-Za-z0-9_]+)",
                body, re.IGNORECASE,
            ):
                members.add((m.group(1), m.group(2)))
            if members:
                break
    except Exception:
        log.warning("Failed to reconstruct batch members for %s", review_id, exc_info=True)
    return members
