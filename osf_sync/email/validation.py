from __future__ import annotations

import logging
import re
from typing import Optional, Tuple

from email_validator import validate_email, EmailNotValidError

logger = logging.getLogger(__name__)


def _try_validate(email: str) -> Optional[str]:
    """Return normalized email if deliverable, else None."""
    try:
        result = validate_email(email, check_deliverability=True)
        return result.normalized
    except EmailNotValidError:
        return None


def repair_email_tld(email: str) -> str:
    """Strip trailing non-TLD segments from an email domain.

    PDF text extraction sometimes concatenates annotation labels onto
    email addresses, e.g. ``user@uni-graz.at.Link`` or
    ``user@ugent.be.Highlights``.  This function checks deliverability
    (DNS/MX) of the full domain and, if it fails, strips dot-segments
    from the right until a deliverable domain is found.

    When a dot-segment contains concatenated junk (e.g. ``ukDrChris``),
    tries truncating at the first uppercase letter to recover multi-part
    TLDs like ``.ac.uk``.

    If no repair is possible the original string is returned unchanged.
    """
    if "@" not in email:
        return email
    # Fast path: already deliverable
    if _try_validate(email):
        return email
    local, _, domain = email.rpartition("@")
    parts = domain.split(".")
    # Try stripping from the right, keeping at least 2 segments (host.tld)
    for end in range(len(parts) - 1, 1, -1):
        candidate_domain = ".".join(parts[:end])
        normalized = _try_validate(local + "@" + candidate_domain)
        if normalized:
            logger.info("Repaired email TLD: %s -> %s", email, normalized)
            return normalized
        # The last segment may have junk concatenated (e.g. "ukDrChrisB"
        # or "uk15Dr").  Try truncating at the first non-lowercase
        # character after position 0.
        last_seg = parts[end - 1]
        m = re.search(r"[^a-z]", last_seg[1:])
        if m:
            truncated = last_seg[: m.start() + 1]
            if len(truncated) >= 2:
                candidate_domain = ".".join(parts[: end - 1] + [truncated])
                normalized = _try_validate(local + "@" + candidate_domain)
                if normalized:
                    logger.info(
                        "Repaired email TLD: %s -> %s", email, normalized
                    )
                    return normalized
    return email


def validate_recipient(email: str) -> Tuple[bool, Optional[str]]:
    """Validate an email address for syntax and MX deliverability.

    Returns (True, None) on success, or (False, error_message) on failure.
    """
    try:
        result = validate_email(email, check_deliverability=True)
        # Use the normalized form
        _ = result.normalized
        return True, None
    except EmailNotValidError as e:
        return False, str(e)
