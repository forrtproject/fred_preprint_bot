from __future__ import annotations

import logging
from typing import Optional, Tuple

from email_validator import validate_email, EmailNotValidError

logger = logging.getLogger(__name__)


def repair_email_tld(email: str) -> str:
    """Strip trailing non-TLD segments from an email domain.

    PDF text extraction sometimes concatenates annotation labels onto
    email addresses, e.g. ``user@uni-graz.at.Link`` or
    ``user@ugent.be.Highlights``.  This function checks deliverability
    (DNS/MX) of the full domain and, if it fails, strips dot-segments
    from the right until a deliverable domain is found.

    If no repair is possible the original string is returned unchanged.
    """
    if "@" not in email:
        return email
    # Fast path: already deliverable
    try:
        validate_email(email, check_deliverability=True)
        return email
    except EmailNotValidError:
        pass
    local, _, domain = email.rpartition("@")
    parts = domain.split(".")
    # Try stripping from the right, keeping at least 2 segments (host.tld)
    for end in range(len(parts) - 1, 1, -1):
        candidate = local + "@" + ".".join(parts[:end])
        try:
            result = validate_email(candidate, check_deliverability=True)
            logger.info("Repaired email TLD: %s -> %s", email, result.normalized)
            return result.normalized
        except EmailNotValidError:
            continue
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
