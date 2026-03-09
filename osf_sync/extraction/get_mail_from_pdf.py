import re
from pypdf import PdfReader

# Function that attempts to read a text from a PDF and find all email adresses. 
# Input is a path to a specific PDF file, output is a list of all emails found in a specific PDF
# (returns an empty list if no emails are found)


STRICT_EMAIL_RE = re.compile(
    pattern=r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,15}\b",
    flags=re.IGNORECASE | re.MULTILINE | re.UNICODE,
)
LOOSE_EMAIL_RE = re.compile(
    pattern=(
        r"\b[A-Za-z0-9._%+\-]+(?:\s+[A-Za-z0-9._%+\-]+){0,4}\s*@\s*"
        r"[A-Za-z0-9.\-]+(?:\s+[A-Za-z0-9.\-]+){0,4}\s*\.\s*[A-Za-z]{2,15}\b"
    ),
    flags=re.IGNORECASE | re.MULTILINE | re.UNICODE,
)


def _normalize_pdf_text_for_email_extraction(text: str) -> str:
    if not text:
        return ""
    # Remove invisible separators and rejoin hyphenated line wraps.
    txt = text.replace("\u00ad", "").replace("\u200b", "")
    txt = re.sub(r"(?<=\w)-\s*\n\s*(?=\w)", "", txt)
    # De-obfuscate common anti-spam tokens.
    txt = re.sub(r"\s*(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\})\s*", "@", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\s*(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\})\s*", ".", txt, flags=re.IGNORECASE)
    # Repair common spacing artifacts around email separators.
    txt = re.sub(r"\s*@\s*", "@", txt)
    txt = re.sub(r"\s*\.\s*", ".", txt)
    txt = re.sub(r"\s*_\s*", "_", txt)
    txt = re.sub(r"\s*\+\s*", "+", txt)
    txt = re.sub(r"\s*-\s*", "-", txt)
    return txt


def _find_best_local(text: str, domain: str) -> str | None:
    """Find a plausible email local part at the end of *text*.

    Tries each position and returns the longest candidate that is all
    lowercase / digits with short segments.  Uses tighter limits for
    locals without separators (max 8 chars) than for locals with
    separators (max 12 per segment).
    """
    best = None
    for i in range(1, len(text)):
        rest = text[i:]
        if not rest or rest[0].isupper() or len(rest) < 3:
            continue
        if any(c.isupper() for c in rest):
            continue
        if not STRICT_EMAIL_RE.fullmatch(rest + "@" + domain):
            continue
        has_sep = any(c in rest for c in "._")
        plausible = (
            _PLAUSIBLE_LOCAL_SEP_RE.fullmatch(rest)
            if has_sep
            else _PLAUSIBLE_LOCAL_NOSEP_RE.fullmatch(rest)
        )
        if not plausible:
            continue
        if best is None or len(rest) > len(best):
            best = rest
    return best


# With separators: each segment 2–12 chars.
_PLAUSIBLE_LOCAL_SEP_RE = re.compile(
    r"^[a-z][a-z0-9]{1,11}(?:[._-][a-z0-9]{1,12}){1,3}$"
)
# Without separators: max 7 chars total.  Longer no-separator locals
# are ambiguous (could be partial CamelCase word + username).  Phrase-
# stripped results bypass this check so longer usernames still work.
_PLAUSIBLE_LOCAL_NOSEP_RE = re.compile(r"^[a-z][a-z0-9]{1,6}$")


def _repair_common_prefix_noise(email: str) -> str:
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)

    # 1. Dot-prefix: Berlin.cornelius.erfort@... → cornelius.erfort@...
    dot_parts = local.split(".")
    if len(dot_parts) >= 3 and dot_parts[0][:1].isupper() and dot_parts[0][1:].islower():
        candidate = ".".join(dot_parts[1:]) + "@" + domain
        if STRICT_EMAIL_RE.fullmatch(candidate):
            return candidate

    # 2. Name-repetition: when a CamelCase word appears again in lowercase
    #    later in the local part, the email starts at the repeated word.
    #    e.g. AnastasiaRousakianastasia.rousaki → anastasia.rousaki
    for m in re.finditer(r"[A-Z][a-z]{2,}", local):
        word_lower = m.group().lower()
        search_from = m.end()
        rest_after = local[search_from:]
        idx = rest_after.lower().find(word_lower)
        if idx >= 0:
            email_start = search_from + idx
            email_local = local[email_start:]
            if email_local[0].islower() and STRICT_EMAIL_RE.fullmatch(
                email_local + "@" + domain
            ):
                return email_local + "@" + domain

    # 3. Known phrase prefixes (case-insensitive).
    phrase_match = re.search(
        r"correspondenceto|addressedto|contactat|mailto",
        local,
        re.IGNORECASE,
    )
    if phrase_match:
        rest = local[phrase_match.end():]
        if rest:
            # Try rest directly if it starts lowercase.
            if rest[0].islower() and STRICT_EMAIL_RE.fullmatch(
                rest + "@" + domain
            ):
                return rest + "@" + domain
            # Rest may have an additional CamelCase name prefix.
            if rest[0].isupper():
                best = _find_best_local(rest, domain)
                if best:
                    return best + "@" + domain

    # 4. General CamelCase prefix before lowercase email local.
    #    Require ≥2 uppercase letters in the local and a long local (>12).
    if sum(1 for c in local if c.isupper()) >= 2 and len(local) > 12:
        best = _find_best_local(local, domain)
        if best and len(best) < len(local):
            prefix = local[: len(local) - len(best)]
            if prefix.isalpha():
                return best + "@" + domain

    # 5. Single CamelCase word + uppercase-starting rest with separator:
    # SingaporeNikita_Rane@... → Nikita_Rane@...
    match = re.match(r"^([A-Z][a-z]{3,})([A-Z][A-Za-z0-9._%+\-]+)$", local)
    if match and any(ch in match.group(2) for ch in "._-"):
        candidate = match.group(2) + "@" + domain
        if STRICT_EMAIL_RE.fullmatch(candidate):
            return candidate

    return email


def _extract_emails_from_text(text: str) -> list[str]:
    if not text:
        return []
    out = []
    for match in STRICT_EMAIL_RE.findall(text):
        out.append(match)
    # Fallback: allow embedded whitespace and repair inside-match spaces.
    for raw in LOOSE_EMAIL_RE.findall(text):
        compact = re.sub(r"\s+", "", raw)
        if STRICT_EMAIL_RE.fullmatch(compact):
            out.append(compact)
    # Stable de-duplication while preserving original case in first occurrence.
    seen = set()
    unique = []
    for item in out:
        repaired = _repair_common_prefix_noise(item)
        key = repaired.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(repaired)
    return unique


def get_mail_from_pdf(path):
    doc = PdfReader(path)
    article_emails = []

    for page in doc.pages:
        page_text = page.extract_text() or ""
        normalized_text = _normalize_pdf_text_for_email_extraction(page_text)
        page_matches = _extract_emails_from_text(normalized_text)
        article_emails.extend(page_matches)

    return article_emails
