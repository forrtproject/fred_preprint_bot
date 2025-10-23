# CURRENTLY DOES NOT WORK

import re
from pypdf import PdfReader

def get_emails_from_pdf(path):
    doc = PdfReader(path)
    regex_pattern = re.compile(pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', flags=re.IGNORECASE | re.MULTILINE | re.UNICODE)

    article_emails = []

    for page in doc.pages:
        page_text = page.extract_text()
        page_matches = re.findall(regex_pattern, page_text)
        article_emails.extend(page_matches)

    return article_emails