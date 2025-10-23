import re
from pypdf import PdfReader

# Function that attempts to read a text from a PDF and find all email adresses. 
# Input is a path to a specific PDF file, output is a list of all emails found in a specific PDF
# (returns an empty list if no emails are found)

# Logs saying "ignoring wrong pointing object x y (offset 0) are a byproduct of using pypdf and can be safely ignored"

def get_mail_from_pdf(path):
    doc = PdfReader(path)
    regex_pattern = re.compile(pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', flags=re.IGNORECASE | re.MULTILINE | re.UNICODE)

    article_emails = []

    for page in doc.pages:
        page_text = page.extract_text()
        page_matches = re.findall(regex_pattern, page_text)
        article_emails.extend(page_matches)

    return article_emails