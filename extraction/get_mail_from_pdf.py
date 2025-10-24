import re
from pypdf import PdfReader

# Function that attempts to read a text from a PDF and find all email adresses. 
# Input is a path to a specific PDF file, output is a list of all emails found in a specific PDF
# (returns an empty list if no emails are found)

# Logs saying "ignoring wrong pointing object x y (offset 0) are a byproduct of using pypdf and can be safely ignored"

def get_mail_from_pdf(path):
    doc = PdfReader(path)
    regex_pattern = re.compile(pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', flags=re.IGNORECASE | re.MULTILINE | re.UNICODE)

    # Pattern explained:
    # \b                   Word boundary. Needed for it to work in a long text, makes sure the email is a 1 word. 
    #[A-Za-z0-9._%+-]+     Local part: letters, numbers, dots, underscores, etc.
    #@                     Literal @
    #[A-Za-z0-9.-]+        Domain name: letters, numbers, dots, hyphens. Supports subdomains (like name@faculty.university.tld)
    #\.                    Literal dot (escaped)
    #[A-Za-z]{2,}          Top-level domain. at least 2 letters, maximum not specified
    #\b                    Word boundary (clean end)

    article_emails = []

    for page in doc.pages:
        page_text = page.extract_text()
        page_matches = re.findall(regex_pattern, page_text)
        article_emails.extend(page_matches)

    return article_emails