import re
from pypdf import PdfReader

# Function that attempts to read a text from a PDF and find all occurences of Orcid. 
# Input is a path to a specific PDF file, output is a list of all orcids found in a specific PDF
# (returns an empty list if no emails are found)

# Logs saying "ignoring wrong pointing object x y (offset 0) are a byproduct of using pypdf and can be safely ignored"

def get_orcid_from_pdf(path):
    doc = PdfReader(path)
    regex_pattern_no_prefix = re.compile(pattern=r'\b[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}\b', flags=re.MULTILINE | re.UNICODE)

    article_orcids = []
    
    for page in doc.pages:
        page_text = page.extract_text()
        page_matches = re.findall(regex_pattern_no_prefix, page_text)
        article_orcids.extend(page_matches)

    return article_orcids