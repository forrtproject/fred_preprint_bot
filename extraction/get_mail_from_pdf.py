# CURRENTLY DOES NOT WORK

import re
from pypdf import PdfReader


def get_emails_from_pdf(path):
    doc = PdfReader(path)
    

    emails = []
    auth_email = r"^\\S+@\\S+\\.\\S+$"


    for page in doc.pages:
        text = page.extract_text()
        # Find all matches of the pattern
        page_matches = re.findall(auth_email, text, re.IGNORECASE)
        
        for match in page_matches:
            emails.append(match)    
    return emails 


get_emails_from_pdf('tester.pdf')





