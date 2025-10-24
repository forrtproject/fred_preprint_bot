from get_mail_from_pdf import get_mail_from_pdf
from os import listdir, path
from enum import Enum
import json

class Sources(Enum):
    EDARXIV = "edarxiv"
    LAWARCHIVE = "lawarchive"
    OSF = "osf"
    PSYARXIV = "psyarxiv"
    SOCARXIV = "socarxiv"
    THESISCOMM = "thesiscommons"

# Function that crawls all of preprints and mines emails from them. If an email is there, this function should find it. 
# Warning: It runs for a long time. 
# Outputs into a json file called "emails_from_pdf.json"

root_path = path.abspath("../data/preprints") # The default if the function is called from it's current position.

def get_mails_from_all_pdfs(root_path=path.abspath("../data/preprints")):
    file_name = "file.pdf" # This is a convension held by the preprint bot

    structure_that_holds_author_emails = {}

    for source in Sources:
        source_path = path.join(root_path, source.value)
        nested_folders = listdir(source_path)
        for folder in nested_folders:
            curr_path = path.join(source_path, folder, file_name)
            emails_from_paper = get_mail_from_pdf(curr_path)
            if not emails_from_paper:
                structure_that_holds_author_emails[folder] = "false" 
            else:
                structure_that_holds_author_emails[folder] = emails_from_paper
                

    with open('emails_from_pdf.json', 'w') as fp:
        json.dump(structure_that_holds_author_emails, fp)