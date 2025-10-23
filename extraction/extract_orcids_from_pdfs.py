from get_orcid_from_pdf import get_orcid_from_pdf
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

# Function that crawls all of preprints and mines ORCiD from them.
# Warning: It runs for a long time. 
# Outputs into a json file called "orcids_from_pdf.json"

root_path = path.abspath("../data/preprints") # The default if the function is called from it's current position.

def get_orcids_from_all_pdfs(root_path=path.abspath("../data/preprints")):
    file_name = "file.pdf" # This is a convension held by the preprint bot

    structure_that_holds_author_orcids = {}

    for source in Sources:
        source_path = path.join(root_path, source.value)
        nested_folders = listdir(source_path)
        for folder in nested_folders:
            curr_path = path.join(source_path, folder, file_name)
            orcids_from_paper = get_orcid_from_pdf(curr_path)
            if not orcids_from_paper:
                structure_that_holds_author_orcids[folder] = "false" 
            else:
                structure_that_holds_author_orcids[folder] = orcids_from_paper
                

    with open('orcids_from_pdf.json', 'w') as fp:
        json.dump(structure_that_holds_author_orcids, fp)

get_orcids_from_all_pdfs()