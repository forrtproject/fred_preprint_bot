#!/bin/bash

# Run the script to extract preprint information and save required fields in JSON
python scripts/augmentation/extract_preprints_and_references.py --data-dir data/preprints --output data/preprints_with_references.json --verbose

# Look for these on CrossRef
python scripts/augmentation/matching_crossref.py

#Look for whatever was not available on CrossRef, on OpenAlex
python scripts/augmentation/doi_check_openalex.py
