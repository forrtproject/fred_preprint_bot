import json
import requests
from time import sleep
from thefuzz import fuzz

# =============================================================================
# SETTINGS
# =============================================================================
# Input and output file paths
INPUT_FILE = r"C:\Users\paspe\fred_preprint_bot\data\preprints_with_references.json"
OUTPUT_FILE = r"C:\Users\paspe\fred_preprint_bot\data\first_preprint_references_with_doi.json"

# Crossref API settings
CROSSREF_URL = "https://api.crossref.org/works"
MAILTO = "pasquale.pellegrini@bih-charite.de"  # included for polite API identification

# Query behavior
SLEEP_SECONDS = 1  # delay between queries to respect Crossref rate limits
# =============================================================================


# -----------------------------------------------------------------------------
# Function: query_crossref
# -----------------------------------------------------------------------------
# Builds and sends a request to the Crossref API for one reference.
# Allows a ±1 year window for publication year mismatch (online vs print date).
def query_crossref(entry):
    params = {
        "query.title": entry.get("title", ""),
        "rows": 5,
        "mailto": MAILTO
    }

    year = entry.get("year")
    if isinstance(year, int):
        params["filter"] = f"from-pub-date:{year-1}-01-01,until-pub-date:{year+1}-12-31"

    try:
        response = requests.get(CROSSREF_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("message", {}).get("items", [])
    except Exception as e:
        print(f"⚠️  [Warning] Crossref query failed for '{entry.get('title','')[:60]}': {e}")
        return []


# -----------------------------------------------------------------------------
# Function: best_crossref_match
# -----------------------------------------------------------------------------
# Among several returned Crossref items, finds the best match based on
# fuzzy similarity between your metadata (title, authors, journal)
# and the Crossref record.
def best_crossref_match(entry):
    items = query_crossref(entry)
    if not items:
        return None

    # Input fields for comparison
    input_title = (entry.get("title") or "").lower()
    input_authors = [a.lower() for a in entry.get("authors", [])]
    input_journal = (entry.get("journal") or "").lower()

    best_item = None
    best_score = 0
    scores = {}

    # Loop through Crossref candidate items
    for item in items:
        # Compare titles
        cross_title = (item.get("title", [""])[0] or "").lower()
        title_score = fuzz.token_set_ratio(input_title, cross_title)

        # Compare authors
        cross_authors = [
            f"{a.get('given','')} {a.get('family','')}".strip().lower()
            for a in item.get("author", [])
        ] if "author" in item else []
        author_score = fuzz.token_set_ratio(" ".join(input_authors), " ".join(cross_authors))

        # Compare journals
        cross_journal = (item.get("container-title", [""])[0] or "").lower()
        journal_score = fuzz.token_set_ratio(input_journal, cross_journal) if input_journal else 0

        # Weighted combined score
        combined = 0.6 * title_score + 0.3 * author_score + 0.1 * journal_score

        if combined > best_score:
            best_score = combined
            best_item = item
            scores = {
                "title": title_score,
                "author": author_score,
                "journal": journal_score,
                "combined": combined
            }

    # Only accept confident matches
    if best_item and best_score > 70:
        return {
            "doi": best_item.get("DOI"),
            "title_crossref": best_item.get("title", [None])[0],
            "year_crossref": best_item.get("issued", {}).get("date-parts", [[None]])[0][0],
            "journal_crossref": best_item.get("container-title", [None])[0],
            "scores": scores
        }

    return None


# -----------------------------------------------------------------------------
# Function: main
# -----------------------------------------------------------------------------
# Main execution flow:
# 1. Loads your JSON file.
# 2. Takes only the first preprint and its references.
# 3. For each reference, searches Crossref.
# 4. Adds missing DOIs and logs results.
def main():
    # --- Load input file ---
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print("No preprints found in the input file.")
        return

    first_preprint = data[0]
    refs = first_preprint.get("references", [])
    print("------------------------------------------------------------")
    print(f"📘 Processing {len(refs)} references from the first preprint:")
    print(f"Title: {first_preprint.get('title','[Untitled]')}")
    print("------------------------------------------------------------\n")

    # --- Initialize counters ---
    total_refs = len(refs)
    refs_with_doi_before = sum(1 for r in refs if r.get("doi"))
    new_dois_found = 0
    doi_mismatch = 0

    # --- Process each reference ---
    for ref in refs:
        title = (ref.get("title") or "").strip()
        if not title:
            print("⚪  Skipped reference with no title.")
            continue

        print(f"🔍  Searching Crossref for: {title[:80]}")
        match = best_crossref_match(ref)

        if match:
            existing_doi = ref.get("doi")
            doi_cross = match["doi"]

            if existing_doi:
                if existing_doi.lower() != doi_cross.lower():
                    doi_mismatch += 1
                    ref["doi_crossref"] = doi_cross
                    ref["doi_match_status"] = "mismatch"
                    print(f"⚠️   DOI mismatch: existing {existing_doi} vs found {doi_cross}")
                else:
                    ref["doi_match_status"] = "match"
                    print(f"✔️   DOI confirmed: {doi_cross}")
            else:
                ref["doi"] = doi_cross
                ref["doi_match_status"] = "added"
                new_dois_found += 1
                print(f"🆕  Added DOI: {doi_cross}")

            ref["match_scores"] = match["scores"]
            ref["crossref_title"] = match["title_crossref"]
            ref["crossref_year"] = match["year_crossref"]
            ref["crossref_journal"] = match["journal_crossref"]
        else:
            print("❌  No match found.")

        sleep(SLEEP_SECONDS)

    # --- Summary ---
    refs_with_doi_after = sum(1 for r in refs if r.get("doi"))
    print("\n============================================================")
    print(f"Total references processed: {total_refs}")
    print(f"With DOI before: {refs_with_doi_before}")
    print(f"New DOIs added: {new_dois_found}")
    print(f"DOI mismatches: {doi_mismatch}")
    print(f"With DOI after: {refs_with_doi_after}")
    print("============================================================\n")

    # --- Save updated dataset ---
    data[0] = first_preprint
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾  Output saved to: {OUTPUT_FILE}")


# -----------------------------------------------------------------------------
# Script entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
