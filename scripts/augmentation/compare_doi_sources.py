import json
import csv
from thefuzz import fuzz, process

# =============================================================================
# SETTINGS
# =============================================================================
CROSSREF_FILE = r"C:\Users\paspe\fred_preprint_bot\data\first_preprint_references_with_doi_crossref.json"
OPENALEX_FILE = r"C:\Users\paspe\fred_preprint_bot\data\openalex_doi_matched_preprints_with_references.json"
OUTPUT_CSV = r"C:\Users\paspe\fred_preprint_bot\data\doi_comparison_fuzzy_titles.csv"

TITLE_MATCH_THRESHOLD = 85  # minimum fuzzy score (0–100) for titles to be considered the same
# =============================================================================


# -----------------------------------------------------------------------------
# Helper: Load and normalize JSON
# -----------------------------------------------------------------------------
# Ensures we always have a list of preprints, even if the JSON file contains a single one.
def load_json(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = [data]
    return data


# -----------------------------------------------------------------------------
# Helper: Build reference dictionary
# -----------------------------------------------------------------------------
# Returns {normalized_title: DOI} for all references in the (first) preprint.
def build_reference_map(data):
    if not data:
        return {}

    preprint = data[0]  # use only the first preprint
    refs = preprint.get("references", [])
    ref_map = {}

    for ref in refs:
        if not isinstance(ref, dict):
            continue
        title = (ref.get("title") or "").strip().lower()
        doi = ref.get("doi")
        if title:
            ref_map[title] = doi

    return ref_map


# -----------------------------------------------------------------------------
# Helper: Fuzzy title matching
# -----------------------------------------------------------------------------
# Given a title and a list of candidate titles, find the closest match.
def best_title_match(title, candidate_titles):
    if not title or not candidate_titles:
        return None, 0
    match, score = process.extractOne(title, candidate_titles, scorer=fuzz.token_set_ratio)
    return match, score


# -----------------------------------------------------------------------------
# MAIN LOGIC
# -----------------------------------------------------------------------------
def main():
    # --- Load data ---
    crossref_data = load_json(CROSSREF_FILE)
    openalex_data = load_json(OPENALEX_FILE)

    # --- Build title→DOI maps for both sources ---
    crossref_map = build_reference_map(crossref_data)
    openalex_map = build_reference_map(openalex_data)

    print("============================================================")
    print("📘 DOI Comparison between Crossref and OpenAlex (Fuzzy titles)")
    print("============================================================")
    print(f"Crossref references: {len(crossref_map)}")
    print(f"OpenAlex references: {len(openalex_map)}")
    print("------------------------------------------------------------")

    # --- Initialize counters ---
    matched = mismatched = only_crossref = only_openalex = no_match = 0
    comparison_rows = []

    crossref_titles = list(crossref_map.keys())
    openalex_titles = list(openalex_map.keys())

    # --- Compare references by fuzzy title matching ---
    for title_cr in crossref_titles:
        doi_cr = crossref_map[title_cr]
        best_match, score = best_title_match(title_cr, openalex_titles)

        if best_match and score >= TITLE_MATCH_THRESHOLD:
            doi_oa = openalex_map.get(best_match)
            if doi_cr and doi_oa:
                if doi_cr.lower() == doi_oa.lower():
                    matched += 1
                    status = "match"
                else:
                    mismatched += 1
                    status = "mismatch"
            elif doi_cr and not doi_oa:
                only_crossref += 1
                status = "only_crossref"
            elif doi_oa and not doi_cr:
                only_openalex += 1
                status = "only_openalex"
            else:
                no_match += 1
                status = "no_doi"
        else:
            only_crossref += 1
            status = "no_title_match"
            doi_oa = None
            score = 0

        comparison_rows.append({
            "title_crossref": title_cr[:120],
            "title_openalex_match": best_match[:120] if best_match else None,
            "similarity_score": score,
            "doi_crossref": doi_cr,
            "doi_openalex": openalex_map.get(best_match) if best_match else None,
            "status": status
        })

    # --- Calculate totals ---
    total_refs = len(crossref_map)
    crossref_with_doi = sum(1 for d in crossref_map.values() if d)
    openalex_with_doi = sum(1 for d in openalex_map.values() if d)
    total_with_doi_in_either = matched + mismatched + only_crossref + only_openalex
    agreement_rate = (matched / total_with_doi_in_either * 100) if total_with_doi_in_either else 0

    # --- Summary output ---
    print(f"Matched DOIs (same): {matched}")
    print(f"Mismatched DOIs: {mismatched}")
    print(f"Only in Crossref: {only_crossref}")
    print(f"Only in OpenAlex: {only_openalex}")
    print(f"No title match: {no_match}")
    print("------------------------------------------------------------")
    print(f"Agreement rate (matched / with DOI in either): {agreement_rate:.2f}%")
    print("============================================================")

    # --- Save detailed comparison to CSV ---
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "title_crossref",
                "title_openalex_match",
                "similarity_score",
                "doi_crossref",
                "doi_openalex",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(comparison_rows)

    print(f"💾 Detailed comparison saved to: {OUTPUT_CSV}")


# -----------------------------------------------------------------------------
# Script entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
