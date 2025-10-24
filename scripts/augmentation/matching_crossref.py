import json
import csv
import requests
from time import sleep, time
from thefuzz import fuzz

# =============================================================================
# SETTINGS
# =============================================================================
# Define file paths, Crossref endpoint, and matching parameters.

INPUT_FILE = "data/preprints_with_references.json"
OUTPUT_FILE = "data/first_preprint_references_with_doi_crossref.json"
CSV_REPORT = "data/crossref_comparison_report.csv"
UNMATCHED_JSON = "data/first_preprint_references_without_doi_crossref.json"

CROSSREF_URL = "https://api.crossref.org/works"
MAILTO = "cruzersoulthrender@gmail.com"
SLEEP_SECONDS = 0.7  # pause between queries (polite use)
YEAR_TOLERANCE = 1   # enforce ±1 year difference
# =============================================================================


def query_crossref(entry):
    """
    Query Crossref by title and year, within a ±1 year window.
    Crossref will prioritize but not strictly enforce the date filter.
    """
    params = {
        "query.title": entry.get("title", ""),
        "rows": 8,
        "mailto": MAILTO
    }

    year = entry.get("year")
    if isinstance(year, int):
        params["filter"] = f"from-pub-date:{year-1}-01-01,until-pub-date:{year+1}-12-31"

    try:
        r = requests.get(CROSSREF_URL, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("message", {}).get("items", [])
    except Exception as e:
        print(f"⚠️  Query failed for '{entry.get('title','')[:60]}': {e}")
        return []


def best_crossref_match(entry):
    """
    Identify the best Crossref match for a reference using fuzzy matching
    on title, author, and journal. Enforces a strict ±YEAR_TOLERANCE filter.
    """
    items = query_crossref(entry)
    if not items:
        return None

    input_title = (entry.get("title") or "").lower()
    input_authors = [a.lower() for a in entry.get("authors", [])]
    input_journal = (entry.get("journal") or "").lower()
    input_year = entry.get("year")

    best_item, best_score, scores = None, 0, {}

    for item in items:
        # --- Extract publication year and enforce tolerance ---
        cross_year = item.get("issued", {}).get("date-parts", [[None]])[0][0]
        if input_year and cross_year:
            try:
                if abs(int(cross_year) - int(input_year)) > YEAR_TOLERANCE:
                    continue  # skip if outside allowed year difference
            except Exception:
                pass

        # --- Fuzzy match title, authors, and journal ---
        cross_title = (item.get("title", [""])[0] or "").lower()
        title_score = fuzz.token_set_ratio(input_title, cross_title)

        cross_authors = [
            f"{a.get('given','')} {a.get('family','')}".strip().lower()
            for a in item.get("author", [])
        ] if "author" in item else []
        author_score = fuzz.token_set_ratio(" ".join(input_authors), " ".join(cross_authors))

        cross_journal = (item.get("container-title", [""])[0] or "").lower()
        journal_score = fuzz.token_set_ratio(input_journal, cross_journal) if input_journal else 0

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

    # Reject weak title matches
    if not best_item or scores.get("title", 0) < 80:
        return None

    # Assign confidence levels
    conf = (
        "high" if scores["combined"] >= 85
        else "medium" if scores["combined"] >= 75
        else "low"
    )

    return {
        "doi": best_item.get("DOI"),
        "title_crossref": best_item.get("title", [None])[0],
        "year_crossref": best_item.get("issued", {}).get("date-parts", [[None]])[0][0],
        "journal_crossref": best_item.get("container-title", [None])[0],
        "authors_crossref": best_item.get("author", []),
        "scores": scores,
        "confidence": conf
    }


def main():
    """Main process: load data, query Crossref, enrich references, and save results."""
    start_time = time()

    # --- Load input JSON ---
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    first_preprint = data[0]
    refs = first_preprint.get("references", [])

    print("------------------------------------------------------------")
    print(f"📘 Processing {len(refs)} references from the first preprint:")
    print(f"Title: {first_preprint.get('title','[Untitled]')}")
    print("------------------------------------------------------------\n")

    total_refs = len(refs)
    refs_with_doi_before = sum(1 for r in refs if r.get("doi"))
    new_dois_found = 0
    query_times = []
    conf_stats = {"high": 0, "medium": 0, "low": 0}
    csv_rows = []
    unmatched_refs = []  # <-- collect refs that couldn't be matched / no DOI added

    # =============================================================================
    # SEARCH CROSSREF FOR MISSING DOIs
    # =============================================================================
    for ref in refs:
        if ref.get("doi"):
            continue  # skip already-identified DOIs

        title = (ref.get("title") or "").strip()
        if not title:
            continue

        t0 = time()
        match = best_crossref_match(ref)
        elapsed = time() - t0
        query_times.append(elapsed)

        if match:
            # Update reference metadata
            ref["doi"] = match["doi"]
            ref["doi_confidence"] = match["confidence"]
            ref["doi_match_status"] = "added"
            ref["match_scores"] = match["scores"]
            new_dois_found += 1
            conf_stats[match["confidence"]] += 1

            print(f"🆕  [{match['confidence'].upper()}] {title[:60]} → {match['doi']} ({elapsed:.2f}s)")

            csv_rows.append({
                "title_input": title,
                "title_crossref": match["title_crossref"],
                "doi_crossref": match["doi"],
                "year_input": ref.get("year"),
                "year_crossref": match["year_crossref"],
                "journal_input": ref.get("journal"),
                "journal_crossref": match["journal_crossref"],
                "authors_input": "; ".join(ref.get("authors", [])),
                "authors_crossref": "; ".join(
                    [f"{a.get('given','')} {a.get('family','')}".strip()
                     for a in match.get("authors_crossref", [])]
                ),
                "title_score": match["scores"]["title"],
                "author_score": match["scores"]["author"],
                "journal_score": match["scores"]["journal"],
                "combined_score": match["scores"]["combined"],
                "confidence": match["confidence"]
            })
        else:
            print(f"❌  No match for: {title[:80]} ({elapsed:.2f}s)")
            # Mark and collect as unmatched (keep original fields for later review)
            ref.setdefault("doi_match_status", "not_found")
            unmatched_refs.append(ref)

        sleep(SLEEP_SECONDS)

    # =============================================================================
    # SUMMARY STATISTICS
    # =============================================================================
    elapsed_total = time() - start_time
    avg_query_time = sum(query_times) / len(query_times) if query_times else 0
    m, s = divmod(elapsed_total, 60)
    refs_with_doi_after = sum(1 for r in refs if r.get("doi"))

    print("\n============================================================")
    print(f"Total references processed: {total_refs}")
    print(f"DOIs before: {refs_with_doi_before}")
    print(f"New DOIs added: {new_dois_found}")
    print(f"Final total DOIs: {refs_with_doi_after}")
    print("------------------------------------------------------------")
    print(f"Confidence breakdown: High={conf_stats['high']}, Medium={conf_stats['medium']}, Low={conf_stats['low']}")
    print(f"⏱  Total runtime: {m:.0f} min {s:.1f} sec")
    print(f"📊  Avg metadata query time: {avg_query_time:.2f}s")
    print("============================================================\n")

    # =============================================================================
    # SAVE UPDATED DATA
    # =============================================================================
    data[0] = first_preprint
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾  JSON output saved to: {OUTPUT_FILE}")

    if csv_rows:
        with open(CSV_REPORT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"📑  Comparison report saved to: {CSV_REPORT}")

    # =============================================================================
    # SAVE UNMATCHED REFERENCES (NO DOI ADDED)
    # =============================================================================
    with open(UNMATCHED_JSON, "w", encoding="utf-8") as f:
        json.dump(unmatched_refs, f, indent=2, ensure_ascii=False)
    print(f"🚫  Unmatched references (no DOI) saved to: {UNMATCHED_JSON}  —  Count: {len(unmatched_refs)}")


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    main()
