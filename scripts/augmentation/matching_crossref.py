import json
import csv
import requests
from time import sleep, time
from thefuzz import fuzz

# =============================================================================
# SETTINGS
# =============================================================================
INPUT_FILE = r"C:\Users\paspe\fred_preprint_bot\data\preprints_with_references.json"
OUTPUT_FILE = r"C:\Users\paspe\fred_preprint_bot\data\first_15_preprints_with_doi_crossref.json"
CSV_REPORT = r"C:\Users\paspe\fred_preprint_bot\data\crossref_comparison_report.csv"
UNMATCHED_JSON = r"C:\Users\paspe\fred_preprint_bot\data\unmatched_references_crossref.json"
UNMATCHED_CSV = r"C:\Users\paspe\fred_preprint_bot\data\unmatched_references_crossref.csv"

CROSSREF_URL = "https://api.crossref.org/works"
MAILTO = "pasquale.pellegrini@bih-charite.de"
SLEEP_SECONDS = 0.7
YEAR_TOLERANCE = 1
# =============================================================================


def query_crossref(entry):
    """Query Crossref by title and year (±1 year tolerance)."""
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
    """Find the best Crossref match using fuzzy title, author, and journal similarity."""
    items = query_crossref(entry)
    if not items:
        return None

    input_title = (entry.get("title") or "").lower()
    input_authors = [a.lower() for a in entry.get("authors", [])]
    input_journal = (entry.get("journal") or "").lower()
    input_year = entry.get("year")

    best_item, best_score, scores = None, 0, {}

    for item in items:
        cross_year = item.get("issued", {}).get("date-parts", [[None]])[0][0]
        if input_year and cross_year:
            try:
                if abs(int(cross_year) - int(input_year)) > YEAR_TOLERANCE:
                    continue
            except Exception:
                pass

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

    if not best_item or scores.get("title", 0) < 80:
        return None

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
    """Process the first 15 preprints and enrich missing DOIs."""
    start_time = time()

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    subset = data[:15]
    processed_preprints = []
    csv_rows = []
    unmatched_refs = []

    print(f"📚 Processing the first {len(subset)} preprints\n")

    for i, preprint in enumerate(subset, start=1):
        refs = preprint.get("references", [])
        print("------------------------------------------------------------")
        print(f"📘 Preprint {i}/{len(subset)} — {len(refs)} references")
        print(f"Title: {preprint.get('title','[Untitled]')}")
        print("------------------------------------------------------------\n")

        total_refs = len(refs)
        refs_with_doi_before = sum(1 for r in refs if r.get("doi"))
        new_dois_found = 0
        conf_stats = {"high": 0, "medium": 0, "low": 0}
        query_times = []

        for ref in refs:
            if ref.get("doi"):
                continue

            title = (ref.get("title") or "").strip()
            if not title:
                continue

            t0 = time()
            match = best_crossref_match(ref)
            elapsed = time() - t0
            query_times.append(elapsed)

            if match:
                ref["doi"] = match["doi"]
                ref["doi_confidence"] = match["confidence"]
                ref["doi_match_status"] = "added"
                ref["match_scores"] = match["scores"]
                new_dois_found += 1
                conf_stats[match["confidence"]] += 1

                print(f"🆕  [{match['confidence'].upper()}] {title[:60]} → {match['doi']} ({elapsed:.2f}s)")

                csv_rows.append({
                    "preprint_index": i,
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
                ref.setdefault("doi_match_status", "not_found")
                unmatched_refs.append({
                    "preprint_index": i,
                    "title": title,
                    "year": ref.get("year"),
                    "journal": ref.get("journal"),
                    "authors": "; ".join(ref.get("authors", []))
                })

            sleep(SLEEP_SECONDS)

        refs_with_doi_after = sum(1 for r in refs if r.get("doi"))
        processed_preprints.append(preprint)

        avg_query_time = sum(query_times) / len(query_times) if query_times else 0
        print("\n------------------------------------------------------------")
        print(f"Preprint {i} summary:")
        print(f"DOIs before: {refs_with_doi_before}")
        print(f"New DOIs added: {new_dois_found}")
        print(f"Final DOIs: {refs_with_doi_after}")
        print(f"Confidence: High={conf_stats['high']} | Medium={conf_stats['medium']} | Low={conf_stats['low']}")
        print(f"Avg query time: {avg_query_time:.2f}s\n")

    elapsed_total = time() - start_time
    m, s = divmod(elapsed_total, 60)
    print("============================================================")
    print(f"✅ Processed {len(subset)} preprints in {m:.0f} min {s:.1f} sec")
    print(f"💾 Total unmatched refs: {len(unmatched_refs)}")
    print("============================================================\n")

    # --- Save JSON with only the first 15 preprints ---
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(processed_preprints, f, indent=2, ensure_ascii=False)
    print(f"💾  JSON output saved to: {OUTPUT_FILE}")

    # --- Save CSV report for matches ---
    if csv_rows:
        with open(CSV_REPORT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"📑  Comparison report saved to: {CSV_REPORT}")

    # --- Save unmatched refs in JSON and CSV ---
    with open(UNMATCHED_JSON, "w", encoding="utf-8") as f:
        json.dump(unmatched_refs, f, indent=2, ensure_ascii=False)

    if unmatched_refs:
        with open(UNMATCHED_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(unmatched_refs[0].keys()))
            writer.writeheader()
            writer.writerows(unmatched_refs)
    print(f"🚫  Unmatched refs saved to: {UNMATCHED_JSON} and {UNMATCHED_CSV}")


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    main()
