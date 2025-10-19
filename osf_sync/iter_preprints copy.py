import time
import datetime
import os
import requests
from typing import Optional, Iterable, List, Dict
from dotenv import load_dotenv
import json
import gzip
from contextlib import contextmanager
from typing import Union, IO

# Load API token from .env file
load_dotenv()
try:
    API_TOKEN = os.environ["OSF_API_TOKEN"]
except KeyError:
    print("API key not found. Please set 'OSF_API_TOKEN' in your .env file.")

HEADERS = {'Authorization': f'Bearer {API_TOKEN}'}
OSF_API = "https://api.osf.io/v2"
MAX_PAGE = 100           # OSF limit
DEFAULT_BATCH = 1000     # your desired per-batch size

# Reuse your HEADERS if you already defined them (with Bearer token)
HEADERS = globals().get("HEADERS", {})


def get_subject_id_by_text(subject_text: str) -> Optional[str]:
    """Resolve 'Psychology' -> OSF taxonomy id."""
    r = requests.get(
        f"{OSF_API}/taxonomies/",
        params={"filter[text]": subject_text},
        headers=HEADERS, timeout=30
    )
    r.raise_for_status()
    items = r.json().get("data", [])
    return items[0]["id"] if items else None


def _fetch_page(url: str, params: Optional[dict]) -> dict:
    """GET with simple 429 backoff and sane timeout."""
    backoff = 1.0
    while True:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        if resp.status_code == 429:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        return resp.json()


def iter_preprints_batches(
    since_date: str,
    subject_text: Optional[str] = None,
    only_published: bool = True,
    batch_size: int = DEFAULT_BATCH,
    sort: str = "date_published"  # oldest→newest; use "-date_published" for newest→oldest
) -> Iterable[List[Dict]]:
    """
    Yields lists ("batches") of preprint records, ~batch_size each (e.g., 1000),
    internally fetching OSF pages of size 100 until all results are consumed.
    """
    # validate ISO date
    datetime.date.fromisoformat(since_date)

    # build initial params
    params = {
        "filter[date_published][gte]": since_date,
        "page[size]": MAX_PAGE,
        "sort": sort,
    }
    if only_published:
        params["filter[is_published]"] = "true"

    if subject_text:
        sid = get_subject_id_by_text(subject_text)
        if sid:
            params["filter[subjects]"] = sid
        else:
            print(
                f"[warn] No taxonomy id found for subject '{subject_text}'. Continuing without subject filter.")

    url = f"{OSF_API}/preprints/"
    batch: List[Dict] = []

    while url:
        # only send params on first call (pagination uses absolute next links)
        data = _fetch_page(
            url, params if url.endswith("/preprints/") else None)

        items = data.get("data", [])
        batch.extend(items)

        # If we’ve accumulated a full batch (~1000), yield it and reset
        if len(batch) >= batch_size:
            yield batch[:batch_size]
            batch = batch[batch_size:]

        # follow pagination
        url = data.get("links", {}).get("next")

        # be nice to the API
        time.sleep(0.2)

    # yield any remainder
    if batch:
        yield batch

@contextmanager
def _smart_open(path: str, gzip_level: int = 0) -> IO[bytes]:
    """
    Open a file as binary. If gzip_level > 0, write gzip-compressed output.
    """
    if gzip_level and not path.endswith(".gz"):
        path = path + ".gz"
    fh = gzip.open(path, "wb", compresslevel=gzip_level) if gzip_level else open(path, "wb")
    try:
        yield fh
    finally:
        fh.close()


def export_preprints_to_json(
    since_date: str,
    out_path: str,
    subject_text: Optional[str] = None,
    only_published: bool = True,
    sort: str = "date_published",
    batch_size: int = DEFAULT_BATCH,
    ndjson: bool = True,
    gzip_level: int = 0,
) -> str:
    """
    Stream export of OSF preprints to disk.

    - ndjson=True  -> one JSON object per line
    - ndjson=False -> a single JSON array written incrementally

    Returns the final output file path (with .gz appended if gzip_level > 0).
    """
    # pick final path (may append .gz)
    final_path = out_path + (".gz" if gzip_level and not out_path.endswith(".gz") else "")

    with _smart_open(out_path, gzip_level=gzip_level) as fh:
        # bytes writer convenience
        def write_text(s: str):
            fh.write(s.encode("utf-8"))

        if ndjson:
            # newline-delimited JSON for easy downstream processing
            for i, batch in enumerate(
                iter_preprints_batches(
                    since_date,
                    subject_text=subject_text,
                    only_published=only_published,
                    batch_size=batch_size,
                    sort=sort,
                ),
                start=1,
            ):
                for item in batch:
                    write_text(json.dumps(item, ensure_ascii=False))
                    write_text("\n")
                print(f"[ndjson] wrote batch {i} ({len(batch)} records)")
        else:
            # stream a single big JSON array without holding everything in memory
            write_text("[")
            first = True
            total = 0
            for i, batch in enumerate(
                iter_preprints_batches(
                    since_date,
                    subject_text=subject_text,
                    only_published=only_published,
                    batch_size=batch_size,
                    sort=sort,
                ),
                start=1,
            ):
                for item in batch:
                    if not first:
                        write_text(",")
                    write_text(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
                    first = False
                    total += 1
                print(f"[array] appended batch {i} ({len(batch)} records, total={total})")
            write_text("]")

    return final_path


# --- examples: call these where you currently have example_usage() ---

def example_export():
    # 1) NDJSON (recommended for very large exports); gzip on
    path1 = export_preprints_to_json(
        since_date="2025-07-01",
        out_path="osf_preprints_since_2025-07-01.ndjson",
        subject_text=None,          # or "Psychology"
        only_published=True,
        sort="date_published",      # or "-date_published"
        batch_size=1000,
        ndjson=True,
        gzip_level=5                # 0 for no compression; 1-9 for gzip
    )
    print(f"NDJSON written to: {path1}")

    # 2) Single JSON array (compact), no gzip
    path2 = export_preprints_to_json(
        since_date="2025-07-01",
        out_path="osf_preprints_since_2025-07-01.json",
        subject_text="Psychology",
        only_published=True,
        sort="-date_published",
        batch_size=1000,
        ndjson=False,
        gzip_level=0
    )
    print(f"Array JSON written to: {path2}")


def example_usage():
    # EXAMPLES:
    # 1) 1000-at-a-time from 2025-07-01, any subject
    for i, batch in enumerate(iter_preprints_batches("2025-07-01", batch_size=1000), start=1):
        print(f"Processing batch {i} with {len(batch)} records")
        # ... do your processing/saving here ...

    # 2) Psychology only, newest first, still batching by 1000
    for i, batch in enumerate(
        iter_preprints_batches(
            "2025-07-01", subject_text="Psychology", batch_size=1000, sort="-date_published"),
        start=1
    ):
        print(f"Processing batch {i} with {len(batch)} records")
        # ... process ...


example_export()