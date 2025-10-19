import time
import datetime
import os
import requests
from requests.exceptions import ReadTimeout, Timeout, ConnectionError
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Optional, Iterable, List, Dict
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.environ.get("OSF_API_TOKEN")
HEADERS = {'Authorization': f'Bearer {API_TOKEN}'} if API_TOKEN else {}
OSF_API = "https://api.osf.io/v2"
MAX_PAGE = 100
DEFAULT_BATCH = 1000

# 🔧 Create a single resilient Session with retries & backoff
def _build_session() -> requests.Session:
    s = requests.Session()
    # Helpful User-Agent (some APIs rate-limit anonymous UA)
    s.headers.update({"User-Agent": "osf-sync/1.0 (+https://example.org)"})
    if HEADERS:
        s.headers.update(HEADERS)

    retry = Retry(
        total=6,                 # total tries
        connect=6,
        read=6,
        backoff_factor=1.0,      # 1s, 2s, 4s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = _build_session()

def get_subject_id_by_text(subject_text: str) -> Optional[str]:
    r = SESSION.get(
        f"{OSF_API}/taxonomies/",
        params={"filter[text]": subject_text},
        timeout=(10, 60),  # (connect, read)
    )
    r.raise_for_status()
    items = r.json().get("data", [])
    return items[0]["id"] if items else None

def _fetch_page(url: str, params: Optional[dict]) -> dict:
    """
    GET with retry/backoff on 429/5xx handled by Session, plus explicit
    retries for timeouts/connection errors.
    """
    backoff = 2.0
    attempts = 0
    while True:
        attempts += 1
        try:
            resp = SESSION.get(url, params=params, timeout=(10, 120))  # longer read
            resp.raise_for_status()
            return resp.json()
        except (ReadTimeout, Timeout, ConnectionError) as e:
            # exponential backoff, cap at ~1 minute
            sleep_s = min(backoff, 60)
            print(f"[warn] transient network error ({e}). retrying in {sleep_s}s (attempt {attempts})")
            time.sleep(sleep_s)
            backoff *= 2
            continue

def iter_preprints_batches(
    since_date: str,
    subject_text: Optional[str] = None,
    only_published: bool = True,
    batch_size: int = DEFAULT_BATCH,
    sort: str = "date_published"
) -> Iterable[List[Dict]]:
    datetime.date.fromisoformat(since_date)
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

    url = f"{OSF_API}/preprints/"
    batch: List[Dict] = []
    while url:
        data = _fetch_page(url, params if url.endswith("/preprints/") else None)
        items = data.get("data", [])
        batch.extend(items)

        if len(batch) >= batch_size:
            yield batch[:batch_size]
            batch = batch[batch_size:]

        url = data.get("links", {}).get("next")

        # be nice to API
        time.sleep(0.3)

    if batch:
        yield batch

def iter_preprints_range(
    start_date: str,
    until_date: Optional[str] = None,          # <-- if None, we use today's date
    subject_text: Optional[str] = None,
    only_published: bool = True,
    batch_size: int = DEFAULT_BATCH,
    sort: str = "date_published"               # oldest → newest
) -> Iterable[List[Dict]]:
    """
    Stream OSF preprints between start_date and until_date (inclusive) in batches.

    Dates must be 'YYYY-MM-DD'. If until_date is None, uses today's UTC date.
    """
    # validate & normalize dates
    datetime.date.fromisoformat(start_date)
    if until_date is None:
        until_date = datetime.datetime.utcnow().date().isoformat()
    else:
        datetime.date.fromisoformat(until_date)

    # base filters
    params = {
        "filter[date_published][gte]": start_date,
        "filter[date_published][lte]": until_date,   # upper bound
        "page[size]": MAX_PAGE,
        "sort": sort,
    }
    if only_published:
        params["filter[is_published]"] = "true"

    # optional subject filter
    if subject_text:
        sid = get_subject_id_by_text(subject_text)
        if sid:
            params["filter[subjects]"] = sid

    url = f"{OSF_API}/preprints/"
    batch: List[Dict] = []
    while url:
        data = _fetch_page(url, params if url.endswith("/preprints/") else None)
        items = data.get("data", [])
        batch.extend(items)

        if len(batch) >= batch_size:
            yield batch[:batch_size]
            batch = batch[batch_size:]

        url = data.get("links", {}).get("next")
        time.sleep(0.3)  # polite to API

    if batch:
        yield batch