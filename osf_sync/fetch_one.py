from __future__ import annotations
import re
from typing import Optional, Dict, Any
from sqlalchemy import text
from .db import engine
from .iter_preprints import SESSION, OSF_API
from .upsert import upsert_batch

_DOI_RE = re.compile(r"^https?://doi.org/(.+)$", re.I)

def _normalize_doi(doi_or_url: str) -> str:
    m = _DOI_RE.match(doi_or_url.strip())
    return m.group(1) if m else doi_or_url.strip()

def fetch_preprint_by_id(osf_id: str) -> Optional[Dict[str, Any]]:
    r = SESSION.get(f"{OSF_API}/preprints/{osf_id}/", timeout=(10, 60))
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("data")

def fetch_preprint_by_doi(doi_or_url: str) -> Optional[Dict[str, Any]]:
    doi = _normalize_doi(doi_or_url)
    # OSF supports filter[doi]; returns a list
    r = SESSION.get(f"{OSF_API}/preprints/", params={"filter[doi]": doi, "page[size]": 1}, timeout=(10, 60))
    r.raise_for_status()
    items = r.json().get("data", [])
    return items[0] if items else None

def upsert_one_preprint(data: Dict[str, Any]) -> int:
    # Reuse existing batch upsert (expects list of records from OSF API)
    return upsert_batch([data])

def exists_in_db(osf_id: str) -> bool:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT 1 FROM preprints WHERE osf_id=:id LIMIT 1"), {"id": osf_id}).first()
        return bool(row)
