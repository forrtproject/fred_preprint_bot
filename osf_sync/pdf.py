from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, Tuple
from requests.exceptions import RequestException, Timeout, ConnectionError
from sqlalchemy import text

from .db import engine
from .iter_preprints import SESSION, OSF_API  # reuse resilient session

def _safe_path(root: str, osf_id: str) -> Path:
    p = Path(root) / osf_id
    p.mkdir(parents=True, exist_ok=True)
    return p

def _get_file_json_via_href(href: str) -> dict:
    r = SESSION.get(href, timeout=(10, 120))
    r.raise_for_status()
    return r.json()

def _get_file_json_via_id(file_id: str) -> dict:
    r = SESSION.get(f"{OSF_API}/files/{file_id}/", timeout=(10, 120))
    r.raise_for_status()
    return r.json()

def resolve_download_url_from_preprint_raw(raw: dict) -> Optional[str]:
    """
    Preferred: follow relationships.primary_file.links.related.href
    Fallback: use relationships.primary_file.data.id -> /files/{id}/
    Returns the direct file download URL (data.links.download) or None.
    """
    rel = (raw.get("relationships") or {}).get("primary_file") or {}
    # 1) preferred via links.related.href
    href = (((rel.get("links") or {}).get("related")) or {}).get("href")
    if href:
        j = _get_file_json_via_href(href)
        return ((j.get("data") or {}).get("links") or {}).get("download")

    # 2) fallback via file id
    fid = ((rel.get("data") or {}).get("id"))
    if fid:
        j = _get_file_json_via_id(fid)
        return ((j.get("data") or {}).get("links") or {}).get("download")

    return None

def download_pdf_via_raw(osf_id: str, raw: dict, dest_root: str = "/data/preprints") -> Tuple[bool, Optional[str]]:
    """
    Resolves the download URL from the preprint 'raw' JSON, downloads to:
      {dest_root}/{osf_id}/file.pdf
    Returns (ok, local_path).
    """
    url = resolve_download_url_from_preprint_raw(raw)
    if not url:
        return (False, None)

    folder = _safe_path(dest_root, osf_id)
    out_path = folder / "file.pdf"
    tmp_path = folder / ".file.tmp"

    try:
        with SESSION.get(url, stream=True, timeout=(10, 300)) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        fh.write(chunk)
        tmp_path.replace(out_path)
        return (True, str(out_path))
    except (RequestException, Timeout, ConnectionError):
        return (False, None)

def mark_downloaded(osf_id: str, local_path: Optional[str], ok: bool):
    sql = text("""
        UPDATE preprints
        SET
            pdf_downloaded = :ok,
            pdf_downloaded_at = CASE WHEN :ok THEN now() ELSE pdf_downloaded_at END,
            pdf_path = CASE WHEN :ok THEN :path ELSE pdf_path END,
            updated_at = now()
        WHERE osf_id = :id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"ok": ok, "path": local_path, "id": osf_id})