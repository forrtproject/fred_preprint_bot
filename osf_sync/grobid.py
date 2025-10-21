from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, Tuple

from sqlalchemy import text
from requests.exceptions import RequestException, Timeout, ConnectionError

from .db import engine
from .iter_preprints import SESSION

GROBID_URL = os.environ.get("GROBID_URL", "http://grobid:8070")
DATA_ROOT   = os.environ.get("PDF_DEST_ROOT", "/data/preprints")

def _pdf_path(provider_id: str, osf_id: str) -> Optional[Path]:
    # New structure
    p = Path(DATA_ROOT) / provider_id / osf_id / "file.pdf"
    if p.exists():
        return p
    # Legacy fallback (no provider)
    p_old = Path(DATA_ROOT) / osf_id / "file.pdf"
    if p_old.exists():
        # Optionally, move into new structure for cleanliness
        new_dir = Path(DATA_ROOT) / provider_id / osf_id
        new_dir.mkdir(parents=True, exist_ok=True)
        new_dst = new_dir / "file.pdf"
        try:
            p_old.replace(new_dst)
            return new_dst
        except Exception:
            return p_old
    return None

def _tei_output_path(provider_id: str, osf_id: str) -> Path:
    d = Path(DATA_ROOT) / provider_id / osf_id
    d.mkdir(parents=True, exist_ok=True)
    return d / "tei.xml"

def process_pdf_to_tei(provider_id: str, osf_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
    pdf = _pdf_path(provider_id, osf_id)
    if not pdf:
        return (False, None, "PDF missing")

    url = f"{GROBID_URL.rstrip('/')}/api/processFulltextDocument"
    files = {"input": ("file.pdf", open(pdf, "rb"), "application/pdf")}
    params = {"consolidateHeader": "1"}
    try:
        with SESSION.post(url, files=files, data=params, timeout=(10, 120)) as r:
            r.raise_for_status()
            tei_path = _tei_output_path(provider_id, osf_id)
            tmp = tei_path.with_suffix(".xml.tmp")
            tmp.write_text(r.text, encoding="utf-8")
            tmp.replace(tei_path)
            return (True, str(tei_path), None)
    except (RequestException, Timeout, ConnectionError) as e:
        return (False, None, str(e))

def mark_tei(osf_id: str, ok: bool, tei_path: Optional[str]):
    sql = text("""
        UPDATE preprints
        SET
            tei_generated = :ok,
            tei_generated_at = CASE WHEN :ok THEN now() ELSE tei_generated_at END,
            tei_path = CASE WHEN :ok THEN :path ELSE tei_path END,
            updated_at = now()
        WHERE osf_id = :id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"ok": ok, "path": tei_path, "id": osf_id})