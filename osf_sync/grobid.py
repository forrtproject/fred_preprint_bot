from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, Tuple

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from sqlalchemy import text

from .db import engine
from .iter_preprints import SESSION  # resilient session with retries, UA, etc.

GROBID_URL = os.environ.get("GROBID_URL", "http://grobid:8070")
DATA_ROOT   = os.environ.get("PDF_DEST_ROOT", "/data/preprints")

def _tei_output_path(osf_id: str) -> Path:
    folder = Path(DATA_ROOT) / osf_id
    folder.mkdir(parents=True, exist_ok=True)
    return folder / "tei.xml"

def _pdf_path(osf_id: str) -> Optional[Path]:
    p = Path(DATA_ROOT) / osf_id / "file.pdf"
    return p if p.exists() else None

def process_pdf_to_tei(osf_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Call GROBID fulltext API with the local PDF, write TEI as {DATA_ROOT}/{osf_id}/tei.xml.
    Returns: (ok, tei_path, error_message)
    """
    pdf = _pdf_path(osf_id)
    if not pdf:
        return (False, None, "PDF missing")

    url = f"{GROBID_URL}/api/processFulltextDocument"
    files = {"input": ("file.pdf", open(pdf, "rb"), "application/pdf")}
    # You can adjust params: consolidatedHeader=1, teiCoordinates=true, etc.
    params = {"consolidateHeader": "1"}
    try:
        with SESSION.post(url, files=files, data=params, timeout=(10, 120)) as r:
            r.raise_for_status()
            # GROBID returns TEI XML in response.text
            tei_path = _tei_output_path(osf_id)
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
