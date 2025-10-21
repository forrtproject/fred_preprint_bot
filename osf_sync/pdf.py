from __future__ import annotations
import os
import subprocess
from pathlib import Path
from typing import Optional, Tuple

from requests.exceptions import RequestException, Timeout, ConnectionError
from sqlalchemy import text

from .db import engine
from .iter_preprints import SESSION, OSF_API  # reuse resilient session

ACCEPT_PDF = {"application/pdf"}
ACCEPT_DOCX = {"application/vnd.openxmlformats-officedocument.wordprocessingml.document"}

def _safe_dir(root: str, provider_id: str, osf_id: str) -> Path:
    p = Path(root) / provider_id / osf_id
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

def _file_meta_from_json(j: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    data = j.get("data") or {}
    links = data.get("links") or {}
    attrs = data.get("attributes") or {}
    return links.get("download"), attrs.get("content_type"), attrs.get("name")

def resolve_primary_file_info_from_raw(raw: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    rel = (raw.get("relationships") or {}).get("primary_file") or {}
    href = (((rel.get("links") or {}).get("related")) or {}).get("href")
    if href:
        return _file_meta_from_json(_get_file_json_via_href(href))
    fid = ((rel.get("data") or {}).get("id"))
    if fid:
        return _file_meta_from_json(_get_file_json_via_id(fid))
    return None, None, None

def _looks_pdf(name: Optional[str]) -> bool:
    return bool(name and name.lower().endswith(".pdf"))

def _looks_docx(name: Optional[str]) -> bool:
    return bool(name and name.lower().endswith(".docx"))

def _download_to(path: Path, url: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with SESSION.get(url, stream=True, timeout=(10, 300)) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    fh.write(chunk)
    tmp.replace(path)

def _convert_docx_to_pdf(in_docx: Path, out_pdf: Path) -> bool:
    cmd = [
        "soffice", "--headless", "--convert-to", "pdf",
        "--outdir", str(out_pdf.parent), str(in_docx)
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        return False
    candidate = out_pdf.parent / (in_docx.stem + ".pdf")
    if candidate.exists():
        candidate.replace(out_pdf)
        return True
    return False

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

def delete_preprint(osf_id: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM preprints WHERE osf_id = :id"), {"id": osf_id})

def ensure_pdf_available_or_delete(
    osf_id: str,
    provider_id: str,
    raw: dict,
    dest_root: str
) -> Tuple[str, Optional[str]]:
    """
    Enforce file-type policy & provider-based foldering.
      PDF: save to {root}/{provider}/{osf_id}/file.pdf
      DOCX: download → convert → save pdf to same folder
      Other: delete row
    Returns (kind, path|None) where kind in {"pdf","docx->pdf","deleted"}.
    """
    url, ctype, name = resolve_primary_file_info_from_raw(raw)
    if not url:
        delete_preprint(osf_id)
        return "deleted", None

    folder = _safe_dir(dest_root, provider_id, osf_id)
    pdf_path = folder / "file.pdf"

    is_pdf = (ctype in ACCEPT_PDF) or (ctype is None and _looks_pdf(name))
    is_docx = (ctype in ACCEPT_DOCX) or (ctype is None and _looks_docx(name))

    if is_pdf:
        _download_to(pdf_path, url)
        return "pdf", str(pdf_path)

    if is_docx:
        docx_path = folder / "file.docx"
        _download_to(docx_path, url)
        ok = _convert_docx_to_pdf(docx_path, pdf_path)
        try:
            if docx_path.exists():
                docx_path.unlink()
        except Exception:
            pass
        if ok:
            return "docx->pdf", str(pdf_path)
        delete_preprint(osf_id)
        try:
            if pdf_path.exists():
                pdf_path.unlink()
        except Exception:
            pass
        return "deleted", None

    delete_preprint(osf_id)
    return "deleted", None