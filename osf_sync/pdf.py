from __future__ import annotations
import logging
import os
import re
import shutil
import subprocess
import tempfile
import threading
from functools import lru_cache
from pathlib import Path
from typing import Optional, Tuple

from pypdf import PdfReader
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError
from .dynamo.preprints_repo import PreprintsRepo
from .iter_preprints import SESSION, OSF_API  # reuse resilient session

ACCEPT_PDF = {"application/pdf"}
ACCEPT_OFFICE = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # .docx
    "application/msword",  # .doc
    "application/vnd.oasis.opendocument.text",  # .odt
}
OFFICE_EXTENSIONS = (".docx", ".doc", ".odt")
OFFICE_MIME_TO_EXTENSION = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".doc",
    "application/vnd.oasis.opendocument.text": ".odt",
}
OFFICE_CONVERT_RETRIES = int(os.environ.get("OFFICE_CONVERT_RETRIES", "3"))
SOFFICE_TIMEOUT_SECONDS = int(os.environ.get("SOFFICE_TIMEOUT_SECONDS", "180"))
MIN_PDF_PAGES = int(os.environ.get("MIN_PDF_PAGES", "5"))
MIN_PDF_WORDS = int(os.environ.get("MIN_PDF_WORDS", "1000"))
WORD_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)
_OFFICE_CONVERSION_LOCK = threading.Lock()
logger = logging.getLogger(__name__)

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

def _looks_office(name: Optional[str]) -> bool:
    if not name:
        return False
    lowered = name.lower()
    return any(lowered.endswith(ext) for ext in OFFICE_EXTENSIONS)


def _office_extension(name: Optional[str], ctype: Optional[str]) -> str:
    lowered_name = (name or "").lower()
    for ext in OFFICE_EXTENSIONS:
        if lowered_name.endswith(ext):
            return ext
    mapped = OFFICE_MIME_TO_EXTENSION.get((ctype or "").lower())
    return mapped or ".docx"


@lru_cache(maxsize=1)
def _office_converter_binary() -> Optional[str]:
    configured = os.environ.get("SOFFICE_BIN")
    if configured:
        return configured
    return shutil.which("soffice") or shutil.which("libreoffice")

def _download_to(path: Path, url: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with SESSION.get(url, stream=True, timeout=(10, 300)) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    fh.write(chunk)
    tmp.replace(path)


def _pdf_length_status(path: Path) -> str:
    try:
        doc = PdfReader(str(path))
        page_count = len(doc.pages)
    except Exception:
        return "unreadable"
    if page_count < MIN_PDF_PAGES:
        return "too_short"

    word_count = 0
    for page in doc.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            # Encrypted / malformed font streams may prevent text extraction;
            # fall back to page-count-only heuristic.
            return "ok" if page_count >= MIN_PDF_PAGES else "too_short"
        word_count += len(WORD_RE.findall(text))
        if word_count >= MIN_PDF_WORDS:
            break
    return "ok" if word_count >= MIN_PDF_WORDS else "too_short"

def _convert_office_to_pdf(in_file: Path, out_pdf: Path) -> bool:
    soffice_bin = _office_converter_binary()
    if not soffice_bin:
        logger.warning("LibreOffice binary not found (expected 'soffice' or 'libreoffice')")
        return False

    attempts = max(1, OFFICE_CONVERT_RETRIES)
    candidate = out_pdf.parent / (in_file.stem + ".pdf")

    with _OFFICE_CONVERSION_LOCK:
        for attempt in range(1, attempts + 1):
            profile_dir = Path(tempfile.mkdtemp(prefix="flora_soffice_profile_"))
            profile_uri = profile_dir.resolve().as_uri()
            cmd = [
                soffice_bin,
                "--headless",
                "--nologo",
                "--nolockcheck",
                "--nodefault",
                "--nofirststartwizard",
                "--norestore",
                f"-env:UserInstallation={profile_uri}",
                "--convert-to",
                "pdf:writer_pdf_Export",
                "--outdir",
                str(out_pdf.parent),
                str(in_file),
            ]
            try:
                if candidate.exists():
                    candidate.unlink()
                if out_pdf.exists():
                    out_pdf.unlink()
                subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=SOFFICE_TIMEOUT_SECONDS,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
                logger.warning(
                    "Office conversion attempt failed",
                    extra={
                        "input": str(in_file),
                        "attempt": attempt,
                        "max_attempts": attempts,
                    },
                )
            finally:
                shutil.rmtree(profile_dir, ignore_errors=True)

            if candidate.exists():
                candidate.replace(out_pdf)
                return True

    return False

def mark_downloaded(osf_id: str, local_path: Optional[str], ok: bool):
    repo = PreprintsRepo()
    repo.mark_pdf(osf_id, ok=ok, path=local_path)

def delete_preprint(osf_id: str):
    repo = PreprintsRepo()
    repo.delete_preprint(osf_id)

def ensure_pdf_available_or_delete(
    osf_id: str,
    provider_id: str,
    raw: dict,
    dest_root: str
) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Enforce file-type policy & provider-based foldering.
      PDF: save to {root}/{provider}/{osf_id}/file.pdf
      Office docs (DOCX/DOC/ODT): download → convert → save pdf to same folder
      Other: delete row
    Returns (kind, path|None, reason|None) where kind in {"pdf","<office_ext>->pdf","deleted"}.
    """
    try:
        url, ctype, name = resolve_primary_file_info_from_raw(raw)
    except HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code in (404, 410):
            logger.info("Primary file gone on OSF [%s] status=%s", osf_id, status_code)
            delete_preprint(osf_id)
            return "deleted", None, "primary_file_gone"
        raise

    if not url:
        delete_preprint(osf_id)
        return "deleted", None, "missing_primary_file"

    folder = _safe_dir(dest_root, provider_id, osf_id)
    pdf_path = folder / "file.pdf"

    is_pdf = (ctype in ACCEPT_PDF) or (ctype is None and _looks_pdf(name))
    is_office = (ctype in ACCEPT_OFFICE) or (ctype is None and _looks_office(name))

    if is_pdf:
        _download_to(pdf_path, url)
        length_status = _pdf_length_status(pdf_path)
        if length_status == "unreadable":
            logger.warning("PDF unreadable by pypdf (encrypted/corrupt), passing to GROBID [%s]", osf_id)
            return "pdf", str(pdf_path), None
        if length_status == "too_short":
            delete_preprint(osf_id)
            try:
                pdf_path.unlink()
            except Exception:
                pass
            return "deleted", None, "pdf_below_minimum_length"
        return "pdf", str(pdf_path), None

    if is_office:
        office_ext = _office_extension(name, ctype)
        office_path = folder / f"file{office_ext}"
        _download_to(office_path, url)
        ok = _convert_office_to_pdf(office_path, pdf_path)
        try:
            if office_path.exists():
                office_path.unlink()
        except Exception:
            pass
        if ok:
            length_status = _pdf_length_status(pdf_path)
            if length_status == "unreadable":
                logger.warning("Converted PDF unreadable by pypdf, passing to GROBID [%s]", osf_id)
                return f"{office_ext[1:]}->pdf", str(pdf_path), None
            if length_status == "too_short":
                delete_preprint(osf_id)
                try:
                    pdf_path.unlink()
                except Exception:
                    pass
                return "deleted", None, "pdf_below_minimum_length"
            return f"{office_ext[1:]}->pdf", str(pdf_path), None
        delete_preprint(osf_id)
        try:
            if pdf_path.exists():
                pdf_path.unlink()
        except Exception:
            pass
        if office_ext == ".docx":
            return "deleted", None, "docx_to_pdf_conversion_failed"
        return "deleted", None, "office_to_pdf_conversion_failed"

    delete_preprint(osf_id)
    return "deleted", None, "unsupported_file_format"


def ensure_pdf_available_or_skip(
    osf_id: str,
    provider_id: str,
    raw: dict,
    dest_root: str,
) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Non-destructive variant used by late/optional stages (e.g., author extraction).
    Unsupported/missing files are skipped and the preprint row is never deleted.

    Returns (kind, path|None, reason|None) where kind in {"pdf","<office_ext>->pdf","skipped"}.
    """
    try:
        url, ctype, name = resolve_primary_file_info_from_raw(raw)
    except HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code in (404, 410):
            return "skipped", None, "primary_file_gone"
        raise

    if not url:
        return "skipped", None, "missing_primary_file"

    folder = _safe_dir(dest_root, provider_id, osf_id)
    pdf_path = folder / "file.pdf"

    is_pdf = (ctype in ACCEPT_PDF) or (ctype is None and _looks_pdf(name))
    is_office = (ctype in ACCEPT_OFFICE) or (ctype is None and _looks_office(name))

    if is_pdf:
        _download_to(pdf_path, url)
        length_status = _pdf_length_status(pdf_path)
        if length_status == "unreadable":
            return "skipped", None, "pdf_length_check_unreadable"
        if length_status == "too_short":
            try:
                pdf_path.unlink()
            except Exception:
                pass
            return "skipped", None, "pdf_below_minimum_length"
        return "pdf", str(pdf_path), None

    if is_office:
        office_ext = _office_extension(name, ctype)
        office_path = folder / f"file{office_ext}"
        _download_to(office_path, url)
        ok = _convert_office_to_pdf(office_path, pdf_path)
        try:
            if office_path.exists():
                office_path.unlink()
        except Exception:
            pass
        if ok:
            length_status = _pdf_length_status(pdf_path)
            if length_status == "unreadable":
                return "skipped", None, "pdf_length_check_unreadable"
            if length_status == "too_short":
                try:
                    pdf_path.unlink()
                except Exception:
                    pass
                return "skipped", None, "pdf_below_minimum_length"
            return f"{office_ext[1:]}->pdf", str(pdf_path), None
        try:
            if pdf_path.exists():
                pdf_path.unlink()
        except Exception:
            pass
        if office_ext == ".docx":
            return "skipped", None, "docx_to_pdf_conversion_failed"
        return "skipped", None, "office_to_pdf_conversion_failed"

    return "skipped", None, "unsupported_file_format"
