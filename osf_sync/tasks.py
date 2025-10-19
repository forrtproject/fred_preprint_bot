# osf_sync/tasks.py

from __future__ import annotations
import os

import datetime as dt
from typing import Optional, Iterable, Dict

from requests.exceptions import RequestException
from sqlalchemy import text

from .celery_app import app
from .db import engine, init_db
from .upsert import upsert_batch
from .iter_preprints import iter_preprints_batches, iter_preprints_range
from celery import chain
from .pdf import mark_downloaded, download_pdf_via_raw
from .grobid import process_pdf_to_tei, mark_tei


PDF_DEST_ROOT = os.environ.get("PDF_DEST_ROOT", "/data/preprints")


SOURCE_KEY_ALL = "osf:all"


# -------------------------------
# Helpers: cursor state in Postgres
# -------------------------------

def _get_cursor(source_key: str) -> Optional[dt.datetime]:
    """
    Returns the last_seen_published (timestamptz) for a given source key,
    or None if not set yet.
    """
    sql = text("SELECT last_seen_published FROM sync_state WHERE source_key = :k")
    with engine.begin() as conn:
        row = conn.execute(sql, {"k": source_key}).one_or_none()
        return row[0] if row else None


def _set_cursor(source_key: str, last_seen: dt.datetime) -> None:
    """
    Upserts the cursor, keeping the greatest of the existing and incoming timestamps.
    """
    sql = text(
        """
        INSERT INTO sync_state (source_key, last_seen_published, last_run_at)
        VALUES (:k, :v, now())
        ON CONFLICT (source_key) DO UPDATE SET
            last_seen_published = GREATEST(sync_state.last_seen_published, EXCLUDED.last_seen_published),
            last_run_at = now();
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"k": source_key, "v": last_seen})


def _parse_iso_dt(value: Optional[str]) -> Optional[dt.datetime]:
    """
    Parses OSF ISO timestamps like '2025-10-14T18:58:51.320919' or with 'Z'.
    Returns an aware UTC datetime when possible.
    """
    if not value:
        return None
    try:
        # fromisoformat supports offset; replace Z with +00:00 if present
        v = value.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(v)
        # Make timezone-aware in UTC if naive (unlikely from OSF, but safe)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        else:
            d = d.astimezone(dt.timezone.utc)
        return d
    except Exception:
        return None


# -------------------------------
# Public tasks
# -------------------------------

@app.task
def init_schema() -> str:
    """
    Idempotent schema creator. Safe to call any time (e.g., at container boot).
    """
    init_db()
    return "OK"


@app.task(
    bind=True,
    autoretry_for=(RequestException,),  # retry on transient network issues
    retry_backoff=30,                   # 30s, 60s, 120s, ...
    retry_jitter=True,
    retry_kwargs={"max_retries": 5},
)
def sync_from_osf(self, subject_text: Optional[str] = None, batch_size: int = 1000) -> Dict[str, str | int | None]:
    """
    Daily incremental sync:

    - Uses a per-source cursor (by subject) on date_published to fetch only new/changed items.
    - Upserts by osf_id (no duplicates).
    - Updates only if incoming row is newer (date_modified or version).
    - Advances the cursor to the max date_published seen in this run.

    Args:
        subject_text: Optional OSF taxonomy text filter (e.g., "Psychology").
        batch_size:   How many items to upsert per DB batch (default 1000).
    Returns:
        Dict with 'upserted' count and 'cursor' (ISO string) after the run.
    """
    # Ensure tables exist (no-op if already created)
    init_db()

    # Build a stable source key for cursoring
    source_key = f"osf:{subject_text}" if subject_text else SOURCE_KEY_ALL

    # Load existing cursor; if none, start from a recent window (7 days)
    since_dt = _get_cursor(source_key)
    if since_dt is None:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)

    # The OSF API filter expects a DATE; we convert to YYYY-MM-DD
    since_iso_date = since_dt.astimezone(dt.timezone.utc).date().isoformat()

    total_upserted = 0
    max_published_seen: Optional[dt.datetime] = None

    # Stream batches from oldest->newest so the cursor only moves forward
    for batch in iter_preprints_batches(
        since_date=since_iso_date,
        subject_text=subject_text,
        batch_size=batch_size,
        sort="date_published",
    ):
        # Track max published in this batch
        for obj in batch:
            pub = _parse_iso_dt((obj.get("attributes") or {}).get("date_published"))
            if pub and (max_published_seen is None or pub > max_published_seen):
                max_published_seen = pub

        # Upsert the batch
        total_upserted += upsert_batch(batch)

    # Advance cursor to newest published we observed, if any
    if max_published_seen:
        _set_cursor(source_key, max_published_seen)
        cursor_out = max_published_seen.isoformat()
    else:
        # No new items found; keep the existing cursor
        cursor_out = since_dt.isoformat()

    return {"upserted": total_upserted, "cursor": cursor_out}

@app.task(bind=True)
def sync_from_date_to_now(self, start_date: str, subject_text: str | None = None, batch_size: int = 1000):
    """
    One-off task: fetch all preprints from a given start_date until today,
    then upsert into PostgreSQL.
    Does NOT modify the sync_state cursor; purely ad-hoc window ingestion.
    """
    init_db()

    total = 0
    for batch in iter_preprints_range(
        start_date=start_date,
        until_date=None,              # defaults to 'today'
        subject_text=subject_text,
        batch_size=batch_size,
    ):
        total += upsert_batch(batch)

    return {"upserted": total, "from": start_date, "to": "now"}

PDF_DEST_ROOT = os.environ.get("PDF_DEST_ROOT", "/data/preprints")  # optional override

@app.task(bind=True, queue="pdf", autoretry_for=(RequestException,), retry_backoff=30, retry_jitter=True, retry_kwargs={"max_retries": 3})
def download_single_pdf(self, osf_id: str):
    """
    Download one preprint's primary PDF via relationships.primary_file.links.related.href
    (fallback to file id) and update DB flags.
    """
    # Fetch raw JSON for this preprint and ensure it still needs downloading
    sql = text("""
        SELECT osf_id, raw
        FROM preprints
        WHERE osf_id = :id
          AND is_published IS TRUE
          AND COALESCE(pdf_downloaded, false) = false
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"id": osf_id}).mappings().one_or_none()

    if not row:
        return {"osf_id": osf_id, "skipped": True}

    ok, path = download_pdf_via_raw(osf_id=row["osf_id"], raw=row["raw"], dest_root=PDF_DEST_ROOT)
    mark_downloaded(osf_id=row["osf_id"], local_path=path, ok=ok)
    return {"osf_id": osf_id, "downloaded": ok, "path": path}

@app.task(bind=True)
def enqueue_pdf_downloads(self, limit: int = 100):
    sql = text("""
        SELECT osf_id
        FROM preprints
        WHERE is_published IS TRUE
          AND (raw -> 'relationships' -> 'primary_file') IS NOT NULL
          AND COALESCE(pdf_downloaded, false) = false
        ORDER BY date_published ASC NULLS LAST
        LIMIT :lim
    """)
    with engine.begin() as conn:
        ids = [r[0] for r in conn.execute(sql, {"lim": limit}).fetchall()]

    if not ids:
        return {"queued": 0}

    # IMPORTANT: use .si for immutable signatures (no prior result passed)
    sigs = [download_single_pdf.si(i).set(queue="pdf") for i in ids]
    chain(*sigs).apply_async()
    return {"queued": len(ids)}

@app.task(bind=True, queue="grobid", autoretry_for=(RequestException,), retry_backoff=30, retry_jitter=True, retry_kwargs={"max_retries": 3})
def grobid_single(self, osf_id: str):
    """
    Process one PDF with GROBID -> TEI XML.
    """
    # Ensure this still needs TEI and the PDF exists
    sql = text("""
        SELECT osf_id, pdf_downloaded, tei_generated
        FROM preprints
        WHERE osf_id = :id
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"id": osf_id}).mappings().one_or_none()

    if not row:
        return {"osf_id": osf_id, "skipped": "not found"}
    if not row["pdf_downloaded"]:
        return {"osf_id": osf_id, "skipped": "pdf not downloaded"}
    if row.get("tei_generated"):
        return {"osf_id": osf_id, "skipped": "already processed"}

    ok, tei_path, err = process_pdf_to_tei(osf_id)
    mark_tei(osf_id, ok=ok, tei_path=tei_path if ok else None)
    return {"osf_id": osf_id, "ok": ok, "tei_path": tei_path, "error": err}

@app.task(bind=True)
def enqueue_grobid(self, limit: int = 50):
    """
    Queue a strictly sequential chain of GROBID jobs (one after the other).
    """
    sql = text("""
        SELECT osf_id
        FROM preprints
        WHERE COALESCE(pdf_downloaded, false) = true
          AND COALESCE(tei_generated, false) = false
        ORDER BY pdf_downloaded_at ASC NULLS LAST
        LIMIT :lim
    """)
    with engine.begin() as conn:
        ids = [r[0] for r in conn.execute(sql, {"lim": limit}).fetchall()]

    if not ids:
        return {"queued": 0}

    sigs = [grobid_single.si(i).set(queue="grobid") for i in ids]  # use .si to avoid passing previous result
    chain(*sigs).apply_async()
    return {"queued": len(ids)}