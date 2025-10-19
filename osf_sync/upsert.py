from __future__ import annotations
import json
from typing import Iterable, Dict, List
from sqlalchemy import text
from .db import engine

UPSERT_SQL = text("""
INSERT INTO preprints (
    osf_id, type, title, description, doi,
    date_created, date_modified, date_published,
    is_published, version, is_latest_version, reviews_state,
    tags, subjects, license_record, provider_id, primary_file_id, links, raw,
    pdf_downloaded, pdf_downloaded_at, pdf_path, updated_at
) VALUES (
    :osf_id, :type, :title, :description, :doi,
    :date_created, :date_modified, :date_published,
    :is_published, :version, :is_latest_version, :reviews_state,
    :tags,
    CAST(:subjects_json AS JSONB),
    CAST(:license_record_json AS JSONB),
    :provider_id, :primary_file_id,
    CAST(:links_json AS JSONB),
    CAST(:raw_json AS JSONB),
    COALESCE(:pdf_downloaded, false), :pdf_downloaded_at, :pdf_path, now()
)
ON CONFLICT (osf_id) DO UPDATE SET
    type = EXCLUDED.type,
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    doi = EXCLUDED.doi,
    date_created = EXCLUDED.date_created,
    -- Keep newest timestamps/versions
    date_modified = GREATEST(preprints.date_modified, EXCLUDED.date_modified),
    date_published = COALESCE(EXCLUDED.date_published, preprints.date_published),
    is_published = EXCLUDED.is_published,
    version = GREATEST(preprints.version, EXCLUDED.version),
    is_latest_version = EXCLUDED.is_latest_version,
    reviews_state = EXCLUDED.reviews_state,
    tags = EXCLUDED.tags,
    subjects = EXCLUDED.subjects,
    license_record = EXCLUDED.license_record,
    provider_id = EXCLUDED.provider_id,
    primary_file_id = EXCLUDED.primary_file_id,
    links = EXCLUDED.links,
    raw = EXCLUDED.raw,
    -- If the file changed OR version increased, force re-download
    pdf_downloaded = CASE
        WHEN (preprints.primary_file_id IS DISTINCT FROM EXCLUDED.primary_file_id)
          OR (EXCLUDED.version > preprints.version)
        THEN false
        ELSE preprints.pdf_downloaded
    END,
    -- Clear file path/timestamp only when we invalidate the download
    pdf_downloaded_at = CASE
        WHEN (preprints.primary_file_id IS DISTINCT FROM EXCLUDED.primary_file_id)
          OR (EXCLUDED.version > preprints.version)
        THEN NULL
        ELSE preprints.pdf_downloaded_at
    END,
    pdf_path = CASE
        WHEN (preprints.primary_file_id IS DISTINCT FROM EXCLUDED.primary_file_id)
          OR (EXCLUDED.version > preprints.version)
        THEN NULL
        ELSE preprints.pdf_path
    END,
    updated_at = now()
WHERE
    (preprints.date_modified IS NULL OR EXCLUDED.date_modified > preprints.date_modified)
    OR (EXCLUDED.version > preprints.version)
    OR (preprints.primary_file_id IS DISTINCT FROM EXCLUDED.primary_file_id);
""")

def _row_from_osf(obj: Dict) -> Dict:
    a = (obj.get("attributes") or {})
    rel = (obj.get("relationships") or {})
    provider = ((rel.get("provider") or {}).get("data") or {})
    primary_file = ((rel.get("primary_file") or {}).get("data") or {})

    subjects = a.get("subjects")
    license_record = a.get("license_record")
    links = obj.get("links")

    return {
        "osf_id": obj.get("id"),
        "type": obj.get("type"),
        "title": a.get("title"),
        "description": a.get("description"),
        "doi": a.get("doi"),
        "date_created": a.get("date_created"),
        "date_modified": a.get("date_modified"),
        "date_published": a.get("date_published"),
        "is_published": a.get("is_published"),
        "version": a.get("version"),
        "is_latest_version": a.get("is_latest_version"),
        "reviews_state": a.get("reviews_state"),
        "tags": a.get("tags") or [],
        "subjects_json": json.dumps(subjects) if subjects is not None else None,
        "license_record_json": json.dumps(license_record) if license_record is not None else None,
        "provider_id": provider.get("id"),
        "primary_file_id": primary_file.get("id"),
        "links_json": json.dumps(links) if links is not None else None,
        "raw_json": json.dumps(obj),
        # download-tracking fields: leave None so INSERT defaults to false/NULL
        "pdf_downloaded": None,
        "pdf_downloaded_at": None,
        "pdf_path": None,
    }

def upsert_batch(objs: Iterable[Dict]) -> int:
    payload: List[Dict] = [_row_from_osf(o) for o in objs]
    if not payload:
        return 0
    with engine.begin() as conn:
        for row in payload:
            conn.execute(UPSERT_SQL, row)
    return len(payload)
