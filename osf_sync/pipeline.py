from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import socket
import time
import uuid
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional

import requests

from .augmentation.doi_multi_method import enrich_missing_with_multi_method
from .augmentation.flora_screening import lookup_and_screen_flora
from .augmentation.run_extract import extract_for_osf_id
from .author_randomization import run_author_randomization
from .db import init_db
from .dynamo.preprints_repo import PreprintsRepo
from .fetch_one import fetch_preprint_by_doi, fetch_preprint_by_id, upsert_one_preprint
from .grobid import mark_tei, process_pdf_to_tei
from .iter_preprints import iter_preprints_batches, iter_preprints_range
from .exclusion_logging import log_preprint_exclusion
from .pdf import ensure_pdf_available_or_delete, mark_downloaded, resolve_primary_file_info_from_raw
from .upsert import upsert_batch
from .email import process_email_batch
from .extraction.extract_author_list import run_author_extract
from .runtime_config import RUNTIME_CONFIG

OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "you@example.com")
PDF_DEST_ROOT = os.environ.get("PDF_DEST_ROOT", "/data/preprints")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL")
PIPELINE_NOTIFY_EMAIL = os.environ.get("PIPELINE_NOTIFY_EMAIL")
SOURCE_KEY_ALL = "osf:all"
DEFAULT_LEASE_SECONDS = int(os.environ.get("PIPELINE_CLAIM_LEASE_SECONDS", "1800"))

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
logger.setLevel(logging.INFO)
class _PypdfNoiseFilter(logging.Filter):
    """Suppress repetitive 'wrong pointing object' warnings from pypdf."""
    def filter(self, record: logging.LogRecord) -> bool:
        return not (record.name.startswith("pypdf") and "wrong pointing object" in record.getMessage())

for _h in logging.getLogger().handlers:
    _h.addFilter(_PypdfNoiseFilter())
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _slack(msg: str, *, level: str = "info", extra: Optional[Dict[str, Any]] = None) -> None:
    if not SLACK_WEBHOOK:
        return
    payload = {"text": f"*[{level.upper()}]* {msg}"}
    if extra:
        payload["attachments"] = [
            {"text": "```" + json.dumps(extra, ensure_ascii=False, indent=2) + "```"}
        ]
    try:
        requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    except Exception:
        logger.debug("Slack send failed", exc_info=True)


def _parse_iso_dt(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        v = value.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(v)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        else:
            d = d.astimezone(dt.timezone.utc)
        return d
    except Exception:
        return None


def _get_cursor(source_key: str) -> Optional[dt.datetime]:
    repo = PreprintsRepo()
    v = repo.get_cursor(source_key)
    if not v:
        return None
    try:
        vv = v.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(vv)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d
    except Exception:
        return None


def _set_cursor(source_key: str, last_seen: dt.datetime) -> None:
    repo = PreprintsRepo()
    repo.set_cursor(source_key, last_seen.isoformat())


def _make_owner(owner: Optional[str] = None) -> str:
    if owner:
        return owner
    host = socket.gethostname()
    pid = os.getpid()
    return f"{host}:{pid}:{uuid.uuid4().hex[:8]}"


def _deadline(max_seconds: Optional[int]) -> Optional[float]:
    if max_seconds is None or max_seconds <= 0:
        return None
    return time.monotonic() + max_seconds


def _time_up(deadline: Optional[float]) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _excluded_summary() -> Dict[str, Any]:
    repo = PreprintsRepo()
    try:
        return repo.summarize_excluded_preprints()
    except Exception:
        logger.warning("failed to summarize excluded preprints", exc_info=True)
        return {"total_excluded_preprints": None, "by_reason": {}}


def _subtract_months(d: dt.date, months: int) -> dt.date:
    year = d.year
    month = d.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)


def _add_months(d: dt.date, months: int) -> dt.date:
    year = d.year
    month = d.month + months
    while month > 12:
        month -= 12
        year += 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)


def _parse_iso_date(value: str) -> Optional[dt.date]:
    raw = (value or "").strip()
    if not raw:
        return None
    if len(raw) == 10:
        try:
            return dt.date.fromisoformat(raw)
        except Exception:
            return None
    parsed = _parse_iso_dt(raw)
    return parsed.date() if parsed else None


def _env_true(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_VALUES


def _email_stage_enabled() -> bool:
    # Safety default: email sending is OFF unless explicitly enabled.
    return _env_true("PIPELINE_ENABLE_EMAIL_STAGE", default=False)


def _disabled_email_result(*, dry_run: bool) -> Dict[str, Any]:
    return {
        "stage": "email",
        "sent": 0,
        "processed": 0,
        "failed": 0,
        "skipped_disabled": True,
        "limit_reached": False,
        "stopped_due_to_time": False,
        "dry_run": dry_run,
    }


def _parse_config_anchor_date() -> Optional[dt.date]:
    anchor_raw = (RUNTIME_CONFIG.ingest.anchor_date or "").strip()
    if not anchor_raw:
        return None
    anchor_dt = _parse_iso_dt(anchor_raw)
    if anchor_dt is None:
        raise RuntimeError(f"Invalid ingest.anchor_date value: {RUNTIME_CONFIG.ingest.anchor_date!r}")
    return anchor_dt.date()


def _should_write_cursor(window_mode: str) -> bool:
    if _env_true("SYNC_DISABLE_CURSOR_WRITE", default=False):
        return False
    if "override" in window_mode and not _env_true("SYNC_OVERRIDE_WRITES_CURSOR", default=False):
        return False
    return True


def _sync_ingest_meta_key(source_key: str) -> str:
    return f"{source_key}:ingest_meta"


def _read_ingest_meta(source_key: str) -> Dict[str, Any]:
    repo = PreprintsRepo()
    return repo.get_sync_item(_sync_ingest_meta_key(source_key)) or {}


def _write_ingest_meta(source_key: str) -> None:
    repo = PreprintsRepo()
    repo.put_sync_item(
        {
            "source_key": _sync_ingest_meta_key(source_key),
            "anchor_date": (RUNTIME_CONFIG.ingest.anchor_date or "").strip(),
            "window_months": int(RUNTIME_CONFIG.ingest.window_months),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
    )


def _ingest_config_changed(meta: Dict[str, Any]) -> bool:
    if not getattr(RUNTIME_CONFIG.ingest, "backfill_on_config_change", True):
        return False
    if not meta:
        return False
    prev_anchor = str(meta.get("anchor_date") or "").strip()
    prev_window_raw = meta.get("window_months")
    try:
        prev_window = int(prev_window_raw) if prev_window_raw is not None and str(prev_window_raw).strip() else None
    except Exception:
        prev_window = None
    if not prev_anchor and prev_window is None:
        return False
    curr_anchor = (RUNTIME_CONFIG.ingest.anchor_date or "").strip()
    curr_window = int(RUNTIME_CONFIG.ingest.window_months)
    return prev_anchor != curr_anchor or prev_window != curr_window


def _resolve_config_change_backfill(source_key: str) -> Optional[tuple[str, str, str]]:
    env_mode = (os.environ.get("PIPELINE_ENV", "dev") or "dev").strip().lower()
    if env_mode != "prod":
        return None
    if (os.environ.get("SYNC_START_DATE_OVERRIDE") or "").strip():
        return None
    meta = _read_ingest_meta(source_key)
    if not _ingest_config_changed(meta):
        return None

    anchor_date = _parse_config_anchor_date()
    if anchor_date is None:
        return None
    window_start = _subtract_months(anchor_date, RUNTIME_CONFIG.ingest.window_months)
    window_end = _add_months(anchor_date, RUNTIME_CONFIG.ingest.window_months)
    today = dt.datetime.now(dt.timezone.utc).date()
    effective_end = min(window_end, today)
    if effective_end < window_start:
        return None
    return window_start.isoformat(), effective_end.isoformat(), "prod_anchor_config_change_backfill"


def _resolve_sync_window(
    cursor_dt: Optional[dt.datetime],
    *,
    now_utc: Optional[dt.datetime] = None,
) -> tuple[str, Optional[str], str]:
    """
    Resolve sync lower/upper bounds.

    If ingest.anchor_date is configured, always sync within that fixed window.
    Otherwise, preserve legacy incremental behavior (cursor or last-7-days bootstrap).
    """
    env_mode = (os.environ.get("PIPELINE_ENV", "dev") or "dev").strip().lower()
    if env_mode not in {"dev", "prod"}:
        env_mode = "dev"

    override_raw = (os.environ.get("SYNC_START_DATE_OVERRIDE") or "").strip()
    override_end_raw = (os.environ.get("SYNC_END_DATE_OVERRIDE") or "").strip()
    if override_raw:
        override_date = _parse_iso_date(override_raw)
        if override_date is None:
            raise RuntimeError(f"Invalid SYNC_START_DATE_OVERRIDE value: {override_raw!r}")

        until_date: Optional[dt.date]
        if override_end_raw:
            until_date = _parse_iso_date(override_end_raw)
            if until_date is None:
                raise RuntimeError(f"Invalid SYNC_END_DATE_OVERRIDE value: {override_end_raw!r}")
        elif env_mode == "prod":
            anchor = _parse_config_anchor_date()
            if anchor is None:
                raise RuntimeError("PIPELINE_ENV=prod with override requires ingest.anchor_date to be set")
            until_date = _add_months(anchor, RUNTIME_CONFIG.ingest.window_months)
            today = (now_utc or dt.datetime.now(dt.timezone.utc)).date()
            until_date = min(until_date, today)
            if until_date is None:
                raise RuntimeError("PIPELINE_ENV=prod with override requires ingest.anchor_date to be set")
        else:
            until_date = None

        if until_date and override_date > until_date:
            raise RuntimeError(
                f"SYNC_START_DATE_OVERRIDE ({override_date.isoformat()}) cannot be after "
                f"SYNC_END_DATE_OVERRIDE/anchor ({until_date.isoformat()})"
            )
        return override_date.isoformat(), (until_date.isoformat() if until_date else None), f"{env_mode}_override"

    if env_mode == "prod":
        anchor_date = _parse_config_anchor_date()
        if anchor_date is None:
            raise RuntimeError("PIPELINE_ENV=prod requires ingest.anchor_date to be set")
        window_start = _subtract_months(anchor_date, RUNTIME_CONFIG.ingest.window_months)
        window_end = _add_months(anchor_date, RUNTIME_CONFIG.ingest.window_months)
        today = (now_utc or dt.datetime.now(dt.timezone.utc)).date()
        effective_end = min(window_end, today)
        if effective_end < window_start:
            raise RuntimeError(
                f"Resolved prod window is empty ({window_start.isoformat()}..{effective_end.isoformat()})"
            )
        if cursor_dt is None:
            start_date = window_start
        else:
            cursor_date = cursor_dt.astimezone(dt.timezone.utc).date()
            start_date = min(max(cursor_date, window_start), effective_end)
        return start_date.isoformat(), effective_end.isoformat(), "prod_anchor_window"

    try:
        lookback_days = int(os.environ.get("DEV_SYNC_LOOKBACK_DAYS", "7"))
        if lookback_days < 1:
            lookback_days = 7
    except Exception:
        lookback_days = 7

    now_dt = now_utc or dt.datetime.now(dt.timezone.utc)
    window_start = (now_dt - dt.timedelta(days=lookback_days)).date()
    if cursor_dt is None:
        start_date = window_start
    else:
        cursor_date = cursor_dt.astimezone(dt.timezone.utc).date()
        start_date = max(cursor_date, window_start)
    return start_date.isoformat(), None, "dev_recent"


def sync_from_osf(
    *,
    subject_text: Optional[str] = None,
    batch_size: int = 1000,
    limit: Optional[int] = None,
    max_seconds: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    init_db()
    source_key = f"osf:{subject_text}" if subject_text else SOURCE_KEY_ALL
    since_dt = _get_cursor(source_key)
    auto_backfill = _resolve_config_change_backfill(source_key)
    if auto_backfill:
        since_iso_date, until_iso_date, window_mode = auto_backfill
    else:
        since_iso_date, until_iso_date, window_mode = _resolve_sync_window(since_dt)

    total_upserted = 0
    processed = 0
    max_created_seen: Optional[dt.datetime] = None
    deadline = _deadline(max_seconds)

    logger.info(
        "sync_from_osf start",
        extra={
            "since": since_iso_date,
            "until": until_iso_date,
            "subject": subject_text,
            "window_mode": window_mode,
        },
    )

    iterator = (
        iter_preprints_range(
            start_date=since_iso_date,
            until_date=until_iso_date,
            subject_text=subject_text,
            batch_size=batch_size,
            sort="date_created",
            date_field="date_created",
        )
        if until_iso_date
        else iter_preprints_batches(
            since_date=since_iso_date,
            subject_text=subject_text,
            batch_size=batch_size,
            sort="date_created",
            date_field="date_created",
        )
    )

    for batch in iterator:
        if _time_up(deadline):
            break

        effective_batch = batch
        if limit is not None:
            remaining = limit - processed
            if remaining <= 0:
                break
            effective_batch = batch[:remaining]

        for obj in effective_batch:
            attrs = obj.get("attributes") or {}
            created = _parse_iso_dt(attrs.get("date_created") or attrs.get("date_published"))
            if created and (max_created_seen is None or created > max_created_seen):
                max_created_seen = created

        processed += len(effective_batch)
        if not dry_run:
            total_upserted += upsert_batch(effective_batch)
        else:
            total_upserted += len(effective_batch)

        logger.info("upserted batch", extra={"batch_size": len(effective_batch), "total": total_upserted})
        time.sleep(0.2)

    cursor_fallback = since_dt or _parse_iso_dt(since_iso_date)
    cursor_out = (max_created_seen or cursor_fallback).isoformat() if (max_created_seen or cursor_fallback) else since_iso_date
    if max_created_seen and not dry_run and _should_write_cursor(window_mode):
        if since_dt and max_created_seen < since_dt:
            logger.info(
                "sync cursor not rewound",
                extra={"source_key": source_key, "existing_cursor": since_dt.isoformat(), "candidate": max_created_seen.isoformat()},
            )
        else:
            _set_cursor(source_key, max_created_seen)

    out = {
        "upserted": total_upserted,
        "cursor": cursor_out,
        "dry_run": dry_run,
        "stopped_due_to_time": _time_up(deadline),
    }
    if not dry_run:
        _write_ingest_meta(source_key)
    _slack("OSF sync finished", extra=out)
    return out


def sync_from_date_to_now(
    *,
    start_date: str,
    subject_text: Optional[str] = None,
    batch_size: int = 1000,
    limit: Optional[int] = None,
    max_seconds: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    init_db()
    total = 0
    processed = 0
    deadline = _deadline(max_seconds)
    logger.info("sync_from_date_to_now", extra={"start": start_date, "subject": subject_text})

    for batch in iter_preprints_range(
        start_date=start_date,
        until_date=None,
        subject_text=subject_text,
        batch_size=batch_size,
    ):
        if _time_up(deadline):
            break

        effective_batch = batch
        if limit is not None:
            remaining = limit - processed
            if remaining <= 0:
                break
            effective_batch = batch[:remaining]

        processed += len(effective_batch)
        if not dry_run:
            total += upsert_batch(effective_batch)
        else:
            total += len(effective_batch)
        logger.info("upserted batch", extra={"batch_size": len(effective_batch), "total": total})

    out = {
        "upserted": total,
        "from": start_date,
        "to": "now",
        "dry_run": dry_run,
        "stopped_due_to_time": _time_up(deadline),
    }
    _slack("Ad-hoc OSF window sync finished", extra=out)
    return out


def _is_withdrawn(raw: Optional[Dict[str, Any]]) -> bool:
    """Check whether the stored OSF API payload indicates a withdrawn preprint."""
    if not raw:
        return False
    attrs = raw.get("attributes") or raw
    return bool(attrs.get("date_withdrawn"))


def download_single_pdf(osf_id: str) -> Dict[str, Any]:
    repo = PreprintsRepo()
    row = repo.get_preprint_basic(osf_id)
    if not row:
        return {"osf_id": osf_id, "skipped": "no longer in DB"}

    provider_id = row["provider_id"] or "unknown"

    if _is_withdrawn(row.get("raw")):
        log_preprint_exclusion(
            reason="withdrawn",
            osf_id=osf_id,
            stage="pdf",
            details={"provider_id": provider_id},
        )
        logger.info("Skipping withdrawn preprint [%s]", osf_id)
        return {"osf_id": osf_id, "skipped": "withdrawn"}

    kind, path, exclusion_reason = ensure_pdf_available_or_delete(
        osf_id=row["osf_id"],
        provider_id=provider_id,
        raw=row["raw"],
        dest_root=PDF_DEST_ROOT,
    )

    if kind == "deleted":
        if exclusion_reason:
            details = {"provider_id": provider_id}
            try:
                _url, ctype, name = resolve_primary_file_info_from_raw(row["raw"])
                if ctype:
                    details["primary_file_content_type"] = ctype
                if name:
                    details["primary_file_name"] = name
            except Exception:
                logger.debug("failed to resolve primary file metadata for exclusion logging", exc_info=True)
            log_preprint_exclusion(
                reason=exclusion_reason,
                osf_id=osf_id,
                stage="pdf",
                details=details,
            )
        return {"osf_id": osf_id, "deleted": True, "reason": exclusion_reason or "unsupported file type"}

    mark_downloaded(osf_id=row["osf_id"], local_path=path, ok=True)
    logger.info("PDF saved [%s] path=%s", osf_id, path)
    return {"osf_id": osf_id, "downloaded": True, "source": kind, "path": path}


def grobid_single(osf_id: str) -> Dict[str, Any]:
    repo = PreprintsRepo()
    full = repo.t_preprints.get_item(
        Key={"osf_id": osf_id},
        ProjectionExpression="osf_id, provider_id, pdf_downloaded, tei_generated",
    ).get("Item")
    if not full:
        return {"osf_id": osf_id, "skipped": "not found"}
    if not full.get("pdf_downloaded"):
        return {"osf_id": osf_id, "skipped": "pdf not downloaded"}
    if full.get("tei_generated"):
        return {"osf_id": osf_id, "skipped": "already processed"}

    provider_id = full.get("provider_id") or "unknown"

    # Ephemeral storage fallback: re-download PDF if missing on disk
    from .grobid import _pdf_path
    if _pdf_path(provider_id, osf_id) is None:
        logger.info("PDF missing on disk, re-downloading [%s]", osf_id)
        download_single_pdf(osf_id)

    ok, tei_path, err = process_pdf_to_tei(provider_id, osf_id)
    if ok:
        mark_tei(osf_id, ok=True, tei_path=tei_path)
    logger.info("GROBID done [%s] ok=%s", osf_id, ok)
    return {"osf_id": osf_id, "ok": ok, "tei_path": tei_path, "error": err}


def extract_from_tei(provider_id: str, osf_id: str) -> Dict[str, Any]:
    base = os.environ.get("PDF_DEST_ROOT", PDF_DEST_ROOT)
    summary = extract_for_osf_id(provider_id, osf_id, base)
    return {
        "osf_id": osf_id,
        "parsed_ok": bool(summary.get("parsed_ok")),
        "written_ok": bool(summary.get("written_ok")),
        "references_upserted": int(summary.get("refs_count") or 0),
        "error": summary.get("error"),
    }


PDF_MAX_RETRIES = int(os.environ.get("PDF_MAX_RETRIES", "3"))


def process_pdf_batch(
    *,
    limit: int = 100,
    owner: Optional[str] = None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    max_seconds: Optional[int] = None,
    workers: int = 1,
    dry_run: bool = False,
) -> Dict[str, Any]:
    repo = PreprintsRepo()
    owner_id = _make_owner(owner)
    deadline = _deadline(max_seconds)

    candidates = repo.select_for_pdf(limit=max(limit * 3, limit))
    claimed = processed = failed = skipped_claimed = 0

    # Claim items sequentially (DynamoDB conditional writes are the sync point)
    claimed_ids = []
    for osf_id in candidates:
        if len(claimed_ids) + failed >= limit or _time_up(deadline):
            break
        if not repo.claim_stage_item("pdf", osf_id, owner=owner_id, lease_seconds=lease_seconds):
            skipped_claimed += 1
            continue
        claimed += 1
        if dry_run:
            repo.release_stage_claim("pdf", osf_id)
            processed += 1
            continue
        claimed_ids.append(osf_id)

    # Process claimed items in parallel
    if claimed_ids and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(download_single_pdf, oid): oid for oid in claimed_ids}
            for future in as_completed(futures):
                osf_id = futures[future]
                try:
                    future.result()
                    processed += 1
                except Exception as exc:
                    failed += 1
                    retries = repo.record_stage_error("pdf", osf_id, str(exc))
                    if retries >= PDF_MAX_RETRIES:
                        log_preprint_exclusion(reason="max_pdf_retries_exceeded", osf_id=osf_id, stage="pdf", details={"last_error": str(exc)[:200]})
                        logger.warning("PDF stage permanently failed [%s] after %d retries: %s", osf_id, retries, exc)
                    else:
                        logger.warning("PDF stage failed [%s] (retry %d/%d): %s", osf_id, retries, PDF_MAX_RETRIES, exc)
    else:
        for osf_id in claimed_ids:
            try:
                download_single_pdf(osf_id)
                processed += 1
            except Exception as exc:
                failed += 1
                retries = repo.record_stage_error("pdf", osf_id, str(exc))
                if retries >= PDF_MAX_RETRIES:
                    log_preprint_exclusion(reason="max_pdf_retries_exceeded", osf_id=osf_id, stage="pdf", details={"last_error": str(exc)[:200]})
                    logger.warning("PDF stage permanently failed [%s] after %d retries: %s", osf_id, retries, exc)
                else:
                    logger.warning("PDF stage failed [%s] (retry %d/%d): %s", osf_id, retries, PDF_MAX_RETRIES, exc)

    out = {
        "stage": "pdf",
        "owner": owner_id,
        "selected": len(candidates),
        "claimed": claimed,
        "processed": processed,
        "failed": failed,
        "skipped_claimed": skipped_claimed,
        "dry_run": dry_run,
        "limit_reached": (processed + failed) >= limit,
        "stopped_due_to_time": _time_up(deadline),
    }
    _slack("PDF stage finished", extra=out)
    return out


GROBID_MAX_RETRIES = 3

def process_grobid_batch(
    *,
    limit: int = 50,
    owner: Optional[str] = None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    max_seconds: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    repo = PreprintsRepo()
    owner_id = _make_owner(owner)
    deadline = _deadline(max_seconds)

    candidates = repo.select_for_grobid(limit=max(limit * 3, limit))
    claimed = processed = failed = skipped_claimed = 0

    for osf_id in candidates:
        if processed + failed >= limit or _time_up(deadline):
            break
        if not repo.claim_stage_item("grobid", osf_id, owner=owner_id, lease_seconds=lease_seconds):
            skipped_claimed += 1
            continue

        claimed += 1
        if dry_run:
            repo.release_stage_claim("grobid", osf_id)
            processed += 1
            continue

        try:
            result = grobid_single(osf_id)
            if result.get("skipped"):
                repo.release_stage_claim("grobid", osf_id)
                processed += 1
            elif not result.get("ok"):
                failed += 1
                error_msg = str(result.get("error") or "unknown error")
                retries = repo.record_stage_error("grobid", osf_id, error_msg)
                if retries >= GROBID_MAX_RETRIES:
                    mark_tei(osf_id, ok=False, tei_path=None)
                    logger.warning("GROBID permanently failed [%s] after %d retries: %s", osf_id, retries, error_msg)
                else:
                    logger.info("GROBID failed [%s] (retry %d/%d): %s", osf_id, retries, GROBID_MAX_RETRIES, error_msg)
            else:
                processed += 1
        except Exception as exc:
            failed += 1
            retries = repo.record_stage_error("grobid", osf_id, str(exc))
            if retries >= GROBID_MAX_RETRIES:
                mark_tei(osf_id, ok=False, tei_path=None)
                logger.warning("GROBID permanently failed [%s] after %d retries: %s", osf_id, retries, exc)
            else:
                logger.warning("GROBID error [%s] (retry %d/%d): %s", osf_id, retries, GROBID_MAX_RETRIES, exc)

    out = {
        "stage": "grobid",
        "owner": owner_id,
        "selected": len(candidates),
        "claimed": claimed,
        "processed": processed,
        "failed": failed,
        "skipped_claimed": skipped_claimed,
        "dry_run": dry_run,
        "limit_reached": (processed + failed) >= limit,
        "stopped_due_to_time": _time_up(deadline),
    }
    _slack("GROBID stage finished", extra=out)
    return out


def process_extract_batch(
    *,
    limit: int = 200,
    owner: Optional[str] = None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    max_seconds: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    repo = PreprintsRepo()
    owner_id = _make_owner(owner)
    deadline = _deadline(max_seconds)

    candidates = repo.select_for_extraction(limit=max(limit * 3, limit))
    claimed = processed = failed = skipped_claimed = 0

    for item in candidates:
        if processed + failed >= limit or _time_up(deadline):
            break

        osf_id = item.get("osf_id")
        if not osf_id:
            continue

        if not repo.claim_stage_item("extract", osf_id, owner=owner_id, lease_seconds=lease_seconds):
            skipped_claimed += 1
            continue

        claimed += 1
        if dry_run:
            repo.release_stage_claim("extract", osf_id)
            processed += 1
            continue

        provider_id = item.get("provider_id") or "unknown"
        try:
            result = extract_from_tei(provider_id, osf_id)
            if result.get("written_ok"):
                if int(result.get("references_upserted") or 0) == 0:
                    log_preprint_exclusion(
                        reason="no_references_extracted",
                        osf_id=osf_id,
                        stage="extract",
                        details={"provider_id": provider_id},
                    )
                processed += 1
            else:
                failed += 1
                repo.record_stage_error("extract", osf_id, str(result.get("error") or "extract failed"))
        except Exception as exc:
            failed += 1
            repo.record_stage_error("extract", osf_id, str(exc))
            logger.exception("Extract stage failed", extra={"osf_id": osf_id})

    out = {
        "stage": "extract",
        "owner": owner_id,
        "selected": len(candidates),
        "claimed": claimed,
        "processed": processed,
        "failed": failed,
        "skipped_claimed": skipped_claimed,
        "dry_run": dry_run,
        "limit_reached": (processed + failed) >= limit,
        "stopped_due_to_time": _time_up(deadline),
    }
    _slack("TEI extract stage finished", extra=out)
    return out


def process_enrich_batch(
    *,
    limit: int = 300,
    threshold: Optional[int] = None,
    mailto: Optional[str] = OPENALEX_EMAIL,
    osf_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    debug: bool = False,
    workers: int = 1,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if dry_run:
        return {
            "stage": "enrich",
            "checked": 0,
            "updated": 0,
            "failed": 0,
            "preprints_selected": 0,
            "limit_reached": False,
            "dry_run": True,
        }
    stats = enrich_missing_with_multi_method(
        limit=limit,
        threshold=threshold,
        mailto=mailto,
        osf_id=osf_id,
        ref_id=ref_id,
        debug=debug,
        workers=workers,
    )
    out = {"stage": "enrich", **stats, "dry_run": False}
    _slack("Reference enrichment finished", extra=out)
    return out


def process_flora_batch(
    *,
    limit_lookup: int = 0,
    limit_screen: int = 0,
    osf_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    cache_ttl_hours: Optional[int] = None,
    persist_flags: bool = True,
    only_unchecked: bool = True,
    debug: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if dry_run:
        return {
            "stage": "flora",
            "lookup": {"checked": 0, "updated": 0, "failed": 0},
            "screen": [],
            "dry_run": True,
        }
    out = lookup_and_screen_flora(
        limit_lookup=limit_lookup,
        limit_screen=limit_screen,
        osf_id=osf_id,
        ref_id=ref_id,
        cache_ttl_hours=cache_ttl_hours,
        persist_flags=persist_flags,
        only_unchecked=only_unchecked,
        debug=debug,
    )
    result = {"stage": "flora", **out, "dry_run": False}
    _slack(
        "FLORA lookup/screen finished",
        extra={"lookup": out.get("lookup", {}), "screen_count": len(out.get("screen", []))},
    )
    return result


def process_author_batch(
    *,
    osf_ids: Optional[list[str]] = None,
    ids_file: Optional[str] = None,
    limit: Optional[int] = None,
    out: Optional[str] = None,
    pdf_root: Optional[str] = None,
    keep_files: bool = False,
    debug: bool = False,
    debug_log: Optional[str] = None,
    match_emails_file: Optional[str] = None,
    match_emails_threshold: float = 0.90,
    include_existing: bool = False,
    write_debug_csv: bool = False,
    orcid_workers: int = 1,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if dry_run:
        return {"stage": "author", "exit_code": 0, "dry_run": True}
    code = run_author_extract(
        osf_ids=osf_ids,
        ids_file=ids_file,
        limit=limit,
        out=out,
        pdf_root=pdf_root,
        keep_files=keep_files,
        debug=debug,
        debug_log=debug_log,
        match_emails_file=match_emails_file,
        match_emails_threshold=match_emails_threshold,
        include_existing=include_existing,
        write_debug_csv=write_debug_csv,
        orcid_workers=orcid_workers,
    )
    out = {"stage": "author", "exit_code": code, "dry_run": False}
    _slack("Author extraction finished", extra=out)
    return out


def process_author_randomization_batch(
    *,
    authors_csv: Optional[str] = "osf_sync/extraction/authorList_ext.csv",
    limit_preprints: Optional[int] = None,
    seed: Optional[int] = None,
    network_state_key: str = "trial:author_network_state",
    dry_run: bool = False,
) -> Dict[str, Any]:
    out = run_author_randomization(
        authors_csv=authors_csv,
        limit_preprints=limit_preprints,
        seed=seed,
        network_state_key=network_state_key,
        dry_run=dry_run,
    )
    result = {"stage": "author-randomize", **out}
    _slack("Author randomization finished", extra=result)
    return result


def sync_one_by_id(*, osf_id: str, run_pdf_and_grobid: bool = True, run_extract: bool = True) -> Dict[str, Any]:
    data = fetch_preprint_by_id(osf_id)
    if not data:
        return {"ok": False, "reason": "not found", "osf_id": osf_id}

    upserted = upsert_one_preprint(data)
    result: Dict[str, Any] = {"ok": True, "osf_id": data["id"], "upserted": upserted}

    if run_pdf_and_grobid:
        pdf = download_single_pdf(data["id"])
        result["pdf"] = pdf
        grobid = grobid_single(data["id"])
        result["grobid"] = grobid
        if run_extract and grobid.get("ok"):
            repo = PreprintsRepo()
            basic = repo.get_preprint_basic(data["id"])
            provider_id = (basic or {}).get("provider_id") or "unknown"
            result["extract"] = extract_from_tei(provider_id, data["id"])
    return result


def sync_one_by_doi(*, doi_or_url: str, run_pdf_and_grobid: bool = True, run_extract: bool = True) -> Dict[str, Any]:
    data = fetch_preprint_by_doi(doi_or_url)
    if not data:
        return {"ok": False, "reason": "not found", "doi": doi_or_url}

    upserted = upsert_one_preprint(data)
    result: Dict[str, Any] = {"ok": True, "osf_id": data["id"], "upserted": upserted}

    if run_pdf_and_grobid:
        pdf = download_single_pdf(data["id"])
        result["pdf"] = pdf
        grobid = grobid_single(data["id"])
        result["grobid"] = grobid
        if run_extract and grobid.get("ok"):
            repo = PreprintsRepo()
            basic = repo.get_preprint_basic(data["id"])
            provider_id = (basic or {}).get("provider_id") or "unknown"
            result["extract"] = extract_from_tei(provider_id, data["id"])
    return result


def run_stage(args: argparse.Namespace) -> Dict[str, Any]:
    stage = args.stage
    if stage == "sync":
        return sync_from_osf(
            subject_text=args.subject,
            batch_size=args.batch_size,
            limit=args.limit,
            max_seconds=args.max_seconds,
            dry_run=args.dry_run,
        )
    if stage == "pdf":
        return process_pdf_batch(
            limit=args.limit or 100,
            owner=args.owner,
            lease_seconds=args.lease_seconds,
            max_seconds=args.max_seconds,
            workers=getattr(args, "download_workers", 1),
            dry_run=args.dry_run,
        )
    if stage == "grobid":
        return process_grobid_batch(
            limit=args.limit or 50,
            owner=args.owner,
            lease_seconds=args.lease_seconds,
            max_seconds=args.max_seconds,
            dry_run=args.dry_run,
        )
    if stage == "extract":
        return process_extract_batch(
            limit=args.limit or 200,
            owner=args.owner,
            lease_seconds=args.lease_seconds,
            max_seconds=args.max_seconds,
            dry_run=args.dry_run,
        )
    if stage == "enrich":
        return process_enrich_batch(
            limit=args.limit or 300,
            threshold=args.threshold,
            mailto=args.mailto,
            osf_id=args.osf_id,
            ref_id=args.ref_id,
            debug=args.debug,
            workers=getattr(args, "enrich_workers", 1),
            dry_run=args.dry_run,
        )
    if stage == "flora":
        return process_flora_batch(
            limit_lookup=args.limit_lookup,
            limit_screen=args.limit_screen,
            osf_id=args.osf_id,
            ref_id=args.ref_id,
            cache_ttl_hours=args.cache_ttl_hours,
            persist_flags=not args.no_persist,
            only_unchecked=not args.include_checked,
            debug=args.debug,
            dry_run=args.dry_run,
        )
    if stage == "author":
        return process_author_batch(
            osf_ids=args.author_osf_ids,
            ids_file=args.ids_file,
            limit=args.limit,
            out=args.out,
            pdf_root=args.pdf_root,
            keep_files=args.keep_files,
            debug=args.debug,
            debug_log=args.debug_log,
            match_emails_file=args.match_emails_file,
            match_emails_threshold=args.match_emails_threshold,
            include_existing=args.include_existing,
            write_debug_csv=args.write_debug_csv,
            orcid_workers=getattr(args, "orcid_workers", 1),
            dry_run=args.dry_run,
        )
    if stage == "author-randomize":
        return process_author_randomization_batch(
            authors_csv=getattr(args, "authors_csv", "osf_sync/extraction/authorList_ext.csv"),
            limit_preprints=getattr(args, "limit_preprints", None) or getattr(args, "limit", None),
            seed=getattr(args, "seed", None),
            network_state_key=getattr(args, "network_state_key", "trial:author_network_state"),
            dry_run=args.dry_run,
        )
    if stage == "email":
        if not _email_stage_enabled():
            logger.warning("Email stage requested but disabled (PIPELINE_ENABLE_EMAIL_STAGE is false)")
            return _disabled_email_result(dry_run=args.dry_run)
        return process_email_batch(
            limit=args.limit or 50,
            max_seconds=args.max_seconds,
            spread_seconds=getattr(args, "spread_seconds", None),
            dry_run=args.dry_run,
            osf_id=args.osf_id,
        )
    if stage == "inbox":
        from .email.inbox import process_inbox, process_validation_responses
        inbox_result = process_inbox(
            max_messages=args.limit or 200,
            dry_run=args.dry_run,
        )
        validation_result = process_validation_responses(
            max_messages=args.limit or 50,
            dry_run=args.dry_run,
        )
        return {**inbox_result, "validation_responses": validation_result}
    raise ValueError(f"Unsupported stage: {stage}")


def _merge_email_results(accumulated: Dict[str, Any], batch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge a new email batch result into the accumulated totals."""
    if not accumulated:
        accumulated = dict(batch)
    else:
        for key in ("sent", "failed", "skipped_suppressed", "skipped_invalid", "skipped_no_context"):
            accumulated[key] = accumulated.get(key, 0) + batch.get(key, 0)
        accumulated["stopped_due_to_time"] = batch.get("stopped_due_to_time", False)
    # Alias so _notify_pipeline_summary picks up the count
    accumulated["processed"] = accumulated.get("sent", 0)
    return accumulated


def run_all(args: argparse.Namespace) -> Dict[str, Any]:
    out: Dict[str, Any] = {"stages": {}}

    include_email = getattr(args, "include_email", False)
    email_limit = getattr(args, "email_limit", 50)
    email_enabled = _email_stage_enabled()

    out["stages"]["sync"] = sync_from_osf(
        subject_text=args.subject,
        batch_size=args.batch_size,
        limit=args.sync_limit,
        max_seconds=args.max_seconds_per_stage,
        dry_run=args.dry_run,
    )

    out["stages"]["pdf"] = process_pdf_batch(
        limit=args.pdf_limit,
        owner=args.owner,
        lease_seconds=args.lease_seconds,
        max_seconds=args.max_seconds_per_stage,
        workers=getattr(args, "download_workers", 1),
        dry_run=args.dry_run,
    )

    out["stages"]["grobid"] = process_grobid_batch(
        limit=args.grobid_limit,
        owner=args.owner,
        lease_seconds=args.lease_seconds,
        max_seconds=args.max_seconds_per_stage,
        dry_run=args.dry_run,
    )

    out["stages"]["extract"] = process_extract_batch(
        limit=args.extract_limit,
        owner=args.owner,
        lease_seconds=args.lease_seconds,
        max_seconds=args.max_seconds_per_stage,
        dry_run=args.dry_run,
    )

    out["stages"]["enrich"] = process_enrich_batch(
        limit=args.enrich_limit,
        threshold=args.threshold,
        mailto=args.mailto,
        osf_id=args.osf_id,
        ref_id=args.ref_id,
        debug=args.debug,
        workers=getattr(args, "enrich_workers", 1),
        dry_run=args.dry_run,
    )

    out["stages"]["flora"] = process_flora_batch(
        limit_lookup=args.limit_lookup,
        limit_screen=args.limit_screen,
        osf_id=args.osf_id,
        ref_id=args.ref_id,
        cache_ttl_hours=args.cache_ttl_hours,
        persist_flags=not args.no_persist,
        only_unchecked=not args.include_checked,
        debug=args.debug,
        dry_run=args.dry_run,
    )

    if not args.skip_author:
        out["stages"]["author"] = process_author_batch(
            osf_ids=args.author_osf_ids,
            ids_file=args.ids_file,
            limit=args.author_limit,
            out=args.out,
            pdf_root=args.pdf_root,
            keep_files=not args.cleanup_author_files,
            debug=args.debug,
            debug_log=args.debug_log,
            match_emails_file=args.match_emails_file,
            match_emails_threshold=args.match_emails_threshold,
            include_existing=args.include_existing,
            write_debug_csv=args.write_debug_csv,
            orcid_workers=getattr(args, "orcid_workers", 1),
            dry_run=args.dry_run,
        )


    from .email.inbox import process_validation_responses
    out["stages"]["inbox"] = process_validation_responses(
        max_messages=50,
        dry_run=args.dry_run,
    )
    if not args.skip_randomization:
        out["stages"]["author-randomize"] = process_author_randomization_batch(
            authors_csv=getattr(args, "authors_csv", "osf_sync/extraction/authorList_ext.csv"),
            limit_preprints=args.randomize_limit,
            seed=getattr(args, "seed", None),
            network_state_key=getattr(args, "network_state_key", "trial:author_network_state"),
            dry_run=args.dry_run,
        )

    if include_email and email_limit > 0 and email_enabled:
        spread = int(email_limit * 270)  # 45 min / 10 = 4.5 min = 270s per msg
        out["stages"]["email"] = process_email_batch(
            limit=email_limit,
            max_seconds=spread + 60,
            spread_seconds=spread,
            dry_run=args.dry_run,
        )
    elif include_email and email_limit > 0 and not email_enabled:
        logger.warning("Email stage requested in run-all but disabled (PIPELINE_ENABLE_EMAIL_STAGE is false)")
        out["stages"]["email"] = _disabled_email_result(dry_run=args.dry_run)

    if not args.dry_run:
        out["excluded_preprints_summary"] = _excluded_summary()

    _notify_pipeline_summary(out)
    return out


def _merge_stage_results(accumulated: Dict[str, Any], batch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge a new stage batch result into accumulated totals."""
    if not accumulated:
        return dict(batch)
    for key in ("selected", "claimed", "processed", "failed", "skipped_claimed"):
        accumulated[key] = accumulated.get(key, 0) + batch.get(key, 0)
    accumulated["limit_reached"] = bool(accumulated.get("limit_reached", False) or batch.get("limit_reached", False))
    accumulated["stopped_due_to_time"] = batch.get("stopped_due_to_time", False)
    return accumulated


def run_grobid_stages(args: argparse.Namespace) -> Dict[str, Any]:
    """Run sync then interleaved pdf+grobid batches (used by the GROBID workflow).

    Instead of downloading all PDFs then running GROBID, this interleaves
    small batches: download N → GROBID N → download N → ... so that on
    ephemeral storage (GitHub Actions) we don't waste downloads that GROBID
    can't reach before the job ends.
    """
    out: Dict[str, Any] = {"stages": {}}

    # Overall deadline so we exit gracefully before the GH Actions job timeout.
    overall_max = getattr(args, "max_seconds", None)
    overall_deadline = _deadline(overall_max)

    sync_budget = getattr(args, "max_seconds_sync", 1200)
    out["stages"]["sync"] = sync_from_osf(
        subject_text=args.subject,
        batch_size=args.batch_size,
        limit=args.sync_limit,
        max_seconds=sync_budget,
        dry_run=args.dry_run,
    )

    # Interleave pdf + grobid in small batches.
    batch_size = getattr(args, "interleave_batch", 50)
    workers = getattr(args, "download_workers", 1)

    pdf_total = 0
    grobid_total = 0
    pdf_result: Dict[str, Any] = {}
    grobid_result: Dict[str, Any] = {}
    pdf_exhausted = False
    grobid_idle_rounds = 0

    while True:
        if _time_up(overall_deadline):
            logger.info("interleave loop: overall deadline reached")
            break

        pdf_remaining = args.pdf_limit - pdf_total
        grobid_remaining = args.grobid_limit - grobid_total
        if pdf_exhausted:
            pdf_remaining = 0
        if pdf_remaining <= 0 and grobid_remaining <= 0:
            break

        # -- download a small batch of PDFs --
        pdf_did_work = False
        if pdf_remaining > 0:
            chunk = min(batch_size, pdf_remaining)
            batch_result = process_pdf_batch(
                limit=chunk,
                owner=args.owner,
                lease_seconds=args.lease_seconds,
                workers=workers,
                dry_run=args.dry_run,
            )
            pdf_total += batch_result.get("processed", 0) + batch_result.get("failed", 0)
            pdf_result = _merge_stage_results(pdf_result, batch_result)
            pdf_did_work = (batch_result.get("processed", 0) + batch_result.get("failed", 0)) > 0
            # Nothing left to download — stop downloading in future iterations
            if not pdf_did_work and batch_result.get("skipped_claimed", 0) == 0:
                pdf_exhausted = True

        # -- GROBID-process a small batch --
        grobid_did_work = False
        if grobid_remaining > 0:
            chunk = min(batch_size, grobid_remaining)
            batch_result = process_grobid_batch(
                limit=chunk,
                owner=args.owner,
                lease_seconds=args.lease_seconds,
                dry_run=args.dry_run,
            )
            grobid_total += batch_result.get("processed", 0) + batch_result.get("failed", 0)
            grobid_result = _merge_stage_results(grobid_result, batch_result)
            grobid_did_work = (batch_result.get("processed", 0) + batch_result.get("failed", 0)) > 0
            if grobid_did_work:
                grobid_idle_rounds = 0
            else:
                grobid_idle_rounds += 1

        # Stop only when both stages have no work.
        # Allow a few idle GROBID rounds for GSI eventual consistency after PDF downloads.
        if pdf_exhausted and not grobid_did_work and grobid_idle_rounds > 2:
            logger.info("interleave loop: both PDF and GROBID exhausted")
            break
        if not pdf_did_work and not grobid_did_work and pdf_exhausted:
            logger.info("interleave loop: no progress in this round, stopping")
            break

    timed_out = _time_up(overall_deadline)

    out["stages"]["pdf"] = pdf_result or {
        "stage": "pdf", "processed": 0, "failed": 0, "limit_reached": False, "dry_run": args.dry_run,
    }
    out["stages"]["grobid"] = grobid_result or {
        "stage": "grobid", "processed": 0, "failed": 0, "limit_reached": False, "dry_run": args.dry_run,
    }
    if timed_out:
        for stage_data in out["stages"].values():
            stage_data["stopped_due_to_time"] = True

    _notify_pipeline_summary(out)
    return out


def run_post_grobid(args: argparse.Namespace) -> Dict[str, Any]:
    """Run extract + enrich + flora + author + randomize + email."""
    out: Dict[str, Any] = {"stages": {}}

    include_email = getattr(args, "include_email", False)
    email_limit = getattr(args, "email_limit", 50)
    email_enabled = _email_stage_enabled()

    out["stages"]["extract"] = process_extract_batch(
        limit=args.extract_limit,
        owner=args.owner,
        lease_seconds=args.lease_seconds,
        max_seconds=args.max_seconds_per_stage,
        dry_run=args.dry_run,
    )

    out["stages"]["enrich"] = process_enrich_batch(
        limit=args.enrich_limit,
        threshold=args.threshold,
        mailto=args.mailto,
        osf_id=args.osf_id,
        ref_id=args.ref_id,
        debug=args.debug,
        workers=getattr(args, "enrich_workers", 1),
        dry_run=args.dry_run,
    )

    out["stages"]["flora"] = process_flora_batch(
        limit_lookup=args.limit_lookup,
        limit_screen=args.limit_screen,
        osf_id=args.osf_id,
        ref_id=args.ref_id,
        cache_ttl_hours=args.cache_ttl_hours,
        persist_flags=not args.no_persist,
        only_unchecked=not args.include_checked,
        debug=args.debug,
        dry_run=args.dry_run,
    )

    if not args.skip_author:
        out["stages"]["author"] = process_author_batch(
            osf_ids=args.author_osf_ids,
            ids_file=args.ids_file,
            limit=args.author_limit,
            out=args.out,
            pdf_root=args.pdf_root,
            keep_files=not args.cleanup_author_files,
            debug=args.debug,
            debug_log=args.debug_log,
            match_emails_file=args.match_emails_file,
            match_emails_threshold=args.match_emails_threshold,
            include_existing=args.include_existing,
            write_debug_csv=args.write_debug_csv,
            orcid_workers=getattr(args, "orcid_workers", 1),
            dry_run=args.dry_run,
        )


    from .email.inbox import process_validation_responses
    out["stages"]["inbox"] = process_validation_responses(
        max_messages=50,
        dry_run=args.dry_run,
    )
    if not args.skip_randomization:
        out["stages"]["author-randomize"] = process_author_randomization_batch(
            authors_csv=getattr(args, "authors_csv", "osf_sync/extraction/authorList_ext.csv"),
            limit_preprints=args.randomize_limit,
            seed=getattr(args, "seed", None),
            network_state_key=getattr(args, "network_state_key", "trial:author_network_state"),
            dry_run=args.dry_run,
        )

    if include_email and email_limit > 0 and email_enabled:
        spread = int(email_limit * 270)
        out["stages"]["email"] = process_email_batch(
            limit=email_limit,
            max_seconds=spread + 60,
            spread_seconds=spread,
            dry_run=args.dry_run,
        )
    elif include_email and email_limit > 0 and not email_enabled:
        logger.warning("Email stage requested in run-post-grobid but disabled (PIPELINE_ENABLE_EMAIL_STAGE is false)")
        out["stages"]["email"] = _disabled_email_result(dry_run=args.dry_run)

    if not args.dry_run:
        out["excluded_preprints_summary"] = _excluded_summary()

    _notify_pipeline_summary(out)
    return out


def _notify_pipeline_summary(result: Dict[str, Any]) -> None:
    """Send a short email summary of the pipeline run."""
    if not PIPELINE_NOTIFY_EMAIL:
        return
    if not getattr(RUNTIME_CONFIG.email, "progress_emails", True):
        return
    try:
        from .email.gmail import send_email

        stages = result.get("stages", {})
        td = 'style="padding:4px 12px;text-align:right"'
        tdl = 'style="padding:4px 12px;text-align:left"'

        # --- identified row (sync stage, when present) ---
        sync_data = stages.get("sync")
        identified_row = ""
        if sync_data is not None:
            identified = sync_data.get("upserted", sync_data.get("processed", 0))
            sync_timed = " (timed out)" if sync_data.get("stopped_due_to_time") else ""
            identified_row = (
                f'<tr style="font-weight:bold;border-bottom:1px solid #ccc">'
                f'<td {tdl}>Preprints identified (OSF sync)</td>'
                f'<td {td}>{identified}</td><td {td}>—</td>'
                f'<td {tdl}>{sync_timed.strip()}</td></tr>'
            )

        # --- downstream stage rows ---
        rows = []
        any_failed = False
        for name, data in stages.items():
            if name == "sync":
                continue
            p = data.get("processed", 0)
            f = data.get("failed", 0)
            timed = " (timed out)" if data.get("stopped_due_to_time") else ""
            if f:
                any_failed = True
            rows.append(f"<tr><td {tdl}>{name}</td><td {td}>{p}</td><td {td}>{f}</td><td {tdl}>{timed.strip()}</td></tr>")

        is_dry_run = any(data.get("dry_run") for data in stages.values())
        status = "DRY RUN" if is_dry_run else ("with errors" if any_failed else "OK")
        table = (
            '<table style="border-collapse:collapse;font-family:monospace;font-size:14px">'
            '<tr style="border-bottom:2px solid #333">'
            f'<th {tdl}>Stage</th>'
            f'<th {td}>Processed</th><th {td}>Failed</th>'
            f'<th {tdl}>Note</th></tr>'
            + identified_row
            + "".join(rows)
            + "</table>"
        )

        # --- exclusion breakdown section ---
        excl = result.get("excluded_preprints_summary", {})
        excl_total = excl.get("total_excluded_preprints")
        excl_section = ""
        if excl_total is not None:
            by_reason = excl.get("by_reason") or {}
            reason_rows = "".join(
                f'<tr><td {tdl} style="padding-left:24px">− {reason}</td><td {td}>{count}</td></tr>'
                for reason, count in sorted(by_reason.items(), key=lambda x: -x[1])
            )
            excl_section = (
                "<h3 style='font-family:monospace;margin-top:16px'>Excluded preprints (cumulative)</h3>"
                '<table style="border-collapse:collapse;font-family:monospace;font-size:14px">'
                f'<tr style="font-weight:bold"><td {tdl}>Total excluded</td><td {td}>{excl_total}</td></tr>'
                + reason_rows
                + "</table>"
            )

        dry_note = "<p><em>This was a dry run — no data was written.</em></p>" if is_dry_run else ""
        html = f"<p>Pipeline run completed <strong>{status}</strong>.</p>{dry_note}{table}{excl_section}"
        send_email(PIPELINE_NOTIFY_EMAIL, f"FLoRA pipeline: {status}", html)
        logger.info("Pipeline summary email sent", extra={"to": PIPELINE_NOTIFY_EMAIL})
    except Exception:
        logger.warning("Failed to send pipeline summary email", exc_info=True)


def _add_output_json_arg(parser: argparse.ArgumentParser) -> None:
    """Add --output-json to a subparser so the result is written to a file."""
    parser.add_argument(
        "--output-json",
        default=None,
        metavar="PATH",
        help="Write JSON result to this file (avoids stdout contamination)",
    )


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Arguments shared by all multi-stage subcommands."""
    parser.add_argument("--max-seconds-per-stage", type=int, default=None)
    parser.add_argument("--owner", default=None)
    parser.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    _add_output_json_arg(parser)


def _add_grobid_args(parser: argparse.ArgumentParser) -> None:
    """Arguments for sync/pdf/grobid stages."""
    parser.add_argument("--sync-limit", type=int, default=1000)
    parser.add_argument("--pdf-limit", type=int, default=100)
    parser.add_argument("--grobid-limit", type=int, default=50)
    parser.add_argument("--download-workers", type=int, default=1, help="Parallel workers for PDF downloads")
    parser.add_argument("--interleave-batch", type=int, default=50, help="PDF+GROBID batch size per interleave round")
    parser.add_argument("--max-seconds-sync", type=int, default=1200, help="Timeout for OSF sync stage (default 20min)")
    parser.add_argument("--subject", default=None)
    parser.add_argument("--batch-size", type=int, default=100)


def _add_downstream_args(parser: argparse.ArgumentParser) -> None:
    """Arguments for extract/enrich/flora/author/email stages."""
    parser.add_argument("--extract-limit", type=int, default=200)
    parser.add_argument("--enrich-limit", type=int, default=300, help="Max preprints for DOI enrichment")
    parser.add_argument("--author-limit", type=int, default=None)
    parser.add_argument("--randomize-limit", type=int, default=None, help="Optional cap for author randomization")
    parser.add_argument("--limit-lookup", type=int, default=0)
    parser.add_argument("--limit-screen", type=int, default=0)
    parser.add_argument("--enrich-workers", type=int, default=1, help="Parallel workers for Crossref/OpenAlex enrichment")
    parser.add_argument("--orcid-workers", type=int, default=3, help="Parallel workers for ORCID lookups")
    parser.add_argument("--skip-author", action="store_true", help="Skip author extraction stage")
    parser.add_argument("--skip-randomization", action="store_true", help="Skip author randomization stage")
    parser.add_argument("--include-email", action="store_true", help="Include email sending stage (off by default)")
    parser.add_argument("--email-limit", type=int, default=50, help="Max emails to send")
    parser.add_argument("--threshold", type=int, default=None)
    parser.add_argument("--mailto", default=OPENALEX_EMAIL)
    parser.add_argument("--osf-id", default=None, help="Restrict enrich/FLORA to a specific OSF id")
    parser.add_argument("--author-osf-id", action="append", dest="author_osf_ids", default=[])
    parser.add_argument("--ids-file", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--pdf-root", default=None)
    parser.add_argument(
        "--cleanup-author-files",
        action="store_true",
        help="Allow author stage to delete local PDF/TEI files after processing (default is keep files)",
    )
    parser.add_argument("--debug-log", default=None)
    parser.add_argument("--match-emails-file", default=None)
    parser.add_argument("--match-emails-threshold", type=float, default=0.90)
    parser.add_argument("--include-existing", action="store_true")
    parser.add_argument("--write-debug-csv", action="store_true")
    parser.add_argument("--authors-csv", default="osf_sync/extraction/authorList_ext.csv")
    parser.add_argument("--network-state-key", default="trial:author_network_state")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--ref-id", default=None)
    parser.add_argument("--cache-ttl-hours", type=int, default=None)
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument("--include-checked", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run OSF pipeline stages without Celery")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run a single pipeline stage")
    p_run.add_argument("--stage", required=True, choices=["sync", "pdf", "grobid", "extract", "enrich", "flora", "author", "author-randomize", "email", "inbox"])
    p_run.add_argument("--limit", type=int, default=None, help="Stage limit (for enrich: max preprints)")
    p_run.add_argument("--max-seconds", type=int, default=None)
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--download-workers", type=int, default=1, help="Parallel workers for PDF downloads")
    p_run.add_argument("--enrich-workers", type=int, default=1, help="Parallel workers for Crossref/OpenAlex enrichment")
    p_run.add_argument("--orcid-workers", type=int, default=3, help="Parallel workers for ORCID lookups")
    p_run.add_argument("--debug", action="store_true")
    p_run.add_argument("--batch-size", type=int, default=1000)
    p_run.add_argument("--subject", default=None)
    p_run.add_argument("--owner", default=None)
    p_run.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    p_run.add_argument("--threshold", type=int, default=None)
    p_run.add_argument("--mailto", default=OPENALEX_EMAIL)
    p_run.add_argument("--osf-id", default=None, help="Restrict enrich/FLORA to a specific OSF id")
    p_run.add_argument("--author-osf-id", action="append", dest="author_osf_ids", default=[])
    p_run.add_argument("--authors-csv", default="osf_sync/extraction/authorList_ext.csv")
    p_run.add_argument("--network-state-key", default="trial:author_network_state")
    p_run.add_argument("--seed", type=int, default=None)
    p_run.add_argument("--ids-file", default=None)
    p_run.add_argument("--out", default=None)
    p_run.add_argument("--pdf-root", default=None)
    p_run.add_argument("--keep-files", action="store_true")
    p_run.add_argument("--debug-log", default=None)
    p_run.add_argument("--match-emails-file", default=None)
    p_run.add_argument("--match-emails-threshold", type=float, default=0.90)
    p_run.add_argument("--include-existing", action="store_true")
    p_run.add_argument("--write-debug-csv", action="store_true")
    p_run.add_argument("--ref-id", default=None)
    p_run.add_argument("--limit-lookup", type=int, default=0)
    p_run.add_argument("--limit-screen", type=int, default=0)
    p_run.add_argument("--cache-ttl-hours", type=int, default=None)
    p_run.add_argument("--no-persist", action="store_true")
    p_run.add_argument("--include-checked", action="store_true")
    p_run.add_argument("--spread-seconds", type=int, default=None, help="Spread email sends over this many seconds (email stage only)")
    _add_output_json_arg(p_run)
    p_run.set_defaults(func=run_stage)

    # -- run-all: full pipeline (sync → pdf → grobid → extract → enrich → flora → author → randomize → email)
    p_all = sub.add_parser("run-all", help="Run the full pipeline (bounded per stage)")
    _add_common_args(p_all)
    _add_grobid_args(p_all)
    _add_downstream_args(p_all)
    p_all.set_defaults(func=run_all)

    # -- run-grobid-stages: sync + pdf + grobid only
    p_grobid = sub.add_parser("run-grobid-stages", help="Run sync + pdf + grobid stages (GROBID workflow)")
    _add_common_args(p_grobid)
    _add_grobid_args(p_grobid)
    p_grobid.add_argument("--max-seconds", type=int, default=None, help="Overall time limit (exits gracefully before GH Actions timeout)")
    p_grobid.set_defaults(func=run_grobid_stages)

    # -- run-post-grobid: extract + enrich + flora + author + randomize + email
    p_post = sub.add_parser("run-post-grobid", help="Run extract + enrich + flora + author + randomize + email")
    _add_common_args(p_post)
    _add_downstream_args(p_post)
    p_post.set_defaults(func=run_post_grobid)

    p_sync_date = sub.add_parser("sync-from-date", help="Ad-hoc ingestion from a specific date")
    p_sync_date.add_argument("--start", required=True)
    p_sync_date.add_argument("--subject", default=None)
    p_sync_date.add_argument("--batch-size", type=int, default=1000)
    p_sync_date.add_argument("--limit", type=int, default=None)
    p_sync_date.add_argument("--max-seconds", type=int, default=None)
    p_sync_date.add_argument("--dry-run", action="store_true")
    p_sync_date.set_defaults(
        func=lambda args: sync_from_date_to_now(
            start_date=args.start,
            subject_text=args.subject,
            batch_size=args.batch_size,
            limit=args.limit,
            max_seconds=args.max_seconds,
            dry_run=args.dry_run,
        )
    )

    p_fetch = sub.add_parser("fetch-one", help="Fetch a single preprint by OSF id or DOI")
    g = p_fetch.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", help="OSF preprint id")
    g.add_argument("--doi", help="DOI or https://doi.org/... link")
    p_fetch.add_argument("--metadata-only", action="store_true", help="Only upsert metadata")
    p_fetch.add_argument("--skip-extract", action="store_true", help="Skip TEI extraction even after GROBID")
    p_fetch.set_defaults(
        func=lambda args: sync_one_by_id(
            osf_id=args.id,
            run_pdf_and_grobid=not args.metadata_only,
            run_extract=not args.skip_extract,
        )
        if args.id
        else sync_one_by_doi(
            doi_or_url=args.doi,
            run_pdf_and_grobid=not args.metadata_only,
            run_extract=not args.skip_extract,
        )
    )

    p_rand = sub.add_parser(
        "author-randomize",
        help="Assign unassigned preprints via a Dynamo-backed author network (initialize or augment)",
    )
    p_rand.add_argument(
        "--authors-csv",
        default="osf_sync/extraction/authorList_ext.csv",
        help="Optional enriched author CSV used for identity resolution (fallbacks to TEI/raw)",
    )
    p_rand.add_argument(
        "--limit-preprints",
        type=int,
        default=None,
        help="Optional cap on unassigned preprints processed in this run",
    )
    p_rand.add_argument("--seed", type=int, default=None, help="Explicit seed override")
    p_rand.add_argument(
        "--network-state-key",
        default="trial:author_network_state",
        help="sync_state key storing network metadata (x-threshold, next ids, seed chain)",
    )
    p_rand.add_argument("--dry-run", action="store_true")
    p_rand.set_defaults(
        func=lambda args: run_author_randomization(
            authors_csv=args.authors_csv,
            limit_preprints=args.limit_preprints,
            seed=args.seed,
            network_state_key=args.network_state_key,
            dry_run=args.dry_run,
        )
    )

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if getattr(args, "debug", False):
        logger.setLevel(logging.DEBUG)

    init_db()
    out = args.func(args)
    json_str = json.dumps(out, ensure_ascii=False, indent=2, default=str)
    output_path = getattr(args, "output_json", None)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(json_str)
    print(json_str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
