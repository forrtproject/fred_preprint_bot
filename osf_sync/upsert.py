from __future__ import annotations
from typing import Iterable, Dict, List, Optional, Any, Tuple
import calendar
import datetime as dt
import logging

from .dynamo.preprints_repo import PreprintsRepo
from .exclusion_logging import log_preprint_exclusion
from .runtime_config import RUNTIME_CONFIG

log = logging.getLogger(__name__)


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


def _parse_anchor_dt() -> Optional[dt.datetime]:
    raw = RUNTIME_CONFIG.ingest.anchor_date
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if len(raw) == 10:
        try:
            d = dt.date.fromisoformat(raw)
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        except Exception:
            log.warning("Invalid ingest.anchor_date value", extra={"value": raw})
            return None
    parsed = _parse_iso_dt(raw)
    if not parsed:
        log.warning("Invalid ingest.anchor_date value", extra={"value": raw})
    return parsed


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


def _effective_created_dt(attrs: Dict[str, Any]) -> Optional[dt.datetime]:
    return _parse_iso_dt(attrs.get("date_created") or attrs.get("date_published"))


def _within_anchor_window(ts_dt: Optional[dt.datetime], anchor_dt: Optional[dt.datetime]) -> bool:
    if anchor_dt is None:
        return True
    if ts_dt is None:
        return False
    raw_window_months = getattr(RUNTIME_CONFIG.ingest, "window_months", 6)
    try:
        window_months = int(raw_window_months)
    except Exception:
        window_months = 6
    anchor_date = anchor_dt.date()
    window_start = _subtract_months(anchor_date, window_months)
    window_end = _add_months(anchor_date, window_months)
    ts_date = ts_dt.date()
    return window_start <= ts_date <= window_end


def _extract_provider_id(obj: Dict[str, Any]) -> Optional[str]:
    try:
        return obj["relationships"]["provider"]["data"]["id"]
    except (KeyError, TypeError):
        return None


def _contains_osf_link(value: Any) -> bool:
    if isinstance(value, str):
        return "osf.io" in value
    if isinstance(value, dict):
        return any(_contains_osf_link(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_osf_link(v) for v in value)
    return False


def _passes_link_rule(links: Optional[Dict[str, Any]]) -> bool:
    if not links:
        return True
    doi_val = links.get("doi")
    if doi_val:
        # Require the DOI link itself to be an OSF or Zenodo link.
        return _contains_osf_link(doi_val) or _contains_zenodo_link(doi_val)
    return True


def _contains_zenodo_link(value: Any) -> bool:
    if isinstance(value, str):
        v = value.lower()
        return ("zenodo.org" in v) or ("doi.org/10.5281/zenodo" in v) or v.startswith("10.5281/zenodo")
    if isinstance(value, dict):
        return any(_contains_zenodo_link(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_zenodo_link(v) for v in value)
    return False


def _filter_ingest_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int, int, int, int, List[Dict[str, Any]]]:
    anchor_dt = _parse_anchor_dt()
    excluded_providers = RUNTIME_CONFIG.ingest.excluded_providers
    kept: List[Dict[str, Any]] = []
    skipped_records: List[Dict[str, Any]] = []
    skipped_provider = 0
    skipped_date = 0
    skipped_links = 0
    skipped_version = 0
    for obj in rows:
        attrs = obj.get("attributes") or {}
        if attrs.get("is_latest_version") is False:
            skipped_version += 1
            skipped_records.append({"osf_id": obj.get("id"), "reason": "not_latest_version"})
            continue
        provider_id = _extract_provider_id(obj)
        if provider_id and provider_id.lower() in excluded_providers:
            skipped_provider += 1
            skipped_records.append(
                {"osf_id": obj.get("id"), "reason": "excluded_provider", "provider_id": provider_id}
            )
            continue
        created_dt = _effective_created_dt(attrs)
        if not _within_anchor_window(created_dt, anchor_dt):
            skipped_date += 1
            skipped_records.append(
                {"osf_id": obj.get("id"), "reason": "date_window", "created_dt": attrs.get("date_created")}
            )
            continue
        if not _passes_link_rule(obj.get("links") or {}):
            skipped_links += 1
            skipped_records.append(
                {"osf_id": obj.get("id"), "reason": "links_doi_not_osf_or_zenodo", "links": obj.get("links")}
            )
            continue
        kept.append(obj)
    return kept, skipped_provider, skipped_date, skipped_links, skipped_version, skipped_records


def upsert_batch(objs: Iterable[Dict]) -> int:
    rows = list(objs)
    if not rows:
        return 0
    filtered, skipped_provider, skipped_date, skipped_links, skipped_version, skipped_records = _filter_ingest_rows(rows)
    if skipped_provider or skipped_date or skipped_links or skipped_version:
        log.info(
            "ingest filter skipped rows",
            extra={
                "skipped_provider": skipped_provider,
                "skipped_date": skipped_date,
                "skipped_links": skipped_links,
                "skipped_version": skipped_version,
                "incoming": len(rows),
                "kept": len(filtered),
                "anchor_date": RUNTIME_CONFIG.ingest.anchor_date,
            },
        )
        for rec in skipped_records:
            osf_id = rec.get("osf_id")
            reason = rec.get("reason")
            log.info("ingest filter skip osf_id=%s reason=%s", osf_id, reason, extra=rec)
            if reason == "excluded_provider":
                exclusion_reason = "ingest_excluded_provider"
            elif reason == "date_window":
                exclusion_reason = "ingest_date_window"
            elif reason == "links_doi_not_osf_or_zenodo":
                exclusion_reason = "ingest_links_doi_not_osf_or_zenodo"
            else:
                exclusion_reason = f"ingest_{reason}"
            log_preprint_exclusion(
                reason=exclusion_reason,
                osf_id=osf_id,
                stage="sync",
                details={"raw_reason": reason},
            )
    if not filtered:
        return 0
    repo = PreprintsRepo()
    return repo.upsert_preprints(filtered)
