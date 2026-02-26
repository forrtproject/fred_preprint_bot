from __future__ import annotations

import csv
import datetime as dt
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from ..dynamo.preprints_repo import PreprintsRepo
from ..logging_setup import get_logger, with_extras
from ..runtime_config import RUNTIME_CONFIG

logger = get_logger(__name__)

FLORA_CSV_URL = RUNTIME_CONFIG.flora.csv_url
FLORA_CSV_PATH_DEFAULT = RUNTIME_CONFIG.flora.csv_path


def _info(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).info(msg)
    else:
        logger.info(msg)


def _warn(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).warning(msg)
    else:
        logger.warning(msg)


def _exception(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).exception(msg)
    else:
        logger.exception(msg)


_DOI_PATTERN = re.compile(r"10\.\S+", re.IGNORECASE)
_ALLOWED_OUTCOMES = {"successful", "failed", "mixed"}
_OUTCOME_ALIASES = {
    "success": "successful",
    "successful": "successful",
    "failure": "failed",
    "failed": "failed",
    "mixed": "mixed",
}


def normalize_doi(doi: Optional[str]) -> Optional[str]:
    """
    Normalize DOI to lowercase and strip URL prefixes; return None if invalid.
    """
    if not doi or not isinstance(doi, str):
        return None
    v = doi.strip().lower()
    v = re.sub(r"^https?://(dx\.)?doi\.org/", "", v)
    m = _DOI_PATTERN.search(v)
    return m.group(0) if m else None


def _normalize_field_name(name: Optional[str]) -> str:
    return (name or "").replace("\ufeff", "").strip().strip('"').strip()


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


def _normalize_outcome(value: Any) -> Optional[str]:
    txt = _clean_text(value)
    if not txt:
        return None
    normalized = _OUTCOME_ALIASES.get(txt.lower())
    if normalized in _ALLOWED_OUTCOMES:
        return normalized
    return None


def _resolve_flora_csv_path(cache_path: Optional[str]) -> Path:
    raw = cache_path or FLORA_CSV_PATH_DEFAULT
    return Path(raw).expanduser()


def _is_file_from_today(path: Path) -> bool:
    try:
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime)
    except FileNotFoundError:
        return False
    except Exception:
        return False
    return mtime.date() >= dt.date.today()


def _download_flora_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with requests.get(FLORA_CSV_URL, stream=True, timeout=(20, 180)) as resp:
            resp.raise_for_status()
            with tmp_path.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _ensure_fresh_flora_csv(path: Path, *, debug: bool = False) -> Dict[str, Any]:
    if _is_file_from_today(path):
        return {"downloaded": False, "used_stale": False}

    try:
        _download_flora_csv(path)
        if debug:
            _info("Downloaded fresh FLORA CSV", path=str(path), source=FLORA_CSV_URL)
        return {"downloaded": True, "used_stale": False}
    except Exception as exc:
        if path.exists():
            _warn(
                "Failed to refresh FLORA CSV; using existing local copy",
                path=str(path),
                error=str(exc),
            )
            return {"downloaded": False, "used_stale": True}
        raise RuntimeError(f"Unable to download FLORA CSV to {path}") from exc


def _load_flora_pairs_by_original(path: Path) -> Dict[str, List[Dict[str, Optional[str]]]]:
    pairs_by_original: Dict[str, List[Dict[str, Optional[str]]]] = {}
    seen_by_original: Dict[str, set] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return pairs_by_original
        field_map = {_normalize_field_name(name): name for name in reader.fieldnames}

        def _row_value(row: Dict[str, Any], key: str) -> Optional[str]:
            return _clean_text(row.get(field_map.get(key, key)))

        for row in reader:
            doi_o = normalize_doi(_row_value(row, "doi_o"))
            if not doi_o:
                continue
            outcome = _normalize_outcome(
                _row_value(row, "outcome")
                or _row_value(row, "outcome_r")
                or _row_value(row, "replication_outcome")
            )
            # Protocol-aligned outcomes are mandatory for replication rows.
            if outcome is None:
                continue
            rec = {
                "doi_o": doi_o,
                "doi_r": normalize_doi(_row_value(row, "doi_r")),
                "apa_ref_o": _row_value(row, "apa_ref_o"),
                "apa_ref_r": _row_value(row, "apa_ref_r"),
                "replication_outcome": outcome,
                "oa_url_r": _row_value(row, "oa_url_r"),
            }
            key = (
                rec["doi_o"],
                rec["doi_r"],
                rec["apa_ref_o"],
                rec["apa_ref_r"],
                rec["replication_outcome"],
            )
            seen = seen_by_original.setdefault(doi_o, set())
            if key in seen:
                continue
            seen.add(key)
            pairs_by_original.setdefault(doi_o, []).append(rec)

    return pairs_by_original


def lookup_originals_with_flora(
    *,
    limit: int = 200,
    osf_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    only_unchecked: bool = True,
    cache_path: Optional[str] = None,
    cache_ttl_hours: Optional[int] = None,
    ignore_cache: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Populate FLORA original/replication pairs from local flora.csv data.
    The file is refreshed once per day from FLORA's public CSV source.
    """
    repo = PreprintsRepo()
    rows = repo.select_refs_with_doi(limit=limit, osf_id=osf_id, ref_id=ref_id, only_unchecked=only_unchecked)

    candidate_ids = sorted({(r or {}).get("osf_id") for r in rows if (r or {}).get("osf_id")})
    allowed_ids = repo.filter_osf_ids_without_sent_email(candidate_ids)
    filtered_rows = [r for r in rows if (r or {}).get("osf_id") in allowed_ids]
    skipped_sent_preprint = len(rows) - len(filtered_rows)
    rows = filtered_rows

    if cache_ttl_hours is not None:
        _warn("cache_ttl_hours is ignored for local FLORA CSV lookup", cache_ttl_hours=cache_ttl_hours)
    if ignore_cache:
        _warn("ignore_cache is ignored for local FLORA CSV lookup")

    flora_path = _resolve_flora_csv_path(cache_path)
    refresh_meta = _ensure_fresh_flora_csv(flora_path, debug=debug)
    flora_pairs = _load_flora_pairs_by_original(flora_path)

    stats: Dict[str, Any] = {
        "checked": 0,
        "updated": 0,
        "failed": 0,
        "skipped_sent_preprint": skipped_sent_preprint,
        "cache_hits": 0,
        "csv_downloaded": 1 if refresh_meta.get("downloaded") else 0,
    }
    processed_osf_ids: set[str] = set()

    for r in rows:
        osfid = r.get("osf_id")
        refid = r.get("ref_id")
        doi = normalize_doi(r.get("doi"))
        if not doi:
            continue

        stats["checked"] += 1
        if osfid:
            processed_osf_ids.add(osfid)
        ref_pairs = flora_pairs.get(doi) or []
        status = bool(ref_pairs)
        try:
            repo.update_reference_flora(
                osfid,
                refid,
                status=status,
                ref_pairs=ref_pairs,
            )
            stats["updated"] += 1
        except Exception:
            stats["failed"] += 1
            _exception(
                "Failed to update FLORA lookup result",
                osf_id=osfid,
                ref_id=refid,
                match_found=status,
            )

    stats["processed_preprints"] = len(processed_osf_ids)
    stats["processed_osf_ids"] = sorted(processed_osf_ids)
    return stats


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Lookup originals via local FLORA CSV for references that already have DOIs."
    )
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--osf_id", default=None)
    ap.add_argument("--ref_id", default=None)
    ap.add_argument("--no-only-unchecked", action="store_true", help="Process all DOI rows even if already checked.")
    ap.add_argument(
        "--cache-path",
        default=None,
        help="Override FLORA CSV path (defaults to flora.csv_path in config/runtime.toml).",
    )
    ap.add_argument(
        "--cache-ttl-hours",
        type=int,
        default=None,
        help="Deprecated; ignored when using local FLORA CSV lookup.",
    )
    ap.add_argument(
        "--ignore-cache",
        action="store_true",
        help="Deprecated; ignored when using local FLORA CSV lookup.",
    )
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out = lookup_originals_with_flora(
        limit=args.limit,
        osf_id=args.osf_id,
        ref_id=args.ref_id,
        only_unchecked=not args.no_only_unchecked,
        cache_path=args.cache_path,
        cache_ttl_hours=args.cache_ttl_hours,
        ignore_cache=args.ignore_cache,
        debug=args.debug,
    )
    print(out)
