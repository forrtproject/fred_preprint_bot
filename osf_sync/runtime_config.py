from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import logging
import tomllib

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "runtime.toml"


@dataclass(frozen=True)
class IngestConfig:
    anchor_date: Optional[str]
    window_months: int
    backfill_on_config_change: bool
    excluded_providers: tuple


@dataclass(frozen=True)
class FloraConfig:
    original_lookup_url: str
    cache_ttl_hours: int
    csv_url: str
    csv_path: str


@dataclass(frozen=True)
class EmailConfig:
    sender_address: str
    sender_display_name: str
    feedback_base_url: str
    report_base_url: str
    flora_learn_more_url: str
    progress_emails: bool


@dataclass(frozen=True)
class ValidationConfig:
    distance_threshold: float
    reviewer_email: str


@dataclass(frozen=True)
class RuntimeConfig:
    ingest: IngestConfig
    flora: FloraConfig
    email: EmailConfig
    validation: ValidationConfig


def _default_config() -> RuntimeConfig:
    return RuntimeConfig(
        ingest=IngestConfig(anchor_date=None, window_months=6, backfill_on_config_change=True, excluded_providers=("thesiscommons",)),
        flora=FloraConfig(
            original_lookup_url="https://rep-api.forrt.org/v1/original-lookup",
            cache_ttl_hours=48,
            csv_url="https://github.com/forrtproject/FReD-data/raw/refs/heads/main/output/flora_filtered.csv",
            csv_path="data/flora.csv",
        ),
        email=EmailConfig(
            sender_address="flora@replications.forrt.org",
            sender_display_name="FLoRA at FORRT",
            feedback_base_url="https://forrt.org/flora-notify/feedback",
            report_base_url="https://forrt.org/flora-notify/report",
            flora_learn_more_url="https://forrt.org/flora/",
            progress_emails=True,
        ),
        validation=ValidationConfig(
            distance_threshold=0.29,
            reviewer_email="",
        ),
    )


def _safe_int(value: Any, fallback: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


def _safe_bool(value: Any, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return fallback


def load_runtime_config(config_path: Optional[Path] = None) -> RuntimeConfig:
    cfg = _default_config()
    path = config_path or _DEFAULT_CONFIG_PATH
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError:
        logger.warning("Runtime config file not found; using defaults", extra={"path": str(path)})
        return cfg
    except tomllib.TOMLDecodeError:
        logger.exception("Runtime config parse failed; using defaults", extra={"path": str(path)})
        return cfg
    except Exception:
        logger.exception("Runtime config load failed; using defaults", extra={"path": str(path)})
        return cfg

    ingest_raw = raw.get("ingest") if isinstance(raw, dict) else {}
    flora_raw = raw.get("flora") if isinstance(raw, dict) else {}
    if not isinstance(ingest_raw, dict):
        ingest_raw = {}
    if not isinstance(flora_raw, dict):
        flora_raw = {}

    anchor_date = ingest_raw.get("anchor_date", cfg.ingest.anchor_date)
    if isinstance(anchor_date, str):
        anchor_date = anchor_date.strip() or None
    elif anchor_date is None:
        anchor_date = None
    else:
        anchor_date = str(anchor_date).strip() or None

    window_months = _safe_int(ingest_raw.get("window_months", cfg.ingest.window_months), cfg.ingest.window_months)
    backfill_on_config_change = _safe_bool(
        ingest_raw.get("backfill_on_config_change", cfg.ingest.backfill_on_config_change),
        cfg.ingest.backfill_on_config_change,
    )

    raw_excluded = ingest_raw.get("excluded_providers")
    if isinstance(raw_excluded, (list, tuple)):
        excluded_providers = tuple(str(p).strip().lower() for p in raw_excluded if str(p).strip())
    else:
        excluded_providers = cfg.ingest.excluded_providers

    original_lookup_url = flora_raw.get("original_lookup_url", cfg.flora.original_lookup_url)
    if not isinstance(original_lookup_url, str) or not original_lookup_url.strip():
        original_lookup_url = cfg.flora.original_lookup_url
    else:
        original_lookup_url = original_lookup_url.strip()

    cache_ttl_hours = _safe_int(flora_raw.get("cache_ttl_hours", cfg.flora.cache_ttl_hours), cfg.flora.cache_ttl_hours)
    csv_url = flora_raw.get("csv_url", cfg.flora.csv_url)
    if not isinstance(csv_url, str) or not csv_url.strip():
        csv_url = cfg.flora.csv_url
    else:
        csv_url = csv_url.strip()

    csv_path = flora_raw.get("csv_path", cfg.flora.csv_path)
    if not isinstance(csv_path, str) or not csv_path.strip():
        csv_path = cfg.flora.csv_path
    else:
        csv_path = csv_path.strip()

    email_raw = raw.get("email") if isinstance(raw, dict) else {}
    if not isinstance(email_raw, dict):
        email_raw = {}

    def _str_field(d: dict, key: str, default: str) -> str:
        val = d.get(key, default)
        if not isinstance(val, str) or not val.strip():
            return default
        return val.strip()

    email_sender = _str_field(email_raw, "sender_address", cfg.email.sender_address)
    email_display_name = _str_field(email_raw, "sender_display_name", cfg.email.sender_display_name)
    email_feedback = _str_field(email_raw, "feedback_base_url", cfg.email.feedback_base_url)
    email_report = _str_field(email_raw, "report_base_url", cfg.email.report_base_url)
    email_learn_more = _str_field(email_raw, "flora_learn_more_url", cfg.email.flora_learn_more_url)
    email_progress_emails = _safe_bool(email_raw.get("progress_emails", cfg.email.progress_emails), cfg.email.progress_emails)

    validation_raw = raw.get("validation") if isinstance(raw, dict) else {}
    if not isinstance(validation_raw, dict):
        validation_raw = {}
    try:
        dist_thresh = float(validation_raw.get("distance_threshold", cfg.validation.distance_threshold))
    except (TypeError, ValueError):
        dist_thresh = cfg.validation.distance_threshold
    reviewer_email = _str_field(validation_raw, "reviewer_email", cfg.validation.reviewer_email)

    return RuntimeConfig(
        ingest=IngestConfig(
            anchor_date=anchor_date,
            window_months=window_months,
            backfill_on_config_change=backfill_on_config_change,
            excluded_providers=excluded_providers,
        ),
        flora=FloraConfig(
            original_lookup_url=original_lookup_url,
            cache_ttl_hours=cache_ttl_hours,
            csv_url=csv_url,
            csv_path=csv_path,
        ),
        email=EmailConfig(
            sender_address=email_sender,
            sender_display_name=email_display_name,
            feedback_base_url=email_feedback,
            report_base_url=email_report,
            flora_learn_more_url=email_learn_more,
            progress_emails=email_progress_emails,
        ),
        validation=ValidationConfig(
            distance_threshold=dist_thresh,
            reviewer_email=reviewer_email,
        ),
    )


RUNTIME_CONFIG = load_runtime_config()
