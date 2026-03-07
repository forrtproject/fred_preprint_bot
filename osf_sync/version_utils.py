"""Utilities for parsing and comparing versioned OSF preprint IDs."""
from __future__ import annotations

import re
from typing import Optional

_VERSION_RE = re.compile(r"^(.+)_v(\d+)$")


def parse_version(osf_id: str) -> tuple[str, Optional[int]]:
    """'pzgxw_v3' -> ('pzgxw', 3).  Non-versioned -> (osf_id, None)."""
    m = _VERSION_RE.match(osf_id)
    if m:
        return m.group(1), int(m.group(2))
    return osf_id, None


def base_id(osf_id: str) -> str:
    """'pzgxw_v3' -> 'pzgxw'."""
    return parse_version(osf_id)[0]


def sibling_ids(osf_id: str, max_version: int = 30) -> list[str]:
    """Generate candidate sibling IDs: all base_vN where N != current, 1..max_version."""
    base, ver = parse_version(osf_id)
    if ver is None:
        return []
    return [f"{base}_v{n}" for n in range(1, max_version + 1) if n != ver]
