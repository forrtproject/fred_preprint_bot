from __future__ import annotations

import csv
import datetime as dt
import json
import logging
import random
import re
import secrets
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .dynamo.preprints_repo import PreprintsRepo
from .exclusion_logging import log_preprint_exclusion

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
ORCID_RE = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X]|\d{15}[0-9X])", flags=re.IGNORECASE)

DEFAULT_NETWORK_STATE_KEY = "trial:author_network_state"


@dataclass
class AuthorMention:
    preprint_id: str
    order: int
    full_name: str
    given: str
    surname: str
    osf_user_id: Optional[str]
    orcid: Optional[str]
    email: Optional[str]
    tokens: List[str]
    node_id: Optional[str] = None
    mapped_existing: bool = False


@dataclass
class PreprintEntry:
    preprint_id: str
    provider_id: str
    date_created: Optional[dt.date]
    mentions: List[AuthorMention]
    contactable_email_count: int
    selected_mentions: List[AuthorMention] = field(default_factory=list)
    cluster_id: str = ""
    arm: str = ""
    status: str = ""
    reason: Optional[str] = None
    email_candidate_positions: List[int] = field(default_factory=list)
    matched_cluster_ids: List[str] = field(default_factory=list)


@dataclass
class ComponentSummary:
    cluster_id: str
    node_ids: List[str]
    preprint_ids: List[str]
    stratum: str
    contactable_preprints: int
    contactable_emails: int
    arm: str = ""


@dataclass
class NodeRecord:
    node_id: str
    cluster_id: str = ""
    names: Set[str] = field(default_factory=set)
    osf_user_ids: Set[str] = field(default_factory=set)
    orcids: Set[str] = field(default_factory=set)
    preprint_ids: Set[str] = field(default_factory=set)
    mention_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_item(cls, item: Dict[str, Any]) -> "NodeRecord":
        return cls(
            node_id=str(item.get("node_id") or ""),
            cluster_id=str(item.get("cluster_id") or ""),
            names=set(item.get("names") or []),
            osf_user_ids=set(item.get("osf_user_ids") or []),
            orcids=set(item.get("orcids") or []),
            preprint_ids=set(item.get("preprint_ids") or []),
            mention_count=int(item.get("mention_count") or 0),
            created_at=item.get("created_at"),
            updated_at=item.get("updated_at"),
        )

    def to_item(self, now_iso: str) -> Dict[str, Any]:
        if not self.created_at:
            self.created_at = now_iso
        self.updated_at = now_iso
        return {
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "names": sorted(self.names),
            "osf_user_ids": sorted(self.osf_user_ids),
            "orcids": sorted(self.orcids),
            "preprint_ids": sorted(self.preprint_ids),
            "preprint_count": len(self.preprint_ids),
            "mention_count": self.mention_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.rank: Dict[str, int] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        self.add(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != x:
            parent = self.parent[x]
            self.parent[x] = root
            x = parent
        return root

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        rank_a = self.rank[ra]
        rank_b = self.rank[rb]
        if rank_a < rank_b:
            self.parent[ra] = rb
            return
        if rank_a > rank_b:
            self.parent[rb] = ra
            return
        self.parent[rb] = ra
        self.rank[ra] += 1


def _norm_space(text: str) -> str:
    return " ".join((text or "").strip().split())


def _to_ascii_lower(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text or "")
    norm = norm.encode("ascii", "ignore").decode("ascii")
    norm = re.sub(r"[^a-zA-Z0-9\s]", " ", norm)
    return _norm_space(norm.lower())


def _clean_value(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    txt = _norm_space(str(raw))
    if not txt or txt.upper() == "NA":
        return None
    return txt


def _normalize_orcid(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    match = ORCID_RE.search(str(raw))
    if not match:
        return None
    val = match.group(1).upper()
    if len(val) == 16:
        val = "-".join([val[i : i + 4] for i in range(0, 16, 4)])
    if not _valid_orcid(val):
        return None
    return val


def _valid_orcid(orcid: str) -> bool:
    digits = (orcid or "").replace("-", "").upper()
    if not re.match(r"^\d{15}[0-9X]$", digits):
        return False
    total = 0
    for ch in digits[:-1]:
        total = (total + int(ch)) * 2
    remainder = total % 11
    check = (12 - remainder) % 11
    expected = "X" if check == 10 else str(check)
    return digits[-1] == expected


def _parse_iso_to_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        parsed = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return parsed.date()
    except ValueError:
        pass
    try:
        return dt.datetime.strptime(txt[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _coerce_raw_preprint(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            obj = json.loads(raw)
        except Exception:
            return {}
        if isinstance(obj, dict):
            return obj
    return {}


def _is_valid_email(raw: Any) -> bool:
    txt = _clean_value(raw)
    if not txt:
        return False
    return EMAIL_RE.match(txt) is not None


def _name_from_parts(given: Optional[str], surname: Optional[str], fallback: Optional[str]) -> Tuple[str, str, str]:
    g = _clean_value(given) or ""
    s = _clean_value(surname) or ""
    if g or s:
        full = _norm_space(f"{g} {s}")
        return full, g, s

    fb = _clean_value(fallback) or ""
    if not fb:
        return "", "", ""
    parts = fb.split()
    if len(parts) == 1:
        return fb, "", parts[0]
    return fb, " ".join(parts[:-1]), parts[-1]


def _is_initials_only(given: str) -> bool:
    """Return True if the given name appears to be just initials (e.g. 'J', 'J A', 'JA')."""
    parts = given.replace(".", " ").split()
    return bool(parts) and all(len(p) <= 1 for p in parts)


def _build_name_keys(given: str, surname: str, full_name: str) -> Tuple[Optional[str], Optional[str]]:
    n_surname = _to_ascii_lower(surname)
    n_given = _to_ascii_lower(given)

    if not n_surname and full_name:
        parts = _to_ascii_lower(full_name).split()
        if parts:
            n_surname = parts[-1]
            n_given = " ".join(parts[:-1])

    full_key: Optional[str] = None
    init_key: Optional[str] = None
    if n_surname:
        full_key = f"namefull:{n_surname}|{n_given}"
        initials = "".join(part[0] for part in n_given.split() if part)
        if initials and _is_initials_only(n_given):
            init_key = f"nameinit:{n_surname}|{initials}"
    return full_key, init_key


def _build_tokens(
    *,
    preprint_id: str,
    order: int,
    full_name: str,
    given: str,
    surname: str,
    osf_user_id: Optional[str],
    orcid: Optional[str],
) -> List[str]:
    tokens: List[str] = []
    if orcid:
        tokens.append(f"orcid:{orcid}")
    if osf_user_id:
        tokens.append(f"osf:{osf_user_id}")

    full_key, init_key = _build_name_keys(given, surname, full_name)
    if full_key:
        tokens.append(full_key)
    if init_key:
        tokens.append(init_key)

    if not tokens and full_name:
        tokens.append(f"nameraw:{_to_ascii_lower(full_name)}")

    if not tokens:
        tokens.append(f"anon:{preprint_id}:{order}")

    out: List[str] = []
    seen: Set[str] = set()
    for token in tokens:
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _extract_contributor_osf_ids(raw_preprint: Dict[str, Any]) -> List[str]:
    rel = (raw_preprint.get("relationships") or {}).get("contributors") or {}
    data = rel.get("data")
    if not isinstance(data, list):
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for row in data:
        user_id = _clean_value((row or {}).get("id"))
        if not user_id or user_id in seen:
            continue
        seen.add(user_id)
        out.append(user_id)
    return out


def _extract_tei_authors(repo: PreprintsRepo, preprint_id: str, cache: Dict[str, List[str]]) -> List[str]:
    if preprint_id in cache:
        return cache[preprint_id]
    item = repo.t_tei.get_item(Key={"osf_id": preprint_id}).get("Item") or {}
    authors = item.get("authors") or []
    out = [_clean_value(a) for a in authors if _clean_value(a)] if isinstance(authors, list) else []
    cache[preprint_id] = out
    return out


def _load_author_rows(path: Optional[str]) -> Dict[str, List[Dict[str, str]]]:
    if not path:
        return {}
    fp = Path(path)
    if not fp.exists():
        logger.warning("author CSV missing; continuing with database (TEI/raw) fallback", extra={"path": str(fp)})
        return {}

    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    with fp.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader):
            preprint_id = _clean_value(row.get("id") or row.get("osf_id"))
            if not preprint_id:
                continue
            rec = dict(row)
            rec["_row_index"] = str(idx)
            grouped[preprint_id].append(rec)

    for preprint_id, rows in grouped.items():
        def _row_key(row: Dict[str, str]) -> Tuple[int, int]:
            n = _clean_value(row.get("n"))
            if n and n.isdigit():
                return (0, int(n))
            idx = row.get("_row_index") or "0"
            return (1, int(idx) if idx.isdigit() else 0)

        rows.sort(key=_row_key)
        grouped[preprint_id] = rows

    return grouped


def _build_mentions_from_csv(preprint_id: str, rows: List[Dict[str, str]]) -> List[AuthorMention]:
    out: List[AuthorMention] = []
    for idx, row in enumerate(rows):
        full_name, given, surname = _name_from_parts(
            row.get("name.given") or row.get("name.given.orcid"),
            row.get("name.surname") or row.get("name.surname.orcid"),
            row.get("osf.name") or row.get("full_name"),
        )
        osf_user_id = _clean_value(row.get("osf.id"))
        orcid = _normalize_orcid(row.get("orcid") or row.get("orcid.osf") or row.get("orcid.xml") or row.get("orcid.pdf"))
        email = _clean_value(row.get("email.possible") or row.get("email"))
        tokens = _build_tokens(
            preprint_id=preprint_id,
            order=idx,
            full_name=full_name,
            given=given,
            surname=surname,
            osf_user_id=osf_user_id,
            orcid=orcid,
        )
        out.append(
            AuthorMention(
                preprint_id=preprint_id,
                order=idx,
                full_name=full_name,
                given=given,
                surname=surname,
                osf_user_id=osf_user_id,
                orcid=orcid,
                email=email,
                tokens=tokens,
            )
        )
    return out


def _build_mentions_fallback(
    *,
    repo: PreprintsRepo,
    preprint_id: str,
    raw_preprint: Dict[str, Any],
    tei_cache: Dict[str, List[str]],
) -> List[AuthorMention]:
    tei_names = _extract_tei_authors(repo, preprint_id, tei_cache)
    osf_ids = _extract_contributor_osf_ids(raw_preprint)

    out: List[AuthorMention] = []
    if tei_names:
        for idx, name in enumerate(tei_names):
            full, given, surname = _name_from_parts(None, None, name)
            osf_user_id = osf_ids[idx] if idx < len(osf_ids) else None
            tokens = _build_tokens(
                preprint_id=preprint_id,
                order=idx,
                full_name=full,
                given=given,
                surname=surname,
                osf_user_id=osf_user_id,
                orcid=None,
            )
            out.append(
                AuthorMention(
                    preprint_id=preprint_id,
                    order=idx,
                    full_name=full,
                    given=given,
                    surname=surname,
                    osf_user_id=osf_user_id,
                    orcid=None,
                    email=None,
                    tokens=tokens,
                )
            )

    if not out and osf_ids:
        for idx, osf_user_id in enumerate(osf_ids):
            tokens = _build_tokens(
                preprint_id=preprint_id,
                order=idx,
                full_name="",
                given="",
                surname="",
                osf_user_id=osf_user_id,
                orcid=None,
            )
            out.append(
                AuthorMention(
                    preprint_id=preprint_id,
                    order=idx,
                    full_name="",
                    given="",
                    surname="",
                    osf_user_id=osf_user_id,
                    orcid=None,
                    email=None,
                    tokens=tokens,
                )
            )

    if not out:
        tokens = _build_tokens(
            preprint_id=preprint_id,
            order=0,
            full_name="",
            given="",
            surname="",
            osf_user_id=None,
            orcid=None,
        )
        out.append(
            AuthorMention(
                preprint_id=preprint_id,
                order=0,
                full_name="",
                given="",
                surname="",
                osf_user_id=None,
                orcid=None,
                email=None,
                tokens=tokens,
            )
        )

    return out


def select_author_positions(
    author_count: int,
    email_candidate_positions: Sequence[int],
) -> List[int]:
    """Select author positions for graph edges: first four + last + email recipients."""
    if author_count <= 0:
        return []
    picks = [0, 1, 2, 3, author_count - 1]
    picks.extend(email_candidate_positions)
    out: List[int] = []
    seen: Set[int] = set()
    for pos in picks:
        if 0 <= pos < author_count and pos not in seen:
            seen.add(pos)
            out.append(pos)
    return out


def _initials_of(given: str) -> str:
    """Extract leading-letter initials from a given name, e.g. 'john andrew' -> 'ja'."""
    return "".join(p[0] for p in given.split() if p)


def _find_namefull_matches_for_initials(
    surname: str,
    initials: str,
    namefull_by_surname: Dict[str, List[Tuple[str, str]]],
) -> List[str]:
    """Return namefull tokens whose surname matches and whose initials match."""
    out: List[str] = []
    for token, given in namefull_by_surname.get(surname, []):
        if _initials_of(given) == initials:
            out.append(token)
    return out


def resolve_author_nodes_from_tokens(token_groups: Sequence[Sequence[str]]) -> List[str]:
    uf = UnionFind()

    # Phase 1: union-find on all non-nameinit tokens.
    # Also collect namefull index and nameinit tokens for phase 2.
    namefull_by_surname: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    nameinit_tokens: List[str] = []

    for tokens in token_groups:
        clean = [t for t in tokens if t]
        if not clean:
            continue

        for token in clean:
            uf.add(token)

        # Union all tokens within the same mention
        head = clean[0]
        for token in clean[1:]:
            uf.union(head, token)

        # Index namefull tokens by surname for phase 2 lookup
        for token in clean:
            if token.startswith("namefull:"):
                rest = token[len("namefull:"):]
                parts = rest.split("|", 1)
                if len(parts) == 2:
                    namefull_by_surname[parts[0]].append((token, parts[1]))
            elif token.startswith("nameinit:"):
                nameinit_tokens.append(token)

    # Phase 2: match nameinit tokens to compatible namefull tokens.
    # This bridges clusters only when an actual initials-only record exists.
    for init_token in nameinit_tokens:
        rest = init_token[len("nameinit:"):]
        parts = rest.split("|", 1)
        if len(parts) != 2:
            continue
        surname, initials = parts
        for full_token in _find_namefull_matches_for_initials(
            surname, initials, namefull_by_surname
        ):
            uf.union(init_token, full_token)

    roots = sorted({uf.find(token) for token in uf.parent.keys()})
    node_by_root = {root: f"N{idx:06d}" for idx, root in enumerate(roots, start=1)}

    out: List[str] = []
    for tokens in token_groups:
        clean = [t for t in tokens if t]
        if not clean:
            out.append("")
            continue
        out.append(node_by_root[uf.find(clean[0])])
    return out


def _connected_components(adjacency: Dict[str, Set[str]]) -> List[Set[str]]:
    out: List[Set[str]] = []
    seen: Set[str] = set()
    for start in sorted(adjacency.keys()):
        if start in seen:
            continue
        stack = [start]
        comp: Set[str] = set()
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            comp.add(node)
            for nxt in adjacency.get(node, set()):
                if nxt not in seen:
                    stack.append(nxt)
        out.append(comp)
    return out


def assign_components_balanced(components: Dict[str, ComponentSummary], *, seed: int) -> Dict[str, str]:
    rng = random.Random(seed)
    by_stratum: Dict[str, List[str]] = defaultdict(list)
    for cluster_id, comp in components.items():
        by_stratum[comp.stratum].append(cluster_id)

    assignments: Dict[str, str] = {}
    for stratum in sorted(by_stratum.keys()):
        order = sorted(by_stratum[stratum])
        rng.shuffle(order)

        t_pre = c_pre = 0

        for cluster_id in order:
            comp = components[cluster_id]
            score_t = abs((t_pre + comp.contactable_preprints) - c_pre)
            score_c = abs(t_pre - (c_pre + comp.contactable_preprints))

            if score_t < score_c:
                chosen = "treatment"
            elif score_c < score_t:
                chosen = "control"
            else:
                chosen = rng.choice(["treatment", "control"])

            assignments[cluster_id] = chosen
            if chosen == "treatment":
                t_pre += comp.contactable_preprints
            else:
                c_pre += comp.contactable_preprints

    return assignments


def _format_node_id(seq: int) -> str:
    return f"N{seq:06d}"


def _format_cluster_id(seq: int) -> str:
    return f"C{seq:06d}"


def _parse_int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except Exception:
        return default


def _parse_int_or_none(raw: Any) -> Optional[int]:
    try:
        return int(raw)
    except Exception:
        return None


def _coerce_cluster_counts(cluster_item: Dict[str, Any]) -> Tuple[int, int, int]:
    preprints_total = _parse_int(cluster_item.get("preprints_total"), 0)
    contactable_preprints = _parse_int(cluster_item.get("contactable_preprints"), 0)
    contactable_emails = _parse_int(cluster_item.get("contactable_emails"), 0)
    return preprints_total, contactable_preprints, contactable_emails


def _choose_provider(item: Dict[str, Any]) -> str:
    provider = _clean_value(item.get("provider_id"))
    if provider:
        return provider
    raw = _coerce_raw_preprint(item.get("raw"))
    rel = (raw.get("relationships") or {}).get("provider") or {}
    data = rel.get("data") or {}
    return _clean_value(data.get("id")) or "unknown"


def _contactable_count(item: Dict[str, Any], csv_rows: List[Dict[str, str]]) -> int:
    csv_emails = {
        (_clean_value(row.get("email.possible") or row.get("email")) or "").lower()
        for row in csv_rows
        if _is_valid_email(row.get("email.possible") or row.get("email"))
    }

    candidates = item.get("author_email_candidates") or []
    cand_emails: Set[str] = set()
    if isinstance(candidates, list):
        for row in candidates:
            email = _clean_value((row or {}).get("email"))
            if _is_valid_email(email):
                cand_emails.add(email.lower())
    return max(len(csv_emails), len(cand_emails))


def _load_unassigned_preprints(
    repo: PreprintsRepo,
    *,
    author_rows: Dict[str, List[Dict[str, str]]],
    limit_preprints: Optional[int],
) -> List[PreprintEntry]:
    tei_cache: Dict[str, List[str]] = {}
    rows = repo.select_unassigned_preprints(limit=limit_preprints)

    out: List[PreprintEntry] = []
    for item in rows:
        preprint_id = _clean_value(item.get("osf_id"))
        if not preprint_id:
            continue

        basic_item: Optional[Dict[str, Any]] = None
        provider_id = _clean_value(item.get("provider_id"))
        raw_preprint: Dict[str, Any] = {}

        csv_rows = author_rows.get(preprint_id, [])
        if csv_rows:
            mentions = _build_mentions_from_csv(preprint_id, csv_rows)
        else:
            basic_item = repo.get_preprint_basic(preprint_id) or {}
            raw_preprint = _coerce_raw_preprint(basic_item.get("raw"))
            provider_id = provider_id or _clean_value(basic_item.get("provider_id"))
            mentions = _build_mentions_fallback(
                repo=repo,
                preprint_id=preprint_id,
                raw_preprint=raw_preprint,
                tei_cache=tei_cache,
            )

        candidates_raw = item.get("author_email_candidates") or []
        email_positions: List[int] = []
        for cand in candidates_raw:
            pos = (cand or {}).get("position")
            if pos is not None:
                email_positions.append(int(pos))

        provider_source = {
            "provider_id": provider_id or item.get("provider_id"),
            "raw": raw_preprint if raw_preprint else (basic_item or {}).get("raw"),
        }
        contactable_count = _contactable_count(item, csv_rows)
        if contactable_count <= 0:
            continue
        if not bool(item.get("flora_eligible")):
            continue
        out.append(
            PreprintEntry(
                preprint_id=preprint_id,
                provider_id=_choose_provider(provider_source),
                date_created=_parse_iso_to_date(item.get("date_created") or item.get("date_published")),
                mentions=mentions,
                contactable_email_count=contactable_count,
                email_candidate_positions=email_positions,
            )
        )

    out.sort(key=lambda p: (p.date_created or dt.date(1900, 1, 1), p.preprint_id))
    return out


def _apply_mention_to_node(node: NodeRecord, mention: AuthorMention) -> None:
    if mention.full_name:
        node.names.add(mention.full_name)
    if mention.osf_user_id:
        node.osf_user_ids.add(mention.osf_user_id)
    if mention.orcid:
        node.orcids.add(mention.orcid)
    node.preprint_ids.add(mention.preprint_id)
    node.mention_count += 1


def _cluster_totals_by_stratum(clusters: Dict[str, Dict[str, Any]], stratum: str) -> Tuple[int, int]:
    t_pre = c_pre = 0
    for cluster in clusters.values():
        if str(cluster.get("stratum") or "unknown") != stratum:
            continue
        arm = str(cluster.get("arm") or "")
        _, cp, _ce = _coerce_cluster_counts(cluster)
        if arm == "treatment":
            t_pre += cp
        elif arm == "control":
            c_pre += cp
    return t_pre, c_pre


def _choose_arm_for_new_cluster(
    *,
    clusters: Dict[str, Dict[str, Any]],
    stratum: str,
    contactable_preprints: int,
    contactable_emails: int,
    rng: random.Random,
) -> str:
    t_pre, c_pre = _cluster_totals_by_stratum(clusters, stratum)

    score_t = abs((t_pre + contactable_preprints) - c_pre)
    score_c = abs(t_pre - (c_pre + contactable_preprints))

    if score_t < score_c:
        return "treatment"
    if score_c < score_t:
        return "control"
    return rng.choice(["treatment", "control"])


def _persist_run(
    *,
    repo: PreprintsRepo,
    run_id: str,
    now_iso: str,
    nodes: Dict[str, NodeRecord],
    touched_node_ids: Set[str],
    token_map_updates: Dict[str, str],
    clusters: Dict[str, Dict[str, Any]],
    touched_cluster_ids: Set[str],
    assignments: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    node_items = [nodes[nid].to_item(now_iso) for nid in sorted(touched_node_ids) if nid in nodes]
    cluster_items = [clusters[cid] for cid in sorted(touched_cluster_ids) if cid in clusters]

    for item in cluster_items:
        item["updated_at"] = now_iso
        item.setdefault("created_at", now_iso)

    accepted_assignments: List[Dict[str, Any]] = []
    for row in assignments:
        row["run_id"] = run_id
        row["assigned_at"] = now_iso
        ok = repo.mark_preprint_trial_assignment(
            osf_id=row["preprint_id"],
            status=row.get("status") or "excluded",
            assigned_at=row["assigned_at"],
            arm=row.get("arm"),
            cluster_id=row.get("cluster_id"),
            reason=row.get("reason"),
            matched_cluster_ids=row.get("matched_cluster_ids") or [],
            run_id=row["run_id"],
        )
        if ok:
            accepted_assignments.append(row)
            preprint_id = str(row.get("preprint_id") or "")
            if not preprint_id:
                continue
            if str(row.get("status") or "") == "assigned" and str(row.get("arm") or "") == "treatment":
                if int(row.get("contactable_email_count") or 0) > 0:
                    repo.set_queue_email(preprint_id)
            else:
                repo.clear_queue_email(preprint_id)

    repo.put_trial_nodes(node_items)
    repo.put_trial_token_map(token_map_updates)
    repo.put_trial_clusters(cluster_items)
    if accepted_assignments:
        repo.put_trial_assignments(accepted_assignments)
    return accepted_assignments


def _initialize_network(
    *,
    candidates: List[PreprintEntry],
    seed: int,
    clusters: Dict[str, Dict[str, Any]],
    nodes: Dict[str, NodeRecord],
    token_map: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Set[str], Set[str]]:
    token_groups: List[List[str]] = []
    mentions_flat: List[AuthorMention] = []

    for preprint in candidates:
        picks = select_author_positions(len(preprint.mentions), preprint.email_candidate_positions)
        preprint.selected_mentions = [preprint.mentions[pos] for pos in picks]
        for mention in preprint.selected_mentions:
            token_groups.append(mention.tokens)
            mentions_flat.append(mention)

    node_ids = resolve_author_nodes_from_tokens(token_groups)
    for mention, node_id in zip(mentions_flat, node_ids):
        mention.node_id = node_id

    adjacency: Dict[str, Set[str]] = defaultdict(set)
    for preprint in candidates:
        selected_node_ids: List[str] = []
        seen: Set[str] = set()
        for mention in preprint.selected_mentions:
            if not mention.node_id:
                continue
            if mention.node_id in seen:
                continue
            seen.add(mention.node_id)
            selected_node_ids.append(mention.node_id)
            adjacency.setdefault(mention.node_id, set())
        for i, a in enumerate(selected_node_ids):
            for b in selected_node_ids[i + 1 :]:
                adjacency[a].add(b)
                adjacency[b].add(a)

    components = _connected_components(adjacency)
    node_to_cluster: Dict[str, str] = {}
    for idx, comp in enumerate(sorted(components, key=min), start=1):
        cluster_id = _format_cluster_id(idx)
        for node_id in comp:
            node_to_cluster[node_id] = cluster_id

    comp_preprints: Dict[str, List[PreprintEntry]] = defaultdict(list)
    comp_nodes: Dict[str, Set[str]] = defaultdict(set)
    for preprint in candidates:
        selected_cluster_ids = {node_to_cluster[m.node_id] for m in preprint.selected_mentions if m.node_id in node_to_cluster}
        if not selected_cluster_ids:
            preprint.status = "excluded"
            preprint.reason = "no_author_nodes"
            continue
        if len(selected_cluster_ids) > 1:
            # When this happens, all clusters originate from this same initialization pass.
            # Use deterministic primary cluster and keep node mapping as constructed.
            primary = sorted(selected_cluster_ids)[0]
        else:
            primary = next(iter(selected_cluster_ids))
        preprint.cluster_id = primary
        comp_preprints[primary].append(preprint)
        for mention in preprint.selected_mentions:
            if mention.node_id:
                comp_nodes[primary].add(mention.node_id)

    summaries: Dict[str, ComponentSummary] = {}
    for cluster_id, preprints in comp_preprints.items():
        provider_counts: Dict[str, int] = defaultdict(int)
        for p in preprints:
            provider_counts[p.provider_id] += 1
        stratum = sorted(provider_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] if provider_counts else "unknown"
        summaries[cluster_id] = ComponentSummary(
            cluster_id=cluster_id,
            node_ids=sorted(comp_nodes.get(cluster_id, set())),
            preprint_ids=sorted(p.preprint_id for p in preprints),
            stratum=stratum,
            contactable_preprints=sum(1 for p in preprints if p.contactable_email_count > 0),
            contactable_emails=sum(p.contactable_email_count for p in preprints),
        )

    arm_by_cluster = assign_components_balanced(summaries, seed=seed)

    touched_clusters: Set[str] = set()
    touched_nodes: Set[str] = set()
    assignments: List[Dict[str, Any]] = []

    for cluster_id, summary in summaries.items():
        arm = arm_by_cluster.get(cluster_id, "control")
        clusters[cluster_id] = {
            "cluster_id": cluster_id,
            "stratum": summary.stratum,
            "arm": arm,
            "preprints_total": len(summary.preprint_ids),
            "contactable_preprints": summary.contactable_preprints,
            "contactable_emails": summary.contactable_emails,
        }
        touched_clusters.add(cluster_id)

    for preprint in candidates:
        if preprint.reason:
            preprint.status = "excluded"
            assignments.append(
                {
                    "preprint_id": preprint.preprint_id,
                    "status": "excluded",
                    "reason": preprint.reason,
                    "arm": None,
                    "cluster_id": None,
                    "matched_cluster_ids": [],
                    "stratum": preprint.provider_id,
                    "contactable_email_count": preprint.contactable_email_count,
                    "contactable_preprint": int(preprint.contactable_email_count > 0),
                }
            )
            continue

        cluster_id = preprint.cluster_id
        arm = str(clusters.get(cluster_id, {}).get("arm") or "")
        preprint.arm = arm
        preprint.status = "assigned"
        preprint.matched_cluster_ids = [cluster_id]
        assignments.append(
            {
                "preprint_id": preprint.preprint_id,
                "status": "assigned",
                "reason": None,
                "arm": arm,
                "cluster_id": cluster_id,
                "matched_cluster_ids": [cluster_id],
                "stratum": preprint.provider_id,
                "contactable_email_count": preprint.contactable_email_count,
                "contactable_preprint": int(preprint.contactable_email_count > 0),
            }
        )

    for preprint in candidates:
        for mention in preprint.selected_mentions:
            node_id = mention.node_id
            if not node_id:
                continue
            cluster_id = node_to_cluster.get(node_id) or preprint.cluster_id
            node = nodes.get(node_id)
            if node is None:
                node = NodeRecord(node_id=node_id, cluster_id=cluster_id)
                nodes[node_id] = node
            elif not node.cluster_id:
                node.cluster_id = cluster_id
            _apply_mention_to_node(node, mention)
            touched_nodes.add(node_id)
            for token in mention.tokens:
                if token and token not in token_map:
                    token_map[token] = node_id

    return assignments, touched_nodes, touched_clusters


def _resolve_mention_nodes(
    tokens: List[str],
    token_map: Dict[str, str],
    nodes: Dict[str, NodeRecord],
) -> Set[str]:
    """Map a mention's tokens to existing node IDs, including initials-compatible matches.

    Direct token matches are checked first.  For any ``nameinit:`` token, we
    also scan the token_map for ``namefull:`` entries with the same surname
    whose given-name initials match.  This ensures that an initials-only
    mention (e.g. "J Smith") finds existing full-name nodes ("John Smith",
    "Jane Smith") even though they did not emit a ``nameinit`` token.
    """
    matched: Set[str] = set()
    for token in tokens:
        if token in token_map and token_map[token] in nodes:
            matched.add(token_map[token])

        if token.startswith("nameinit:"):
            rest = token[len("nameinit:"):]
            parts = rest.split("|", 1)
            if len(parts) == 2:
                surname, initials = parts
                prefix = f"namefull:{surname}|"
                for existing_token, node_id in token_map.items():
                    if (
                        existing_token.startswith(prefix)
                        and node_id in nodes
                        and _initials_of(existing_token[len(prefix):]) == initials
                    ):
                        matched.add(node_id)
    return matched


def _augment_network(
    *,
    candidates: List[PreprintEntry],
    seed: int,
    next_node_seq: int,
    next_cluster_seq: int,
    clusters: Dict[str, Dict[str, Any]],
    nodes: Dict[str, NodeRecord],
    token_map: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Set[str], Set[str], int, int]:
    rng = random.Random(seed)

    assignments: List[Dict[str, Any]] = []
    touched_nodes: Set[str] = set()
    touched_clusters: Set[str] = set()

    for preprint in candidates:
        picks = select_author_positions(len(preprint.mentions), preprint.email_candidate_positions)
        preprint.selected_mentions = [preprint.mentions[pos] for pos in picks]

        existing_cluster_ids: Set[str] = set()
        existing_arms: Set[str] = set()

        for mention in preprint.selected_mentions:
            mapped_nodes = sorted(
                _resolve_mention_nodes(mention.tokens, token_map, nodes)
            )
            if mapped_nodes:
                mention.node_id = mapped_nodes[0]
                mention.mapped_existing = True
                cluster_id = nodes[mention.node_id].cluster_id
                if cluster_id:
                    existing_cluster_ids.add(cluster_id)
                    arm = str(clusters.get(cluster_id, {}).get("arm") or "")
                    if arm:
                        existing_arms.add(arm)
            else:
                mention.node_id = _format_node_id(next_node_seq)
                next_node_seq += 1
                mention.mapped_existing = False

        if len(existing_arms) > 1:
            preprint.status = "excluded"
            preprint.reason = "cross_arm_author_overlap"
            assignments.append(
                {
                    "preprint_id": preprint.preprint_id,
                    "status": "excluded",
                    "reason": preprint.reason,
                    "arm": None,
                    "cluster_id": None,
                    "matched_cluster_ids": sorted(existing_cluster_ids),
                    "stratum": preprint.provider_id,
                    "contactable_email_count": preprint.contactable_email_count,
                    "contactable_preprint": int(preprint.contactable_email_count > 0),
                }
            )
            continue

        if existing_cluster_ids:
            primary_cluster = sorted(existing_cluster_ids)[0]
            arm = next(iter(existing_arms)) if existing_arms else str(clusters.get(primary_cluster, {}).get("arm") or "control")
        else:
            primary_cluster = _format_cluster_id(next_cluster_seq)
            next_cluster_seq += 1
            arm = _choose_arm_for_new_cluster(
                clusters=clusters,
                stratum=preprint.provider_id,
                contactable_preprints=int(preprint.contactable_email_count > 0),
                contactable_emails=preprint.contactable_email_count,
                rng=rng,
            )
            clusters[primary_cluster] = {
                "cluster_id": primary_cluster,
                "stratum": preprint.provider_id,
                "arm": arm,
                "preprints_total": 0,
                "contactable_preprints": 0,
                "contactable_emails": 0,
            }
            touched_clusters.add(primary_cluster)

        preprint.cluster_id = primary_cluster
        preprint.arm = arm
        preprint.status = "assigned"
        preprint.matched_cluster_ids = sorted(existing_cluster_ids) if existing_cluster_ids else []

        cluster = clusters[primary_cluster]
        pre_total, c_pre, c_email = _coerce_cluster_counts(cluster)
        cluster["preprints_total"] = pre_total + 1
        cluster["contactable_preprints"] = c_pre + int(preprint.contactable_email_count > 0)
        cluster["contactable_emails"] = c_email + preprint.contactable_email_count
        touched_clusters.add(primary_cluster)

        for mention in preprint.selected_mentions:
            node_id = mention.node_id or ""
            if not node_id:
                continue
            node = nodes.get(node_id)
            if node is None:
                node = NodeRecord(node_id=node_id, cluster_id=primary_cluster)
                nodes[node_id] = node
            elif not node.cluster_id:
                node.cluster_id = primary_cluster
            _apply_mention_to_node(node, mention)
            touched_nodes.add(node_id)

            for token in mention.tokens:
                if token and token not in token_map:
                    token_map[token] = node_id

        assignments.append(
            {
                "preprint_id": preprint.preprint_id,
                "status": "assigned",
                "reason": None,
                "arm": arm,
                "cluster_id": primary_cluster,
                "matched_cluster_ids": preprint.matched_cluster_ids,
                "stratum": preprint.provider_id,
                "contactable_email_count": preprint.contactable_email_count,
                "contactable_preprint": int(preprint.contactable_email_count > 0),
            }
        )

    return assignments, touched_nodes, touched_clusters, next_node_seq, next_cluster_seq


def run_author_randomization(
    *,
    authors_csv: Optional[str] = None,
    limit_preprints: Optional[int] = None,
    seed: Optional[int] = None,
    network_state_key: str = DEFAULT_NETWORK_STATE_KEY,
    dry_run: bool = False,
) -> Dict[str, Any]:
    repo = PreprintsRepo()
    state = repo.get_sync_item(network_state_key)

    author_rows = _load_author_rows(authors_csv)
    candidates = _load_unassigned_preprints(repo, author_rows=author_rows, limit_preprints=limit_preprints)
    if not candidates:
        return {
            "run_id": None,
            "mode": "noop",
            "dry_run": dry_run,
            "processed_preprints": 0,
            "assigned": 0,
            "excluded": 0,
            "network_initialized": bool(state.get("initialized")),
        }

    existing_clusters_list = repo.list_trial_clusters()
    clusters: Dict[str, Dict[str, Any]] = {
        str(item.get("cluster_id")): dict(item)
        for item in existing_clusters_list
        if item.get("cluster_id")
    }

    state_initialized = bool(state.get("initialized"))
    if clusters and not state_initialized:
        raise RuntimeError(
            "Trial clusters exist but network state is missing/uninitialized for "
            f"key '{network_state_key}'. Refusing to augment with ambiguous state."
        )

    network_initialized = state_initialized

    seed_used = int(seed) if seed is not None else _parse_int(state.get("next_seed"), 0)
    if seed_used <= 0:
        seed_used = secrets.randbits(63) or 1

    run_id = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ") + f"-{secrets.token_hex(3)}"
    now_iso = dt.datetime.utcnow().isoformat()

    nodes_by_id: Dict[str, NodeRecord] = {}
    token_map: Dict[str, str] = {}

    if network_initialized:
        all_tokens: Set[str] = set()
        for preprint in candidates:
            picks = select_author_positions(len(preprint.mentions), preprint.email_candidate_positions)
            for pos in picks:
                all_tokens.update(preprint.mentions[pos].tokens)
        raw_token_map = repo.get_trial_token_map(all_tokens)
        nodes_by_id = {nid: NodeRecord.from_item(item) for nid, item in repo.get_trial_nodes(set(raw_token_map.values())).items()}
        token_map = {token: node_id for token, node_id in raw_token_map.items() if node_id in nodes_by_id}
    else:
        # initialize fresh graph in-memory from current unassigned cohort
        nodes_by_id = {}
        token_map = {}

    if network_initialized:
        all_existing_nodes = repo.list_trial_nodes()
        max_existing_node = max(
            [_parse_int((str(item.get("node_id") or "")[1:]), 0) for item in all_existing_nodes if str(item.get("node_id") or "").startswith("N")],
            default=0,
        )
    else:
        max_existing_node = max([_parse_int((nid or "")[1:], 0) for nid in nodes_by_id.keys() if str(nid).startswith("N")], default=0)
    max_existing_cluster = max([_parse_int((cid or "")[1:], 0) for cid in clusters.keys() if str(cid).startswith("C")], default=0)

    raw_next_node_seq = _parse_int_or_none(state.get("next_node_seq"))
    raw_next_cluster_seq = _parse_int_or_none(state.get("next_cluster_seq"))

    if network_initialized:
        if raw_next_node_seq is None or raw_next_node_seq < max_existing_node + 1:
            raise RuntimeError(
                "Refusing to augment without valid next_node_seq in state: "
                f"{raw_next_node_seq} (max_existing_node={max_existing_node})"
            )
        if raw_next_cluster_seq is None or raw_next_cluster_seq < max_existing_cluster + 1:
            raise RuntimeError(
                "Refusing to augment without valid next_cluster_seq in state: "
                f"{raw_next_cluster_seq} (max_existing_cluster={max_existing_cluster})"
            )
        next_node_seq = raw_next_node_seq
        next_cluster_seq = raw_next_cluster_seq
    else:
        next_node_seq = max(_parse_int(state.get("next_node_seq"), 1), max_existing_node + 1)
        next_cluster_seq = max(_parse_int(state.get("next_cluster_seq"), 1), max_existing_cluster + 1)

    if not network_initialized:
        assignments, touched_nodes, touched_clusters = _initialize_network(
            candidates=candidates,
            seed=seed_used,
            clusters=clusters,
            nodes=nodes_by_id,
            token_map=token_map,
        )
        max_new_node = max([_parse_int((nid or "")[1:], 0) for nid in nodes_by_id.keys() if str(nid).startswith("N")], default=0)
        max_new_cluster = max([_parse_int((cid or "")[1:], 0) for cid in clusters.keys() if str(cid).startswith("C")], default=0)
        next_node_seq = max(next_node_seq, max_new_node + 1)
        next_cluster_seq = max(next_cluster_seq, max_new_cluster + 1)
        mode = "initialize"
    else:
        assignments, touched_nodes, touched_clusters, next_node_seq, next_cluster_seq = _augment_network(
            candidates=candidates,
            seed=seed_used,
            next_node_seq=next_node_seq,
            next_cluster_seq=next_cluster_seq,
            clusters=clusters,
            nodes=nodes_by_id,
            token_map=token_map,
        )
        mode = "augment"

    assigned = sum(1 for row in assignments if row.get("status") == "assigned")
    excluded = sum(1 for row in assignments if row.get("status") == "excluded")

    next_seed = random.Random(seed_used).randrange(1, 2**63 - 1)

    if not dry_run:
        accepted_assignments = _persist_run(
            repo=repo,
            run_id=run_id,
            now_iso=now_iso,
            nodes=nodes_by_id,
            touched_node_ids=touched_nodes,
            token_map_updates=token_map,
            clusters=clusters,
            touched_cluster_ids=touched_clusters,
            assignments=assignments,
        )
        for row in accepted_assignments:
            if str(row.get("status") or "") != "excluded":
                continue
            log_preprint_exclusion(
                reason=str(row.get("reason") or "randomization_excluded"),
                osf_id=str(row.get("preprint_id") or ""),
                stage="author-randomize",
                details={"matched_cluster_ids": row.get("matched_cluster_ids") or []},
            )

        state_item = dict(state)
        state_item.update(
            {
                "source_key": network_state_key,
                "initialized": True,
                "next_node_seq": int(next_node_seq),
                "next_cluster_seq": int(next_cluster_seq),
                "next_seed": str(next_seed),
                "last_seed_used": str(seed_used),
                "last_run_at": now_iso,
                "last_run_id": run_id,
                "last_mode": mode,
            }
        )
        repo.put_sync_item(state_item)

    return {
        "run_id": run_id,
        "mode": mode,
        "dry_run": dry_run,
        "processed_preprints": len(candidates),
        "assigned": assigned,
        "excluded": excluded,
        "seed_used": seed_used,
        "next_seed": next_seed,
        "next_node_seq": next_node_seq,
        "next_cluster_seq": next_cluster_seq,
        "network_initialized": True,
        "touched_nodes": len(touched_nodes),
        "touched_clusters": len(touched_clusters),
    }
