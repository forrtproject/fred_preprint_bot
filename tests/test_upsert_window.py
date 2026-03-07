import unittest
from types import SimpleNamespace
from unittest.mock import patch

from osf_sync import upsert


def _cfg(anchor_date="2026-02-20", excluded_providers=()):
    return SimpleNamespace(
        ingest=SimpleNamespace(
            anchor_date=anchor_date,
            excluded_providers=excluded_providers,
        ),
    )


class UpsertWindowTests(unittest.TestCase):
    def test_filter_uses_creation_date_not_publication_date(self) -> None:
        rows = [
            {
                "id": "p1",
                "attributes": {
                    "date_created": "2026-03-01T00:00:00Z",
                    "date_published": "2027-01-01T00:00:00Z",
                },
                "links": {},
            }
        ]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg()):
            kept, skipped_provider, skipped_date, _skipped_links, _skipped_version, _records = upsert._filter_ingest_rows(rows)

        self.assertEqual(len(kept), 1)
        self.assertEqual(skipped_provider, 0)
        self.assertEqual(skipped_date, 0)

    def test_filter_applies_plus_minus_six_month_window(self) -> None:
        rows = [
            {
                "id": "before_window",
                "attributes": {"date_created": "2025-08-19T00:00:00Z"},
                "links": {},
            },
            {
                "id": "inside_window",
                "attributes": {"date_created": "2026-08-20T00:00:00Z"},
                "links": {},
            },
            {
                "id": "after_window",
                "attributes": {"date_created": "2026-08-21T00:00:00Z"},
                "links": {},
            },
        ]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg()):
            kept, _skipped_provider, skipped_date, _skipped_links, _skipped_version, _records = upsert._filter_ingest_rows(rows)

        kept_ids = {row["id"] for row in kept}
        self.assertEqual(kept_ids, {"inside_window"})
        self.assertEqual(skipped_date, 2)


class UpsertProviderFilterTests(unittest.TestCase):
    def test_thesiscommons_filtered_psyarxiv_passes(self) -> None:
        rows = [
            {
                "id": "thesis1",
                "attributes": {"date_created": "2026-02-01T00:00:00Z"},
                "links": {},
                "relationships": {"provider": {"data": {"id": "thesiscommons"}}},
            },
            {
                "id": "psyarxiv1",
                "attributes": {"date_created": "2026-02-01T00:00:00Z"},
                "links": {},
                "relationships": {"provider": {"data": {"id": "psyarxiv"}}},
            },
        ]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg(excluded_providers=("thesiscommons",))):
            kept, skipped_provider, skipped_date, skipped_links, _skipped_version, records = upsert._filter_ingest_rows(rows)

        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["id"], "psyarxiv1")
        self.assertEqual(skipped_provider, 1)
        self.assertEqual(skipped_date, 0)
        self.assertEqual(skipped_links, 0)
        self.assertEqual(records[0]["reason"], "excluded_provider")
        self.assertEqual(records[0]["provider_id"], "thesiscommons")

    def test_missing_relationships_does_not_crash(self) -> None:
        rows = [
            {
                "id": "no_rels",
                "attributes": {"date_created": "2026-02-01T00:00:00Z"},
                "links": {},
            },
            {
                "id": "empty_rels",
                "attributes": {"date_created": "2026-02-01T00:00:00Z"},
                "links": {},
                "relationships": {},
            },
        ]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg(excluded_providers=("thesiscommons",))):
            kept, skipped_provider, _skipped_date, _skipped_links, _skipped_version, _records = upsert._filter_ingest_rows(rows)

        self.assertEqual(len(kept), 2)
        self.assertEqual(skipped_provider, 0)

    def test_provider_filter_is_case_insensitive(self) -> None:
        rows = [
            {
                "id": "t1",
                "attributes": {"date_created": "2026-02-01T00:00:00Z"},
                "links": {},
                "relationships": {"provider": {"data": {"id": "ThesisCommons"}}},
            },
        ]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg(excluded_providers=("thesiscommons",))):
            kept, skipped_provider, _skipped_date, _skipped_links, _skipped_version, _records = upsert._filter_ingest_rows(rows)

        self.assertEqual(len(kept), 0)
        self.assertEqual(skipped_provider, 1)

    def test_empty_excluded_providers_keeps_all(self) -> None:
        rows = [
            {
                "id": "thesis1",
                "attributes": {"date_created": "2026-02-01T00:00:00Z"},
                "links": {},
                "relationships": {"provider": {"data": {"id": "thesiscommons"}}},
            },
        ]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg(excluded_providers=())):
            kept, skipped_provider, _skipped_date, _skipped_links, _skipped_version, _records = upsert._filter_ingest_rows(rows)

        self.assertEqual(len(kept), 1)
        self.assertEqual(skipped_provider, 0)


if __name__ == "__main__":
    unittest.main()
