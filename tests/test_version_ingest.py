"""Tests for version filtering in _filter_ingest_rows and sibling handling in upsert_preprints."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

from osf_sync import upsert
from osf_sync.dynamo.preprints_repo import PreprintsRepo


def _cfg(anchor_date="2026-02-20", excluded_providers=()):
    return SimpleNamespace(
        ingest=SimpleNamespace(
            anchor_date=anchor_date,
            excluded_providers=excluded_providers,
        ),
    )


def _make_row(osf_id, is_latest_version=None, **extra_attrs):
    attrs = {"date_created": "2026-02-01T00:00:00Z", "is_published": True}
    if is_latest_version is not None:
        attrs["is_latest_version"] = is_latest_version
    attrs.update(extra_attrs)
    return {
        "id": osf_id,
        "type": "preprints",
        "attributes": attrs,
        "links": {},
        "relationships": {
            "provider": {"data": {"id": "psyarxiv"}},
            "primary_file": {"data": {"id": "file1"}},
        },
    }


# ---------------------------------------------------------------------------
# _filter_ingest_rows: version filter
# ---------------------------------------------------------------------------
class FilterVersionTests(unittest.TestCase):
    def test_not_latest_version_filtered(self):
        rows = [_make_row("abc_v1", is_latest_version=False)]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg()):
            kept, _, _, _, skipped_version, records = upsert._filter_ingest_rows(rows)
        self.assertEqual(len(kept), 0)
        self.assertEqual(skipped_version, 1)
        self.assertEqual(records[0]["reason"], "not_latest_version")

    def test_latest_version_passes(self):
        rows = [_make_row("abc_v2", is_latest_version=True)]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg()):
            kept, _, _, _, skipped_version, _ = upsert._filter_ingest_rows(rows)
        self.assertEqual(len(kept), 1)
        self.assertEqual(skipped_version, 0)

    def test_missing_is_latest_version_passes(self):
        rows = [_make_row("abc_v2", is_latest_version=None)]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg()):
            kept, _, _, _, skipped_version, _ = upsert._filter_ingest_rows(rows)
        self.assertEqual(len(kept), 1)
        self.assertEqual(skipped_version, 0)

    def test_non_versioned_id_passes(self):
        rows = [_make_row("abc")]
        with patch.object(upsert, "RUNTIME_CONFIG", _cfg()):
            kept, _, _, _, skipped_version, _ = upsert._filter_ingest_rows(rows)
        self.assertEqual(len(kept), 1)
        self.assertEqual(skipped_version, 0)


# ---------------------------------------------------------------------------
# Fake DynamoDB helpers (following test_ingest_exclusion_reentry.py patterns)
# ---------------------------------------------------------------------------
class _FakeBatchWriter:
    def __init__(self):
        self.items = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def put_item(self, Item):
        self.items.append(Item)


class _FakeTable:
    def __init__(self, name="preprints"):
        self.name = name
        self.writer = _FakeBatchWriter()
        self._store = {}
        self._put_items = []
        self._update_items = []
        self.key_schema = [{"AttributeName": "osf_id", "KeyType": "HASH"}]

    def batch_writer(self, overwrite_by_pkeys=None):
        return self.writer

    def put_item(self, **kwargs):
        item = kwargs.get("Item", {})
        self._put_items.append(item)
        osf_id = item.get("osf_id")
        if osf_id:
            self._store[osf_id] = item

    def update_item(self, **kwargs):
        self._update_items.append(kwargs)


class _FakeDDB:
    """Fake DynamoDB resource supporting batch_get_item."""
    def __init__(self, preprints_data=None):
        self._preprints_data = preprints_data or {}

    def batch_get_item(self, RequestItems=None):
        responses = {}
        for table_name, req in (RequestItems or {}).items():
            keys = req.get("Keys", [])
            items = []
            for key in keys:
                osf_id = key.get("osf_id")
                if osf_id in self._preprints_data:
                    items.append(self._preprints_data[osf_id])
            responses[table_name] = items
        return {"Responses": responses, "UnprocessedKeys": {}}


def _make_repo(preprints_data=None, excluded_data=None):
    """Create a PreprintsRepo with faked DynamoDB tables."""
    repo = PreprintsRepo.__new__(PreprintsRepo)
    fake_ddb = _FakeDDB(preprints_data or {})
    repo.ddb = fake_ddb

    repo.t_preprints = _FakeTable("preprints")
    repo.t_preprints.name = "preprints"

    excluded_table = _FakeTable("excluded_preprints")
    excluded_table.name = "excluded_preprints"
    repo.t_excluded = excluded_table

    # Stub _fetch_excluded_reasons to return provided data
    repo._fetch_excluded_reasons = lambda ids: excluded_data or {}

    return repo


# ---------------------------------------------------------------------------
# upsert_preprints: sibling handling
# ---------------------------------------------------------------------------
class SiblingVersionTests(unittest.TestCase):
    def test_new_version_old_not_randomized_supersedes(self):
        """Old version exists, not randomized -> old excluded, new ingested fresh."""
        repo = _make_repo(
            preprints_data={
                "abc_v1": {"osf_id": "abc_v1", "trial_assignment_status": None, "excluded": None},
            },
        )
        rows = [_make_row("abc_v2", is_latest_version=True)]

        with patch.dict("os.environ", {"OSF_INGEST_SKIP_EXISTING": "false"}, clear=False):
            count = repo.upsert_preprints(rows)

        self.assertEqual(count, 1)
        # New version should be put_item (not update_item)
        put_ids = {item.get("osf_id") for item in repo.t_preprints._put_items}
        self.assertIn("abc_v2", put_ids)
        # Old sibling should be excluded
        excluded_put_ids = {item.get("osf_id") for item in repo.t_excluded._put_items}
        self.assertIn("abc_v1", excluded_put_ids)

    def test_new_version_old_randomized_rejects_new(self):
        """Old version exists AND is randomized -> new version gets excluded."""
        repo = _make_repo(
            preprints_data={
                "abc_v1": {"osf_id": "abc_v1", "trial_assignment_status": "assigned", "excluded": None},
            },
        )
        rows = [_make_row("abc_v2", is_latest_version=True)]

        with patch.dict("os.environ", {"OSF_INGEST_SKIP_EXISTING": "false"}, clear=False):
            count = repo.upsert_preprints(rows)

        # New version should NOT be ingested
        self.assertEqual(count, 0)
        put_ids = {item.get("osf_id") for item in repo.t_preprints._put_items}
        self.assertNotIn("abc_v2", put_ids)
        # New version should be excluded
        excluded_put_ids = {item.get("osf_id") for item in repo.t_excluded._put_items}
        self.assertIn("abc_v2", excluded_put_ids)

    def test_non_versioned_id_no_sibling_check(self):
        """Non-versioned ID passes through without sibling checks."""
        repo = _make_repo()
        rows = [_make_row("abc")]

        with patch.dict("os.environ", {"OSF_INGEST_SKIP_EXISTING": "false"}, clear=False):
            count = repo.upsert_preprints(rows)

        self.assertEqual(count, 1)
        put_ids = {item.get("osf_id") for item in repo.t_preprints._put_items}
        self.assertIn("abc", put_ids)

    def test_old_sibling_already_excluded_not_re_excluded(self):
        """Already-excluded sibling should not be excluded again."""
        repo = _make_repo(
            preprints_data={
                "abc_v1": {"osf_id": "abc_v1", "trial_assignment_status": None, "excluded": True},
            },
        )
        rows = [_make_row("abc_v2", is_latest_version=True)]

        with patch.dict("os.environ", {"OSF_INGEST_SKIP_EXISTING": "false"}, clear=False):
            count = repo.upsert_preprints(rows)

        self.assertEqual(count, 1)
        # abc_v1 should NOT appear in excluded_preprints writes (already excluded)
        excluded_put_ids = {item.get("osf_id") for item in repo.t_excluded._put_items}
        self.assertNotIn("abc_v1", excluded_put_ids)

    def test_no_existing_siblings_passes_through(self):
        """Versioned ID with no existing siblings ingests normally."""
        repo = _make_repo()
        rows = [_make_row("abc_v1", is_latest_version=True)]

        with patch.dict("os.environ", {"OSF_INGEST_SKIP_EXISTING": "false"}, clear=False):
            count = repo.upsert_preprints(rows)

        self.assertEqual(count, 1)
        put_ids = {item.get("osf_id") for item in repo.t_preprints._put_items}
        self.assertIn("abc_v1", put_ids)


if __name__ == "__main__":
    unittest.main()
