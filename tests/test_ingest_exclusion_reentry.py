import unittest
from unittest.mock import patch

from osf_sync.dynamo.preprints_repo import PreprintsRepo


class _FakeBatchWriter:
    def __init__(self) -> None:
        self.items = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def put_item(self, Item):
        self.items.append(Item)


class _FakePreprintsTable:
    def __init__(self) -> None:
        self.writer = _FakeBatchWriter()
        self.name = "preprints"
        self.key_schema = [{"AttributeName": "osf_id", "KeyType": "HASH"}]

    def batch_writer(self, overwrite_by_pkeys=None):
        return self.writer

    def put_item(self, Item=None, **kwargs):
        self.writer.items.append(Item)

    def update_item(self, **kwargs):
        pass


class IngestExclusionReentryTests(unittest.TestCase):
    def test_ingest_date_window_exclusion_can_reenter(self) -> None:
        repo = PreprintsRepo.__new__(PreprintsRepo)
        repo.t_preprints = _FakePreprintsTable()
        repo._fetch_existing_ids = lambda ids: set()
        repo._fetch_excluded_reasons = lambda ids: {
            "osf_date_window": "ingest_date_window",
            "osf_docx_fail": "docx_to_pdf_conversion_failed",
            "osf_permanent": "links_doi_not_osf_or_zenodo",
        }

        rows = [
            {
                "id": "osf_date_window",
                "type": "preprints",
                "attributes": {"is_published": True, "date_published": "2026-03-02"},
                "relationships": {"provider": {"data": {"id": "psyarxiv"}}, "primary_file": {"data": {"id": "file1"}}},
            },
            {
                "id": "osf_docx_fail",
                "type": "preprints",
                "attributes": {"is_published": True, "date_published": "2026-03-02"},
                "relationships": {"provider": {"data": {"id": "psyarxiv"}}, "primary_file": {"data": {"id": "file3"}}},
            },
            {
                "id": "osf_permanent",
                "type": "preprints",
                "attributes": {"is_published": True, "date_published": "2026-03-02"},
                "relationships": {"provider": {"data": {"id": "psyarxiv"}}, "primary_file": {"data": {"id": "file2"}}},
            },
        ]

        with patch.dict("os.environ", {"OSF_INGEST_SKIP_EXISTING": "false"}, clear=False):
            count = PreprintsRepo.upsert_preprints(repo, rows)

        self.assertEqual(count, 2)
        written_ids = {item.get("osf_id") for item in repo.t_preprints.writer.items}
        self.assertEqual(written_ids, {"osf_date_window", "osf_docx_fail"})


if __name__ == "__main__":
    unittest.main()
