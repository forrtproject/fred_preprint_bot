import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from osf_sync.dynamo.preprints_repo import PreprintsRepo


class _FakeTable:
    def __init__(self, name: str) -> None:
        self.name = name
        self.put_calls = []
        self.update_calls = []
        self.items = {}

    def put_item(self, **kwargs):
        item = dict(kwargs.get("Item") or {})
        cond = kwargs.get("ConditionExpression")
        osf_id = item.get("osf_id")
        if cond == "attribute_not_exists(osf_id)" and osf_id in self.items:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}},
                "PutItem",
            )
        if osf_id:
            self.items[osf_id] = item
        self.put_calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def update_item(self, **kwargs):
        cond = kwargs.get("ConditionExpression", "")
        key = kwargs.get("Key", {})
        osf_id = key.get("osf_id")
        expr_vals = kwargs.get("ExpressionAttributeValues", {})
        # Simulate condition checks for items stored via _preprint_states
        if hasattr(self, "_preprint_states") and osf_id:
            state = self._preprint_states.get(osf_id)
            if state is None and "attribute_exists(osf_id)" in cond:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException", "Message": "not exists"}},
                    "UpdateItem",
                )
            if state is not None and "email_sent = :true" in cond:
                if state.get("email_sent") is not True:
                    raise ClientError(
                        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "email not sent"}},
                        "UpdateItem",
                    )
        self.update_calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, **kwargs):
        key = kwargs.get("Key") or {}
        osf_id = key.get("osf_id")
        if osf_id in self.items:
            return {"Item": self.items[osf_id]}
        return {}


class _FakeDynamo:
    def __init__(self) -> None:
        self._tables = {}

    def Table(self, name: str):
        if name not in self._tables:
            self._tables[name] = _FakeTable(name)
        return self._tables[name]


class PreprintExclusionTests(unittest.TestCase):
    @patch("osf_sync.dynamo.preprints_repo.get_dynamo_resource")
    def test_mark_preprint_excluded_inserts_single_row_per_osf_id(self, mock_get) -> None:
        fake = _FakeDynamo()
        mock_get.return_value = fake
        repo = PreprintsRepo()

        first = repo.mark_preprint_excluded(
            osf_id="osf123",
            reason="unsupported_file_format",
            stage="pdf",
            details={"provider_id": "psyarxiv"},
        )
        second = repo.mark_preprint_excluded(
            osf_id="osf123",
            reason="no_references_extracted",
            stage="extract",
        )

        self.assertTrue(first)
        self.assertFalse(second)

        excluded_table = fake._tables[repo.t_excluded.name]
        self.assertEqual(len(excluded_table.put_calls), 1)
        self.assertEqual(len(excluded_table.items), 1)

        item = excluded_table.items["osf123"]
        self.assertEqual(item["osf_id"], "osf123")
        self.assertEqual(item["exclusion_reason"], "unsupported_file_format")
        self.assertEqual(item["exclusion_stage"], "pdf")

    @patch("osf_sync.dynamo.preprints_repo.get_dynamo_resource")
    def test_mark_preprint_excluded_requires_reason_and_osf_id(self, mock_get) -> None:
        fake = _FakeDynamo()
        mock_get.return_value = fake
        repo = PreprintsRepo()

        with self.assertRaises(ValueError):
            repo.mark_preprint_excluded(osf_id="", reason="x")
        with self.assertRaises(ValueError):
            repo.mark_preprint_excluded(osf_id="abc", reason=" ")


    @patch("osf_sync.dynamo.preprints_repo.get_dynamo_resource")
    def test_exclude_emailed_preprint_preserves_queue_email(self, mock_get) -> None:
        """When a preprint has email_sent=True, queue_email must NOT be removed."""
        fake = _FakeDynamo()
        mock_get.return_value = fake
        repo = PreprintsRepo()

        # Simulate preprint with email_sent=True
        preprints_table = fake.Table(repo.t_preprints.name)
        preprints_table._preprint_states = {"emailed1": {"email_sent": True}}

        result = repo.mark_preprint_excluded(
            osf_id="emailed1", reason="superseded_by_newer_version", stage="sync",
        )
        self.assertTrue(result)

        # The first update_item call should succeed (email_sent=True path)
        # and queue_email should NOT appear in the REMOVE expression
        update_call = preprints_table.update_calls[-1]
        update_expr = update_call["UpdateExpression"]
        self.assertNotIn("queue_email", update_expr)

    @patch("osf_sync.dynamo.preprints_repo.get_dynamo_resource")
    def test_exclude_non_emailed_preprint_removes_queue_email(self, mock_get) -> None:
        """When a preprint has NOT been emailed, queue_email is removed normally."""
        fake = _FakeDynamo()
        mock_get.return_value = fake
        repo = PreprintsRepo()

        # Simulate preprint without email_sent
        preprints_table = fake.Table(repo.t_preprints.name)
        preprints_table._preprint_states = {"nomail1": {"email_sent": False}}

        result = repo.mark_preprint_excluded(
            osf_id="nomail1", reason="no_references_extracted", stage="extract",
        )
        self.assertTrue(result)

        # The second update_item call should succeed (fallback path)
        # and queue_email SHOULD appear in the REMOVE expression
        update_call = preprints_table.update_calls[-1]
        update_expr = update_call["UpdateExpression"]
        self.assertIn("queue_email", update_expr)


if __name__ == "__main__":
    unittest.main()
