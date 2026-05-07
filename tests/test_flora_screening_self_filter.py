import unittest
from unittest.mock import MagicMock, patch

from osf_sync.augmentation import flora_screening as fs


class BuildSelfDoisTests(unittest.TestCase):
    def test_returns_empty_for_missing_or_empty_preprint(self) -> None:
        self.assertEqual(fs._build_self_dois(None), set())
        self.assertEqual(fs._build_self_dois({}), set())

    def test_includes_published_doi_when_set(self) -> None:
        out = fs._build_self_dois({"doi": "10.1177/08902070251410250"})
        self.assertIn("10.1177/08902070251410250", out)

    def test_expands_osf_doi_versions_from_preprint_doi_link(self) -> None:
        preprint = {
            "osf_id": "n9kgz_v2",
            "links": {"preprint_doi": "https://doi.org/10.31234/osf.io/n9kgz_v2"},
        }
        out = fs._build_self_dois(preprint)
        # Original variant
        self.assertIn("10.31234/osf.io/n9kgz_v2", out)
        # Unversioned base
        self.assertIn("10.31234/osf.io/n9kgz", out)
        # Sibling versions
        self.assertIn("10.31234/osf.io/n9kgz_v1", out)
        self.assertIn("10.31234/osf.io/n9kgz_v3", out)

    def test_handles_unversioned_osf_id(self) -> None:
        preprint = {
            "osf_id": "abcde",
            "links": {"preprint_doi": "https://doi.org/10.31235/osf.io/abcde"},
        }
        out = fs._build_self_dois(preprint)
        self.assertIn("10.31235/osf.io/abcde", out)
        # Also includes version siblings (FLoRA may register a versioned form later)
        self.assertIn("10.31235/osf.io/abcde_v1", out)

    def test_non_osf_preprint_doi_yields_only_that_doi(self) -> None:
        preprint = {"links": {"preprint_doi": "https://doi.org/10.1101/2024.01.01.000001"}}
        out = fs._build_self_dois(preprint)
        self.assertEqual(out, {"10.1101/2024.01.01.000001"})


class SelfReplicationFilterIntegrationTests(unittest.TestCase):
    """End-to-end test of _process_preprint dropping self-replication FLoRA pairs."""

    def _build_repo_mock(self, refs):
        repo = MagicMock()
        # _query_all_refs uses repo.t_refs.query
        page = {"Items": refs}
        repo.t_refs.query.return_value = page
        # _process_preprint fetches the preprint record
        repo.t_preprints.get_item.return_value = {
            "Item": {
                "osf_id": "n9kgz_v2",
                "doi": None,
                "links": {"preprint_doi": "https://doi.org/10.31234/osf.io/n9kgz_v2"},
            }
        }
        return repo

    @patch.object(fs, "_validate_eligible_ref_distances", return_value=[])
    @patch.object(fs, "_ensure_fresh_flora_csv", return_value={"downloaded": False})
    @patch.object(fs, "_load_flora_pairs_by_original")
    @patch.object(fs, "_resolve_flora_csv_path")
    @patch.object(fs, "PreprintsRepo")
    def test_self_only_replication_clears_state_and_flags_not_eligible(
        self,
        mock_repo_cls,
        mock_resolve,
        mock_load_pairs,
        _mock_ensure_fresh,
        _mock_validate,
    ) -> None:
        # FLoRA has one original (the cited ref). Both replications listed are self.
        mock_load_pairs.return_value = {
            "10.1037/pspa0000306": [
                {
                    "doi_o": "10.1037/pspa0000306",
                    "doi_r": "10.31234/osf.io/n9kgz_v2",  # self by OSF DOI
                    "apa_ref_o": "Achar & Lee (2022)...",
                    "apa_ref_r": "Self preprint",
                },
                {
                    "doi_o": "10.1037/pspa0000306",
                    "doi_r": "10.1177/08902070251410250",  # self by published DOI
                    "apa_ref_o": "Achar & Lee (2022)...",
                    "apa_ref_r": "Self published version",
                },
            ]
        }
        # The preprint cites the original
        refs = [{"osf_id": "n9kgz_v2", "ref_id": "b1", "doi": "10.1037/pspa0000306"}]
        repo = self._build_repo_mock(refs)
        # Add the published DOI to the preprint metadata so both replications
        # are detected as self.
        repo.t_preprints.get_item.return_value["Item"]["doi"] = "10.1177/08902070251410250"
        repo.select_preprints_for_flora_check.return_value = ["n9kgz_v2"]
        repo.filter_osf_ids_without_sent_email.return_value = {"n9kgz_v2"}
        mock_repo_cls.return_value = repo

        result = fs.lookup_and_screen_flora(
            limit=0,
            osf_id="n9kgz_v2",
            persist_flags=True,
            only_unchecked=False,
        )

        # On re-run, the stale self-only state must be overwritten with
        # empty ref_pairs and replication_cited=True so email assembly skips it.
        repo.update_reference_flora_result.assert_called_once()
        kwargs = repo.update_reference_flora_result.call_args.kwargs
        self.assertEqual(kwargs["ref_pairs"], [])
        self.assertTrue(kwargs["replication_cited"])

        # Preprint should be flagged as not eligible
        repo.update_preprint_flora_eligibility.assert_called_once()
        kwargs = repo.update_preprint_flora_eligibility.call_args.kwargs
        self.assertFalse(kwargs.get("eligible"))
        self.assertEqual(kwargs.get("eligible_count"), 0)

        screen = result["screen"][0]
        self.assertFalse(screen["eligible"])

    @patch.object(fs, "_validate_eligible_ref_distances", return_value=[])
    @patch.object(fs, "_ensure_fresh_flora_csv", return_value={"downloaded": False})
    @patch.object(fs, "_load_flora_pairs_by_original")
    @patch.object(fs, "_resolve_flora_csv_path")
    @patch.object(fs, "PreprintsRepo")
    def test_mixed_self_and_real_replication_persists_only_real(
        self,
        mock_repo_cls,
        mock_resolve,
        mock_load_pairs,
        _mock_ensure_fresh,
        _mock_validate,
    ) -> None:
        mock_load_pairs.return_value = {
            "10.1037/pspa0000306": [
                {
                    "doi_o": "10.1037/pspa0000306",
                    "doi_r": "10.31234/osf.io/n9kgz_v2",  # self
                    "apa_ref_r": "Self preprint",
                },
                {
                    "doi_o": "10.1037/pspa0000306",
                    "doi_r": "10.5555/somebody-else",  # genuine replication
                    "apa_ref_r": "Other replication",
                },
            ]
        }
        refs = [{"osf_id": "n9kgz_v2", "ref_id": "b1", "doi": "10.1037/pspa0000306"}]
        repo = self._build_repo_mock(refs)
        repo.select_preprints_for_flora_check.return_value = ["n9kgz_v2"]
        repo.filter_osf_ids_without_sent_email.return_value = {"n9kgz_v2"}
        mock_repo_cls.return_value = repo

        fs.lookup_and_screen_flora(
            limit=0,
            osf_id="n9kgz_v2",
            persist_flags=True,
            only_unchecked=False,
        )

        repo.update_reference_flora_result.assert_called_once()
        kwargs = repo.update_reference_flora_result.call_args.kwargs
        persisted_pairs = kwargs["ref_pairs"]
        self.assertEqual(len(persisted_pairs), 1)
        self.assertEqual(persisted_pairs[0]["doi_r"], "10.5555/somebody-else")


if __name__ == "__main__":
    unittest.main()
