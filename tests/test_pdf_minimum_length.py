import unittest
from unittest.mock import patch

from osf_sync import pdf


class PdfMinimumLengthTests(unittest.TestCase):
    @patch("osf_sync.pdf.delete_preprint")
    @patch("osf_sync.pdf._pdf_length_status", return_value="too_short")
    @patch("osf_sync.pdf._download_to")
    @patch("osf_sync.pdf.resolve_primary_file_info_from_raw")
    def test_pdf_below_minimum_length_is_deleted(
        self,
        mock_resolve,
        _mock_download,
        _mock_minimum,
        mock_delete,
    ) -> None:
        mock_resolve.return_value = ("https://example.org/file.pdf", "application/pdf", "paper.pdf")

        kind, path, reason = pdf.ensure_pdf_available_or_delete(
            osf_id="p1",
            provider_id="psyarxiv",
            raw={"relationships": {"primary_file": {"data": {"id": "f1"}}}},
            dest_root="/tmp/flora_preprint_tests",
        )

        self.assertEqual(kind, "deleted")
        self.assertIsNone(path)
        self.assertEqual(reason, "pdf_below_minimum_length")
        mock_delete.assert_called_once_with("p1")

    @patch("osf_sync.pdf.delete_preprint")
    @patch("osf_sync.pdf._pdf_length_status", return_value="unreadable")
    @patch("osf_sync.pdf._download_to")
    @patch("osf_sync.pdf.resolve_primary_file_info_from_raw")
    def test_unreadable_pdf_passed_to_grobid(
        self,
        mock_resolve,
        _mock_download,
        _mock_length_status,
        mock_delete,
    ) -> None:
        mock_resolve.return_value = ("https://example.org/file.pdf", "application/pdf", "paper.pdf")

        kind, path, reason = pdf.ensure_pdf_available_or_delete(
            osf_id="p1",
            provider_id="psyarxiv",
            raw={"relationships": {"primary_file": {"data": {"id": "f1"}}}},
            dest_root="/tmp/flora_preprint_tests",
        )

        self.assertEqual(kind, "pdf")
        self.assertIsNotNone(path)
        self.assertIsNone(reason)
        mock_delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
