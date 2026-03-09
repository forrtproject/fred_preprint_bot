import unittest
from unittest.mock import patch

from osf_sync.extraction.get_mail_from_pdf import (
    _extract_emails_from_text,
    _normalize_pdf_text_for_email_extraction,
)
from osf_sync.email.validation import repair_email_tld


class PdfEmailExtractionTests(unittest.TestCase):
    def test_recovers_email_with_bracketed_at_dot_tokens(self) -> None:
        text = "Contact: jane [at] uni [dot] edu"
        normalized = _normalize_pdf_text_for_email_extraction(text)
        emails = _extract_emails_from_text(normalized)
        self.assertIn("jane@uni.edu", [e.lower() for e in emails])

    def test_recovers_email_with_parenthesized_tokens(self) -> None:
        text = "Reach me at john (at) ed (dot) ac (dot) uk"
        normalized = _normalize_pdf_text_for_email_extraction(text)
        emails = _extract_emails_from_text(normalized)
        self.assertIn("john@ed.ac.uk", [e.lower() for e in emails])

    def test_recovers_email_with_spaces_around_symbols(self) -> None:
        text = "Contact: jane . doe @ uni . edu"
        normalized = _normalize_pdf_text_for_email_extraction(text)
        emails = _extract_emails_from_text(normalized)
        self.assertIn("jane.doe@uni.edu", [e.lower() for e in emails])

    def test_recovers_email_split_across_lines(self) -> None:
        text = "Reach me at\njohn.doe@ed.ac.\nuk for details"
        normalized = _normalize_pdf_text_for_email_extraction(text)
        emails = _extract_emails_from_text(normalized)
        self.assertIn("john.doe@ed.ac.uk", [e.lower() for e in emails])

    def test_discards_long_suffix_garbage_after_tld(self) -> None:
        text = "vasildinev@gmail.comBulgarianAcademyofSciences"
        normalized = _normalize_pdf_text_for_email_extraction(text)
        emails = _extract_emails_from_text(normalized)
        self.assertEqual(emails, [])

    def test_repairs_common_location_prefix_noise(self) -> None:
        text = "Berlin.cornelius.erfort@hu-berlin.de"
        normalized = _normalize_pdf_text_for_email_extraction(text)
        emails = _extract_emails_from_text(normalized)
        self.assertIn("cornelius.erfort@hu-berlin.de", [e.lower() for e in emails])

    def test_repairs_multi_word_camelcase_prefix(self) -> None:
        """UniversityCollegeLondonstnvcp1@ucl.ac.uk -> stnvcp1@ucl.ac.uk"""
        text = "UniversityCollegeLondonstnvcp1@ucl.ac.uk"
        emails = _extract_emails_from_text(text)
        self.assertIn("stnvcp1@ucl.ac.uk", [e.lower() for e in emails])

    def test_repairs_correspondence_prefix(self) -> None:
        """Pleaseaddresscorrespondencetojostarck@stanford.edu -> jostarck@stanford.edu"""
        text = "Pleaseaddresscorrespondencetojostarck@stanford.edu"
        emails = _extract_emails_from_text(text)
        self.assertIn("jostarck@stanford.edu", [e.lower() for e in emails])

    def test_repairs_addressed_to_with_name_prefix(self) -> None:
        """addressedtoChunWangchun_wang@njmu.edu.cn -> chun_wang@njmu.edu.cn"""
        text = "addressedtoChunWangchun_wang@njmu.edu.cn"
        emails = _extract_emails_from_text(text)
        self.assertIn("chun_wang@njmu.edu.cn", [e.lower() for e in emails])

    def test_repairs_duplicate_name_prefix(self) -> None:
        """AnastasiaRousakianastasia.rousaki@manchester.ac.uk -> anastasia.rousaki@..."""
        text = "AnastasiaRousakianastasia.rousaki@manchester.ac.uk"
        emails = _extract_emails_from_text(text)
        self.assertIn(
            "anastasia.rousaki@manchester.ac.uk", [e.lower() for e in emails]
        )


class RepairEmailTldTests(unittest.TestCase):
    """Tests for repair_email_tld, especially multi-part TLD recovery."""

    def _mock_validate(self, email, check_deliverability=True):
        """Simulate validate_email: accept known-good domains, reject others."""
        from email_validator import EmailNotValidError

        # Domains that "exist" in our mock
        valid_domains = {
            "york.ac.uk",
            "manchester.ac.uk",
            "ucl.ac.uk",
            "edgehill.ac.uk",
            "liverpool.ac.uk",
            "stanford.edu",
            "njmu.edu.cn",
            "syr.edu",
            "york.ac",  # .ac is a valid TLD (Ascension Island)
        }
        local, _, domain = email.rpartition("@")
        if domain.lower() in valid_domains:

            class Result:
                normalized = f"{local}@{domain.lower()}"

            return Result()
        raise EmailNotValidError("mock: domain not found")

    @patch("osf_sync.email.validation.validate_email")
    def test_prefers_ac_uk_over_ac(self, mock_val):
        """megan.frith@york.ac.ukDrChrisB.Stride → york.ac.uk, not york.ac"""
        mock_val.side_effect = self._mock_validate
        result = repair_email_tld("megan.frith@york.ac.ukDrChrisB.Stride")
        self.assertEqual(result, "megan.frith@york.ac.uk")

    @patch("osf_sync.email.validation.validate_email")
    def test_strips_suffix_after_ac_uk(self, mock_val):
        mock_val.side_effect = self._mock_validate
        result = repair_email_tld("Jessica.Talbot@edgehill.ac.uk15Dr.Daniele")
        self.assertEqual(result, "Jessica.Talbot@edgehill.ac.uk")

    @patch("osf_sync.email.validation.validate_email")
    def test_strips_abstract_suffix(self, mock_val):
        mock_val.side_effect = self._mock_validate
        result = repair_email_tld("stnvcp1@ucl.ac.ukABSTRACT.This")
        self.assertEqual(result, "stnvcp1@ucl.ac.uk")

    @patch("osf_sync.email.validation.validate_email")
    def test_strips_keywords_suffix(self, mock_val):
        mock_val.side_effect = self._mock_validate
        result = repair_email_tld("eboyland@liverpool.ac.uk.Keywords")
        self.assertEqual(result, "eboyland@liverpool.ac.uk")


if __name__ == "__main__":
    unittest.main()
