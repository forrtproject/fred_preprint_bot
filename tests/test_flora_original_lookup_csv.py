import tempfile
import unittest
from pathlib import Path

from osf_sync.augmentation import flora_original_lookup as fol


class FloraOriginalLookupCsvTests(unittest.TestCase):
    def test_load_pairs_by_original_dedupes_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "flora.csv"
            csv_path.write_text(
                "\ufeff\"doi_o\",\"doi_r\",\"apa_ref_o\",\"apa_ref_r\",\"outcome\"\n"
                "\"10.1000/abc\",\"10.2000/rep\",\"O1\",\"R1\",\"successful\"\n"
                "\"10.1000/abc\",\"10.2000/rep\",\"O1\",\"R1\",\"successful\"\n"
                "\"10.1000/abc\",\"\",\"O1\",\"\",\"mixed\"\n",
                encoding="utf-8",
            )

            pairs = fol._load_flora_pairs_by_original(csv_path)

            self.assertIn("10.1000/abc", pairs)
            self.assertEqual(len(pairs["10.1000/abc"]), 2)
            self.assertEqual(pairs["10.1000/abc"][0]["doi_r"], "10.2000/rep")
            self.assertIsNone(pairs["10.1000/abc"][1]["doi_r"])

    def test_load_pairs_filters_non_protocol_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "flora.csv"
            csv_path.write_text(
                "\ufeff\"doi_o\",\"doi_r\",\"apa_ref_o\",\"apa_ref_r\",\"outcome\"\n"
                "\"10.1000/abc\",\"10.2000/rep1\",\"O1\",\"R1\",\"descriptive only\"\n"
                "\"10.1000/abc\",\"10.2000/rep2\",\"O1\",\"R2\",\"successful\"\n",
                encoding="utf-8",
            )

            pairs = fol._load_flora_pairs_by_original(csv_path)

            self.assertIn("10.1000/abc", pairs)
            self.assertEqual(len(pairs["10.1000/abc"]), 1)
            self.assertEqual(pairs["10.1000/abc"][0]["doi_r"], "10.2000/rep2")
            self.assertEqual(pairs["10.1000/abc"][0]["replication_outcome"], "successful")


if __name__ == "__main__":
    unittest.main()
