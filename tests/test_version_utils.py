import unittest

from osf_sync.version_utils import parse_version, base_id, sibling_ids


class ParseVersionTests(unittest.TestCase):
    def test_standard_versioned(self):
        self.assertEqual(parse_version("pzgxw_v3"), ("pzgxw", 3))

    def test_version_one(self):
        self.assertEqual(parse_version("abc_v1"), ("abc", 1))

    def test_non_versioned(self):
        self.assertEqual(parse_version("pzgxw"), ("pzgxw", None))

    def test_single_char_base(self):
        self.assertEqual(parse_version("x_v2"), ("x", 2))

    def test_high_version(self):
        self.assertEqual(parse_version("foo_v99"), ("foo", 99))

    def test_base_with_underscore(self):
        self.assertEqual(parse_version("foo_bar_v5"), ("foo_bar", 5))


class BaseIdTests(unittest.TestCase):
    def test_versioned(self):
        self.assertEqual(base_id("pzgxw_v3"), "pzgxw")

    def test_non_versioned(self):
        self.assertEqual(base_id("pzgxw"), "pzgxw")


class SiblingIdsTests(unittest.TestCase):
    def test_excludes_own_version(self):
        sibs = sibling_ids("abc_v2", max_version=5)
        self.assertNotIn("abc_v2", sibs)
        self.assertEqual(sibs, ["abc_v1", "abc_v3", "abc_v4", "abc_v5"])

    def test_non_versioned_returns_empty(self):
        self.assertEqual(sibling_ids("abc"), [])

    def test_default_max_version(self):
        sibs = sibling_ids("abc_v1")
        self.assertEqual(len(sibs), 29)
        self.assertIn("abc_v30", sibs)
        self.assertNotIn("abc_v1", sibs)


if __name__ == "__main__":
    unittest.main()
