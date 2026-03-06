import unittest
import random

from osf_sync.author_randomization import (
    ComponentSummary,
    _load_unassigned_preprints,
    _choose_arm_for_new_cluster,
    assign_components_balanced,
    resolve_author_nodes_from_tokens,
    select_author_positions,
)


class AuthorRandomizationTests(unittest.TestCase):
    def test_select_author_positions_first4_plus_last(self) -> None:
        self.assertEqual(select_author_positions(20, []), [0, 1, 2, 3, 19])

    def test_select_author_positions_small_list(self) -> None:
        # 3 authors: first4+last collapses to all 3
        self.assertEqual(select_author_positions(3, []), [0, 1, 2])

    def test_select_author_positions_extra_positions_included(self) -> None:
        self.assertEqual(select_author_positions(20, [7, 12]), [0, 1, 2, 3, 19, 7, 12])

    def test_select_author_positions_duplicates_deduped(self) -> None:
        self.assertEqual(select_author_positions(20, [0, 19]), [0, 1, 2, 3, 19])

    def test_select_author_positions_out_of_bounds_filtered(self) -> None:
        self.assertEqual(select_author_positions(10, [15]), [0, 1, 2, 3, 9])

    def test_select_author_positions_five_authors(self) -> None:
        # 5 authors: first 4 + last = all 5
        self.assertEqual(select_author_positions(5, []), [0, 1, 2, 3, 4])

    def test_select_author_positions_six_authors(self) -> None:
        # 6 authors: first 4 (0-3) + last (5), position 4 excluded
        self.assertEqual(select_author_positions(6, []), [0, 1, 2, 3, 5])

    def test_resolve_nodes_no_phantom_initials_bridging(self) -> None:
        """Full-name records should NOT merge via computed initials alone."""
        token_groups = [
            ["namefull:smith|john"],
            ["namefull:smith|jane"],
            ["orcid:0000-0000-0000-0002", "namefull:smith|mary"],
        ]
        node_ids = resolve_author_nodes_from_tokens(token_groups)
        # John and Jane share initials but no initials-only record bridges them
        self.assertNotEqual(node_ids[0], node_ids[1])
        self.assertNotEqual(node_ids[0], node_ids[2])
        self.assertNotEqual(node_ids[1], node_ids[2])

    def test_resolve_nodes_initials_record_bridges_full_names(self) -> None:
        """An actual initials-only record should bridge compatible full-name records."""
        token_groups = [
            ["orcid:0000-0000-0000-0001", "nameinit:smith|j"],  # "J Smith"
            ["osf:abc123", "namefull:smith|john"],               # "John Smith"
            ["namefull:smith|jane"],                              # "Jane Smith"
            ["orcid:0000-0000-0000-0002", "namefull:smith|mary"],  # unrelated
        ]
        node_ids = resolve_author_nodes_from_tokens(token_groups)
        # J Smith bridges John and Jane
        self.assertEqual(node_ids[0], node_ids[1])
        self.assertEqual(node_ids[1], node_ids[2])
        # Mary is separate
        self.assertNotEqual(node_ids[0], node_ids[3])

    def test_assign_balances_contactable_preprints(self) -> None:
        components = {
            "C000001": ComponentSummary(
                cluster_id="C000001",
                node_ids=["N1"],
                preprint_ids=["p1"],
                stratum="psyarxiv",
                contactable_preprints=1,
                contactable_emails=9,
            ),
            "C000002": ComponentSummary(
                cluster_id="C000002",
                node_ids=["N2"],
                preprint_ids=["p2"],
                stratum="psyarxiv",
                contactable_preprints=1,
                contactable_emails=2,
            ),
            "C000003": ComponentSummary(
                cluster_id="C000003",
                node_ids=["N3"],
                preprint_ids=["p3"],
                stratum="psyarxiv",
                contactable_preprints=1,
                contactable_emails=1,
            ),
            "C000004": ComponentSummary(
                cluster_id="C000004",
                node_ids=["N4"],
                preprint_ids=["p4"],
                stratum="psyarxiv",
                contactable_preprints=1,
                contactable_emails=1,
            ),
        }

        assignments = assign_components_balanced(components, seed=17)

        treatment_preprints = 0
        control_preprints = 0
        for cluster_id, arm in assignments.items():
            if arm == "treatment":
                treatment_preprints += components[cluster_id].contactable_preprints
            else:
                control_preprints += components[cluster_id].contactable_preprints

        self.assertLessEqual(abs(treatment_preprints - control_preprints), 1)

    def test_new_cluster_tie_break_ignores_email_imbalance(self) -> None:
        clusters = {
            "C000001": {
                "cluster_id": "C000001",
                "stratum": "psyarxiv",
                "arm": "treatment",
                "preprints_total": 1,
                "contactable_preprints": 1,
                "contactable_emails": 10,
            },
            "C000002": {
                "cluster_id": "C000002",
                "stratum": "psyarxiv",
                "arm": "control",
                "preprints_total": 1,
                "contactable_preprints": 1,
                "contactable_emails": 1,
            },
        }

        # Preprint counts are tied (1 vs 1), so assignment should be random.
        # With seed=1, rng.choice(["treatment", "control"]) yields "treatment".
        arm = _choose_arm_for_new_cluster(
            clusters=clusters,
            stratum="psyarxiv",
            contactable_preprints=1,
            contactable_emails=1,
            rng=random.Random(1),
        )
        self.assertEqual(arm, "treatment")

    def test_load_unassigned_preprints_requires_flora_and_contactable_email(self) -> None:
        class _FakeRepo:
            def select_unassigned_preprints(self, limit=None):
                return [
                    {
                        "osf_id": "eligible",
                        "provider_id": "psyarxiv",
                        "date_created": "2026-02-01",
                        "flora_eligible": True,
                        "author_email_candidates": [{"email": "a@uni.edu", "position": 0}],
                    },
                    {
                        "osf_id": "no_flora",
                        "provider_id": "psyarxiv",
                        "date_created": "2026-02-01",
                        "flora_eligible": False,
                        "author_email_candidates": [{"email": "b@uni.edu", "position": 0}],
                    },
                    {
                        "osf_id": "no_email",
                        "provider_id": "psyarxiv",
                        "date_created": "2026-02-01",
                        "flora_eligible": True,
                        "author_email_candidates": [],
                    },
                ]

        author_rows = {
            "eligible": [{"osf.name": "Alice Example", "n": "1", "email.possible": "a@uni.edu"}],
            "no_flora": [{"osf.name": "Bob Example", "n": "1", "email.possible": "b@uni.edu"}],
            "no_email": [{"osf.name": "Cara Example", "n": "1"}],
        }

        out = _load_unassigned_preprints(
            _FakeRepo(),
            author_rows=author_rows,
            limit_preprints=None,
        )

        self.assertEqual([p.preprint_id for p in out], ["eligible"])


if __name__ == "__main__":
    unittest.main()
