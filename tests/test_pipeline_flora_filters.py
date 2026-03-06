import unittest
from unittest.mock import patch

from osf_sync import pipeline


class PipelineFloraFilterTests(unittest.TestCase):
    @patch("osf_sync.pipeline._slack")
    @patch("osf_sync.pipeline.lookup_and_screen_flora")
    def test_process_flora_batch_calls_lookup_and_screen(self, mock_lookup, _mock_slack) -> None:
        mock_lookup.return_value = {"lookup": {"preprints_checked": 1}, "screen": []}

        pipeline.process_flora_batch(
            limit_lookup=10,
            limit_screen=10,
            dry_run=False,
        )

        self.assertTrue(mock_lookup.called)
        self.assertEqual(mock_lookup.call_args.kwargs["limit"], 10)


if __name__ == "__main__":
    unittest.main()
