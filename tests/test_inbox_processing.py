"""Tests for osf_sync.email.inbox — IMAP bounce/unsubscribe processing."""
import email
import email.policy
import unittest
from unittest.mock import MagicMock, Mock, patch

from osf_sync.email.inbox import (
    _extract_bounce_addresses,
    _extract_unsub_sender,
    _parse_validation_body,
    process_inbox,
)


def _make_bounce_dsn(recipient: str = "user@example.com") -> email.message.Message:
    """Build a minimal multipart/report bounce with a delivery-status part."""
    raw = (
        "From: MAILER-DAEMON@google.com\r\n"
        "To: flora@replications.forrt.org\r\n"
        "Subject: Delivery Status Notification (Failure)\r\n"
        "Content-Type: multipart/report; report-type=delivery-status; boundary=\"bound\"\r\n"
        "\r\n"
        "--bound\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        "Your message was not delivered.\r\n"
        "--bound\r\n"
        "Content-Type: message/delivery-status\r\n"
        "\r\n"
        "Reporting-MTA: dns; google.com\r\n"
        "\r\n"
        f"Final-Recipient: rfc822;{recipient}\r\n"
        "Action: failed\r\n"
        "Status: 5.1.1\r\n"
        "--bound--\r\n"
    )
    return email.message_from_string(raw, policy=email.policy.default)


def _make_bounce_plaintext(recipient: str = "user@example.com") -> email.message.Message:
    """Build a plain-text bounce without structured delivery-status."""
    raw = (
        "From: MAILER-DAEMON@google.com\r\n"
        "To: flora@replications.forrt.org\r\n"
        "Subject: Delivery Status Notification (Failure)\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        f"Delivery to the following recipient failed: {recipient}\r\n"
        "Technical details: 550 No such user\r\n"
    )
    return email.message_from_string(raw, policy=email.policy.default)


def _make_unsub_message(sender: str = "bob@example.com") -> email.message.Message:
    raw = (
        f"From: {sender}\r\n"
        "To: flora@replications.forrt.org\r\n"
        "Subject: Unsubscribe\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        "Please unsubscribe me.\r\n"
    )
    return email.message_from_string(raw, policy=email.policy.default)


class TestExtractBounceAddresses(unittest.TestCase):
    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    def test_rfc3464_final_recipient(self, _) -> None:
        msg = _make_bounce_dsn("alice@uni.edu")
        addrs = _extract_bounce_addresses(msg)
        self.assertEqual(addrs, {"alice@uni.edu"})

    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    def test_plaintext_fallback(self, _) -> None:
        msg = _make_bounce_plaintext("bob@uni.edu")
        addrs = _extract_bounce_addresses(msg)
        self.assertIn("bob@uni.edu", addrs)

    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    def test_self_address_excluded(self, _) -> None:
        msg = _make_bounce_dsn("flora@replications.forrt.org")
        addrs = _extract_bounce_addresses(msg)
        self.assertEqual(addrs, set())


class TestExtractUnsubSender(unittest.TestCase):
    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    def test_extracts_sender(self, _) -> None:
        msg = _make_unsub_message("bob@example.com")
        addr = _extract_unsub_sender(msg)
        self.assertEqual(addr, "bob@example.com")

    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    def test_skips_self(self, _) -> None:
        msg = _make_unsub_message("flora@replications.forrt.org")
        addr = _extract_unsub_sender(msg)
        self.assertIsNone(addr)


class TestProcessInbox(unittest.TestCase):
    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    @patch("osf_sync.email.inbox._is_prod", return_value=False)
    @patch("osf_sync.dynamo.suppression_repo.SuppressionRepo")
    @patch("osf_sync.email.inbox.imaplib.IMAP4_SSL")
    @patch.dict("os.environ", {"GMAIL_APP_PASSWORD": "fake-pw"})
    def test_finds_bounces_and_unsubscribes(self, mock_imap_cls, mock_repo_cls, _is_prod, _sender) -> None:
        repo = Mock()
        repo.add_suppression.return_value = True
        mock_repo_cls.return_value = repo

        bounce_msg = _make_bounce_dsn("alice@uni.edu")
        unsub_msg = _make_unsub_message("bob@example.com")

        imap = MagicMock()
        mock_imap_cls.return_value = imap
        imap.select.return_value = ("OK", [b"1"])

        # First search returns bounce, second returns unsub
        def search_side_effect(_charset, *criteria):
            criteria_str = " ".join(criteria)
            if "mailer-daemon" in criteria_str.lower():
                return ("OK", [b"1"])
            if "Unsubscribe" in criteria_str:
                return ("OK", [b"2"])
            return ("OK", [b""])

        imap.search.side_effect = search_side_effect

        def fetch_side_effect(msg_id, _fmt):
            if msg_id == b"1":
                return ("OK", [(b"1", bounce_msg.as_bytes())])
            if msg_id == b"2":
                return ("OK", [(b"2", unsub_msg.as_bytes())])
            return ("OK", [None])

        imap.fetch.side_effect = fetch_side_effect

        stats = process_inbox()
        self.assertEqual(stats["bounces_found"], 1)
        self.assertEqual(stats["unsubscribes_found"], 1)
        self.assertEqual(stats["errors"], 0)

        # Verify suppression was written
        calls = repo.add_suppression.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].args, ("alice@uni.edu", "bounce"))
        self.assertEqual(calls[1].args, ("bob@example.com", "unsubscribe"))

    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    @patch("osf_sync.email.inbox._is_prod", return_value=False)
    @patch("osf_sync.dynamo.suppression_repo.SuppressionRepo")
    @patch("osf_sync.email.inbox.imaplib.IMAP4_SSL")
    @patch.dict("os.environ", {"GMAIL_APP_PASSWORD": "fake-pw"})
    def test_dry_run_does_not_write(self, mock_imap_cls, mock_repo_cls, _is_prod, _sender) -> None:
        repo = Mock()
        mock_repo_cls.return_value = repo

        bounce_msg = _make_bounce_dsn("alice@uni.edu")

        imap = MagicMock()
        mock_imap_cls.return_value = imap
        imap.select.return_value = ("OK", [b"1"])

        def search_side_effect(_charset, *criteria):
            criteria_str = " ".join(criteria)
            if "mailer-daemon" in criteria_str.lower():
                return ("OK", [b"1"])
            return ("OK", [b""])

        imap.search.side_effect = search_side_effect
        imap.fetch.return_value = ("OK", [(b"1", bounce_msg.as_bytes())])

        stats = process_inbox(dry_run=True)
        self.assertEqual(stats["bounces_found"], 1)
        self.assertTrue(stats["dry_run"])
        repo.add_suppression.assert_not_called()

    @patch.dict("os.environ", {"GMAIL_APP_PASSWORD": ""})
    def test_no_credentials_returns_skipped(self) -> None:
        stats = process_inbox()
        self.assertEqual(stats["skipped"], "no credentials")

    @patch("osf_sync.email.inbox._sender_address", return_value="flora@replications.forrt.org")
    @patch("osf_sync.email.inbox._is_prod", return_value=True)
    @patch("osf_sync.dynamo.suppression_repo.SuppressionRepo")
    @patch("osf_sync.email.inbox.imaplib.IMAP4_SSL")
    @patch.dict("os.environ", {"GMAIL_APP_PASSWORD": "fake-pw"})
    def test_prod_marks_messages_as_read(self, mock_imap_cls, mock_repo_cls, _is_prod, _sender) -> None:
        repo = Mock()
        repo.add_suppression.return_value = True
        mock_repo_cls.return_value = repo

        unsub_msg = _make_unsub_message("bob@example.com")

        imap = MagicMock()
        mock_imap_cls.return_value = imap
        imap.select.return_value = ("OK", [b"1"])

        def search_side_effect(_charset, *criteria):
            criteria_str = " ".join(criteria)
            if "mailer-daemon" in criteria_str.lower():
                return ("OK", [b""])
            if "Unsubscribe" in criteria_str:
                return ("OK", [b"5"])
            return ("OK", [b""])

        imap.search.side_effect = search_side_effect
        imap.fetch.return_value = ("OK", [(b"5", unsub_msg.as_bytes())])

        process_inbox()

        imap.store.assert_called_once_with(b"5", "+FLAGS", "\\Seen")


class TestParseValidationBody(unittest.TestCase):
    def test_approve_individual_ref(self) -> None:
        body = "APPROVE osf123/ref456\n\nReview ID: review-batch-abcd1234"
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(review_id, "review-batch-abcd1234")
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0], ("approve", "osf123/ref456", False))

    def test_reject_individual_ref(self) -> None:
        body = "REJECT osf123/ref456\n\nReview ID: review-batch-abcd1234"
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(decisions[0], ("reject", "osf123/ref456", False))

    def test_approve_use_apa(self) -> None:
        body = "APPROVE osf123/ref456 USE APA\n\nReview ID: review-batch-abcd1234"
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0], ("approve", "osf123/ref456", True))

    def test_approve_remainder(self) -> None:
        body = "APPROVE REMAINDER\n\nReview ID: review-batch-abcd1234"
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(decisions[0], ("approve", "REMAINDER", False))

    def test_approve_remainder_use_apa(self) -> None:
        body = "APPROVE REMAINDER USE APA\n\nReview ID: review-batch-abcd1234"
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(decisions[0], ("approve", "REMAINDER", True))

    def test_reject_remainder(self) -> None:
        body = "REJECT REMAINDER\n\nReview ID: review-batch-abcd1234"
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(decisions[0], ("reject", "REMAINDER", False))

    def test_mixed_individual_and_remainder(self) -> None:
        body = (
            "REJECT osf1/ref1\n"
            "APPROVE REMAINDER\n\n"
            "Review ID: review-batch-abcd1234"
        )
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(len(decisions), 2)
        self.assertEqual(decisions[0], ("reject", "osf1/ref1", False))
        self.assertEqual(decisions[1], ("approve", "REMAINDER", False))

    def test_multiple_individual_decisions(self) -> None:
        body = (
            "APPROVE osf1/ref1\n"
            "REJECT osf2/ref2\n"
            "APPROVE osf1/ref3 USE APA\n\n"
            "Review ID: review-batch-xyz12345"
        )
        review_id, decisions = _parse_validation_body(body)
        self.assertEqual(len(decisions), 3)
        self.assertEqual(decisions[0], ("approve", "osf1/ref1", False))
        self.assertEqual(decisions[1], ("reject", "osf2/ref2", False))
        self.assertEqual(decisions[2], ("approve", "osf1/ref3", True))

    def test_no_review_id(self) -> None:
        body = "APPROVE osf1/ref1\n"
        review_id, decisions = _parse_validation_body(body)
        self.assertIsNone(review_id)
        self.assertEqual(len(decisions), 1)


if __name__ == "__main__":
    unittest.main()
