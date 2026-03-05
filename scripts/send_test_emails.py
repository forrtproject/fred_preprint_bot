"""Send 3 sample emails to l.wallrich@bbk.ac.uk using real preprint + FLORA data.

Each email has a first line: "This would have been sent to [email address(es)]"
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from osf_sync.email.data_assembly import assemble_email_context
from osf_sync.email.gmail import send_email
from osf_sync.email.template import render_email

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TEST_RECIPIENT = "l.wallrich@bbk.ac.uk"

# Three real preprints with FLORA matches — picked for variety:
#  - mqpu2_v3: multi-recipient (2 authors)
#  - jbh7g_v1: 2 eligible FLORA refs
#  - zj6ey_v1: single recipient, 1 ref
TEST_OSF_IDS = ["mqpu2_v3", "jbh7g_v1", "zj6ey_v1"]

def inject_test_header(html_body: str, plain_body: str, original_emails: list[str]) -> tuple[str, str]:
    """Prepend a test header line showing the original recipients."""
    email_list = ", ".join(original_emails)
    header_plain = f"This would have been sent to {email_list}\n\n"
    header_html = f"<p><strong>This would have been sent to {email_list}</strong></p>\n"

    html_body = html_body.replace(
        '<body style="font-family: sans-serif; line-height: 1.5; color: #333; max-width: 700px;">',
        f'<body style="font-family: sans-serif; line-height: 1.5; color: #333; max-width: 700px;">\n{header_html}',
        1,
    )
    plain_body = header_plain + plain_body
    return html_body, plain_body


def main() -> None:
    for i, osf_id in enumerate(TEST_OSF_IDS, 1):
        context = assemble_email_context(osf_id)
        if context is None:
            logger.error("Could not assemble context for %s — skipping", osf_id)
            continue

        recipients = context.get("_recipients", [])
        original_emails = [r["email"] for r in recipients]

        subject, html_body, plain_body = render_email(context)
        html_body, plain_body = inject_test_header(html_body, plain_body, original_emails)

        subject = f"[TEST {i}/3] {subject}"

        logger.info(
            "Sending test email %d/3: %s -> %s (originally: %s)",
            i, subject[:60], TEST_RECIPIENT, ", ".join(original_emails),
        )

        result = send_email(
            to=TEST_RECIPIENT,
            subject=subject,
            html_body=html_body,
            plain_body=plain_body,
        )
        logger.info("Sent! Message ID: %s", result.get("id"))

    logger.info("All done — test emails sent to %s", TEST_RECIPIENT)


if __name__ == "__main__":
    main()
