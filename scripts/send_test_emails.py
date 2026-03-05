"""Send 5 sample emails to l.wallrich@bbk.ac.uk.

First two are synthetic (1 replication and 2 replications for same original).
Last three use real preprint + FLORA data.
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

_SYNTH_REPLICATION = {
    "full_reference": "Open Science Collaboration (2015). Estimating the reproducibility of psychological science. Science, 349(6251), aac4716.",
    "doi": "10.1126/science.aac4716",
    "doi_url": "https://doi.org/10.1126/science.aac4716",
    "oa_url": "https://osf.io/ez4st/",
    "outcome": "non-replication",
}


def build_synthetic_context(multi: bool) -> dict:
    """Build a minimal context dict for template testing — no DynamoDB needed."""
    replications = [_SYNTH_REPLICATION, _SYNTH_REPLICATION] if multi else [_SYNTH_REPLICATION]
    originals = [
        {
            "full_reference": "Bem, D. J. (2011). Feeling the future: Experimental evidence for anomalous retroactive influences on cognition and affect. Journal of Personality and Social Psychology, 100(3), 407–425.",
            "doi": "10.1037/a0021524",
            "doi_url": "https://doi.org/10.1037/a0021524",
            "replications": replications,
        }
    ]
    total = sum(len(o["replications"]) for o in originals)
    base = "https://forrt.org/marco/feedback"
    return {
        "preprint_title": "Synthetic Test Preprint",
        "server_name": "OSF Preprints",
        "author_greeting": "Dear Dr Smith,",
        "originals": originals,
        "total_replication_count": total,
        "some_replications_cited": False,
        "cited_replication_count": 0,
        "feedback_helpful_url": f"{base}?email_choice=helpful_singular&server_id=osf",
        "feedback_not_helpful_url": f"{base}?email_choice=not_helpful_singular&server_id=osf",
        "feedback_already_aware_url": f"{base}?email_choice=already_aware&server_id=osf",
        "feedback_report_error_url": f"{base}?email_choice=report_error_singular&server_id=osf",
        "unsubscribe_mailto": "mailto:flora-notify@forrt.org?subject=Unsubscribe&body=Please%20unsubscribe%20me",
        "_recipients": [{"email": TEST_RECIPIENT, "name": "Dr Smith"}],
    }


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
    # Synthetic emails (no DynamoDB needed)
    for i, multi in enumerate([False, True], 1):
        context = build_synthetic_context(multi)
        subject, html_body, plain_body = render_email(context)
        html_body, plain_body = inject_test_header(html_body, plain_body, [TEST_RECIPIENT])
        subject = f"[SYNTH {i}/2] {subject}"
        logger.info("Sending synthetic test email %d/2: %s", i, subject[:60])
        result = send_email(
            to=TEST_RECIPIENT,
            subject=subject,
            html_body=html_body,
            plain_body=plain_body,
        )
        logger.info("Sent! Message ID: %s", result.get("id"))

    # Real emails from DynamoDB
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

    logger.info("All done — 5 test emails sent to %s", TEST_RECIPIENT)


if __name__ == "__main__":
    main()
