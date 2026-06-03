from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import EmailTemplate


DEFAULT_TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "template_key": "account.verify_email",
        "name": "Account email verification",
        "category": "account",
        "from_name": "Walnut Intelligence Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Verify your Walnut Intelligence email",
        "preheader": "Confirm your email address for Walnut Market Terminal.",
        "variables": ["first_name", "verification_url", "expires_minutes"],
        "body_text": (
            "Hello {{first_name}},\n\n"
            "Please verify this email address for your Walnut Intelligence account.\n\n"
            "Verification link: {{verification_url}}\n\n"
            "This link expires in {{expires_minutes}} minutes. Walnut Market Terminal uses verified email for account security and service notices.\n\n"
            "If you did not create this account, you can ignore this message.\n\n"
            "Walnut Intelligence Support"
        ),
        "body_html": (
            "<p>Hello {{first_name}},</p>"
            "<p>Please verify this email address for your Walnut Intelligence account.</p>"
            "<p><a href=\"{{verification_url}}\">Verify your email</a></p>"
            "<p>This link expires in {{expires_minutes}} minutes. Walnut Market Terminal uses verified email for account security and service notices.</p>"
            "<p>If you did not create this account, you can ignore this message.</p>"
            "<p>Walnut Intelligence Support</p>"
        ),
    },
    {
        "template_key": "account.password_reset",
        "name": "Password reset instructions",
        "category": "account",
        "from_name": "Walnut Intelligence Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Reset your Walnut Intelligence password",
        "preheader": "Use this link to reset your Walnut Market Terminal password.",
        "variables": ["first_name", "reset_url", "expires_minutes"],
        "body_text": (
            "Hello {{first_name}},\n\n"
            "A password reset was requested for your Walnut Intelligence account.\n\n"
            "Reset link: {{reset_url}}\n\n"
            "This link expires in {{expires_minutes}} minutes. If you did not request this change, you can ignore this message.\n\n"
            "Walnut Intelligence Support"
        ),
        "body_html": (
            "<p>Hello {{first_name}},</p>"
            "<p>A password reset was requested for your Walnut Intelligence account.</p>"
            "<p><a href=\"{{reset_url}}\">Reset your password</a></p>"
            "<p>This link expires in {{expires_minutes}} minutes. If you did not request this change, you can ignore this message.</p>"
            "<p>Walnut Intelligence Support</p>"
        ),
    },
    {
        "template_key": "billing.monthly_statement",
        "name": "Monthly billing statement",
        "category": "billing",
        "from_name": "Walnut Intelligence Billing",
        "from_email": "billing@walnut-intel.com",
        "reply_to": "billing@walnut-intel.com",
        "subject": "Your Walnut Intelligence monthly statement",
        "preheader": "Your Walnut Market Terminal billing statement is available.",
        "variables": ["first_name", "statement_month", "statement_url", "amount_due", "currency"],
        "body_text": (
            "Hello {{first_name}},\n\n"
            "Your Walnut Intelligence statement for {{statement_month}} is available.\n\n"
            "Amount: {{amount_due}} {{currency}}\n"
            "Statement link: {{statement_url}}\n\n"
            "This statement reflects Walnut Market Terminal subscription activity for the period shown.\n\n"
            "Walnut Intelligence Billing"
        ),
        "body_html": (
            "<p>Hello {{first_name}},</p>"
            "<p>Your Walnut Intelligence statement for {{statement_month}} is available.</p>"
            "<p>Amount: {{amount_due}} {{currency}}</p>"
            "<p><a href=\"{{statement_url}}\">View statement</a></p>"
            "<p>This statement reflects Walnut Market Terminal subscription activity for the period shown.</p>"
            "<p>Walnut Intelligence Billing</p>"
        ),
    },
    {
        "template_key": "alerts.watchlist_activity",
        "name": "Watchlist activity alert",
        "category": "alerts",
        "from_name": "Walnut Intelligence Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Watchlist activity: {{watchlist_name}}",
        "preheader": "New activity matched your Walnut Market Terminal watchlist.",
        "variables": ["first_name", "watchlist_name", "summary", "activity_url"],
        "body_text": (
            "Hello {{first_name}},\n\n"
            "Walnut Market Terminal detected new activity for {{watchlist_name}}.\n\n"
            "{{summary}}\n\n"
            "Review: {{activity_url}}\n\n"
            "Walnut Intelligence Alerts"
        ),
        "body_html": (
            "<p>Hello {{first_name}},</p>"
            "<p>Walnut Market Terminal detected new activity for {{watchlist_name}}.</p>"
            "<p>{{summary}}</p>"
            "<p><a href=\"{{activity_url}}\">Review activity</a></p>"
            "<p>Walnut Intelligence Alerts</p>"
        ),
    },
    {
        "template_key": "alerts.signal_alert",
        "name": "Signal alert",
        "category": "alerts",
        "from_name": "Walnut Intelligence Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Signal alert: {{symbol}}",
        "preheader": "A monitored Walnut Market Terminal signal changed.",
        "variables": ["first_name", "symbol", "signal_title", "summary", "signal_url"],
        "body_text": (
            "Hello {{first_name}},\n\n"
            "{{signal_title}}\n\n"
            "{{summary}}\n\n"
            "Review: {{signal_url}}\n\n"
            "Walnut Intelligence Alerts"
        ),
        "body_html": (
            "<p>Hello {{first_name}},</p>"
            "<p><strong>{{signal_title}}</strong></p>"
            "<p>{{summary}}</p>"
            "<p><a href=\"{{signal_url}}\">Review signal</a></p>"
            "<p>Walnut Intelligence Alerts</p>"
        ),
    },
    {
        "template_key": "alerts.monitoring_digest",
        "name": "Monitoring digest",
        "category": "alerts",
        "from_name": "Walnut Intelligence Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut Market Terminal monitoring digest",
        "preheader": "A digest of recent monitoring activity.",
        "variables": ["first_name", "digest_date", "summary", "digest_url"],
        "body_text": (
            "Hello {{first_name}},\n\n"
            "Your Walnut Market Terminal monitoring digest for {{digest_date}} is ready.\n\n"
            "{{summary}}\n\n"
            "Review: {{digest_url}}\n\n"
            "Walnut Intelligence Alerts"
        ),
        "body_html": (
            "<p>Hello {{first_name}},</p>"
            "<p>Your Walnut Market Terminal monitoring digest for {{digest_date}} is ready.</p>"
            "<p>{{summary}}</p>"
            "<p><a href=\"{{digest_url}}\">Review digest</a></p>"
            "<p>Walnut Intelligence Alerts</p>"
        ),
    },
)


def seed_default_email_templates(db: Session) -> int:
    inserted = 0
    existing_keys = set(db.execute(select(EmailTemplate.template_key)).scalars().all())
    for template in DEFAULT_TEMPLATES:
        if template["template_key"] in existing_keys:
            continue
        db.add(
            EmailTemplate(
                template_key=template["template_key"],
                name=template["name"],
                category=template["category"],
                from_name=template["from_name"],
                from_email=template["from_email"],
                reply_to=template["reply_to"],
                subject=template["subject"],
                preheader=template["preheader"],
                body_text=template["body_text"],
                body_html=template["body_html"],
                variables_json=json.dumps(template["variables"]),
                enabled=True,
            )
        )
        inserted += 1
    if inserted:
        db.commit()
    return inserted
