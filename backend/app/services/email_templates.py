from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import EmailTemplate


BRAND_NAME = "Walnut Intelligence"
PRODUCT_NAME = "Walnut Market Terminal"
SUPPORT_EMAIL = "support@walnut-intel.com"
DISCLAIMER = "Informational and research purposes only. Not investment advice."


def _text_email(*, greeting: str, intro: str, sections: list[str], cta_label: str | None = None, cta_url: str | None = None) -> str:
    lines = [greeting, "", intro]
    for section in sections:
        if section:
            lines.extend(["", section])
    if cta_label and cta_url:
        lines.extend(["", f"{cta_label}: {{{{{cta_url}}}}}"])
    lines.extend(["", "Walnut Intelligence Inc.", SUPPORT_EMAIL, DISCLAIMER])
    return "\n".join(lines)


def _html_email(*, eyebrow: str, title: str, intro: str, sections: list[str], cta_label: str | None = None, cta_url: str | None = None) -> str:
    cta = (
        f"""
        <tr>
          <td style="padding:18px 0 4px 0;">
            <a href="{{{{{cta_url}}}}}" style="display:inline-block;background:#14b8a6;color:#051316;text-decoration:none;font-weight:700;border-radius:6px;padding:12px 18px;">{cta_label}</a>
          </td>
        </tr>
        """
        if cta_label and cta_url
        else ""
    )
    body_sections = "".join(
        f'<tr><td style="padding:10px 0;color:#334155;font-size:14px;line-height:1.65;">{section}</td></tr>'
        for section in sections
        if section
    )
    return f"""<!doctype html>
<html>
  <body style="margin:0;background:#f3f7f8;padding:0;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f7f8;margin:0;padding:24px 0;">
      <tr>
        <td align="center" style="padding:0 12px;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background:#ffffff;border:1px solid #dbe6ea;border-radius:8px;overflow:hidden;">
            <tr>
              <td style="background:#071114;padding:22px 26px;border-bottom:3px solid #14b8a6;">
                <div style="font-size:18px;font-weight:800;color:#ffffff;letter-spacing:0;">Walnut Intelligence</div>
                <div style="margin-top:4px;font-size:12px;color:#8ddbd2;">Walnut Market Terminal</div>
              </td>
            </tr>
            <tr>
              <td style="padding:26px;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                  <tr><td style="font-size:12px;font-weight:700;text-transform:uppercase;color:#0f766e;">{eyebrow}</td></tr>
                  <tr><td style="padding-top:8px;font-size:24px;line-height:1.25;font-weight:800;color:#0f172a;">{title}</td></tr>
                  <tr><td style="padding-top:14px;color:#334155;font-size:15px;line-height:1.65;">{intro}</td></tr>
                  {body_sections}
                  {cta}
                </table>
              </td>
            </tr>
            <tr>
              <td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:18px 26px;color:#64748b;font-size:12px;line-height:1.6;">
                <div style="font-weight:700;color:#334155;">Walnut Intelligence Inc.</div>
                <div><a href="mailto:{SUPPORT_EMAIL}" style="color:#0f766e;text-decoration:none;">{SUPPORT_EMAIL}</a></div>
                <div style="margin-top:8px;">{DISCLAIMER}</div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""


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
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="Welcome to Walnut Intelligence. Please verify this email address for your account.",
            sections=[
                "This link expires in {{expires_minutes}} minutes.",
                "Email verification helps protect your account and enables Walnut notifications.",
                "If you did not create this account, you can ignore this message.",
            ],
            cta_label="Verify email",
            cta_url="verification_url",
        ),
        "body_html": _html_email(
            eyebrow="Account verification",
            title="Verify your Walnut Intelligence email",
            intro="Hello {{first_name}}, welcome to Walnut Intelligence. Please verify this email address for your account.",
            sections=[
                "This link expires in {{expires_minutes}} minutes.",
                "Email verification helps protect your account and enables Walnut notifications.",
                "If you did not create this account, you can ignore this message.",
            ],
            cta_label="Verify email",
            cta_url="verification_url",
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
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="A password reset was requested for your Walnut Intelligence account.",
            sections=[
                "This link expires in {{expires_minutes}} minutes.",
                "If you did not request this change, you can ignore this message.",
            ],
            cta_label="Reset password",
            cta_url="reset_url",
        ),
        "body_html": _html_email(
            eyebrow="Password reset",
            title="Reset your Walnut Intelligence password",
            intro="Hello {{first_name}}, a password reset was requested for your Walnut Intelligence account.",
            sections=[
                "This link expires in {{expires_minutes}} minutes.",
                "If you did not request this change, you can ignore this message.",
            ],
            cta_label="Reset password",
            cta_url="reset_url",
        ),
    },
    {
        "template_key": "account.password_changed",
        "name": "Password changed confirmation",
        "category": "account",
        "from_name": "Walnut Intelligence Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Your Walnut Intelligence password was changed",
        "preheader": "A confirmation that your account password was changed.",
        "variables": ["first_name", "changed_at", "support_email", "login_url"],
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="The password for your Walnut Intelligence account was changed on {{changed_at}}.",
            sections=[
                "If you made this change, no action is needed.",
                "If you did not make this change, contact support immediately at {{support_email}}.",
            ],
            cta_label="Sign in",
            cta_url="login_url",
        ),
        "body_html": _html_email(
            eyebrow="Account security",
            title="Your Walnut Intelligence password was changed",
            intro="Hello {{first_name}}, the password for your Walnut Intelligence account was changed on {{changed_at}}.",
            sections=[
                "If you made this change, no action is needed.",
                "If you did not make this change, contact support immediately at {{support_email}}.",
            ],
            cta_label="Sign in",
            cta_url="login_url",
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
        "variables": ["first_name", "billing_period", "plan", "amount_due", "currency", "payment_status", "statement_url"],
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="Your Walnut Intelligence monthly statement is ready.",
            sections=[
                "Billing period: {{billing_period}}\nPlan: {{plan}}\nAmount: {{amount_due}} {{currency}}\nPayment status: {{payment_status}}",
                "Questions about billing? Contact support@walnut-intel.com.",
            ],
            cta_label="View statement",
            cta_url="statement_url",
        ),
        "body_html": _html_email(
            eyebrow="Billing",
            title="Your Walnut Intelligence monthly statement",
            intro="Hello {{first_name}}, your Walnut Intelligence monthly statement is ready.",
            sections=[
                "<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;\"><tr><td style=\"padding:10px 12px;color:#475569;\">Billing period</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{billing_period}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Plan</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{plan}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Amount</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{amount_due}} {{currency}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Payment status</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{payment_status}}</td></tr></table>",
                "Questions about billing? Contact support@walnut-intel.com.",
            ],
            cta_label="View statement",
            cta_url="statement_url",
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
        "variables": ["first_name", "watchlist_name", "summary", "items_text", "items_html", "activity_url"],
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="Walnut Market Terminal detected new filings or events for {{watchlist_name}}.",
            sections=["{{summary}}", "{{items_text}}"],
            cta_label="Review watchlist",
            cta_url="activity_url",
        ),
        "body_html": _html_email(
            eyebrow="Watchlist activity",
            title="Watchlist activity: {{watchlist_name}}",
            intro="Hello {{first_name}}, Walnut Market Terminal detected new filings or events for {{watchlist_name}}.",
            sections=["{{summary}}", "{{{items_html}}}"],
            cta_label="Review watchlist",
            cta_url="activity_url",
        ),
    },
    {
        "template_key": "alerts.signal_alert",
        "name": "Signal alert",
        "category": "alerts",
        "from_name": "Walnut Intelligence Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut signal alert: {{ticker}}",
        "preheader": "A monitored Walnut Market Terminal signal changed.",
        "variables": ["first_name", "ticker", "signal_score", "direction", "why_notable", "source_stack", "cautions", "signals_text", "signals_html", "signal_url"],
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="Walnut detected a notable signal for {{ticker}}.",
            sections=[
                "Signal score: {{signal_score}}\nDirection: {{direction}}\nWhy notable: {{why_notable}}\nSource stack: {{source_stack}}\nCautions: {{cautions}}",
                "{{signals_text}}",
            ],
            cta_label="View signal",
            cta_url="signal_url",
        ),
        "body_html": _html_email(
            eyebrow="Signal alert",
            title="Walnut signal alert: {{ticker}}",
            intro="Hello {{first_name}}, Walnut detected a notable signal for {{ticker}}.",
            sections=[
                "<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;\"><tr><td style=\"padding:10px 12px;color:#475569;\">Signal score</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{signal_score}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Direction</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{direction}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Why notable</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{why_notable}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Source stack</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{source_stack}}</td></tr><tr><td style=\"padding:10px 12px;color:#475569;\">Cautions</td><td style=\"padding:10px 12px;color:#0f172a;font-weight:700;\">{{cautions}}</td></tr></table>",
                "{{{signals_html}}}",
            ],
            cta_label="View signal",
            cta_url="signal_url",
        ),
    },
    {
        "template_key": "alerts.monitoring_digest",
        "name": "Monitoring digest",
        "category": "alerts",
        "from_name": "Walnut Intelligence Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut monitoring digest: {{watchlist_name}}",
        "preheader": "A digest of recent monitoring activity.",
        "variables": ["first_name", "watchlist_name", "digest_date", "summary", "items_text", "items_html", "digest_url"],
        "body_text": _text_email(
            greeting="Hello {{first_name}},",
            intro="Your Walnut monitoring digest for {{watchlist_name}} is ready.",
            sections=["Digest date: {{digest_date}}", "{{summary}}", "{{items_text}}"],
            cta_label="Review monitoring activity",
            cta_url="digest_url",
        ),
        "body_html": _html_email(
            eyebrow="Monitoring digest",
            title="Walnut monitoring digest: {{watchlist_name}}",
            intro="Hello {{first_name}}, your Walnut monitoring digest is ready.",
            sections=["Digest date: {{digest_date}}", "{{summary}}", "{{{items_html}}}"],
            cta_label="Review monitoring activity",
            cta_url="digest_url",
        ),
    },
)

DEFAULT_TEMPLATE_BY_KEY = {str(template["template_key"]): template for template in DEFAULT_TEMPLATES}


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


def default_email_template(template_key: str) -> dict[str, Any] | None:
    return DEFAULT_TEMPLATE_BY_KEY.get(template_key)


def reset_email_template_to_default(db: Session, template_key: str) -> EmailTemplate | None:
    default = default_email_template(template_key)
    if default is None:
        return None
    template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == template_key)).scalar_one_or_none()
    if template is None:
        template = EmailTemplate(template_key=template_key, enabled=True)
        db.add(template)
    for field in (
        "name",
        "category",
        "from_name",
        "from_email",
        "reply_to",
        "subject",
        "preheader",
        "body_text",
        "body_html",
    ):
        setattr(template, field, default[field])
    template.variables_json = json.dumps(default["variables"])
    template.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(template)
    return template
