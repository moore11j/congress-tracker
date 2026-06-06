from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import EmailTemplate


COMPANY_NAME = "Walnut Intelligence Inc."
BRAND_NAME = "Walnut"
PRODUCT_NAME = "Market Terminal"
PRODUCT_FULL_NAME = "Walnut Market Terminal"
SUPPORT_EMAIL = "support@walnut-intel.com"
SUPPORT_URL = "https://walnut-intel.com"
APP_URL = "https://app.walnut-intel.com"
INVESTMENT_DISCLAIMER = (
    "This email is for informational and research purposes only and does not constitute investment advice."
)
ACCOUNT_NOTICE = (
    "You are receiving this email because you have a Walnut account. "
    "For security, Walnut will never ask for your password or verification code by email."
)
NOTIFICATION_NOTICE = (
    "You are receiving this email because you have a Walnut account or enabled "
    f"{PRODUCT_FULL_NAME} notifications. Manage notifications in Account Settings."
)


def walnut_button(label: str, url_variable: str) -> str:
    return f"""
      <table role="presentation" cellspacing="0" cellpadding="0" style="margin:22px 0 4px 0;">
        <tr>
          <td style="border-radius:6px;background:#14d6a3;">
            <a href="{{{{{url_variable}}}}}" style="display:inline-block;padding:13px 18px;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:18px;font-weight:700;color:#05201c;text-decoration:none;border-radius:6px;">
              {label}
            </a>
          </td>
        </tr>
      </table>
    """


def walnut_info_card(title: str, body: str) -> str:
    return f"""
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:18px 0 0 0;border-collapse:separate;background:#f7fbfc;border:1px solid #d8e6ea;border-radius:7px;">
        <tr>
          <td style="padding:15px 16px;border-left:4px solid #14d6a3;">
            <div style="font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:16px;font-weight:700;text-transform:uppercase;color:#0f766e;">{title}</div>
            <div style="margin-top:7px;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:#334155;">{body}</div>
          </td>
        </tr>
      </table>
    """


def walnut_metric_card(rows: list[tuple[str, str]]) -> str:
    body = "".join(
        f"""
        <tr>
          <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:18px;font-weight:700;text-transform:uppercase;color:#64748b;">{label}</td>
          <td align="right" style="padding:10px 12px;border-bottom:1px solid #e2e8f0;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:20px;font-weight:700;color:#0f172a;">{value}</td>
        </tr>
        """
        for label, value in rows
    )
    return f"""
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:18px 0 0 0;border-collapse:collapse;background:#f8fafc;border:1px solid #d8e6ea;border-radius:7px;overflow:hidden;">
        {body}
      </table>
    """


def _walnut_mark_html() -> str:
    return """
      <table role="presentation" width="44" height="44" cellspacing="0" cellpadding="0" style="width:44px;height:44px;border-collapse:separate;background:#081316;border:1px solid #1f3a3f;border-radius:8px;">
        <tr>
          <td align="center" valign="middle" style="font-family:Arial,Helvetica,sans-serif;font-size:22px;line-height:22px;font-weight:800;color:#2dd4bf;">
            W
          </td>
        </tr>
      </table>
    """


def walnut_signature_footer(*, sender: str, include_investment_disclaimer: bool) -> str:
    legal_line = f"{COMPANY_NAME} operates {PRODUCT_FULL_NAME}."
    legal_footer = (
        f"{legal_line} {INVESTMENT_DISCLAIMER}"
        if include_investment_disclaimer
        else legal_line
    )
    notice = NOTIFICATION_NOTICE if include_investment_disclaimer else ACCOUNT_NOTICE
    return f"""
      <tr>
        <td style="background:#f8fafc;border-top:1px solid #dbe6ea;padding:20px 24px;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
            <tr>
              <td width="52" valign="top" style="width:52px;padding:0 12px 0 0;">
                {_walnut_mark_html()}
              </td>
              <td width="1" style="width:1px;background:#14d6a3;font-size:1px;line-height:1px;">&nbsp;</td>
              <td valign="top" style="padding:0 0 0 14px;font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:18px;color:#475569;">
                <div style="font-size:13px;line-height:18px;font-weight:800;color:#0f172a;">{sender}</div>
                <div style="font-size:12px;line-height:18px;color:#334155;">{PRODUCT_NAME}</div>
                <div style="margin-top:7px;">
                  <a href="mailto:{SUPPORT_EMAIL}" style="color:#0f766e;text-decoration:none;">{SUPPORT_EMAIL}</a>
                  <span style="color:#94a3b8;"> | </span>
                  <a href="{SUPPORT_URL}" style="color:#0f766e;text-decoration:none;">walnut-intel.com</a>
                  <span style="color:#94a3b8;"> | </span>
                  <a href="{APP_URL}" style="color:#0f766e;text-decoration:none;">Launch Terminal</a>
                </div>
                <div style="margin-top:10px;color:#64748b;">{legal_footer}</div>
                <div style="margin-top:8px;color:#64748b;">{notice}</div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    """


def walnut_email_html(
    *,
    sender: str,
    eyebrow: str,
    title: str,
    intro: str,
    content_html: str,
    cta_label: str | None = None,
    cta_url: str | None = None,
    include_investment_disclaimer: bool = False,
) -> str:
    cta = walnut_button(cta_label, cta_url) if cta_label and cta_url else ""
    sender_label = sender.replace("Walnut ", "", 1)
    return f"""<!doctype html>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
  </head>
  <body style="margin:0;padding:0;background:#eef3f7;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:0;padding:24px 0;background:#eef3f7;">
      <tr>
        <td align="center" style="padding:0 12px;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="width:100%;max-width:640px;border-collapse:separate;background:#ffffff;border:1px solid #d8e2ea;border-radius:8px;overflow:hidden;">
            <tr>
              <td style="padding:20px 24px;background:#ffffff;border-bottom:1px solid #dbe6ea;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                  <tr>
                    <td width="52" valign="middle" style="width:52px;padding:0 12px 0 0;">
                      {_walnut_mark_html()}
                    </td>
                    <td width="1" style="width:1px;background:#14d6a3;font-size:1px;line-height:1px;">&nbsp;</td>
                    <td valign="middle" style="padding:0 0 0 14px;">
                      <div style="font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:20px;font-weight:800;color:#071114;">{BRAND_NAME}</div>
                      <div style="margin-top:3px;font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:16px;font-weight:700;color:#0f766e;">{PRODUCT_NAME}</div>
                    </td>
                    <td align="right" valign="middle" style="font-family:Arial,Helvetica,sans-serif;font-size:11px;line-height:15px;font-weight:700;text-transform:uppercase;color:#64748b;">
                      {sender_label}
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="background:#071114;border-bottom:3px solid #14d6a3;padding:10px 24px;">
                <div style="font-family:Arial,Helvetica,sans-serif;font-size:11px;line-height:16px;font-weight:700;text-transform:uppercase;color:#99f6e4;">{eyebrow}</div>
              </td>
            </tr>
            <tr>
              <td style="padding:28px 24px 24px 24px;background:#ffffff;">
                <h1 style="margin:0;font-family:Arial,Helvetica,sans-serif;font-size:24px;line-height:31px;font-weight:800;color:#0f172a;letter-spacing:0;">{title}</h1>
                <p style="margin:14px 0 0 0;font-family:Arial,Helvetica,sans-serif;font-size:15px;line-height:24px;color:#334155;">{intro}</p>
                {content_html}
                {cta}
              </td>
            </tr>
            {walnut_signature_footer(sender=sender, include_investment_disclaimer=include_investment_disclaimer)}
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""


def walnut_email_text(
    *,
    greeting: str,
    intro: str,
    sections: list[str],
    cta_label: str | None = None,
    cta_url: str | None = None,
    sender: str,
    include_investment_disclaimer: bool = False,
) -> str:
    lines = [greeting, "", intro]
    for section in sections:
        if section:
            lines.extend(["", section])
    if cta_label and cta_url:
        lines.extend(["", f"{cta_label}: {{{{{cta_url}}}}}"])
    legal_line = f"{COMPANY_NAME} operates {PRODUCT_FULL_NAME}."
    lines.extend(["", sender, PRODUCT_NAME, f"{SUPPORT_EMAIL} | walnut-intel.com | Launch Terminal: {APP_URL}"])
    if include_investment_disclaimer:
        lines.extend(["", f"{legal_line} {INVESTMENT_DISCLAIMER}", NOTIFICATION_NOTICE])
    else:
        lines.extend(["", legal_line, ACCOUNT_NOTICE])
    return "\n".join(lines)


DEFAULT_TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "template_key": "account.verify_email",
        "name": "Verify your email",
        "category": "account",
        "from_name": "Walnut Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Verify your Walnut email",
        "preheader": "Verify your email address for Walnut Market Terminal.",
        "variables": ["first_name", "verification_url", "expires_minutes"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="Welcome to Walnut. Verify your email to secure your account and enable account notifications.",
            sections=[
                "This link expires in {{expires_minutes}} minutes.",
                "If you did not create this account, you can ignore this message.",
            ],
            cta_label="Verify email",
            cta_url="verification_url",
            sender="Walnut Support",
        ),
        "body_html": walnut_email_html(
            sender="Walnut Support",
            eyebrow="Verify your email",
            title="Verify your Walnut email",
            intro="Hello {{first_name}}, welcome to Walnut. Verify your email to secure your account and enable account notifications.",
            content_html=walnut_info_card(
                "Expires",
                "This verification link expires in {{expires_minutes}} minutes. If you did not create this account, you can ignore this message.",
            ),
            cta_label="Verify email",
            cta_url="verification_url",
        ),
    },
    {
        "template_key": "account.welcome",
        "name": "Welcome to Walnut",
        "category": "account",
        "from_name": "Walnut Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Welcome to Walnut",
        "preheader": "Your Walnut Market Terminal account is ready.",
        "variables": ["first_name", "app_url", "support_email"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="Welcome to Walnut. Your Walnut Market Terminal account is ready.",
            sections=[
                "Launch the terminal to review market signals, watchlists, and source-backed research.",
                "Walnut Market Terminal is for informational and research purposes only and is not investment advice.",
                "Questions? Contact {{support_email}}.",
            ],
            cta_label="Launch Terminal",
            cta_url="app_url",
            sender="Walnut Support",
        ),
        "body_html": walnut_email_html(
            sender="Walnut Support",
            eyebrow="Welcome",
            title="Welcome to Walnut",
            intro="Hello {{first_name}}, welcome to Walnut. Your Walnut Market Terminal account is ready.",
            content_html=walnut_info_card(
                "Research reminder",
                "Walnut Market Terminal is for informational and research purposes only and is not investment advice. Questions? Contact {{support_email}}.",
            ),
            cta_label="Launch Terminal",
            cta_url="app_url",
        ),
    },
    {
        "template_key": "account.password_reset",
        "name": "Password reset instructions",
        "category": "account",
        "from_name": "Walnut Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Reset your Walnut password",
        "preheader": "Use this link to reset your Walnut Market Terminal password.",
        "variables": ["first_name", "reset_url", "expires_minutes"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="A password reset was requested for your Walnut account.",
            sections=[
                "This link expires in {{expires_minutes}} minutes.",
                "If you did not request this, you can safely ignore this email.",
            ],
            cta_label="Reset password",
            cta_url="reset_url",
            sender="Walnut Support",
        ),
        "body_html": walnut_email_html(
            sender="Walnut Support",
            eyebrow="Account security",
            title="Reset your password",
            intro="Hello {{first_name}}, a password reset was requested for your Walnut account.",
            content_html=walnut_info_card(
                "Security note",
                "This link expires in {{expires_minutes}} minutes. If you did not request this, you can safely ignore this email.",
            ),
            cta_label="Reset password",
            cta_url="reset_url",
        ),
    },
    {
        "template_key": "account.password_changed",
        "name": "Password changed confirmation",
        "category": "account",
        "from_name": "Walnut Support",
        "from_email": "support@walnut-intel.com",
        "reply_to": "support@walnut-intel.com",
        "subject": "Your Walnut password was changed",
        "preheader": "A confirmation that your account password was changed.",
        "variables": ["first_name", "changed_at", "support_email", "login_url"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="The password for your Walnut account was changed on {{changed_at}}.",
            sections=[
                "If you made this change, no action is needed.",
                "If you did not make this change, contact support immediately at {{support_email}}.",
            ],
            cta_label="Launch Terminal",
            cta_url="login_url",
            sender="Walnut Support",
        ),
        "body_html": walnut_email_html(
            sender="Walnut Support",
            eyebrow="Account security",
            title="Password changed",
            intro="Hello {{first_name}}, the password for your Walnut account was changed on {{changed_at}}.",
            content_html=walnut_info_card(
                "Security note",
                "If you made this change, no action is needed. If you did not, contact support immediately at {{support_email}}.",
            ),
            cta_label="Launch Terminal",
            cta_url="login_url",
        ),
    },
    {
        "template_key": "alerts.monitoring_digest",
        "name": "Monitoring digest",
        "category": "alerts",
        "from_name": "Walnut Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut monitoring digest",
        "preheader": "A digest of recent monitoring activity.",
        "variables": ["first_name", "watchlist_name", "digest_date", "summary", "items_text", "items_html", "digest_url"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="Your Walnut monitoring digest for {{digest_date}} is ready.",
            sections=["Watchlist: {{watchlist_name}}", "{{summary}}", "{{items_text}}"],
            cta_label="Review digest",
            cta_url="digest_url",
            sender="Walnut Alerts",
            include_investment_disclaimer=True,
        ),
        "body_html": walnut_email_html(
            sender="Walnut Alerts",
            eyebrow="Monitoring digest",
            title="Monitoring digest",
            intro="Hello {{first_name}}, your Walnut monitoring digest for {{digest_date}} is ready.",
            content_html=walnut_info_card("Summary", "{{summary}}") + "{{{items_html}}}",
            cta_label="Review digest",
            cta_url="digest_url",
            include_investment_disclaimer=True,
        ),
    },
    {
        "template_key": "alerts.signal_alert",
        "name": "Signal digest",
        "category": "alerts",
        "from_name": "Walnut Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut signal digest",
        "preheader": "Daily summary of Walnut Market Terminal signal activity.",
        "variables": [
            "first_name",
            "signal_title",
            "signal_intro",
            "signal_cta_label",
            "ticker",
            "signal_score",
            "direction",
            "why_notable",
            "source_stack",
            "cautions",
            "signals_text",
            "signals_html",
            "signal_url",
        ],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="{{signal_intro}}",
            sections=[
                "Ticker: {{ticker}}\nSignal score: {{signal_score}}\nDirection: {{direction}}\nWhy notable: {{why_notable}}\nSource stack: {{source_stack}}",
                "Signals are research inputs, not recommendations. {{cautions}}",
                "{{signals_text}}",
            ],
            cta_label="{{signal_cta_label}}",
            cta_url="signal_url",
            sender="Walnut Alerts",
            include_investment_disclaimer=True,
        ),
        "body_html": walnut_email_html(
            sender="Walnut Alerts",
            eyebrow="{{signal_title}}",
            title="{{signal_title}}",
            intro="Hello {{first_name}}, {{signal_intro}}",
            content_html=walnut_metric_card(
                [
                    ("Ticker", "{{ticker}}"),
                    ("Signal score", "{{signal_score}}"),
                    ("Direction", "{{direction}}"),
                    ("Why notable", "{{why_notable}}"),
                    ("Source stack", "{{source_stack}}"),
                ]
            )
            + walnut_info_card("Research caution", "Signals are research inputs, not recommendations. {{cautions}}")
            + "{{{signals_html}}}",
            cta_label="{{signal_cta_label}}",
            cta_url="signal_url",
            include_investment_disclaimer=True,
        ),
    },
    {
        "template_key": "alerts.watchlist_intraday",
        "name": "Intraday watchlist activity alert",
        "category": "alerts",
        "from_name": "Walnut Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut high-priority watchlist alert: {{ticker}}",
        "preheader": "High-priority watchlist activity cleared Walnut intraday alert thresholds.",
        "variables": [
            "first_name",
            "ticker",
            "watchlist_name",
            "alert_title",
            "alert_intro",
            "event_type",
            "actor",
            "amount",
            "signal_score",
            "direction",
            "trigger",
            "why_notable",
            "source_stack",
            "event_date",
            "alert_url",
        ],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="{{alert_intro}}",
            sections=[
                "Intraday Alerts\nTicker: {{ticker}}\nWatchlist: {{watchlist_name}}\nEvent: {{event_type}}\nActor: {{actor}}\nAmount: {{amount}}\nScore: {{signal_score}}\nDirection: {{direction}}\nTrigger: {{trigger}}\nWhy notable: {{why_notable}}\nSource stack: {{source_stack}}\nObserved: {{event_date}}",
                "Daily Digests\nLower and medium-priority watchlist activity stays in your daily digest unless it clears intraday thresholds.",
            ],
            cta_label="Review activity",
            cta_url="alert_url",
            sender="Walnut Alerts",
            include_investment_disclaimer=True,
        ),
        "body_html": walnut_email_html(
            sender="Walnut Alerts",
            eyebrow="Intraday Alerts",
            title="{{alert_title}}",
            intro="Hello {{first_name}}, {{alert_intro}}",
            content_html=walnut_metric_card(
                [
                    ("Ticker", "{{ticker}}"),
                    ("Watchlist", "{{watchlist_name}}"),
                    ("Event", "{{event_type}}"),
                    ("Actor", "{{actor}}"),
                    ("Amount", "{{amount}}"),
                    ("Score", "{{signal_score}}"),
                    ("Direction", "{{direction}}"),
                    ("Trigger", "{{trigger}}"),
                ]
            )
            + walnut_info_card("Why this sent now", "{{why_notable}} Source stack: {{source_stack}}")
            + walnut_info_card("Daily Digests", "Lower and medium-priority watchlist activity remains in the daily digest unless it clears intraday thresholds."),
            cta_label="Review activity",
            cta_url="alert_url",
            include_investment_disclaimer=True,
        ),
    },
    {
        "template_key": "alerts.signal_intraday",
        "name": "Intraday signal alert",
        "category": "alerts",
        "from_name": "Walnut Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Walnut high-conviction signal: {{ticker}}",
        "preheader": "A saved signal cleared Walnut intraday conviction thresholds.",
        "variables": [
            "first_name",
            "ticker",
            "alert_title",
            "alert_intro",
            "event_type",
            "signal_score",
            "direction",
            "trigger",
            "why_notable",
            "source_stack",
            "event_date",
            "alert_url",
        ],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="{{alert_intro}}",
            sections=[
                "Intraday Alerts\nTicker: {{ticker}}\nSignal score: {{signal_score}}\nDirection: {{direction}}\nTrigger: {{trigger}}\nWhy notable: {{why_notable}}\nSource stack: {{source_stack}}\nObserved: {{event_date}}",
                "Daily Digests\nNormal signal activity that does not clear intraday conviction thresholds is summarized in the daily signal digest.",
            ],
            cta_label="Review signal",
            cta_url="alert_url",
            sender="Walnut Alerts",
            include_investment_disclaimer=True,
        ),
        "body_html": walnut_email_html(
            sender="Walnut Alerts",
            eyebrow="Intraday Alerts",
            title="{{alert_title}}",
            intro="Hello {{first_name}}, {{alert_intro}}",
            content_html=walnut_metric_card(
                [
                    ("Ticker", "{{ticker}}"),
                    ("Signal score", "{{signal_score}}"),
                    ("Direction", "{{direction}}"),
                    ("Trigger", "{{trigger}}"),
                    ("Why notable", "{{why_notable}}"),
                    ("Source stack", "{{source_stack}}"),
                ]
            )
            + walnut_info_card("Daily Digests", "Normal signal activity that does not clear intraday conviction thresholds is summarized in the daily signal digest."),
            cta_label="Review signal",
            cta_url="alert_url",
            include_investment_disclaimer=True,
        ),
    },
    {
        "template_key": "alerts.watchlist_activity",
        "name": "Watchlist activity digest",
        "category": "alerts",
        "from_name": "Walnut Alerts",
        "from_email": "alerts@walnut-intel.com",
        "reply_to": "alerts@walnut-intel.com",
        "subject": "Watchlist activity from Walnut",
        "preheader": "Daily summary of Walnut Market Terminal watchlist activity.",
        "variables": ["first_name", "watchlist_name", "summary", "items_text", "items_html", "activity_url"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="Your Walnut daily watchlist digest for {{watchlist_name}} is ready.",
            sections=["Daily Digests", "{{summary}}", "{{items_text}}"],
            cta_label="Review activity",
            cta_url="activity_url",
            sender="Walnut Alerts",
            include_investment_disclaimer=True,
        ),
        "body_html": walnut_email_html(
            sender="Walnut Alerts",
            eyebrow="Daily Digests",
            title="Watchlist activity digest",
            intro="Hello {{first_name}}, your Walnut daily watchlist digest for {{watchlist_name}} is ready.",
            content_html=walnut_info_card("Activity summary", "{{summary}}") + "{{{items_html}}}",
            cta_label="Review activity",
            cta_url="activity_url",
            include_investment_disclaimer=True,
        ),
    },
    {
        "template_key": "billing.monthly_statement",
        "name": "Monthly billing statement",
        "category": "billing",
        "from_name": "Walnut Billing",
        "from_email": "billing@walnut-intel.com",
        "reply_to": "billing@walnut-intel.com",
        "subject": "Your Walnut monthly statement",
        "preheader": "Your Walnut Market Terminal billing statement is available.",
        "variables": ["first_name", "billing_period", "plan", "amount_due", "currency", "payment_status", "statement_url"],
        "body_text": walnut_email_text(
            greeting="Hello {{first_name}},",
            intro="Your Walnut monthly statement is ready.",
            sections=[
                "Billing period: {{billing_period}}\nPlan: {{plan}}\nAmount: {{amount_due}} {{currency}}\nPayment status: {{payment_status}}",
                "Questions about billing? Contact support@walnut-intel.com.",
            ],
            cta_label="View billing",
            cta_url="statement_url",
            sender="Walnut Billing",
        ),
        "body_html": walnut_email_html(
            sender="Walnut Billing",
            eyebrow="Billing",
            title="Monthly statement",
            intro="Hello {{first_name}}, your Walnut monthly statement is ready.",
            content_html=walnut_metric_card(
                [
                    ("Billing period", "{{billing_period}}"),
                    ("Plan", "{{plan}}"),
                    ("Amount", "{{amount_due}} {{currency}}"),
                    ("Payment status", "{{payment_status}}"),
                ]
            )
            + walnut_info_card("Billing support", "Questions about billing? Contact support@walnut-intel.com."),
            cta_label="View billing",
            cta_url="statement_url",
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


def reset_email_templates_to_defaults(db: Session, template_keys: list[str] | None = None) -> list[EmailTemplate]:
    keys = template_keys if template_keys else list(DEFAULT_TEMPLATE_BY_KEY.keys())
    reset_templates: list[EmailTemplate] = []
    for key in keys:
        template = reset_email_template_to_default(db, key)
        if template is not None:
            reset_templates.append(template)
    return reset_templates
