from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from email.utils import formataddr, parseaddr
from typing import Any

import requests
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import normalize_email
from app.models import EmailDelivery, EmailTemplate
from app.services.email_renderer import render_template_string
from app.services.email_templates import seed_default_email_templates

logger = logging.getLogger(__name__)


def email_delivery_enabled() -> bool:
    return os.getenv("EMAIL_DELIVERY_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def send_email(
    db: Session,
    *,
    to_email: str,
    template_key: str,
    context: dict[str, Any],
    user_id: int | None = None,
    category: str,
    idempotency_key: str | None = None,
    force_log_only: bool = False,
    raise_http_errors: bool = False,
) -> dict[str, Any]:
    normalized_to = normalize_email(to_email)
    if not normalized_to or "@" not in normalized_to:
        raise HTTPException(status_code=422, detail="A valid recipient email is required.")

    if idempotency_key:
        existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == idempotency_key)).scalar_one_or_none()
        if existing:
            return _delivery_result(existing)

    template = _get_template(db, template_key)
    if not template.enabled:
        delivery = _create_delivery(
            db,
            to_email=normalized_to,
            from_email=_sender_for_template(template)[1],
            template_key=template.template_key,
            category=category,
            subject=template.subject,
            provider=_provider_name(),
            status="skipped",
            idempotency_key=idempotency_key,
            error="Template is disabled.",
            context=context,
            user_id=user_id,
        )
        return _delivery_result(delivery)

    try:
        rendered = _render_template(template, context)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    sender_name, from_email = _sender_for_template(template)
    reply_to = _reply_to_for_template(template)
    provider = _provider_name()

    if force_log_only:
        delivery = _create_delivery(
            db,
            to_email=normalized_to,
            from_email=from_email,
            template_key=template.template_key,
            category=category,
            subject=rendered["subject"],
            provider=provider,
            status="log_only",
            idempotency_key=idempotency_key,
            context=context,
            user_id=user_id,
        )
        return {**_delivery_result(delivery), **rendered}

    if not email_delivery_enabled():
        delivery = _create_delivery(
            db,
            to_email=normalized_to,
            from_email=from_email,
            template_key=template.template_key,
            category=category,
            subject=rendered["subject"],
            provider=provider,
            status="skipped",
            idempotency_key=idempotency_key,
            error="Email delivery is disabled.",
            context=context,
            user_id=user_id,
        )
        return _delivery_result(delivery)

    api_key = _provider_api_key(provider)
    if not api_key:
        delivery = _create_delivery(
            db,
            to_email=normalized_to,
            from_email=from_email,
            template_key=template.template_key,
            category=category,
            subject=rendered["subject"],
            provider=provider,
            status="log_only",
            idempotency_key=idempotency_key,
            error="Provider API key is not configured.",
            context=context,
            user_id=user_id,
        )
        return {**_delivery_result(delivery), **rendered}

    delivery = _create_delivery(
        db,
        to_email=normalized_to,
        from_email=from_email,
        template_key=template.template_key,
        category=category,
        subject=rendered["subject"],
        provider=provider,
        status="queued",
        idempotency_key=idempotency_key,
        context=context,
        user_id=user_id,
    )

    try:
        provider_message_id = _send_with_provider(
            provider=provider,
            api_key=api_key,
            from_value=formataddr((sender_name, from_email)),
            to_email=normalized_to,
            reply_to=reply_to,
            subject=rendered["subject"],
            body_text=rendered["body_text"],
            body_html=rendered["body_html"],
        )
    except Exception as exc:
        delivery.status = "failed"
        delivery.error = str(exc)[:500]
        db.commit()
        logger.warning("email_delivery_failed template_key=%s provider=%s", template_key, provider, exc_info=True)
        if raise_http_errors:
            raise HTTPException(status_code=502, detail="Email provider request failed.") from exc
        return _delivery_result(delivery)

    delivery.status = "sent"
    delivery.provider_message_id = provider_message_id
    delivery.sent_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(delivery)
    return _delivery_result(delivery)


def _get_template(db: Session, template_key: str) -> EmailTemplate:
    template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == template_key)).scalar_one_or_none()
    if template:
        return template
    seed_default_email_templates(db)
    template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == template_key)).scalar_one_or_none()
    if not template:
        raise HTTPException(status_code=404, detail="Email template not found.")
    return template


def _render_template(template: EmailTemplate, context: dict[str, Any]) -> dict[str, str | None]:
    allowed_variables = _template_variables(template)
    return {
        "subject": render_template_string(template.subject, context, allowed_variables),
        "body_text": render_template_string(template.body_text, context, allowed_variables),
        "body_html": render_template_string(template.body_html, context, allowed_variables) if template.body_html else None,
    }


def _template_variables(template: EmailTemplate) -> list[str]:
    try:
        parsed = json.loads(template.variables_json or "[]")
    except Exception:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _create_delivery(
    db: Session,
    *,
    to_email: str,
    from_email: str,
    template_key: str | None,
    category: str,
    subject: str,
    provider: str,
    status: str,
    idempotency_key: str | None,
    context: dict[str, Any],
    user_id: int | None = None,
    error: str | None = None,
) -> EmailDelivery:
    delivery = EmailDelivery(
        user_id=user_id,
        to_email=to_email,
        from_email=from_email,
        template_key=template_key,
        category=category,
        subject=subject,
        provider=provider,
        status=status,
        idempotency_key=idempotency_key,
        error=error,
        payload_json=json.dumps(_safe_payload(context), sort_keys=True),
    )
    db.add(delivery)
    db.commit()
    db.refresh(delivery)
    return delivery


def _safe_payload(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "context_keys": sorted(str(key) for key in context.keys()),
        "redacted_fields": sorted(key for key in ("reset_url", "verification_url") if key in context),
    }


def _delivery_result(delivery: EmailDelivery) -> dict[str, Any]:
    return {
        "id": delivery.id,
        "status": delivery.status,
        "provider": delivery.provider,
        "provider_message_id": delivery.provider_message_id,
        "template_key": delivery.template_key,
        "category": delivery.category,
        "to_email": delivery.to_email,
        "error": delivery.error,
    }


def _provider_name() -> str:
    return os.getenv("EMAIL_PROVIDER", "resend").strip().lower() or "resend"


def _provider_api_key(provider: str) -> str | None:
    if provider == "resend":
        return os.getenv("RESEND_API_KEY", "").strip() or None
    return None


def _sender_for_template(template: EmailTemplate) -> tuple[str, str]:
    env_key = {
        "account": "EMAIL_FROM_SUPPORT",
        "billing": "EMAIL_FROM_BILLING",
        "alerts": "EMAIL_FROM_ALERTS",
    }.get(template.category)
    configured = os.getenv(env_key or "", "").strip() if env_key else ""
    if configured:
        parsed_name, parsed_email = parseaddr(configured)
        if parsed_email:
            return parsed_name or template.from_name, parsed_email
        return template.from_name, configured
    return template.from_name, template.from_email


def _reply_to_for_template(template: EmailTemplate) -> str | None:
    env_key = {
        "account": "EMAIL_REPLY_TO_SUPPORT",
        "billing": "EMAIL_REPLY_TO_BILLING",
        "alerts": "EMAIL_REPLY_TO_ALERTS",
    }.get(template.category)
    configured = os.getenv(env_key or "", "").strip() if env_key else ""
    return configured or template.reply_to


def _send_with_provider(
    *,
    provider: str,
    api_key: str,
    from_value: str,
    to_email: str,
    reply_to: str | None,
    subject: str,
    body_text: str,
    body_html: str | None,
) -> str | None:
    if provider != "resend":
        raise ValueError(f"Unsupported email provider: {provider}")
    payload: dict[str, Any] = {
        "from": from_value,
        "to": [to_email],
        "subject": subject,
        "text": body_text,
    }
    if body_html:
        payload["html"] = body_html
    if reply_to:
        payload["reply_to"] = reply_to
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    parsed = response.json()
    return str(parsed.get("id")) if isinstance(parsed, dict) and parsed.get("id") else None
