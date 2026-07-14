from __future__ import annotations

import json
import logging
import os
import base64
from dataclasses import dataclass
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


@dataclass(frozen=True)
class SenderResolution:
    from_name: str
    from_email: str
    source: str


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
    reply_to: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_to = normalize_email(to_email)
    if not normalized_to or "@" not in normalized_to:
        raise HTTPException(status_code=422, detail="A valid recipient email is required.")

    if idempotency_key:
        existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == idempotency_key)).scalar_one_or_none()
        if existing:
            return _delivery_result(existing)

    template = _get_template(db, template_key)
    sender = resolve_sender_for_template(template)
    _log_sender_resolution(template.template_key, sender)
    reply_to_value = reply_to or _reply_to_for_template(template)
    if not template.enabled:
        delivery = _create_delivery(
            db,
            to_email=normalized_to,
            from_email=sender.from_email,
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
    provider = _provider_name()

    if force_log_only:
        delivery = _create_delivery(
            db,
            to_email=normalized_to,
            from_email=sender.from_email,
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
            from_email=sender.from_email,
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
            from_email=sender.from_email,
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
        from_email=sender.from_email,
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
            from_value=formataddr((sender.from_name, sender.from_email)),
            to_email=normalized_to,
            reply_to=reply_to_value,
            subject=rendered["subject"],
            body_text=rendered["body_text"],
            body_html=rendered["body_html"],
            attachments=attachments,
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
        "body_html": render_template_string(template.body_html, context, allowed_variables, html=True) if template.body_html else None,
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
    if provider == "postmark":
        return os.getenv("POSTMARK_SERVER_TOKEN", "").strip() or None
    return None


def resolve_sender_for_template(template: EmailTemplate) -> SenderResolution:
    template_from_email = (template.from_email or "").strip()
    template_from_name = (template.from_name or "").strip()
    configured = _sender_env_value(template)
    parsed_name, parsed_email = parseaddr(configured) if configured else ("", "")
    if template_from_email:
        return SenderResolution(
            from_name=template_from_name or parsed_name,
            from_email=template_from_email,
            source="template",
        )
    if configured:
        return SenderResolution(
            from_name=template_from_name or parsed_name,
            from_email=parsed_email or configured,
            source="env_fallback",
        )
    return SenderResolution(
        from_name=template_from_name,
        from_email=template_from_email,
        source="template",
    )


def _sender_env_value(template: EmailTemplate) -> str:
    env_keys: list[str] = []
    if template.template_key == "account.password_reset":
        env_keys.append("PASSWORD_RESET_FROM")
    if template.category == "account":
        env_keys.append("EMAIL_FROM")
    env_key = {
        "account": "EMAIL_FROM_SUPPORT",
        "billing": "EMAIL_FROM_BILLING",
        "alerts": "EMAIL_FROM_ALERTS",
    }.get(template.category)
    if env_key:
        env_keys.append(env_key)
    if "EMAIL_FROM" not in env_keys:
        env_keys.append("EMAIL_FROM")
    return next((os.getenv(key, "").strip() for key in env_keys if os.getenv(key, "").strip()), "")


def _sender_for_template(template: EmailTemplate) -> tuple[str, str]:
    sender = resolve_sender_for_template(template)
    return sender.from_name, sender.from_email


def log_sender_resolution(template_key: str, sender: SenderResolution) -> None:
    _log_sender_resolution(template_key, sender)


def _log_sender_resolution(template_key: str, sender: SenderResolution) -> None:
    logger.info(
        "email_send_sender_resolved template_key=%s from_email=%s source=%s",
        template_key,
        sender.from_email,
        sender.source,
    )


def _reply_to_for_template(template: EmailTemplate) -> str | None:
    template_reply_to = (template.reply_to or "").strip()
    if template_reply_to:
        return template_reply_to

    env_keys: list[str] = []
    if template.category == "account":
        env_keys.append("EMAIL_REPLY_TO")
    env_key = {
        "account": "EMAIL_REPLY_TO_SUPPORT",
        "billing": "EMAIL_REPLY_TO_BILLING",
        "alerts": "EMAIL_REPLY_TO_ALERTS",
    }.get(template.category)
    if env_key:
        env_keys.append(env_key)
    if "EMAIL_REPLY_TO" not in env_keys:
        env_keys.append("EMAIL_REPLY_TO")
    configured = next((os.getenv(key, "").strip() for key in env_keys if os.getenv(key, "").strip()), "")
    return configured or None


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
    attachments: list[dict[str, Any]] | None = None,
) -> str | None:
    normalized_attachments = _normalize_email_attachments(attachments)
    if provider == "postmark":
        return _send_with_postmark(
            api_key=api_key,
            from_value=from_value,
            to_email=to_email,
            reply_to=reply_to,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachments=normalized_attachments,
        )
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
    if normalized_attachments:
        payload["attachments"] = [
            {
                "filename": attachment["name"],
                "content": attachment["content_base64"],
                "content_type": attachment["content_type"],
            }
            for attachment in normalized_attachments
        ]
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    parsed = response.json()
    return str(parsed.get("id")) if isinstance(parsed, dict) and parsed.get("id") else None


def _send_with_postmark(
    *,
    api_key: str,
    from_value: str,
    to_email: str,
    reply_to: str | None,
    subject: str,
    body_text: str,
    body_html: str | None,
    attachments: list[dict[str, str]] | None = None,
) -> str | None:
    payload: dict[str, Any] = {
        "From": from_value,
        "To": to_email,
        "Subject": subject,
        "TextBody": body_text,
        "MessageStream": "outbound",
    }
    if body_html:
        payload["HtmlBody"] = body_html
    if reply_to:
        payload["ReplyTo"] = reply_to
    if attachments:
        payload["Attachments"] = [
            {
                "Name": attachment["name"],
                "Content": attachment["content_base64"],
                "ContentType": attachment["content_type"],
            }
            for attachment in attachments
        ]
    response = requests.post(
        "https://api.postmarkapp.com/email",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Postmark-Server-Token": api_key,
        },
        json=payload,
        timeout=20,
    )
    if response.status_code < 200 or response.status_code >= 300:
        raise ValueError(_provider_error_detail(response))
    parsed = response.json()
    return str(parsed.get("MessageID")) if isinstance(parsed, dict) and parsed.get("MessageID") else None


def _normalize_email_attachments(attachments: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen_names: set[str] = set()
    for attachment in attachments or []:
        if not isinstance(attachment, dict):
            continue
        name = str(attachment.get("name") or attachment.get("filename") or "").strip()
        content_type = str(attachment.get("content_type") or attachment.get("contentType") or "application/octet-stream").strip()
        if not name:
            continue
        content_base64 = str(attachment.get("content_base64") or "").strip()
        if not content_base64:
            raw_content = attachment.get("content")
            if isinstance(raw_content, bytes):
                content_base64 = base64.b64encode(raw_content).decode("ascii")
            elif isinstance(raw_content, str) and raw_content:
                content_base64 = base64.b64encode(raw_content.encode("utf-8")).decode("ascii")
        if not content_base64:
            continue
        safe_name = _safe_attachment_name(name)
        stem, dot, suffix = safe_name.rpartition(".")
        unique_name = safe_name
        counter = 2
        while unique_name.lower() in seen_names:
            unique_name = f"{stem or safe_name}-{counter}{dot}{suffix}" if dot else f"{safe_name}-{counter}"
            counter += 1
        seen_names.add(unique_name.lower())
        normalized.append({"name": unique_name, "content_base64": content_base64, "content_type": content_type})
    return normalized[:10]


def _safe_attachment_name(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in value.strip())
    cleaned = cleaned.strip(".-") or "attachment"
    return cleaned[:120]


def _provider_error_detail(response: requests.Response) -> str:
    body = ""
    try:
        parsed = response.json()
    except Exception:
        parsed = None
    if isinstance(parsed, dict):
        body = str(parsed.get("Message") or parsed.get("message") or parsed.get("ErrorCode") or "")
    if not body:
        body = getattr(response, "text", "") or ""
    return f"Provider returned HTTP {response.status_code}: {body[:300]}"
