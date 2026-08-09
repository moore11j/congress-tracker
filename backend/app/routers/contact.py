from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import normalize_email
from app.db import get_db
from app.rate_limit import rate_limit_contact_form
from app.services.email_delivery import send_email

router = APIRouter(tags=["contact"])

CONTACT_REQUEST_TYPES = {
    "Feedback",
    "Reporting a bug",
    "Requesting a new feature",
    "General inquiry",
}
SUPPORT_EMAIL = os.getenv("SUPPORT_EMAIL", "support@walnutmarkets.com").strip() or "support@walnutmarkets.com"
CONTACT_SUCCESS_MESSAGE = "Your message was successfully sent. We will try to respond within the next 2-3 business days."


class ContactFormPayload(BaseModel):
    request_type: Literal["Feedback", "Reporting a bug", "Requesting a new feature", "General inquiry"]
    email: str = Field(min_length=3, max_length=320)
    message: str = Field(min_length=2, max_length=5000)


def _clean_message(value: str) -> str:
    message = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not message:
        raise HTTPException(status_code=422, detail="Message is required.")
    return message


def _request_page_url(request: Request) -> str:
    referer = (request.headers.get("referer") or "").strip()
    if referer:
        return referer[:500]
    return str(request.url)


@router.post("/contact", dependencies=[Depends(rate_limit_contact_form)])
def submit_contact_form(payload: ContactFormPayload, request: Request, db: Session = Depends(get_db)):
    sender_email = normalize_email(payload.email)
    if "@" not in sender_email:
        raise HTTPException(status_code=422, detail="A valid email address is required.")

    request_type = str(payload.request_type).strip()
    if request_type not in CONTACT_REQUEST_TYPES:
        raise HTTPException(status_code=422, detail="Unsupported contact request type.")

    result = send_email(
        db,
        to_email=SUPPORT_EMAIL,
        template_key="support.contact_form",
        category="support",
        reply_to=sender_email,
        context={
            "request_type": request_type,
            "sender_email": sender_email,
            "message": _clean_message(payload.message),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "page_url": _request_page_url(request),
            "user_agent": (request.headers.get("user-agent") or "unknown")[:500],
        },
        raise_http_errors=True,
    )
    if result.get("status") != "sent":
        raise HTTPException(status_code=503, detail="Contact email could not be sent.")
    return {"status": "sent", "message": CONTACT_SUCCESS_MESSAGE}
