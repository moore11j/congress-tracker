"""Optional, at-most-once provider delivery of authoritative paid funnel rows.

Claims are committed before any network send. Ambiguous attempts are never
retried automatically: the first-party billing/analytics record is retained.
No provider call participates in the Stripe transaction.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import PageViewEvent

logger = logging.getLogger(__name__)
PAID_PATH = "/events/subscription_completed"
HEYCATCH_BRIDGE = "https://app.walnutmarkets.com/api/internal/paid-analytics"
GA4_ENDPOINT = "https://www.google-analytics.com/mp/collect"
GA4_MEASUREMENT_ID = "G-QQTFFK7FBH"


def production_enabled() -> bool:
    env = os.getenv("APP_ENV", "").strip().lower()
    return env in {"production", "prod"} or (not env and bool(os.getenv("FLY_APP_NAME")))


def metadata(raw: str | None) -> dict:
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError):
        return {}


def safe_ga_context(value: object) -> dict:
    if not isinstance(value, dict):
        return {}
    client = str(value.get("client_id") or "")
    session = str(value.get("session_id") or "")
    if not re.fullmatch(r"[1-9][0-9]{0,19}\.[1-9][0-9]{0,19}", client):
        return {}
    if not re.fullmatch(r"[1-9][0-9]{0,19}", session):
        return {}
    return {"client_id": client, "session_id": session}


def bridge_signature(body: bytes, timestamp: str, scope: str) -> str:
    secret = os.getenv("ANALYTICS_FORWARDING_SECRET", "")
    return hmac.new(secret.encode(), timestamp.encode() + b"." + scope.encode() + b"." + body, hashlib.sha256).hexdigest()


def verify_bridge(body: bytes, timestamp: str, signature: str, scope: str) -> bool:
    if not production_enabled() or len(os.getenv("ANALYTICS_FORWARDING_SECRET", "")) < 32:
        return False
    if not re.fullmatch(r"[0-9]{10}", timestamp) or abs(time.time() - int(timestamp)) > 60:
        return False
    return hmac.compare_digest(bridge_signature(body, timestamp, scope), signature)


def claim_delivery(db: Session, event_id: int, provider: str) -> dict | None:
    """Compare-and-swap also protects against concurrent bridge replays."""
    # PageViewEvent.id is a PostgreSQL INTEGER. A signed, otherwise valid
    # JavaScript integer can exceed that range; it cannot identify a real row.
    if not 0 < event_id <= 2_147_483_647:
        return None
    row = db.get(PageViewEvent, event_id)
    if row is None or row.normalized_path != PAID_PATH or not row.user_id:
        return None
    raw = row.metadata_json
    data = metadata(raw)
    if data.get("source") != "stripe_webhook" or not data.get("invoice_id"):
        return None
    if data.get("forwarding", {}).get(provider):
        return None
    created = row.created_at.replace(tzinfo=timezone.utc) if row.created_at.tzinfo is None else row.created_at
    if datetime.now(timezone.utc) - created > timedelta(hours=72):
        return None
    ga = safe_ga_context(data.get("ga_context"))
    if provider == "ga4" and not ga:
        return None
    data.setdefault("forwarding", {})[provider] = "attempted"
    changed = db.execute(update(PageViewEvent).where(
        PageViewEvent.id == event_id, PageViewEvent.metadata_json == raw,
    ).values(metadata_json=json.dumps(data, sort_keys=True)), execution_options={"synchronize_session": False})
    db.commit()
    if changed.rowcount != 1:
        return None
    db.expire_all()
    # Only properties sanitized by the authoritative insertion are retained.
    props = {k: v for k, v in data.get("properties", {}).items()
             if k in {"authenticated", "current_plan", "target_plan", "billing_interval",
                      "acquisition_source", "utm_source", "utm_medium", "utm_campaign", "source_page", "route"}
             and isinstance(v, (str, int, float, bool))}
    props["event_id"] = hashlib.sha256(("paid:" + data["invoice_id"]).encode()).hexdigest()
    props["route"] = "/account/billing"
    return {"user_id": str(row.user_id), "properties": props, "ga_context": ga,
            "timestamp_micros": int(created.timestamp() * 1_000_000)}


def set_delivery_result(db: Session, event_id: int, provider: str, result: str) -> None:
    row = db.get(PageViewEvent, event_id)
    if row is None:
        return
    raw = row.metadata_json
    data = metadata(raw)
    data.setdefault("forwarding", {})[provider] = result
    db.execute(update(PageViewEvent).where(PageViewEvent.id == event_id, PageViewEvent.metadata_json == raw)
               .values(metadata_json=json.dumps(data, sort_keys=True)), execution_options={"synchronize_session": False})
    db.commit()
    db.expire_all()


def ga4_payload(claim: dict) -> dict:
    # A server payment adds no browser engagement time. No synthetic client ID.
    params = {k: v[:100] if isinstance(v, str) else v for k, v in claim["properties"].items()}
    params.update({"session_id": claim["ga_context"]["session_id"], "engagement_time_msec": 0})
    return {"client_id": claim["ga_context"]["client_id"], "user_id": claim["user_id"],
            "timestamp_micros": claim["timestamp_micros"],
            "consent": {"ad_user_data": "DENIED", "ad_personalization": "DENIED"},
            "events": [{"name": "subscription_completed", "params": params}]}


def forward_paid_event(event_id: int) -> None:
    # BackgroundTasks runs after billing commits. Even a database outage here
    # must not turn a successful Stripe webhook into an application exception.
    try:
        _forward_paid_event(event_id)
    except Exception:
        logger.warning("Paid analytics dispatch unavailable; billing was already committed")


def _forward_paid_event(event_id: int) -> None:
    if not production_enabled():
        return
    with SessionLocal() as db:
        secret = os.getenv("GA4_API_SECRET", "")
        if secret:
            try:
                claim = claim_delivery(db, event_id, "ga4")
                if claim:
                    response = requests.post(GA4_ENDPOINT,
                        params={"measurement_id": GA4_MEASUREMENT_ID, "api_secret": secret},
                        json=ga4_payload(claim), timeout=(2, 3), allow_redirects=False)
                    # 2xx is receipt, not proof GA processed a valid event.
                    set_delivery_result(db, event_id, "ga4", "received" if 200 <= response.status_code < 300 else "rejected")
            except Exception:
                db.rollback()
                logger.warning("GA4 paid analytics attempt failed; retained without automatic retry")
        row = db.get(PageViewEvent, event_id)
        already_claimed = bool(row and metadata(row.metadata_json).get("forwarding", {}).get("heycatch"))
        if not already_claimed and len(os.getenv("ANALYTICS_FORWARDING_SECRET", "")) >= 32:
            try:
                # The Node bridge must call back to claim the verified row before
                # using the documented HeyCatch server SDK. Replays get no data.
                body = json.dumps({"event_id": event_id}, separators=(",", ":")).encode()
                timestamp = str(int(time.time()))
                requests.post(HEYCATCH_BRIDGE, data=body, headers={"Content-Type": "application/json",
                    "X-Walnut-Timestamp": timestamp,
                    "X-Walnut-Signature": bridge_signature(body, timestamp, "dispatch")},
                    timeout=(2, 15), allow_redirects=False)
            except Exception:
                logger.warning("HeyCatch bridge unavailable; inspect durable delivery state before retrying")


def forward_pending_paid_events() -> None:
    """Also callable by an operator; claimed/ambiguous sends are never resent."""
    if not production_enabled():
        return
    try:
        with SessionLocal() as db:
            ids = list(db.execute(select(PageViewEvent.id).where(
                PageViewEvent.normalized_path == PAID_PATH,
                PageViewEvent.created_at >= datetime.now(timezone.utc) - timedelta(hours=72),
            ).order_by(PageViewEvent.id.desc()).limit(100)).scalars())
        for event_id in ids:
            forward_paid_event(event_id)
    except Exception:
        logger.warning("Paid analytics dispatch unavailable; billing was already committed")
