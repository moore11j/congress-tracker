"""Stripe sandbox adapter. Never reads the live Stripe credential variables."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlparse

import requests

from app.services.research_credits import CreditError, CreditStore, PURCHASE_CENTS_PER_CREDIT, purchased_credits

CATALOG_VERSION = "walnut_research_v2"
API_VERSION = "2026-11-18.endive"


def sandbox_configuration():
    if os.getenv("WALNUT_CREDITS_SANDBOX_ENABLED") != "1":
        raise CreditError("Credit sandbox is not enabled")
    key = os.getenv("WALNUT_STRIPE_SANDBOX_SECRET_KEY", "")
    origin = os.getenv("WALNUT_CREDITS_SANDBOX_ORIGIN", "").rstrip("/")
    parsed = urlparse(origin)
    if not key.startswith(("sk_test_", "rk_test_")):
        raise CreditError("A Stripe sandbox key is required")
    if (parsed.hostname in {"walnutmarkets.com", "www.walnutmarkets.com", "app.walnutmarkets.com"}
            or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path
            or not parsed.hostname
            or (parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1"}))):
        raise CreditError("A separate sandbox frontend origin is required")
    price = os.getenv("WALNUT_STRIPE_SANDBOX_CREDIT_PRICE_ID", "")
    if not price.startswith("price_"):
        raise CreditError("Sandbox credit price is not configured")
    return key, origin, price


class SandboxStripe:
    def __init__(self, key):
        if not key.startswith(("sk_test_", "rk_test_")):
            raise CreditError("Live Stripe keys are prohibited in this adapter")
        self.key = key

    def request(self, method, path, data=None, *, idempotency=None):
        headers = {"Stripe-Version": API_VERSION}
        if idempotency:
            headers["Idempotency-Key"] = idempotency
        response = requests.request(method, f"https://api.stripe.com/v1/{path}",
            auth=(self.key, ""), headers=headers, timeout=20, allow_redirects=False,
            **({"params": data or {}} if method == "GET" else {"data": data or {}}))
        if not 200 <= response.status_code < 300:
            # Do not expose Stripe response text or credential-bearing request details.
            raise CreditError(f"Stripe sandbox request failed ({response.status_code})")
        result = response.json()
        if not isinstance(result, dict) or result.get("livemode") is True:
            raise CreditError("Unexpected Stripe environment")
        return result


def verify_signature(payload: bytes, signature: str, secret: str, *, now=None):
    if not secret.startswith("whsec_") or len(payload) > 256_000:
        raise CreditError("Invalid webhook configuration or size")
    parts = [part.split("=", 1) for part in signature.split(",") if "=" in part]
    timestamps = [v for k, v in parts if k == "t"]
    if len(timestamps) != 1:
        raise CreditError("Invalid webhook signature")
    try:
        stamp = int(timestamps[0])
    except ValueError as exc:
        raise CreditError("Invalid webhook signature") from exc
    if abs((time.time() if now is None else now) - stamp) > 300:
        raise CreditError("Expired webhook signature")
    digest = hmac.new(secret.encode(), str(stamp).encode()+b"."+payload, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(digest, v) for k, v in parts if k == "v1"):
        raise CreditError("Invalid webhook signature")
    try:
        event = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise CreditError("Invalid webhook JSON") from exc
    if not isinstance(event, dict) or event.get("livemode") is not False or event.get("account"):
        raise CreditError("Only direct sandbox events are accepted")
    return event


def validate_price(price):
    if (price.get("livemode") is not False or price.get("active") is not True
            or price.get("currency") != "usd" or type(price.get("unit_amount")) is not int
            or price["unit_amount"] != PURCHASE_CENTS_PER_CREDIT or price.get("type") != "one_time"
            or price.get("recurring") or price.get("transform_quantity")
            or price.get("billing_scheme") != "per_unit"
            or price.get("metadata", {}).get("walnut_catalog") != CATALOG_VERSION):
        raise CreditError("Expected sandbox USD two-cent research-credit price")


def create_checkout(store: CreditStore, stripe, user_id, order_id, amount_cents, origin, price_id):
    quantity = purchased_credits(amount_cents)
    price = stripe.request("GET", f"prices/{price_id}")
    validate_price(price)
    order = store.create_order(user_id, order_id, amount_cents)
    if order.get("state") != "created":
        raise CreditError("This order has already been processed")
    if order.get("session_id"):
        session = stripe.request("GET", f"checkout/sessions/{order['session_id']}")
    else:
        session = stripe.request("POST", "checkout/sessions", {
        "mode": "payment", "line_items[0][price]": price_id,
        "line_items[0][quantity]": quantity,
        "client_reference_id": user_id,
        "metadata[walnut_order]": order_id,
        "metadata[walnut_catalog]": CATALOG_VERSION,
        "payment_intent_data[metadata][walnut_order]": order_id,
        "success_url": origin+"/ask/credits?checkout=returned",
        "cancel_url": origin+"/ask/credits?checkout=cancelled",
        }, idempotency=f"walnut-credit-checkout:{order_id}")
    session_id, url = session.get("id", ""), session.get("url", "")
    parsed = urlparse(url)
    if (session.get("livemode") is not False or not session_id.startswith("cs_test_")
            or parsed.scheme != "https" or parsed.netloc != "checkout.stripe.com"):
        raise CreditError("Invalid sandbox Checkout response")
    store.bind_session(user_id, order_id, session_id)
    return {"id": session_id, "url": url, "sandbox": True}


def process_event(store: CreditStore, stripe, event, price_id):
    if event.get("livemode") is not False or event.get("account"):
        raise CreditError("Only sandbox events are accepted")
    kind = event.get("type")
    obj = event.get("data", {}).get("object", {})
    if kind in {"charge.refunded", "charge.dispute.created"}:
        payment = obj.get("payment_intent")
        if not payment and kind == "charge.dispute.created" and isinstance(obj.get("charge"), str):
            payment = stripe.request("GET", f"charges/{obj['charge']}").get("payment_intent")
        if not payment:
            raise CreditError("Missing payment for billing review")
        store.hold_payment(event["id"], payment, kind)
        return {"status": "review"}
    if kind not in {"checkout.session.completed", "checkout.session.async_payment_succeeded"}:
        return {"status": "ignored"}
    session_id = obj.get("id", "")
    if not isinstance(session_id, str) or not session_id.startswith("cs_test_"):
        raise CreditError("Invalid sandbox session")
    session = stripe.request("GET", f"checkout/sessions/{session_id}", {"expand[]": "line_items.data.price"})
    if session.get("id") != session_id or session.get("livemode") is not False or session.get("mode") != "payment":
        raise CreditError("Unexpected checkout environment or mode")
    if session.get("payment_status") != "paid":
        return {"status": "unpaid"}
    if session.get("metadata", {}).get("walnut_catalog") != CATALOG_VERSION:
        return {"status": "ignored"}
    lines = session.get("line_items", {})
    rows = lines.get("data", [])
    if lines.get("has_more") or len(rows) != 1:
        raise CreditError("Unexpected checkout items")
    line, price = rows[0], rows[0].get("price", {})
    validate_price(price)
    quantity = line.get("quantity")
    if type(quantity) is not int or quantity <= 0:
        raise CreditError("Invalid credit quantity")
    amount = quantity * PURCHASE_CENTS_PER_CREDIT
    purchased_credits(amount)
    if (price.get("id") != price_id
            or session.get("currency") != "usd" or session.get("amount_subtotal") != amount
            or session.get("total_details", {}).get("amount_discount", 0) != 0
            or session.get("amount_total") != amount + session.get("total_details", {}).get("amount_tax", 0)
            or not isinstance(session.get("payment_intent"), str)):
        raise CreditError("Paid amounts or price do not match the credit contract")
    # Owner/account and amount come from the locally created order, not webhook metadata.
    added = store.fulfill(session["metadata"].get("walnut_order"), session_id,
                         session["payment_intent"], amount)
    return {"status": "credited" if added else "already_processed_or_review"}
