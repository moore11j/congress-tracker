import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select

from app.services.research_credits import CreditStore, CreditError, coverage
from app.services.research_credit_checkout import CATALOG_VERSION
from app.services.research_credit_subscriptions import (
    PLAN_AMOUNTS, grant_due, month_boundary, process_subscription_event,
)


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'subscription.sandbox.db'}",
        connect_args={"check_same_thread": False, "timeout": 30})
    result = CreditStore(engine)
    result.install_schema()
    yield result
    engine.dispose()


START = int(datetime(2026, 1, 31, 12, tzinfo=timezone.utc).timestamp())


class Stripe:
    def __init__(self, tier="premium", interval="month"):
        self.contracts = {"price_plan": (tier, interval)}
        amount = PLAN_AMOUNTS[(tier, interval)]
        self.price = dict(id="price_plan", livemode=False, currency="usd", unit_amount=amount,
            type="recurring", billing_scheme="per_unit", recurring={"interval": interval, "interval_count": 1},
            metadata={"walnut_catalog": CATALOG_VERSION})
        self.sub = dict(id="sub_1", livemode=False, customer="cus_1", status="active", billing_cycle_anchor=START)
        self.invoice = dict(id="in_1", livemode=False, status="paid", amount_remaining=0,
            amount_paid=amount, currency="usd", customer="cus_1", billing_reason="subscription_create",
            parent={"subscription_details": {"subscription": "sub_1"}},
            lines={"has_more": False, "data": [dict(livemode=False, amount=amount, currency="usd", quantity=1,
                parent={"subscription_item_details": {"subscription": "sub_1", "proration": False}},
                pricing={"price_details": {"price": "price_plan"}},
                period={"start": START, "end": month_boundary(START, 12 if interval == "year" else 1)})]})
        self.event = dict(id="evt_1", livemode=False, created=START, type="invoice.paid", data={"object": {"id": "in_1"}})
        self.calls = []

    def request(self, method, path, data=None):
        self.calls.append(path)
        return copy.deepcopy(self.invoice if path.startswith("invoices/") else self.sub if path.startswith("subscriptions/") else self.price)


def deliver(store, stripe, *, now=START + 1, resolver=lambda customer: "u" if customer == "cus_1" else None):
    return process_subscription_event(store, stripe, stripe.event, resolver, stripe.contracts, now=now)


@pytest.mark.parametrize("tier,credits", [("premium", 2000), ("pro", 4000)])
def test_paid_subscription_and_duplicate_delivery_grant_once(store, tier, credits):
    stripe = Stripe(tier)
    assert deliver(store, stripe)["status"] == "credited"
    assert deliver(store, stripe)["status"] == "subscription_synced"
    assert store.balance("u", now=START + 1)["available"] == credits
    assert stripe.calls[:3] == ["invoices/in_1", "subscriptions/sub_1", "prices/price_plan"]


def test_concurrent_invoice_delivery_is_one_grant(store):
    stripe = Stripe()
    with ThreadPoolExecutor(max_workers=6) as pool:
        result = list(pool.map(lambda _: deliver(store, stripe)["status"], range(12)))
    assert result.count("credited") == 1
    assert store.balance("u", now=START + 1)["available"] == 2000


def test_annual_months_do_not_drift_or_stack_missed_allowances(store):
    stripe = Stripe("pro", "year")
    deliver(store, stripe)
    store.reserve("u", "job", 400, now=START + 1)
    store.finish("u", "job", success=True)
    feb = month_boundary(START, 1)
    march = month_boundary(START, 2)
    assert datetime.fromtimestamp(feb, timezone.utc).day == 28
    assert datetime.fromtimestamp(march, timezone.utc).day == 31
    assert grant_due(store, "u", now=feb)
    assert not grant_due(store, "u", now=feb + 1)
    assert store.balance("u", now=feb)["available"] == 4000
    assert grant_due(store, "u", now=month_boundary(START, 7))
    assert store.balance("u", now=month_boundary(START, 7))["available"] == 4000
    assert not grant_due(store, "u", now=month_boundary(START, 12))
    assert store.balance("u", now=month_boundary(START, 12))["available"] == 0


def test_unpaid_and_trialing_never_grant(store):
    stripe = Stripe()
    stripe.invoice["status"] = "open"
    assert deliver(store, stripe)["status"] == "unpaid"
    stripe.invoice["status"] = "paid"
    stripe.sub["status"] = "trialing"
    deliver(store, stripe)
    assert store.balance("u", now=START + 1)["available"] == 0


def test_monthly_renewal_expires_old_allowance_and_preserves_purchased(store):
    stripe = Stripe()
    store.create_order("u", "order", 500)
    store.bind_session("u", "order", "cs_test_1")
    store.fulfill("order", "cs_test_1", "pi_1", 500)
    deliver(store, stripe)
    next_start = month_boundary(START, 1)
    stripe.invoice["id"] = "in_2"
    stripe.event["data"]["object"]["id"] = "in_2"
    stripe.event["created"] = next_start
    stripe.invoice["billing_reason"] = "subscription_cycle"
    stripe.invoice["lines"]["data"][0]["period"] = dict(start=next_start, end=month_boundary(START, 2))
    deliver(store, stripe, now=next_start + 1)
    assert store.balance("u", now=next_start + 1)["available"] == 2250


def test_cancellation_stops_future_annual_grants_and_stale_event_cannot_reopen(store):
    stripe = Stripe(interval="year")
    deliver(store, stripe)
    stripe.sub["status"] = "canceled"
    stripe.event.update(type="customer.subscription.deleted", created=START + 10, data={"object": {"id": "sub_1"}})
    deliver(store, stripe)
    stripe.sub["status"] = "active"
    stripe.event.update(type="customer.subscription.updated", created=START)
    deliver(store, stripe)
    assert not grant_due(store, "u", now=month_boundary(START, 1))
    assert store.balance("u", now=month_boundary(START, 1))["available"] == 0


@pytest.mark.parametrize("change", ["owner", "live", "wrong_customer", "price", "quantity", "amount", "currency", "period", "proration", "multi_line", "truncated", "discount", "credit_note", "pre_credit_note", "plan_change"])
def test_incomplete_or_wrong_invoice_cannot_mint(store, change):
    stripe = Stripe()
    line = stripe.invoice["lines"]["data"][0]
    if change == "owner":
        with pytest.raises(CreditError): deliver(store, stripe, resolver=lambda _: None)
        return
    if change == "live": stripe.invoice["livemode"] = True
    if change == "wrong_customer": stripe.invoice["customer"] = "cus_other"
    if change == "price": stripe.price["unit_amount"] = 1
    if change == "quantity": line["quantity"] = True
    if change == "amount": stripe.invoice["amount_paid"] = 0
    if change == "currency": line["currency"] = "eur"
    if change == "period": line["period"]["end"] += 1
    if change == "proration": line["parent"]["subscription_item_details"]["proration"] = True
    if change == "multi_line": stripe.invoice["lines"]["data"].append(copy.deepcopy(line))
    if change == "truncated": stripe.invoice["lines"]["has_more"] = True
    if change == "discount": stripe.invoice["total_discount_amounts"] = [{"amount": 100}]
    if change == "credit_note": stripe.invoice["post_payment_credit_notes_amount"] = 1
    if change == "pre_credit_note": stripe.invoice["pre_payment_credit_notes_amount"] = 1
    if change == "plan_change": stripe.invoice["billing_reason"] = "subscription_update"
    with pytest.raises(CreditError): deliver(store, stripe)
    assert store.balance("u", now=START + 1)["available"] == 0
    with store.engine.connect() as conn: assert conn.execute(select(coverage)).first() is None


def test_second_overlapping_subscription_and_changed_invoice_rejected(store):
    stripe = Stripe()
    deliver(store, stripe)
    stripe.invoice["id"] = "in_2"
    stripe.event["data"]["object"]["id"] = "in_2"
    with pytest.raises(CreditError, match="Overlapping"):
        deliver(store, stripe)
    assert store.balance("u", now=START + 1)["available"] == 2000


def test_signed_subscription_webhook_uses_existing_verified_account_binding(monkeypatch, store):
    import hashlib
    import hmac
    import json
    import time
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session
    from sqlalchemy.pool import StaticPool
    from app.models import UserAccount
    from app.routers import research_credits as api
    stripe = Stripe()
    started = int(time.time()) - 10
    stripe.invoice["lines"]["data"][0]["period"] = dict(start=started, end=month_boundary(started, 1))
    stripe.sub["billing_cycle_anchor"] = stripe.invoice["lines"]["data"][0]["period"]["start"]
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    UserAccount.__table__.create(engine)
    with Session(engine) as db:
        db.add(UserAccount(id=1, email="fixture@example.invalid", email_verified_at=datetime.now(timezone.utc),
            stripe_customer_id="cus_1", is_suspended=False))
        db.commit()
    def account_db():
        with Session(engine) as db: yield db
    app = FastAPI(); app.include_router(api.router); app.dependency_overrides[api.get_db] = account_db
    monkeypatch.setattr(api, "configured", lambda: (store, stripe, "https://preview.example", "price_credit"))
    monkeypatch.setenv("WALNUT_STRIPE_SANDBOX_PREMIUM_MONTHLY_PRICE_ID", "price_plan")
    monkeypatch.setenv("WALNUT_STRIPE_SANDBOX_WEBHOOK_SECRET", "whsec_fixture")
    payload = json.dumps(stripe.event).encode(); stamp = int(time.time())
    signature = hmac.new(b"whsec_fixture", str(stamp).encode()+b"."+payload, hashlib.sha256).hexdigest()
    with TestClient(app) as client:
        response = client.post("/research-credits/stripe-webhook", content=payload,
            headers={"stripe-signature": f"t={stamp},v1={signature}"})
        assert response.status_code == 200, response.text
    assert store.balance("1")["available"] == 2000
    assert store.balance("u")["available"] == 0
    engine.dispose()


@pytest.mark.parametrize("early", [True, False])
def test_subscription_refund_holds_current_and_future_spend(store, early):
    from app.services.research_credit_checkout import process_event
    stripe = Stripe(interval="year")
    refund = dict(id="evt_refund", livemode=False, type="charge.refunded",
        data={"object": {"payment_intent": "pi_refund", "customer": "cus_1"}})
    def hold():
        return process_event(store, stripe, refund, "price_credit", resolve_customer=lambda _: "u")
    if early: hold()
    deliver(store, stripe)
    if not early: hold()
    assert store.balance("u", now=START + 1)["billing_hold"]
    with pytest.raises(CreditError, match="Billing review"):
        store.reserve("u", "answer", 25, now=START + 1)
    assert not grant_due(store, "u", now=month_boundary(START, 1))
