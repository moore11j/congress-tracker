import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json

import pytest
from sqlalchemy import create_engine

from app.services.research_credits import CreditStore, CreditError
from app.services.research_credit_checkout import (
    CATALOG_VERSION, SandboxStripe, create_checkout, process_event,
    sandbox_configuration, verify_signature,
)


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'credits.sandbox.db'}",
                          connect_args={"check_same_thread": False, "timeout": 30})
    result = CreditStore(engine)
    result.install_schema()
    yield result
    engine.dispose()


def purchase(store, user="u", order="o", session="cs_test_1", payment="pi_1", amount=1000):
    store.create_order(user, order, amount)
    store.bind_session(user, order, session)
    return store.fulfill(order, session, payment, amount)


def test_monthly_and_purchased_credits_keep_distinct_expiry(store):
    assert store.grant_month("u", "free", 10, 100, now=20)
    assert not store.grant_month("u", "free", 10, 100, now=20)
    purchase(store)
    assert store.balance("u", now=20)["available"] == 600
    store.reserve("u", "job", 150, now=20)
    assert store.balance("u", now=20) == dict(monthly=0, purchased=450, available=450, reserved=150, billing_hold=False)
    store.finish("u", "job", success=False)
    assert store.balance("u", now=101)["available"] == 500
    assert store.finish("u", "job", success=False) == "released"
    with pytest.raises(CreditError):
        store.finish("u", "job", success=True)


def test_job_and_order_identity_are_bound_to_account_and_cost(store):
    purchase(store)
    store.reserve("u", "job", 10)
    assert store.reserve("u", "job", 10) == "reserved"
    for user, cost in [("other", 10), ("u", 50)]:
        with pytest.raises(CreditError):
            store.reserve(user, "job", cost)
    with pytest.raises(CreditError):
        store.finish("other", "job", success=False)
    for user, cost in [("other", 500), ("u", 600)]:
        with pytest.raises(CreditError):
            store.create_order(user, "o", cost)
    assert store.finish("u", "job", success=True) == "spent"
    assert store.finish("u", "job", success=True) == "spent"
    assert store.balance("u")["available"] == 490


@pytest.mark.parametrize("amount", [True, 0, -1, 1.5, "500", 499, 501, 1001, 100001])
def test_invalid_purchase_amounts_do_not_create_credit(store, amount):
    with pytest.raises(CreditError):
        store.create_order("u", "o", amount)
    assert store.balance("u")["available"] == 0


def test_concurrent_spend_cannot_overdraw_and_failure_rolls_back(store):
    purchase(store)
    def reserve(number):
        try:
            return store.reserve("u", f"job{number}", 100)
        except CreditError:
            return "insufficient"
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(12)))
    assert results.count("reserved") == 5
    assert results.count("insufficient") == 7
    assert store.balance("u")["available"] == 0
    assert store.balance("u")["reserved"] == 500


def test_concurrent_duplicate_payment_grants_once(store):
    store.create_order("u", "o", 500)
    store.bind_session("u", "o", "cs_test_1")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.fulfill("o", "cs_test_1", "pi_1", 500), range(12)))
    assert results.count(True) == 1
    assert store.balance("u")["available"] == 250


@pytest.mark.parametrize("early", [True, False])
def test_refund_before_or_after_fulfillment_blocks_new_spend_and_purchase(store, early):
    if early:
        store.hold_payment("evt_refund", "pi_1", "charge.refunded")
    purchase(store)
    if not early:
        store.reserve("u", "existing", 10)
        store.hold_payment("evt_refund", "pi_1", "charge.refunded")
    store.hold_payment("evt_refund", "pi_1", "charge.refunded")
    assert store.balance("u")["billing_hold"]
    for job in ["new", "existing"]:
        with pytest.raises(CreditError, match="Billing review"):
            store.reserve("u", job, 10)
    with pytest.raises(CreditError, match="Billing review"):
        store.create_order("u", "second", 500)
    if early:
        assert store.balance("u")["available"] == 0
    else:
        store.finish("u", "existing", success=False)
        assert store.balance("u")["available"] == 500


def test_concurrent_refund_and_fulfillment_always_leave_billing_hold(store):
    store.create_order("u", "o", 500)
    store.bind_session("u", "o", "cs_test_1")
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(store.fulfill, "o", "cs_test_1", "pi_1", 500),
                pool.submit(store.hold_payment, "evt_refund", "pi_1", "charge.refunded")]
        for job in jobs:
            job.result()
    assert store.balance("u")["billing_hold"]


def test_month_grants_reject_annual_period_and_plan_switch_regrant(store):
    with pytest.raises(CreditError):
        store.grant_month("u", "pro", 1, 365 * 86400, now=10)
    store.grant_month("u", "free", 10, 100, now=20)
    with pytest.raises(CreditError, match="identity"):
        store.grant_month("u", "pro", 10, 100, now=20)


def test_free_first_month_is_250_total_then_100_and_cannot_reset(store):
    assert store.grant_month("u", "free", 10, 100, now=20, account_created_at=10)
    assert not store.grant_month("u", "free", 10, 100, now=20, account_created_at=10)
    assert store.balance("u", now=20)["available"] == 250
    # A changed account date must not mint another bonus or even commit the base grant.
    with pytest.raises(CreditError):
        store.grant_month("u", "free", 100, 200, now=110, account_created_at=100)
    assert store.balance("u", now=110)["available"] == 0
    assert store.grant_month("u", "free", 100, 200, now=110, account_created_at=10)
    assert store.balance("u", now=110)["available"] == 100


def test_first_month_concurrent_retries_grant_once(store):
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: store.grant_month("u", "free", 10, 100, now=20, account_created_at=10), range(10)))
    assert results.count(True) == 1
    assert store.balance("u", now=20)["available"] == 250


def test_approved_premium_allowance_and_paid_first_month(store):
    store.grant_month("u", "premium", 10, 100, now=20, account_created_at=10)
    assert store.balance("u", now=20)["available"] == 2000
    store.grant_month("u", "free", 100, 200, now=110, account_created_at=10)
    assert store.balance("u", now=110)["available"] == 100


EVIDENCE = "a" * 64


def paid_answer(store):
    purchase(store)
    store.reserve("u", "answer", 25)
    store.finish("u", "answer", success=True, evidence_key=EVIDENCE)


def clarify(store, job="clarify", **overrides):
    args = dict(user_id="u", parent_job_id="answer", job_id=job, evidence_key=EVIDENCE,
                question="Explain the last point", access_allowed=True)
    args.update(overrides)
    return store.reserve_clarification(**args)


def test_included_clarification_spends_no_credits_and_cannot_chain(store):
    paid_answer(store)
    assert clarify(store) == "reserved"
    assert clarify(store) == "reserved"
    assert store.balance("u")["available"] == 475
    store.finish("u", "clarify", success=True)
    assert clarify(store) == "spent"
    with pytest.raises(CreditError, match="already used"):
        clarify(store, "second")
    with pytest.raises(CreditError, match="paid answer"):
        clarify(store, "chain", parent_job_id="clarify")
    assert store.balance("u")["available"] == 475


@pytest.mark.parametrize("overrides", [{"user_id": "other"}, {"access_allowed": False},
    {"evidence_key": "b" * 64}, {"evidence_key": None}, {"question": ""}, {"question": "x" * 601}])
def test_clarification_rejects_wrong_owner_denied_access_changed_evidence_or_unbounded_question(store, overrides):
    paid_answer(store)
    with pytest.raises(CreditError):
        clarify(store, **overrides)
    assert clarify(store) == "reserved"


def test_clarification_failure_releases_slot_but_never_rewrites_question_or_evidence(store):
    paid_answer(store)
    clarify(store)
    with pytest.raises(CreditError, match="identity"):
        clarify(store, question="A different question")
    with pytest.raises(CreditError):
        store.finish("u", "answer", success=True, evidence_key="b" * 64)
    store.finish("u", "clarify", success=False)
    assert clarify(store) == "released"
    assert clarify(store, "retry") == "reserved"
    store.finish("u", "retry", success=True)
    with pytest.raises(CreditError):
        clarify(store, "third")


def test_concurrent_clarifications_claim_only_one_slot(store):
    paid_answer(store)
    def claim(number):
        try:
            return clarify(store, f"clarify{number}")
        except CreditError:
            return "unavailable"
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(claim, range(10)))
    assert results.count("reserved") == 1
    assert results.count("unavailable") == 9


def test_incomplete_or_unbound_answer_cannot_offer_clarification(store):
    purchase(store)
    store.reserve("u", "answer", 25)
    with pytest.raises(CreditError):
        clarify(store)
    store.finish("u", "answer", success=True)
    with pytest.raises(CreditError):
        clarify(store)


PRICE = dict(id="price_credit", livemode=False, active=True, currency="usd", unit_amount=2,
             type="one_time", billing_scheme="per_unit", metadata={"walnut_catalog": CATALOG_VERSION})
SESSION = dict(id="cs_test_1", livemode=False, mode="payment", payment_status="paid",
               url="https://checkout.stripe.com/c/pay/cs_test_1", payment_intent="pi_1",
               currency="usd", amount_subtotal=500, amount_total=500,
               metadata={"walnut_order": "o", "walnut_catalog": CATALOG_VERSION},
               total_details={"amount_discount": 0, "amount_tax": 0},
               line_items={"data": [{"price": PRICE, "quantity": 250}], "has_more": False})
EVENT = dict(id="evt_1", livemode=False, type="checkout.session.completed",
             data={"object": {"id": "cs_test_1"}})


class Stripe:
    def __init__(self):
        self.price, self.session, self.calls = copy.deepcopy(PRICE), copy.deepcopy(SESSION), []

    def request(self, method, path, data=None, **kwargs):
        self.calls.append((method, path, data, kwargs))
        return self.price if path.startswith("prices/") else self.session


def test_checkout_return_cannot_grant_and_bound_session_retries_reuse_session(store):
    stripe = Stripe()
    args = (store, stripe, "u", "o", 500, "https://preview.example", "price_credit")
    assert create_checkout(*args)["sandbox"]
    assert create_checkout(*args)["id"] == "cs_test_1"
    assert sum(call[0] == "POST" for call in stripe.calls) == 1
    assert store.balance("u")["available"] == 0
    assert process_event(store, stripe, EVENT, "price_credit")["status"] == "credited"
    assert process_event(store, stripe, EVENT, "price_credit")["status"] == "already_processed_or_review"
    assert store.balance("u")["available"] == 250
    with pytest.raises(CreditError):
        create_checkout(*args)


@pytest.mark.parametrize("field,value", [("livemode", True), ("id", "cs_test_wrong"),
    ("amount_total", 1), ("amount_subtotal", 1), ("currency", "eur"), ("payment_intent", None)])
def test_mismatched_paid_session_never_grants(store, field, value):
    stripe = Stripe()
    store.create_order("u", "o", 500)
    store.bind_session("u", "o", "cs_test_1")
    stripe.session[field] = value
    with pytest.raises(CreditError):
        process_event(store, stripe, EVENT, "price_credit")
    assert store.balance("u")["available"] == 0


def test_unpaid_session_waits_for_successful_payment(store):
    stripe = Stripe()
    store.create_order("u", "o", 500)
    store.bind_session("u", "o", "cs_test_1")
    stripe.session["payment_status"] = "unpaid"
    assert process_event(store, stripe, EVENT, "price_credit")["status"] == "unpaid"
    assert store.balance("u")["available"] == 0
    stripe.session["payment_status"] = "paid"
    event = {**EVENT, "type": "checkout.session.async_payment_succeeded"}
    assert process_event(store, stripe, event, "price_credit")["status"] == "credited"


@pytest.mark.parametrize("amount,quantity", [(500, 250), (1250, 625), (100000, 50000)])
def test_two_cent_checkout_quantity_and_grant_match_exact_amount(store, amount, quantity):
    stripe = Stripe()
    stripe.session["amount_subtotal"] = amount
    stripe.session["amount_total"] = amount
    stripe.session["line_items"]["data"][0]["quantity"] = quantity
    create_checkout(store, stripe, "u", "o", amount, "https://preview.example", "price_credit")
    posted = next(call[2] for call in stripe.calls if call[0] == "POST")
    assert posted["line_items[0][quantity]"] == quantity
    assert process_event(store, stripe, EVENT, "price_credit")["status"] == "credited"
    assert store.balance("u")["available"] == quantity
    process_event(store, stripe, EVENT, "price_credit")
    assert store.balance("u")["available"] == quantity


@pytest.mark.parametrize("change", ["old_unit_price", "old_catalog", "wrong_quantity"])
def test_legacy_or_mismatched_price_cannot_credit_at_new_rate(store, change):
    stripe = Stripe()
    store.create_order("u", "o", 500)
    store.bind_session("u", "o", "cs_test_1")
    price = stripe.session["line_items"]["data"][0]["price"]
    if change == "old_unit_price":
        price["unit_amount"] = 1
    elif change == "old_catalog":
        price["metadata"]["walnut_catalog"] = "walnut_research_v1"
    else:
        stripe.session["line_items"]["data"][0]["quantity"] = 500
    with pytest.raises(CreditError):
        process_event(store, stripe, EVENT, "price_credit")
    assert store.balance("u")["available"] == 0


def test_signature_requires_raw_payload_recent_timestamp_and_test_environment():
    secret, stamp = "whsec_test_fixture", 1000
    def signed(event):
        payload = json.dumps(event).encode()
        digest = hmac.new(secret.encode(), b"1000." + payload, hashlib.sha256).hexdigest()
        return payload, f"t={stamp},v1=old,v1={digest}"
    payload, signature = signed(EVENT)
    assert verify_signature(payload, signature, secret, now=1001) == EVENT
    for body, header, now in [(payload+b" ", signature, 1001), (payload, signature, 1301),
                              (payload, "", 1001), (payload, signature+",t=1000", 1001)]:
        with pytest.raises(CreditError):
            verify_signature(body, header, secret, now=now)
    for event in [{**EVENT, "livemode": True}, {**EVENT, "account": "acct_other"}]:
        body, header = signed(event)
        with pytest.raises(CreditError):
            verify_signature(body, header, secret, now=1001)


def test_sandbox_never_uses_live_key_or_production_origin(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_unused")
    monkeypatch.setenv("WALNUT_CREDITS_SANDBOX_ENABLED", "1")
    monkeypatch.setenv("WALNUT_CREDITS_SANDBOX_ORIGIN", "https://preview.example")
    monkeypatch.setenv("WALNUT_STRIPE_SANDBOX_CREDIT_PRICE_ID", "price_credit")
    monkeypatch.delenv("WALNUT_STRIPE_SANDBOX_SECRET_KEY", raising=False)
    with pytest.raises(CreditError):
        sandbox_configuration()

    with pytest.raises(CreditError):
        SandboxStripe("sk_live_unused")
    monkeypatch.setenv("WALNUT_STRIPE_SANDBOX_SECRET_KEY", "sk_test_fixture")
    assert sandbox_configuration()[1] == "https://preview.example"
    monkeypatch.setenv("WALNUT_CREDITS_SANDBOX_ORIGIN", "https://app.walnutmarkets.com")
    with pytest.raises(CreditError):
        sandbox_configuration()


def test_catalog_installer_creates_three_products_five_prices_and_retries_without_duplicates():
    from scripts.setup_research_credit_sandbox import install_catalog
    class CatalogStripe:
        def __init__(self):
            self.products, self.prices = {}, {}
        def request(self, method, path, data, **kwargs):
            if method == "GET":
                row = self.products.get(data["ids[]"]) if path == "products" else self.prices.get(data["lookup_keys[]"])
                return {"data": [row] if row else []}
            if path == "products":
                row = dict(id=data["id"], name=data["name"], active=True, livemode=False,
                           metadata={"walnut_catalog": data["metadata[walnut_catalog]"]})
                if "metadata[monthly_research_credits]" in data:
                    row["metadata"]["monthly_research_credits"] = str(data["metadata[monthly_research_credits]"])
                self.products[row["id"]] = row
                return row
            row = dict(id=f"price_{len(self.prices)}", active=True, livemode=False,
                       product=data["product"], currency=data["currency"], unit_amount=data["unit_amount"],
                       billing_scheme="per_unit", metadata={"walnut_catalog": CATALOG_VERSION})
            if "recurring[interval]" in data:
                row["recurring"] = dict(interval=data["recurring[interval]"], interval_count=1)
            self.prices[data["lookup_key"]] = row
            return row
    stripe = CatalogStripe()
    receipt = install_catalog(stripe)
    assert install_catalog(stripe) == receipt
    assert len(stripe.products) == 3
    assert len(stripe.prices) == 5
    assert sorted(p["unit_amount"] for p in stripe.prices.values()) == [2, 2495, 3995, 24950, 39995]
    next(iter(stripe.prices.values()))["unit_amount"] = 999
    with pytest.raises(CreditError, match="conflicts"):
        install_catalog(stripe)


@pytest.mark.parametrize("app_env,account,ledger,valid", [
    ("production", "sqlite:///accounts.sandbox.db", "sqlite:///credits.sandbox.db", False),
    ("sandbox", "sqlite:///accounts.db", "sqlite:///credits.sandbox.db", False),
    ("sandbox", "sqlite:///accounts.sandbox.db", "sqlite:///accounts.sandbox.db", False),
    ("sandbox", "bad", "sqlite:///credits.sandbox.db", False),
    ("sandbox", "sqlite:///accounts.sandbox.db", "sqlite:///credits.sandbox.db", True),
])
def test_backend_requires_explicit_separate_sandbox_databases(monkeypatch, app_env, account, ledger, valid):
    from app.routers.research_credits import isolated_database_url
    monkeypatch.setenv("APP_ENV", app_env)
    monkeypatch.setenv("DATABASE_URL", account)
    monkeypatch.setenv("WALNUT_CREDITS_DATABASE_URL", ledger)
    if valid:
        assert isolated_database_url() == ledger
    else:
        with pytest.raises(CreditError):
            isolated_database_url()


def test_http_routes_require_configuration_auth_origin_and_signature(monkeypatch, store):
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient
    from app.routers import research_credits as api
    from types import SimpleNamespace
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[api.get_db] = lambda: None
    client = TestClient(app)
    stripe = Stripe()
    monkeypatch.setattr(api, "configured", lambda: (store, stripe, "https://preview.example", "price_credit"))
    monkeypatch.setattr(api, "current_user", lambda *args, **kwargs: (_ for _ in ()).throw(HTTPException(401)))
    assert client.get("/research-credits/balance").status_code == 401
    monkeypatch.setattr(api, "current_user", lambda *args, **kwargs: SimpleNamespace(id=1, email_verified_at=None))
    assert client.get("/research-credits/balance").status_code == 403
    monkeypatch.setattr(api, "current_user", lambda *args, **kwargs: SimpleNamespace(id=1, email_verified_at=1))
    assert client.get("/research-credits/balance").json()["available"] == 0
    body = dict(amount_cents=500, request_id="c0297685-920e-4799-a31b-653556f5a885")
    assert client.post("/research-credits/checkout", json=body).status_code == 403
    assert client.post("/research-credits/checkout", json={**body, "user_id": "victim"}, headers={"origin": "https://preview.example"}).status_code == 422
    assert client.post("/research-credits/stripe-webhook", json=EVENT).status_code == 400
    api.rate_limiter.reset()
    response = client.post("/research-credits/checkout", json=body, headers={"origin": "https://preview.example"})
    assert response.status_code == 200
    assert response.json()["sandbox"]
    assert store.balance("1")["available"] == 0
    assert any(c[2] and c[2].get("client_reference_id") == "1" for c in stripe.calls)
    api.rate_limiter.reset()


def test_authenticated_return_confirms_payment_immediately_without_double_grant(monkeypatch, store):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.routers import research_credits as api
    from types import SimpleNamespace
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[api.get_db] = lambda: None
    stripe = Stripe()
    monkeypatch.setattr(api, "configured", lambda: (store, stripe, "https://preview.example", "price_credit"))
    monkeypatch.setattr(api, "current_user", lambda *args, **kwargs: SimpleNamespace(id=1, email_verified_at=1))
    store.create_order("1", "o", 500)
    store.bind_session("1", "o", "cs_test_1")
    api.rate_limiter.reset()
    with TestClient(app) as client:
        body, headers = {"session_id": "cs_test_1"}, {"origin": "https://preview.example"}
        assert client.post("/research-credits/confirm", json=body).status_code == 403
        first = client.post("/research-credits/confirm", json=body, headers=headers)
        assert first.status_code == 200
        assert first.json()["status"] == "credited"
        assert first.json()["balance"]["available"] == 250
        assert client.get("/research-credits/balance").json()["available"] == 250
        process_event(store, stripe, EVENT, "price_credit")
        assert client.post("/research-credits/confirm", json=body, headers=headers).json()["balance"]["available"] == 250
        monkeypatch.setattr(api, "current_user", lambda *args, **kwargs: SimpleNamespace(id=2, email_verified_at=1))
        calls = len(stripe.calls)
        assert client.post("/research-credits/confirm", json=body, headers=headers).status_code == 404
        assert len(stripe.calls) == calls
    api.rate_limiter.reset()
