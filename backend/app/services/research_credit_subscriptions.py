"""Invoice-qualified subscription grants for the isolated credit sandbox.

The customer resolver must use a verified server-side account binding. Never
resolve ownership from an email, event metadata or a browser-provided account ID.
"""
import calendar
import os
import re
from datetime import datetime, timezone

from sqlalchemy import select, update

from app.services.research_credits import (
    CreditError, MONTHLY_ALLOWANCE, coverage, grants, subscriptions, wallets, now_seconds,
)
from app.services.research_credit_checkout import CATALOG_VERSION

PLAN_AMOUNTS = {("premium", "month"): 2495, ("premium", "year"): 24950,
                ("pro", "month"): 3995, ("pro", "year"): 39995}


def price_contracts():
    result = {}
    for tier, interval in PLAN_AMOUNTS:
        suffix = "MONTHLY" if interval == "month" else "ANNUAL"
        value = os.getenv(f"WALNUT_STRIPE_SANDBOX_{tier.upper()}_{suffix}_PRICE_ID", "")
        if value:
            object_id(value, "price")
            if value in result:
                raise CreditError("Subscription price IDs must be distinct")
            result[value] = (tier, interval)
    return result


def object_id(value, prefix):
    if not isinstance(value, str) or not re.fullmatch(prefix + r"_[A-Za-z0-9_]+", value):
        raise CreditError("Invalid billing object identity")
    return value


def month_boundary(start, offset):
    anchor = datetime.fromtimestamp(start, timezone.utc)
    absolute = anchor.year * 12 + anchor.month - 1 + offset
    year, month = absolute // 12, absolute % 12 + 1
    return int(anchor.replace(year=year, month=month,
        day=min(anchor.day, calendar.monthrange(year, month)[1])).timestamp())


def _sync(conn, store, user_id, sub, event_created):
    old = conn.execute(select(subscriptions).where(subscriptions.c.subscription_id == sub["id"])).mappings().first()
    if old and (old["user_id"] != user_id or old["customer_id"] != sub["customer"]):
        raise CreditError("Subscription ownership conflict")
    data = dict(user_id=user_id, customer_id=sub["customer"], status=sub["status"], event_created=event_created)
    if not old:
        conn.execute(subscriptions.insert().values(subscription_id=sub["id"], **data))
    elif old["status"] != "canceled" and event_created >= old["event_created"]:
        conn.execute(update(subscriptions).where(subscriptions.c.subscription_id == sub["id"]).values(**data))


def _allocate(conn, store, user_id, now):
    wallet = conn.execute(select(wallets).where(wallets.c.user_id == user_id)).mappings().one()
    if wallet["billing_hold"]:
        return False
    rows = conn.execute(select(coverage).join(subscriptions,
        coverage.c.subscription_id == subscriptions.c.subscription_id).where(
        coverage.c.user_id == user_id, coverage.c.period_start <= now,
        coverage.c.period_end > now, subscriptions.c.status == "active")).mappings().all()
    if len(rows) > 1:
        raise CreditError("Overlapping paid subscription coverage needs reconciliation")
    added = False
    for row in rows:
        start, end = row["period_start"], row["period_end"]
        if row["interval"] == "year":
            for offset in range(12):
                lower, upper = month_boundary(start, offset), min(month_boundary(start, offset + 1), end)
                if lower <= now < upper:
                    start, end = lower, upper
                    break
        source = f"paid:{row['subscription_id']}:{start}"
        added = store._grant(conn, user_id, source, MONTHLY_ALLOWANCE[row["tier"]], "monthly", end)
        if added:
            # Paid allowance replaces unused introductory/free credits, never top-ups.
            conn.execute(update(grants).where(grants.c.user_id == user_id,
                grants.c.kind == "monthly", grants.c.expires_at > now,
                ~grants.c.source.startswith("paid:")).values(expires_at=now))
    return added


def grant_due(store, user_id, *, now=None):
    """Lazy monthly allocation also used by the scheduled sandbox reconciler.

    Missed months do not accumulate. Annual credits unlock one month at a time.
    """
    now = now_seconds() if now is None else now
    # Most balance polls do no writes. Recheck eligibility under the wallet lock
    # only when a current paid month could still need its allocation.
    with store.engine.connect() as conn:
        rows = conn.execute(select(coverage).join(subscriptions,
            coverage.c.subscription_id == subscriptions.c.subscription_id).where(
            coverage.c.user_id == user_id, coverage.c.period_start <= now,
            coverage.c.period_end > now, subscriptions.c.status == "active")).mappings().all()
        if not rows:
            return False
        if len(rows) == 1:
            row = rows[0]
            start = row["period_start"]
            if row["interval"] == "year":
                start = max(month_boundary(start, offset) for offset in range(12) if month_boundary(start, offset) <= now)
            if conn.execute(select(grants.c.source).where(grants.c.source == f"paid:{row['subscription_id']}:{start}")).first():
                return False
    with store.engine.begin() as conn:
        store._lock(conn, user_id)
        return _allocate(conn, store, user_id, now)


def process_subscription_event(store, stripe, event, resolve_customer, contracts=None, *, now=None):
    if event.get("livemode") is not False or event.get("account"):
        raise CreditError("Only direct sandbox events are accepted")
    kind = event.get("type", "")
    if kind not in {"invoice.paid", "invoice.payment_failed", "customer.subscription.created",
                    "customer.subscription.updated", "customer.subscription.deleted",
                    "customer.subscription.paused", "customer.subscription.resumed"}:
        return {"status": "ignored"}
    contracts = price_contracts() if contracts is None else contracts
    created = event.get("created")
    if type(created) is not int or created <= 0:
        raise CreditError("Missing event ordering timestamp")
    now = now_seconds() if now is None else now
    obj = event.get("data", {}).get("object", {})
    invoice = None
    if kind.startswith("invoice."):
        invoice_id = object_id(obj.get("id"), "in")
        invoice = stripe.request("GET", f"invoices/{invoice_id}")
        if invoice.get("id") != invoice_id or invoice.get("livemode") is not False:
            raise CreditError("Invoice environment or identity mismatch")
        sub_id = (invoice.get("parent") or {}).get("subscription_details", {}).get("subscription")
        if not sub_id:
            return {"status": "ignored"}
    else:
        sub_id = obj.get("id")
    sub_id = object_id(sub_id, "sub")
    sub = stripe.request("GET", f"subscriptions/{sub_id}")
    if sub.get("id") != sub_id or sub.get("livemode") is not False:
        raise CreditError("Subscription environment or identity mismatch")
    customer = object_id(sub.get("customer"), "cus")
    user_id = resolve_customer(customer)
    if not user_id:
        raise CreditError("Verified subscription account binding is not ready")
    user_id = str(user_id)
    if invoice and invoice.get("customer") != customer:
        raise CreditError("Invoice customer mismatch")
    record = None
    if kind == "invoice.paid":
        if invoice.get("status") != "paid" or invoice.get("amount_remaining") != 0:
            return {"status": "unpaid"}
        if invoice.get("billing_reason") not in {"subscription_create", "subscription_cycle"}:
            raise CreditError("Proration or plan change needs explicit credit reconciliation")
        rows = invoice.get("lines", {})
        if rows.get("has_more") or len(rows.get("data", [])) != 1:
            raise CreditError("Expected one complete subscription invoice line")
        line = rows["data"][0]
        parent = line.get("parent", {}).get("subscription_item_details", {})
        price_id = line.get("pricing", {}).get("price_details", {}).get("price")
        if price_id not in contracts:
            raise CreditError("Unconfigured subscription price")
        tier, interval = contracts[price_id]
        price = stripe.request("GET", f"prices/{object_id(price_id, 'price')}")
        if (price.get("id") != price_id or price.get("livemode") is not False
                or price.get("currency") != "usd" or price.get("unit_amount") != PLAN_AMOUNTS[(tier, interval)]
                or price.get("type") != "recurring" or price.get("billing_scheme") != "per_unit"
                or price.get("transform_quantity") or price.get("recurring", {}).get("interval") != interval
                or price.get("recurring", {}).get("interval_count") != 1
                or price.get("metadata", {}).get("walnut_catalog") != CATALOG_VERSION):
            raise CreditError("Subscription price does not match the approved catalog")
        amount = PLAN_AMOUNTS[(tier, interval)]
        start, end = line.get("period", {}).get("start"), line.get("period", {}).get("end")
        anchor = sub.get("billing_cycle_anchor")
        if type(anchor) is not int or anchor <= 0 or type(start) is not int or start < anchor:
            raise CreditError("Missing subscription calendar anchor")
        first, current = datetime.fromtimestamp(anchor, timezone.utc), datetime.fromtimestamp(start, timezone.utc)
        offset = (current.year - first.year) * 12 + current.month - first.month
        if (parent.get("subscription") != sub_id or parent.get("proration") is not False
                or line.get("livemode") is not False or type(line.get("quantity")) is not int
                or line["quantity"] != 1 or line.get("amount") != amount or line.get("currency") != "usd"
                or invoice.get("currency") != "usd" or invoice.get("amount_paid", 0) < amount
                or invoice.get("total_discount_amounts") or invoice.get("post_payment_credit_notes_amount", 0)
                or invoice.get("pre_payment_credit_notes_amount", 0)
                or type(start) is not int or type(end) is not int or start <= 0
                or start != month_boundary(anchor, offset)
                or (interval == "year" and offset % 12 != 0)
                or end != month_boundary(anchor, offset + (12 if interval == "year" else 1))):
            raise CreditError("Invoice coverage or payment does not match the approved plan")
        record = dict(invoice_id=invoice["id"], subscription_id=sub_id, user_id=user_id,
                      tier=tier, interval=interval, period_start=start, period_end=end)
    with store.engine.begin() as conn:
        store._lock(conn, user_id)
        _sync(conn, store, user_id, sub, created)
        if record:
            prior = conn.execute(select(coverage).where(coverage.c.invoice_id == record["invoice_id"])).mappings().first()
            if prior and dict(prior) != record:
                raise CreditError("Invoice coverage identity changed")
            if not prior:
                overlap = conn.execute(select(coverage).where(coverage.c.user_id == user_id,
                    coverage.c.period_end > record["period_start"], coverage.c.period_start < record["period_end"])).first()
                if overlap:
                    raise CreditError("Overlapping invoices need credit reconciliation")
                conn.execute(coverage.insert().values(**record))
        added = _allocate(conn, store, user_id, now)
    return {"status": "credited" if added else "subscription_synced"}
