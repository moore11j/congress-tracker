"""Transactional research-credit ledger. No schema or network work on import.

Entitlements are deliberately outside this ledger: a balance never grants data access.
Schema installation is explicit and belongs in an isolated sandbox before release.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from typing import Any

from sqlalchemy import (Boolean, Column, Integer, JSON, MetaData, String, Table,
                        CheckConstraint, delete, select, update)

metadata = MetaData()
wallets = Table("research_credit_wallets", metadata,
    Column("user_id", String(80), primary_key=True),
    Column("version", Integer, nullable=False, default=0),
    Column("billing_hold", Boolean, nullable=False, default=False))
grants = Table("research_credit_grants", metadata,
    Column("source", String(180), primary_key=True),
    Column("user_id", String(80), nullable=False, index=True),
    Column("kind", String(20), nullable=False),
    Column("total", Integer, nullable=False),
    Column("remaining", Integer, nullable=False),
    Column("expires_at", Integer),
    CheckConstraint("remaining >= 0 AND remaining <= total AND total > 0"))
jobs = Table("research_credit_jobs", metadata,
    Column("job_id", String(180), primary_key=True),
    Column("user_id", String(80), nullable=False, index=True),
    Column("credits", Integer, nullable=False),
    Column("state", String(20), nullable=False),
    Column("parent_job_id", String(180)),
    Column("evidence_key", String(64)),
    Column("question_hash", String(64)),
    Column("allocations", JSON, nullable=False))
clarifications = Table("research_credit_clarifications", metadata,
    Column("parent_job_id", String(180), primary_key=True),
    Column("job_id", String(180), nullable=False, unique=True))
orders = Table("research_credit_orders", metadata,
    Column("order_id", String(80), primary_key=True),
    Column("user_id", String(80), nullable=False, index=True),
    Column("amount_cents", Integer, nullable=False),
    Column("session_id", String(180), unique=True),
    Column("payment_intent", String(180), unique=True),
    Column("state", String(20), nullable=False))
holds = Table("research_credit_holds", metadata,
    Column("event_id", String(180), primary_key=True),
    Column("payment_intent", String(180), nullable=False),
    Column("reason", String(60), nullable=False))
payments = Table("research_credit_payment_locks", metadata,
    Column("payment_intent", String(180), primary_key=True),
    Column("version", Integer, nullable=False))

subscriptions = Table("research_credit_subscriptions", metadata,
    Column("subscription_id", String(180), primary_key=True),
    Column("user_id", String(80), nullable=False, index=True),
    Column("customer_id", String(180), nullable=False),
    Column("status", String(40), nullable=False),
    Column("event_created", Integer, nullable=False))
coverage = Table("research_credit_subscription_coverage", metadata,
    Column("invoice_id", String(180), primary_key=True),
    Column("subscription_id", String(180), nullable=False, index=True),
    Column("user_id", String(80), nullable=False, index=True),
    Column("tier", String(20), nullable=False),
    Column("interval", String(10), nullable=False),
    Column("period_start", Integer, nullable=False),
    Column("period_end", Integer, nullable=False))

MONTHLY_ALLOWANCE = {"free": 100, "premium": 2000, "pro": 4000}
FREE_FIRST_MONTH = 250
TASK_PRICES = {"answer": 25, "analysis": 100, "report": 400}
PURCHASE_CENTS_PER_CREDIT = 2


class CreditError(ValueError):
    pass


def positive(value: Any) -> int:
    if type(value) is not int or value <= 0:
        raise CreditError("A positive integer is required")
    return value


def now_seconds() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def purchased_credits(amount_cents: int) -> int:
    positive(amount_cents)
    if not 500 <= amount_cents <= 100000:
        raise CreditError("Sandbox purchases must be between $5 and $1,000")
    if amount_cents % PURCHASE_CENTS_PER_CREDIT:
        raise CreditError("Use $0.02 increments for whole credits")
    return amount_cents // PURCHASE_CENTS_PER_CREDIT


class CreditStore:
    def __init__(self, engine):
        self.engine = engine

    def install_schema(self):
        metadata.create_all(self.engine)

    def _lock(self, conn, user_id):
        if not isinstance(user_id, str) or not user_id or len(user_id) > 80:
            raise CreditError("Invalid account")
        if conn.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif conn.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise CreditError("Unsupported credit database")
        conn.execute(insert(wallets).values(user_id=user_id, version=0, billing_hold=False)
                     .on_conflict_do_nothing(index_elements=["user_id"]))
        # An UPDATE serializes the complete transaction on both PostgreSQL and SQLite.
        conn.execute(update(wallets).where(wallets.c.user_id == user_id)
                     .values(version=wallets.c.version + 1))
        return conn.execute(select(wallets).where(wallets.c.user_id == user_id)).mappings().one()

    def _grant(self, conn, user_id, source, credits, kind, expires_at):
        positive(credits)
        if kind not in {"monthly", "purchased"} or (kind == "monthly" and not isinstance(expires_at, int)) or (kind == "purchased" and expires_at is not None):
            raise CreditError("Invalid grant kind")
        old = conn.execute(select(grants).where(grants.c.source == source)).mappings().first()
        expected = dict(user_id=user_id, total=credits, kind=kind, expires_at=expires_at)
        if old:
            if any(old[k] != v for k, v in expected.items()):
                raise CreditError("Grant identity conflict")
            return False
        conn.execute(grants.insert().values(source=source, remaining=credits, **expected))
        return True

    def grant_month(self, user_id, tier, cycle_start, cycle_end, *, now=None, account_created_at=None):
        """Internal only. Caller must prove paid coverage for this exact month.

        A unique user/month source prevents reset exploits through plan switching.
        account_created_at must come from the verified account, never a request body.
        Annual scheduling and invoice qualification are not implemented here.
        """
        now = now_seconds() if now is None else now
        if tier not in MONTHLY_ALLOWANCE or not (0 < cycle_start <= now < cycle_end):
            raise CreditError("Invalid current grant cycle")
        if cycle_end - cycle_start > 32 * 86400:
            raise CreditError("Grant must cover one month, not an annual prepayment")
        if account_created_at is not None and (type(account_created_at) is not int or not 0 < account_created_at <= now):
            raise CreditError("Invalid account creation date")
        with self.engine.begin() as conn:
            self._lock(conn, user_id)
            added = self._grant(conn, user_id, f"monthly:{user_id}:{cycle_start}",
                                MONTHLY_ALLOWANCE[tier], "monthly", cycle_end)
            if tier == "free" and account_created_at == cycle_start:
                # One permanent source identity prevents cancel/rejoin bonus resets.
                bonus = self._grant(conn, user_id, f"welcome:{user_id}",
                                    FREE_FIRST_MONTH - MONTHLY_ALLOWANCE["free"], "monthly", cycle_end)
                added = bonus or added
            return added

    def balance(self, user_id, *, now=None):
        now = now_seconds() if now is None else now
        with self.engine.connect() as conn:
            wallet = conn.execute(select(wallets).where(wallets.c.user_id == user_id)).mappings().first()
            rows = conn.execute(select(grants).where(grants.c.user_id == user_id)).mappings().all()
            pending = conn.execute(select(jobs.c.credits).where(jobs.c.user_id == user_id,
                                   jobs.c.state == "reserved")).scalars().all()
        monthly = sum(r["remaining"] for r in rows if r["kind"] == "monthly" and r["expires_at"] > now)
        purchased = sum(r["remaining"] for r in rows if r["kind"] == "purchased")
        return {"monthly": monthly, "purchased": purchased, "available": monthly+purchased,
                "reserved": sum(pending), "billing_hold": bool(wallet and wallet["billing_hold"])}

    def reserve(self, user_id, job_id, credits, *, now=None):
        positive(credits)
        now = now_seconds() if now is None else now
        with self.engine.begin() as conn:
            wallet = self._lock(conn, user_id)
            if wallet["billing_hold"]:
                raise CreditError("Billing review required")
            old = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if old:
                if old["user_id"] != user_id or old["credits"] != credits:
                    raise CreditError("Job identity conflict")
                return old["state"]
            rows = conn.execute(select(grants).where(grants.c.user_id == user_id,
                 grants.c.remaining > 0)).mappings().all()
            rows = sorted((r for r in rows if r["expires_at"] is None or r["expires_at"] > now),
                          key=lambda r: (r["expires_at"] is None, r["expires_at"] or 0, r["source"]))
            if sum(r["remaining"] for r in rows) < credits:
                raise CreditError("Insufficient credits")
            needed, allocations = credits, []
            for row in rows:
                take = min(needed, row["remaining"])
                if not take:
                    break
                conn.execute(update(grants).where(grants.c.source == row["source"])
                             .values(remaining=row["remaining"]-take))
                allocations.append({"source": row["source"], "credits": take})
                needed -= take
            conn.execute(jobs.insert().values(job_id=job_id, user_id=user_id, credits=credits,
                         state="reserved", allocations=allocations))
            return "reserved"

    def reserve_clarification(self, user_id, parent_job_id, job_id, evidence_key, question, *, access_allowed=False):
        """Internal ledger claim only; caller must reauthorize the bound evidence.

        A future answer runner must use only that evidence, no new tools/retrieval,
        and keep this brief response inside the parent's total provider budget.
        No model runner or public clarification endpoint exists here.
        """
        if access_allowed is not True:
            raise CreditError("Evidence access required")
        if not isinstance(evidence_key, str) or not re.fullmatch(r"[0-9a-f]{64}", evidence_key):
            raise CreditError("Invalid evidence binding")
        if not isinstance(question, str) or not question.strip() or len(question) > 600:
            raise CreditError("Clarification must be brief")
        question_hash = hashlib.sha256(question.strip().encode()).hexdigest()
        with self.engine.begin() as conn:
            wallet = self._lock(conn, user_id)
            if wallet["billing_hold"]:
                raise CreditError("Billing review required")
            parent = conn.execute(select(jobs).where(jobs.c.job_id == parent_job_id)).mappings().first()
            if not parent or parent["user_id"] != user_id or parent["state"] != "spent" or parent["credits"] <= 0:
                raise CreditError("A completed paid answer is required")
            if parent["evidence_key"] != evidence_key:
                raise CreditError("Clarification requires the original evidence")
            old = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if old:
                if (old["user_id"] != user_id or old["parent_job_id"] != parent_job_id
                        or old["evidence_key"] != evidence_key or old["question_hash"] != question_hash):
                    raise CreditError("Clarification identity conflict")
                return old["state"]
            if conn.execute(select(clarifications).where(clarifications.c.parent_job_id == parent_job_id)).first():
                raise CreditError("Included clarification already used or reserved")
            conn.execute(jobs.insert().values(job_id=job_id, user_id=user_id, credits=0, state="reserved",
                         parent_job_id=parent_job_id, evidence_key=evidence_key, question_hash=question_hash, allocations=[]))
            conn.execute(clarifications.insert().values(parent_job_id=parent_job_id, job_id=job_id))
            return "reserved"

    def finish(self, user_id, job_id, *, success, evidence_key=None):
        if evidence_key is not None and (not isinstance(evidence_key, str) or not re.fullmatch(r"[0-9a-f]{64}", evidence_key)):
            raise CreditError("Invalid evidence binding")
        with self.engine.begin() as conn:
            self._lock(conn, user_id)
            job = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if not job or job["user_id"] != user_id:
                raise CreditError("Unknown job")
            if evidence_key is not None and job["evidence_key"] not in {None, evidence_key}:
                raise CreditError("Answer evidence identity conflict")
            target = "spent" if success else "released"
            if job["state"] != "reserved":
                if job["state"] != target:
                    raise CreditError("Job already finalized")
                if evidence_key is not None and job["evidence_key"] != evidence_key:
                    raise CreditError("Finalized answer evidence cannot change")
                return target
            if not success:
                for allocation in job["allocations"]:
                    # Restore original grant: release must not extend its expiry.
                    conn.execute(update(grants).where(grants.c.source == allocation["source"])
                                 .values(remaining=grants.c.remaining + allocation["credits"]))
                conn.execute(delete(clarifications).where(clarifications.c.job_id == job_id))
            conn.execute(update(jobs).where(jobs.c.job_id == job_id)
                         .values(state=target, evidence_key=evidence_key or job["evidence_key"]))
            return target

    def create_order(self, user_id, order_id, amount_cents):
        purchased_credits(amount_cents)
        with self.engine.begin() as conn:
            wallet = self._lock(conn, user_id)
            if wallet["billing_hold"]:
                raise CreditError("Billing review required")
            old = conn.execute(select(orders).where(orders.c.order_id == order_id)).mappings().first()
            if old:
                if old["user_id"] != user_id or old["amount_cents"] != amount_cents:
                    raise CreditError("Order identity conflict")
                return dict(old)
            data = dict(order_id=order_id, user_id=user_id, amount_cents=amount_cents, state="created")
            conn.execute(orders.insert().values(**data))
            return data

    def bind_session(self, user_id, order_id, session_id):
        with self.engine.begin() as conn:
            self._lock(conn, user_id)
            order = conn.execute(select(orders).where(orders.c.order_id == order_id)).mappings().one()
            if order["user_id"] != user_id or order["session_id"] not in {None, session_id}:
                raise CreditError("Checkout identity conflict")
            conn.execute(update(orders).where(orders.c.order_id == order_id).values(session_id=session_id))

    def fulfill(self, order_id, session_id, payment_intent, amount_cents):
        credits = purchased_credits(amount_cents)
        with self.engine.begin() as conn:
            self._lock_payment(conn, payment_intent)
            order = conn.execute(select(orders).where(orders.c.order_id == order_id)).mappings().first()
            if not order:
                raise CreditError("Unknown order")
            self._lock(conn, order["user_id"])
            order = conn.execute(select(orders).where(orders.c.order_id == order_id)).mappings().one()
            if order["session_id"] != session_id or order["amount_cents"] != amount_cents:
                raise CreditError("Paid checkout does not match order")
            if order["payment_intent"] not in {None, payment_intent}:
                raise CreditError("Payment identity conflict")
            blocked = conn.execute(select(holds).where(holds.c.payment_intent == payment_intent)).first()
            if blocked:
                conn.execute(update(wallets).where(wallets.c.user_id == order["user_id"]).values(billing_hold=True))
                conn.execute(update(orders).where(orders.c.order_id == order_id)
                             .values(payment_intent=payment_intent, state="review"))
                return False
            added = self._grant(conn, order["user_id"], f"checkout:{session_id}", credits, "purchased", None)
            conn.execute(update(orders).where(orders.c.order_id == order_id)
                         .values(payment_intent=payment_intent, state="paid"))
            return added

    def hold_payment(self, event_id, payment_intent, reason):
        with self.engine.begin() as conn:
            self._lock_payment(conn, payment_intent)
            # Persist an early refund/dispute, even if fulfillment arrives later.
            if conn.dialect.name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert
            else:
                from sqlalchemy.dialects.sqlite import insert
            conn.execute(insert(holds).values(event_id=event_id, payment_intent=payment_intent,
                         reason=reason).on_conflict_do_nothing(index_elements=["event_id"]))
            order = conn.execute(select(orders).where(orders.c.payment_intent == payment_intent)).mappings().first()
            if order:
                self._lock(conn, order["user_id"])
                conn.execute(update(wallets).where(wallets.c.user_id == order["user_id"]).values(billing_hold=True))
                conn.execute(update(orders).where(orders.c.order_id == order["order_id"]).values(state="review"))

    def _lock_payment(self, conn, payment_intent):
        if not isinstance(payment_intent, str) or not payment_intent.startswith("pi_"):
            raise CreditError("Invalid payment identity")
        if conn.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        conn.execute(insert(payments).values(payment_intent=payment_intent, version=0)
                     .on_conflict_do_nothing(index_elements=["payment_intent"]))
        conn.execute(update(payments).where(payments.c.payment_intent == payment_intent)
                     .values(version=payments.c.version + 1))
