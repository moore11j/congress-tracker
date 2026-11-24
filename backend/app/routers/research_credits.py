"""Opt-in, isolated-sandbox-only routes. Live checkout is intentionally unavailable."""
from functools import lru_cache
import os
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.rate_limit import RateLimitRule, rate_limiter
from app.services.research_credits import CreditError, CreditStore, MONTHLY_ALLOWANCE, TASK_PRICES
from app.services.research_credit_checkout import (SandboxStripe, create_checkout,
    process_event, sandbox_configuration, verify_signature)

router = APIRouter(prefix="/research-credits", tags=["research-credit-sandbox"])


def isolated_database_url():
    if os.getenv("APP_ENV") not in {"test", "local", "sandbox"}:
        raise CreditError("Credit billing requires an isolated sandbox backend")
    values = [os.getenv("DATABASE_URL", ""), os.getenv("WALNUT_CREDITS_DATABASE_URL", "")]
    for value in values:
        if not value:
            raise CreditError("Explicit sandbox databases are required")
        try:
            url = make_url(value)
        except ArgumentError as exc:
            raise CreditError("Invalid sandbox database URL") from exc
        if url.get_backend_name() not in {"sqlite", "postgresql"}:
            raise CreditError("Unsupported sandbox database")
        name = url.database or ""
        if not (name.endswith(".sandbox.db") or name.endswith("_sandbox")):
            raise CreditError("Both account and credit databases must be named sandbox databases")
    if make_url(values[0]) == make_url(values[1]):
        raise CreditError("Credit sandbox must use its separate ledger database")
    return values[1]


@lru_cache(maxsize=1)
def credit_store(url):
    engine = create_engine(url, hide_parameters=True,
        connect_args={"check_same_thread": False, "timeout": 30} if url.startswith("sqlite:") else {})
    return CreditStore(engine)


def configured():
    key, origin, price = sandbox_configuration()
    store = credit_store(isolated_database_url())
    return store, SandboxStripe(key), origin, price


def verified_user(db, request):
    user = current_user(db, request, required=True)
    if user.email_verified_at is None:
        raise HTTPException(status_code=403, detail="Verify your sandbox account email first")
    return user


class CheckoutInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount_cents: StrictInt = Field(ge=500, le=100000)
    request_id: UUID


@router.get("/balance")
def balance(request: Request, db: Session = Depends(get_db)):
    try:
        store, _, _, _ = configured()
    except (CreditError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="Credit sandbox is not configured") from exc
    user = verified_user(db, request)
    return {**store.balance(str(user.id)), "sandbox": True,
            "allowances": MONTHLY_ALLOWANCE, "task_prices": TASK_PRICES}


@router.post("/checkout")
def checkout(payload: CheckoutInput, request: Request, db: Session = Depends(get_db)):
    try:
        store, stripe, origin, price = configured()
    except (CreditError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="Credit sandbox is not configured") from exc
    if request.headers.get("origin") != origin:
        raise HTTPException(status_code=403, detail="Untrusted checkout origin")
    user = verified_user(db, request)
    # Account-bound idempotency; neither amount nor recipient is read from URL returns.
    rate_limiter.check([RateLimitRule("research_credit_checkout", 10, 60, str(user.id))],
                       endpoint_group="research_credit_checkout")
    order_id = f"{user.id}:{payload.request_id}"
    try:
        return create_checkout(store, stripe, str(user.id), order_id, payload.amount_cents, origin, price)
    except CreditError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/stripe-webhook")
async def webhook(request: Request):
    try:
        store, stripe, _, price = configured()
    except (CreditError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="Credit sandbox is not configured") from exc
    payload = bytearray()
    async for chunk in request.stream():
        payload.extend(chunk)
        if len(payload) > 256000:
            raise HTTPException(status_code=413, detail="Webhook too large")
    try:
        event = verify_signature(bytes(payload), request.headers.get("stripe-signature", ""),
                                 os.getenv("WALNUT_STRIPE_SANDBOX_WEBHOOK_SECRET", ""))
    except CreditError as exc:
        raise HTTPException(status_code=400, detail="Invalid sandbox webhook") from exc
    # Processing failures must return non-2xx so Stripe can retry after reconciliation.
    try:
        return process_event(store, stripe, event, price)
    except CreditError as exc:
        raise HTTPException(status_code=409, detail="Sandbox payment needs reconciliation") from exc
