from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from html import escape as html_escape
from io import BytesIO
from typing import Any, Literal
from urllib.parse import urlencode, urlparse

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.auth import (
    SESSION_COOKIE_NAME,
    admin_emails,
    attach_legacy_watchlists_to_user,
    current_user,
    get_or_create_user,
    hash_password,
    is_admin_user,
    normalize_email,
    require_admin_user,
    reset_token_hash,
    sign_session_payload,
    verify_session_token,
    verify_password,
)
from app.db import get_db
from app.entitlements import (
    DEFAULT_FEATURE_GATES,
    PAID_SUBSCRIPTION_STATUSES,
    plan_config_payload,
    current_entitlements,
    entitlement_payload,
    feature_gate_payloads,
    normalize_tier,
    seed_plan_prices,
    set_plan_limit,
    set_plan_price,
    set_feature_gate,
)
from app.models import AppSetting, BillingTransaction, PlanPrice, StripeWebhookEvent, UserAccount

router = APIRouter(tags=["accounts"])


class LoginPayload(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str | None = Field(default=None, min_length=8, max_length=240)
    name: str | None = Field(default=None, max_length=160)
    admin_token: str | None = Field(default=None, max_length=240)


class RegisterPayload(BaseModel):
    name: str | None = Field(default=None, max_length=160)
    first_name: str = Field(min_length=1, max_length=80)
    last_name: str = Field(min_length=1, max_length=80)
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=240)
    country: str = Field(min_length=1, max_length=2)
    state_province: str = Field(default="", max_length=100)
    postal_code: str = Field(min_length=1, max_length=32)
    city: str = Field(min_length=1, max_length=120)
    address_line1: str = Field(min_length=1, max_length=240)
    address_line2: str = Field(default="", max_length=240)


class PasswordResetRequestPayload(BaseModel):
    email: str = Field(min_length=3, max_length=320)


class PasswordResetConfirmPayload(BaseModel):
    token: str = Field(min_length=16, max_length=240)
    password: str = Field(min_length=8, max_length=240)


class GoogleCallbackPayload(BaseModel):
    code: str = Field(min_length=1)
    state: str = Field(min_length=1)
    redirect_uri: str | None = None


class ProfileUpdatePayload(BaseModel):
    first_name: str | None = Field(default=None, max_length=80)
    last_name: str | None = Field(default=None, max_length=80)
    country: str | None = Field(default=None, max_length=2)
    state_province: str | None = Field(default=None, max_length=100)
    postal_code: str | None = Field(default=None, max_length=32)
    city: str | None = Field(default=None, max_length=120)
    address_line1: str | None = Field(default=None, max_length=240)
    address_line2: str | None = Field(default=None, max_length=240)


class PasswordChangePayload(BaseModel):
    current_password: str = Field(min_length=1, max_length=240)
    new_password: str = Field(min_length=8, max_length=240)
    confirm_password: str = Field(min_length=8, max_length=240)


class NotificationSettingsPayload(BaseModel):
    alerts_enabled: bool
    email_notifications_enabled: bool
    watchlist_activity_notifications: bool
    signals_notifications: bool


class ManualPremiumPayload(BaseModel):
    tier: Literal["free", "premium"] | None = None


class SuspendPayload(BaseModel):
    suspended: bool


class FeatureGatePayload(BaseModel):
    required_tier: Literal["free", "premium"]


class PlanLimitPayload(BaseModel):
    tier: Literal["free", "premium"]
    limit_value: int = Field(ge=0, le=100000)


class PlanPricePayload(BaseModel):
    amount_cents: int = Field(ge=0, le=10000000)
    currency: str = Field(default="USD", min_length=3, max_length=8)


class OAuthSettingsPayload(BaseModel):
    google_client_id: str = Field(default="", max_length=512)


class StripeTaxSettingsPayload(BaseModel):
    automatic_tax_enabled: bool = False
    require_billing_address: bool = True
    product_tax_code: str | None = Field(default=None, max_length=80)
    price_tax_behavior: Literal["unspecified", "exclusive", "inclusive"] = "unspecified"


class CheckoutSessionPayload(BaseModel):
    billing_interval: Literal["monthly", "annual"] = "monthly"


SalesLedgerPeriod = Literal[
    "last_7_days",
    "last_30_days",
    "month_to_date",
    "year_to_date",
    "all_dates",
    "current_month",
    "current_quarter",
    "current_year",
    "last_month",
    "last_quarter",
    "last_year",
    "custom",
]
SalesLedgerSortBy = Literal["date_charged", "customer_name", "gross_amount", "country"]
SalesLedgerSortDir = Literal["asc", "desc"]
AdminUserPlanFilter = Literal["all", "free", "premium"]
AdminUserAdminFilter = Literal["all", "admin", "non_admin"]
AdminUserSortBy = Literal["created_at", "last_seen_at", "email", "name", "country", "plan", "status"]
AdminUserSortDir = Literal["asc", "desc"]
SubscriptionInterval = Literal["monthly", "annual"]


def _admin_token_matches(value: str | None) -> bool:
    configured = os.getenv("ADMIN_TOKEN", "").strip()
    return bool(configured and value and hmac.compare_digest(configured, value))


def _split_name(value: str | None) -> tuple[str | None, str | None]:
    cleaned = (value or "").strip()
    if not cleaned:
        return None, None
    first, _, last = cleaned.partition(" ")
    return first.strip() or None, last.strip() or None


def _display_name(first_name: str | None, last_name: str | None) -> str | None:
    full = " ".join(part for part in [(first_name or "").strip(), (last_name or "").strip()] if part)
    return full or None


BILLING_REQUIRED_FIELDS: tuple[tuple[str, str], ...] = (
    ("first_name", "First name"),
    ("last_name", "Last name"),
    ("country", "Country"),
    ("postal_code", "Postal code"),
    ("city", "City"),
    ("address_line1", "Address line 1"),
)
COUNTRIES_REQUIRING_BILLING_REGION = {"AU", "CA", "US"}

BILLING_LOCATION_FIELDS: tuple[str, ...] = (
    "country",
    "state_province",
    "postal_code",
    "city",
    "address_line1",
    "address_line2",
)


def _clean_profile_value(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None


def _clean_country(value: str | None) -> str | None:
    cleaned = _clean_profile_value(value)
    return cleaned.upper() if cleaned else None


def _validate_country_code(country: str | None) -> None:
    if country and len(country) != 2:
        raise HTTPException(status_code=422, detail="Country must use a two-letter ISO country code.")


def _billing_profile_missing_fields(user: UserAccount) -> list[str]:
    missing: list[str] = []
    for field, _label in BILLING_REQUIRED_FIELDS:
        if not _clean_profile_value(str(getattr(user, field) or "")):
            missing.append(field)
    if (user.country or "").strip().upper() in COUNTRIES_REQUIRING_BILLING_REGION and not _clean_profile_value(user.state_province):
        missing.append("state_province")
    return missing


def _billing_missing_fields_for_location(location: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in ("country", "postal_code", "city", "address_line1"):
        if not str(location.get(field) or "").strip():
            missing.append(field)
    country = str(location.get("country") or "").strip().upper()
    if country in COUNTRIES_REQUIRING_BILLING_REGION and not str(location.get("state_province") or "").strip():
        missing.append("state_province")
    return missing


def _billing_location_payload(user: UserAccount) -> dict[str, Any]:
    return {
        "first_name": user.first_name,
        "last_name": user.last_name,
        "country": user.country,
        "state_province": user.state_province,
        "postal_code": user.postal_code,
        "city": user.city,
        "address_line1": user.address_line1,
        "address_line2": user.address_line2,
    }


def _set_billing_profile(
    user: UserAccount,
    *,
    first_name: str | None,
    last_name: str | None,
    country: str | None,
    state_province: str | None,
    postal_code: str | None,
    city: str | None,
    address_line1: str | None,
    address_line2: str | None,
) -> None:
    cleaned_country = _clean_country(country)
    _validate_country_code(cleaned_country)
    user.first_name = _clean_profile_value(first_name)
    user.last_name = _clean_profile_value(last_name)
    user.name = _display_name(user.first_name, user.last_name)
    user.country = cleaned_country
    user.state_province = _clean_profile_value(state_province)
    user.postal_code = _clean_profile_value(postal_code)
    user.city = _clean_profile_value(city)
    user.address_line1 = _clean_profile_value(address_line1)
    user.address_line2 = _clean_profile_value(address_line2)


def _payload_fields_set(payload: BaseModel) -> set[str]:
    fields = getattr(payload, "model_fields_set", None)
    if fields is None:
        fields = getattr(payload, "__fields_set__", set())
    return set(fields)


def _password_meets_account_rules(value: str) -> bool:
    return (
        len(value) >= 8
        and any(char.isalpha() for char in value)
        and any(char.isdigit() for char in value)
        and any(not char.isalnum() for char in value)
    )


def _notification_settings(user: UserAccount) -> dict[str, bool]:
    return {
        "alerts_enabled": bool(user.alerts_enabled),
        "email_notifications_enabled": bool(user.email_notifications_enabled),
        "watchlist_activity_notifications": bool(user.watchlist_activity_notifications),
        "signals_notifications": bool(user.signals_notifications),
    }


def _setting_value(db: Session, key: str) -> str | None:
    row = db.get(AppSetting, key)
    value = row.value if row else None
    return value.strip() if value and value.strip() else None


def _set_setting(db: Session, key: str, value: str | None) -> AppSetting:
    row = db.get(AppSetting, key)
    if not row:
        row = AppSetting(key=key)
        db.add(row)
    row.value = value.strip() if value and value.strip() else None
    row.updated_at = datetime.now(timezone.utc)
    db.flush()
    return row


def _public_user(user: UserAccount) -> dict[str, Any]:
    billing_missing = _billing_profile_missing_fields(user)
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "country": user.country,
        "state_province": user.state_province,
        "postal_code": user.postal_code,
        "city": user.city,
        "address_line1": user.address_line1,
        "address_line2": user.address_line2,
        "billing_location": _billing_location_payload(user),
        "billing_profile_complete": not billing_missing,
        "billing_profile_missing_fields": billing_missing,
        "auth_provider": user.auth_provider,
        "avatar_url": user.avatar_url,
        "role": user.role,
        "is_admin": is_admin_user(user),
        "entitlement_tier": user.entitlement_tier,
        "manual_tier_override": user.manual_tier_override,
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "access_expires_at": user.access_expires_at,
        "stripe_customer_id": user.stripe_customer_id,
        "stripe_subscription_id": user.stripe_subscription_id,
        "is_suspended": user.is_suspended,
        "created_at": user.created_at,
        "last_seen_at": user.last_seen_at,
        "notifications": _notification_settings(user),
    }


SALES_LEDGER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("transaction id", "transaction_id"),
    ("customer name", "customer_name"),
    ("date charged", "date_charged"),
    ("description", "description"),
    ("country", "country"),
    ("state/province", "state_province"),
    ("net revenue amount", "net_revenue_display"),
    ("VAT1 label", "vat1_label"),
    ("VAT1 collected", "vat1_collected_display"),
    ("VAT2 label", "vat2_label"),
    ("VAT2 collected", "vat2_collected_display"),
    ("gross amount", "gross_amount_display"),
    ("status / refund state if available", "status_refund_state"),
)

ADMIN_USER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("user name", "name"),
    ("email", "email"),
    ("country", "country"),
    ("state/province", "state_province"),
    ("plan", "plan"),
    ("status", "status"),
    ("registered date", "created_at"),
    ("last active", "last_seen_at"),
    ("admin flag", "admin_flag"),
    ("access/subscription expiration", "access_expires_at"),
)


def _parse_date(value: str | None, field_name: str) -> date | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{field_name} must be YYYY-MM-DD.") from exc


def _quarter_start(value: date) -> date:
    month = ((value.month - 1) // 3) * 3 + 1
    return date(value.year, month, 1)


def _add_months(value: date, months: int) -> date:
    zero_based = value.month - 1 + months
    year = value.year + zero_based // 12
    month = zero_based % 12 + 1
    return date(year, month, 1)


def _sales_ledger_period_bounds(
    period: SalesLedgerPeriod,
    start_date: str | None,
    end_date: str | None,
    *,
    today: date | None = None,
) -> tuple[datetime | None, datetime | None, str | None, str | None]:
    current = today or datetime.now(timezone.utc).date()
    start: date | None = None
    end_exclusive: date | None = None

    if period == "last_7_days":
        start = current - timedelta(days=6)
        end_exclusive = current + timedelta(days=1)
    elif period == "last_30_days":
        start = current - timedelta(days=29)
        end_exclusive = current + timedelta(days=1)
    elif period in {"month_to_date", "current_month"}:
        start = date(current.year, current.month, 1)
        end_exclusive = current + timedelta(days=1)
    elif period in {"year_to_date", "current_year"}:
        start = date(current.year, 1, 1)
        end_exclusive = current + timedelta(days=1)
    elif period == "all_dates":
        start = None
        end_exclusive = None
    elif period == "current_quarter":
        start = _quarter_start(current)
        end_exclusive = _add_months(start, 3)
    elif period == "last_month":
        start = _add_months(date(current.year, current.month, 1), -1)
        end_exclusive = _add_months(start, 1)
    elif period == "last_quarter":
        start = _add_months(_quarter_start(current), -3)
        end_exclusive = _add_months(start, 3)
    elif period == "last_year":
        start = date(current.year - 1, 1, 1)
        end_exclusive = date(current.year, 1, 1)
    elif period == "custom":
        start = _parse_date(start_date, "start_date")
        end_inclusive = _parse_date(end_date, "end_date")
        if start and end_inclusive and start > end_inclusive:
            raise HTTPException(status_code=422, detail="start_date must be on or before end_date.")
        end_exclusive = end_inclusive + timedelta(days=1) if end_inclusive else None

    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc) if start else None
    end_dt = datetime.combine(end_exclusive, datetime.min.time(), tzinfo=timezone.utc) if end_exclusive else None
    display_end = (end_exclusive - timedelta(days=1)).isoformat() if end_exclusive else None
    return start_dt, end_dt, start.isoformat() if start else None, display_end


def _amount_cents(value: int | None) -> int:
    return int(value or 0)


def _billing_net_amount(row: BillingTransaction) -> int:
    if row.subtotal_amount is not None:
        return _amount_cents(row.subtotal_amount)
    if row.total_amount is not None:
        return _amount_cents(row.total_amount) - _amount_cents(row.tax_amount)
    return 0


def _billing_gross_amount(row: BillingTransaction) -> int:
    if row.total_amount is not None:
        return _amount_cents(row.total_amount)
    return _amount_cents(row.subtotal_amount) + _amount_cents(row.tax_amount)


def _money_display(cents: int | None, currency: str | None) -> str:
    code = (currency or "USD").upper()
    return f"{code} {_amount_cents(cents) / 100:.2f}"


def _tax_component_label(item: dict[str, Any], fallback: str) -> str:
    for key in ("display_name", "label", "jurisdiction", "taxability_reason"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    for container_key in ("tax_rate", "tax", "rate"):
        nested = item.get(container_key)
        if isinstance(nested, dict):
            for key in ("display_name", "label", "jurisdiction", "country", "state", "id"):
                value = str(nested.get(key) or "").strip()
                if value:
                    return value
        elif nested:
            value = str(nested).strip()
            if value:
                return value
    return fallback


def _tax_component_amount(item: dict[str, Any]) -> int:
    for key in ("amount", "tax_amount", "tax"):
        try:
            return int(item.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0


def _billing_tax_components(row: BillingTransaction) -> list[dict[str, Any]]:
    raw_components: list[dict[str, Any]] = []
    if row.tax_breakdown_json:
        try:
            parsed = json.loads(row.tax_breakdown_json)
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            for key in ("total_tax_amounts", "total_taxes", "line_taxes"):
                values = parsed.get(key)
                if isinstance(values, list) and values:
                    raw_components = [item for item in values if isinstance(item, dict)]
                    break

    components: list[dict[str, Any]] = []
    for index, item in enumerate(raw_components):
        amount = _tax_component_amount(item)
        if amount == 0:
            continue
        components.append({"label": _tax_component_label(item, f"VAT {index + 1}"), "amount": amount})

    if not components and row.tax_amount:
        components.append({"label": "Tax", "amount": _amount_cents(row.tax_amount)})

    if len(components) <= 2:
        return components

    remainder = sum(_amount_cents(item.get("amount")) for item in components[1:])
    return [components[0], {"label": "Multiple taxes", "amount": remainder}]


def _status_refund_state(row: BillingTransaction) -> str:
    payment = (row.payment_status or "").strip() or "unknown"
    refund = (row.refund_status or "").strip()
    if refund and refund.lower() != "none":
        return f"{payment} / {refund.replace('_', ' ')}"
    return payment


def _billing_payload(row: BillingTransaction) -> dict[str, Any]:
    if not row.payload_json:
        return {}
    try:
        parsed = json.loads(row.payload_json)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _stripe_artifact_url(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    parsed = urlparse(cleaned)
    host = (parsed.hostname or "").lower()
    stripe_host = host in {"stripe.com", "stripe.network"} or host.endswith(".stripe.com") or host.endswith(".stripe.network")
    if parsed.scheme != "https" or not stripe_host:
        return None
    return cleaned


def _charge_receipt_url(invoice: dict[str, Any]) -> str | None:
    charge = invoice.get("charge")
    if isinstance(charge, dict):
        receipt_url = _stripe_artifact_url(charge.get("receipt_url"))
        if receipt_url:
            return receipt_url

    payment_intent = invoice.get("payment_intent")
    if isinstance(payment_intent, dict):
        latest_charge = payment_intent.get("latest_charge")
        if isinstance(latest_charge, dict):
            receipt_url = _stripe_artifact_url(latest_charge.get("receipt_url"))
            if receipt_url:
                return receipt_url
        charges = payment_intent.get("charges") if isinstance(payment_intent.get("charges"), dict) else {}
        for item in charges.get("data") or []:
            if isinstance(item, dict):
                receipt_url = _stripe_artifact_url(item.get("receipt_url"))
                if receipt_url:
                    return receipt_url

    return _stripe_artifact_url(invoice.get("receipt_url"))


def _stripe_billing_documents(row: BillingTransaction) -> dict[str, Any]:
    invoice = _billing_payload(row)
    hosted_invoice_url = _stripe_artifact_url(invoice.get("hosted_invoice_url"))
    invoice_pdf_url = _stripe_artifact_url(invoice.get("invoice_pdf"))
    receipt_url = _charge_receipt_url(invoice)
    has_document = bool(hosted_invoice_url or invoice_pdf_url or receipt_url)
    return {
        "invoice_number": str(invoice.get("number") or row.stripe_invoice_id or "").strip() or None,
        "hosted_invoice_url": hosted_invoice_url,
        "invoice_pdf_url": invoice_pdf_url,
        "receipt_url": receipt_url,
        "has_stripe_document": has_document,
        "fallback_message": None if has_document else "Stripe has not provided a hosted invoice or receipt for this transaction yet.",
    }


def _customer_billing_history_row(row: BillingTransaction) -> dict[str, Any]:
    gross_amount = _billing_gross_amount(row)
    currency = (row.currency or "USD").upper()
    return {
        "id": row.id,
        "transaction_id": row.stripe_invoice_id or row.stripe_payment_intent_id or row.stripe_charge_id or str(row.id),
        "stripe_invoice_id": row.stripe_invoice_id,
        "stripe_payment_intent_id": row.stripe_payment_intent_id,
        "stripe_charge_id": row.stripe_charge_id,
        "date_charged": row.charged_at.isoformat() if row.charged_at else None,
        "description": row.description or row.billing_period_type or "Billing transaction",
        "billing_period_type": row.billing_period_type,
        "service_period_start": row.service_period_start.isoformat() if row.service_period_start else None,
        "service_period_end": row.service_period_end.isoformat() if row.service_period_end else None,
        "subtotal_amount": row.subtotal_amount,
        "tax_amount": row.tax_amount,
        "total_amount": gross_amount,
        "total_display": _money_display(gross_amount, currency),
        "currency": currency,
        "status": row.payment_status or "unknown",
        "refund_state": row.refund_status or "none",
        "status_refund_state": _status_refund_state(row),
        "documents": _stripe_billing_documents(row),
    }


def _sales_ledger_row(row: BillingTransaction) -> dict[str, Any]:
    taxes = _billing_tax_components(row)
    vat1 = taxes[0] if len(taxes) > 0 else {"label": "", "amount": 0}
    vat2 = taxes[1] if len(taxes) > 1 else {"label": "", "amount": 0}
    net_amount = _billing_net_amount(row)
    gross_amount = _billing_gross_amount(row)
    currency = (row.currency or "USD").upper()
    return {
        "id": row.id,
        "transaction_id": row.stripe_invoice_id or row.stripe_payment_intent_id or row.stripe_charge_id or str(row.id),
        "customer_name": row.customer_name or row.customer_email or "Unknown customer",
        "date_charged": row.charged_at.isoformat() if row.charged_at else None,
        "description": row.description or row.billing_period_type or "",
        "country": (row.billing_country or "").upper(),
        "state_province": row.billing_state_province or "",
        "net_revenue_amount": net_amount,
        "net_revenue_display": _money_display(net_amount, currency),
        "vat1_label": vat1["label"],
        "vat1_collected": _amount_cents(vat1["amount"]),
        "vat1_collected_display": _money_display(vat1["amount"], currency) if vat1["amount"] else "",
        "vat2_label": vat2["label"],
        "vat2_collected": _amount_cents(vat2["amount"]),
        "vat2_collected_display": _money_display(vat2["amount"], currency) if vat2["amount"] else "",
        "gross_amount": gross_amount,
        "gross_amount_display": _money_display(gross_amount, currency),
        "currency": currency,
        "status": row.payment_status or "unknown",
        "refund_state": row.refund_status or "none",
        "status_refund_state": _status_refund_state(row),
    }


def _sales_ledger_filtered_query(
    *,
    period: SalesLedgerPeriod,
    start_date: str | None,
    end_date: str | None,
    country: str | None,
) -> tuple[Any, dict[str, Any]]:
    start_dt, end_dt, effective_start, effective_end = _sales_ledger_period_bounds(period, start_date, end_date)
    conditions = []
    if start_dt:
        conditions.append(BillingTransaction.charged_at >= start_dt)
    if end_dt:
        conditions.append(BillingTransaction.charged_at < end_dt)
    country_code = (country or "").strip().upper()
    if country_code:
        if len(country_code) != 2:
            raise HTTPException(status_code=422, detail="country must use a two-letter ISO country code.")
        conditions.append(func.upper(BillingTransaction.billing_country) == country_code)
    query = select(BillingTransaction)
    if conditions:
        query = query.where(*conditions)
    return query, {
        "period": period,
        "start_date": effective_start,
        "end_date": effective_end,
        "country": country_code or None,
    }


def _sales_ledger_rows(
    db: Session,
    *,
    period: SalesLedgerPeriod,
    start_date: str | None,
    end_date: str | None,
    country: str | None,
    sort_by: SalesLedgerSortBy,
    sort_dir: SalesLedgerSortDir,
    page: int | None = None,
    page_size: int | None = None,
) -> tuple[list[BillingTransaction], int, dict[str, Any]]:
    query, filters = _sales_ledger_filtered_query(
        period=period,
        start_date=start_date,
        end_date=end_date,
        country=country,
    )
    count_query = select(func.count()).select_from(query.subquery())
    total = int(db.execute(count_query).scalar_one() or 0)
    sort_columns = {
        "date_charged": BillingTransaction.charged_at,
        "customer_name": BillingTransaction.customer_name,
        "gross_amount": BillingTransaction.total_amount,
        "country": BillingTransaction.billing_country,
    }
    sort_column = sort_columns[sort_by]
    ordered = query.order_by(sort_column.asc() if sort_dir == "asc" else sort_column.desc(), BillingTransaction.id.desc())
    if page is not None and page_size is not None:
        ordered = ordered.offset((page - 1) * page_size).limit(page_size)
    rows = db.execute(ordered).scalars().all()
    return rows, total, filters


def _effective_user_plan(user: UserAccount) -> str:
    return (user.manual_tier_override or user.entitlement_tier or user.subscription_plan or "free").strip().lower() or "free"


def _admin_user_status(user: UserAccount) -> str:
    if user.is_suspended:
        return "suspended"
    return (user.subscription_status or "active").strip().lower() or "active"


def _iso_or_blank(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _admin_user_row(user: UserAccount) -> dict[str, Any]:
    payload = _public_user(user)
    plan = _effective_user_plan(user)
    status = _admin_user_status(user)
    payload.update(
        {
            "plan": plan,
            "status": status,
            "admin_flag": "yes" if payload["is_admin"] else "no",
        }
    )
    return payload


def _admin_user_filtered_query(
    *,
    plan: AdminUserPlanFilter,
    status: str | None,
    country: str | None,
    admin: AdminUserAdminFilter,
) -> tuple[Any, dict[str, Any]]:
    conditions = []
    normalized_plan = (plan or "all").strip().lower()
    if normalized_plan != "all":
        conditions.append(func.lower(func.coalesce(UserAccount.manual_tier_override, UserAccount.entitlement_tier, "free")) == normalized_plan)

    normalized_status = (status or "").strip().lower()
    if normalized_status:
        if normalized_status == "suspended":
            conditions.append(UserAccount.is_suspended.is_(True))
        elif normalized_status == "active":
            conditions.append(UserAccount.is_suspended.is_(False))
            conditions.append(or_(UserAccount.subscription_status.is_(None), func.lower(UserAccount.subscription_status) == "active"))
        else:
            conditions.append(UserAccount.is_suspended.is_(False))
            conditions.append(func.lower(UserAccount.subscription_status) == normalized_status)

    country_code = (country or "").strip().upper()
    if country_code:
        if len(country_code) != 2:
            raise HTTPException(status_code=422, detail="country must use a two-letter ISO country code.")
        conditions.append(func.upper(UserAccount.country) == country_code)

    admin_emails_normalized = sorted(admin_emails())
    admin_condition = UserAccount.role == "admin"
    if admin_emails_normalized:
        admin_condition = or_(admin_condition, func.lower(UserAccount.email).in_(admin_emails_normalized))
    if admin == "admin":
        conditions.append(admin_condition)
    elif admin == "non_admin":
        conditions.append(~admin_condition)

    query = select(UserAccount)
    if conditions:
        query = query.where(*conditions)
    return query, {
        "plan": normalized_plan,
        "status": normalized_status or None,
        "country": country_code or None,
        "admin": admin,
    }


def _admin_user_rows(
    db: Session,
    *,
    plan: AdminUserPlanFilter,
    status: str | None,
    country: str | None,
    admin: AdminUserAdminFilter,
    sort_by: AdminUserSortBy,
    sort_dir: AdminUserSortDir,
    page: int | None = None,
    page_size: int | None = None,
) -> tuple[list[UserAccount], int, dict[str, Any]]:
    query, filters = _admin_user_filtered_query(plan=plan, status=status, country=country, admin=admin)
    total = int(db.execute(select(func.count()).select_from(query.subquery())).scalar_one() or 0)
    sort_columns = {
        "created_at": UserAccount.created_at,
        "last_seen_at": UserAccount.last_seen_at,
        "email": UserAccount.email,
        "name": UserAccount.name,
        "country": UserAccount.country,
        "plan": func.coalesce(UserAccount.manual_tier_override, UserAccount.entitlement_tier, "free"),
        "status": func.coalesce(UserAccount.subscription_status, "active"),
    }
    sort_column = sort_columns[sort_by]
    ordered = query.order_by(sort_column.asc() if sort_dir == "asc" else sort_column.desc(), UserAccount.id.desc())
    if page is not None and page_size is not None:
        ordered = ordered.offset((page - 1) * page_size).limit(page_size)
    rows = db.execute(ordered).scalars().all()
    return rows, total, filters


def _export_filename(prefix: str, extension: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{prefix}-{stamp}.{extension}"


def _xlsx_col_name(index: int) -> str:
    value = index + 1
    letters = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _xlsx_inline_cell(reference: str, value: Any) -> str:
    return f'<c r="{reference}" t="inlineStr"><is><t>{html_escape(str(value or ""))}</t></is></c>'


def _table_xlsx(rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...], *, sheet_name: str, title: str) -> bytes:
    sheet_rows: list[str] = []
    header_cells = [
        _xlsx_inline_cell(f"{_xlsx_col_name(index)}1", header)
        for index, (header, _key) in enumerate(columns)
    ]
    sheet_rows.append(f'<row r="1">{"".join(header_cells)}</row>')
    for row_index, row in enumerate(rows, start=2):
        cells = [
            _xlsx_inline_cell(f"{_xlsx_col_name(col_index)}{row_index}", row[key])
            for col_index, (_header, key) in enumerate(columns)
        ]
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    worksheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        "</worksheet>"
    )
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
            '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
            "</Types>",
        )
        workbook.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
            "</Relationships>",
        )
        workbook.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets><sheet name="{html_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        workbook.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            "</Relationships>",
        )
        workbook.writestr("xl/worksheets/sheet1.xml", worksheet)
        workbook.writestr(
            "docProps/core.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
            f'xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>{html_escape(title)}</dc:title></cp:coreProperties>',
        )
        workbook.writestr(
            "docProps/app.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">'
            "<Application>Congress Tracker</Application></Properties>",
        )
    return output.getvalue()


def _sales_ledger_xlsx(rows: list[dict[str, Any]]) -> bytes:
    return _table_xlsx(rows, SALES_LEDGER_COLUMNS, sheet_name="Sales Ledger", title="Sales Ledger")


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_text_line(x: int, y: int, text: str, size: int = 8) -> str:
    return f"BT /F1 {size} Tf {x} {y} Td ({_pdf_escape(text)}) Tj ET\n"


def _sales_ledger_pdf(rows: list[dict[str, Any]], filters: dict[str, Any]) -> bytes:
    pages: list[str] = []
    title = "Sales Ledger"
    filter_line = "Filters: " + ", ".join(f"{key}={value}" for key, value in filters.items() if value) if filters else "Filters: none"
    current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, filter_line[:120], 8)]
    y = 530
    for row in rows:
        if y < 70:
            pages.append("".join(current_lines))
            current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, "continued", 8)]
            y = 530
        line_one = (
            f"{row['transaction_id']} | {row['date_charged'] or ''} | {row['customer_name']} | "
            f"{row['gross_amount_display']} | {row['status_refund_state']}"
        )
        line_two = (
            f"{row['description']} | {row['country']} {row['state_province']} | net {row['net_revenue_display']} | "
            f"{row['vat1_label']} {row['vat1_collected_display']} | {row['vat2_label']} {row['vat2_collected_display']}"
        )
        current_lines.append(_pdf_text_line(36, y, line_one[:145], 8))
        current_lines.append(_pdf_text_line(36, y - 11, line_two[:145], 8))
        y -= 30
    pages.append("".join(current_lines))

    objects: list[bytes] = []
    page_object_ids: list[int] = []
    content_object_ids: list[int] = []
    for content in pages:
        content_object_ids.append(4 + len(objects))
        objects.append(f"<< /Length {len(content.encode('latin-1', 'replace'))} >>\nstream\n{content}endstream".encode("latin-1", "replace"))
        page_object_ids.append(4 + len(objects))
        objects.append(b"")

    kids = " ".join(f"{object_id} 0 R" for object_id in page_object_ids)
    base_objects = [
        f"<< /Type /Catalog /Pages 2 0 R >>".encode(),
        f"<< /Type /Pages /Kids [{kids}] /Count {len(page_object_ids)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    full_objects = base_objects + objects
    for index, page_object_id in enumerate(page_object_ids):
        content_id = content_object_ids[index]
        full_objects[page_object_id - 1] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 792 612] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
        ).encode()

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for object_id, body in enumerate(full_objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{object_id} 0 obj\n".encode())
        pdf.extend(body)
        pdf.extend(b"\nendobj\n")
    xref_at = len(pdf)
    pdf.extend(f"xref\n0 {len(full_objects) + 1}\n".encode())
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode())
    pdf.extend(
        f"trailer\n<< /Size {len(full_objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()
    )
    return bytes(pdf)


def _admin_users_export_rows(users: list[UserAccount]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for user in users:
        row = _admin_user_row(user)
        rows.append(
            {
                **row,
                "name": row.get("name") or "",
                "country": row.get("country") or "",
                "state_province": row.get("state_province") or "",
                "created_at": _iso_or_blank(row.get("created_at")),
                "last_seen_at": _iso_or_blank(row.get("last_seen_at")),
                "access_expires_at": _iso_or_blank(row.get("access_expires_at")),
            }
        )
    return rows


def _admin_users_xlsx(rows: list[dict[str, Any]]) -> bytes:
    return _table_xlsx(rows, ADMIN_USER_COLUMNS, sheet_name="Users", title="Admin Users")


def _admin_users_pdf(rows: list[dict[str, Any]], filters: dict[str, Any]) -> bytes:
    pages: list[str] = []
    title = "Admin Users"
    filter_line = "Filters: " + ", ".join(f"{key}={value}" for key, value in filters.items() if value and value != "all")
    if filter_line == "Filters: ":
        filter_line = "Filters: none"
    current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, filter_line[:120], 8)]
    y = 530
    for row in rows:
        if y < 70:
            pages.append("".join(current_lines))
            current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, "continued", 8)]
            y = 530
        line_one = (
            f"{row['name'] or '-'} | {row['email']} | {row['country'] or '-'} {row['state_province'] or '-'} | "
            f"{row['plan']} | {row['status']} | admin {row['admin_flag']}"
        )
        line_two = (
            f"registered {row['created_at'] or '-'} | last active {row['last_seen_at'] or '-'} | "
            f"expires {row['access_expires_at'] or '-'}"
        )
        current_lines.append(_pdf_text_line(36, y, line_one[:145], 8))
        current_lines.append(_pdf_text_line(36, y - 11, line_two[:145], 8))
        y -= 30
    pages.append("".join(current_lines))

    objects: list[bytes] = []
    page_object_ids: list[int] = []
    content_object_ids: list[int] = []
    for content in pages:
        content_object_ids.append(4 + len(objects))
        objects.append(f"<< /Length {len(content.encode('latin-1', 'replace'))} >>\nstream\n{content}endstream".encode("latin-1", "replace"))
        page_object_ids.append(4 + len(objects))
        objects.append(b"")

    kids = " ".join(f"{object_id} 0 R" for object_id in page_object_ids)
    base_objects = [
        f"<< /Type /Catalog /Pages 2 0 R >>".encode(),
        f"<< /Type /Pages /Kids [{kids}] /Count {len(page_object_ids)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    full_objects = base_objects + objects
    for index, page_object_id in enumerate(page_object_ids):
        content_id = content_object_ids[index]
        full_objects[page_object_id - 1] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 792 612] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
        ).encode()

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for object_id, body in enumerate(full_objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{object_id} 0 obj\n".encode())
        pdf.extend(body)
        pdf.extend(b"\nendobj\n")
    xref_at = len(pdf)
    pdf.extend(f"xref\n0 {len(full_objects) + 1}\n".encode())
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode())
    pdf.extend(
        f"trailer\n<< /Size {len(full_objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()
    )
    return bytes(pdf)


def _stripe_secret_key() -> str | None:
    return os.getenv("STRIPE_SECRET_KEY", "").strip() or None


def _stripe_price_id(billing_interval: str | None = None) -> str | None:
    interval = (billing_interval or "").strip().lower()
    if interval == "annual":
        return os.getenv("STRIPE_PRICE_ID_ANNUAL", "").strip() or os.getenv("STRIPE_PRICE_ID", "").strip() or None
    if interval == "monthly":
        return os.getenv("STRIPE_PRICE_ID_MONTHLY", "").strip() or os.getenv("STRIPE_PRICE_ID", "").strip() or None
    return os.getenv("STRIPE_PRICE_ID", "").strip() or None


def _stripe_webhook_secret() -> str | None:
    return os.getenv("STRIPE_WEBHOOK_SECRET", "").strip() or None


def _frontend_base_url() -> str:
    return os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").rstrip("/")


def _api_base_url() -> str:
    return os.getenv("PUBLIC_API_BASE_URL", os.getenv("API_BASE", "http://localhost:8000")).rstrip("/")


def _google_client_id(db: Session | None = None) -> str | None:
    if db is not None:
        saved = _setting_value(db, "google_client_id")
        if saved:
            return saved
    return os.getenv("GOOGLE_CLIENT_ID", "").strip() or None


def _google_client_secret() -> str | None:
    return os.getenv("GOOGLE_CLIENT_SECRET", "").strip() or None


def _google_redirect_uri() -> str:
    return os.getenv("GOOGLE_REDIRECT_URI", f"{_frontend_base_url()}/auth/google/callback").strip()


def _stripe_post(path: str, data: dict[str, Any]) -> dict[str, Any]:
    secret = _stripe_secret_key()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe secret key is not configured.")
    response = requests.post(
        f"https://api.stripe.com/v1/{path.lstrip('/')}",
        auth=(secret, ""),
        data=data,
        timeout=20,
    )
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Stripe request failed: {response.text[:500]}")
    parsed = response.json()
    return parsed if isinstance(parsed, dict) else {}


def _stripe_address_payload(user: UserAccount) -> dict[str, Any]:
    values = {
        "address[country]": user.country,
        "address[state]": user.state_province,
        "address[postal_code]": user.postal_code,
        "address[city]": user.city,
        "address[line1]": user.address_line1,
        "address[line2]": user.address_line2,
    }
    return {key: value for key, value in values.items() if value}


def _stripe_customer_sync_payload(user: UserAccount, *, validate_location: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "email": user.email,
        "name": user.name or _display_name(user.first_name, user.last_name) or user.email,
        "metadata[user_id]": user.id,
        "metadata[email]": user.email,
    }
    payload.update(_stripe_address_payload(user))
    if validate_location:
        payload["tax[validate_location]"] = "immediately"
    return payload


def _sync_stripe_customer_for_billing(db: Session, user: UserAccount) -> str:
    tax_settings = _stripe_tax_settings(db)
    readiness = stripe_tax_billing_readiness(db, _billing_location_payload(user))
    if tax_settings["automatic_tax_enabled"] and not readiness["can_start_checkout"]:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "billing_location_required",
                "message": "Complete billing location before starting taxable checkout.",
                "missing_fields": readiness["missing_fields"],
            },
        )

    payload = _stripe_customer_sync_payload(user, validate_location=bool(tax_settings["automatic_tax_enabled"]))
    if user.stripe_customer_id:
        customer = _stripe_post(f"customers/{user.stripe_customer_id}", payload)
    else:
        customer = _stripe_post("customers", payload)
        customer_id = str(customer.get("id") or "").strip()
        if not customer_id:
            raise HTTPException(status_code=502, detail="Stripe did not return a customer id.")
        user.stripe_customer_id = customer_id
        db.commit()
        db.refresh(user)
    return user.stripe_customer_id or str(customer.get("id") or "")


def _stripe_config_status() -> dict[str, Any]:
    secret = _stripe_secret_key()
    price = _stripe_price_id() or _stripe_price_id("monthly")
    monthly_price = _stripe_price_id("monthly")
    annual_price = _stripe_price_id("annual")
    webhook = _stripe_webhook_secret()
    return {
        "configured": bool(secret and price and webhook),
        "secret_key": "configured" if secret else "missing",
        "price_id": price or "missing",
        "monthly_price_id": monthly_price or "missing",
        "annual_price_id": annual_price or "missing",
        "webhook_secret": "configured" if webhook else "missing",
        "success_url": f"{_frontend_base_url()}/account/billing?checkout=success",
        "cancel_url": f"{_frontend_base_url()}/account/billing?checkout=cancelled",
        "webhook_url": f"{_api_base_url()}/api/billing/stripe/webhook",
        "notes": "Secrets are read from environment variables: STRIPE_SECRET_KEY, STRIPE_PRICE_ID, STRIPE_WEBHOOK_SECRET.",
    }


def _setting_bool(db: Session, key: str, default: bool = False) -> bool:
    value = _setting_value(db, key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _stripe_tax_settings(db: Session) -> dict[str, Any]:
    behavior = _setting_value(db, "stripe_tax_price_tax_behavior") or "unspecified"
    if behavior not in {"unspecified", "exclusive", "inclusive"}:
        behavior = "unspecified"
    return {
        "automatic_tax_enabled": _setting_bool(db, "stripe_tax_automatic_tax_enabled", False),
        "require_billing_address": _setting_bool(db, "stripe_tax_require_billing_address", True),
        "product_tax_code": _setting_value(db, "stripe_tax_product_tax_code"),
        "price_tax_behavior": behavior,
    }


def _stripe_business_support_info() -> dict[str, Any]:
    values = {
        "business_name": os.getenv("PUBLIC_BUSINESS_NAME", "").strip() or os.getenv("STRIPE_BUSINESS_NAME", "").strip(),
        "support_email": os.getenv("SUPPORT_EMAIL", "").strip() or os.getenv("STRIPE_SUPPORT_EMAIL", "").strip(),
        "support_url": os.getenv("SUPPORT_URL", "").strip() or os.getenv("STRIPE_SUPPORT_URL", "").strip(),
        "support_phone": os.getenv("SUPPORT_PHONE", "").strip() or os.getenv("STRIPE_SUPPORT_PHONE", "").strip(),
    }
    present = {key: bool(value) for key, value in values.items()}
    return {
        "configured": bool(values["business_name"] and (values["support_email"] or values["support_url"] or values["support_phone"])),
        "fields": present,
    }


def _readiness_check(key: str, label: str, ok: bool, detail: str, *, required: bool = True) -> dict[str, Any]:
    status = "ready" if ok else ("missing" if required else "optional")
    return {"key": key, "label": label, "status": status, "detail": detail, "required": required}


def stripe_tax_billing_readiness(db: Session, customer_location: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = _stripe_tax_settings(db)
    location = customer_location or {}
    missing_fields: list[str] = []
    if settings["automatic_tax_enabled"] and settings["require_billing_address"]:
        missing_fields = _billing_missing_fields_for_location(location)
    should_prompt = bool(settings["automatic_tax_enabled"] and missing_fields)
    return {
        "automatic_tax_enabled": settings["automatic_tax_enabled"],
        "requires_customer_location": bool(settings["automatic_tax_enabled"] and settings["require_billing_address"]),
        "has_required_customer_location": not missing_fields,
        "missing_fields": missing_fields,
        "should_prompt_for_location": should_prompt,
        "can_start_checkout": not should_prompt,
        "note": "Future billing flows can use this helper to prompt for location before enabling Stripe automatic tax.",
    }


def _stripe_tax_config(db: Session) -> dict[str, Any]:
    settings = _stripe_tax_settings(db)
    secret = _stripe_secret_key()
    price = _stripe_price_id() or _stripe_price_id("monthly")
    webhook = _stripe_webhook_secret()
    business_support = _stripe_business_support_info()
    checks = [
        _readiness_check(
            "stripe_secret_key",
            "Stripe secret key",
            bool(secret),
            "Present in environment." if secret else "Set STRIPE_SECRET_KEY in the deployment environment.",
        ),
        _readiness_check(
            "stripe_price_id",
            "Stripe price",
            bool(price),
            f"Using {price}." if price else "Set STRIPE_PRICE_ID for the subscription price.",
        ),
        _readiness_check(
            "automatic_tax",
            "Automatic tax app flag",
            bool(settings["automatic_tax_enabled"]),
            "Future billing flows will request Stripe automatic tax." if settings["automatic_tax_enabled"] else "Turn on when ready to request Stripe automatic tax in billing flows.",
        ),
        _readiness_check(
            "customer_location",
            "Customer location collection",
            bool(settings["require_billing_address"]),
            "Billing flows should collect address/location before checkout." if settings["require_billing_address"] else "Billing flows are not marked to require address/location collection.",
        ),
        _readiness_check(
            "business_support_info",
            "Business/support invoice info",
            bool(business_support["configured"]),
            "Public business/support fields are present." if business_support["configured"] else "Configure business/support info in Stripe or environment before invoices go live.",
            required=False,
        ),
        _readiness_check(
            "webhook_secret",
            "Stripe webhook secret",
            bool(webhook),
            "Present in environment." if webhook else "Set STRIPE_WEBHOOK_SECRET for subscription sync.",
        ),
    ]
    return {
        **settings,
        "configured": bool(secret and price and settings["automatic_tax_enabled"]),
        "stripe_tax_status": "ready_in_app" if secret and price and settings["automatic_tax_enabled"] else "not_ready",
        "stripe_dashboard_status": "managed_in_stripe",
        "price_id": price or "missing",
        "price_configured": bool(price),
        "secret_key": "configured" if secret else "missing",
        "webhook_secret": "configured" if webhook else "missing",
        "business_support": business_support,
        "readiness": stripe_tax_billing_readiness(db),
        "checks": checks,
        "notes": "Stripe Tax will calculate tax from customer location and your Stripe registrations/settings. App settings here only prepare integration readiness; they do not calculate manual tax rates.",
    }


def _normalize_subscription_interval(value: str | None) -> SubscriptionInterval | None:
    normalized = (value or "").strip().lower()
    if normalized in {"annual", "annually", "year", "yearly"}:
        return "annual"
    if normalized in {"monthly", "month"}:
        return "monthly"
    return None


def _premium_price_lookup(db: Session) -> tuple[dict[SubscriptionInterval, int], list[str]]:
    notes: list[str] = []
    seed_plan_prices(db)
    rows = db.execute(
        select(PlanPrice).where(PlanPrice.tier == "premium").order_by(PlanPrice.billing_interval.asc())
    ).scalars().all()
    prices: dict[SubscriptionInterval, int] = {}
    for row in rows:
        interval = _normalize_subscription_interval(row.billing_interval)
        if interval is None:
            continue
        prices[interval] = int(row.amount_cents or 0)
    if "monthly" not in prices or "annual" not in prices:
        prices.setdefault("monthly", 1995)
        prices.setdefault("annual", 19995)
        notes.append("Premium pricing settings were incomplete. Default premium pricing fallback was used.")
    return prices, notes


def _latest_billing_intervals_by_user(db: Session) -> dict[int, SubscriptionInterval]:
    rows = db.execute(
        select(
            BillingTransaction.user_id,
            BillingTransaction.billing_period_type,
            BillingTransaction.charged_at,
            BillingTransaction.created_at,
            BillingTransaction.id,
        )
        .where(BillingTransaction.user_id.is_not(None))
        .where(BillingTransaction.billing_period_type.is_not(None))
        .order_by(
            BillingTransaction.user_id.asc(),
            BillingTransaction.charged_at.desc().nullslast(),
            BillingTransaction.created_at.desc(),
            BillingTransaction.id.desc(),
        )
    ).all()
    intervals: dict[int, SubscriptionInterval] = {}
    for row in rows:
        user_id = int(row.user_id)
        if user_id in intervals:
            continue
        interval = _normalize_subscription_interval(row.billing_period_type)
        if interval is not None:
            intervals[user_id] = interval
    return intervals


def _has_recent_activity(user: UserAccount, cutoff: datetime, *, use_created_fallback: bool) -> bool:
    last_seen = _aware_utc(user.last_seen_at)
    if last_seen is not None:
        return last_seen >= cutoff
    created_at = _aware_utc(user.created_at)
    return bool(use_created_fallback and created_at is not None and created_at >= cutoff)


def _has_actual_paid_access(user: UserAccount, now: datetime) -> bool:
    status = (user.subscription_status or "").strip().lower()
    paid_through = _aware_utc(user.access_expires_at)
    return bool(status in PAID_SUBSCRIPTION_STATUSES or (paid_through is not None and paid_through > now))


def _has_fallback_premium_access(user: UserAccount) -> bool:
    fallback_values = [user.manual_tier_override, user.entitlement_tier, user.subscription_plan]
    return any(normalize_tier(value) == "premium" for value in fallback_values if value)


def _reports_summary(db: Session) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    current_date = now.date()
    ytd_start = datetime(current_date.year, 1, 1, tzinfo=timezone.utc)
    ytd_end = datetime.combine(current_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    notes: list[str] = []

    activity_data_available = (
        db.execute(select(func.count()).select_from(UserAccount).where(UserAccount.last_seen_at.is_not(None))).scalar_one() > 0
    )
    if not activity_data_available:
        notes.append("Active user metric uses created_at fallback because last_seen is unavailable.")

    prices_cents, price_notes = _premium_price_lookup(db)
    notes.extend(price_notes)
    latest_intervals_by_user = _latest_billing_intervals_by_user(db)
    users = db.execute(select(UserAccount).order_by(UserAccount.id.asc())).scalars().all()

    active_free_users = 0
    active_premium_users = 0
    monthly_recurring_revenue_cents = 0.0
    used_premium_fallback = False

    for user in users:
        actual_paid = _has_actual_paid_access(user, now)
        fallback_premium = not actual_paid and _has_fallback_premium_access(user)
        recent_activity = _has_recent_activity(user, cutoff, use_created_fallback=not activity_data_available)
        premium_for_counts = actual_paid or fallback_premium

        if is_admin_user(user) and not actual_paid:
            premium_for_counts = False

        if premium_for_counts:
            if actual_paid or recent_activity:
                active_premium_users += 1

            if actual_paid or (fallback_premium and recent_activity and not is_admin_user(user)):
                interval = latest_intervals_by_user.get(user.id) or _normalize_subscription_interval(user.subscription_plan) or "monthly"
                if user.id not in latest_intervals_by_user:
                    used_premium_fallback = True
                if interval == "annual":
                    monthly_recurring_revenue_cents += prices_cents["annual"] / 12
                else:
                    monthly_recurring_revenue_cents += prices_cents["monthly"]
            continue

        if recent_activity and not is_admin_user(user):
            active_free_users += 1

    if used_premium_fallback:
        notes.append("Premium user and MRR metrics use entitlement/subscription-plan fallback where full subscription state is unavailable.")

    billing_transactions_exist = db.execute(select(func.count()).select_from(BillingTransaction)).scalar_one() > 0
    revenue_ytd_cents = db.execute(
        select(func.coalesce(func.sum(BillingTransaction.total_amount), 0))
        .select_from(BillingTransaction)
        .where(BillingTransaction.charged_at.is_not(None))
        .where(BillingTransaction.charged_at >= ytd_start)
        .where(BillingTransaction.charged_at < ytd_end)
        .where(func.lower(func.coalesce(BillingTransaction.payment_status, "")).in_(["paid", "succeeded"]))
    ).scalar_one()
    if not billing_transactions_exist:
        notes.append("Revenue collection data not connected yet.")

    new_users_last_30_days = db.execute(
        select(func.count()).select_from(UserAccount).where(UserAccount.created_at >= cutoff)
    ).scalar_one()
    total_users = db.execute(select(func.count()).select_from(UserAccount)).scalar_one()

    payload: dict[str, Any] = {
        "active_free_users": int(active_free_users),
        "active_premium_users": int(active_premium_users),
        "monthly_recurring_revenue": round(float(monthly_recurring_revenue_cents) / 100, 2),
        "revenue_ytd": round(float(revenue_ytd_cents or 0) / 100, 2),
        "new_users_last_30_days": int(new_users_last_30_days),
        "total_users": int(total_users or 0),
        "currency": "USD",
        "generated_at": now.isoformat(),
    }
    if notes:
        payload["notes"] = notes
    return payload


def _auth_response_for_user(db: Session, user: UserAccount) -> dict[str, Any]:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return {
        "token": token,
        "user": _public_user(user),
        "entitlements": entitlement_payload(current_entitlements(_request_from_token(token), db), user=user),
    }


@router.post("/auth/login")
def login(payload: LoginPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    wants_admin = email in admin_emails()
    existing = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    existing_is_admin = is_admin_user(existing)
    admin_token_valid = _admin_token_matches(payload.admin_token)
    if (wants_admin or existing_is_admin) and not (admin_token_valid or verify_password(payload.password, existing.password_hash if existing else None)):
        raise HTTPException(status_code=401, detail="Admin token required for this account.")

    if existing and existing.password_hash and not (admin_token_valid or verify_password(payload.password, existing.password_hash)):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if existing and not existing.password_hash and not (admin_token_valid or wants_admin or existing_is_admin):
        raise HTTPException(status_code=401, detail="Set a password with the reset flow before signing in.")
    if not existing and not (admin_token_valid or wants_admin):
        raise HTTPException(status_code=401, detail="No account exists for this email. Register first.")

    user = get_or_create_user(db, email=email, name=payload.name)
    if payload.name and not (user.first_name or user.last_name):
        user.first_name, user.last_name = _split_name(payload.name)
    if wants_admin:
        user.role = "admin"
    user.last_seen_at = datetime.now(timezone.utc)
    attach_legacy_watchlists_to_user(db, user)
    db.commit()
    db.refresh(user)

    return _auth_response_for_user(db, user)


@router.post("/auth/register")
def register(payload: RegisterPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    existing = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    if existing and existing.password_hash:
        raise HTTPException(status_code=409, detail="An account already exists for this email.")

    cleaned_registration = {
        "first_name": _clean_profile_value(payload.first_name),
        "last_name": _clean_profile_value(payload.last_name),
        "country": _clean_country(payload.country),
        "state_province": _clean_profile_value(payload.state_province),
        "postal_code": _clean_profile_value(payload.postal_code),
        "city": _clean_profile_value(payload.city),
        "address_line1": _clean_profile_value(payload.address_line1),
        "address_line2": _clean_profile_value(payload.address_line2),
    }
    missing = [label for field, label in BILLING_REQUIRED_FIELDS if not cleaned_registration.get(field)]
    if cleaned_registration.get("country") in COUNTRIES_REQUIRING_BILLING_REGION and not cleaned_registration.get("state_province"):
        missing.append("State/province")
    if missing:
        raise HTTPException(status_code=422, detail=f"{', '.join(missing)} required.")

    user = existing or get_or_create_user(db, email=email, name=payload.name or _display_name(payload.first_name, payload.last_name))
    _set_billing_profile(user, **cleaned_registration)
    user.password_hash = hash_password(payload.password)
    user.auth_provider = user.auth_provider or "email"
    if email in admin_emails():
        user.role = "admin"
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.last_seen_at = datetime.now(timezone.utc)
    attach_legacy_watchlists_to_user(db, user)
    db.commit()
    db.refresh(user)
    return _auth_response_for_user(db, user)


@router.post("/auth/password-reset/request")
def request_password_reset(payload: PasswordResetRequestPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    response: dict[str, Any] = {
        "status": "ok",
        "message": "If an account exists, a reset link is ready.",
    }
    if not user:
        return response

    token = secrets.token_urlsafe(32)
    user.password_reset_token_hash = reset_token_hash(token)
    user.password_reset_expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
    db.commit()
    reset_path = f"/reset-password?token={token}"
    response["reset_path"] = reset_path
    return response


@router.post("/auth/password-reset/confirm")
def confirm_password_reset(payload: PasswordResetConfirmPayload, db: Session = Depends(get_db)):
    token_hash = reset_token_hash(payload.token)
    user = db.execute(
        select(UserAccount).where(UserAccount.password_reset_token_hash == token_hash)
    ).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")
    expires_at = user.password_reset_expires_at
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if not expires_at or expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")

    user.password_hash = hash_password(payload.password)
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.last_seen_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _auth_response_for_user(db, user)


def _request_from_token(token: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode("utf-8"))]})


@router.get("/auth/google/start")
def google_auth_start(return_to: str | None = None, db: Session = Depends(get_db)):
    client_id = _google_client_id(db)
    if not client_id:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")
    state = sign_session_payload(
        {
            "kind": "google_oauth_state",
            "return_to": return_to or "/account/billing",
            "exp": int(time.time()) + 600,
        }
    )
    params = {
        "client_id": client_id,
        "redirect_uri": _google_redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "select_account",
    }
    return {"authorization_url": f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}", "state": state}


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        raise HTTPException(status_code=401, detail="Invalid Google identity token.")
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        import base64

        parsed = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid Google identity token.") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=401, detail="Invalid Google identity token.")
    return parsed


def _verify_google_claims(db: Session, claims: dict[str, Any]) -> dict[str, Any]:
    client_id = _google_client_id(db)
    if not client_id:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")
    if claims.get("aud") != client_id:
        raise HTTPException(status_code=401, detail="Google token audience mismatch.")
    if claims.get("iss") not in {"accounts.google.com", "https://accounts.google.com"}:
        raise HTTPException(status_code=401, detail="Google token issuer mismatch.")
    try:
        exp = int(claims.get("exp") or 0)
    except (TypeError, ValueError):
        exp = 0
    if exp < int(time.time()):
        raise HTTPException(status_code=401, detail="Google token expired.")
    if claims.get("email_verified") not in {True, "true", "True", "1", 1}:
        raise HTTPException(status_code=401, detail="Google email is not verified.")
    email = normalize_email(str(claims.get("email") or ""))
    sub = str(claims.get("sub") or "").strip()
    if not email or not sub:
        raise HTTPException(status_code=401, detail="Google token missing account identity.")
    return claims


def upsert_google_user(db: Session, claims: dict[str, Any]) -> UserAccount:
    claims = _verify_google_claims(db, claims)
    email = normalize_email(str(claims.get("email")))
    sub = str(claims.get("sub"))
    name = str(claims.get("name") or "").strip() or None
    picture = str(claims.get("picture") or "").strip() or None

    user = db.execute(select(UserAccount).where(UserAccount.google_sub == sub)).scalar_one_or_none()
    if not user:
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    if not user:
        user = get_or_create_user(db, email=email, name=name)

    user.auth_provider = "google"
    user.google_sub = sub
    if name:
        user.name = name
        if not (user.first_name or user.last_name):
            user.first_name, user.last_name = _split_name(name)
    if picture:
        user.avatar_url = picture
    if email in admin_emails():
        user.role = "admin"
    user.last_seen_at = datetime.now(timezone.utc)
    db.flush()
    return user


@router.post("/auth/google/callback")
def google_auth_callback(payload: GoogleCallbackPayload, db: Session = Depends(get_db)):
    parsed_state = verify_session_token(payload.state)
    if (
        not parsed_state
        or parsed_state.get("kind") != "google_oauth_state"
        or int(parsed_state.get("exp") or 0) < int(time.time())
    ):
        raise HTTPException(status_code=401, detail="Invalid Google sign-in state.")
    client_id = _google_client_id(db)
    client_secret = _google_client_secret()
    if not client_id or not client_secret:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")

    response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": payload.code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": payload.redirect_uri or _google_redirect_uri(),
            "grant_type": "authorization_code",
        },
        timeout=20,
    )
    if response.status_code >= 400:
        raise HTTPException(status_code=401, detail=f"Google token exchange failed: {response.text[:300]}")
    token_payload = response.json()
    id_token = token_payload.get("id_token") if isinstance(token_payload, dict) else None
    if not isinstance(id_token, str):
        raise HTTPException(status_code=401, detail="Google did not return an identity token.")
    user = upsert_google_user(db, _decode_jwt_payload(id_token))
    db.commit()
    db.refresh(user)
    auth = _auth_response_for_user(db, user)
    auth["return_to"] = parsed_state.get("return_to") or "/account/billing"
    return auth


@router.get("/auth/me")
def me(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=False)
    return {
        "user": _public_user(user) if user else None,
        "entitlements": entitlement_payload(current_entitlements(request, db), user=user),
    }


@router.get("/account/settings")
def account_settings(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    return {
        "user": _public_user(user),
        "notifications": _notification_settings(user),
    }


@router.get("/account/billing/history")
def account_billing_history(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = Query(25, ge=1, le=100),
):
    user = current_user(db, request, required=True)
    conditions = [BillingTransaction.user_id == user.id]
    if user.stripe_customer_id:
        conditions.append(BillingTransaction.stripe_customer_id == user.stripe_customer_id)
    if user.stripe_subscription_id:
        conditions.append(BillingTransaction.stripe_subscription_id == user.stripe_subscription_id)
    email = normalize_email(user.email)
    if email:
        conditions.append(func.lower(BillingTransaction.customer_email) == email)

    rows = db.execute(
        select(BillingTransaction)
        .where(or_(*conditions))
        .order_by(BillingTransaction.charged_at.desc(), BillingTransaction.id.desc())
        .limit(limit)
    ).scalars().all()
    return {"items": [_customer_billing_history_row(row) for row in rows]}


@router.patch("/account/profile")
def update_account_profile(payload: ProfileUpdatePayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    provided_fields = _payload_fields_set(payload)
    next_values = _billing_location_payload(user)
    for field in ("first_name", "last_name", *BILLING_LOCATION_FIELDS):
        if field in provided_fields:
            value = getattr(payload, field)
            next_values[field] = _clean_country(value) if field == "country" else _clean_profile_value(value)

    if provided_fields.intersection(BILLING_LOCATION_FIELDS):
        missing = [label for field, label in BILLING_REQUIRED_FIELDS if not next_values.get(field)]
        if next_values.get("country") in COUNTRIES_REQUIRING_BILLING_REGION and not next_values.get("state_province"):
            missing.append("State/province")
        if missing:
            raise HTTPException(status_code=422, detail=f"{', '.join(missing)} required.")

    _set_billing_profile(user, **next_values)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.patch("/account/password")
def update_account_password(payload: PasswordChangePayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not verify_password(payload.current_password, user.password_hash):
        raise HTTPException(status_code=401, detail="Current password is incorrect.")
    if payload.new_password != payload.confirm_password:
        raise HTTPException(status_code=422, detail="Confirm password must match the new password.")
    if not _password_meets_account_rules(payload.new_password):
        raise HTTPException(
            status_code=422,
            detail="New password must include at least one letter, one number, and one special character.",
        )
    user.password_hash = hash_password(payload.new_password)
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return {"status": "ok"}


@router.patch("/account/notifications")
def update_account_notifications(
    payload: NotificationSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    user.alerts_enabled = payload.alerts_enabled
    user.email_notifications_enabled = payload.email_notifications_enabled
    user.watchlist_activity_notifications = payload.watchlist_activity_notifications
    user.signals_notifications = payload.signals_notifications
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _notification_settings(user)


@router.post("/auth/logout")
def logout():
    return {"status": "ok", "clear_cookie": SESSION_COOKIE_NAME}


@router.post("/billing/checkout-session")
def create_checkout_session(
    request: Request,
    payload: CheckoutSessionPayload | None = None,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    billing_interval = payload.billing_interval if payload else "monthly"
    price_id = _stripe_price_id(billing_interval)
    if not price_id:
        raise HTTPException(status_code=503, detail="Stripe price id is not configured.")

    customer_id = _sync_stripe_customer_for_billing(db, user)
    tax_settings = _stripe_tax_settings(db)
    data: dict[str, Any] = {
        "mode": "subscription",
        "line_items[0][price]": price_id,
        "line_items[0][quantity]": 1,
        "success_url": f"{_frontend_base_url()}/account/billing?checkout=success",
        "cancel_url": f"{_frontend_base_url()}/account/billing?checkout=cancelled",
        "customer": customer_id,
        "client_reference_id": str(user.id),
        "metadata[user_id]": user.id,
        "metadata[email]": user.email,
        "metadata[billing_interval]": billing_interval,
        "subscription_data[metadata][user_id]": user.id,
        "subscription_data[metadata][email]": user.email,
        "subscription_data[metadata][billing_interval]": billing_interval,
    }
    if tax_settings["automatic_tax_enabled"]:
        data["automatic_tax[enabled]"] = "true"
        data["billing_address_collection"] = "required"
        data["customer_update[address]"] = "auto"
        data["customer_update[name]"] = "auto"

    session = _stripe_post("checkout/sessions", data)
    return {"id": session.get("id"), "url": session.get("url")}


@router.post("/billing/customer-portal")
def create_customer_portal_session(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not user.stripe_customer_id:
        raise HTTPException(status_code=404, detail="No Stripe customer is linked to this account.")
    session = _stripe_post(
        "billing_portal/sessions",
        {"customer": user.stripe_customer_id, "return_url": f"{_frontend_base_url()}/account/billing"},
    )
    return {"url": session.get("url")}


@router.post("/billing/subscription/cancel")
def cancel_subscription_at_period_end(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not user.stripe_subscription_id:
        raise HTTPException(status_code=404, detail="No Stripe subscription is linked to this account.")
    subscription = _stripe_post(
        f"subscriptions/{user.stripe_subscription_id}",
        {"cancel_at_period_end": "true"},
    )
    status = str(subscription.get("status") or user.subscription_status or "active")
    _sync_user_subscription(db, obj=subscription, status=status)
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.post("/billing/subscription/reactivate")
def reactivate_subscription_before_expiry(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not user.stripe_subscription_id:
        raise HTTPException(status_code=404, detail="No Stripe subscription is linked to this account.")
    access_expires_at = _aware_utc(user.access_expires_at)
    if access_expires_at is not None and access_expires_at <= datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Subscription access has already expired.")
    subscription = _stripe_post(
        f"subscriptions/{user.stripe_subscription_id}",
        {"cancel_at_period_end": "false"},
    )
    status = str(subscription.get("status") or user.subscription_status or "active")
    _sync_user_subscription(db, obj=subscription, status=status)
    db.commit()
    db.refresh(user)
    return _public_user(user)


def _verify_stripe_signature(payload: bytes, signature_header: str | None) -> None:
    secret = _stripe_webhook_secret()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe webhook secret is not configured.")
    if not signature_header:
        raise HTTPException(status_code=400, detail="Missing Stripe signature.")

    parts = dict(part.split("=", 1) for part in signature_header.split(",") if "=" in part)
    timestamp = parts.get("t")
    signature = parts.get("v1")
    if not timestamp or not signature:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.")

    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.")


def _stripe_object_id(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("id")
    cleaned = str(value or "").strip()
    return cleaned or None


def _datetime_from_epoch(value: Any) -> datetime | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def _aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _extract_metadata(obj: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    candidates = [
        obj.get("metadata"),
        (obj.get("subscription_details") or {}).get("metadata") if isinstance(obj.get("subscription_details"), dict) else None,
    ]
    lines = obj.get("lines") if isinstance(obj.get("lines"), dict) else {}
    for line in lines.get("data") or []:
        if isinstance(line, dict):
            candidates.append(line.get("metadata"))
            price = line.get("price") if isinstance(line.get("price"), dict) else {}
            candidates.append(price.get("metadata"))
    for candidate in candidates:
        if isinstance(candidate, dict):
            metadata.update({str(key): value for key, value in candidate.items()})
    return metadata


def _extract_subscription_id(obj: dict[str, Any]) -> str | None:
    subscription = _stripe_object_id(obj.get("subscription"))
    if subscription:
        return subscription
    parent = obj.get("parent") if isinstance(obj.get("parent"), dict) else {}
    details = parent.get("subscription_details") if isinstance(parent.get("subscription_details"), dict) else {}
    return _stripe_object_id(details.get("subscription"))


def _invoice_line_items(invoice: dict[str, Any]) -> list[dict[str, Any]]:
    lines = invoice.get("lines") if isinstance(invoice.get("lines"), dict) else {}
    return [line for line in lines.get("data") or [] if isinstance(line, dict)]


def _invoice_service_period(invoice: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for line in _invoice_line_items(invoice):
        period = line.get("period") if isinstance(line.get("period"), dict) else {}
        start = _datetime_from_epoch(period.get("start"))
        end = _datetime_from_epoch(period.get("end"))
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    return (min(starts) if starts else None, max(ends) if ends else None)


def _invoice_billing_period_type(invoice: dict[str, Any]) -> str | None:
    metadata = _extract_metadata(invoice)
    interval = str(metadata.get("billing_interval") or "").strip().lower()
    if interval in {"monthly", "annual"}:
        return interval
    for line in _invoice_line_items(invoice):
        price = line.get("price") if isinstance(line.get("price"), dict) else {}
        recurring = price.get("recurring") if isinstance(price.get("recurring"), dict) else {}
        stripe_interval = str(recurring.get("interval") or "").strip().lower()
        if stripe_interval == "month":
            return "monthly"
        if stripe_interval == "year":
            return "annual"
    return None


def _invoice_tax_breakdown(invoice: dict[str, Any]) -> tuple[int | None, str | None]:
    total = 0
    found = False
    breakdown: dict[str, Any] = {}
    for key in ("total_tax_amounts", "total_taxes"):
        values = invoice.get(key)
        if isinstance(values, list) and values:
            breakdown[key] = values
            for item in values:
                if isinstance(item, dict):
                    try:
                        total += int(item.get("amount") or 0)
                        found = True
                    except (TypeError, ValueError):
                        pass
    line_taxes: list[Any] = []
    for line in _invoice_line_items(invoice):
        for key in ("tax_amounts", "taxes"):
            values = line.get(key)
            if isinstance(values, list) and values:
                line_taxes.extend(values)
                if not found:
                    for item in values:
                        if isinstance(item, dict):
                            try:
                                total += int(item.get("amount") or 0)
                                found = True
                            except (TypeError, ValueError):
                                pass
    if line_taxes:
        breakdown["line_taxes"] = line_taxes
    if not found:
        try:
            total = int(invoice.get("tax") or 0)
            found = "tax" in invoice
        except (TypeError, ValueError):
            total = 0
    return (total if found else None, json.dumps(breakdown, sort_keys=True) if breakdown else None)


def _invoice_description(invoice: dict[str, Any]) -> str | None:
    description = str(invoice.get("description") or "").strip()
    if description:
        return description
    for line in _invoice_line_items(invoice):
        description = str(line.get("description") or "").strip()
        if description:
            return description
    return None


def _refund_status(invoice: dict[str, Any]) -> str | None:
    charge = invoice.get("charge") if isinstance(invoice.get("charge"), dict) else {}
    try:
        amount_refunded = int(invoice.get("amount_refunded") or charge.get("amount_refunded") or 0)
        total = int(invoice.get("total") or charge.get("amount") or 0)
    except (TypeError, ValueError):
        amount_refunded = 0
        total = 0
    if amount_refunded > 0 and total > 0 and amount_refunded >= total:
        return "refunded"
    if amount_refunded > 0:
        return "partially_refunded"
    if charge.get("refunded") is True:
        return "refunded"
    return "none"


def _persist_billing_snapshot(db: Session, invoice: dict[str, Any]) -> BillingTransaction | None:
    invoice_id = _stripe_object_id(invoice.get("id"))
    if not invoice_id:
        return None
    user = _find_user_for_stripe_object(db, invoice)
    service_start, service_end = _invoice_service_period(invoice)
    tax_amount, tax_breakdown_json = _invoice_tax_breakdown(invoice)
    customer_address = invoice.get("customer_address") if isinstance(invoice.get("customer_address"), dict) else {}
    status_transitions = invoice.get("status_transitions") if isinstance(invoice.get("status_transitions"), dict) else {}
    charged_at = (
        _datetime_from_epoch(status_transitions.get("paid_at"))
        or _datetime_from_epoch(invoice.get("created"))
    )
    row = db.execute(
        select(BillingTransaction).where(BillingTransaction.stripe_invoice_id == invoice_id)
    ).scalar_one_or_none()
    if not row:
        row = BillingTransaction(stripe_invoice_id=invoice_id)
        db.add(row)

    row.stripe_customer_id = _stripe_object_id(invoice.get("customer"))
    row.stripe_subscription_id = _extract_subscription_id(invoice)
    row.stripe_payment_intent_id = _stripe_object_id(invoice.get("payment_intent"))
    row.stripe_charge_id = _stripe_object_id(invoice.get("charge"))
    row.user_id = user.id if user else None
    row.customer_name = invoice.get("customer_name") or (user.name if user else None)
    row.customer_email = normalize_email(invoice.get("customer_email") or (user.email if user else ""))
    row.billing_country = customer_address.get("country") or (user.country if user else None)
    row.billing_state_province = customer_address.get("state") or (user.state_province if user else None)
    row.billing_postal_code = customer_address.get("postal_code") or (user.postal_code if user else None)
    row.description = _invoice_description(invoice)
    row.billing_period_type = _invoice_billing_period_type(invoice)
    row.service_period_start = service_start
    row.service_period_end = service_end
    row.subtotal_amount = int(invoice.get("subtotal") or 0) if invoice.get("subtotal") is not None else None
    row.tax_amount = tax_amount
    row.total_amount = int(invoice.get("total") or 0) if invoice.get("total") is not None else None
    row.currency = str(invoice.get("currency") or "").upper() or None
    row.charged_at = charged_at
    row.payment_status = str(invoice.get("status") or "").strip() or None
    row.access_expires_at = service_end
    row.refund_status = _refund_status(invoice)
    row.tax_breakdown_json = tax_breakdown_json
    row.payload_json = json.dumps(invoice, sort_keys=True)
    row.updated_at = datetime.now(timezone.utc)
    db.flush()
    return row


def _find_user_for_stripe_object(db: Session, obj: dict[str, Any]) -> UserAccount | None:
    metadata = _extract_metadata(obj)
    user_id = metadata.get("user_id")
    if user_id:
        try:
            user = db.get(UserAccount, int(user_id))
            if user:
                return user
        except (TypeError, ValueError):
            pass

    customer = _stripe_object_id(obj.get("customer"))
    subscription = _extract_subscription_id(obj) or obj.get("subscription") or obj.get("id")
    email = normalize_email(metadata.get("email") or obj.get("customer_email"))
    query = select(UserAccount)
    conditions = []
    if customer:
        conditions.append(UserAccount.stripe_customer_id == str(customer))
    if subscription:
        conditions.append(UserAccount.stripe_subscription_id == str(subscription))
    if email:
        conditions.append(func.lower(UserAccount.email) == email)
    if not conditions:
        return None
    return db.execute(query.where(or_(*conditions))).scalar_one_or_none()


def _sync_user_subscription(
    db: Session,
    *,
    obj: dict[str, Any],
    status: str,
    tier: Literal["free", "premium"] | None = None,
    access_expires_at: datetime | None = None,
) -> UserAccount | None:
    user = _find_user_for_stripe_object(db, obj)
    if not user:
        metadata = _extract_metadata(obj)
        email = normalize_email(metadata.get("email") or obj.get("customer_email"))
        if email:
            user = get_or_create_user(db, email=email)
    if not user:
        return None

    customer = _stripe_object_id(obj.get("customer"))
    subscription = _extract_subscription_id(obj) or (obj.get("id") if str(obj.get("object")) == "subscription" else None)
    period_end = access_expires_at or _datetime_from_epoch(obj.get("current_period_end"))
    if customer:
        user.stripe_customer_id = customer
    if subscription:
        user.stripe_subscription_id = str(subscription)
    user.subscription_status = status
    user.subscription_plan = "premium"
    if "cancel_at_period_end" in obj:
        user.subscription_cancel_at_period_end = bool(obj.get("cancel_at_period_end"))
    if period_end:
        user.access_expires_at = period_end
    paid_through = _aware_utc(user.access_expires_at)
    has_paid_access = bool(
        status in {"active", "trialing"}
        or (paid_through is not None and paid_through > datetime.now(timezone.utc))
    )
    user.entitlement_tier = tier or ("premium" if has_paid_access else "free")
    user.updated_at = datetime.now(timezone.utc)
    db.flush()
    return user


def process_stripe_event(db: Session, event: dict[str, Any]) -> dict[str, Any]:
    event_id = str(event.get("id") or "")
    event_type = str(event.get("type") or "")
    if event_id and db.get(StripeWebhookEvent, event_id):
        return {"status": "already_processed", "event_type": event_type}

    obj = (event.get("data") or {}).get("object") if isinstance(event.get("data"), dict) else {}
    if not isinstance(obj, dict):
        obj = {}

    handled = True
    if event_type == "checkout.session.completed":
        _sync_user_subscription(db, obj=obj, status="active", tier="premium")
    elif event_type in {"invoice.paid", "invoice.payment_succeeded"}:
        snapshot = _persist_billing_snapshot(db, obj)
        _sync_user_subscription(
            db,
            obj=obj,
            status="active",
            tier="premium",
            access_expires_at=snapshot.access_expires_at if snapshot else None,
        )
    elif event_type == "invoice.payment_failed":
        _sync_user_subscription(db, obj=obj, status="payment_failed")
    elif event_type == "customer.subscription.updated":
        status = str(obj.get("status") or "unknown")
        _sync_user_subscription(db, obj=obj, status=status)
    elif event_type == "customer.subscription.deleted":
        _sync_user_subscription(db, obj=obj, status="canceled")
    else:
        handled = False

    if event_id:
        db.add(
            StripeWebhookEvent(
                event_id=event_id,
                event_type=event_type,
                payload_json=json.dumps(event, sort_keys=True),
            )
        )
    db.commit()
    return {"status": "processed" if handled else "ignored", "event_type": event_type}


@router.post("/billing/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.body()
    _verify_stripe_signature(payload, request.headers.get("stripe-signature"))
    try:
        event = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid webhook payload.") from exc
    return process_stripe_event(db, event)


@router.get("/admin/reports/sales-ledger")
def admin_sales_ledger(
    request: Request,
    db: Session = Depends(get_db),
    period: SalesLedgerPeriod = "month_to_date",
    start_date: str | None = None,
    end_date: str | None = None,
    country: str | None = None,
    sort_by: SalesLedgerSortBy = "date_charged",
    sort_dir: SalesLedgerSortDir = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    require_admin_user(db, request)
    rows, total, filters = _sales_ledger_rows(
        db,
        period=period,
        start_date=start_date,
        end_date=end_date,
        country=country,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )
    page_count = max(1, (total + page_size - 1) // page_size)
    payload_rows = [_sales_ledger_row(row) for row in rows]
    return {
        "items": payload_rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": page_count,
        "has_previous": page > 1,
        "has_next": page < page_count,
        "filters": filters,
        "sort": {"sort_by": sort_by, "sort_dir": sort_dir},
        "summary": {
            "net_revenue_amount": sum(row["net_revenue_amount"] for row in payload_rows),
            "vat_collected": sum(row["vat1_collected"] + row["vat2_collected"] for row in payload_rows),
            "gross_amount": sum(row["gross_amount"] for row in payload_rows),
        },
    }


@router.get("/admin/reports/summary")
def admin_reports_summary(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return _reports_summary(db)


@router.get("/admin/reports/sales-ledger/export.{export_format}")
def admin_sales_ledger_export(
    export_format: Literal["xlsx", "pdf"],
    request: Request,
    db: Session = Depends(get_db),
    period: SalesLedgerPeriod = "month_to_date",
    start_date: str | None = None,
    end_date: str | None = None,
    country: str | None = None,
    sort_by: SalesLedgerSortBy = "date_charged",
    sort_dir: SalesLedgerSortDir = "desc",
):
    require_admin_user(db, request)
    rows, _total, filters = _sales_ledger_rows(
        db,
        period=period,
        start_date=start_date,
        end_date=end_date,
        country=country,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    payload_rows = [_sales_ledger_row(row) for row in rows]
    if export_format == "xlsx":
        content = _sales_ledger_xlsx(payload_rows)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        content = _sales_ledger_pdf(payload_rows, filters)
        media_type = "application/pdf"
    filename = _export_filename("sales-ledger", export_format)
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/admin/users")
def admin_users(
    request: Request,
    db: Session = Depends(get_db),
    plan: AdminUserPlanFilter = "all",
    status: str | None = None,
    country: str | None = None,
    admin: AdminUserAdminFilter = "all",
    sort_by: AdminUserSortBy = "created_at",
    sort_dir: AdminUserSortDir = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    require_admin_user(db, request)
    rows, total, filters = _admin_user_rows(
        db,
        plan=plan,
        status=status,
        country=country,
        admin=admin,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )
    page_count = max(1, (total + page_size - 1) // page_size)
    return {
        "items": [_admin_user_row(user) for user in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": page_count,
        "has_previous": page > 1,
        "has_next": page < page_count,
        "filters": filters,
        "sort": {"sort_by": sort_by, "sort_dir": sort_dir},
    }


@router.get("/admin/users/export.{export_format}")
def admin_users_export(
    export_format: Literal["xlsx", "pdf"],
    request: Request,
    db: Session = Depends(get_db),
    plan: AdminUserPlanFilter = "all",
    status: str | None = None,
    country: str | None = None,
    admin: AdminUserAdminFilter = "all",
    sort_by: AdminUserSortBy = "created_at",
    sort_dir: AdminUserSortDir = "desc",
):
    require_admin_user(db, request)
    users, _total, filters = _admin_user_rows(
        db,
        plan=plan,
        status=status,
        country=country,
        admin=admin,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    rows = _admin_users_export_rows(users)
    if export_format == "xlsx":
        content = _admin_users_xlsx(rows)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        content = _admin_users_pdf(rows, filters)
        media_type = "application/pdf"
    filename = _export_filename("admin-users", export_format)
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/admin/settings")
def admin_settings(request: Request, db: Session = Depends(get_db), include_users: bool = True):
    require_admin_user(db, request)
    users = (
        db.execute(select(UserAccount).order_by(UserAccount.created_at.desc(), UserAccount.id.desc())).scalars().all()
        if include_users
        else []
    )
    return {
        "stripe": _stripe_config_status(),
        "stripe_tax": _stripe_tax_config(db),
        "oauth": {"google_client_id": _google_client_id(db) or ""},
        "users": [_public_user(user) for user in users],
        "feature_gates": feature_gate_payloads(db),
        "features": DEFAULT_FEATURE_GATES,
        "plan_config": plan_config_payload(db),
    }


@router.get("/plan-config")
def public_plan_config(db: Session = Depends(get_db)):
    return plan_config_payload(db)


@router.patch("/admin/settings/oauth")
def admin_update_oauth_settings(
    payload: OAuthSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = _set_setting(db, "google_client_id", payload.google_client_id)
    db.commit()
    return {"google_client_id": row.value or ""}


@router.patch("/admin/settings/stripe-tax")
def admin_update_stripe_tax_settings(
    payload: StripeTaxSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    _set_setting(db, "stripe_tax_automatic_tax_enabled", "true" if payload.automatic_tax_enabled else "false")
    _set_setting(db, "stripe_tax_require_billing_address", "true" if payload.require_billing_address else "false")
    _set_setting(db, "stripe_tax_product_tax_code", payload.product_tax_code)
    _set_setting(db, "stripe_tax_price_tax_behavior", payload.price_tax_behavior)
    db.commit()
    return _stripe_tax_config(db)


@router.post("/admin/users/{user_id}/premium")
def admin_set_premium(user_id: int, payload: ManualPremiumPayload, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    user.manual_tier_override = payload.tier
    if payload.tier:
        user.entitlement_tier = payload.tier
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.post("/admin/users/{user_id}/suspend")
def admin_suspend_user(user_id: int, payload: SuspendPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    if admin.id == user_id and payload.suspended:
        raise HTTPException(status_code=400, detail="Admin cannot suspend the current admin session.")
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    user.is_suspended = payload.suspended
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.delete("/admin/users/{user_id}")
def admin_delete_user(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    if admin.id == user_id:
        raise HTTPException(status_code=400, detail="Admin cannot delete the current admin session.")
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    db.delete(user)
    db.commit()
    return {"status": "deleted", "user_id": user_id}


@router.patch("/admin/feature-gates/{feature_key}")
def admin_update_feature_gate(
    feature_key: str,
    payload: FeatureGatePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = set_feature_gate(db, feature_key=feature_key, required_tier=normalize_tier(payload.required_tier))  # type: ignore[arg-type]
    return {
        "feature_key": row.feature_key,
        "required_tier": row.required_tier,
        "description": row.description,
    }


@router.patch("/admin/plan-limits/{feature_key}")
def admin_update_plan_limit(
    feature_key: str,
    payload: PlanLimitPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = set_plan_limit(
        db,
        feature_key=feature_key,  # type: ignore[arg-type]
        tier=normalize_tier(payload.tier),
        limit_value=payload.limit_value,
    )
    return {
        "feature_key": row.feature_key,
        "tier": row.tier,
        "limit_value": row.limit_value,
    }


@router.patch("/admin/plan-prices/{tier}/{billing_interval}")
def admin_update_plan_price(
    tier: str,
    billing_interval: str,
    payload: PlanPricePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    normalized_interval = "annual" if billing_interval == "annual" else "monthly"
    row = set_plan_price(
        db,
        tier=normalize_tier(tier),
        billing_interval=normalized_interval,  # type: ignore[arg-type]
        amount_cents=payload.amount_cents,
        currency=payload.currency,
    )
    return {
        "tier": row.tier,
        "billing_interval": row.billing_interval,
        "amount_cents": row.amount_cents,
        "currency": row.currency,
    }
