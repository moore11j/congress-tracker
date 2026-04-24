from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Member(Base):
    __tablename__ = "members"
    id: Mapped[int] = mapped_column(primary_key=True)
    bioguide_id: Mapped[str] = mapped_column(unique=True, index=True)
    first_name: Mapped[Optional[str]]
    last_name: Mapped[Optional[str]]
    chamber: Mapped[str]
    party: Mapped[Optional[str]]
    state: Mapped[Optional[str]]


class Security(Base):
    __tablename__ = "securities"
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[Optional[str]] = mapped_column(unique=True, index=True)
    name: Mapped[str]
    asset_class: Mapped[str]
    sector: Mapped[Optional[str]]


class Filing(Base):
    __tablename__ = "filings"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int]
    source: Mapped[str]
    filing_date: Mapped[Optional[date]]
    document_url: Mapped[Optional[str]]
    document_hash: Mapped[Optional[str]]


class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    filing_id: Mapped[int]
    member_id: Mapped[int]
    security_id: Mapped[Optional[int]]

    owner_type: Mapped[str]
    transaction_type: Mapped[str]
    trade_date: Mapped[Optional[date]]
    report_date: Mapped[Optional[date]]
    amount_range_min: Mapped[Optional[float]]
    amount_range_max: Mapped[Optional[float]]
    description: Mapped[Optional[str]]


class InsiderTransaction(Base):
    __tablename__ = "insider_transactions"
    __table_args__ = (
        Index("ix_insider_transactions_symbol", "symbol"),
        Index("ix_insider_transactions_filing_date", "filing_date"),
        Index("ix_insider_transactions_external_id", "external_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str]
    external_id: Mapped[str] = mapped_column(unique=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reporting_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    insider_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    role: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ownership: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_date: Mapped[Optional[date]]
    filing_date: Mapped[Optional[date]]
    shares: Mapped[Optional[float]]
    price: Mapped[Optional[float]]
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class InstitutionalTransaction(Base):
    __tablename__ = "institutional_transactions"
    __table_args__ = (
        Index("ix_institutional_transactions_symbol", "symbol"),
        Index("ix_institutional_transactions_filing_date", "filing_date"),
        Index("ix_institutional_transactions_external_id", "external_id"),
        Index("ix_institutional_transactions_institution_cik", "institution_cik"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str]
    external_id: Mapped[str] = mapped_column(unique=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    institution_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    institution_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[Optional[date]]
    report_date: Mapped[Optional[date]]
    shares: Mapped[Optional[float]]
    market_value: Mapped[Optional[float]]
    avg_price: Mapped[Optional[float]]
    change_in_shares: Mapped[Optional[float]]
    change_pct: Mapped[Optional[float]]
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class Watchlist(Base):
    __tablename__ = "watchlists"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    owner_user_id: Mapped[Optional[int]]


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    watchlist_id: Mapped[int]
    security_id: Mapped[int]


class WatchlistViewState(Base):
    __tablename__ = "watchlist_view_states"
    watchlist_id: Mapped[int] = mapped_column(primary_key=True)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ConfirmationMonitoringSnapshot(Base):
    __tablename__ = "confirmation_monitoring_snapshots"
    __table_args__ = (
        Index(
            "ix_confirmation_monitoring_snapshot_scope",
            "user_id",
            "watchlist_id",
            "ticker",
            unique=True,
        ),
        Index("ix_confirmation_monitoring_snapshot_observed", "observed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    watchlist_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    score: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    band: Mapped[str] = mapped_column(Text, default="inactive", server_default="inactive")
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral")
    source_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    status: Mapped[str] = mapped_column(Text, default="Inactive", server_default="Inactive")
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ConfirmationMonitoringEvent(Base):
    __tablename__ = "confirmation_monitoring_events"
    __table_args__ = (
        Index("ix_confirmation_monitoring_events_watchlist_created", "user_id", "watchlist_id", "created_at"),
        Index(
            "ix_confirmation_monitoring_events_dedupe",
            "user_id",
            "watchlist_id",
            "ticker",
            "event_type",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    watchlist_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    score_before: Mapped[Optional[int]]
    score_after: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    band_before: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    band_after: Mapped[str] = mapped_column(Text)
    direction_before: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    direction_after: Mapped[str] = mapped_column(Text)
    source_count_before: Mapped[Optional[int]]
    source_count_after: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class UserAccount(Base):
    __tablename__ = "user_accounts"
    __table_args__ = (
        Index("ix_user_accounts_email", "email", unique=True),
        Index("ix_user_accounts_stripe_customer", "stripe_customer_id", unique=True),
        Index("ix_user_accounts_stripe_subscription", "stripe_subscription_id", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    state_province: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    address_line1: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    address_line2: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auth_provider: Mapped[str] = mapped_column(Text, default="email", server_default="email")
    google_sub: Mapped[Optional[str]] = mapped_column(Text, nullable=True, unique=True)
    avatar_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    password_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    password_reset_token_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    password_reset_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    role: Mapped[str] = mapped_column(Text, default="user", server_default="user")
    entitlement_tier: Mapped[str] = mapped_column(Text, default="free", server_default="free")
    manual_tier_override: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_suspended: Mapped[bool] = mapped_column(default=False, server_default=text("0"))
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_plan: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_cancel_at_period_end: Mapped[bool] = mapped_column(default=False, server_default=text("0"))
    access_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    alerts_enabled: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    email_notifications_enabled: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    watchlist_activity_notifications: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    signals_notifications: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class FeatureGate(Base):
    __tablename__ = "feature_gates"

    feature_key: Mapped[str] = mapped_column(Text, primary_key=True)
    required_tier: Mapped[str] = mapped_column(Text, default="premium", server_default="premium")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PlanLimit(Base):
    __tablename__ = "plan_limits"

    tier: Mapped[str] = mapped_column(Text, primary_key=True)
    feature_key: Mapped[str] = mapped_column(Text, primary_key=True)
    limit_value: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PlanPrice(Base):
    __tablename__ = "plan_prices"

    tier: Mapped[str] = mapped_column(Text, primary_key=True)
    billing_interval: Mapped[str] = mapped_column(Text, primary_key=True)
    amount_cents: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    currency: Mapped[str] = mapped_column(Text, default="USD", server_default="USD")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class StripeWebhookEvent(Base):
    __tablename__ = "stripe_webhook_events"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class BillingTransaction(Base):
    __tablename__ = "billing_transactions"
    __table_args__ = (
        Index("ix_billing_transactions_user_charged", "user_id", "charged_at"),
        Index("ix_billing_transactions_customer", "stripe_customer_id"),
        Index("ix_billing_transactions_subscription", "stripe_subscription_id"),
        Index("ix_billing_transactions_invoice", "stripe_invoice_id", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_invoice_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_charge_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_id: Mapped[Optional[int]]
    customer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    customer_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_state_province: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_postal_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_period_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    service_period_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    service_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    subtotal_amount: Mapped[Optional[int]]
    tax_amount: Mapped[Optional[int]]
    total_amount: Mapped[Optional[int]]
    currency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    charged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    payment_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    access_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tax_breakdown_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class NotificationSubscription(Base):
    __tablename__ = "notification_subscriptions"
    __table_args__ = (
        Index("ix_notification_subscriptions_source", "source_type", "source_id"),
        Index("ix_notification_subscriptions_active_frequency", "active", "frequency"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(Text)
    source_id: Mapped[str] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(Text)
    source_payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    frequency: Mapped[str] = mapped_column(Text, default="daily", server_default="daily")
    only_if_new: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    active: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    alert_triggers_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    min_smart_score: Mapped[Optional[int]]
    large_trade_amount: Mapped[Optional[int]]
    last_delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SavedScreen(Base):
    __tablename__ = "saved_screens"
    __table_args__ = (
        Index("ix_saved_screens_user_updated", "user_id", "updated_at"),
        Index("ix_saved_screens_user_refreshed", "user_id", "last_refreshed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    name: Mapped[str] = mapped_column(Text)
    params_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    last_viewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_refreshed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SavedScreenSnapshot(Base):
    __tablename__ = "saved_screen_snapshots"
    __table_args__ = (
        Index("ix_saved_screen_snapshots_screen_observed", "saved_screen_id", "observed_at"),
        Index("ix_saved_screen_snapshots_scope", "user_id", "saved_screen_id", "ticker", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    saved_screen_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    confirmation_score: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    confirmation_band: Mapped[str] = mapped_column(Text, default="inactive", server_default="inactive")
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral")
    source_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    why_now_state: Mapped[str] = mapped_column(Text, default="inactive", server_default="inactive")
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SavedScreenEvent(Base):
    __tablename__ = "saved_screen_events"
    __table_args__ = (
        Index("ix_saved_screen_events_user_created", "user_id", "created_at"),
        Index("ix_saved_screen_events_screen_created", "saved_screen_id", "created_at"),
        Index("ix_saved_screen_events_dedupe", "user_id", "saved_screen_id", "ticker", "event_type", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    saved_screen_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    before_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class NotificationDelivery(Base):
    __tablename__ = "notification_deliveries"
    __table_args__ = (
        Index("ix_notification_deliveries_subscription_created", "subscription_id", "created_at"),
        Index("ix_notification_deliveries_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    subscription_id: Mapped[int]
    channel: Mapped[str] = mapped_column(Text, default="email", server_default="email")
    status: Mapped[str] = mapped_column(Text)
    subject: Mapped[str] = mapped_column(Text)
    body_text: Mapped[str] = mapped_column(Text)
    items_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    alerts_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        Index("ix_events_event_type_ts", "event_type", "ts"),
        Index("ix_events_ts", "ts"),
        Index("ix_events_symbol", "symbol"),
        Index("ix_events_member_bioguide_id", "member_bioguide_id"),
        Index("ix_events_party", "party"),
        Index("ix_events_chamber", "chamber"),
        Index("ix_events_trade_type", "trade_type"),
        Index("ix_events_event_date", "event_date"),
        Index("ix_events_symbol_event_date", "symbol", "event_date"),
        Index("ix_events_symbol_ts", "symbol", "ts"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str]
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[str]
    impact_score: Mapped[float] = mapped_column(
        default=0.0,
        server_default=text("0.0"),
    )
    payload_json: Mapped[str] = mapped_column(Text)
    member_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_bioguide_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chamber: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    party: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amount_min: Mapped[Optional[int]]
    amount_max: Mapped[Optional[int]]
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class QuoteCache(Base):
    __tablename__ = "quotes_cache"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    price: Mapped[float]
    asof_ts: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class PriceCache(Base):
    __tablename__ = "price_cache"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    date: Mapped[str] = mapped_column(Text, primary_key=True)
    close: Mapped[float]
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TickerMeta(Base):
    __tablename__ = "ticker_meta"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    company_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )


class CikMeta(Base):
    __tablename__ = "cik_meta"

    cik: Mapped[str] = mapped_column(Text, primary_key=True)
    company_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )


class TradeOutcome(Base):
    __tablename__ = "trade_outcomes"
    __table_args__ = (
        Index("ix_trade_outcomes_member_trade_date", "member_id", "trade_date"),
        Index("ix_trade_outcomes_status", "scoring_status"),
        Index("ix_trade_outcomes_symbol", "symbol"),
        Index("ix_trade_outcomes_benchmark_trade_date_member", "benchmark_symbol", "trade_date", "member_id"),
        Index(
            "ix_trade_outcomes_benchmark_status_trade_date_member",
            "benchmark_symbol",
            "scoring_status",
            "trade_date",
            "member_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[int] = mapped_column(unique=True, index=True)
    member_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_date: Mapped[Optional[date]]
    entry_price: Mapped[Optional[float]]
    entry_price_date: Mapped[Optional[date]]
    current_price: Mapped[Optional[float]]
    current_price_date: Mapped[Optional[date]]
    benchmark_symbol: Mapped[str] = mapped_column(Text, default="^GSPC", server_default="^GSPC")
    benchmark_entry_price: Mapped[Optional[float]]
    benchmark_current_price: Mapped[Optional[float]]
    return_pct: Mapped[Optional[float]]
    benchmark_return_pct: Mapped[Optional[float]]
    alpha_pct: Mapped[Optional[float]]
    holding_days: Mapped[Optional[int]]
    amount_min: Mapped[Optional[int]]
    amount_max: Mapped[Optional[int]]
    scoring_status: Mapped[str] = mapped_column(Text, default="ok", server_default="ok")
    scoring_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    methodology_version: Mapped[str] = mapped_column(Text, default="congress_v1", server_default="congress_v1")
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CongressMemberAlias(Base):
    __tablename__ = "congress_member_aliases"
    __table_args__ = (
        Index("ix_congress_member_aliases_group_key", "group_key"),
        Index("ix_congress_member_aliases_chamber_group_key", "chamber", "group_key"),
    )

    alias_member_id: Mapped[str] = mapped_column(Text, primary_key=True)
    group_key: Mapped[str] = mapped_column(Text, nullable=False)
    authoritative_member_id: Mapped[str] = mapped_column(Text, nullable=False)
    member_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_slug: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chamber: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    party: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    state: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
