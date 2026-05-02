from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")

if DATABASE_URL.startswith("sqlite:////data/"):
    Path("/data").mkdir(parents=True, exist_ok=True)

IS_SQLITE = DATABASE_URL.startswith("sqlite")
connect_args = {"check_same_thread": False, "timeout": 30} if IS_SQLITE else {}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
)

if IS_SQLITE:

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def ensure_event_columns() -> None:
    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
        ).fetchone()
        if not table_exists:
            return
        existing = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info(events)")).fetchall()
            if len(row) > 1
        }
        columns = {
            "member_name": "TEXT",
            "member_bioguide_id": "TEXT",
            "chamber": "TEXT",
            "party": "TEXT",
            "transaction_type": "TEXT",
            "trade_type": "TEXT",
            "amount_min": "INTEGER",
            "amount_max": "INTEGER",
            "symbol": "TEXT",
            "event_date": "TIMESTAMP",
        }
        for name, column_type in columns.items():
            if name not in existing:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {name} {column_type}"))
        if "ticker" in existing:
            conn.execute(
                text(
                    "UPDATE events "
                    "SET symbol = UPPER(ticker) "
                    "WHERE (symbol IS NULL OR symbol = '') AND ticker IS NOT NULL"
                )
            )
        conn.execute(
            text(
                "UPDATE events "
                "SET event_date = ts "
                "WHERE event_date IS NULL AND ts IS NOT NULL"
            )
        )
        conn.execute(
            text(
                "UPDATE events "
                "SET trade_type = transaction_type "
                "WHERE trade_type IS NULL AND transaction_type IS NOT NULL"
            )
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_events_event_date ON events (event_date)")
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_ts ON events (ts)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_symbol ON events (symbol)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_member_bioguide_id "
                "ON events (member_bioguide_id)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_party ON events (party)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_chamber ON events (chamber)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_trade_type "
                "ON events (trade_type)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_symbol_event_date "
                "ON events (symbol, event_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_symbol_ts "
                "ON events (symbol, ts)"
            )
        )
        trade_outcomes_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_outcomes'")
        ).fetchone()
        if trade_outcomes_exists:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_trade_outcomes_benchmark_trade_date_member "
                    "ON trade_outcomes (benchmark_symbol, trade_date, member_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_trade_outcomes_benchmark_status_trade_date_member "
                    "ON trade_outcomes (benchmark_symbol, scoring_status, trade_date, member_id)"
                )
            )
        congress_aliases_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='congress_member_aliases'")
        ).fetchone()
        if congress_aliases_exists:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_congress_member_aliases_group_key "
                    "ON congress_member_aliases (group_key)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_congress_member_aliases_chamber_group_key "
                    "ON congress_member_aliases (chamber, group_key)"
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS watchlist_view_states (
                    watchlist_id INTEGER PRIMARY KEY,
                    last_seen_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        watchlists_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='watchlists'")
        ).fetchone()
        if watchlists_exists:
            existing_watchlist_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(watchlists)")).fetchall()
                if len(row) > 1
            }
            if "owner_user_id" not in existing_watchlist_columns:
                conn.execute(text("ALTER TABLE watchlists ADD COLUMN owner_user_id INTEGER"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_watchlists_owner_user_id "
                    "ON watchlists (owner_user_id)"
                )
            )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_watchlist_view_states_last_seen_at "
                "ON watchlist_view_states (last_seen_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS confirmation_monitoring_snapshots (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    watchlist_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    score INTEGER NOT NULL DEFAULT 0,
                    band TEXT NOT NULL DEFAULT 'inactive',
                    direction TEXT NOT NULL DEFAULT 'neutral',
                    source_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'Inactive',
                    observed_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_confirmation_monitoring_snapshot_scope "
                "ON confirmation_monitoring_snapshots (user_id, watchlist_id, ticker)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_confirmation_monitoring_snapshot_observed "
                "ON confirmation_monitoring_snapshots (observed_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS confirmation_monitoring_events (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    watchlist_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT,
                    score_before INTEGER,
                    score_after INTEGER NOT NULL DEFAULT 0,
                    band_before TEXT,
                    band_after TEXT NOT NULL,
                    direction_before TEXT,
                    direction_after TEXT NOT NULL,
                    source_count_before INTEGER,
                    source_count_after INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_confirmation_monitoring_events_watchlist_created "
                "ON confirmation_monitoring_events (user_id, watchlist_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_confirmation_monitoring_events_dedupe "
                "ON confirmation_monitoring_events (user_id, watchlist_id, ticker, event_type, created_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS saved_screens (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL DEFAULT '{}',
                    last_viewed_at TIMESTAMP,
                    last_refreshed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screens_user_updated "
                "ON saved_screens (user_id, updated_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screens_user_refreshed "
                "ON saved_screens (user_id, last_refreshed_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS saved_screen_snapshots (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    saved_screen_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    confirmation_score INTEGER NOT NULL DEFAULT 0,
                    confirmation_band TEXT NOT NULL DEFAULT 'inactive',
                    direction TEXT NOT NULL DEFAULT 'neutral',
                    source_count INTEGER NOT NULL DEFAULT 0,
                    why_now_state TEXT NOT NULL DEFAULT 'inactive',
                    observed_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_saved_screen_snapshots_scope "
                "ON saved_screen_snapshots (user_id, saved_screen_id, ticker)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_snapshots_screen_observed "
                "ON saved_screen_snapshots (saved_screen_id, observed_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS saved_screen_events (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    saved_screen_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    before_json TEXT,
                    after_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_events_user_created "
                "ON saved_screen_events (user_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_events_screen_created "
                "ON saved_screen_events (saved_screen_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_events_dedupe "
                "ON saved_screen_events (user_id, saved_screen_id, ticker, event_type, created_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS monitoring_alerts (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    event_id INTEGER NOT NULL,
                    alert_type TEXT NOT NULL,
                    symbol TEXT,
                    title TEXT NOT NULL,
                    body TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    event_created_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    read_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_monitoring_alert_source_event "
                "ON monitoring_alerts (user_id, source_type, source_id, event_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_user_read "
                "ON monitoring_alerts (user_id, read_at, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_source_read "
                "ON monitoring_alerts (user_id, source_type, source_id, read_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_event_created "
                "ON monitoring_alerts (event_created_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS notification_subscriptions (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_payload_json TEXT,
                    frequency TEXT NOT NULL DEFAULT 'daily',
                    only_if_new BOOLEAN NOT NULL DEFAULT 1,
                    active BOOLEAN NOT NULL DEFAULT 1,
                    alert_triggers_json TEXT NOT NULL DEFAULT '[]',
                    min_smart_score INTEGER,
                    large_trade_amount INTEGER,
                    last_delivered_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_subscriptions_source "
                "ON notification_subscriptions (source_type, source_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_subscriptions_active_frequency "
                "ON notification_subscriptions (active, frequency)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS notification_deliveries (
                    id INTEGER PRIMARY KEY,
                    subscription_id INTEGER NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'email',
                    status TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body_text TEXT NOT NULL,
                    items_count INTEGER NOT NULL DEFAULT 0,
                    alerts_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    delivered_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_deliveries_subscription_created "
                "ON notification_deliveries (subscription_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_deliveries_status "
                "ON notification_deliveries (status)"
            )
        )
        user_accounts_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_accounts'")
        ).fetchone()
        if user_accounts_exists:
            existing_user_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(user_accounts)")).fetchall()
                if len(row) > 1
            }
            user_columns = {
                "first_name": "TEXT",
                "last_name": "TEXT",
                "country": "TEXT",
                "state_province": "TEXT",
                "postal_code": "TEXT",
                "city": "TEXT",
                "address_line1": "TEXT",
                "address_line2": "TEXT",
                "auth_provider": "TEXT NOT NULL DEFAULT 'email'",
                "google_sub": "TEXT",
                "avatar_url": "TEXT",
                "password_hash": "TEXT",
                "password_reset_token_hash": "TEXT",
                "password_reset_expires_at": "TIMESTAMP",
                "alerts_enabled": "BOOLEAN NOT NULL DEFAULT 1",
                "email_notifications_enabled": "BOOLEAN NOT NULL DEFAULT 1",
                "watchlist_activity_notifications": "BOOLEAN NOT NULL DEFAULT 1",
                "signals_notifications": "BOOLEAN NOT NULL DEFAULT 1",
                "subscription_cancel_at_period_end": "BOOLEAN NOT NULL DEFAULT 0",
                "access_expires_at": "TIMESTAMP",
            }
            for name, column_type in user_columns.items():
                if name not in existing_user_columns:
                    conn.execute(text(f"ALTER TABLE user_accounts ADD COLUMN {name} {column_type}"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_user_accounts_google_sub "
                    "ON user_accounts (google_sub)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_created_at "
                    "ON user_accounts (created_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_last_seen_at "
                    "ON user_accounts (last_seen_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_subscription_status "
                    "ON user_accounts (subscription_status)"
                )
            )
            watchlists_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='watchlists'")
            ).fetchone()
            if watchlists_exists:
                legacy_owner_email = "moore11j@gmail.com"
                owner = conn.execute(
                    text("SELECT id FROM user_accounts WHERE lower(email) = :email"),
                    {"email": legacy_owner_email},
                ).fetchone()
                if owner is None:
                    conn.execute(
                        text(
                            "INSERT INTO user_accounts "
                            "(email, name, auth_provider, role, entitlement_tier, last_seen_at) "
                            "VALUES (:email, :name, 'email', 'admin', 'free', CURRENT_TIMESTAMP)"
                        ),
                        {"email": legacy_owner_email, "name": "Moore"},
                    )
                    owner = conn.execute(
                        text("SELECT id FROM user_accounts WHERE lower(email) = :email"),
                        {"email": legacy_owner_email},
                    ).fetchone()
                if owner is not None:
                    conn.execute(
                        text(
                            "UPDATE watchlists "
                            "SET owner_user_id = :owner_user_id "
                            "WHERE owner_user_id IS NULL"
                        ),
                        {"owner_user_id": owner[0]},
                    )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS billing_transactions (
                    id INTEGER PRIMARY KEY,
                    stripe_customer_id TEXT,
                    stripe_subscription_id TEXT,
                    stripe_invoice_id TEXT,
                    stripe_payment_intent_id TEXT,
                    stripe_charge_id TEXT,
                    user_id INTEGER,
                    customer_name TEXT,
                    customer_email TEXT,
                    billing_country TEXT,
                    billing_state_province TEXT,
                    billing_postal_code TEXT,
                    description TEXT,
                    billing_period_type TEXT,
                    service_period_start TIMESTAMP,
                    service_period_end TIMESTAMP,
                    subtotal_amount INTEGER,
                    tax_amount INTEGER,
                    total_amount INTEGER,
                    currency TEXT,
                    charged_at TIMESTAMP,
                    payment_status TEXT,
                    access_expires_at TIMESTAMP,
                    refund_status TEXT,
                    tax_breakdown_json TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_billing_transactions_invoice "
                "ON billing_transactions (stripe_invoice_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_user_charged "
                "ON billing_transactions (user_id, charged_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_customer "
                "ON billing_transactions (stripe_customer_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_subscription "
                "ON billing_transactions (stripe_subscription_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_charged_at "
                "ON billing_transactions (charged_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS plan_limits (
                    tier TEXT NOT NULL,
                    feature_key TEXT NOT NULL,
                    limit_value INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (tier, feature_key)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS plan_prices (
                    tier TEXT NOT NULL,
                    billing_interval TEXT NOT NULL,
                    amount_cents INTEGER NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (tier, billing_interval)
                )
                """
            )
        )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def is_database_locked_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message
