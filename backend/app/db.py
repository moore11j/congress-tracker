from __future__ import annotations

import os
import logging
from time import perf_counter
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.request_priority import get_request_context

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

if DATABASE_URL.startswith("sqlite:////data/"):
    Path("/data").mkdir(parents=True, exist_ok=True)

IS_SQLITE = DATABASE_URL.startswith("sqlite")
connect_args = {"check_same_thread": False, "timeout": 30} if IS_SQLITE else {}
pool_options = (
    {}
    if IS_SQLITE
    else {
        "pool_size": int(os.getenv("DB_POOL_SIZE", "8")),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "4")),
        "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "2")),
        "pool_recycle": int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800")),
        "pool_use_lifo": True,
    }
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
    hide_parameters=True,
    **pool_options,
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


if not IS_SQLITE:

    @event.listens_for(engine, "checkout")
    def _log_slow_db_checkout(_dbapi_connection, _connection_record, _connection_proxy) -> None:
        context = get_request_context()
        started_at = context.get("started_at")
        if not isinstance(started_at, (int, float)):
            return
        elapsed_ms = (perf_counter() - started_at) * 1000
        threshold_ms = float(os.getenv("DB_CHECKOUT_SLOW_LOG_MS", "250") or 250)
        if elapsed_ms < threshold_ms:
            return
        logger.warning(
            "db_pool_checkout_slow path=%s priority=%s walnut_route=%s walnut_component=%s elapsed_ms=%.1f",
            context.get("path", "unknown"),
            context.get("priority", "unknown"),
            context.get("walnut_route", "unknown"),
            context.get("walnut_component", "unknown"),
            elapsed_ms,
        )


class Base(DeclarativeBase):
    pass


def ensure_price_cache_volume_columns(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='price_cache'")
            ).fetchone()
            if not table_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(price_cache)")).fetchall()
                if len(row) > 1
            }
            for name in ("volume", "day_volume"):
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE price_cache ADD COLUMN {name} FLOAT"))
            return

        if dialect_name == "postgresql":
            table_exists = conn.execute(text("SELECT to_regclass('public.price_cache')")).scalar()
            if table_exists is None:
                return
            existing = {
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'price_cache'
                          AND column_name IN ('volume', 'day_volume')
                        """
                    )
                ).fetchall()
            }
            missing = [name for name in ("volume", "day_volume") if name not in existing]
            if not missing:
                return
            try:
                conn.execute(text("SET LOCAL lock_timeout = '2s'"))
                conn.execute(text("SET LOCAL statement_timeout = '5s'"))
                for name in missing:
                    conn.execute(text(f"ALTER TABLE price_cache ADD COLUMN {name} FLOAT"))
            except SQLAlchemyError as exc:
                logger.warning("price_cache_volume_schema_update_skipped reason=%s", exc.__class__.__name__)


def ensure_fundamentals_cache_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fundamentals_cache (
                        id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        fetched_at TIMESTAMP NOT NULL,
                        period_date DATE,
                        status TEXT NOT NULL DEFAULT 'ok',
                        error TEXT,
                        company_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        country TEXT,
                        exchange TEXT,
                        market_cap FLOAT,
                        price FLOAT,
                        volume FLOAT,
                        avg_volume FLOAT,
                        beta FLOAT,
                        dividend_yield FLOAT,
                        trailing_pe FLOAT,
                        forward_pe FLOAT,
                        price_to_sales FLOAT,
                        ev_to_ebitda FLOAT,
                        gross_margin FLOAT,
                        operating_margin FLOAT,
                        net_margin FLOAT,
                        roe FLOAT,
                        roic FLOAT,
                        revenue_growth FLOAT,
                        eps_growth FLOAT,
                        ebitda_growth FLOAT,
                        free_cash_flow FLOAT,
                        fcf_margin FLOAT,
                        fcf_growth FLOAT,
                        debt_to_equity FLOAT,
                        current_ratio FLOAT,
                        net_debt_to_ebitda FLOAT,
                        eps_ttm FLOAT,
                        earnings_yield FLOAT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(fundamentals_cache)")).fetchall()
                if len(row) > 1
            }
            columns = {
                "period_date": "DATE",
                "status": "TEXT NOT NULL DEFAULT 'ok'",
                "error": "TEXT",
                "company_name": "TEXT",
                "sector": "TEXT",
                "industry": "TEXT",
                "country": "TEXT",
                "exchange": "TEXT",
                "market_cap": "FLOAT",
                "price": "FLOAT",
                "volume": "FLOAT",
                "avg_volume": "FLOAT",
                "beta": "FLOAT",
                "dividend_yield": "FLOAT",
                "trailing_pe": "FLOAT",
                "forward_pe": "FLOAT",
                "price_to_sales": "FLOAT",
                "ev_to_ebitda": "FLOAT",
                "gross_margin": "FLOAT",
                "operating_margin": "FLOAT",
                "net_margin": "FLOAT",
                "roe": "FLOAT",
                "roic": "FLOAT",
                "revenue_growth": "FLOAT",
                "eps_growth": "FLOAT",
                "ebitda_growth": "FLOAT",
                "free_cash_flow": "FLOAT",
                "fcf_margin": "FLOAT",
                "fcf_growth": "FLOAT",
                "debt_to_equity": "FLOAT",
                "current_ratio": "FLOAT",
                "net_debt_to_ebitda": "FLOAT",
                "eps_ttm": "FLOAT",
                "earnings_yield": "FLOAT",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }
            for name, column_type in columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE fundamentals_cache ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fundamentals_cache (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        fetched_at TIMESTAMPTZ NOT NULL,
                        period_date DATE,
                        status TEXT NOT NULL DEFAULT 'ok',
                        error TEXT,
                        company_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        country TEXT,
                        exchange TEXT,
                        market_cap DOUBLE PRECISION,
                        price DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        avg_volume DOUBLE PRECISION,
                        beta DOUBLE PRECISION,
                        dividend_yield DOUBLE PRECISION,
                        trailing_pe DOUBLE PRECISION,
                        forward_pe DOUBLE PRECISION,
                        price_to_sales DOUBLE PRECISION,
                        ev_to_ebitda DOUBLE PRECISION,
                        gross_margin DOUBLE PRECISION,
                        operating_margin DOUBLE PRECISION,
                        net_margin DOUBLE PRECISION,
                        roe DOUBLE PRECISION,
                        roic DOUBLE PRECISION,
                        revenue_growth DOUBLE PRECISION,
                        eps_growth DOUBLE PRECISION,
                        ebitda_growth DOUBLE PRECISION,
                        free_cash_flow DOUBLE PRECISION,
                        fcf_margin DOUBLE PRECISION,
                        fcf_growth DOUBLE PRECISION,
                        debt_to_equity DOUBLE PRECISION,
                        current_ratio DOUBLE PRECISION,
                        net_debt_to_ebitda DOUBLE PRECISION,
                        eps_ttm DOUBLE PRECISION,
                        earnings_yield DOUBLE PRECISION,
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(text("ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS period_date DATE"))
            conn.execute(text("ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ok'"))
            conn.execute(text("ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
            for name in ("error", "company_name", "sector", "industry", "country", "exchange"):
                conn.execute(text(f"ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS {name} TEXT"))
            for name in (
                "market_cap",
                "price",
                "volume",
                "avg_volume",
                "beta",
                "dividend_yield",
                "trailing_pe",
                "forward_pe",
                "price_to_sales",
                "ev_to_ebitda",
                "gross_margin",
                "operating_margin",
                "net_margin",
                "roe",
                "roic",
                "revenue_growth",
                "eps_growth",
                "ebitda_growth",
                "free_cash_flow",
                "fcf_margin",
                "fcf_growth",
                "debt_to_equity",
                "current_ratio",
                "net_debt_to_ebitda",
                "eps_ttm",
                "earnings_yield",
            ):
                conn.execute(text(f"ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS {name} DOUBLE PRECISION"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_fundamentals_cache_symbol_provider "
                "ON fundamentals_cache (symbol, provider)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_fundamentals_cache_provider_fetched "
                "ON fundamentals_cache (provider, fetched_at)"
            )
        )


def ensure_search_and_insights_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS insights_snapshots (
                        kind TEXT PRIMARY KEY,
                        payload_json TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'fmp',
                        fetched_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_insights_snapshots_fetched_at ON insights_snapshots (fetched_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_securities_symbol_lower ON securities (lower(symbol))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_securities_name_lower ON securities (lower(name))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ticker_meta_symbol_lower ON ticker_meta (lower(symbol))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ticker_meta_company_name_lower ON ticker_meta (lower(company_name))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_members_name_lower ON members (lower(first_name), lower(last_name))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_member_name_lower ON events (lower(member_name))"))
            return

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS insights_snapshots (
                    kind TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'fmp',
                    fetched_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE insights_snapshots ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'fmp'"))
        conn.execute(text("ALTER TABLE insights_snapshots ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()"))
        conn.execute(text("ALTER TABLE insights_snapshots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_insights_snapshots_kind ON insights_snapshots (kind)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_insights_snapshots_fetched_at ON insights_snapshots (fetched_at)"))

        indexed_tables = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name IN ('securities', 'ticker_meta', 'members', 'events')
                    """
                )
            ).fetchall()
        }
        if "securities" in indexed_tables:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_securities_symbol_lower ON securities ((lower(symbol)))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_securities_name_lower ON securities ((lower(name)))"))
        if "ticker_meta" in indexed_tables:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ticker_meta_symbol_lower ON ticker_meta ((lower(symbol)))"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ticker_meta_company_name_lower ON ticker_meta ((lower(company_name)))"))
        if "members" in indexed_tables:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_members_name_lower ON members ((lower(first_name)), (lower(last_name)))"))
        if "events" in indexed_tables:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_member_name_lower ON events ((lower(member_name)))"))


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
                "monthly_price_override": "INTEGER",
                "annual_price_override": "INTEGER",
                "override_currency": "TEXT",
                "override_note": "TEXT",
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
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_manual_tier_override "
                    "ON user_accounts (manual_tier_override)"
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


def ensure_monitoring_alert_columns() -> None:
    with engine.begin() as conn:
        if DATABASE_URL.startswith("sqlite"):
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='monitoring_alerts'")
            ).fetchone()
        else:
            table_exists = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name = 'monitoring_alerts'"
                )
            ).fetchone()
        if not table_exists:
            return
        if DATABASE_URL.startswith("sqlite"):
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(monitoring_alerts)")).fetchall()
                if len(row) > 1
            }
            if "dismissed_at" not in existing:
                conn.execute(text("ALTER TABLE monitoring_alerts ADD COLUMN dismissed_at TIMESTAMP"))
        else:
            conn.execute(text("ALTER TABLE monitoring_alerts ADD COLUMN IF NOT EXISTS dismissed_at TIMESTAMP"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_user_dismissed "
                "ON monitoring_alerts (user_id, dismissed_at, created_at)"
            )
        )


def ensure_house_annual_disclosure_schema() -> None:
    with engine.begin() as conn:
        if DATABASE_URL.startswith("sqlite"):
            positions_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='replicated_portfolio_positions'")
            ).fetchone()
            if positions_exists:
                existing_position_columns = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(replicated_portfolio_positions)")).fetchall()
                    if len(row) > 1
                }
                for name in ("source_type", "source_reason", "confidence", "source_document_id", "source_url"):
                    if name not in existing_position_columns:
                        conn.execute(text(f"ALTER TABLE replicated_portfolio_positions ADD COLUMN {name} TEXT"))

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_documents (
                        id INTEGER PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        filing_date DATE,
                        state_district TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_holdings (
                        id INTEGER PRIMARY KEY,
                        document_row_id INTEGER NOT NULL,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        filing_date DATE,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        asset_name TEXT NOT NULL,
                        symbol TEXT,
                        owner TEXT,
                        asset_type TEXT,
                        value_range TEXT,
                        value_min REAL,
                        value_max REAL,
                        income_type TEXT,
                        income_range TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
        else:
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_type TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_reason TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS confidence TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_document_id TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_url TEXT"))

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_documents (
                        id SERIAL PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        filing_date DATE,
                        state_district TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_holdings (
                        id SERIAL PRIMARY KEY,
                        document_row_id INTEGER NOT NULL,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        filing_date DATE,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        asset_name TEXT NOT NULL,
                        symbol TEXT,
                        owner TEXT,
                        asset_type TEXT,
                        value_range TEXT,
                        value_min DOUBLE PRECISION,
                        value_max DOUBLE PRECISION,
                        income_type TEXT,
                        income_range TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_house_annual_documents_doc "
                "ON house_annual_disclosure_documents (document_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_documents_member_year "
                "ON house_annual_disclosure_documents (member_bioguide_id, filing_year)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_documents_filing_date "
                "ON house_annual_disclosure_documents (filing_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_holdings_member_symbol "
                "ON house_annual_disclosure_holdings (member_bioguide_id, symbol)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_holdings_document "
                "ON house_annual_disclosure_holdings (document_row_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_holdings_filing_date "
                "ON house_annual_disclosure_holdings (filing_date)"
            )
        )


def ensure_trade_outcomes_amount_bigint() -> None:
    if IS_SQLITE:
        return
    with engine.begin() as conn:
        table_exists = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'trade_outcomes'
                """
            )
        ).scalar_one_or_none()
        if not table_exists:
            return

        for column_name in ("amount_min", "amount_max"):
            data_type = conn.execute(
                text(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'trade_outcomes'
                      AND column_name = :column_name
                    """
                ),
                {"column_name": column_name},
            ).scalar_one_or_none()
            if data_type in {"smallint", "integer"}:
                conn.execute(
                    text(
                        f"ALTER TABLE trade_outcomes "
                        f"ALTER COLUMN {column_name} TYPE BIGINT"
                    )
                )


def ensure_user_account_billing_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            user_accounts_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_accounts'")
            ).fetchone()
            if not user_accounts_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(user_accounts)")).fetchall()
                if len(row) > 1
            }
            user_columns = {
                "stripe_price_id": "TEXT",
                "subscription_interval": "TEXT",
            }
            for name, column_type in user_columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE user_accounts ADD COLUMN {name} {column_type}"))
            return

        if dialect_name == "postgresql":
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS stripe_price_id TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS subscription_interval TEXT"))


def ensure_email_notification_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_templates (
                        id INTEGER PRIMARY KEY,
                        template_key TEXT NOT NULL,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        from_name TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        reply_to TEXT,
                        subject TEXT NOT NULL,
                        preheader TEXT,
                        body_text TEXT NOT NULL,
                        body_html TEXT,
                        variables_json TEXT NOT NULL DEFAULT '[]',
                        enabled BOOLEAN NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_deliveries (
                        id INTEGER PRIMARY KEY,
                        user_id INTEGER,
                        to_email TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        template_key TEXT,
                        category TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        provider_message_id TEXT,
                        status TEXT NOT NULL DEFAULT 'queued',
                        idempotency_key TEXT,
                        error TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        sent_at TIMESTAMP
                    )
                    """
                )
            )
            user_accounts_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_accounts'")
            ).fetchone()
            if user_accounts_exists:
                existing = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(user_accounts)")).fetchall()
                    if len(row) > 1
                }
                user_columns = {
                    "email_verified_at": "TIMESTAMP",
                    "email_verification_token_hash": "TEXT",
                    "email_verification_expires_at": "TIMESTAMP",
                }
                for name, column_type in user_columns.items():
                    if name not in existing:
                        conn.execute(text(f"ALTER TABLE user_accounts ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_templates (
                        id SERIAL PRIMARY KEY,
                        template_key TEXT NOT NULL,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        from_name TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        reply_to TEXT,
                        subject TEXT NOT NULL,
                        preheader TEXT,
                        body_text TEXT NOT NULL,
                        body_html TEXT,
                        variables_json TEXT NOT NULL DEFAULT '[]',
                        enabled BOOLEAN NOT NULL DEFAULT true,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_deliveries (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER,
                        to_email TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        template_key TEXT,
                        category TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        provider_message_id TEXT,
                        status TEXT NOT NULL DEFAULT 'queued',
                        idempotency_key TEXT,
                        error TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        sent_at TIMESTAMPTZ
                    )
                    """
                )
            )
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS email_verification_token_hash TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS email_verification_expires_at TIMESTAMPTZ"))

            template_columns = {
                "reply_to": "TEXT",
                "preheader": "TEXT",
                "body_html": "TEXT",
                "variables_json": "TEXT NOT NULL DEFAULT '[]'",
                "enabled": "BOOLEAN NOT NULL DEFAULT true",
                "created_at": "TIMESTAMPTZ DEFAULT now()",
                "updated_at": "TIMESTAMPTZ DEFAULT now()",
            }
            for name, column_type in template_columns.items():
                conn.execute(text(f"ALTER TABLE email_templates ADD COLUMN IF NOT EXISTS {name} {column_type}"))

            delivery_columns = {
                "user_id": "INTEGER",
                "provider_message_id": "TEXT",
                "idempotency_key": "TEXT",
                "error": "TEXT",
                "payload_json": "TEXT",
                "created_at": "TIMESTAMPTZ DEFAULT now()",
                "sent_at": "TIMESTAMPTZ",
            }
            for name, column_type in delivery_columns.items():
                conn.execute(text(f"ALTER TABLE email_deliveries ADD COLUMN IF NOT EXISTS {name} {column_type}"))

        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_email_templates_template_key "
                "ON email_templates (template_key)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_templates_category "
                "ON email_templates (category)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_deliveries_user_created "
                "ON email_deliveries (user_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_deliveries_status "
                "ON email_deliveries (status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_deliveries_template_key "
                "ON email_deliveries (template_key)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_email_deliveries_idempotency_key "
                "ON email_deliveries (idempotency_key)"
            )
        )


def get_db():
    context = get_request_context()
    session_started = perf_counter()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        if context:
            elapsed_ms = (perf_counter() - session_started) * 1000
            threshold_ms = float(os.getenv("DB_SESSION_SLOW_LOG_MS", "2000") or 2000)
            if elapsed_ms >= threshold_ms:
                logger.info(
                    "db_session_timing path=%s priority=%s walnut_route=%s walnut_component=%s duration_ms=%.1f",
                    context.get("path", "unknown"),
                    context.get("priority", "unknown"),
                    context.get("walnut_route", "unknown"),
                    context.get("walnut_component", "unknown"),
                    elapsed_ms,
                )


def is_database_locked_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message
