from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")

if DATABASE_URL.startswith("sqlite:////data/"):
    Path("/data").mkdir(parents=True, exist_ok=True)

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
)

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
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_watchlist_view_states_last_seen_at "
                "ON watchlist_view_states (last_seen_at)"
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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
