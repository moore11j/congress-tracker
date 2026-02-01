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
            "amount_min": "REAL",
            "amount_max": "REAL",
            "symbol": "TEXT",
            "event_date": "TIMESTAMP",
        }
        for name, column_type in columns.items():
            if name not in existing:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {name} {column_type}"))
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_events_event_date ON events (event_date)")
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_symbol_event_date "
                "ON events (symbol, event_date)"
            )
        )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
