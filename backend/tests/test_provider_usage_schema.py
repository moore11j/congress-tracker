from sqlalchemy import create_engine, text

from app.db import ensure_provider_usage_schema


def test_provider_usage_schema_does_not_require_optional_tables():
    engine = create_engine("sqlite:///:memory:", future=True)

    ensure_provider_usage_schema(engine)
    ensure_provider_usage_schema(engine)

    with engine.connect() as conn:
        table = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='provider_usage_events'")
        ).scalar()

    assert table == "provider_usage_events"


def test_provider_usage_schema_indexes_optional_tables_when_present():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE members (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)"))
        conn.execute(text("CREATE TABLE events (id INTEGER PRIMARY KEY, member_name TEXT)"))

    ensure_provider_usage_schema(engine)

    with engine.connect() as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='index'")
            ).fetchall()
        }

    assert "ix_members_name_lower" in indexes
    assert "ix_events_member_name_lower" in indexes
