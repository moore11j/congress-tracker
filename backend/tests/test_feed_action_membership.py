"""Preserve action-only visibility without a whole-action-table feed prefetch."""
import pytest
from sqlalchemy import create_engine, select, text, or_
from sqlalchemy.dialects import postgresql
from app.models import Event
from app.routers.events import (
    GOVERNMENT_CONTRACT_EVENT_TYPES,
    _government_contract_action_event_id_select,
    _government_contract_action_events_only_clause,
)

@pytest.mark.parametrize("event_type", [None, "government_contract", "government_contract_award", "contract_award", "government_exposure", "insider_trade"])
def test_indexed_action_membership_preserves_visibility_and_pagination(event_type):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as db:
        db.execute(text("CREATE TABLE events (id INTEGER PRIMARY KEY, event_type TEXT)"))
        db.execute(text("CREATE TABLE government_contract_actions (event_id INTEGER)"))
        rows = [(1, "insider_trade"), (2, "congress_trade"), (3, "government_contract"),
                (4, "government_contract"), (5, "government_contract_award"),
                (6, "government_contract_award"), (7, "contract_award"),
                (8, "government_exposure"), (9, "government_exposure"), (10, "news")]
        db.execute(text("INSERT INTO events VALUES (:id, :kind)"), [{"id": i, "kind": k} for i, k in rows])
        db.execute(text("INSERT INTO government_contract_actions VALUES (:id)"),
                   [{"id": i} for i in [None, 3, 3, 5, 7, 8, 999]])
        old = or_(Event.event_type.notin_(GOVERNMENT_CONTRACT_EVENT_TYPES),
                  Event.id.in_(_government_contract_action_event_id_select()))
        new = _government_contract_action_events_only_clause()
        def ids(clause, offset=0, limit=100):
            q = select(Event.id).where(clause).order_by(Event.id.desc())
            if event_type: q = q.where(Event.event_type == event_type)
            return list(db.execute(q.offset(offset).limit(limit)).scalars())
        assert ids(new) == ids(old)
        if event_type is None:
            assert ids(new) == [10, 8, 7, 5, 3, 2, 1]
        for offset in [0, 2, 4, 8]:
            assert ids(new, offset, 2) == ids(old, offset, 2)
    engine.dispose()


def test_postgres_probe_is_correlated_and_bounded():
    sql = str(select(Event.id).where(_government_contract_action_events_only_clause())
              .compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "government_contract_actions.event_id = events.id" in sql
    assert "LIMIT 1" in sql
    assert "IS NOT NULL" in sql
    assert "FROM government_contract_actions, events" not in sql
