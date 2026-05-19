from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers.events import (
    _member_insider_event_suggestions_query,
    _member_suggestions_query,
    list_events,
    suggest_member_insider,
    suggest_role,
)


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def test_member_suggestion_distinct_sort_key_is_selected_for_postgres():
    compiled = str(
        _member_suggestions_query("trump", 10).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    select_clause = compiled.split(" FROM ", 1)[0]
    assert "lower(events.member_name) AS member_name_sort" in select_clause
    assert "ORDER BY member_name_sort" in compiled


def test_member_insider_suggestion_distinct_sort_key_is_selected_for_postgres():
    compiled = str(
        _member_insider_event_suggestions_query("%trump%", 10).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    select_clause = compiled.split(" FROM ", 1)[0]
    assert "lower(events.member_name) AS member_name_sort" in select_clause
    assert "ORDER BY member_name_sort" in compiled


def test_member_insider_suggest_returns_trump_matches_without_requiring_exact_case():
    now = datetime(2026, 5, 17, tzinfo=timezone.utc)
    with Session(_engine()) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=now,
                event_date=now,
                symbol="ACME",
                source="test",
                member_name="Donald J. Trump",
                payload_json=json.dumps({"reporting_cik": "0001234567"}),
            )
        )
        db.commit()

        lower_response = suggest_member_insider(db=db, q="trump", limit=10)
        title_response = suggest_member_insider(db=db, q="Trump", limit=10)
        miss_response = suggest_member_insider(db=db, q="trmp", limit=10)

    assert any(item["value"] == "Donald J. Trump" for item in lower_response["items"])
    assert any(item["value"] == "Donald J. Trump" for item in title_response["items"])
    assert miss_response == {"items": []}


def test_member_insider_suggest_returns_insiders_with_company_context_from_payload():
    now = datetime(2026, 5, 17, tzinfo=timezone.utc)
    with Session(_engine()) as db:
        db.add(
            Event(
                id=2,
                event_type="insider_trade",
                ts=now,
                event_date=now,
                symbol="AAPL",
                source="test",
                member_name=None,
                payload_json=json.dumps({
                    "insider_name": "Tim Cook",
                    "reporting_cik": "0001214156",
                    "company_name": "Apple Inc.",
                    "role": "CEO",
                }),
            )
        )
        db.commit()

        response = suggest_member_insider(db=db, q="Tim", limit=10)

    item = next(item for item in response["items"] if item["category"] == "insider")
    assert item["value"] == "Tim Cook"
    assert item["label"] == "Tim Cook · Apple Inc. · AAPL · CEO"
    assert item["symbol"] == "AAPL"


def test_role_suggest_returns_canonical_aliases():
    now = datetime(2026, 5, 17, tzinfo=timezone.utc)
    with Session(_engine()) as db:
        db.add_all(
            [
                Event(
                    id=3,
                    event_type="insider_trade",
                    ts=now,
                    event_date=now,
                    symbol="AAPL",
                    source="test",
                    member_name="Tim Cook",
                    payload_json=json.dumps({"officerTitle": "Chief Executive Officer"}),
                ),
                Event(
                    id=4,
                    event_type="insider_trade",
                    ts=now,
                    event_date=now,
                    symbol="NKE",
                    source="test",
                    member_name="Tim Cook",
                    payload_json=json.dumps({"role": "Director"}),
                ),
            ]
        )
        db.commit()

        ceo = suggest_role(db=db, q="ce", limit=10)
        director = suggest_role(db=db, q="dir", limit=10)

    assert "CEO" in ceo["items"]
    assert "Director" in director["items"]


def test_member_text_filter_matches_insider_events_and_reports_debug_diagnostics():
    now = datetime(2026, 5, 17, tzinfo=timezone.utc)
    with Session(_engine()) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=now,
                event_date=now,
                symbol="ACME",
                source="test",
                member_name="Donald J. Trump",
                trade_type="purchase",
                payload_json=json.dumps({"reporting_cik": "0001234567"}),
            )
        )
        db.commit()

        response = list_events(db=db, member="trump", limit=50, enrich_prices=False, debug=True)

    assert len(response.items) == 1
    assert response.items[0].member_name == "Donald J. Trump"
    assert response.debug is not None
    assert "event_type=congress_trade" not in response.debug.applied_filters
    assert response.debug.diagnostics["member_name_insider_matches"] == 1
