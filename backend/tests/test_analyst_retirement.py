"""Retiring the live provider cannot erase or fabricate analyst observations."""
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from app.jobs import refresh_analyst_consensus as consensus_job, refresh_analyst_events as events_job
from app.models import AnalystConsensusSnapshot
from app.services import analyst_consensus as analysts
from app.services.analyst_consensus_shadow_review import analyst_consensus_shadow_component_score
from test_analyst_consensus import _session


def forbidden(*args, **kwargs):
    pytest.fail("Retired analyst source attempted database/provider work")


@pytest.mark.parametrize("module,method", [(consensus_job, "refresh_analyst_consensus"), (events_job, "refresh_analyst_events")])
def test_scheduled_refresh_exits_before_schema_or_database_work(monkeypatch, module, method):
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "1")
    monkeypatch.setattr(module, "ensure_analyst_consensus_schema", forbidden)
    monkeypatch.setattr(module, "SessionLocal", forbidden)
    result = getattr(module, method)()
    assert result == {"status": "skipped", "reason": "provider_disabled", "committed": False, "symbols_attempted": 0}


def test_off_switch_preserves_snapshots_and_dated_history_but_removes_current_inputs(monkeypatch):
    sessions, engine = _session()
    with sessions() as db:
        row = AnalystConsensusSnapshot(symbol="AAPL", snapshot_date=date(2026, 10, 8),
            availability_status="available", provider_status="available", source="fmp",
            ingested_at=datetime(2026, 10, 8, tzinfo=timezone.utc), raw_payload_json='{"saved":"original"}',
            recommendation_label="Bullish", weighted_rating_value=1.2, price_target_consensus=250,
            consensus_implied_upside_pct=20)
        db.add(row); db.commit(); db.refresh(row)
        before = {c.name: getattr(row, c.name) for c in row.__table__.columns}
        history_before = analysts.history_payload(db, "AAPL", start_date=date(2026, 10, 1), end_date=date(2026, 10, 9))
        monkeypatch.setenv("FMP_PROVIDER_DISABLED", "1")
        for name in ("fetch_grade_events", "fetch_grades_summary", "fetch_historical_grades", "fetch_price_target_consensus",
                     "fetch_price_target_news", "fetch_price_target_summary"):
            monkeypatch.setattr(analysts, name, forbidden)
        for _ in range(2):
            assert analysts.refresh_consensus_on_cache_miss(db, "AAPL") == {"attempted": False, "reason": "provider_disabled"}
            for method in (analysts.ingest_symbol_consensus, analysts.ingest_symbol_grade_events,
                           analysts.ingest_symbol_historical_grade_events, analysts.ingest_symbol_price_target_events):
                assert method(db, "AAPL")["error"] == "provider_disabled"
            for details in (False, True):
                current = analysts.current_consensus_payload(db, "AAPL", include_details=details)
                assert current["currentSnapshot"] is None
                assert current["availability"] == {"status": "unavailable", "reason": "provider_disabled"}
                assert current["access"]["detailsLocked"] == (not details)
                compared = analysts.compare_consensus_payload(db, ["AAPL", "MSFT", "AAPL"], include_details=details)
                assert compared["symbols"] == ["AAPL", "MSFT"]
                assert all(value["currentSnapshot"] is None for value in compared["items"].values())
            component = analysts.analyst_consensus_component_inputs(db, "AAPL")
            assert analyst_consensus_shadow_component_score(component["inputs"]) is None
            assert component["inputs"]["freshnessStatus"] == "unavailable"
            db.commit(); db.refresh(row)
            assert {c.name: getattr(row, c.name) for c in row.__table__.columns} == before
            assert len(list(db.scalars(select(AnalystConsensusSnapshot)))) == 1
        assert analysts.history_payload(db, "AAPL", start_date=date(2026, 10, 1), end_date=date(2026, 10, 9)) == history_before
        monkeypatch.setenv("FMP_PROVIDER_DISABLED", "0")
        assert analysts.current_consensus_payload(db, "AAPL")["currentSnapshot"]["symbol"] == "AAPL"
    engine.dispose()
