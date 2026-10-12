"""Source transitions must not look like company developments."""
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import (FundamentalsCache, MonitoringAlert, Security, UserAccount,
    Watchlist, WatchlistItem, WatchlistAlertRule, WatchlistAlertRuleState)
from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts, validate_conditions


@pytest.fixture
def scenario():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        now = datetime(2026, 10, 12, 6, tzinfo=timezone.utc)
        user = UserAccount(email="basis@example.test")
        security = Security(symbol="TEST", name="Test", asset_class="equity")
        db.add_all([user, security]); db.flush()
        watchlist = Watchlist(name="Basis", owner_user_id=user.id)
        db.add(watchlist); db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type="ticker"))
        rule = WatchlistAlertRule(user_id=user.id, watchlist_id=watchlist.id, name="Leverage",
            enabled=True, scope_type="any_watchlist_ticker", match_type="all", delivery="both",
            conditions_json=json.dumps(validate_conditions([dict(metric="net_debt_to_ebitda",
                operator="gt", comparison_type="value", comparison_value=2)])))
        row = FundamentalsCache(symbol="TEST", provider="fmp", status="ok", fetched_at=now,
            net_debt_to_ebitda=1, current_ratio=2)
        db.add_all([rule, row]); db.commit()
        def run(minutes):
            result = evaluate_watchlist_custom_alerts(db, user_id=user.id,
                watchlist_id=watchlist.id, now=now+timedelta(minutes=minutes))
            db.commit()
            return result
        yield db, rule, row, run
    engine.dispose()


@pytest.mark.parametrize("transition", ["provider", "methodology", "field_source", "field_basis", "rollback", "comparison"])
def test_financial_transition_rebaselines_then_real_changes_trigger_once(scenario, transition):
    db, rule, row, run = scenario
    evidence = {"methodology": "free_v1", "supplement": {"version": "finnhub_v2", "used_fields": [], "fields": {}}}
    if transition != "provider":
        row.provider = "free_direct"
        row.source_evidence_json = json.dumps(evidence)
    if transition == "field_basis":
        evidence["supplement"].update(used_fields=["net_debt_to_ebitda"],
            fields={"net_debt_to_ebitda": {"source": "finnhub", "basis": "ttm", "unit": "multiple", "path": "example"}})
        row.source_evidence_json = json.dumps(evidence)
    if transition == "comparison":
        rule.conditions_json = json.dumps(validate_conditions([dict(metric="price", operator="gt",
            comparison_type="metric", comparison_metric="current_ratio")]))
        from app.models import QuoteCache
        db.add(QuoteCache(symbol="TEST", price=1.5, asof_ts=row.fetched_at))
    db.commit()
    assert run(0)["triggered"] == 0
    if transition == "provider": row.provider = "free_direct"
    elif transition == "rollback": row.provider = "fmp"
    elif transition == "methodology": evidence["methodology"] = "free_v2"
    elif transition in {"field_source", "comparison"}:
        metric = "current_ratio" if transition == "comparison" else "net_debt_to_ebitda"
        evidence["supplement"].update(used_fields=[metric], fields={metric: {"source": "finnhub", "basis": "quarter_end"}})
    else: evidence["supplement"]["fields"]["net_debt_to_ebitda"]["basis"] = "annual"
    row.source_evidence_json = json.dumps(evidence)
    row.net_debt_to_ebitda = 3
    row.current_ratio = 1
    result = run(5)
    assert result["triggered"] == 0
    assert result["initialized"] == 1
    assert run(10)["triggered"] == 0
    row.net_debt_to_ebitda = 1; row.current_ratio = 2
    assert run(15)["triggered"] == 0
    row.net_debt_to_ebitda = 3; row.current_ratio = 1
    assert run(20)["triggered"] == 1
    assert run(25)["triggered"] == 0
    assert len(db.scalars(select(MonitoringAlert)).all()) == 1


def test_dates_source_hashes_and_unrelated_supplements_do_not_hide_real_change(scenario):
    db, _, row, run = scenario
    row.provider = "free_direct"
    row.source_evidence_json = json.dumps({"methodology": "free_v1", "period_end": "2026-03-31", "facts": {"sha256": "old"}})
    assert run(0)["triggered"] == 0
    row.source_evidence_json = json.dumps({"methodology": "free_v1", "period_end": "2026-06-30", "facts": {"sha256": "new"},
        "supplement": {"used_fields": ["roe"], "fields": {"roe": {"basis": "ttm"}}}})
    row.net_debt_to_ebitda = 3
    assert run(5)["triggered"] == 1


def test_legacy_rule_state_is_baselined_once(scenario):
    db, rule, row, run = scenario
    db.add(WatchlistAlertRuleState(rule_id=rule.id, ticker="TEST", current_result=False,
        previous_result=False, last_evaluated_at=row.fetched_at, values_json=json.dumps({"0": 1})))
    row.net_debt_to_ebitda = 3
    db.commit()
    assert run(5)["triggered"] == 0
    assert run(10)["initialized"] == 0


def test_missing_replacement_retains_last_valid_state_until_baseline(scenario):
    db, rule, row, run = scenario
    assert run(0)["initialized"] == 1
    row.provider = "free_direct"
    row.net_debt_to_ebitda = None
    assert run(5)["unavailable"] == 1
    state = db.scalar(select(WatchlistAlertRuleState).where(WatchlistAlertRuleState.rule_id == rule.id))
    assert json.loads(state.values_json)["__fundamental_basis"]["net_debt_to_ebitda"]["provider"] == "fmp"
    row.net_debt_to_ebitda = 3
    assert run(10)["initialized"] == 1
    assert run(15)["triggered"] == 0


def test_two_cached_providers_use_value_and_identity_from_same_newest_row(scenario):
    db, _, old, run = scenario
    assert run(0)["triggered"] == 0
    new = FundamentalsCache(symbol="TEST", provider="free_direct", fetched_at=old.fetched_at+timedelta(minutes=5),
        status="ok", net_debt_to_ebitda=3)
    db.add(new)
    assert run(5)["initialized"] == 1
    # Roll back by refreshing the old provider; the old row remains retained.
    old.fetched_at = old.fetched_at+timedelta(minutes=10)
    assert run(10)["initialized"] == 1
    old.net_debt_to_ebitda = 3
    assert run(15)["triggered"] == 1


def test_supplement_dates_and_numbers_change_without_rebaselining(scenario):
    db, _, row, run = scenario
    row.provider = "free_direct"
    evidence = {"methodology": "free_v1", "supplement": {"version": "finnhub_v2",
        "used_fields": ["net_debt_to_ebitda"], "fields": {"net_debt_to_ebitda": {
            "source": "finnhub", "basis": "ttm", "unit": "multiple", "period_date": "2026-03-31"}},
        "observed_at": "2026-07-01", "payload_sha256": "old"}}
    row.source_evidence_json = json.dumps(evidence)
    assert run(0)["initialized"] == 1
    evidence["supplement"]["fields"]["net_debt_to_ebitda"]["period_date"] = "2026-06-30"
    evidence["supplement"].update(observed_at="2026-10-01", payload_sha256="new")
    row.source_evidence_json = json.dumps(evidence)
    row.net_debt_to_ebitda = 3
    assert run(5)["triggered"] == 1


def test_financial_rule_uses_one_cache_read_for_multiple_conditions(scenario):
    db, rule, row, run = scenario
    from sqlalchemy import event
    reads = []
    rule.conditions_json = json.dumps(validate_conditions([
        dict(metric="net_debt_to_ebitda", operator="gt", comparison_type="value", comparison_value=2),
        dict(metric="current_ratio", operator="gt", comparison_type="value", comparison_value=1)]))
    db.commit()
    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "FROM fundamentals_cache" in statement:
            reads.append(statement)
    event.listen(db.bind, "before_cursor_execute", record)
    try: assert run(0)["initialized"] == 1
    finally: event.remove(db.bind, "before_cursor_execute", record)
    assert len(reads) == 1


def test_formula_change_rebaselines_even_when_provider_and_version_match(scenario):
    db, _, row, run = scenario
    row.provider = "free_direct"
    evidence = {"methodology": "free_v1", "calculations": {
        "net_debt_to_ebitda": "net debt / operating-income-based EBITDA"}}
    row.source_evidence_json = json.dumps(evidence)
    assert run(0)["initialized"] == 1
    evidence["calculations"]["net_debt_to_ebitda"] = "net debt / net-income-based EBITDA"
    row.source_evidence_json = json.dumps(evidence)
    row.net_debt_to_ebitda = 3
    assert run(5)["initialized"] == 1
    assert run(10)["triggered"] == 0
    row.net_debt_to_ebitda = 1
    run(15)
    row.net_debt_to_ebitda = 3
    assert run(20)["triggered"] == 1


def test_actual_normalizers_preserve_real_changes_and_baseline_fallback(scenario):
    from app.services import finnhub_metrics, free_fundamentals
    db, rule, row, run = scenario
    rule.conditions_json = json.dumps(validate_conditions([dict(metric="current_ratio",
        operator="gt", comparison_type="value", comparison_value=2)]))
    observed = row.fetched_at.replace(tzinfo=timezone.utc)
    def apply(sec_value, vendor_value, minutes):
        sec = dict(symbol="TEST", provider="sec_edgar", current_ratio=sec_value,
            source_evidence_json=json.dumps({"methodology": "sec_fundamentals_v1",
                "calculations": {"current_ratio": "period-end current assets / current liabilities"}}))
        raw = {"symbol": "TEST", "metricType": "all", "metric": {},
            "series": {"quarterly": {"currentRatio": [{"period": "2026-09-30", "v": vendor_value}]}}}
        metrics = finnhub_metrics.normalize(raw, symbol="TEST", observed_at=observed+timedelta(minutes=minutes))
        combined = free_fundamentals.combine(sec, metrics)
        row.provider = combined["provider"]
        row.current_ratio = combined["current_ratio"]
        row.source_evidence_json = combined["source_evidence_json"]
    apply(1, 3, 0)
    assert run(0)["initialized"] == 1
    apply(None, 3, 5)
    assert run(5)["initialized"] == 1
    apply(None, 1, 10)
    assert run(10)["triggered"] == 0
    apply(None, 3, 15)
    assert run(15)["triggered"] == 1
    apply(1, 3, 20)
    assert run(20)["initialized"] == 1
    apply(3, 3, 25)
    assert run(25)["triggered"] == 1
    assert len(db.scalars(select(MonitoringAlert)).all()) == 2
