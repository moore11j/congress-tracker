from __future__ import annotations

import json
from datetime import datetime, timezone

from app.models import Event, TradeOutcome
from app.routers.events import _event_payload, _parse_event_payload


class NoWriteSession:
    def add(self, *_args, **_kwargs):  # pragma: no cover - defensive
        raise AssertionError("serialization must not write")

    def execute(self, *_args, **_kwargs):  # pragma: no cover - defensive
        raise AssertionError("serialization must not query when enrichment maps are supplied")

    def commit(self):  # pragma: no cover - defensive
        raise AssertionError("serialization must not commit")

    def rollback(self):  # pragma: no cover - defensive
        raise AssertionError("serialization must not rollback")


def _insider_event(payload) -> Event:
    now = datetime(2026, 1, 15, tzinfo=timezone.utc)
    return Event(
        id=42,
        event_type="insider_trade",
        ts=now,
        event_date=now,
        symbol="ACME",
        source="fmp",
        impact_score=1.0,
        payload_json=payload,
        member_name="Example Insider",
        trade_type="sale",
        amount_min=1000,
        amount_max=5000,
        created_at=now,
    )


def _serialize(event: Event):
    return _event_payload(
        event,
        NoWriteSession(),  # type: ignore[arg-type]
        price_memo={},
        current_price_memo={},
        current_quote_meta={},
        member_net_30d_map={},
        symbol_net_30d_map={},
        confirmation_metrics_map={},
        ticker_meta={},
        cik_names={},
        baseline_map={},
        enrich_prices=False,
    )


def test_parse_event_payload_accepts_sqlite_json_string():
    event = _insider_event(json.dumps({"raw": {"companyName": "Acme Corp"}}))

    payload = _parse_event_payload(event)

    assert payload["raw"]["companyName"] == "Acme Corp"


def test_parse_event_payload_accepts_postgres_dict_object():
    event = _insider_event({"raw": {"companyName": "Acme Corp"}})

    payload = _parse_event_payload(event)

    assert payload["raw"]["companyName"] == "Acme Corp"


def test_insider_event_preserves_company_name_from_raw_company_name():
    event = _insider_event(json.dumps({"raw": {"companyName": "Acme Corp"}, "trade_type": "sale"}))

    out = _serialize(event)

    assert out.payload["company_name"] == "Acme Corp"
    assert out.payload["raw"]["companyName"] == "Acme Corp"


def test_insider_event_preserves_company_name_from_postgres_dict_payload():
    event = _insider_event({"raw": {"companyName": "Acme Corp"}, "trade_type": "sale"})

    out = _serialize(event)

    assert out.payload["company_name"] == "Acme Corp"
    assert out.payload["raw"]["companyName"] == "Acme Corp"


def test_insider_event_missing_company_name_is_graceful_and_read_only():
    event = _insider_event(json.dumps({"raw": {}, "trade_type": "sale"}))

    out = _serialize(event)

    assert "company_name" not in out.payload
    assert out.payload["raw"] == {}


def test_event_payload_uses_persisted_outcome_when_price_enrichment_is_disabled():
    event = _insider_event({"raw": {"companyName": "Acme Corp"}, "trade_type": "sale"})
    outcome = TradeOutcome(
        event_id=event.id,
        symbol="ACME",
        trade_type="sale",
        entry_price=10.0,
        current_price=8.75,
        return_pct=12.5,
        alpha_pct=3.0,
        benchmark_return_pct=1.0,
        holding_days=14,
        scoring_status="ok",
        methodology_version="insider_v1",
    )

    out = _event_payload(
        event,
        NoWriteSession(),  # type: ignore[arg-type]
        price_memo={},
        current_price_memo={},
        current_quote_meta={},
        member_net_30d_map={},
        symbol_net_30d_map={},
        confirmation_metrics_map={},
        ticker_meta={},
        cik_names={},
        baseline_map={},
        enrich_prices=False,
        outcome=outcome,
    )

    assert out.pnl_pct == 12.5
    assert out.pnl_source == "trade_outcome"
    assert out.outcome_status == "ok"
    assert out.trade_price == 10.0
    assert out.current_price == 8.75
    assert out.holding_period_days == 14
