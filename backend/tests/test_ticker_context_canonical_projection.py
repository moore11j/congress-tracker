from __future__ import annotations

import json

from sqlalchemy.orm import Session

import app.main as main_module
from app.entitlements import ENTITLEMENTS
from app.services.confirmation_score import confirmation_score_bundle_from_source_contexts
from test_ticker_signals_summary import _engine, _mock_ticker_context_bundle_dependencies


def _canonical_payload() -> dict:
    source_contexts = {
        "price_volume": {"status": "active", "direction": "bullish", "title": "Public tape"},
        "fundamentals": {"status": "active", "direction": "bullish", "title": "Public fundamentals"},
        "insiders": {"status": "inactive", "direction": "neutral", "title": "No insider activity"},
        "congress": {"status": "inactive", "direction": "neutral", "title": "No Congress activity"},
        "government_contracts": {"status": "inactive", "direction": "neutral", "title": "No contracts"},
        "signals": {
            "status": "active",
            "direction": "bullish",
            "title": "Private signal evidence",
            "recent_count": 1,
            "latest_score": 88,
        },
        "macro_positioning": {"status": "active", "direction": "bullish", "title": "Private macro evidence"},
    }
    bundle = confirmation_score_bundle_from_source_contexts(
        "TEST",
        lookback_days=30,
        source_contexts={
            **source_contexts,
            "options_flow": {"status": "active", "direction": "bullish", "title": "Private options evidence"},
            "institutional_activity": {"status": "active", "direction": "bullish", "title": "Private institutional evidence"},
        },
    )
    return {
        "symbol": "TEST",
        "status": "ok",
        "generated_at": "2026-08-25T00:00:00+00:00",
        "confirmation_score_bundle": bundle,
        "cross_source_divergence": None,
        "similar_historical_setups": {"items": [{"detail": "Premium-only history"}]},
        "source_cards": {**source_contexts},
        "options_flow_summary": {
            "ticker": "TEST",
            "lookback_days": 30,
            "state": "bullish",
            "summary": "Private options evidence",
            "signals": ["Private options evidence"],
            "metrics": {"total_premium": 1000000},
        },
        "signals_summary": {
            "symbol": "TEST",
            "signals": source_contexts["signals"],
            "rows": [{"title": "PRIVATE_SIGNAL_ROW"}],
            "items": [{"title": "PRIVATE_SIGNAL_ROW"}],
            "recent_count": 1,
            "recent_signal_count": 1,
            "latest_signal_score": 88,
        },
    }


def test_canonical_context_cache_is_projected_before_a_free_response_is_returned():
    canonical = _canonical_payload()
    free = main_module._project_ticker_context_bundle_for_entitlements(
        canonical,
        symbol="TEST",
        source_entitlements=main_module._ticker_context_source_entitlements(ENTITLEMENTS["free"]),
    )

    assert free["signals_summary"]["items"] == []
    assert free["signals_summary"]["rows"] == []
    assert free["signals_summary"]["latest_signal_score"] is None
    assert free["source_entitlements"]["signals"]["locked"] is True
    assert free["confirmation_score_bundle"]["sources"]["signals"]["locked"] is True
    assert free["confirmation_score_bundle"]["sources"]["options_flow"]["locked"] is True
    assert free["confirmation_score_bundle"]["sources"]["institutional_activity"]["locked"] is True
    assert "PRIVATE_SIGNAL_ROW" not in json.dumps(free)
    assert "Private options evidence" not in json.dumps(free)
    assert "Private institutional evidence" not in json.dumps(free)
    assert "Private macro evidence" not in json.dumps(free)
    assert free["options_flow_summary"]["locked"] is True
    assert free["source_cards"]["macro_positioning"]["locked"] is True


def test_canonical_context_cache_keeps_paid_content_server_rendered_for_paid_tiers():
    canonical = _canonical_payload()
    premium = main_module._project_ticker_context_bundle_for_entitlements(
        canonical,
        symbol="TEST",
        source_entitlements=main_module._ticker_context_source_entitlements(ENTITLEMENTS["premium"]),
    )
    pro = main_module._project_ticker_context_bundle_for_entitlements(
        canonical,
        symbol="TEST",
        source_entitlements=main_module._ticker_context_source_entitlements(ENTITLEMENTS["pro"]),
    )

    assert premium["signals_summary"]["items"]
    assert premium["confirmation_score_bundle"]["sources"]["signals"].get("locked") is not True
    assert premium["confirmation_score_bundle"]["sources"]["options_flow"]["locked"] is True
    assert premium["options_flow_summary"]["locked"] is True
    assert pro["confirmation_score_bundle"]["sources"]["options_flow"].get("locked") is not True
    assert pro["confirmation_score_bundle"]["sources"]["institutional_activity"].get("locked") is not True


def test_free_warms_one_canonical_bundle_that_a_pro_viewer_reuses(monkeypatch):
    main_module._TICKER_CONTEXT_BUNDLE_MEMORY_CACHE.clear()
    engine = _engine()
    with Session(engine) as db:
        counters = _mock_ticker_context_bundle_dependencies(monkeypatch, tier="free")
        free = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )
        monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: ENTITLEMENTS["pro"])
        pro = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )

    assert free["signals_summary"]["items"] == []
    assert free["source_entitlements"]["signals"]["locked"] is True
    assert pro["signals_summary"]["items"]
    assert pro["source_entitlements"]["signals"]["locked"] is False
    assert pro["source_entitlements"]["institutional_activity"]["locked"] is False
    assert counters == {"profile": 1, "signals": 1}
