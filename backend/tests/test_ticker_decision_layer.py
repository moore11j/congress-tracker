from __future__ import annotations

from app.services.ticker_decision_layer import build_ticker_decision_layer


def _source(
    *,
    present: bool,
    direction: str = "neutral",
    label: str = "Source",
    status: str = "active",
    freshness_days: int | None = 1,
    locked: bool = False,
) -> dict:
    return {
        "present": present,
        "direction": direction,
        "strength": 70 if present else 0,
        "quality": 70 if present else 0,
        "freshness_days": freshness_days,
        "label": label,
        "summary": label,
        "status": status,
        "locked": locked,
    }


def _bundle(**sources: dict) -> dict:
    merged = {
        "price_volume": _source(present=False, label="No price confirmation"),
        "fundamentals": _source(present=False, label="Fundamentals unavailable", status="unavailable"),
        "insiders": _source(present=False, label="Insiders inactive"),
        "congress": _source(present=False, label="Congress inactive"),
        "signals": _source(present=False, label="Signals inactive"),
        "government_contracts": _source(present=False, label="No government contracts"),
        "options_flow": _source(present=False, label="Options flow locked", status="pro_locked", locked=True),
        "institutional_activity": _source(present=False, label="Institutional locked", status="pro_locked", locked=True),
        "macro_positioning": _source(present=False, label="Macro locked", status="pro_locked", locked=True),
    }
    merged.update(sources)
    return {
        "ticker": "MU",
        "score": 70,
        "band": "strong",
        "direction": "bearish",
        "status": "2-source bearish confirmation",
        "explanation": "Bearish evidence outweighs bullish evidence.",
        "sources": merged,
        "history": [],
    }


def test_decision_layer_does_not_fabricate_changes_without_history_or_events():
    payload = build_ticker_decision_layer(
        "MU",
        confirmation_bundle=_bundle(
            price_volume=_source(present=True, direction="bearish", label="Bearish tape"),
            fundamentals=_source(present=True, direction="bullish", label="Fundamental strength"),
        ),
        source_contexts={
            "price_volume": {
                "status": "active",
                "direction": "bearish",
                "latest_date": "2026-07-17",
                "macd": {"status": "ok", "signal": "bearish", "message": "MACD below signal"},
            },
            "fundamentals": {"status": "bullish"},
        },
        generated_at="2026-07-20T00:00:00Z",
    )

    assert payload["confirmation"]["score"] == 70
    assert payload["summary"] == "Fundamentals look supportive, but price and volume still lean negative."
    assert payload["confirmation"]["history"] == []
    assert payload["what_changed"] == []
    assert not any("From " in item["description"] for item in payload["what_changed"])


def test_decision_layer_uses_real_score_history_when_supplied():
    bundle = _bundle(price_volume=_source(present=True, direction="bearish", label="Bearish tape"))
    bundle["history"] = [
        {"date": "2026-07-01", "score": 82},
        {"date": "2026-07-18", "score": 70},
    ]

    payload = build_ticker_decision_layer(
        "MU",
        confirmation_bundle=bundle,
        source_contexts={},
        generated_at="2026-07-20T00:00:00Z",
    )

    assert payload["confirmation"]["history"] == bundle["history"]
    assert payload["what_changed"][0]["title"] == "Confirmation score dropped"
    assert payload["what_changed"][0]["description"] == "From 82 to 70"


def test_decision_layer_excludes_locked_pro_sources_from_premium_payload():
    payload = build_ticker_decision_layer(
        "AAPL",
        confirmation_bundle=_bundle(
            price_volume=_source(present=True, direction="bullish", label="Bullish tape"),
            institutional_activity=_source(
                present=True,
                direction="bullish",
                label="Institutional accumulation",
                status="pro_locked",
                locked=True,
            ),
            options_flow=_source(
                present=True,
                direction="bullish",
                label="Bullish options flow",
                status="pro_locked",
                locked=True,
            ),
        ),
        source_contexts={},
        generated_at="2026-07-20T00:00:00Z",
    )

    titles = {item["title"] for item in payload["catalysts"]}
    assert "Reported institutional accumulation" not in titles
    assert "Bullish options flow" not in titles
    assert all("requires" not in item["description"].lower() for item in payload["catalysts"])


def test_decision_layer_reports_unavailable_as_unavailable_not_locked():
    payload = build_ticker_decision_layer(
        "EMPTY",
        confirmation_bundle=_bundle(
            fundamentals=_source(present=False, label="Fundamentals unavailable", status="unavailable", locked=False),
        ),
        source_contexts={},
        generated_at="2026-07-20T00:00:00Z",
    )

    assert "Fundamentals unavailable." in payload["missing_data_notes"]
    assert not any("locked" in note.lower() for note in payload["missing_data_notes"])
