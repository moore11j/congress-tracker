from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import FundamentalsCache, LeaderboardSnapshot, TickerContextBundleCache
from app.services import top_stocks
from app.services.confirmation_score import confirmation_band_for_score


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine, tables=[FundamentalsCache.__table__, LeaderboardSnapshot.__table__, TickerContextBundleCache.__table__])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def _bundle(symbol, score, direction="bullish"):
    return {"ticker": symbol, "lookback_days": 30, "score": score, "band": confirmation_band_for_score(score), "direction": direction, "status": "3-source confirmation", "source_count": 3}


def _row(symbol, score, **kwargs):
    return {"symbol": symbol, "company_name": symbol, "market_cap": 20_000_000_000, "sector": "Technology", "country": "United States", "confirmation": _bundle(symbol, score), **kwargs}


def _refresh(db, monkeypatch, rows, scores):
    def discover(_db):
        return rows
    monkeypatch.setattr(top_stocks, "cached_screener_rows", discover)
    monkeypatch.setattr(top_stocks, "build_ticker_confirmation_context", lambda _db, symbols: {"bundles": {s: _bundle(s, scores[s]) for s in symbols}})
    return top_stocks.refresh_top_stocks_leaderboard(db, now=datetime.now(timezone.utc) - timedelta(minutes=10))


def _cache(db, symbol, score, *, direction="bullish", segment="canonical", expired=False, version=None):
    from app.main import _TICKER_CONTEXT_BUNDLE_VERSION
    now = datetime.now(timezone.utc)
    db.add(TickerContextBundleCache(cache_key=f"ticker-context-bundle:v{version or _TICKER_CONTEXT_BUNDLE_VERSION}:{symbol}:30:all:3:{segment}", symbol=symbol, user_segment=segment, payload_json=json.dumps({"confirmation_score_bundle": _bundle(symbol, score, direction)}), generated_at=now, stale_after=now + timedelta(minutes=5), expires_at=now + timedelta(days=-1 if expired else 1)))
    db.commit()


def test_old_incompatible_snapshot_is_not_served(monkeypatch):
    with _session() as db:
        db.add(LeaderboardSnapshot(leaderboard_key="top_stocks", generated_at=datetime.now(timezone.utc), payload_json=json.dumps({"items": [{"symbol": "AMZN", "confirmation_score": 77}]})))
        db.commit()
        assert top_stocks.build_top_stocks_response(db)["items"] == []


def test_refresh_scores_before_qualification_and_persists_all_candidates(monkeypatch):
    with _session() as db:
        result = _refresh(db, monkeypatch, [_row("AMZN", 77), _row("NEW", 20), _row("OUT", 90)], {"AMZN": 100, "NEW": 85, "OUT": 19})
        assert [(r["symbol"], r["confirmation_score"]) for r in result["items"]] == [("AMZN", 100), ("NEW", 85)]
        assert result["filter_items"]["tech"] == result["items"]
        stored = json.loads(db.scalar(select(LeaderboardSnapshot)).payload_json)
        assert len(stored["candidate_rows"]) == 3
        assert top_stocks.build_top_stocks_response(db) == result
        assert "candidate_rows" not in result
        assert "confirmation_bundle" not in json.dumps(result)


def test_refresh_ranks_late_alphabet_stocks_beyond_screener_limit(monkeypatch):
    from app.entitlements import ENTITLEMENTS
    from app.services.ranking_access import project_ranking

    now = datetime.now(timezone.utc)
    early_symbols = [f"A{i:04}" for i in range(600)]
    symbols = early_symbols + ["META", "ZZZ"]
    batches = []

    def score(_db, batch):
        batches.append(batch)
        return {"bundles": {symbol: _bundle(symbol, {"META": 90, "ZZZ": 85}.get(symbol, 40)) for symbol in batch}}

    monkeypatch.setattr(top_stocks, "build_ticker_confirmation_context", score)
    with _session() as db:
        db.add_all(FundamentalsCache(symbol=symbol, provider="fmp", status="ok", fetched_at=now,
                                    sector="Technology", country="US") for symbol in symbols + ["[SYMBOL]", "%INVALID"])
        db.add(FundamentalsCache(symbol="FAILED", provider="fmp", status="failed", fetched_at=now))
        db.add(FundamentalsCache(symbol="OTHER", provider="other", status="ok", fetched_at=now))
        db.commit()
        result = top_stocks.refresh_top_stocks_leaderboard(db, now=now)
        assert len(batches) > 1
        assert all(len(batch) <= top_stocks.TOP_STOCKS_SCORE_BATCH_SIZE for batch in batches)
        assert [symbol for batch in batches for symbol in batch] == symbols
        stored = json.loads(db.scalar(select(LeaderboardSnapshot)).payload_json)
        assert len(stored["candidate_rows"]) == 602
        assert result["returned"] == 25
        assert [row["symbol"] for row in result["items"][:2]] == ["META", "ZZZ"]
        assert result["filter_items"]["tech"] == result["items"]
        assert result["filter_items"]["us"] == result["items"]
        served = top_stocks.build_top_stocks_response(db)
        assert served == result
        # Both the landing page and dashboard consume these shared projections.
        for tier in ("free", "premium", "pro"):
            projected = project_ranking(served, authenticated=True, stocks=True,
                                        entitlements=ENTITLEMENTS[tier], full=tier != "free")
            assert [(row["rank"], row["symbol"]) for row in projected["items"][:2]] == [(1, "META"), (2, "ZZZ")]
        preview = project_ranking(served, authenticated=False, stocks=True)
        assert [row["rank"] for row in preview["items"]] == [3, 4, 5]


def test_failed_later_scoring_batch_keeps_previous_complete_snapshot(monkeypatch):
    with _session() as db:
        _refresh(db, monkeypatch, [_row("OLD", 80)], {"OLD": 80})
        previous = db.scalar(select(LeaderboardSnapshot)).payload_json
        monkeypatch.setattr(top_stocks, "TOP_STOCKS_SCORE_BATCH_SIZE", 1)
        monkeypatch.setattr(top_stocks, "cached_screener_rows", lambda _db: [_row("NEW", 90), _row("ZZZ", 95)])
        monkeypatch.setattr(top_stocks, "build_ticker_confirmation_context",
                            lambda _db, symbols: {"bundles": {"NEW": _bundle("NEW", 90)} if symbols == ["NEW"] else {}})
        with pytest.raises(ValueError, match="Missing ticker confirmation for ZZZ"):
            top_stocks.refresh_top_stocks_leaderboard(db)
        db.expire_all()
        assert db.scalar(select(LeaderboardSnapshot)).payload_json == previous


def test_get_uses_ticker_cache_reranks_and_removes_nonqualifiers_without_scoring(monkeypatch):
    with _session() as db:
        _refresh(db, monkeypatch, [_row("AMZN", 77), _row("NEW", 50), _row("OUT", 90)], {"AMZN": 77, "NEW": 50, "OUT": 90})
        _cache(db, "AMZN", 100)
        _cache(db, "NEW", 95)
        _cache(db, "OUT", 59, direction="mixed")
        def forbidden(*args, **kwargs):
            raise AssertionError("GET must not calculate scores or write")
        monkeypatch.setattr(top_stocks, "cached_screener_rows", forbidden)
        monkeypatch.setattr(top_stocks, "build_ticker_confirmation_context", forbidden)
        monkeypatch.setattr(db, "commit", forbidden)
        result = top_stocks.build_top_stocks_response(db)
        for key in ("all", "tech", "large_cap", "us"):
            assert [(r["rank"], r["symbol"], r["confirmation_score"]) for r in result["filter_items"][key]] == [(1, "AMZN", 100), (2, "NEW", 95)]
        assert not db.dirty


@pytest.mark.parametrize("cache_args", [{"segment": "free"}, {"expired": True}, {"version": 7}])
def test_get_ignores_redacted_expired_or_obsolete_caches(monkeypatch, cache_args):
    with _session() as db:
        _refresh(db, monkeypatch, [_row("AMZN", 77)], {"AMZN": 100})
        _cache(db, "AMZN", 61, **cache_args)
        assert top_stocks.build_top_stocks_response(db)["items"][0]["confirmation_score"] == 100


def test_older_unexpired_ticker_cache_matches_what_ticker_currently_displays(monkeypatch):
    with _session() as db:
        _cache(db, "AMZN", 88)
        _refresh(db, monkeypatch, [_row("AMZN", 77)], {"AMZN": 100})
        snapshot = db.scalar(select(LeaderboardSnapshot))
        payload = json.loads(snapshot.payload_json)
        payload["candidate_rows"][0]["updated_at"] = (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat()
        snapshot.payload_json = json.dumps(payload)
        db.commit()
        assert top_stocks.build_top_stocks_response(db)["items"][0]["confirmation_score"] == 88


def test_daily_refresh_matches_real_ticker_calculation_and_tier_projection(monkeypatch):
    import app.main as main
    from app.entitlements import ENTITLEMENTS
    from test_ticker_signals_summary import _engine, _seed_score_contract_fixture
    engine = _engine()
    Base.metadata.create_all(bind=engine, tables=[LeaderboardSnapshot.__table__])
    with sessionmaker(bind=engine)() as db:
        _seed_score_contract_fixture(db)
        db.commit()
        symbols = ["AAPL", "MSTR", "NBIS"]
        monkeypatch.setattr(top_stocks, "cached_screener_rows", lambda *_args, **_kwargs: [_row(s, 1) for s in symbols])
        top_stocks.refresh_top_stocks_leaderboard(db)
        stored = json.loads(db.scalar(select(LeaderboardSnapshot)).payload_json)
        for row in stored["candidate_rows"]:
            actual = main._ticker_confirmation_context(db, row["symbol"])["confirmation_score_bundle"]
            expected = row["confirmation_bundle"]
            for key in ("score", "band", "direction"):
                assert actual[key] == expected[key]
        for tier in ("premium", "pro", "admin"):
            result = top_stocks.build_top_stocks_response(db, entitlements=ENTITLEMENTS[tier])
            for item in result["items"]:
                row = next(r for r in stored["candidate_rows"] if r["symbol"] == item["symbol"])
                projected = main._redact_locked_ticker_confirmation_sources(row["confirmation_bundle"], main._ticker_context_source_entitlements(ENTITLEMENTS[tier]))
                assert item["confirmation_score"] == projected["score"]
                assert item["confirmation_direction"] == projected["direction"]

def test_score_does_not_change_with_paginated_signal_cards():
    import app.main as main
    bundle = {**_bundle("AMZN", 100), "score_context_version": "ticker_confirmation_30d_v1"}
    card = {"status": "active", "direction": "bearish", "recent_count": 3, "latest_score": 90}
    assert main._merge_authorized_signal_context_into_confirmation_bundle(bundle, card, {}) == bundle
    assert main._merge_fresh_public_contexts_into_confirmation_bundle(bundle, {"signals": card}) == bundle


def test_shared_score_inputs_do_not_hydrate_incomplete_fundamentals(monkeypatch):
    import app.main as main
    from app.models import FundamentalsCache
    from test_ticker_signals_summary import _engine
    engine = _engine()
    def forbidden(*args, **kwargs):
        raise AssertionError("Scoring may only read stored inputs")
    monkeypatch.setattr(main, "fetch_fundamentals_for_symbol", forbidden)
    monkeypatch.setattr(main, "_cached_ticker_fundamentals_row", forbidden)
    monkeypatch.setattr(main, "enqueue_data_enrichment_job", forbidden)
    with sessionmaker(bind=engine)() as db:
        db.add(FundamentalsCache(symbol="TEST", provider="fmp", status="ok", fetched_at=datetime.now(timezone.utc) - timedelta(days=10)))
        db.commit()
        result = main.build_ticker_signals_summary_contexts_from_cache("TEST", db=db)
        assert result["fundamentals"]
        assert not db.dirty


def test_obsolete_scoring_snapshot_is_not_presented_as_current(monkeypatch):
    with _session() as db:
        _refresh(db, monkeypatch, [_row("BA", 100)], {"BA": 100})
        snapshot = db.scalar(select(LeaderboardSnapshot))
        payload = json.loads(snapshot.payload_json)
        payload["score_context_version"] = "ticker_confirmation_30d_v1"
        snapshot.payload_json = json.dumps(payload)
        db.commit()
        assert top_stocks.build_top_stocks_response(db)["items"] == []


def test_score_then_market_cap_then_average_volume_break_ties():
    high = _row("HIGH", 95, market_cap=1, avg_volume=1)
    large = _row("ZZZ", 80, market_cap=30_000_000_000, avg_volume=1)
    liquid = _row("LIQUID", 80, avg_volume=2_000_000)
    steady = _row("STEADY", 80, avg_volume=1_000_000)
    clustered = _row("AAA", 80, market_cap=1_000_000_000, avg_volume=10_000_000,
                     ranking_context={"baseline_score": 70, "insider_cluster_count": 3, "strategy_entries": 5})
    clustered["confirmation"]["sources"] = {"congress": {"present": True, "direction": "bullish"}}
    payload = top_stocks._ranked_payload([steady, liquid, clustered, large, high], generated_at="2026-09-25T20:00:00Z")
    expected = ["HIGH", "ZZZ", "LIQUID", "STEADY", "AAA"]
    for key in ("all", "us", "tech"):
        assert [(item["rank"], item["symbol"]) for item in payload["filter_items"][key]] == list(enumerate(expected, 1))
    assert [item["symbol"] for item in payload["filter_items"]["large_cap"]] == ["ZZZ", "LIQUID", "STEADY"]
    assert payload["items"][-1]["why_ranked"] == "Insider cluster with Congress confirmation"
    assert "baseline_score" not in json.dumps(payload)
    assert "idea_score" not in json.dumps(payload)


@pytest.mark.parametrize("missing", [None, 0, -1, float("nan"), float("inf"), "unknown", True])
def test_missing_market_data_sorts_last_and_exact_ties_use_ticker(missing):
    rows = [
        _row("MISSING", 80, market_cap=missing, avg_volume=1_000_000),
        _row("ZULU", 80, market_cap=100, avg_volume=10),
        _row("NO_VOLUME", 80, market_cap=100, avg_volume=missing),
        _row("ALPHA", 80, market_cap=100, avg_volume=10),
    ]
    for candidates in (rows, list(reversed(rows))):
        result = top_stocks._ranked_payload(candidates, generated_at="2026-10-03T20:00:00Z")
        assert [item["symbol"] for item in result["items"]] == ["ALPHA", "ZULU", "NO_VOLUME", "MISSING"]


def test_stored_candidates_and_new_ticker_scores_use_market_tiebreaks(monkeypatch):
    with _session() as db:
        rows = [_row("AAA", 80, market_cap=1_000_000_000),
                _row("ZZZ", 80, avg_volume=1_000_000),
                _row("YYY", 80, avg_volume=2_000_000)]
        refreshed = _refresh(db, monkeypatch, rows, {row["symbol"]: 80 for row in rows})
        assert [item["symbol"] for item in refreshed["items"]] == ["YYY", "ZZZ", "AAA"]
        assert top_stocks.build_top_stocks_response(db) == refreshed
        _cache(db, "AAA", 90)
        assert [item["symbol"] for item in top_stocks.build_top_stocks_response(db)["items"]] == ["AAA", "YYY", "ZZZ"]


def test_tier_evidence_projection_does_not_reorder_canonical_ranks():
    first = _row("FIRST", 95, visible_confirmation=_bundle("FIRST", 65))
    second = _row("SECOND", 85, visible_confirmation=_bundle("SECOND", 80))
    payload = top_stocks._ranked_payload([second, first], generated_at="2026-09-25T20:00:00Z")
    assert [(item["rank"], item["symbol"], item["confirmation_score"]) for item in payload["items"]] == [(1, "FIRST", 65), (2, "SECOND", 80)]


def test_relative_ranking_keeps_absolute_strength_labels_and_requires_corroboration():
    developing = _row("DEVELOPING", 38)
    too_thin = _row("THIN", 19)
    single = _row("SINGLE", 20)
    single["confirmation"]["source_count"] = 1
    strong = _row("STRONG", 70)
    payload = top_stocks._ranked_payload([too_thin, single, developing, strong], generated_at="2026-09-26T20:00:00Z")
    assert [row["symbol"] for row in payload["items"]] == ["STRONG", "DEVELOPING"]
    assert payload["items"][0]["why_ranked"].startswith("Strong")
    assert payload["items"][1]["why_ranked"].startswith("Developing")
