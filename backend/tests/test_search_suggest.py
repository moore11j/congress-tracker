from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, Member, Security, TickerMeta, Watchlist, WatchlistItem
import app.services.search_suggest as search_suggest_module
from app.services.search_suggest import search_suggestions


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def test_search_suggest_exact_symbol_ranks_first_and_respects_limit():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="AAPL", name="Apple Inc.", asset_class="stock", sector="Technology"),
                Security(symbol="AAP", name="Advance Auto Parts", asset_class="stock", sector="Consumer"),
                TickerMeta(symbol="AAPL", company_name="Apple Inc.", exchange="NASDAQ"),
            ]
        )
        db.commit()

        payload = search_suggestions(db, "aapl", limit=1)

        assert len(payload["items"]) == 1
        assert payload["items"][0]["kind"] == "ticker"
        assert payload["items"][0]["symbol"] == "AAPL"
        assert payload["items"][0]["href"] == "/ticker/AAPL"
    finally:
        db.close()


def test_search_suggest_exact_lowercase_ticker_resolves_from_profile_cache_only():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        db.add(TickerMeta(symbol="MSFT", company_name="Microsoft Corporation", exchange="NASDAQ"))
        db.commit()

        payload = search_suggestions(db, "msft", limit=5)

        assert payload["items"] == [
            {
                "kind": "ticker",
                "id": "MSFT",
                "symbol": "MSFT",
                "label": "Microsoft Corporation",
                "subtitle": "Ticker - Microsoft Corporation - NASDAQ",
                "href": "/ticker/MSFT",
            }
        ]
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_exact_uppercase_ticker_resolves_from_security_cache():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        db.add(Security(symbol="NVDA", name="NVIDIA Corporation", asset_class="stock", sector="Technology"))
        db.commit()

        item = search_suggestions(db, "NVDA", limit=5)["items"][0]

        assert item["kind"] == "ticker"
        assert item["symbol"] == "NVDA"
        assert item["label"] == "NVIDIA Corporation"
        assert item["href"] == "/ticker/NVDA"
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_profile_cache_ticker_does_not_require_event_rows():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        db.add(TickerMeta(symbol="AAPL", company_name="Apple Inc.", exchange="NASDAQ"))
        db.commit()

        item = search_suggestions(db, "aapl", limit=5)["items"][0]

        assert item["kind"] == "ticker"
        assert item["symbol"] == "AAPL"
        assert item["label"] == "Apple Inc."
        assert item["href"] == "/ticker/AAPL"
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_exact_share_class_query_uses_cached_variant_symbol():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        db.add(TickerMeta(symbol="BRK-B", company_name="Berkshire Hathaway Inc.", exchange="NYSE"))
        db.commit()

        item = search_suggestions(db, "brk.b", limit=5)["items"][0]

        assert item["kind"] == "ticker"
        assert item["symbol"] == "BRK-B"
        assert item["href"] == "/ticker/BRK-B"
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_ticker_pattern_fallback_returns_route_and_queues_enrichment(monkeypatch):
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(search_suggest_module, "enqueue_data_enrichment_job", lambda **kwargs: calls.append(kwargs) or True)
    try:
        item = search_suggestions(db, "xqzz", limit=5)["items"][0]

        assert item == {
            "kind": "ticker",
            "id": "XQZZ",
            "symbol": "XQZZ",
            "label": "Ticker: XQZZ",
            "subtitle": "Ticker - XQZZ",
            "href": "/ticker/XQZZ",
        }
        assert [call["job_type"] for call in calls] == ["ticker_meta", "profile"]
        assert {call["symbol"] for call in calls} == {"XQZZ"}
        assert all(call["source"] == "search" for call in calls)
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_invalid_random_query_still_returns_no_matches(monkeypatch):
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(search_suggest_module, "enqueue_data_enrichment_job", lambda **kwargs: calls.append(kwargs) or True)
    try:
        payload = search_suggestions(db, "not a ticker", limit=5)

        assert payload["items"] == []
        assert calls == []
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_company_name_uses_profile_cache_without_security_rows():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        db.add(TickerMeta(symbol="MSFT", company_name="Microsoft Corporation", exchange="NASDAQ"))
        db.commit()

        item = search_suggestions(db, "Microsoft", limit=5)["items"][0]

        assert item["kind"] == "ticker"
        assert item["symbol"] == "MSFT"
        assert item["label"] == "Microsoft Corporation"
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_member_name_beats_lightweight_ticker_fallback():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        db.add(Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"))
        db.commit()

        items = search_suggestions(db, "nancy", limit=5)["items"]

        assert items[0]["kind"] == "member"
        assert items[0]["label"] == "Nancy Pelosi"
        assert all(item["href"] != "/ticker/NANCY" for item in items)
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_includes_low_priority_event_badge_result():
    db = _db()
    search_suggest_module._anonymous_suggestion_cache.clear()
    try:
        now = datetime.now(timezone.utc)
        db.add_all(
            [
                Security(symbol="ORBT", name="Orbital Systems Inc.", asset_class="stock", sector="Industrial"),
                Event(
                    event_type="government_contract",
                    ts=now,
                    event_date=now,
                    symbol="ORBT",
                    source="test",
                    payload_json="{}",
                ),
            ]
        )
        db.commit()

        items = search_suggestions(db, "Orbital", limit=8)["items"]

        assert any(item["kind"] == "event" and item["symbol"] == "ORBT" and item["href"] == "/feed?symbol=ORBT" for item in items)
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_symbol_prefix_beats_name_contains():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="TSM", name="Taiwan Semiconductor Manufacturing", asset_class="stock", sector="Technology"),
                Security(symbol="ABC", name="TSM Holdings", asset_class="stock", sector="Industrial"),
            ]
        )
        db.commit()

        items = search_suggestions(db, "ts", limit=5)["items"]

        assert items[0]["symbol"] == "TSM"
    finally:
        db.close()


def test_search_suggest_includes_members_after_tickers():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="PELO", name="Peloton Adjacent", asset_class="stock", sector=None),
                Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"),
                InsiderTransaction(
                    source="test",
                    external_id="insider-1",
                    filing_date=datetime.now(timezone.utc).date(),
                    transaction_date=datetime.now(timezone.utc).date(),
                    payload_json="{}",
                    insider_name="Jane Pelosi",
                    reporting_cik="0001234567",
                    symbol="AAPL",
                ),
            ]
        )
        db.commit()

        items = search_suggestions(db, "pel", limit=8)["items"]
        kinds = [item["kind"] for item in items]

        assert "ticker" in kinds
        assert "member" in kinds
    finally:
        db.close()


def test_search_suggest_finds_normalized_form4_insider():
    db = _db()
    try:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.add_all(
            [
                InsiderTransactionNormalized(
                    accession_number="0000320193-26-000001",
                    issuer_name="Apple Inc.",
                    ticker_normalized="AAPL",
                    reporting_owner_cik="0001214156",
                    reporting_owner_name="Tim Cook",
                    officer_title="Chief Executive Officer",
                    transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                    filing_date=datetime(2026, 4, 2, tzinfo=timezone.utc).date(),
                    normalized_hash="tim-cook-aapl-1",
                ),
                InsiderTransactionNormalized(
                    accession_number="0000320187-26-000001",
                    issuer_name="Nike Inc.",
                    ticker_normalized="NKE",
                    reporting_owner_cik="0001214156",
                    reporting_owner_name="Tim Cook",
                    is_director=True,
                    transaction_date=datetime(2026, 4, 3, tzinfo=timezone.utc).date(),
                    filing_date=datetime(2026, 4, 4, tzinfo=timezone.utc).date(),
                    normalized_hash="tim-cook-nke-1",
                ),
            ]
        )
        db.commit()

        items = search_suggestions(db, "tim cook", limit=8)["items"]

        insider_items = [item for item in items if item["kind"] == "insider"]
        insider_hrefs = {item["href"] for item in insider_items}
        assert "/insider/tim-cook-0001214156?issuer=AAPL" in insider_hrefs
        assert "/insider/tim-cook-0001214156?issuer=NKE" in insider_hrefs
        assert any(item["symbol"] == "AAPL" and "Chief Executive Officer" in str(item["subtitle"]) for item in insider_items)
        assert any(item["symbol"] == "NKE" and "Nike Inc." in str(item["subtitle"]) for item in insider_items)
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_finds_legacy_payload_insider_with_null_name():
    db = _db()
    try:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.add_all(
            [
                InsiderTransaction(
                    source="fmp",
                    external_id="tim-cook-aapl-payload",
                    symbol="AAPL",
                    reporting_cik="0001214156",
                    insider_name=None,
                    transaction_type="M-Exempt",
                    role=None,
                    transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                    filing_date=datetime(2026, 4, 3, tzinfo=timezone.utc).date(),
                    shares=131576,
                    price=0,
                    payload_json=json.dumps(
                        {
                            "reportingName": "COOK TIMOTHY D",
                            "reportingCik": "0001214156",
                            "symbol": "AAPL",
                            "typeOfOwner": "director, officer: Chief Executive Officer",
                        }
                    ),
                ),
                InsiderTransaction(
                    source="fmp",
                    external_id="tim-cook-nke-payload",
                    symbol="NKE",
                    reporting_cik="0001214156",
                    insider_name=None,
                    transaction_type="P-Purchase",
                    role=None,
                    transaction_date=datetime(2026, 4, 10, tzinfo=timezone.utc).date(),
                    filing_date=datetime(2026, 4, 14, tzinfo=timezone.utc).date(),
                    shares=25000,
                    price=42.43,
                    payload_json=json.dumps(
                        {
                            "reportingName": "COOK TIMOTHY D",
                            "reportingCik": "0001214156",
                            "symbol": "NKE",
                            "typeOfOwner": "director",
                        }
                    ),
                ),
            ]
        )
        db.commit()

        items = search_suggestions(db, "Tim Cook", limit=8)["items"]

        insider_items = [item for item in items if item["kind"] == "insider"]
        insider_hrefs = {item["href"] for item in insider_items}
        assert "/insider/tim-cook-0001214156?issuer=AAPL" in insider_hrefs
        assert "/insider/tim-cook-0001214156?issuer=NKE" in insider_hrefs
        assert any(item["symbol"] == "AAPL" and "Chief Executive Officer" in str(item["subtitle"]) for item in insider_items)
        assert any(item["symbol"] == "NKE" and "Director" in str(item["subtitle"]) for item in insider_items)
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_suggest_finds_company_name_typo():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="MSFT", name="Microsoft Corporation", asset_class="stock", sector="Technology"),
                Security(symbol="MSTR", name="MicroStrategy Incorporated", asset_class="stock", sector="Technology"),
                TickerMeta(symbol="MSFT", company_name="Microsoft Corporation", exchange="NASDAQ"),
            ]
        )
        db.commit()

        items = search_suggestions(db, "microsft", limit=5)["items"]

        assert items[0]["kind"] == "ticker"
        assert items[0]["symbol"] == "MSFT"
    finally:
        db.close()


def test_search_suggest_finds_member_last_name_typo():
    db = _db()
    try:
        db.add_all(
            [
                Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"),
                Member(bioguide_id="P000608", first_name="Gary", last_name="Palmer", chamber="house", party="R", state="AL"),
            ]
        )
        db.commit()

        items = search_suggestions(db, "pelsoi", limit=5)["items"]

        assert items[0]["kind"] == "member"
        assert items[0]["label"] == "Nancy Pelosi"
    finally:
        db.close()


def test_search_suggest_personalizes_watchlist_symbols():
    db = _db()
    try:
        tesla = Security(symbol="TSLA", name="Tesla Inc.", asset_class="stock", sector="Consumer")
        test = Security(symbol="TEST", name="Test Industries", asset_class="stock", sector="Industrial")
        db.add_all([tesla, test])
        db.flush()
        watchlist = Watchlist(id=101, name="My AI basket", owner_user_id=42)
        db.add(watchlist)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=tesla.id))
        db.commit()

        anonymous_items = search_suggestions(db, "tes", limit=5)["items"]
        personalized_items = search_suggestions(db, "tes", limit=5, user_id=42)["items"]

        assert anonymous_items[0]["symbol"] == "TEST"
        assert personalized_items[0]["symbol"] == "TSLA"
    finally:
        db.close()


def test_anonymous_search_suggest_reuses_short_ttl_cache(monkeypatch):
    db = _db()
    try:
        search_suggest_module._anonymous_suggestion_cache.clear()
        calls = {"ticker": 0}

        def fake_ticker_suggestions(*args, **kwargs):
            calls["ticker"] += 1
            return [
                {
                    "kind": "ticker",
                    "id": "MSFT",
                    "symbol": "MSFT",
                    "label": "Microsoft Corporation",
                    "subtitle": "Ticker - Microsoft Corporation",
                    "href": "/ticker/MSFT",
                    "score": 1000,
                }
            ]

        monkeypatch.setattr(search_suggest_module, "_ticker_suggestions", fake_ticker_suggestions)
        monkeypatch.setattr(search_suggest_module, "_member_suggestions", lambda *args, **kwargs: [])
        monkeypatch.setattr(search_suggest_module, "_insider_suggestions", lambda *args, **kwargs: [])
        monkeypatch.setattr(search_suggest_module, "_agency_suggestions", lambda *args, **kwargs: [])

        first = search_suggestions(db, "ms", limit=5)
        second = search_suggestions(db, "ms", limit=5)

        assert first == second
        assert calls["ticker"] == 1
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()
