from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import IndexMembership, UserAccount, Watchlist
from app.services import index_memberships
from app.services.index_memberships import (
    FmpIndexMembershipProvider,
    MembershipDataset,
    MembershipRecord,
    WikipediaIndexMembershipProvider,
    active_index_membership_snapshot,
    parse_wikipedia_membership_payload,
    refresh_index_memberships_from_dataset,
    refresh_index_memberships_from_provider,
    validate_membership_dataset,
)
from app.services.market_pressure import build_market_pressure_response


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine, tables=[IndexMembership.__table__, UserAccount.__table__, Watchlist.__table__])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


@pytest.fixture
def db():
    Session = _session_factory()
    session = Session()
    try:
        yield session
    finally:
        session.close()


def _rows(symbols: list[str], *, sector: str = "Technology", symbol_header: str = "Symbol") -> str:
    sectors = ["Technology", "Financials", "Energy", "Health Care"]
    return "\n".join(
        f"<tr><td>{symbol}</td><td>{symbol} Corp</td><td>{sectors[idx % len(sectors)]}</td><td>Software</td></tr>"
        for idx, symbol in enumerate(symbols)
    )


def _sp_symbols() -> list[str]:
    required = ["NVDA", "AAPL", "MSFT", "JPM", "XOM", "BRK.B", "BF.B"]
    generated = [f"SP{i:03d}" for i in range(1, 504 - len(required))]
    return required + generated


def _nasdaq_symbols() -> list[str]:
    required = ["NVDA", "AAPL", "MSFT", "AMZN"]
    generated = [f"NQ{i:03d}" for i in range(1, 102 - len(required))]
    return required + generated


def _payload(index_code: str, symbols: list[str], *, rev: int = 123, title: str | None = None) -> dict:
    if index_code == "sp500":
        header = "<tr><th>Symbol[1]</th><th>Security</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>"
        body = _rows(symbols)
        table = f"<table><tr><th>Date</th><th>Added</th></tr><tr><td>Old</td><td>Old Co</td></tr></table><table class='wikitable sortable'>{header}{body}</table>"
        display = title or "List of S&amp;P 500 companies"
    else:
        header = "<tr><th>Ticker</th><th>Company</th><th>Sector</th></tr>"
        sectors = ["Technology", "Financials", "Consumer Discretionary", "Health Care"]
        body = "\n".join(f"<tr><td>{symbol}</td><td>{symbol} Corp</td><td>{sectors[idx % len(sectors)]}</td></tr>" for idx, symbol in enumerate(symbols))
        table = f"<table class='wikitable'><tr><th>Year</th><th>Return</th></tr><tr><td>2024</td><td>1%</td></tr></table><table>{header}{body}</table>"
        display = title or "Nasdaq-100"
    return {"parse": {"revid": rev, "displaytitle": display, "text": table}}


def _dataset(index_code: str, symbols: list[str], *, rev: str = "123", source: str = "wikipedia") -> MembershipDataset:
    return MembershipDataset(
        index_code=index_code,
        records=[
            MembershipRecord(symbol=symbol, raw_symbol=symbol, company_name=f"{symbol} Corp", sector=["Technology", "Financials", "Energy"][idx % 3])
            for idx, symbol in enumerate(symbols)
        ],
        source=source,
        source_kind=index_memberships.WIKIPEDIA_SOURCE_KIND if source == "wikipedia" else "fixture",
        source_page=index_memberships.WIKIPEDIA_PAGES[index_code],
        resolved_source_title="Fixture",
        source_revision_id=rev,
        source_as_of=date(2026, 7, 16),
        retrieved_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        parser_version=index_memberships.WIKIPEDIA_PARSER_VERSION,
        parsed_count=len(symbols),
    )


def test_mediawiki_sp500_response_parses_correct_table_and_normalizes_share_classes():
    dataset = parse_wikipedia_membership_payload("sp500", _payload("sp500", _sp_symbols()), source_page="List_of_S%26P_500_companies")
    assert dataset.parsed_count == 503
    assert dataset.source == "wikipedia"
    assert dataset.source_kind == index_memberships.WIKIPEDIA_SOURCE_KIND
    assert dataset.source_revision_id == "123"
    assert "BRK.B" in dataset.symbols
    assert "BF.B" in dataset.symbols
    assert validate_membership_dataset(index_memberships.INDEX_UNIVERSES["sp500"], dataset) is None


def test_mediawiki_nasdaq_response_parses_current_components_not_historical_table():
    dataset = parse_wikipedia_membership_payload("nasdaq100", _payload("nasdaq100", _nasdaq_symbols(), rev=456), source_page="Nasdaq-100")
    assert dataset.parsed_count == 101
    assert dataset.resolved_source_title == "Nasdaq-100"
    assert "NVDA" in dataset.symbols
    assert "AMZN" in dataset.symbols
    assert validate_membership_dataset(index_memberships.INDEX_UNIVERSES["nasdaq100"], dataset) is None


def test_wikipedia_parser_rejects_bad_payloads_and_duplicate_symbols():
    with pytest.raises(ValueError, match="wikipedia_missing_revision_id"):
        parse_wikipedia_membership_payload("sp500", {"parse": {"displaytitle": "x", "text": "<table></table>"}}, source_page="x")
    with pytest.raises(ValueError, match="wikipedia_api_error"):
        parse_wikipedia_membership_payload("sp500", {"error": {"code": "bad"}}, source_page="x")
    with pytest.raises(ValueError, match="wikipedia_component_table_not_found"):
        parse_wikipedia_membership_payload("sp500", {"parse": {"revid": 1, "displaytitle": "x", "text": "<p>No table</p>"}}, source_page="x")
    with pytest.raises(ValueError, match="duplicate_normalized_symbols"):
        parse_wikipedia_membership_payload("sp500", _payload("sp500", ["AAPL", "AAPL"] + _sp_symbols()[2:]), source_page="x")


def test_suspiciously_small_datasets_are_rejected():
    sp_dataset = parse_wikipedia_membership_payload("sp500", _payload("sp500", ["NVDA", "AAPL", "MSFT", "JPM", "XOM"]), source_page="x")
    nq_dataset = parse_wikipedia_membership_payload("nasdaq100", _payload("nasdaq100", ["NVDA", "AAPL", "MSFT", "AMZN"]), source_page="x")
    assert validate_membership_dataset(index_memberships.INDEX_UNIVERSES["sp500"], sp_dataset) == "membership_count_too_low"
    assert validate_membership_dataset(index_memberships.INDEX_UNIVERSES["nasdaq100"], nq_dataset) == "membership_count_too_low"


def test_wikipedia_provider_sends_user_agent_and_handles_redirected_title():
    seen = {}

    class Response:
        status_code = 200

        def json(self):
            return _payload("nasdaq100", _nasdaq_symbols(), title="NASDAQ-100")

    class Session:
        def get(self, url, params, headers, timeout):
            seen.update({"url": url, "params": params, "headers": headers, "timeout": timeout})
            return Response()

    dataset = WikipediaIndexMembershipProvider(session=Session()).fetch_memberships("nasdaq100")
    assert seen["url"] == index_memberships.WIKIPEDIA_API_URL
    assert seen["params"]["redirects"] == "1"
    assert "WalnutMarkets-IndexMembership" in seen["headers"]["User-Agent"]
    assert dataset.resolved_source_title == "NASDAQ-100"


def test_fmp_402_is_restricted_and_preserves_existing_memberships(db, monkeypatch):
    original = refresh_index_memberships_from_dataset(db, _dataset("sp500", _sp_symbols(), rev="old", source="fixture"))
    assert original.status == "ok"

    def restricted(_code):
        raise index_memberships.FMPSubscriptionRestrictedError("restricted")

    monkeypatch.setattr(index_memberships, "fetch_index_constituents", restricted)
    result = refresh_index_memberships_from_provider(db, "sp500", source="fmp")
    snapshot = active_index_membership_snapshot(db, "sp500")
    assert result.status == "restricted"
    assert result.reason == "provider_restricted_non_retryable"
    assert snapshot.membership_count == 503


def test_activation_records_wikipedia_metadata_and_matching_revision_is_unchanged(db):
    first = refresh_index_memberships_from_dataset(db, _dataset("nasdaq100", _nasdaq_symbols(), rev="456"))
    second = refresh_index_memberships_from_dataset(db, _dataset("nasdaq100", _nasdaq_symbols(), rev="456"))
    snapshot = active_index_membership_snapshot(db, "nasdaq100")
    assert first.status == "ok"
    assert second.status == "unchanged"
    assert snapshot.source == "wikipedia"
    assert snapshot.source_kind == index_memberships.WIKIPEDIA_SOURCE_KIND
    assert snapshot.source_revision_id == "456"
    assert snapshot.supported is True


def test_failed_activation_preserves_prior_records(db, monkeypatch):
    refresh_index_memberships_from_dataset(db, _dataset("sp500", _sp_symbols(), rev="old", source="fixture"))

    def fail_commit():
        raise RuntimeError("commit failed")

    monkeypatch.setattr(db, "commit", fail_commit)
    with pytest.raises(RuntimeError):
        refresh_index_memberships_from_dataset(db, _dataset("sp500", _sp_symbols()[:-1] + ["NEW"], rev="new"))
    active = db.query(IndexMembership).filter_by(index_code="sp500", is_active=True).all()
    assert sorted(row.symbol for row in active) == sorted(_sp_symbols())


def test_market_pressure_request_path_makes_no_wikipedia_calls(db, monkeypatch):
    user = UserAccount(email="owner@example.com")
    db.add(user)
    db.commit()

    def forbidden_provider(*_args, **_kwargs):
        raise AssertionError("request path must not call membership providers")

    monkeypatch.setattr(WikipediaIndexMembershipProvider, "fetch_memberships", forbidden_provider)
    response = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, _symbols: {},
    )
    assert response["universe"] == "watchlist"
