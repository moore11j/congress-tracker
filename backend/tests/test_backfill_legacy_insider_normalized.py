from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.backfill_legacy_insider_normalized import backfill_legacy_insider_normalized
from app.db import Base
from app.models import InsiderTransaction, InsiderTransactionNormalized, SecForm4Filing


def _session_factory(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr("app.backfill_legacy_insider_normalized.engine", engine)
    monkeypatch.setattr("app.backfill_legacy_insider_normalized.SessionLocal", SessionLocal)
    return SessionLocal


def _legacy_row(row_id: int = 1) -> InsiderTransaction:
    return InsiderTransaction(
        id=row_id,
        source="fmp",
        external_id=f"legacy-{row_id}",
        symbol="TSM",
        reporting_cik="0002114023",
        insider_name=None,
        transaction_type="P-Purchase",
        role=None,
        ownership="D",
        transaction_date=date(2026, 5, 19),
        filing_date=date(2026, 5, 19),
        shares=1000.0,
        price=69.98,
        payload_json=json.dumps(
            {
                "companyCik": "0001046179",
                "reportingCik": "0002114023",
                "reportingName": "Tien Bor-Zen",
                "securityName": "Common Shares (2330.TW)",
                "symbol": "TSM",
                "transactionDate": "2026-05-19",
                "filingDate": "2026-05-19",
                "transactionType": "P-Purchase",
                "typeOfOwner": "officer: VP",
                "securitiesTransacted": 1000,
                "price": 69.98,
                "url": "https://www.sec.gov/Archives/edgar/data/1046179/000104617926000285/0001046179-26-000285-index.htm",
            }
        ),
    )


def test_backfill_legacy_insider_normalized_dry_run_does_not_write(monkeypatch):
    SessionLocal = _session_factory(monkeypatch)
    with SessionLocal() as db:
        db.add(_legacy_row())
        db.commit()

    report = backfill_legacy_insider_normalized(apply=False)

    assert report["scanned"] == 1
    assert report["inserted_transactions"] == 1
    with SessionLocal() as db:
        assert db.query(InsiderTransactionNormalized).count() == 0
        assert db.query(SecForm4Filing).count() == 0


def test_backfill_legacy_insider_normalized_populates_identity_and_role(monkeypatch):
    SessionLocal = _session_factory(monkeypatch)
    with SessionLocal() as db:
        db.add(_legacy_row())
        db.commit()

    report = backfill_legacy_insider_normalized(apply=True)

    assert report["scanned"] == 1
    assert report["inserted_transactions"] == 1
    assert report["inserted_filings"] == 1
    with SessionLocal() as db:
        row = db.query(InsiderTransactionNormalized).one()
        filing = db.query(SecForm4Filing).one()
        relationship = json.loads(row.owner_relationship_json)
        assert row.accession_number == "0001046179-26-000285"
        assert row.ticker_normalized == "TSM"
        assert row.reporting_owner_name == "Tien Bor-Zen"
        assert row.reporting_owner_cik == "0002114023"
        assert row.transaction_type_normalized == "open_market_purchase"
        assert row.transaction_code == "P"
        assert row.is_officer is True
        assert row.officer_title == "VP"
        assert relationship["raw"] == "officer: VP"
        assert row.value == 69980.0
        assert filing.parser_version == "legacy_fmp_insider_v1"


def test_backfill_legacy_insider_normalized_is_idempotent(monkeypatch):
    SessionLocal = _session_factory(monkeypatch)
    with SessionLocal() as db:
        db.add(_legacy_row())
        db.commit()

    first = backfill_legacy_insider_normalized(apply=True)
    second = backfill_legacy_insider_normalized(apply=True)

    assert first["inserted_transactions"] == 1
    assert second["inserted_transactions"] == 0
    assert second["skipped_existing"] == 1
    with SessionLocal() as db:
        assert db.query(InsiderTransactionNormalized).count() == 1


def test_backfill_legacy_insider_normalized_filters_id_range(monkeypatch):
    SessionLocal = _session_factory(monkeypatch)
    with SessionLocal() as db:
        db.add(_legacy_row(1))
        second = _legacy_row(2)
        second.external_id = "legacy-2"
        second.payload_json = second.payload_json.replace("000285", "000286")
        db.add(second)
        db.commit()

    report = backfill_legacy_insider_normalized(apply=True, min_id=2, max_id=2)

    assert report["scanned"] == 1
    assert report["min_id"] == 2
    assert report["max_id"] == 2
    with SessionLocal() as db:
        row = db.query(InsiderTransactionNormalized).one()
        assert row.accession_number == "0001046179-26-000286"
