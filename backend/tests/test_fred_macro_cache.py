from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import FredObservation
from app.services.fred_macro_cache import build_fred_macro_sections, fred_macro_cache_diagnostics, refresh_fred_macro_cache


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _csv(series_id: str, rows: list[tuple[str, str]]) -> str:
    return "observation_date,{series}\n{body}\n".format(
        series=series_id,
        body="\n".join(f"{day},{value}" for day, value in rows),
    )


def test_fred_refresh_stores_observations_and_builds_macro_sections(monkeypatch):
    db = _db()
    try:
        payloads = {
            "CPILFESL": _csv("CPILFESL", [("2025-04-01", "300"), ("2026-03-01", "306"), ("2026-04-01", "309")]),
            "FEDFUNDS": _csv("FEDFUNDS", [("2026-03-01", "4.25"), ("2026-04-01", "4.50")]),
            "UNRATE": _csv("UNRATE", [("2026-03-01", "4.0"), ("2026-04-01", "4.1")]),
            "RSAFS": _csv("RSAFS", [("2026-03-01", "650000"), ("2026-04-01", "656500")]),
            "GDPC1": _csv("GDPC1", [("2025-10-01", "23100"), ("2026-01-01", "23200"), ("2026-04-01", "23300")]),
            "GFDEGDQ188S": _csv("GFDEGDQ188S", [("2026-01-01", "119.8"), ("2026-04-01", "120.4")]),
            "DGS10": _csv("DGS10", [("2026-04-01", "4.20"), ("2026-04-02", "4.25")]),
        }

        def fake_get(_url, params=None, timeout=10):
            assert timeout == 10
            return _FakeResponse(payloads[params["id"]])

        monkeypatch.setattr("app.services.fred_macro_cache.requests.get", fake_get)
        result = refresh_fred_macro_cache(db, series_ids=tuple(payloads), force=True)

        assert result["status"] == "ok"
        assert result["refreshed_series"] == len(payloads)
        assert db.execute(select(FredObservation).where(FredObservation.series_id == "CPILFESL")).scalars().first() is not None

        sections = build_fred_macro_sections(db)
        economics = {item["label"]: item for item in sections["economics"]}
        treasury = {item["label"]: item for item in sections["treasury"]}

        assert economics["Fed Overnight Rate"]["value"] == 4.5
        assert economics["Fed Overnight Rate"]["change_value"] == 25.0
        assert round(economics["Core CPI"]["value"], 2) == 3.0
        assert economics["Core CPI"]["source"] == "fred"
        assert economics["Retail Sales"]["value"] == 656_500_000_000.0
        assert round(economics["Debt/GDP"]["change_value"], 1) == 0.6
        assert treasury["10Y Treasury"]["value"] == 4.25
        assert round(treasury["10Y Treasury"]["change"], 1) == 5.0

        diagnostics = fred_macro_cache_diagnostics(db, series_ids=tuple(payloads))
        assert diagnostics["last_refresh_at"]
        assert diagnostics["missing_series"] == []
    finally:
        db.close()
