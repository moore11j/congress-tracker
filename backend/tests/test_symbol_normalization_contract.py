from __future__ import annotations

import app.services.ticker_financials as financials_module
from app.services.data_enrichment_queue import build_dedupe_key, is_valid_enrichment_symbol
from app.services.ticker_financials import clear_financials_cache, get_ticker_financials
from app.utils.symbols import normalize_symbol, symbol_variants


def test_shared_symbol_normalization_accepts_global_fixture_tickers() -> None:
    for raw, expected in {
        " aapl ": "AAPL",
        "mstr": "MSTR",
        "NBIS": "NBIS",
        "bmnr": "BMNR",
        "sdrl": "SDRL",
        "SPCX": "SPCX",
        "NYSE:BRK.B": "BRK.B",
        "brk-b": "BRK-B",
        "BF/B": "BF/B",
    }.items():
        assert normalize_symbol(raw) == expected

    variants = set(symbol_variants("brk.b"))
    assert {"BRK.B", "BRK-B", "BRK/B", "BRKB"} <= variants


def test_shared_symbol_normalization_rejects_placeholders() -> None:
    for raw in (None, "", " ", "[SYMBOL]", "SYMBOL", "UNKNOWN", "foo[bar]", "NULL", "NONE"):
        assert normalize_symbol(raw) is None
        assert not is_valid_enrichment_symbol(raw)

    assert build_dedupe_key(job_type="ticker_meta", symbol="[SYMBOL]") == "ticker_meta|||"


def test_financials_write_and_read_use_same_normalized_symbol(monkeypatch) -> None:
    rows_by_key = {
        "annual_income": [{"date": "2025-12-31", "period": "FY", "revenue": 10.0, "netIncome": 2.0, "eps": 0.2}],
        "quarterly_income": [{"date": "2026-03-31", "period": "Q1", "revenue": 3.0, "netIncome": 1.0, "eps": 0.1}],
        "annual_cash": [],
        "quarterly_cash": [],
        "annual_balance": [],
        "quarterly_balance": [],
        "earnings": [],
        "earnings_calendar": [],
        "quarterly_estimates": [],
        "annual_estimates": [],
        "quote": [],
        "ratios_ttm": [],
        "key_metrics_ttm": [],
    }
    written_symbols: list[str] = []
    read_symbols: list[str] = []

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(financials_module, "_db_cache_set", lambda symbol, payload: written_symbols.append(symbol))
    monkeypatch.setattr(financials_module, "_db_cache_get", lambda symbol: read_symbols.append(symbol) or None)
    monkeypatch.setattr(financials_module, "_fetch_financial_sections", lambda symbol: (rows_by_key, set(), {}))

    for raw in ("aapl", " MSTR ", "nbis", "BMNR", "sdrl"):
        clear_financials_cache()
        written_symbols.clear()
        read_symbols.clear()

        payload = get_ticker_financials(raw)

        normalized = normalize_symbol(raw)
        assert payload["symbol"] == normalized
        assert read_symbols == [normalized]
        assert written_symbols == [normalized]
