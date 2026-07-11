from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import PriceCache
from app.services.fred_macro_cache import build_fred_macro_sections
from app.services.fmp_market_snapshot import get_sector_performance_snapshot, get_treasury_rates_snapshot

US_INDEX_ETF_PROXIES = (
    ("S&P 500 ETF Proxy", "SPY"),
    ("Nasdaq 100 ETF Proxy", "QQQ"),
    ("Dow ETF Proxy", "DIA"),
    ("Russell 2000 ETF Proxy", "IWM"),
)

WORLD_ETF_PROXIES = (
    ("Canada", "VFV"),
    ("United Kingdom", "ISF"),
    ("Japan", "IJP"),
    ("Germany", "EWG"),
    ("China", "MCHI"),
)

COMMODITY_ETF_PROXIES = (
    ("Gold", "GLD"),
    ("Silver", "SLV"),
    ("Oil", "USO"),
    ("Copper", "COPX"),
)

SECTOR_ETF_PROXIES = (
    ("Technology", "XLK"),
    ("Financials", "XLF"),
    ("Energy", "XLE"),
    ("Health Care", "XLV"),
    ("Consumer Discretionary", "XLY"),
    ("Consumer Staples", "XLP"),
    ("Industrials", "XLI"),
    ("Utilities", "XLU"),
    ("Materials", "XLB"),
    ("Real Estate", "XLRE"),
    ("Communication Services", "XLC"),
)

CURRENCY_DISABLED = (
    ("USD/CAD", "USDCAD"),
    ("EUR/USD", "EURUSD"),
    ("GBP/USD", "GBPUSD"),
    ("USD/JPY", "USDJPY"),
    ("EUR/CAD", "EURCAD"),
)

CRYPTO_DISABLED = (
    ("BTC/USD", "BTCUSD"),
    ("ETH/USD", "ETHUSD"),
    ("SOL/USD", "SOLUSD"),
    ("XRP/USD", "XRPUSD"),
    ("BNB/USD", "BNBUSD"),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _latest_price_rows(db: Session, symbol: str, *, limit: int = 2) -> list[PriceCache]:
    return (
        db.execute(
            select(PriceCache)
            .where(func.upper(PriceCache.symbol) == symbol.upper())
            .order_by(PriceCache.date.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


def _proxy_instrument(label: str, symbol: str, rows: list[PriceCache]) -> dict[str, Any]:
    if not rows:
        return {
            "label": label,
            "symbol": symbol,
            "value": None,
            "change": None,
            "change_pct": None,
            "timeframe_label": "EOD change",
            "status": "unavailable",
            "is_proxy": True,
            "source": "eod_etf_proxy",
        }
    latest = rows[0]
    previous = rows[1] if len(rows) > 1 else None
    change = latest.close - previous.close if previous and previous.close is not None else None
    change_pct = (change / previous.close) * 100.0 if change is not None and previous and previous.close else None
    return {
        "label": label,
        "symbol": symbol,
        "value": latest.close,
        "change": change,
        "change_pct": change_pct,
        "timeframe_label": "EOD change",
        "date": latest.date,
        "status": "ok",
        "is_proxy": True,
        "source": "eod_etf_proxy",
    }


def _proxy_instruments(db: Session, targets: tuple[tuple[str, str], ...]) -> list[dict[str, Any]]:
    return [_proxy_instrument(label, symbol, _latest_price_rows(db, symbol)) for label, symbol in targets]


def _sector_performance(db: Session) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for sector, symbol in SECTOR_ETF_PROXIES:
        rows = _latest_price_rows(db, symbol)
        if len(rows) < 2 or not rows[1].close:
            continue
        change_pct = ((rows[0].close / rows[1].close) - 1.0) * 100.0
        items.append({"sector": f"{sector} ({symbol})", "change_pct": change_pct, "source": "eod_etf_proxy"})
    return items


def _disabled_instruments(targets: tuple[tuple[str, str], ...], *, source: str) -> list[dict[str, Any]]:
    return [
        {
            "label": label,
            "symbol": symbol,
            "value": "Coming soon",
            "change": None,
            "change_pct": None,
            "timeframe_label": "Launch disabled",
            "status": "disabled",
            "source": source,
        }
        for label, symbol in targets
    ]


def _has_values(items: list[dict[str, Any]]) -> bool:
    return any(
        item.get("status") != "disabled" and (item.get("value") is not None or item.get("change_pct") is not None)
        for item in items
    )


def _has_macro_values(items: list[dict[str, Any]]) -> bool:
    return any(item.get("value") is not None for item in items)


def _block_status(*, items: list[dict[str, Any]] | None = None, disabled: bool = False, source: str) -> dict[str, Any]:
    if disabled:
        return {"status": "disabled", "source": source}
    values = items or []
    if not values:
        return {"status": "unavailable", "source": source}
    if _has_values(values) or _has_macro_values(values):
        missing_count = sum(1 for item in values if item.get("value") is None and item.get("change_pct") is None)
        return {"status": "partial" if missing_count else "ok", "source": source, "missing_count": missing_count}
    return {"status": "unavailable", "source": source, "missing_count": len(values)}


def build_builder_safe_insights_snapshot(db: Session) -> dict[str, Any]:
    fred_sections = build_fred_macro_sections(db)
    indexes = _proxy_instruments(db, US_INDEX_ETF_PROXIES)
    world_indexes = _proxy_instruments(db, WORLD_ETF_PROXIES)
    commodities = _proxy_instruments(db, COMMODITY_ETF_PROXIES)
    currencies = _disabled_instruments(CURRENCY_DISABLED, source="disabled_for_launch")
    crypto = _disabled_instruments(CRYPTO_DISABLED, source="disabled_for_launch")
    sector_performance = get_sector_performance_snapshot()
    sector_source = "sector_performance_snapshot" if sector_performance else "eod_etf_proxy"
    if not sector_performance:
        sector_performance = _sector_performance(db)
    economics = fred_sections["economics"]
    treasury = get_treasury_rates_snapshot()
    treasury_source = "treasury_rates" if treasury else "fred_cache"
    if not treasury:
        treasury = fred_sections["treasury"]

    available_sections = sum(
        [
            _has_values(indexes),
            _has_values(world_indexes),
            _has_values(commodities),
            bool(sector_performance),
            _has_macro_values(economics),
            _has_macro_values(treasury),
        ]
    )
    status = "unavailable" if available_sections == 0 else "ok" if available_sections == 6 else "partial"

    return {
        "world_indexes": world_indexes,
        "indexes": indexes,
        "treasury": treasury,
        "economics": economics,
        "commodities": commodities,
        "currencies": currencies,
        "crypto": crypto,
        "sector_performance": sector_performance,
        "status": status,
        "generated_at": _now_iso(),
        "source": "builder_safe_cache",
        "data_mode": "builder_safe",
        "fred_macro_cache": fred_sections["diagnostics"],
        "block_status": {
            "world_indexes": _block_status(items=world_indexes, source="eod_etf_proxy"),
            "currencies": _block_status(disabled=True, source="disabled_for_launch"),
            "commodities": _block_status(items=commodities, source="eod_etf_proxy"),
            "crypto": _block_status(disabled=True, source="disabled_for_launch"),
            "us_macro": _block_status(items=economics, source="fred_cache"),
            "us_treasury": _block_status(items=treasury, source=treasury_source),
            "us_indexes": _block_status(items=indexes, source="eod_etf_proxy"),
            "us_sectors": _block_status(items=sector_performance, source=sector_source),
        },
    }
