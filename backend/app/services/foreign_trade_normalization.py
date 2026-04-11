from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any

from app.utils.symbols import normalize_symbol


@dataclass(frozen=True)
class ForeignTradeProfile:
    symbol: str
    filing_currency: str
    filing_share_basis: str
    display_currency: str
    display_share_basis: str
    ordinary_shares_per_adr: float
    notes: str


@dataclass(frozen=True)
class NormalizedInsiderPrice:
    raw_price: float | None
    raw_currency: str | None
    raw_share_basis: str | None
    display_price: float | None
    display_currency: str
    display_share_basis: str
    fx_rate: float | None
    fx_rate_date: str | None
    ordinary_shares_per_adr: float | None
    method: str
    status: str
    confidence: str
    notes: str | None = None

    @property
    def is_comparable(self) -> bool:
        return self.display_price is not None and self.status in {"normalized", "same_basis"}


FOREIGN_TRADE_PROFILES: dict[str, ForeignTradeProfile] = {
    "ASX": ForeignTradeProfile(
        symbol="ASX",
        filing_currency="TWD",
        filing_share_basis="ordinary_share",
        display_currency="USD",
        display_share_basis="adr",
        ordinary_shares_per_adr=2.0,
        notes="ASE Technology Holding ADSs represent two ordinary shares.",
    ),
}

_REFERENCE_USD_PER_UNIT: dict[str, float] = {
    # Reference fallback used only for configured profiles when no env override
    # is present. TWD moves slowly enough that this keeps ASX filings in the
    # right economic basis without mutating source values.
    "TWD": 0.0307,
}


def _parse_positive_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("$", "").replace(",", "").strip()
        if not value:
            return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _raw_payload(payload: dict) -> dict:
    raw = payload.get("raw")
    return raw if isinstance(raw, dict) else {}


def insider_filing_price(payload: dict) -> float | None:
    raw = _raw_payload(payload)
    candidates = (
        payload.get("price"),
        payload.get("transaction_price"),
        payload.get("transactionPrice"),
        payload.get("price_per_share"),
        payload.get("pricePerShare"),
        raw.get("price"),
        raw.get("transactionPrice"),
        raw.get("transaction_price"),
        raw.get("transactionPricePerShare"),
        raw.get("pricePerShare"),
    )
    for candidate in candidates:
        parsed = _parse_positive_float(candidate)
        if parsed is not None:
            return parsed
    return None


def _date_key(value: date | str | None) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        cleaned = value.strip()[:10]
        return cleaned if cleaned else None
    return None


def usd_per_currency_unit(currency: str, trade_date: date | str | None = None) -> tuple[float | None, str | None, str]:
    normalized = currency.strip().upper()
    if normalized == "USD":
        return 1.0, _date_key(trade_date), "usd"

    day = _date_key(trade_date)
    env_keys = []
    if day:
        env_keys.append(f"FX_USD_PER_{normalized}_{day.replace('-', '')}")
    env_keys.append(f"FX_USD_PER_{normalized}")
    for key in env_keys:
        rate = _parse_positive_float(os.getenv(key))
        if rate is not None:
            return rate, day, "env"

    fallback = _REFERENCE_USD_PER_UNIT.get(normalized)
    if fallback is not None:
        return fallback, day, "reference"
    return None, day, "missing"


def foreign_trade_profile_for_symbol(symbol: str | None) -> ForeignTradeProfile | None:
    normalized = normalize_symbol(symbol)
    return FOREIGN_TRADE_PROFILES.get(normalized or "")


def normalize_insider_price(
    *,
    symbol: str | None,
    payload: dict,
    trade_date: date | str | None,
) -> NormalizedInsiderPrice:
    raw_price = insider_filing_price(payload)
    profile = foreign_trade_profile_for_symbol(symbol or payload.get("symbol"))
    if profile is None:
        return NormalizedInsiderPrice(
            raw_price=raw_price,
            raw_currency="USD" if raw_price is not None else None,
            raw_share_basis="listed_share" if raw_price is not None else None,
            display_price=raw_price,
            display_currency="USD",
            display_share_basis="listed_share",
            fx_rate=1.0 if raw_price is not None else None,
            fx_rate_date=_date_key(trade_date),
            ordinary_shares_per_adr=None,
            method="none",
            status="same_basis" if raw_price is not None else "missing_price",
            confidence="high" if raw_price is not None else "none",
        )

    if raw_price is None:
        return NormalizedInsiderPrice(
            raw_price=None,
            raw_currency=profile.filing_currency,
            raw_share_basis=profile.filing_share_basis,
            display_price=None,
            display_currency=profile.display_currency,
            display_share_basis=profile.display_share_basis,
            fx_rate=None,
            fx_rate_date=_date_key(trade_date),
            ordinary_shares_per_adr=profile.ordinary_shares_per_adr,
            method="configured_adr_fx",
            status="missing_price",
            confidence="none",
            notes=profile.notes,
        )

    fx_rate, fx_rate_date, fx_source = usd_per_currency_unit(profile.filing_currency, trade_date)
    if fx_rate is None:
        return NormalizedInsiderPrice(
            raw_price=raw_price,
            raw_currency=profile.filing_currency,
            raw_share_basis=profile.filing_share_basis,
            display_price=None,
            display_currency=profile.display_currency,
            display_share_basis=profile.display_share_basis,
            fx_rate=None,
            fx_rate_date=fx_rate_date,
            ordinary_shares_per_adr=profile.ordinary_shares_per_adr,
            method="configured_adr_fx",
            status="missing_fx",
            confidence="none",
            notes=profile.notes,
        )

    display_price = raw_price * fx_rate * profile.ordinary_shares_per_adr
    return NormalizedInsiderPrice(
        raw_price=raw_price,
        raw_currency=profile.filing_currency,
        raw_share_basis=profile.filing_share_basis,
        display_price=display_price,
        display_currency=profile.display_currency,
        display_share_basis=profile.display_share_basis,
        fx_rate=fx_rate,
        fx_rate_date=fx_rate_date,
        ordinary_shares_per_adr=profile.ordinary_shares_per_adr,
        method=f"configured_adr_fx:{fx_source}",
        status="normalized",
        confidence="high",
        notes=profile.notes,
    )


def normalization_payload(normalized: NormalizedInsiderPrice) -> dict[str, Any]:
    return {
        "raw_price": normalized.raw_price,
        "raw_currency": normalized.raw_currency,
        "raw_share_basis": normalized.raw_share_basis,
        "display_price": normalized.display_price,
        "display_currency": normalized.display_currency,
        "display_share_basis": normalized.display_share_basis,
        "fx_rate": normalized.fx_rate,
        "fx_rate_date": normalized.fx_rate_date,
        "ordinary_shares_per_adr": normalized.ordinary_shares_per_adr,
        "method": normalized.method,
        "status": normalized.status,
        "confidence": normalized.confidence,
        "notes": normalized.notes,
    }
