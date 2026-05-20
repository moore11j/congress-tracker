from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date


CONGRESS_EQUITY_EVENT_TYPE = "congress_trade"
CONGRESS_TREASURY_EVENT_TYPE = "congress_treasury_trade"
CONGRESS_CRYPTO_EVENT_TYPE = "congress_crypto_trade"
CONGRESS_DISCLOSURE_EVENT_TYPES = (
    CONGRESS_EQUITY_EVENT_TYPE,
    CONGRESS_TREASURY_EVENT_TYPE,
    CONGRESS_CRYPTO_EVENT_TYPE,
)
CONGRESS_NON_EQUITY_EVENT_TYPES = (
    CONGRESS_TREASURY_EVENT_TYPE,
    CONGRESS_CRYPTO_EVENT_TYPE,
)

TREASURY_ASSET_CLASS = "treasury"
CRYPTO_ASSET_CLASS = "crypto"
OTHER_ASSET_CLASS = "other"
PUBLIC_EQUITY_ASSET_CLASS = "equity"
ETF_FUND_ASSET_CLASS = "etf_fund"

CANONICAL_PUBLIC_EQUITY_BUCKET = "public_equity"
CANONICAL_ETF_FUND_BUCKET = "etf_fund"
CANONICAL_TREASURY_BUCKET = "treasury"
CANONICAL_CRYPTO_BUCKET = "crypto"
CANONICAL_OTHER_BUCKET = "other"

_TREASURY_TERMS = (
    "treasury",
    "t-bill",
    "tbill",
    "t bill",
    "u.s. bills",
    "us bills",
)

_ETF_FUND_TERMS = (
    " etf",
    "exchange traded fund",
    "mutual fund",
    " index fund",
    " closed end fund",
    " money market fund",
    "fund ",
    " fund",
    " trust etf",
)

_ETF_FUND_ASSET_CLASSES = {
    "etf",
    "exchange traded fund",
    "fund",
    "mutual fund",
    "closed end fund",
    "money market fund",
}

_PUBLIC_EQUITY_ASSET_CLASSES = {
    "stock",
    "stocks",
    "equity",
    "equities",
    "common stock",
    "common shares",
    "ordinary shares",
    "public equity",
    "public stock",
}

_OTHER_ASSET_CLASSES = {
    "other",
    "corporate bond",
    "corporate bonds",
    "municipal bond",
    "municipal bonds",
    "private fund",
    "private funds",
    "bond",
    "bonds",
}

_DIRECT_CRYPTO_ALIASES: dict[str, tuple[str, str]] = {
    "bitcoin": ("bitcoin", "BTC"),
    "btc": ("bitcoin", "BTC"),
    "ethereum": ("ethereum", "ETH"),
    "ether": ("ethereum", "ETH"),
    "eth": ("ethereum", "ETH"),
    "solana": ("solana", "SOL"),
    "sol": ("solana", "SOL"),
    "xrp": ("xrp", "XRP"),
    "ripple": ("xrp", "XRP"),
    "dogecoin": ("dogecoin", "DOGE"),
    "doge": ("dogecoin", "DOGE"),
    "cardano": ("cardano", "ADA"),
    "ada": ("cardano", "ADA"),
    "binance coin": ("binance_coin", "BNB"),
    "bnb": ("binance_coin", "BNB"),
    "usd coin": ("stablecoin", "USDC"),
    "usdc": ("stablecoin", "USDC"),
    "tether": ("stablecoin", "USDT"),
    "usdt": ("stablecoin", "USDT"),
}

_CRYPTO_SECURITY_WORDS = (
    " etf",
    " fund",
    " trust",
    " shares",
    " class ",
    " inc",
    " corp",
    " corporation",
    " ltd",
    " plc",
)

_CRYPTO_ASSET_CLASSES = {
    "crypto",
    "cryptocurrency",
    "digital asset",
    "digital assets",
    "virtual currency",
}


@dataclass(frozen=True)
class CongressAssetClassification:
    asset_class: str
    event_type: str
    instrument_type: str
    issuer_name: str | None
    security_description: str
    symbol: str | None = None
    ticker: str | None = None
    maturity_date: str | None = None
    duration_days: int | None = None
    duration_label: str | None = None
    coupon_rate: float | None = None
    cusip: str | None = None

    def payload_fields(self) -> dict[str, object | None]:
        return {
            "asset_class": self.asset_class,
            "assetClass": self.asset_class,
            "instrument_type": self.instrument_type,
            "instrumentType": self.instrument_type,
            "issuer_name": self.issuer_name,
            "issuerName": self.issuer_name,
            "security_description": self.security_description,
            "securityDescription": self.security_description,
            "symbol": self.symbol,
            "ticker": self.ticker,
            "maturity_date": self.maturity_date,
            "maturityDate": self.maturity_date,
            "duration_days": self.duration_days,
            "durationDays": self.duration_days,
            "duration_label": self.duration_label,
            "durationLabel": self.duration_label,
            "coupon_rate": self.coupon_rate,
            "couponRate": self.coupon_rate,
            "cusip": self.cusip,
        }


def _clean_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalized_text(value: object | None) -> str:
    text = _clean_text(value).lower()
    return re.sub(r"\s+", " ", text.replace(".", " ")).strip()


def _normalized_symbol(value: object | None) -> str | None:
    raw = _clean_text(value).upper()
    if not raw:
        return None
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", raw):
        return None
    return raw


def _combined_text(*values: object | None) -> str:
    return f" {' '.join(_normalized_text(value) for value in values if _clean_text(value))} "


def is_etf_or_fund_text(*values: object | None) -> bool:
    text = _combined_text(*values)
    class_values = {_normalized_text(value) for value in values if _clean_text(value)}
    if any(class_value in _ETF_FUND_ASSET_CLASSES for class_value in class_values):
        return True
    return any(term in text for term in _ETF_FUND_TERMS)


def _duration_label(unit: str, count: int) -> str:
    unit = unit.lower()
    if unit.startswith("week") or unit in {"wk", "w"}:
        if count == 52:
            return "1Y"
        return f"{count}W"
    if unit.startswith("month") or unit in {"mo", "m"}:
        if count == 12:
            return "1Y"
        return f"{count}M"
    if unit.startswith("year") or unit in {"yr", "y"}:
        return f"{count}Y"
    return f"{count}{unit[:1].upper()}"


def _duration_days(unit: str, count: int) -> int:
    unit = unit.lower()
    if unit.startswith("week") or unit in {"wk", "w"}:
        return count * 7
    if unit.startswith("month") or unit in {"mo", "m"}:
        return count * 30
    if unit.startswith("year") or unit in {"yr", "y"}:
        return count * 365
    return count


def _parse_maturity_date(text: str) -> str | None:
    due = re.search(r"\b(?:due|matures?|maturity)\s+(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text, re.I)
    if due:
        month, day, year = (int(due.group(1)), int(due.group(2)), int(due.group(3)))
        if year < 100:
            year += 2000
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    due_year = re.search(r"\b(?:due|matures?|maturity)\s+((?:20|19)\d{2})\b", text, re.I)
    if due_year:
        return due_year.group(1)

    return None


def parse_treasury_details(description: object | None) -> dict[str, object | None]:
    text = _clean_text(description)
    normalized = _normalized_text(text)

    instrument_type = "treasury_security"
    if re.search(r"\b(treasury\s+)?(bill|bills|t-bill|tbill)\b", normalized):
        instrument_type = "treasury_bill"
    elif re.search(r"\b(treasury\s+)?notes?\b", normalized):
        instrument_type = "treasury_note"
    elif re.search(r"\b(treasury\s+)?bonds?\b", normalized):
        instrument_type = "treasury_bond"

    duration_days = None
    duration_label = None
    duration = re.search(
        r"\b(\d{1,2})\s*(week|weeks|wk|w|month|months|mo|m|year|years|yr|y)\s+(?:u\.?s\.?\s+)?(?:treasury\s+)?(?:bill|bills|note|notes|bond|bonds|security|securities)\b",
        text,
        re.I,
    )
    if duration:
        count = int(duration.group(1))
        unit = duration.group(2)
        duration_days = _duration_days(unit, count)
        duration_label = _duration_label(unit, count)

    coupon = re.search(r"\b(\d+(?:\.\d+)?)\s*%\b", text)
    cusip = re.search(r"\b(?:CUSIP[:\s]*)?([0-9A-Z]{9})\b", text, re.I)
    cusip_value = cusip.group(1).upper() if cusip and (cusip.group(0).upper().startswith("CUSIP") or cusip.group(1).startswith("912")) else None

    return {
        "instrument_type": instrument_type,
        "maturity_date": _parse_maturity_date(text),
        "duration_days": duration_days,
        "duration_label": duration_label,
        "coupon_rate": float(coupon.group(1)) if coupon else None,
        "cusip": cusip_value,
    }


def _is_treasury(description: str, asset_class: str) -> bool:
    if is_etf_or_fund_text(description, asset_class):
        return False
    normalized = _normalized_text(f"{description} {asset_class}")
    return any(term in normalized for term in _TREASURY_TERMS)


def _classify_crypto(description: str, asset_class: str, raw_symbol: str | None) -> tuple[str, str] | None:
    if is_etf_or_fund_text(description, asset_class):
        return None

    normalized_description = f" {_normalized_text(description)} "
    normalized_class = _normalized_text(asset_class)
    normalized_symbol = _clean_text(raw_symbol).upper()

    if normalized_symbol in {symbol for _instrument, symbol in _DIRECT_CRYPTO_ALIASES.values()}:
        if not any(word in normalized_description for word in _CRYPTO_SECURITY_WORDS):
            for _alias, (instrument, symbol) in _DIRECT_CRYPTO_ALIASES.items():
                if normalized_symbol == symbol:
                    return instrument, symbol

    direct_crypto_class = normalized_class in _CRYPTO_ASSET_CLASSES
    for alias, (instrument, symbol) in _DIRECT_CRYPTO_ALIASES.items():
        alias_text = f" {alias} "
        if alias_text not in normalized_description:
            continue
        if direct_crypto_class or not any(word in normalized_description for word in _CRYPTO_SECURITY_WORDS):
            return instrument, symbol

    if direct_crypto_class:
        return "crypto_asset", None
    return None


def canonical_asset_bucket(
    *,
    event_type: object | None = None,
    asset_class: object | None = None,
    instrument_type: object | None = None,
    symbol: object | None = None,
    security_description: object | None = None,
    company_name: object | None = None,
) -> str:
    """Return the canonical UI/filter bucket for a disclosure asset.

    The order is deliberate: fund/ETF wrappers are listed securities even when
    their strategy contains Treasury or crypto words, while direct Treasury and
    direct crypto rows should not create ticker-linked events.
    """

    event_text = _normalized_text(event_type)
    class_text = _normalized_text(asset_class)
    instrument_text = _normalized_text(instrument_type)
    symbol_text = _normalized_symbol(symbol)
    combined = _combined_text(asset_class, instrument_type, security_description, company_name)

    if is_etf_or_fund_text(asset_class, instrument_type, security_description, company_name):
        return CANONICAL_ETF_FUND_BUCKET

    direct_treasury_event = event_text == CONGRESS_TREASURY_EVENT_TYPE
    direct_crypto_event = event_text == CONGRESS_CRYPTO_EVENT_TYPE

    if direct_treasury_event or class_text == TREASURY_ASSET_CLASS or instrument_text.startswith("treasury_"):
        return CANONICAL_TREASURY_BUCKET
    if any(term in combined for term in _TREASURY_TERMS):
        return CANONICAL_TREASURY_BUCKET

    if direct_crypto_event or class_text in _CRYPTO_ASSET_CLASSES or instrument_text in {value[0] for value in _DIRECT_CRYPTO_ALIASES.values()}:
        return CANONICAL_CRYPTO_BUCKET
    if _classify_crypto(_clean_text(security_description or company_name), _clean_text(asset_class), symbol_text):
        return CANONICAL_CRYPTO_BUCKET

    if symbol_text:
        if class_text in _ETF_FUND_ASSET_CLASSES or instrument_text in _ETF_FUND_ASSET_CLASSES:
            return CANONICAL_ETF_FUND_BUCKET
        return CANONICAL_PUBLIC_EQUITY_BUCKET

    if class_text in _PUBLIC_EQUITY_ASSET_CLASSES or " common stock " in combined or " stock " in combined:
        return CANONICAL_PUBLIC_EQUITY_BUCKET
    if class_text in _OTHER_ASSET_CLASSES:
        return CANONICAL_OTHER_BUCKET

    return CANONICAL_OTHER_BUCKET


def canonical_asset_class_value(
    *,
    event_type: object | None = None,
    asset_class: object | None = None,
    instrument_type: object | None = None,
    symbol: object | None = None,
    security_description: object | None = None,
    company_name: object | None = None,
) -> str:
    bucket = canonical_asset_bucket(
        event_type=event_type,
        asset_class=asset_class,
        instrument_type=instrument_type,
        symbol=symbol,
        security_description=security_description,
        company_name=company_name,
    )
    if bucket == CANONICAL_PUBLIC_EQUITY_BUCKET:
        return PUBLIC_EQUITY_ASSET_CLASS
    if bucket == CANONICAL_ETF_FUND_BUCKET:
        return ETF_FUND_ASSET_CLASS
    if bucket == CANONICAL_TREASURY_BUCKET:
        return TREASURY_ASSET_CLASS
    if bucket == CANONICAL_CRYPTO_BUCKET:
        return CRYPTO_ASSET_CLASS
    return OTHER_ASSET_CLASS


def classify_congress_disclosure_asset(
    *,
    security_description: object | None,
    asset_class: object | None = None,
    raw_symbol: object | None = None,
) -> CongressAssetClassification | None:
    description = _clean_text(security_description)
    class_text = _clean_text(asset_class)
    symbol_text = _clean_text(raw_symbol) or None

    if _is_treasury(description, class_text):
        details = parse_treasury_details(description)
        return CongressAssetClassification(
            asset_class=TREASURY_ASSET_CLASS,
            event_type=CONGRESS_TREASURY_EVENT_TYPE,
            instrument_type=str(details["instrument_type"]),
            issuer_name="U.S. Treasury",
            security_description=description or "U.S. Treasury security",
            maturity_date=details["maturity_date"],
            duration_days=details["duration_days"],
            duration_label=details["duration_label"],
            coupon_rate=details["coupon_rate"],
            cusip=details["cusip"],
        )

    crypto = _classify_crypto(description, class_text, symbol_text)
    if crypto:
        instrument_type, asset_symbol = crypto
        return CongressAssetClassification(
            asset_class=CRYPTO_ASSET_CLASS,
            event_type=CONGRESS_CRYPTO_EVENT_TYPE,
            instrument_type=instrument_type,
            issuer_name=None,
            security_description=description or asset_symbol or "Crypto asset",
            symbol=asset_symbol,
            ticker=None,
        )

    return None


def is_supported_non_equity_disclosure(
    *,
    security_description: object | None,
    asset_class: object | None = None,
    raw_symbol: object | None = None,
) -> bool:
    return classify_congress_disclosure_asset(
        security_description=security_description,
        asset_class=asset_class,
        raw_symbol=raw_symbol,
    ) is not None
