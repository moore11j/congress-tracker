from __future__ import annotations

import re
from collections import defaultdict
from datetime import date
from math import isfinite
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.ingest.government_contracts import government_contracts_table_exists
from app.models import GovernmentContract, GovernmentContractAction, Security, TickerMeta
from app.utils.symbols import normalize_symbol


DEPARTMENT_ALIASES: dict[str, tuple[str, ...]] = {
    "Department of Defense": (
        "Department of Defense",
        "Defense Department",
        "DoD",
        "DOD",
        "Dept of Defense",
        "Dept. of Defense",
    ),
    "Department of Energy": (
        "Department of Energy",
        "Energy Department",
        "Dept of Energy",
        "Dept. of Energy",
    ),
    "Department of Health and Human Services": (
        "Department of Health and Human Services",
        "Health and Human Services",
        "HHS",
        "Dept of Health and Human Services",
        "Dept. of Health and Human Services",
    ),
    "NASA": (
        "NASA",
        "National Aeronautics and Space Administration",
    ),
    "Department of Homeland Security": (
        "Department of Homeland Security",
        "Homeland Security",
        "DHS",
    ),
    "Department of Veterans Affairs": (
        "Department of Veterans Affairs",
        "Veterans Affairs",
        "VA",
    ),
    "Department of Agriculture": (
        "Department of Agriculture",
        "Agriculture Department",
        "USDA",
    ),
    "Department of Transportation": (
        "Department of Transportation",
        "Transportation Department",
        "DOT",
    ),
    "Department of Justice": (
        "Department of Justice",
        "Justice Department",
        "DOJ",
    ),
    "General Services Administration": (
        "General Services Administration",
        "GSA",
    ),
    "Department of Education": (
        "Department of Education",
        "Education Department",
    ),
}


def _normalize_department_key(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\bdept\.?\b", "department", text)
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_ALIAS_TO_CANONICAL = {
    _normalize_department_key(alias): canonical
    for canonical, aliases in DEPARTMENT_ALIASES.items()
    for alias in aliases
}


def department_slug(name: str | None) -> str | None:
    canonical = canonical_department_name(name)
    if not canonical:
        return None
    if canonical == "NASA":
        return "nasa"
    slug = re.sub(r"[^a-z0-9]+", "-", canonical.lower()).strip("-")
    return slug or None


def canonical_department_name(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return None
    mapped = _ALIAS_TO_CANONICAL.get(_normalize_department_key(cleaned))
    if mapped:
        return mapped
    return _title_case_department(cleaned)


def resolve_department_slug(db: Session, slug: str) -> str | None:
    cleaned_slug = (slug or "").strip().lower()
    if not cleaned_slug:
        return None

    for canonical, aliases in DEPARTMENT_ALIASES.items():
        if cleaned_slug == department_slug(canonical):
            return canonical
        if any(cleaned_slug == department_slug(alias) for alias in aliases):
            return canonical

    for item in list_departments(db).get("items", []):
        if item.get("slug") == cleaned_slug:
            return item.get("name")
    return None


def list_departments(db: Session) -> dict[str, Any]:
    stats: dict[str, dict[str, Any]] = {
        name: {
            "slug": department_slug(name),
            "name": name,
            "aliases": list(aliases),
            "totalAwarded": 0.0,
            "contractCount": 0,
            "linkedTickerCount": 0,
            "latestAwardDate": None,
            "_symbols": set(),
        }
        for name, aliases in DEPARTMENT_ALIASES.items()
    }

    if government_contracts_table_exists(db):
        rows = db.execute(
            select(
                GovernmentContract.awarding_agency,
                GovernmentContract.funding_agency,
                func.upper(GovernmentContract.symbol).label("symbol"),
                GovernmentContract.award_amount,
                GovernmentContract.award_date,
            )
        ).all()
        for row in rows:
            canonical = canonical_department_name(row.awarding_agency) or canonical_department_name(row.funding_agency)
            if not canonical:
                continue
            bucket = stats.setdefault(
                canonical,
                {
                    "slug": department_slug(canonical),
                    "name": canonical,
                    "aliases": list(DEPARTMENT_ALIASES.get(canonical, (canonical,))),
                    "totalAwarded": 0.0,
                    "contractCount": 0,
                    "linkedTickerCount": 0,
                    "latestAwardDate": None,
                    "_symbols": set(),
                },
            )
            amount = _number(row.award_amount)
            if amount is not None:
                bucket["totalAwarded"] += amount
            bucket["contractCount"] += 1
            symbol = normalize_symbol(row.symbol)
            if symbol:
                bucket["_symbols"].add(symbol)
            latest = _date_iso(row.award_date)
            if latest and (bucket["latestAwardDate"] is None or latest > bucket["latestAwardDate"]):
                bucket["latestAwardDate"] = latest

    items = []
    for bucket in stats.values():
        symbols = bucket.pop("_symbols", set())
        bucket["linkedTickerCount"] = len(symbols)
        bucket["totalAwarded"] = round(float(bucket["totalAwarded"] or 0.0), 2)
        items.append(bucket)

    items.sort(key=lambda item: (-(item.get("totalAwarded") or 0), item.get("name") or ""))
    return {"items": items}


def get_department_profile(db: Session, slug: str, *, limit: int = 10) -> dict[str, Any] | None:
    canonical = resolve_department_slug(db, slug)
    if not canonical:
        return None

    bounded_limit = max(1, min(int(limit or 10), 50))
    contract_rows = _department_contract_rows(db, canonical)
    action_rows = _department_action_rows(db, canonical)
    company_names = _company_names_for_symbols(db, [row.symbol for row in contract_rows] + [row.symbol for row in action_rows])

    ticker_buckets: dict[str, dict[str, Any]] = {}
    for row in contract_rows:
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        bucket = ticker_buckets.setdefault(
            symbol,
            {
                "symbol": symbol,
                "companyName": company_names.get(symbol) or _best_company_name(row, symbol),
                "totalAwarded": 0.0,
                "contractCount": 0,
                "latestAwardDate": None,
                "topDescription": None,
                "_topAmount": -1.0,
            },
        )
        amount = _number(row.award_amount) or 0.0
        bucket["totalAwarded"] += amount
        bucket["contractCount"] += 1
        award_date = _date_iso(row.award_date)
        if award_date and (bucket["latestAwardDate"] is None or award_date > bucket["latestAwardDate"]):
            bucket["latestAwardDate"] = award_date
        if amount > bucket["_topAmount"] and row.description:
            bucket["_topAmount"] = amount
            bucket["topDescription"] = row.description

    if not ticker_buckets:
        for row in action_rows:
            symbol = normalize_symbol(row.symbol)
            if not symbol:
                continue
            bucket = ticker_buckets.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "companyName": company_names.get(symbol) or _best_company_name(row, symbol),
                    "totalAwarded": 0.0,
                    "contractCount": 0,
                    "latestAwardDate": None,
                    "topDescription": None,
                    "_topAmount": -1.0,
                },
            )
            amount = _number(row.obligated_amount) or 0.0
            bucket["totalAwarded"] += amount
            bucket["contractCount"] += 1
            action_date = _date_iso(row.action_date)
            if action_date and (bucket["latestAwardDate"] is None or action_date > bucket["latestAwardDate"]):
                bucket["latestAwardDate"] = action_date
            if amount > bucket["_topAmount"] and row.description:
                bucket["_topAmount"] = amount
                bucket["topDescription"] = row.description

    tickers = []
    for bucket in ticker_buckets.values():
        bucket.pop("_topAmount", None)
        bucket["totalAwarded"] = round(float(bucket["totalAwarded"] or 0.0), 2)
        tickers.append(bucket)
    tickers.sort(key=lambda item: (-(item.get("totalAwarded") or 0), item.get("symbol") or ""))

    context_rows = action_rows if action_rows else contract_rows
    recent_contracts = [
        _contract_payload(row, canonical, company_names)
        for row in sorted(context_rows, key=lambda row: (_row_date_iso(row) or "", _row_amount(row) or 0, getattr(row, "id", 0)), reverse=True)[:bounded_limit]
    ]
    largest_contracts = [
        _contract_payload(row, canonical, company_names)
        for row in sorted(context_rows, key=lambda row: (_row_amount(row) or 0, _row_date_iso(row) or "", getattr(row, "id", 0)), reverse=True)[:bounded_limit]
    ]

    summary_rows = contract_rows if contract_rows else action_rows
    total_awarded = round(sum(_row_amount(row) or 0.0 for row in summary_rows), 2)
    latest_award_date = max((_row_date_iso(row) for row in summary_rows if _row_date_iso(row)), default=None)
    top_ticker = tickers[0] if tickers else None

    return {
        "slug": department_slug(canonical),
        "name": canonical,
        "aliases": list(DEPARTMENT_ALIASES.get(canonical, (canonical,))),
        "summary": {
            "totalAwarded": total_awarded if summary_rows else None,
            "contractCount": len(summary_rows),
            "linkedTickerCount": len(tickers),
            "latestAwardDate": latest_award_date,
            "topTicker": top_ticker.get("symbol") if top_ticker else None,
            "topCompany": top_ticker.get("companyName") if top_ticker else None,
        },
        "tickers": tickers,
        "recentContracts": recent_contracts,
        "largestContracts": largest_contracts,
        "trend": _trend_payload(summary_rows),
    }


def department_suggestions(db: Session, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []
    q_key = _normalize_department_key(q)
    items = list_departments(db).get("items", [])
    matches = []
    for item in items:
        name = item.get("name") or ""
        aliases = item.get("aliases") or []
        searchable = [name, *aliases]
        if any(q_key in _normalize_department_key(value) for value in searchable):
            matches.append(
                {
                    "type": "government_agency",
                    "id": item.get("slug"),
                    "label": name,
                    "subtitle": "Government department - contract awards",
                    "route": f"/departments/{item.get('slug')}",
                    "symbol": "",
                    "name": name,
                }
            )
    matches.sort(key=lambda item: (0 if _normalize_department_key(item["label"]).startswith(q_key) else 1, item["label"]))
    return matches[: max(1, min(int(limit or 10), 50))]


def _department_contract_rows(db: Session, canonical: str) -> list[GovernmentContract]:
    if not government_contracts_table_exists(db):
        return []
    return db.execute(
        select(GovernmentContract)
        .where(_department_clause(GovernmentContract, canonical, include_funding=True))
        .order_by(GovernmentContract.award_date.desc(), GovernmentContract.award_amount.desc(), GovernmentContract.id.desc())
    ).scalars().all()


def _department_action_rows(db: Session, canonical: str) -> list[GovernmentContractAction]:
    if not government_contracts_table_exists(db):
        return []
    return db.execute(
        select(GovernmentContractAction)
        .where(_department_clause(GovernmentContractAction, canonical, include_funding=False))
        .order_by(GovernmentContractAction.action_date.desc(), GovernmentContractAction.obligated_amount.desc(), GovernmentContractAction.id.desc())
    ).scalars().all()


def _department_clause(model: Any, canonical: str, *, include_funding: bool):
    aliases = set(DEPARTMENT_ALIASES.get(canonical, (canonical,)))
    aliases.add(canonical)
    lowered = {_normalize_department_key(alias).lower() for alias in aliases if alias}
    fields = [model.awarding_agency]
    if include_funding and hasattr(model, "funding_agency"):
        fields.append(model.funding_agency)

    clauses = []
    for field in fields:
        lower_field = func.lower(func.trim(field))
        clauses.append(lower_field.in_(lowered))
        for alias in lowered:
            if len(alias) >= 8:
                clauses.append(lower_field.like(f"%{alias}%"))
    return or_(*clauses)


def _company_names_for_symbols(db: Session, symbols: list[str | None]) -> dict[str, str]:
    normalized = sorted({symbol for symbol in (normalize_symbol(value) for value in symbols) if symbol})
    if not normalized:
        return {}
    names: dict[str, str] = {}
    security_rows = db.execute(
        select(func.upper(Security.symbol), Security.name)
        .where(Security.symbol.is_not(None))
        .where(func.upper(Security.symbol).in_(normalized))
    ).all()
    for symbol, name in security_rows:
        if symbol and name:
            names[str(symbol).upper()] = name
    meta_rows = db.execute(
        select(func.upper(TickerMeta.symbol), TickerMeta.company_name)
        .where(func.upper(TickerMeta.symbol).in_(normalized))
    ).all()
    for symbol, name in meta_rows:
        if symbol and name and symbol not in names:
            names[str(symbol).upper()] = name
    return names


def _contract_payload(row: GovernmentContract | GovernmentContractAction, department: str, company_names: dict[str, str]) -> dict[str, Any]:
    symbol = normalize_symbol(getattr(row, "symbol", None))
    amount = _row_amount(row)
    return {
        "id": str(getattr(row, "id", "")),
        "symbol": symbol,
        "companyName": company_names.get(symbol or "") or _best_company_name(row, symbol),
        "recipientName": getattr(row, "recipient_name", None) or getattr(row, "raw_recipient_name", None) or getattr(row, "company_name", None) or "Recipient unavailable",
        "amount": round(amount, 2) if amount is not None else None,
        "date": _row_date_iso(row),
        "department": department,
        "agency": getattr(row, "awarding_sub_agency", None),
        "description": getattr(row, "description", None),
        "awardId": getattr(row, "award_id", None) or getattr(row, "parent_award_id", None),
    }


def _trend_payload(rows: list[GovernmentContract] | list[GovernmentContractAction]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"period": "", "totalAwarded": 0.0, "contractCount": 0})
    for row in rows:
        row_date = _row_date(row)
        if row_date is None:
            continue
        period = f"{row_date.year:04d}-{row_date.month:02d}"
        bucket = buckets[period]
        bucket["period"] = period
        bucket["totalAwarded"] += _row_amount(row) or 0.0
        bucket["contractCount"] += 1
    trend = sorted(buckets.values(), key=lambda item: item["period"])
    for item in trend:
        item["totalAwarded"] = round(float(item["totalAwarded"] or 0.0), 2)
    return trend


def _row_amount(row: Any) -> float | None:
    return _number(getattr(row, "award_amount", None) if hasattr(row, "award_amount") else getattr(row, "obligated_amount", None))


def _row_date(row: Any) -> date | None:
    value = getattr(row, "award_date", None) if hasattr(row, "award_date") else getattr(row, "action_date", None)
    return value if isinstance(value, date) else None


def _row_date_iso(row: Any) -> str | None:
    return _date_iso(_row_date(row))


def _best_company_name(row: Any, symbol: str | None) -> str:
    return (
        getattr(row, "company_name", None)
        or getattr(row, "recipient_name", None)
        or getattr(row, "raw_recipient_name", None)
        or symbol
        or "Company unavailable"
    )


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _date_iso(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    return None


def _title_case_department(value: str) -> str:
    upper = value.strip().upper()
    if upper in {"NASA", "GSA", "HHS", "DHS", "USDA", "DOJ"}:
        return upper
    lower_words = {"and", "of", "the", "for"}
    words = []
    for index, word in enumerate(re.sub(r"\s+", " ", value.strip()).split(" ")):
        lower = word.lower()
        if index > 0 and lower in lower_words:
            words.append(lower)
        elif word.isupper() and 2 <= len(word) <= 5:
            words.append(word)
        else:
            words.append(lower[:1].upper() + lower[1:])
    return " ".join(words)
