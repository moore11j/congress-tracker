from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import case, func, select, tuple_
from sqlalchemy.orm import Session

from app.models import (
    Event,
    GovernmentContract,
    GovernmentContractAction,
    InstitutionalActivityEvent,
    InstitutionalFiling,
    InstitutionalHolder,
    InstitutionalPosition,
    InstitutionalPositionChange,
    Member,
    Security,
    TickerMeta,
)
from app.services.government_departments import department_slug, list_departments
from app.services.institutional_activity import normalize_cik
from app.utils.symbols import normalize_symbol


PROFILE_ACTIVITY_TYPES = (
    "congress_trade",
    "insider_trade",
    "institutional_accumulation",
    "institutional_distribution",
    "new_institutional_position",
    "major_holder_reduction",
    "major_holder_exit",
    "cluster_accumulation",
    "cluster_distribution",
    "smart_money_confirmation",
    "government_contract",
)


def profiles_summary(db: Session, *, activity_type: str = "all", activity_limit: int = 25, include_institutions: bool = False) -> dict[str, Any]:
    return {
        "status": "ok",
        "cards": [
            _profile_card(
                "congress",
                "Congress",
                "Track disclosed trades and portfolio activity from U.S. lawmakers.",
                "/members",
                [
                    {"label": "Trades", "value": _count_events(db, "congress_trade")},
                    {"label": "Active Members", "value": _count_distinct_event_field(db, "congress_trade", Event.member_bioguide_id)},
                ],
            ),
            _profile_card(
                "insiders",
                "Insiders",
                "Track buying and selling by executives, directors, and major shareholders.",
                "/insiders",
                [
                    {"label": "Trades", "value": _count_events(db, "insider_trade")},
                    {"label": "Active Insiders", "value": _count_distinct_event_field(db, "insider_trade", Event.member_bioguide_id)},
                ],
            ),
            _profile_card(
                "institutions",
                "Institutions",
                "Track institutional portfolios and quarterly position changes.",
                "/institutions",
                [
                    {"label": "Institutions", "value": _count_rows(db, InstitutionalHolder.cik)},
                    {"label": "Portfolio Value", "value": _latest_institutional_value(db), "format": "currency"},
                ],
                locked=not include_institutions,
                required_plan="pro" if not include_institutions else None,
            ),
            _profile_card(
                "departments",
                "Departments",
                "Track government contract awards and agency spending activity.",
                "/departments",
                [
                    {"label": "Departments / Agencies", "value": len(list_departments(db).get("items", []))},
                    {"label": "Contract Value", "value": _government_contract_total(db), "format": "currency"},
                ],
            ),
        ],
        "activity": profile_activity(db, activity_type=activity_type, limit=activity_limit, include_institutions=include_institutions),
    }


def congress_overview(db: Session, *, chamber: str = "all", period_days: int = 365) -> dict[str, Any]:
    period_days = _bounded_period(period_days)
    since = datetime.now(timezone.utc) - timedelta(days=period_days)
    prev_since = since - timedelta(days=period_days)
    chamber_value = chamber if chamber in {"house", "senate"} else "all"

    base = _event_query("congress_trade", since=since, chamber=chamber_value)
    previous = _event_query("congress_trade", since=prev_since, before=since, chamber=chamber_value)

    total_trades = db.execute(select(func.count()).select_from(Event).where(*base)).scalar_one()
    previous_trades = db.execute(select(func.count()).select_from(Event).where(*previous)).scalar_one()
    buy_value = _sum_amount(db, base, sides=("buy", "purchase", "p-purchase"))
    sell_value = _sum_amount(db, base, sides=("sell", "sale", "s-sale"))
    previous_buy_value = _sum_amount(db, previous, sides=("buy", "purchase", "p-purchase"))
    previous_sell_value = _sum_amount(db, previous, sides=("sell", "sale", "s-sale"))
    active_members = db.execute(select(func.count(func.distinct(Event.member_bioguide_id))).where(*base, Event.member_bioguide_id.is_not(None))).scalar_one()
    average_trade_size = db.execute(select(func.avg(func.coalesce(Event.amount_max, Event.amount_min))).where(*base)).scalar_one()

    return {
        "status": "ok",
        "period_days": period_days,
        "chamber": chamber_value,
        "summary": [
            _metric("Total Trades", total_trades, previous_trades),
            _metric("Total Buy Value", buy_value, previous_buy_value, "currency"),
            _metric("Total Sell Value", sell_value, previous_sell_value, "currency"),
            _metric("Active Members", active_members, None),
            _metric("Average Trade Size", average_trade_size, None, "currency"),
        ],
        "top_members": _top_congress_members(db, base),
        "most_traded_stocks": _most_traded_event_stocks(db, base),
        "sector_exposure": _event_sector_exposure(db, "congress_trade", since=since, chamber=chamber_value),
        "top_buyers": _top_event_actors(db, base, sides=("buy", "purchase", "p-purchase")),
        "top_sellers": _top_event_actors(db, base, sides=("sell", "sale", "s-sale")),
        "recent_disclosures": _recent_event_rows(db, base, limit=8),
        "largest_recent_trades": _largest_event_rows(db, base, limit=8),
        "note": "Based on disclosed Congressional holdings and transactions. Reporting may be delayed under disclosure requirements.",
    }


def insiders_overview(db: Session, *, period_days: int = 365, sector: str | None = None) -> dict[str, Any]:
    period_days = _bounded_period(period_days)
    since = datetime.now(timezone.utc) - timedelta(days=period_days)
    sector_value = (sector or "").strip()
    symbols = _symbols_for_sector(db, sector_value) if sector_value else None
    base = _event_query("insider_trade", since=since, symbols=symbols)
    previous = _event_query("insider_trade", since=since - timedelta(days=period_days), before=since, symbols=symbols)

    total_trades = db.execute(select(func.count()).select_from(Event).where(*base)).scalar_one()
    previous_trades = db.execute(select(func.count()).select_from(Event).where(*previous)).scalar_one()
    buy_value = _sum_amount(db, base, sides=("buy", "purchase", "p-purchase"))
    sell_value = _sum_amount(db, base, sides=("sell", "sale", "s-sale"))
    active_insiders = db.execute(select(func.count(func.distinct(Event.member_bioguide_id))).where(*base, Event.member_bioguide_id.is_not(None))).scalar_one()
    average_trade_size = db.execute(select(func.avg(func.coalesce(Event.amount_max, Event.amount_min))).where(*base)).scalar_one()

    return {
        "status": "ok",
        "period_days": period_days,
        "sector": sector_value or "all",
        "summary": [
            _metric("Total Insider Trades", total_trades, previous_trades),
            _metric("Buy Value", buy_value, _sum_amount(db, previous, sides=("buy", "purchase", "p-purchase")), "currency"),
            _metric("Sell Value", sell_value, _sum_amount(db, previous, sides=("sell", "sale", "s-sale")), "currency"),
            _metric("Active Insiders", active_insiders, None),
            _metric("Average Trade Size", average_trade_size, None, "currency"),
        ],
        "top_insiders": _top_insiders(db, base),
        "most_traded_stocks": _most_traded_event_stocks(db, base),
        "sector_activity": _event_sector_exposure(db, "insider_trade", since=since, symbols=symbols),
        "recent_purchases": _recent_event_rows(db, [*base, _side_clause(("buy", "purchase", "p-purchase"))], limit=8),
        "largest_buys": _largest_event_rows(db, [*base, _side_clause(("buy", "purchase", "p-purchase"))], limit=8),
        "cluster_buying": _cluster_buying(db, since=since),
    }


def institutions_overview(db: Session, *, year: int | None = None, quarter: int | None = None, include_details: bool = False) -> dict[str, Any]:
    if not include_details:
        return {
            "status": "pro_locked",
            "locked": True,
            "required_plan": "pro",
            "message": "Institutional holdings are available on Pro. The landing page remains discoverable.",
            "summary": [
                _metric("Tracked Institutions", _count_rows(db, InstitutionalHolder.cik), None),
                _metric("Total Portfolio Value", _latest_institutional_value(db), None, "currency"),
                _metric("Total Position Increases", None, None),
                _metric("Total Position Decreases", None, None),
                _metric("Net Reported Value Change", None, None, "currency"),
            ],
            "top_institutions": [],
            "position_changes": [],
            "sector_exposure": [],
            "most_widely_held": [],
            "largest_new_positions": [],
            "largest_exits": [],
            "recent_filings": [],
        }

    period = _latest_institutional_period(db, year=year, quarter=quarter)
    if period is None:
        return {"status": "no_data", "summary": [], "top_institutions": [], "position_changes": [], "sector_exposure": [], "recent_filings": []}
    report_year, report_quarter = period
    period_filter = [InstitutionalPosition.report_year == report_year, InstitutionalPosition.report_quarter == report_quarter]
    change_filter = [InstitutionalPositionChange.report_year == report_year, InstitutionalPositionChange.report_quarter == report_quarter]

    total_value = db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(*period_filter)).scalar_one()
    increases = db.execute(select(func.count()).select_from(InstitutionalPositionChange).where(*change_filter, InstitutionalPositionChange.shares_delta > 0)).scalar_one()
    decreases = db.execute(select(func.count()).select_from(InstitutionalPositionChange).where(*change_filter, InstitutionalPositionChange.shares_delta < 0)).scalar_one()
    net_change = db.execute(select(func.sum(InstitutionalPositionChange.value_delta_usd)).where(*change_filter)).scalar_one()

    return {
        "status": "ok",
        "report_year": report_year,
        "report_quarter": report_quarter,
        "summary": [
            _metric("Tracked Institutions", _count_rows(db, InstitutionalHolder.cik), None),
            _metric("Total Portfolio Value", total_value, None, "currency"),
            _metric("Total Position Increases", increases, None),
            _metric("Total Position Decreases", decreases, None),
            _metric("Net Reported Value Change", net_change, None, "currency"),
        ],
        "top_institutions": _top_institutions(db, report_year, report_quarter),
        "position_changes": _institutional_position_changes(db, report_year, report_quarter),
        "sector_exposure": _institution_sector_exposure(db),
        "most_widely_held": _most_widely_held(db, report_year, report_quarter),
        "largest_new_positions": _institutional_position_changes(db, report_year, report_quarter, change_types=("new", "new_position"), limit=8),
        "largest_exits": _institutional_position_changes(db, report_year, report_quarter, change_types=("exit", "exited"), limit=8, descending=False),
        "recent_filings": _recent_filings(db),
    }


def departments_overview(db: Session, *, fiscal_year: int | None = None) -> dict[str, Any]:
    departments = list_departments(db).get("items", [])
    contract_count = _count_rows(db, GovernmentContract.id) + _count_rows(db, GovernmentContractAction.id)
    total_value = _government_contract_total(db)
    active_vendors = _active_vendor_count(db)
    average_size = (total_value / contract_count) if contract_count else None
    modification_count = _count_rows(db, GovernmentContractAction.id)

    return {
        "status": "ok",
        "fiscal_year": fiscal_year,
        "summary": [
            _metric("Total Contracts", contract_count, None),
            _metric("Total Contract Value", total_value, None, "currency"),
            _metric("Active Vendors", active_vendors, None),
            _metric("Average Contract Size", average_size, None, "currency"),
            _metric("Contract Modifications", modification_count, None),
        ],
        "top_departments": _top_departments(departments),
        "top_vendors": _top_vendors(db),
        "contract_value_over_time": _contract_value_over_time(db),
        "largest_recent_awards": _largest_recent_awards(db),
        "fastest_growing_vendors": _fastest_growing_vendors(db),
        "most_active_departments": _most_active_departments(departments),
    }


def profile_activity(db: Session, *, activity_type: str = "all", limit: int = 25, include_institutions: bool = False) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit or 25), 100))
    requested = (activity_type or "all").strip().lower()
    event_types = list(PROFILE_ACTIVITY_TYPES)
    if requested == "congress":
        event_types = ["congress_trade"]
    elif requested == "insiders":
        event_types = ["insider_trade"]
    elif requested == "institutions":
        event_types = [value for value in PROFILE_ACTIVITY_TYPES if value.startswith(("institutional", "major_holder", "cluster", "smart_money"))]
    elif requested == "departments":
        event_types = ["government_contract"]
    if not include_institutions:
        event_types = [value for value in event_types if not value.startswith(("institutional", "major_holder", "cluster", "smart_money"))]
    rows = db.execute(
        select(Event)
        .where(Event.event_type.in_(event_types))
        .order_by(Event.ts.desc(), Event.id.desc())
        .limit(bounded_limit)
    ).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [_activity_payload(row, company_names) for row in rows]


def _profile_card(kind: str, title: str, description: str, href: str, metrics: list[dict[str, Any]], *, locked: bool = False, required_plan: str | None = None) -> dict[str, Any]:
    return {"kind": kind, "title": title, "description": description, "href": href, "metrics": metrics, "locked": locked, "required_plan": required_plan}


def _metric(label: str, value: Any, previous: Any = None, format_type: str = "number") -> dict[str, Any]:
    change_pct = None
    try:
        if previous not in (None, 0) and value is not None:
            change_pct = ((float(value) - float(previous)) / abs(float(previous))) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        change_pct = None
    return {"label": label, "value": _float_or_int(value), "previous_value": _float_or_int(previous), "change_pct": _float_or_int(change_pct), "format": format_type}


def _float_or_int(value: Any) -> int | float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed.is_integer():
        return int(parsed)
    return parsed


def _bounded_period(period_days: int) -> int:
    try:
        return max(30, min(int(period_days), 1095))
    except (TypeError, ValueError):
        return 365


def _count_events(db: Session, event_type: str) -> int:
    return int(db.execute(select(func.count()).select_from(Event).where(Event.event_type == event_type)).scalar_one() or 0)


def _count_rows(db: Session, column: Any) -> int:
    return int(db.execute(select(func.count(column))).scalar_one() or 0)


def _count_distinct_event_field(db: Session, event_type: str, column: Any) -> int:
    return int(db.execute(select(func.count(func.distinct(column))).where(Event.event_type == event_type, column.is_not(None))).scalar_one() or 0)


def _event_query(event_type: str, *, since: datetime, before: datetime | None = None, chamber: str = "all", symbols: set[str] | None = None) -> list[Any]:
    clauses = [Event.event_type == event_type, Event.ts >= since]
    if before is not None:
        clauses.append(Event.ts < before)
    if chamber != "all":
        clauses.append(func.lower(func.coalesce(Event.chamber, "")) == chamber)
    if symbols is not None:
        clauses.append(func.upper(Event.symbol).in_(symbols))
    return clauses


def _side_clause(sides: tuple[str, ...]) -> Any:
    lowered = [side.lower() for side in sides]
    return func.lower(func.coalesce(Event.trade_type, Event.transaction_type, "")).in_(lowered)


def _sum_amount(db: Session, clauses: list[Any], *, sides: tuple[str, ...]) -> float | None:
    return _float_or_int(db.execute(select(func.sum(func.coalesce(Event.amount_max, Event.amount_min))).where(*clauses, _side_clause(sides))).scalar_one())


def _top_congress_members(db: Session, clauses: list[Any], *, limit: int = 10) -> list[dict[str, Any]]:
    rows = db.execute(
        select(
            Event.member_bioguide_id,
            Event.member_name,
            Event.party,
            Event.chamber,
            func.count(Event.id).label("trades"),
            func.sum(func.coalesce(Event.amount_max, Event.amount_min)).label("value"),
            func.max(Event.ts).label("latest"),
        )
        .where(*clauses)
        .group_by(Event.member_bioguide_id, Event.member_name, Event.party, Event.chamber)
        .order_by(func.sum(func.coalesce(Event.amount_max, Event.amount_min)).desc().nullslast(), func.count(Event.id).desc())
        .limit(limit)
    ).all()
    return [
        {
            "name": row.member_name or "Member unavailable",
            "member_id": row.member_bioguide_id,
            "party": row.party,
            "chamber": row.chamber,
            "estimated_portfolio_value": _float_or_int(row.value),
            "trades": int(row.trades or 0),
            "recent_activity": _date_iso(row.latest),
            "href": _member_href(row.member_name),
        }
        for row in rows
    ]


def _top_event_actors(db: Session, clauses: list[Any], *, sides: tuple[str, ...], limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(
        select(Event.member_name, Event.member_bioguide_id, func.count(Event.id), func.sum(func.coalesce(Event.amount_max, Event.amount_min)), func.max(Event.ts))
        .where(*clauses, _side_clause(sides))
        .group_by(Event.member_name, Event.member_bioguide_id)
        .order_by(func.sum(func.coalesce(Event.amount_max, Event.amount_min)).desc().nullslast())
        .limit(limit)
    ).all()
    return [
        {"name": row[0] or "Profile unavailable", "id": row[1], "trades": int(row[2] or 0), "value": _float_or_int(row[3]), "last_activity": _date_iso(row[4]), "href": _member_href(row[0])}
        for row in rows
    ]


def _most_traded_event_stocks(db: Session, clauses: list[Any], *, limit: int = 10) -> list[dict[str, Any]]:
    side_value = func.coalesce(Event.amount_max, Event.amount_min)
    rows = db.execute(
        select(
            func.upper(Event.symbol).label("symbol"),
            func.sum(case((_side_clause(("buy", "purchase", "p-purchase")), side_value), else_=0)).label("buy_value"),
            func.sum(case((_side_clause(("sell", "sale", "s-sale")), side_value), else_=0)).label("sell_value"),
            func.count(func.distinct(Event.member_bioguide_id)).label("actors"),
        )
        .where(*clauses, Event.symbol.is_not(None))
        .group_by(func.upper(Event.symbol))
        .order_by(func.sum(side_value).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [
        {
            "symbol": row.symbol,
            "company": company_names.get(row.symbol) or row.symbol,
            "buy_value": _float_or_int(row.buy_value),
            "sell_value": _float_or_int(row.sell_value),
            "net_value": _float_or_int((row.buy_value or 0) - (row.sell_value or 0)),
            "actor_count": int(row.actors or 0),
            "href": f"/ticker/{row.symbol}" if row.symbol else None,
        }
        for row in rows
    ]


def _event_sector_exposure(db: Session, event_type: str, *, since: datetime, chamber: str = "all", symbols: set[str] | None = None) -> list[dict[str, Any]]:
    clauses = _event_query(event_type, since=since, chamber=chamber, symbols=symbols)
    rows = db.execute(
        select(Event.ts, func.upper(Event.symbol), func.coalesce(Event.amount_max, Event.amount_min))
        .where(*clauses, Event.symbol.is_not(None))
        .order_by(Event.ts.asc())
        .limit(5000)
    ).all()
    sector_by_symbol = _sectors(db, [row[1] for row in rows])
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for ts, symbol, value in rows:
        period = _quarter_label(ts)
        sector = sector_by_symbol.get(symbol) or "Other"
        buckets[period][sector] += float(value or 0)
    return _allocation_payload(buckets)


def _top_insiders(db: Session, clauses: list[Any], *, limit: int = 10) -> list[dict[str, Any]]:
    side_value = func.coalesce(Event.amount_max, Event.amount_min)
    rows = db.execute(
        select(
            Event.member_name,
            Event.member_bioguide_id,
            func.upper(Event.symbol).label("symbol"),
            func.sum(case((_side_clause(("buy", "purchase", "p-purchase")), side_value), else_=0)).label("buy_value"),
            func.sum(case((_side_clause(("sell", "sale", "s-sale")), side_value), else_=0)).label("sell_value"),
            func.count(Event.id).label("trades"),
            func.max(Event.ts).label("latest"),
            func.max(Event.payload_json).label("payload_json"),
        )
        .where(*clauses)
        .group_by(Event.member_name, Event.member_bioguide_id, func.upper(Event.symbol))
        .order_by((func.sum(case((_side_clause(("buy", "purchase", "p-purchase")), side_value), else_=0)) - func.sum(case((_side_clause(("sell", "sale", "s-sale")), side_value), else_=0))).desc())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [
        {
            "name": row.member_name or "Insider unavailable",
            "reporting_cik": row.member_bioguide_id,
            "symbol": row.symbol,
            "company": company_names.get(row.symbol) or row.symbol,
            "role": _payload_text(row.payload_json, "role", "officerTitle", "insiderRole", "position", "typeOfOwner"),
            "net_buy_value": _float_or_int((row.buy_value or 0) - (row.sell_value or 0)),
            "trades": int(row.trades or 0),
            "last_transaction": _date_iso(row.latest),
            "href": _insider_href(row.member_name, row.member_bioguide_id),
        }
        for row in rows
    ]


def _recent_event_rows(db: Session, clauses: list[Any], *, limit: int) -> list[dict[str, Any]]:
    rows = db.execute(select(Event).where(*clauses).order_by(Event.ts.desc(), Event.id.desc()).limit(limit)).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [_activity_payload(row, company_names) for row in rows]


def _largest_event_rows(db: Session, clauses: list[Any], *, limit: int) -> list[dict[str, Any]]:
    rows = db.execute(select(Event).where(*clauses).order_by(func.coalesce(Event.amount_max, Event.amount_min).desc().nullslast(), Event.ts.desc()).limit(limit)).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [_activity_payload(row, company_names) for row in rows]


def _cluster_buying(db: Session, *, since: datetime, limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(
        select(func.upper(Event.symbol), func.count(func.distinct(Event.member_bioguide_id)), func.sum(func.coalesce(Event.amount_max, Event.amount_min)), func.max(Event.ts))
        .where(Event.event_type == "insider_trade", Event.ts >= since, Event.symbol.is_not(None), _side_clause(("buy", "purchase", "p-purchase")))
        .group_by(func.upper(Event.symbol))
        .having(func.count(func.distinct(Event.member_bioguide_id)) >= 2)
        .order_by(func.count(func.distinct(Event.member_bioguide_id)).desc(), func.sum(func.coalesce(Event.amount_max, Event.amount_min)).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [
        {"symbol": row[0], "company": company_names.get(row[0]) or row[0], "unique_insiders": int(row[1] or 0), "buy_value": _float_or_int(row[2]), "last_transaction": _date_iso(row[3]), "href": f"/ticker/{row[0]}"}
        for row in rows
    ]


def _latest_institutional_period(db: Session, *, year: int | None = None, quarter: int | None = None) -> tuple[int, int] | None:
    if year and quarter:
        return int(year), int(quarter)
    row = db.execute(
        select(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter)
        .group_by(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter)
        .order_by(InstitutionalPosition.report_year.desc(), InstitutionalPosition.report_quarter.desc())
        .limit(1)
    ).first()
    return (int(row[0]), int(row[1])) if row else None


def _latest_institutional_value(db: Session) -> float | None:
    period = _latest_institutional_period(db)
    if period is None:
        return None
    year, quarter = period
    return _float_or_int(db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(InstitutionalPosition.report_year == year, InstitutionalPosition.report_quarter == quarter)).scalar_one())


def _top_institutions(db: Session, year: int, quarter: int, *, limit: int = 15) -> list[dict[str, Any]]:
    rows = db.execute(
        select(
            InstitutionalPosition.cik,
            func.max(InstitutionalHolder.holder_name),
            func.sum(InstitutionalPosition.value_usd),
            func.count(InstitutionalPosition.id),
        )
        .outerjoin(InstitutionalHolder, InstitutionalHolder.cik == InstitutionalPosition.cik)
        .where(InstitutionalPosition.report_year == year, InstitutionalPosition.report_quarter == quarter)
        .group_by(InstitutionalPosition.cik)
        .order_by(func.sum(InstitutionalPosition.value_usd).desc().nullslast())
        .limit(limit)
    ).all()
    result = []
    for cik, name, value, positions in rows:
        top = db.execute(
            select(InstitutionalPosition.normalized_symbol, InstitutionalPosition.issuer_name, InstitutionalPosition.value_usd)
            .where(InstitutionalPosition.cik == cik, InstitutionalPosition.report_year == year, InstitutionalPosition.report_quarter == quarter)
            .order_by(InstitutionalPosition.value_usd.desc().nullslast())
            .limit(1)
        ).first()
        previous = _previous_institution_value(db, str(cik), year, quarter)
        result.append(
            {
                "name": name or "Institution unavailable",
                "cik": normalize_cik(cik),
                "portfolio_value": _float_or_int(value),
                "previous_value": previous,
                "qoq_change": _float_or_int((float(value or 0) - float(previous)) if previous is not None else None),
                "positions": int(positions or 0),
                "largest_holding": {"symbol": top[0], "company": top[1], "value": _float_or_int(top[2])} if top else None,
                "href": f"/institution/{normalize_cik(cik)}" if normalize_cik(cik) else None,
            }
        )
    return result


def _previous_institution_value(db: Session, cik: str, year: int, quarter: int) -> float | None:
    prev_year, prev_quarter = (year - 1, 4) if quarter == 1 else (year, quarter - 1)
    return _float_or_int(db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(InstitutionalPosition.cik == cik, InstitutionalPosition.report_year == prev_year, InstitutionalPosition.report_quarter == prev_quarter)).scalar_one())


def _institutional_position_changes(db: Session, year: int, quarter: int, *, change_types: tuple[str, ...] | None = None, limit: int = 10, descending: bool = True) -> list[dict[str, Any]]:
    query = (
        select(
            func.upper(InstitutionalPositionChange.normalized_symbol),
            func.max(InstitutionalPositionChange.symbol),
            func.max(InstitutionalPositionChange.cusip),
            func.sum(InstitutionalPositionChange.curr_value_usd),
            func.sum(InstitutionalPositionChange.prev_value_usd),
            func.sum(InstitutionalPositionChange.value_delta_usd),
            func.count(func.distinct(InstitutionalPositionChange.cik)),
            func.max(InstitutionalPositionChange.change_type),
        )
        .where(InstitutionalPositionChange.report_year == year, InstitutionalPositionChange.report_quarter == quarter, InstitutionalPositionChange.normalized_symbol.is_not(None))
    )
    if change_types:
        query = query.where(func.lower(InstitutionalPositionChange.change_type).in_([item.lower() for item in change_types]))
    query = query.group_by(func.upper(InstitutionalPositionChange.normalized_symbol))
    order_col = func.sum(InstitutionalPositionChange.value_delta_usd)
    query = query.order_by(order_col.desc().nullslast() if descending else order_col.asc().nullslast()).limit(limit)
    rows = db.execute(query).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [
        {
            "symbol": row[0] or row[1],
            "company": company_names.get(row[0]) or row[0] or row[1],
            "current_value": _float_or_int(row[3]),
            "previous_value": _float_or_int(row[4]),
            "increase_value": _float_or_int(row[5]),
            "increase_pct": _pct(row[5], row[4]),
            "institution_count": int(row[6] or 0),
            "change_type": row[7],
            "href": f"/ticker/{row[0]}" if row[0] else None,
        }
        for row in rows
    ]


def _institution_sector_exposure(db: Session) -> list[dict[str, Any]]:
    periods = db.execute(
        select(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter)
        .group_by(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter)
        .order_by(InstitutionalPosition.report_year.desc(), InstitutionalPosition.report_quarter.desc())
        .limit(8)
    ).all()
    if not periods:
        return []
    period_set = {(int(y), int(q)) for y, q in periods}
    rows = db.execute(
        select(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter, func.upper(InstitutionalPosition.normalized_symbol), InstitutionalPosition.value_usd)
        .where(
            tuple_(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter).in_(period_set)  # type: ignore[name-defined]
        )
        .limit(10000)
    ).all()
    sectors = _sectors(db, [row[2] for row in rows])
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for year, quarter, symbol, value in rows:
        buckets[f"Q{quarter} {year}"][sectors.get(symbol) or "Other"] += float(value or 0)
    return _allocation_payload(buckets)


def _most_widely_held(db: Session, year: int, quarter: int, *, limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(
        select(func.upper(InstitutionalPosition.normalized_symbol), func.count(func.distinct(InstitutionalPosition.cik)), func.sum(InstitutionalPosition.value_usd))
        .where(InstitutionalPosition.report_year == year, InstitutionalPosition.report_quarter == quarter, InstitutionalPosition.normalized_symbol.is_not(None))
        .group_by(func.upper(InstitutionalPosition.normalized_symbol))
        .order_by(func.count(func.distinct(InstitutionalPosition.cik)).desc(), func.sum(InstitutionalPosition.value_usd).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [{"symbol": row[0], "company": company_names.get(row[0]) or row[0], "holders": int(row[1] or 0), "value": _float_or_int(row[2]), "href": f"/ticker/{row[0]}"} for row in rows]


def _recent_filings(db: Session, *, limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(
        select(InstitutionalFiling, InstitutionalHolder.holder_name)
        .outerjoin(InstitutionalHolder, InstitutionalHolder.cik == InstitutionalFiling.cik)
        .order_by(InstitutionalFiling.filing_date.desc(), InstitutionalFiling.id.desc())
        .limit(limit)
    ).all()
    return [
        {"cik": normalize_cik(filing.cik), "name": name or "Institution unavailable", "filing_date": _date_iso(filing.filing_date), "report_period": f"Q{filing.report_quarter} {filing.report_year}", "form_type": filing.form_type, "href": f"/institution/{normalize_cik(filing.cik)}"}
        for filing, name in rows
    ]


def _top_departments(items: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    return [
        {
            "name": item.get("name"),
            "href": f"/departments/{item.get('slug')}",
            "contract_value": item.get("totalAwarded"),
            "previous_value": None,
            "change_pct": None,
            "contracts": item.get("contractCount"),
            "top_vendor": None,
        }
        for item in sorted(items, key=lambda row: -(row.get("totalAwarded") or 0))[:limit]
    ]


def _most_active_departments(items: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    return [{"name": item.get("name"), "href": f"/departments/{item.get('slug')}", "contracts": item.get("contractCount"), "contract_value": item.get("totalAwarded")} for item in sorted(items, key=lambda row: -(row.get("contractCount") or 0))[:limit]]


def _top_vendors(db: Session, *, limit: int = 10) -> list[dict[str, Any]]:
    rows = db.execute(
        select(func.upper(GovernmentContract.symbol), func.max(GovernmentContract.recipient_name), func.sum(GovernmentContract.award_amount), func.count(GovernmentContract.id), func.max(GovernmentContract.awarding_agency))
        .where(GovernmentContract.symbol.is_not(None))
        .group_by(func.upper(GovernmentContract.symbol))
        .order_by(func.sum(GovernmentContract.award_amount).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [{"vendor": company_names.get(row[0]) or row[1] or row[0], "symbol": row[0], "href": f"/ticker/{row[0]}" if row[0] else None, "contract_value": _float_or_int(row[2]), "contracts": int(row[3] or 0), "top_department": row[4]} for row in rows]


def _contract_value_over_time(db: Session) -> list[dict[str, Any]]:
    rows = db.execute(select(GovernmentContract.award_date, GovernmentContract.award_amount).order_by(GovernmentContract.award_date.asc()).limit(5000)).all()
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"period": "", "value": 0.0, "contracts": 0})
    for award_date, amount in rows:
        if not isinstance(award_date, date):
            continue
        period = f"Q{((award_date.month - 1) // 3) + 1} {award_date.year}"
        buckets[period]["period"] = period
        buckets[period]["value"] += float(amount or 0)
        buckets[period]["contracts"] += 1
    return list(buckets.values())[-12:]


def _largest_recent_awards(db: Session, *, limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(select(GovernmentContract).order_by(GovernmentContract.award_date.desc(), GovernmentContract.award_amount.desc()).limit(limit)).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [
        {"symbol": normalize_symbol(row.symbol), "company": company_names.get(normalize_symbol(row.symbol) or "") or row.recipient_name, "department": row.awarding_agency, "value": _float_or_int(row.award_amount), "date": _date_iso(row.award_date), "description": row.description, "href": f"/ticker/{normalize_symbol(row.symbol)}" if normalize_symbol(row.symbol) else None}
        for row in rows
    ]


def _fastest_growing_vendors(db: Session, *, limit: int = 8) -> list[dict[str, Any]]:
    today = date.today()
    current_since = today - timedelta(days=365)
    previous_since = today - timedelta(days=730)
    rows = db.execute(
        select(
            func.upper(GovernmentContract.symbol),
            func.sum(case((GovernmentContract.award_date >= current_since, GovernmentContract.award_amount), else_=0)),
            func.sum(case((GovernmentContract.award_date < current_since, GovernmentContract.award_amount), else_=0)),
        )
        .where(GovernmentContract.award_date >= previous_since, GovernmentContract.symbol.is_not(None))
        .group_by(func.upper(GovernmentContract.symbol))
        .limit(100)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    scored = []
    for symbol, current, previous in rows:
        delta = float(current or 0) - float(previous or 0)
        if delta <= 0:
            continue
        scored.append({"symbol": symbol, "company": company_names.get(symbol) or symbol, "current_value": _float_or_int(current), "previous_value": _float_or_int(previous), "increase_value": _float_or_int(delta), "href": f"/ticker/{symbol}"})
    scored.sort(key=lambda item: -(item.get("increase_value") or 0))
    return scored[:limit]


def _government_contract_total(db: Session) -> float | None:
    contracts = db.execute(select(func.sum(GovernmentContract.award_amount))).scalar_one()
    actions = db.execute(select(func.sum(GovernmentContractAction.obligated_amount))).scalar_one()
    return _float_or_int(float(contracts or 0) + float(actions or 0))


def _active_vendor_count(db: Session) -> int:
    contract_symbols = db.execute(select(func.upper(GovernmentContract.symbol)).where(GovernmentContract.symbol.is_not(None)).group_by(func.upper(GovernmentContract.symbol))).all()
    action_symbols = db.execute(select(func.upper(GovernmentContractAction.symbol)).where(GovernmentContractAction.symbol.is_not(None)).group_by(func.upper(GovernmentContractAction.symbol))).all()
    return len({row[0] for row in [*contract_symbols, *action_symbols] if row[0]})


def _activity_payload(row: Event, company_names: dict[str, str]) -> dict[str, Any]:
    symbol = normalize_symbol(row.symbol)
    payload = _safe_json(row.payload_json)
    kind = _profile_kind(row.event_type)
    profile_name = row.member_name or _payload_first(payload, "insider_name", "holder_name", "institution_name", "department", "agency") or "Profile unavailable"
    profile_href = _profile_href(kind, profile_name, row.member_bioguide_id, payload)
    value = row.amount_max if row.amount_max is not None else row.amount_min
    return {
        "id": row.id,
        "time": _date_iso(row.ts),
        "type": kind,
        "profile": profile_name,
        "profile_href": profile_href,
        "symbol": symbol,
        "company": company_names.get(symbol or "") or _payload_first(payload, "company_name", "issuer_name", "recipient_name") or symbol,
        "ticker_href": f"/ticker/{symbol}" if symbol else None,
        "activity": _activity_label(row.event_type, row.trade_type or row.transaction_type),
        "value": _float_or_int(value),
        "metric": _float_or_int(row.impact_score),
    }


def _profile_kind(event_type: str) -> str:
    if event_type == "congress_trade":
        return "Congress"
    if event_type == "insider_trade":
        return "Insider"
    if event_type == "government_contract":
        return "Department"
    return "Institution"


def _profile_href(kind: str, name: str, identifier: str | None, payload: dict[str, Any]) -> str | None:
    if kind == "Congress":
        return _member_href(name)
    if kind == "Insider":
        cik = identifier or _payload_first(payload, "reporting_cik", "reportingCik")
        return _insider_href(name, cik)
    if kind == "Institution":
        cik = normalize_cik(identifier or _payload_first(payload, "cik", "institution_cik", "reporting_cik"))
        return f"/institution/{cik}" if cik else None
    if kind == "Department":
        slug = department_slug(_payload_first(payload, "department", "agency") or name)
        return f"/departments/{slug}" if slug else "/departments"
    return None


def _activity_label(event_type: str, trade_type: str | None) -> str:
    label = (trade_type or "").replace("_", " ").strip()
    if label:
        return label.title()
    labels = {
        "government_contract": "New contract",
        "institutional_accumulation": "Position increased",
        "institutional_distribution": "Position decreased",
        "new_institutional_position": "New position",
        "major_holder_exit": "Exited position",
        "major_holder_reduction": "Position reduced",
    }
    return labels.get(event_type, event_type.replace("_", " ").title())


def _member_href(name: str | None) -> str | None:
    if not name:
        return None
    slug = "-".join(part for part in "".join(ch.lower() if ch.isalnum() else " " for ch in name).split() if part)
    return f"/member/{slug}" if slug else None


def _insider_href(name: str | None, cik: str | None) -> str | None:
    normalized = normalize_cik(cik)
    if normalized:
        slug_name = "-".join(part for part in "".join(ch.lower() if ch.isalnum() else " " for ch in (name or "insider")).split() if part)
        return f"/insider/{slug_name}-{normalized}" if slug_name else f"/insider/{normalized}"
    return None


def _company_names(db: Session, symbols: list[str | None]) -> dict[str, str]:
    normalized = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized:
        return {}
    names: dict[str, str] = {}
    for symbol, name in db.execute(select(func.upper(Security.symbol), Security.name).where(Security.symbol.is_not(None), func.upper(Security.symbol).in_(normalized))).all():
        if symbol and name:
            names[str(symbol)] = name
    for symbol, name in db.execute(select(func.upper(TickerMeta.symbol), TickerMeta.company_name).where(func.upper(TickerMeta.symbol).in_(normalized))).all():
        if symbol and name and symbol not in names:
            names[str(symbol)] = name
    return names


def _sectors(db: Session, symbols: list[str | None]) -> dict[str, str]:
    normalized = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized:
        return {}
    return {
        str(symbol): sector
        for symbol, sector in db.execute(select(func.upper(Security.symbol), Security.sector).where(Security.symbol.is_not(None), func.upper(Security.symbol).in_(normalized))).all()
        if symbol and sector
    }


def _symbols_for_sector(db: Session, sector: str) -> set[str]:
    if not sector:
        return set()
    rows = db.execute(select(func.upper(Security.symbol)).where(func.lower(func.coalesce(Security.sector, "")) == sector.lower())).all()
    return {row[0] for row in rows if row[0]}


def _allocation_payload(buckets: dict[str, dict[str, float]]) -> list[dict[str, Any]]:
    items = []
    for period in sorted(buckets.keys())[-8:]:
        values = buckets[period]
        total = sum(values.values())
        items.append(
            {
                "period": period,
                "segments": [
                    {"label": label, "value": round(value, 2), "percent": round((value / total) * 100.0, 2) if total else 0}
                    for label, value in sorted(values.items(), key=lambda entry: -entry[1])
                ],
            }
        )
    return items


def _quarter_label(value: datetime | None) -> str:
    if not isinstance(value, datetime):
        return "Unknown"
    return f"Q{((value.month - 1) // 3) + 1} {value.year}"


def _pct(numerator: Any, denominator: Any) -> float | None:
    try:
        if denominator in (None, 0):
            return None
        return round((float(numerator or 0) / abs(float(denominator))) * 100.0, 2)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _date_iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return None


def _safe_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _payload_first(payload: dict[str, Any], *keys: str) -> str | None:
    payloads = [payload]
    for nested_key in ("payload", "raw", "insider", "transaction"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            payloads.append(nested)
    for candidate in payloads:
        for key in keys:
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _payload_text(payload_json: str | None, *keys: str) -> str | None:
    return _payload_first(_safe_json(payload_json), *keys)
