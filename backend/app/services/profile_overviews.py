from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import case, func, or_, select, tuple_, union_all
from sqlalchemy.orm import Session

from app.models import (
    Event,
    GovernmentContract,
    GovernmentContractAction,
    InsiderTransaction,
    InsiderTransactionNormalized,
    InstitutionalActivityEvent,
    InstitutionalFiling,
    InstitutionalHolder,
    InstitutionalPosition,
    InstitutionalPositionChange,
    InstitutionalSymbolSummary,
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

MAX_REASONABLE_INSIDER_TRADE_VALUE = 10_000_000_000

TECH_PLATFORM_SECTOR_SYMBOLS = {"AAPL", "NVDA", "MSFT", "GOOG", "GOOGL", "AMZN", "META"}
INSTITUTIONAL_PERIOD_MIN_COVERAGE_RATIO = 0.5
INSTITUTIONAL_PERIOD_MIN_INSTITUTIONS = 25
GOVERNMENT_CONTRACT_COMPARISON_MIN_COVERAGE_RATIO = 0.6
GOVERNMENT_CONTRACT_COMPARISON_MIN_PRIOR_ROWS = 500


def profiles_summary(
    db: Session,
    *,
    activity_type: str = "all",
    activity_limit: int = 25,
    activity_per_type: int | None = None,
    include_institutions: bool = False,
    include_activity: bool = False,
) -> dict[str, Any]:
    today = date.today()
    period_start = today - timedelta(days=365)
    previous_period_start = period_start - timedelta(days=365)
    period_end = today + timedelta(days=1)
    period_label = "latest 365 days vs prior 365 days"

    congress_current = _congress_profile_period_metrics(db, since=period_start, before=period_end)
    congress_previous = _congress_profile_period_metrics(db, since=previous_period_start, before=period_start)
    insider_current = _insider_profile_period_metrics(db, since=period_start, before=period_end)
    insider_previous = _insider_profile_period_metrics(db, since=previous_period_start, before=period_start)
    department_current = _government_contract_period_metrics(db, since=period_start, before=period_end)
    department_previous = _government_contract_period_metrics(db, since=previous_period_start, before=period_start)
    institutional_current, institutional_previous, institutional_comparison, institutional_period = _institutional_profile_period_metrics(db)

    congress_trade_count = congress_current["trades"]
    active_members = congress_current["active_members"]
    insider_trade_count = insider_current["trades"]
    active_insiders = insider_current["active_insiders"]
    contract_value = department_current["total_value"]
    department_count = department_current["departments"]
    latest_institutional_value = institutional_current["portfolio_value"]
    institutional_count = institutional_current["institutions"]
    card_trends = _profile_card_trends(db, institutional_period=institutional_period)
    return {
        "status": "ok",
        "cards": [
            _profile_card(
                "congress",
                "Congress",
                "Track disclosed trades and portfolio activity from U.S. lawmakers.",
                "/members",
                [
                    _metric("Trades", congress_trade_count, congress_previous["trades"]),
                    _metric("Active Members", active_members, congress_previous["active_members"]),
                ],
                comparison_label=period_label,
                trend=card_trends["congress"],
            ),
            _profile_card(
                "insiders",
                "Insiders",
                "Track buying and selling by executives, directors, and major shareholders.",
                "/insiders",
                [
                    _metric("Trades", insider_trade_count, insider_previous["trades"]),
                    _metric("Active Insiders", active_insiders, insider_previous["active_insiders"]),
                ],
                comparison_label=period_label,
                trend=card_trends["insiders"],
            ),
            _profile_card(
                "institutions",
                "Institutions",
                "Track institutional portfolios and quarterly position changes.",
                "/institutions",
                [
                    _metric("Institutions", institutional_count, institutional_previous["institutions"]),
                    _metric("Portfolio Value", latest_institutional_value, institutional_previous["portfolio_value"], "currency"),
                ],
                locked=not include_institutions,
                required_plan="pro" if not include_institutions else None,
                comparison_label=institutional_comparison,
                trend=card_trends["institutions"],
            ),
            _profile_card(
                "departments",
                "Departments",
                "Track government contract awards and agency spending activity.",
                "/departments",
                [
                    _metric("Departments / Agencies", department_count, department_previous["departments"]),
                    _metric("Contract Value", contract_value, department_previous["total_value"], "currency"),
                ],
                comparison_label=period_label,
                trend=card_trends["departments"],
            ),
        ],
        "directories": profile_directories(
            db,
            include_institutions=include_institutions,
            include_rankings=True,
            metric_overrides={
                "department_count": department_count,
                "contract_value": contract_value,
                "institutional_count": institutional_count,
                "institutional_value": latest_institutional_value,
                "institutional_period": institutional_period,
                "active_insiders": active_insiders,
                "congress_trade_count": congress_trade_count,
                "active_members": active_members,
                "insider_trade_count": insider_trade_count,
            },
        ),
        "activity": profile_activity(
            db,
            activity_type=activity_type,
            limit=activity_limit,
            per_type_limit=activity_per_type,
            include_institutions=include_institutions,
        ) if include_activity else [],
        "activity_mix": _profile_activity_mix(
            db,
            congress_trades=congress_trade_count,
            insider_trades=insider_trade_count,
            institutional_period=institutional_period,
            department_events=department_current["contract_count"],
        ),
        "activity_by_profile_type": _profile_activity_by_month(db, include_institutions=include_institutions),
        "top_moving_sectors": _top_profile_sector_movers(db, include_institutions=include_institutions),
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
    member_identity = _congress_member_identity()
    active_member_count = db.execute(select(func.count(func.distinct(member_identity))).where(*base, member_identity.is_not(None))).scalar_one()
    average_trade_size = db.execute(select(func.avg(func.coalesce(Event.amount_max, Event.amount_min))).where(*base)).scalar_one()
    analytics = _congress_market_analytics(db, since=prev_since, chamber=chamber_value)
    top_members, most_active_members, stocks = _top_congress_members(db, base), _most_active_congress_members(db, base), _most_traded_event_stocks(db, base)
    buyers = _top_event_actors(db, base, sides=("buy", "purchase", "p-purchase"))
    most_active_sector = max(analytics["sector_activity"], key=lambda row: int(row.get("trades") or 0), default=None)

    return {
        "status": "ok",
        "period_days": period_days,
        "chamber": chamber_value,
        "summary": [
            _metric("Total Trades", total_trades, previous_trades),
            _metric("Total Buy Value", buy_value, previous_buy_value, "currency"),
            _metric("Total Sell Value", sell_value, previous_sell_value, "currency"),
            _metric("Active Members", active_member_count, None),
            _metric("Average Trade Size", average_trade_size, None, "currency"),
        ],
        "monthly_activity": analytics["monthly_activity"],
        "snapshot": {"total_trades": int(total_trades or 0), "top_member": most_active_members[0] if most_active_members else None, "most_traded_ticker": stocks[0] if stocks else None, "top_buyer": buyers[0] if buyers else None, "most_active_sector": most_active_sector},
        "top_members": top_members,
        "most_traded_stocks": stocks,
        "sector_exposure": analytics["sector_exposure"],
        "sector_activity": analytics["sector_activity"],
        "chamber_mix": analytics["chamber_mix"],
        "top_moving_sectors": analytics["top_moving_sectors"],
        "top_buyers": buyers,
        "top_sellers": _top_event_actors(db, base, sides=("sell", "sale", "s-sale")),
        "recent_disclosures": _recent_event_rows(db, base, limit=8),
        "largest_recent_trades": _largest_event_rows(db, base, limit=8),
        "recent_notable_trades": _recent_event_rows(db, base, limit=8),
        "note": "Based on disclosed Congressional holdings and transactions. Reporting may be delayed under disclosure requirements.",
    }


def insiders_overview(db: Session, *, period_days: int = 365, sector: str | None = None) -> dict[str, Any]:
    period_days = _bounded_period(period_days)
    since = datetime.now(timezone.utc) - timedelta(days=period_days)
    sector_value = (sector or "").strip()
    symbols = _symbols_for_sector(db, sector_value) if sector_value else None
    base = _insider_transaction_filters(since=since.date(), symbols=symbols)
    previous = _insider_transaction_filters(since=(since - timedelta(days=period_days)).date(), before=since.date(), symbols=symbols)
    buy_base = [*base, _insider_side_clause("buy")]
    sell_base = [*base, _insider_side_clause("sell")]
    previous_buy = [*previous, _insider_side_clause("buy")]
    previous_sell = [*previous, _insider_side_clause("sell")]

    total_trades = db.execute(select(func.count()).select_from(InsiderTransactionNormalized).where(*base)).scalar_one()
    previous_trades = db.execute(select(func.count()).select_from(InsiderTransactionNormalized).where(*previous)).scalar_one()
    buy_value = _sum_insider_transaction_value(db, buy_base)
    sell_value = _sum_insider_transaction_value(db, sell_base)
    active_insiders = _active_insider_count(db, since=since.date(), symbols=symbols)
    average_trade_size = db.execute(select(func.avg(InsiderTransactionNormalized.value)).where(*base)).scalar_one()

    return {
        "status": "ok",
        "period_days": period_days,
        "sector": sector_value or "all",
        "summary": [
            _metric("Open-Market Trades", total_trades, previous_trades),
            _metric("Buy Value", buy_value, _sum_insider_transaction_value(db, previous_buy), "currency"),
            _metric("Sell Value", sell_value, _sum_insider_transaction_value(db, previous_sell), "currency"),
            _metric("Active Insiders", active_insiders, None),
            _metric("Average Trade Size", average_trade_size, None, "currency"),
        ],
        "top_insiders": _top_insiders(db, base, limit=5),
        "most_traded_stocks": _most_traded_insider_stocks(db, base, limit=5),
        "monthly_activity": _insider_monthly_activity(db, base),
        "sector_activity": _insider_sector_activity(db, base),
        "sector_net_activity": _insider_sector_net_activity(db, base),
        "role_mix": _insider_role_mix(db, base),
        "top_moving_sectors": _insider_top_moving_sectors(db, base, previous),
        "recent_purchases": _recent_insider_transactions(db, buy_base, limit=8),
        "recent_notable_trades": _recent_insider_transactions(db, base, limit=8),
        "largest_buys": _largest_insider_transactions(db, buy_base, limit=8),
        "cluster_buying": _cluster_buying(db, since=since, symbols=symbols),
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
            "institutional_activity_over_time": [],
            "most_widely_held": [],
            "largest_new_positions": [],
            "largest_exits": [],
            "recent_filings": [],
        }

    available_periods = _institutional_periods(db)
    period = _latest_institutional_period(db, year=year, quarter=quarter, periods=available_periods)
    if period is None:
        return {"status": "no_data", "summary": [], "top_institutions": [], "position_changes": [], "sector_exposure": [], "institutional_activity_over_time": [], "recent_filings": []}
    report_year, report_quarter = period
    latest_available_period = available_periods[0] if available_periods else None
    coverage = _institutional_period_coverage(db, available_periods[:2])
    latest_available_coverage = coverage.get(latest_available_period, {}) if latest_available_period else {}
    latest_available_reference = available_periods[1] if len(available_periods) > 1 else None
    latest_available_reference_coverage = coverage.get(latest_available_reference, {}) if latest_available_reference else {}
    latest_available_institutions = int(latest_available_coverage.get("institutions") or 0)
    latest_available_reference_institutions = int(latest_available_reference_coverage.get("institutions") or 0)
    latest_available_coverage_pct = (
        round((latest_available_institutions / latest_available_reference_institutions) * 100, 1)
        if latest_available_reference_institutions > 0
        else None
    )
    previous_period = _previous_comparable_institutional_period(db, report_year, report_quarter)
    table_previous_period = previous_period or _previous_institutional_period_with_data(db, report_year, report_quarter)
    prev_year, prev_quarter = previous_period if previous_period is not None else (None, None)
    period_filter = [InstitutionalPosition.report_year == report_year, InstitutionalPosition.report_quarter == report_quarter]
    change_filter = [InstitutionalPositionChange.report_year == report_year, InstitutionalPositionChange.report_quarter == report_quarter]
    previous_position_filter = [InstitutionalPosition.report_year == prev_year, InstitutionalPosition.report_quarter == prev_quarter] if previous_period is not None else []
    previous_change_filter = [InstitutionalPositionChange.report_year == prev_year, InstitutionalPositionChange.report_quarter == prev_quarter] if previous_period is not None else []

    total_value = db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(*period_filter)).scalar_one()
    previous_total_value = db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(*previous_position_filter)).scalar_one() if previous_position_filter else None
    mapped_sector_value = db.execute(
        select(func.sum(InstitutionalSymbolSummary.total_value_usd)).where(
            InstitutionalSymbolSummary.report_year == report_year,
            InstitutionalSymbolSummary.report_quarter == report_quarter,
        )
    ).scalar_one()
    previous_mapped_sector_value = db.execute(
        select(func.sum(InstitutionalSymbolSummary.total_value_usd)).where(
            InstitutionalSymbolSummary.report_year == prev_year,
            InstitutionalSymbolSummary.report_quarter == prev_quarter,
        )
    ).scalar_one() if previous_period is not None else None
    sector_mapping_coverage_pct = round((float(mapped_sector_value or 0) / float(total_value or 0)) * 100, 1) if total_value else None
    previous_sector_mapping_coverage_pct = round((float(previous_mapped_sector_value or 0) / float(previous_total_value or 0)) * 100, 1) if previous_total_value else None
    sector_mapping_is_comparable = bool(
        sector_mapping_coverage_pct is not None
        and previous_sector_mapping_coverage_pct is not None
        and sector_mapping_coverage_pct >= 60
        and previous_sector_mapping_coverage_pct >= 60
        and min(sector_mapping_coverage_pct, previous_sector_mapping_coverage_pct) / max(sector_mapping_coverage_pct, previous_sector_mapping_coverage_pct) >= 0.8
    )
    tracked_institutions = db.execute(select(func.count(func.distinct(InstitutionalPosition.cik))).where(*period_filter)).scalar_one()
    previous_tracked_institutions = db.execute(select(func.count(func.distinct(InstitutionalPosition.cik))).where(*previous_position_filter)).scalar_one() if previous_position_filter else None
    increases = db.execute(select(func.count()).select_from(InstitutionalPositionChange).where(*change_filter, InstitutionalPositionChange.shares_delta > 0)).scalar_one()
    previous_increases = db.execute(select(func.count()).select_from(InstitutionalPositionChange).where(*previous_change_filter, InstitutionalPositionChange.shares_delta > 0)).scalar_one() if previous_change_filter else None
    decreases = db.execute(select(func.count()).select_from(InstitutionalPositionChange).where(*change_filter, InstitutionalPositionChange.shares_delta < 0)).scalar_one()
    previous_decreases = db.execute(select(func.count()).select_from(InstitutionalPositionChange).where(*previous_change_filter, InstitutionalPositionChange.shares_delta < 0)).scalar_one() if previous_change_filter else None
    net_change = db.execute(select(func.sum(InstitutionalPositionChange.value_delta_usd)).where(*change_filter)).scalar_one()
    previous_net_change = db.execute(select(func.sum(InstitutionalPositionChange.value_delta_usd)).where(*previous_change_filter)).scalar_one() if previous_change_filter else None

    return {
        "status": "ok",
        "report_year": report_year,
        "report_quarter": report_quarter,
        "previous_report_year": prev_year,
        "previous_report_quarter": prev_quarter,
        # The newest filing quarter may still be incomplete while managers submit
        # their 13Fs. Keep the dashboard on the latest comparable period, but
        # expose progress so the automatic update is visible to users.
        "latest_available_report_year": latest_available_period[0] if latest_available_period else None,
        "latest_available_report_quarter": latest_available_period[1] if latest_available_period else None,
        "latest_available_institution_count": latest_available_institutions or None,
        "latest_available_reference_institution_count": latest_available_reference_institutions or None,
        "latest_available_coverage_pct": latest_available_coverage_pct,
        "latest_available_is_comparable": latest_available_period == period,
        "sector_mapping_coverage_pct": sector_mapping_coverage_pct,
        "previous_sector_mapping_coverage_pct": previous_sector_mapping_coverage_pct,
        "sector_mapping_is_comparable": sector_mapping_is_comparable,
        "summary": [
            _metric("Tracked Institutions", tracked_institutions, previous_tracked_institutions),
            _metric("Total Portfolio Value", total_value, previous_total_value, "currency"),
            _metric("Total Position Increases", increases, previous_increases),
            _metric("Total Position Decreases", decreases, previous_decreases),
            _metric("Net Reported Value Change", net_change, previous_net_change, "currency"),
        ],
        "top_institutions": _top_institutions(db, report_year, report_quarter, previous_period=table_previous_period),
        "position_changes": _institutional_position_changes(db, report_year, report_quarter),
        "sector_exposure": _institution_sector_exposure(db),
        "institutional_activity_over_time": _institutional_activity_over_time(db),
        "most_widely_held": _most_widely_held(db, report_year, report_quarter),
        "largest_new_positions": _institutional_position_changes(db, report_year, report_quarter, change_types=("new", "new_position"), limit=8),
        "largest_exits": _institutional_position_changes(db, report_year, report_quarter, change_types=("exit", "exited"), limit=8, descending=False),
        "recent_filings": _recent_filings(db),
    }


def departments_overview(db: Session, *, fiscal_year: int | None = None, period_days: int = 365) -> dict[str, Any]:
    period_days = _bounded_period(period_days)
    today = date.today()
    since = today - timedelta(days=period_days)
    previous_since = since - timedelta(days=period_days)
    trend_since = today - timedelta(days=365 * 3)
    before = today + timedelta(days=1)

    current = _government_contract_period_metrics(db, since=since, before=before)
    previous = _government_contract_period_metrics(db, since=previous_since, before=since)
    comparison = _government_contract_comparison_status(db, today=today, period_days=period_days)
    comparison_available = comparison["status"] == "ok"
    previous_summary = previous if comparison_available else {}
    top_departments_previous_since = previous_since if comparison_available else None

    return {
        "status": "ok",
        "fiscal_year": fiscal_year,
        "period_days": period_days,
        "comparison": comparison,
        "summary": [
            _metric("Total Contracts", current["contract_count"], previous_summary.get("contract_count")),
            _metric("Total Contract Value", current["total_value"], previous_summary.get("total_value"), "currency"),
            _metric("Active Vendors", current["active_vendors"], previous_summary.get("active_vendors")),
            _metric("Average Contract Size", current["average_size"], previous_summary.get("average_size"), "currency"),
            _metric("Contract Modifications", current["modification_count"], previous_summary.get("modification_count")),
        ],
        "top_departments": _top_departments(db, since=since, before=before, previous_since=top_departments_previous_since),
        "top_vendors": _top_vendors(db, since=since, before=before),
        "contract_value_over_time": _contract_value_by_sector_over_time(db, since=trend_since, before=before),
        "largest_recent_awards": _largest_recent_awards(db, since=since, before=before),
        "fastest_growing_vendors": _fastest_growing_vendors(db, period_days=period_days),
        "most_active_departments": _most_active_departments(db, since=since, before=before),
    }


def profile_activity(
    db: Session,
    *,
    activity_type: str = "all",
    limit: int = 25,
    per_type_limit: int | None = None,
    include_institutions: bool = False,
) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit or 25), 100))
    requested = (activity_type or "all").strip().lower()
    now = datetime.now(timezone.utc) + timedelta(days=1)
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
    # The Profiles activity widget renders five records for All or for any one
    # of its type tabs. Fetch the newest records per tab for the overview rather
    # than a global list that can crowd out less-frequent activity types.
    if requested == "all" and per_type_limit is not None:
        bounded_per_type_limit = max(1, min(int(per_type_limit), 10))
        activity_groups = (
            ("congress", ["congress_trade"]),
            ("insiders", ["insider_trade"]),
            ("institutions", [value for value in PROFILE_ACTIVITY_TYPES if value.startswith(("institutional", "major_holder", "cluster", "smart_money"))]),
            ("departments", ["government_contract"]),
        )
        rows = []
        for group, group_event_types in activity_groups:
            if group == "institutions" and not include_institutions:
                continue
            rows.extend(
                db.execute(
                    select(Event)
                    .where(Event.event_type.in_(group_event_types))
                    .where(Event.ts <= now)
                    .order_by(Event.ts.desc(), Event.id.desc())
                    # Source rows can lack a reportable profile name. Fetch a
                    # small buffer so those records do not starve a tab of the
                    # five useful entries the dashboard can render.
                    .limit(bounded_per_type_limit * 3)
                ).scalars().all()
            )
        rows.sort(key=lambda row: (row.ts or datetime.min.replace(tzinfo=timezone.utc), row.id or 0), reverse=True)
    else:
        rows = db.execute(
            select(Event)
            .where(Event.event_type.in_(event_types))
            .where(Event.ts <= now)
            .order_by(Event.ts.desc(), Event.id.desc())
            .limit(bounded_limit)
        ).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    activity = [_activity_payload(row, company_names) for row in rows]
    if requested == "all" and per_type_limit is not None:
        visible: list[dict[str, Any]] = []
        counts: dict[str, int] = defaultdict(int)
        for item in activity:
            if not item.get("profile") or item["profile"] == "Profile unavailable":
                continue
            kind = str(item["type"])
            if counts[kind] >= bounded_per_type_limit:
                continue
            counts[kind] += 1
            visible.append(item)
        return visible
    return activity


def profile_directories(
    db: Session,
    *,
    include_institutions: bool = False,
    include_rankings: bool = True,
    metric_overrides: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=365)
    congress_base = _event_query("congress_trade", since=since)
    insider_base = _insider_transaction_filters(since=since.date())
    overrides = metric_overrides or {}
    department_count = overrides.get("department_count")
    contract_value = overrides.get("contract_value")
    institutional_count = overrides.get("institutional_count")
    institutional_value = overrides.get("institutional_value")
    has_institutional_value_override = "institutional_value" in overrides
    active_insiders = overrides.get("active_insiders")
    congress_trade_count = overrides.get("congress_trade_count")
    active_members = overrides.get("active_members")
    insider_trade_count = overrides.get("insider_trade_count")
    institutional_period = overrides.get("institutional_period") if include_institutions and include_rankings else None
    if institutional_period is None and include_institutions and include_rankings:
        institutional_period = _latest_institutional_period(db)

    directories = [
        _profile_directory(
            "congress",
            "Congress",
            "/members",
            "Member profiles, disclosure history, traded tickers, and chamber-level activity.",
            [
                {"label": "Trades", "value": congress_trade_count if congress_trade_count is not None else _count_events(db, "congress_trade")},
                {"label": "Active Members", "value": active_members if active_members is not None else _count_distinct_event_field(db, "congress_trade", Event.member_bioguide_id)},
            ],
            "Top Congress by Trading Value",
            [
                _directory_item(
                    row.get("name"),
                    row.get("href"),
                    row.get("estimated_portfolio_value"),
                    "currency",
                    f"{int(row.get('trades') or 0):,} trades",
                )
                for row in _top_congress_members(db, congress_base, limit=4)
            ] if include_rankings else [],
            "Most Traded Tickers",
            [
                _directory_item(
                    row.get("symbol") or row.get("company"),
                    row.get("href"),
                    row.get("value"),
                    "currency",
                    row.get("company") if row.get("company") != row.get("symbol") else None,
                )
                for row in _most_traded_event_stocks(db, congress_base, limit=4)
            ] if include_rankings else [],
            "Open Congress profiles",
        ),
        _profile_directory(
            "insiders",
            "Insiders",
            "/insiders",
            "Corporate officers, directors, major shareholders, and their recent Form 4 activity.",
            [
                {"label": "Trades", "value": insider_trade_count if insider_trade_count is not None else _count_events(db, "insider_trade")},
                {"label": "Active Insiders", "value": active_insiders if active_insiders is not None else _active_insider_count(db, since=since.date())},
            ],
            "Top Insiders by Net Buying",
            [
                _directory_item(
                    row.get("name"),
                    row.get("href"),
                    row.get("net_buy_value"),
                    "currency",
                    " - ".join(part for part in [str(row.get("symbol") or ""), f"{int(row.get('trades') or 0):,} trades"] if part),
                )
                for row in _top_insiders(db, insider_base, limit=4)
            ] if include_rankings else [],
            "Cluster Buying",
            [
                _directory_item(
                    row.get("symbol") or row.get("company"),
                    row.get("href"),
                    row.get("buy_value"),
                    "currency",
                    f"{int(row.get('unique_insiders') or 0):,} insiders",
                )
                for row in _cluster_buying(db, since=since, limit=4)
            ] if include_rankings else [],
            "Open insider profiles",
        ),
    ]

    if include_institutions and institutional_period is not None:
        year, quarter = institutional_period
        directories.append(
            _profile_directory(
                "institutions",
                "Institutions",
                "/institutions",
                "13F managers, portfolio value, holdings concentration, and quarter-over-quarter changes.",
                [
                    {"label": "Institutions", "value": institutional_count if institutional_count is not None else _count_rows(db, InstitutionalHolder.cik)},
                    {"label": f"Q{quarter} {year} Value", "value": institutional_value if institutional_value is not None else _latest_institutional_value(db), "format": "currency"},
                ],
                "Top Institutions by Portfolio Value",
                [
                    _directory_item(
                        row.get("name"),
                        row.get("href"),
                        row.get("portfolio_value"),
                        "currency",
                        f"{int(row.get('positions') or 0):,} positions",
                    )
                    for row in _top_institutions(db, year, quarter, limit=4)
                ] if include_rankings else [],
                "Widely Held Tickers",
                [
                    _directory_item(
                        row.get("symbol") or row.get("company"),
                        row.get("href"),
                        row.get("value"),
                        "currency",
                        f"{int(row.get('holders') or 0):,} holders",
                    )
                    for row in _most_widely_held(db, year, quarter, limit=4)
                ] if include_rankings else [],
                "Open institution profiles",
            )
        )
    else:
        directories.append(
            _profile_directory(
                "institutions",
                "Institutions",
                "/institutions",
                "13F managers, portfolio value, holdings concentration, and quarter-over-quarter changes.",
                [
                    {"label": "Institutions", "value": institutional_count if institutional_count is not None else _count_rows(db, InstitutionalHolder.cik)},
                    {"label": "Portfolio Value", "value": institutional_value if has_institutional_value_override else _latest_institutional_value(db), "format": "currency"},
                ],
                "Top Institutions by Portfolio Value",
                [],
                "Widely Held Tickers",
                [],
                "Open institution profiles",
                locked=not include_institutions,
                message="Upgrade to Pro to unlock institution rankings and holding-level detail.",
            )
        )

    directories.append(
        _profile_directory(
            "departments",
            "Departments",
            "/departments",
                "Government agencies, contract awards, public-company vendors, and spending leaders.",
                [
                {"label": "Departments / Agencies", "value": department_count if department_count is not None else _department_count(db)},
                {"label": "Contract Value", "value": contract_value if contract_value is not None else _government_contract_total(db), "format": "currency"},
            ],
            "Top Departments by Contract Value",
            [
                _directory_item(
                    row.get("name"),
                    row.get("href"),
                    row.get("contract_value"),
                    "currency",
                    f"{int(row.get('contracts') or 0):,} contracts",
                )
                for row in _top_departments(db, limit=4)
            ] if include_rankings else [],
            "Top Vendors",
            [
                _directory_item(
                    row.get("symbol") or row.get("vendor"),
                    row.get("href"),
                    row.get("contract_value"),
                    "currency",
                    row.get("vendor") if row.get("vendor") != row.get("symbol") else None,
                )
                for row in _top_vendors(db, limit=4)
            ] if include_rankings else [],
            "Open department profiles",
        )
    )

    return directories


def _profile_card(
    kind: str,
    title: str,
    description: str,
    href: str,
    metrics: list[dict[str, Any]],
    *,
    locked: bool = False,
    required_plan: str | None = None,
    comparison_label: str | None = None,
    trend: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "title": title,
        "description": description,
        "href": href,
        "metrics": metrics,
        "locked": locked,
        "required_plan": required_plan,
        "comparison_label": comparison_label,
        "trend": trend,
    }


def _profile_directory(
    kind: str,
    title: str,
    href: str,
    description: str,
    metrics: list[dict[str, Any]],
    primary_title: str,
    primary_items: list[dict[str, Any]],
    secondary_title: str,
    secondary_items: list[dict[str, Any]],
    cta_label: str,
    *,
    locked: bool = False,
    message: str | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "title": title,
        "href": href,
        "description": description,
        "metrics": metrics,
        "primary_title": primary_title,
        "primary_items": primary_items,
        "secondary_title": secondary_title,
        "secondary_items": secondary_items,
        "cta_label": cta_label,
        "locked": locked,
        "message": message,
    }


def _directory_item(label: Any, href: Any, value: Any, value_format: str = "number", detail: Any = None) -> dict[str, Any]:
    return {
        "label": str(label or "Profile unavailable"),
        "href": href if isinstance(href, str) and href else None,
        "value": _float_or_int(value),
        "value_format": value_format,
        "detail": str(detail) if detail else None,
    }


def _metric(label: str, value: Any, previous: Any = None, format_type: str = "number") -> dict[str, Any]:
    change_pct = None
    try:
        if previous not in (None, 0) and value is not None:
            change_pct = ((float(value) - float(previous)) / abs(float(previous))) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        change_pct = None
    return {"label": label, "value": _float_or_int(value), "previous_value": _float_or_int(previous), "change_pct": _float_or_int(change_pct), "format": format_type}


def _profile_card_trends(db: Session, *, institutional_period: tuple[int, int] | None) -> dict[str, dict[str, Any]]:
    """Return the compact-card series at the cadence provided by each source.

    Congressional disclosures, Form 4s, and contract awards can be grouped by
    month. Institutional portfolio values are reported through quarterly 13F
    filings, so their card deliberately exposes filing quarters instead of
    interpolating values into made-up monthly observations.
    """
    return {
        "congress": {
            "metric_label": "Trades",
            "value_format": "number",
            "cadence": "monthly",
            "points": _monthly_congress_trade_trend(db),
        },
        "insiders": {
            "metric_label": "Trades",
            "value_format": "number",
            "cadence": "monthly",
            "points": _monthly_insider_trade_trend(db),
        },
        "institutions": {
            "metric_label": "Portfolio Value",
            "value_format": "currency",
            "cadence": "quarterly filings",
            "points": _quarterly_institutional_value_trend(db, latest_period=institutional_period),
        },
        "departments": {
            "metric_label": "Contract Value",
            "value_format": "currency",
            "cadence": "monthly",
            "points": _monthly_department_contract_trend(db),
        },
    }


def _monthly_trend_months() -> list[date]:
    current_month = date.today().replace(day=1)
    return [_add_months(current_month, offset) for offset in range(-11, 1)]


def _monthly_congress_trade_trend(db: Session) -> list[dict[str, Any]]:
    months = _monthly_trend_months()
    start = datetime.combine(months[0], datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(_add_months(months[-1], 1), datetime.min.time(), tzinfo=timezone.utc)
    rows = db.execute(
        select(func.extract("year", Event.ts), func.extract("month", Event.ts), func.count(Event.id))
        .where(Event.event_type == "congress_trade", Event.ts >= start, Event.ts < end)
        .group_by(func.extract("year", Event.ts), func.extract("month", Event.ts))
    ).all()
    values = {(int(year), int(month)): int(total or 0) for year, month, total in rows}
    return [{"label": month.strftime("%b %y"), "value": values.get((month.year, month.month), 0)} for month in months]


def _monthly_insider_trade_trend(db: Session) -> list[dict[str, Any]]:
    months = _monthly_trend_months()
    rows = db.execute(
        select(
            func.extract("year", InsiderTransactionNormalized.transaction_date),
            func.extract("month", InsiderTransactionNormalized.transaction_date),
            func.count(InsiderTransactionNormalized.id),
        )
        .where(
            *_insider_transaction_filters(since=months[0], before=_add_months(months[-1], 1)),
        )
        .group_by(
            func.extract("year", InsiderTransactionNormalized.transaction_date),
            func.extract("month", InsiderTransactionNormalized.transaction_date),
        )
    ).all()
    values = {(int(year), int(month)): int(total or 0) for year, month, total in rows}
    return [{"label": month.strftime("%b %y"), "value": values.get((month.year, month.month), 0)} for month in months]


def _monthly_department_contract_trend(db: Session) -> list[dict[str, Any]]:
    months = _monthly_trend_months()
    end = _add_months(months[-1], 1)
    contract_rows = db.execute(
        select(
            func.extract("year", GovernmentContract.award_date),
            func.extract("month", GovernmentContract.award_date),
            func.sum(GovernmentContract.award_amount),
        )
        .where(GovernmentContract.award_date >= months[0], GovernmentContract.award_date < end)
        .group_by(func.extract("year", GovernmentContract.award_date), func.extract("month", GovernmentContract.award_date))
    ).all()
    action_rows = db.execute(
        select(
            func.extract("year", GovernmentContractAction.action_date),
            func.extract("month", GovernmentContractAction.action_date),
            func.sum(GovernmentContractAction.obligated_amount),
        )
        .where(GovernmentContractAction.action_date >= months[0], GovernmentContractAction.action_date < end)
        .group_by(func.extract("year", GovernmentContractAction.action_date), func.extract("month", GovernmentContractAction.action_date))
    ).all()
    values: dict[tuple[int, int], float] = defaultdict(float)
    for year, month, value in [*contract_rows, *action_rows]:
        values[(int(year), int(month))] += float(value or 0)
    return [{"label": month.strftime("%b %y"), "value": _float_or_int(values[(month.year, month.month)]) or 0} for month in months]


def _quarterly_institutional_value_trend(db: Session, *, latest_period: tuple[int, int] | None) -> list[dict[str, Any]]:
    periods = _institutional_periods(db)
    if latest_period is not None:
        periods = [period for period in periods if period <= latest_period]
    complete_periods = _complete_institutional_periods(db, periods)
    selected = complete_periods[:4]
    if latest_period is not None and latest_period not in selected:
        selected = [latest_period, *[period for period in selected if period != latest_period]][:4]
    if not selected:
        return []
    rows = db.execute(
        select(
            InstitutionalPosition.report_year,
            InstitutionalPosition.report_quarter,
            func.sum(InstitutionalPosition.value_usd),
        )
        .where(tuple_(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter).in_(selected))  # type: ignore[name-defined]
        .group_by(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter)
    ).all()
    values = {(int(year), int(quarter)): _float_or_int(value) or 0 for year, quarter, value in rows}
    return [
        {"label": f"Q{quarter} {year}", "value": values.get((year, quarter), 0)}
        for year, quarter in reversed(selected)
    ]


def _congress_profile_period_metrics(db: Session, *, since: date, before: date) -> dict[str, int]:
    clauses = _event_query(
        "congress_trade",
        since=datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc),
        before=datetime.combine(before, datetime.min.time(), tzinfo=timezone.utc),
    )
    return {
        "trades": int(db.execute(select(func.count(Event.id)).where(*clauses)).scalar_one() or 0),
        "active_members": int(db.execute(select(func.count(func.distinct(_congress_member_identity()))).where(*clauses, _congress_member_identity().is_not(None))).scalar_one() or 0),
    }


def _insider_profile_period_metrics(db: Session, *, since: date, before: date) -> dict[str, int]:
    clauses = _insider_transaction_filters(since=since, before=before)
    owner_key = func.coalesce(InsiderTransactionNormalized.reporting_owner_cik, InsiderTransactionNormalized.reporting_owner_name)
    return {
        "trades": int(db.execute(select(func.count(InsiderTransactionNormalized.id)).where(*clauses)).scalar_one() or 0),
        "active_insiders": int(db.execute(select(func.count(func.distinct(owner_key))).where(*clauses, owner_key.is_not(None))).scalar_one() or 0),
    }


def _institutional_profile_period_metrics(db: Session) -> tuple[dict[str, int | float | None], dict[str, int | float | None], str, tuple[int, int] | None]:
    # Holder snapshots are tiny and updated with each filing ingest. They avoid the
    # landing page scanning a dozen large position periods just to find the latest one.
    coverage_rows = db.execute(
        select(InstitutionalHolder.latest_report_year, InstitutionalHolder.latest_report_quarter, func.count(InstitutionalHolder.cik))
        .where(InstitutionalHolder.latest_report_year.is_not(None), InstitutionalHolder.latest_report_quarter.is_not(None))
        .group_by(InstitutionalHolder.latest_report_year, InstitutionalHolder.latest_report_quarter)
        .order_by(InstitutionalHolder.latest_report_year.desc(), InstitutionalHolder.latest_report_quarter.desc())
        .limit(12)
    ).all()
    # A new quarter appears before all 13F managers have filed. Use the latest
    # holder period with comparable coverage, rather than displaying a false collapse.
    baseline_count = max((int(row[2] or 0) for row in coverage_rows), default=0)
    minimum_count = max(INSTITUTIONAL_PERIOD_MIN_INSTITUTIONS, int(baseline_count * INSTITUTIONAL_PERIOD_MIN_COVERAGE_RATIO))
    selected = next((row for row in coverage_rows if int(row[2] or 0) >= minimum_count), None)
    period = (int(selected[0]), int(selected[1])) if selected else None
    empty = {"institutions": 0, "portfolio_value": None}
    if period is None:
        return empty, empty.copy(), "latest reported quarter vs prior comparable quarter", None
    year, quarter = period
    previous_period = _previous_quarter(year, quarter)

    def period_values(target: tuple[int, int] | None) -> dict[str, int | float | None]:
        if target is None:
            return empty.copy()
        target_year, target_quarter = target
        institutions, portfolio_value = db.execute(
            select(func.count(func.distinct(InstitutionalPosition.cik)), func.sum(InstitutionalPosition.value_usd))
            .where(InstitutionalPosition.report_year == target_year, InstitutionalPosition.report_quarter == target_quarter)
        ).one()
        return {"institutions": int(institutions or 0), "portfolio_value": _float_or_int(portfolio_value)}

    previous_values = period_values(previous_period)
    comparison = f"Q{quarter} {year} vs Q{previous_period[1]} {previous_period[0]}"
    if not previous_values["institutions"]:
        comparison = f"Q{quarter} {year}; prior comparable quarter pending"
    return period_values(period), previous_values, comparison, period


def _profile_activity_by_month(db: Session, *, include_institutions: bool) -> list[dict[str, Any]]:
    current_month = date.today().replace(day=1)
    start_month = _add_months(current_month, -11)
    start = datetime.combine(start_month, datetime.min.time(), tzinfo=timezone.utc)
    end = _add_months(current_month, 1)
    event_types = ["congress_trade", "insider_trade", "government_contract"]
    if include_institutions:
        event_types.extend(value for value in PROFILE_ACTIVITY_TYPES if value not in event_types)
    kind = case(
        (Event.event_type == "congress_trade", "Congress"),
        (Event.event_type == "insider_trade", "Insider"),
        (Event.event_type == "government_contract", "Department"),
        else_="Institution",
    )
    rows = db.execute(
        select(func.extract("year", Event.ts), func.extract("month", Event.ts), kind, func.count(Event.id))
        .where(Event.event_type.in_(event_types), Event.ts >= start, Event.ts < datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc))
        .group_by(func.extract("year", Event.ts), func.extract("month", Event.ts), kind)
    ).all()
    counts = {(int(year), int(month), str(profile_type)): int(total or 0) for year, month, profile_type, total in rows}
    result: list[dict[str, Any]] = []
    for offset in range(12):
        month = _add_months(start_month, offset)
        result.append({
            "period": month.strftime("%b %y"),
            "Congress": counts.get((month.year, month.month, "Congress"), 0),
            "Insider": counts.get((month.year, month.month, "Insider"), 0),
            "Institution": counts.get((month.year, month.month, "Institution"), 0),
            "Department": counts.get((month.year, month.month, "Department"), 0),
        })
    return result


def _profile_activity_mix(
    db: Session,
    *,
    congress_trades: int,
    insider_trades: int,
    institutional_period: tuple[int, int] | None,
    department_events: int,
) -> list[dict[str, Any]]:
    return [
        {"type": "Congress", "value": int(congress_trades or 0)},
        {"type": "Insider", "value": int(insider_trades or 0)},
        {"type": "Institution", "value": _institutional_activity_count(db, institutional_period)},
        {"type": "Department", "value": int(department_events or 0)},
    ]


def _institutional_activity_count(db: Session, period: tuple[int, int] | None) -> int:
    if period is None:
        return 0
    year, quarter = period
    changes = int(
        db.execute(
            select(func.count(InstitutionalPositionChange.id)).where(
                InstitutionalPositionChange.report_year == year,
                InstitutionalPositionChange.report_quarter == quarter,
            )
        ).scalar_one()
        or 0
    )
    if changes:
        return changes
    return int(
        db.execute(
            select(func.count(InstitutionalPosition.id)).where(
                InstitutionalPosition.report_year == year,
                InstitutionalPosition.report_quarter == quarter,
            )
        ).scalar_one()
        or 0
    )


def _top_profile_sector_movers(db: Session, *, include_institutions: bool, limit: int = 8) -> list[dict[str, Any]]:
    today = datetime.now(timezone.utc)
    current_since = today - timedelta(days=365)
    previous_since = current_since - timedelta(days=365)
    event_types = ["congress_trade", "insider_trade", "government_contract"]
    if include_institutions:
        event_types.extend(value for value in PROFILE_ACTIVITY_TYPES if value not in event_types)
    symbol = func.upper(Event.symbol)
    current_count = func.sum(case((Event.ts >= current_since, 1), else_=0))
    rows = db.execute(
        select(Event.event_type, symbol, current_count, func.count(Event.id))
        .where(Event.event_type.in_(event_types), Event.symbol.is_not(None), Event.ts >= previous_since)
        .group_by(Event.event_type, symbol)
    ).all()
    sector_by_symbol = _sectors(db, [row[1] for row in rows])
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"current": 0, "previous": 0, "profiles": defaultdict(int)})
    for event_type, ticker, current, total in rows:
        sector = _normalize_sector_label(sector_by_symbol.get(ticker))
        if not sector:
            continue
        current_value = int(current or 0)
        total_value = int(total or 0)
        bucket = buckets[sector]
        bucket["current"] += current_value
        bucket["previous"] += total_value - current_value
        bucket["profiles"][_profile_kind(str(event_type))] += current_value
    movers = [
        {
            "sector": sector,
            "current_value": int(values["current"]),
            "previous_value": int(values["previous"]),
            "change": int(values["current"] - values["previous"]),
            "segments": [
                {"type": profile_type, "value": int(values["profiles"].get(profile_type, 0))}
                for profile_type in ("Congress", "Insider", "Institution", "Department")
            ],
        }
        for sector, values in buckets.items()
        if values["current"] or values["previous"]
    ]
    movers.sort(key=lambda row: (abs(row["change"]), row["current_value"]), reverse=True)
    return movers[:limit]


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


def _active_insider_count(db: Session, *, since: date | None = None, symbols: set[str] | None = None) -> int:
    normalized_owner = func.coalesce(InsiderTransactionNormalized.reporting_owner_cik, InsiderTransactionNormalized.reporting_owner_name)
    normalized_filters = [*_insider_transaction_filters(since=since, symbols=symbols), normalized_owner.is_not(None)]
    normalized_count = int(db.execute(select(func.count(func.distinct(normalized_owner))).where(*normalized_filters)).scalar_one() or 0)
    if normalized_count:
        return normalized_count

    legacy_owner = func.coalesce(InsiderTransaction.reporting_cik, InsiderTransaction.insider_name)
    legacy_filters = [legacy_owner.is_not(None)]
    if since is not None:
        legacy_filters.append(InsiderTransaction.transaction_date >= since)
    if symbols is not None:
        legacy_filters.append(func.upper(InsiderTransaction.symbol).in_(symbols))
    legacy_count = int(db.execute(select(func.count(func.distinct(legacy_owner))).where(*legacy_filters)).scalar_one() or 0)
    if legacy_count:
        return legacy_count

    event_filters = [Event.event_type == "insider_trade", Event.member_name.is_not(None)]
    if since is not None:
        event_filters.append(Event.ts >= datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc))
    if symbols is not None:
        event_filters.append(func.upper(Event.symbol).in_(symbols))
    return int(db.execute(select(func.count(func.distinct(Event.member_name))).where(*event_filters)).scalar_one() or 0)


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


def _insider_transaction_filters(*, since: date | None = None, before: date | None = None, symbols: set[str] | None = None) -> list[Any]:
    clauses = [
        InsiderTransactionNormalized.ticker_normalized.is_not(None),
        InsiderTransactionNormalized.transaction_date.is_not(None),
        InsiderTransactionNormalized.value.is_not(None),
        InsiderTransactionNormalized.value > 0,
        InsiderTransactionNormalized.value <= MAX_REASONABLE_INSIDER_TRADE_VALUE,
        InsiderTransactionNormalized.is_duplicate.is_(False),
        InsiderTransactionNormalized.is_derivative.is_(False),
        or_(_insider_side_clause("buy"), _insider_side_clause("sell")),
    ]
    if since is not None:
        clauses.append(InsiderTransactionNormalized.transaction_date >= since)
    if before is not None:
        clauses.append(InsiderTransactionNormalized.transaction_date < before)
    if symbols is not None:
        clauses.append(func.upper(InsiderTransactionNormalized.ticker_normalized).in_(symbols))
    return clauses


def _insider_side_clause(side: str) -> Any:
    code = func.lower(func.coalesce(InsiderTransactionNormalized.transaction_code, ""))
    kind = func.lower(func.coalesce(InsiderTransactionNormalized.transaction_type_normalized, ""))
    if side == "buy":
        return or_(code == "p", kind.in_(("purchase", "open_market_purchase")), kind.like("%purchase%"))
    return or_(code == "s", kind.in_(("sale", "sell", "open_market_sale")), kind.like("%sale%"))


def _sum_insider_transaction_value(db: Session, clauses: list[Any]) -> float | None:
    return _float_or_int(db.execute(select(func.sum(InsiderTransactionNormalized.value)).where(*clauses)).scalar_one())


def _top_congress_members(db: Session, clauses: list[Any], *, limit: int = 10) -> list[dict[str, Any]]:
    member_name = func.trim(Event.member_name)
    member_key = func.lower(member_name)
    rows = db.execute(
        select(
            func.max(Event.member_bioguide_id).label("member_bioguide_id"),
            func.max(member_name).label("member_name"),
            func.max(Event.party).label("party"),
            func.max(Event.chamber).label("chamber"),
            func.count(Event.id).label("trades"),
            func.sum(func.coalesce(Event.amount_max, Event.amount_min)).label("value"),
            func.max(Event.ts).label("latest"),
        )
        .where(*clauses, Event.member_name.is_not(None), member_name != "")
        .group_by(member_key)
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


def _most_active_congress_members(db: Session, clauses: list[Any], *, limit: int = 10) -> list[dict[str, Any]]:
    member_name = func.trim(Event.member_name)
    identity_key = func.lower(member_name)
    rows = db.execute(
        select(
            func.max(member_name),
            func.count(Event.id).label("trades"),
        )
        .where(*clauses, Event.member_name.is_not(None), member_name != "")
        .group_by(identity_key)
        .order_by(func.count(Event.id).desc(), func.max(Event.ts).desc())
        .limit(limit)
    ).all()
    return [
        {
            "name": name or "Member unavailable",
            "trades": int(trades or 0),
            "href": _member_href(name),
        }
        for name, trades in rows
    ]


def _top_event_actors(db: Session, clauses: list[Any], *, sides: tuple[str, ...], limit: int = 8) -> list[dict[str, Any]]:
    actor_key = func.lower(func.trim(Event.member_name))
    rows = db.execute(
        select(
            func.max(Event.member_name),
            func.max(Event.member_bioguide_id),
            func.count(Event.id),
            func.sum(func.coalesce(Event.amount_max, Event.amount_min)),
            func.max(Event.ts),
        )
        .where(*clauses, _side_clause(sides), Event.member_name.is_not(None), actor_key != "")
        .group_by(actor_key)
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
            func.count(Event.id).label("trades"),
        )
        .where(*clauses, Event.symbol.is_not(None))
        .group_by(func.upper(Event.symbol))
        .order_by(func.count(Event.id).desc(), func.sum(side_value).desc().nullslast())
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
            "trades": int(row.trades or 0),
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


def _congress_member_identity() -> Any:
    return func.coalesce(
        func.nullif(func.lower(func.trim(Event.member_name)), ""),
        func.nullif(func.lower(func.trim(Event.member_bioguide_id)), ""),
    )


def _congress_market_analytics(db: Session, *, since: datetime, chamber: str) -> dict[str, Any]:
    rows = db.execute(select(Event.ts, Event.symbol, Event.trade_type, Event.transaction_type, Event.amount_min, Event.amount_max, Event.member_name, Event.member_bioguide_id, Event.chamber).where(*_event_query("congress_trade", since=since, chamber=chamber)).order_by(Event.ts.asc())).all()
    sectors = _sectors(db, [row.symbol for row in rows]); now = datetime.now(timezone.utc); current_since = now - timedelta(days=365); months = [_add_months(date(now.year, now.month, 1), offset) for offset in range(-11, 1)]
    monthly: dict[date, dict[str, Any]] = {month: {"trades": 0, "buy": 0.0, "sell": 0.0, "value": 0.0, "members": set()} for month in months}; exposure: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float)); sector_data: dict[str, dict[str, Any]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "prior_buy": 0.0, "prior_sell": 0.0, "trades": 0, "trend": defaultdict(float)}); chambers: dict[str, float] = defaultdict(float)
    for ts, symbol, trade_type, transaction_type, amount_min, amount_max, member_name, member_id, row_chamber in rows:
        if not isinstance(ts, datetime): continue
        ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts; value = float(amount_max or amount_min or 0); buy = str(trade_type or transaction_type or "").lower() in {"buy", "purchase", "p-purchase"}; sector = sectors.get(normalize_symbol(symbol) or "") or "Other"; exposure[_quarter_label(ts)][sector] += value; values = sector_data[sector]
        if ts >= current_since:
            values["buy" if buy else "sell"] += value; values["trades"] += 1; values["trend"][date(ts.year, ts.month, 1).strftime("%b %y")] += value; chambers[(str(row_chamber or "Unknown").title() or "Unknown")] += value; month = date(ts.year, ts.month, 1)
            if month in monthly:
                point = monthly[month]; point["trades"] += 1; point["value"] += value; point["buy" if buy else "sell"] += value
                member_identity = str(member_name or member_id or "").strip().casefold()
                if member_identity: point["members"].add(member_identity)
        else: values["prior_buy" if buy else "prior_sell"] += value
    total_sector_trades = sum(int(value["trades"] or 0) for value in sector_data.values())
    activity = [{"sector": sector, "current_value": _float_or_int(value["buy"] - value["sell"]) or 0, "previous_value": _float_or_int(value["prior_buy"] - value["prior_sell"]) or 0, "current_activity_value": _float_or_int(value["buy"] + value["sell"]) or 0, "previous_activity_value": _float_or_int(value["prior_buy"] + value["prior_sell"]) or 0, "change_pct": _float_or_int(_change_pct(value["buy"] + value["sell"], value["prior_buy"] + value["prior_sell"])), "buy_value": _float_or_int(value["buy"]) or 0, "sell_value": _float_or_int(value["sell"]) or 0, "trades": int(value["trades"] or 0), "trade_percent": round((int(value["trades"] or 0) / total_sector_trades) * 100, 1) if total_sector_trades else 0, "trend": [{"label": month.strftime("%b %y"), "value": _float_or_int(value["trend"].get(month.strftime("%b %y"), 0)) or 0} for month in months]} for sector, value in sector_data.items()]; activity.sort(key=lambda row: abs(float(row["current_value"])), reverse=True)
    return {"monthly_activity": [{"period": month.strftime("%b %y"), "trades": value["trades"], "buy_value": _float_or_int(value["buy"]) or 0, "sell_value": _float_or_int(value["sell"]) or 0, "active_members": len(value["members"]), "average_trade_size": _float_or_int(value["value"] / value["trades"]) if value["trades"] else None} for month, value in monthly.items()], "sector_exposure": _allocation_payload(exposure), "sector_activity": activity[:10], "top_moving_sectors": sorted(activity, key=lambda row: abs(float(row.get("change_pct") or 0)), reverse=True)[:6], "chamber_mix": [{"label": label, "value": _float_or_int(value) or 0} for label, value in sorted(chambers.items(), key=lambda item: -item[1])]}


def _top_insiders(db: Session, clauses: list[Any], *, limit: int = 5) -> list[dict[str, Any]]:
    owner_key = func.coalesce(InsiderTransactionNormalized.reporting_owner_cik, InsiderTransactionNormalized.reporting_owner_name)
    symbol_key = func.upper(InsiderTransactionNormalized.ticker_normalized)
    buy_value = func.sum(case((_insider_side_clause("buy"), InsiderTransactionNormalized.value), else_=0))
    sell_value = func.sum(case((_insider_side_clause("sell"), InsiderTransactionNormalized.value), else_=0))
    director_flag = func.max(case((InsiderTransactionNormalized.is_director.is_(True), 1), else_=0))
    officer_flag = func.max(case((InsiderTransactionNormalized.is_officer.is_(True), 1), else_=0))
    ten_percent_owner_flag = func.max(case((InsiderTransactionNormalized.is_ten_percent_owner.is_(True), 1), else_=0))
    rows = db.execute(
        select(
            func.max(InsiderTransactionNormalized.reporting_owner_name),
            func.max(InsiderTransactionNormalized.reporting_owner_cik),
            symbol_key,
            func.max(InsiderTransactionNormalized.issuer_name),
            func.max(InsiderTransactionNormalized.officer_title),
            director_flag,
            officer_flag,
            ten_percent_owner_flag,
            buy_value,
            sell_value,
            func.count(InsiderTransactionNormalized.id),
            func.max(InsiderTransactionNormalized.transaction_date),
        )
        .where(*clauses)
        .where(owner_key.is_not(None), symbol_key.is_not(None))
        .group_by(owner_key, symbol_key)
        .having((buy_value - sell_value) > 0)
        .order_by((buy_value - sell_value).desc())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[2] for row in rows])
    return [
        {
            "name": row[0] or "Named insider",
            "reporting_cik": row[1],
            "symbol": row[2],
            "company": company_names.get(row[2]) or row[3] or row[2],
            "role": _insider_role(row[4], row[5], row[6], row[7]),
            "net_buy_value": _float_or_int((row[8] or 0) - (row[9] or 0)),
            "trades": int(row[10] or 0),
            "last_transaction": _date_iso(row[11]),
            "href": _insider_href(row[0], row[1]),
        }
        for row in rows
    ]


def _most_traded_insider_stocks(db: Session, clauses: list[Any], *, limit: int = 5) -> list[dict[str, Any]]:
    owner_key = func.coalesce(InsiderTransactionNormalized.reporting_owner_cik, InsiderTransactionNormalized.reporting_owner_name)
    symbol_key = func.upper(InsiderTransactionNormalized.ticker_normalized)
    buy_value = func.sum(case((_insider_side_clause("buy"), InsiderTransactionNormalized.value), else_=0))
    sell_value = func.sum(case((_insider_side_clause("sell"), InsiderTransactionNormalized.value), else_=0))
    rows = db.execute(
        select(
            symbol_key,
            func.max(InsiderTransactionNormalized.issuer_name),
            buy_value,
            sell_value,
            func.count(func.distinct(owner_key)),
        )
        .where(*clauses, symbol_key.is_not(None))
        .group_by(symbol_key)
        .order_by((buy_value + sell_value).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [
        {
            "symbol": row[0],
            "company": company_names.get(row[0]) or row[1] or row[0],
            "buy_value": _float_or_int(row[2]),
            "sell_value": _float_or_int(row[3]),
            "net_value": _float_or_int((row[2] or 0) - (row[3] or 0)),
            "actor_count": int(row[4] or 0),
            "href": f"/ticker/{row[0]}" if row[0] else None,
        }
        for row in rows
    ]


def _insider_sector_activity(db: Session, clauses: list[Any]) -> list[dict[str, Any]]:
    rows = db.execute(
        select(InsiderTransactionNormalized.transaction_date, func.upper(InsiderTransactionNormalized.ticker_normalized), InsiderTransactionNormalized.value)
        .where(*clauses, InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .order_by(InsiderTransactionNormalized.transaction_date.asc())
    ).all()
    sector_by_symbol = _sectors(db, [row[1] for row in rows])
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for transaction_date, symbol, value in rows:
        sector = sector_by_symbol.get(symbol)
        if not sector:
            continue
        buckets[_quarter_label_for_date(transaction_date)][sector] += float(value or 0)
    return _allocation_payload(buckets)


def _insider_monthly_activity(db: Session, clauses: list[Any]) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    months = [_add_months(date(now.year, now.month, 1), offset) for offset in range(-11, 1)]
    buckets: dict[date, dict[str, Any]] = {month: {"buy": 0.0, "sell": 0.0, "trades": 0.0, "value": 0.0, "insiders": set()} for month in months}
    rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(*clauses)
        .order_by(InsiderTransactionNormalized.transaction_date.asc(), InsiderTransactionNormalized.id.asc())
    ).scalars().all()
    for row in rows:
        if not isinstance(row.transaction_date, date):
            continue
        month = date(row.transaction_date.year, row.transaction_date.month, 1)
        bucket = buckets.get(month)
        if bucket is None:
            continue
        bucket["trades"] += 1
        bucket["value"] += float(row.value or 0)
        owner_key = normalize_cik(row.reporting_owner_cik) or (row.reporting_owner_name or "").strip().casefold()
        if owner_key:
            bucket["insiders"].add(owner_key)
        bucket["buy" if _is_buy_transaction(row) else "sell"] += float(row.value or 0)
    return [
        {
            "period": month.strftime("%b %y"),
            "net_value": _float_or_int(value["buy"] - value["sell"]) or 0,
            "buy_value": _float_or_int(value["buy"]) or 0,
            "sell_value": _float_or_int(value["sell"]) or 0,
            "trades": int(value["trades"] or 0),
            "active_insiders": len(value["insiders"]),
            "average_trade_size": _float_or_int(value["value"] / value["trades"]) if value["trades"] else None,
        }
        for month, value in buckets.items()
    ]


def _insider_sector_net_activity(db: Session, clauses: list[Any]) -> list[dict[str, Any]]:
    rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(*clauses, InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .order_by(InsiderTransactionNormalized.transaction_date.desc(), InsiderTransactionNormalized.id.desc())
        .limit(20000)
    ).scalars().all()
    sector_by_symbol = _sectors(db, [row.ticker_normalized or row.ticker_raw for row in rows])
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "trades": 0.0})
    for row in rows:
        symbol = normalize_symbol(row.ticker_normalized or row.ticker_raw)
        sector = sector_by_symbol.get(symbol or "")
        if not sector:
            continue
        value = float(row.value or 0)
        bucket = buckets[sector]
        bucket["trades"] += 1
        bucket["buy" if _is_buy_transaction(row) else "sell"] += value
    payload = [
        {
            "sector": sector,
            "current_value": _float_or_int(values["buy"] - values["sell"]) or 0,
            "buy_value": _float_or_int(values["buy"]) or 0,
            "sell_value": _float_or_int(values["sell"]) or 0,
            "trades": int(values["trades"] or 0),
        }
        for sector, values in buckets.items()
    ]
    return sorted(payload, key=lambda row: abs(float(row["current_value"])), reverse=True)[:10]


def _insider_role_mix(db: Session, clauses: list[Any]) -> list[dict[str, Any]]:
    rows = db.execute(
        select(
            InsiderTransactionNormalized.officer_title,
            InsiderTransactionNormalized.is_director,
            InsiderTransactionNormalized.is_officer,
            InsiderTransactionNormalized.is_ten_percent_owner,
        )
        .where(*clauses)
        .limit(20000)
    ).all()
    buckets = {"CEOs": 0, "Directors": 0, "10% Owners": 0, "Officers": 0, "Other": 0}
    for officer_title, is_director, is_officer, is_ten_percent_owner in rows:
        buckets[_insider_role_bucket(officer_title, is_director, is_officer, is_ten_percent_owner)] += 1
    total = sum(buckets.values())
    payload = [
        {"label": label, "value": value, "percent": round((value / total) * 100, 1) if total else 0}
        for label, value in buckets.items()
        if value
    ]
    return sorted(payload, key=lambda row: row["value"], reverse=True)


def _insider_top_moving_sectors(db: Session, current_clauses: list[Any], previous_clauses: list[Any], *, limit: int = 6) -> list[dict[str, Any]]:
    current_rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(*current_clauses, InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .order_by(InsiderTransactionNormalized.transaction_date.asc(), InsiderTransactionNormalized.id.asc())
    ).scalars().all()
    previous_rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(*previous_clauses, InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .order_by(InsiderTransactionNormalized.transaction_date.asc(), InsiderTransactionNormalized.id.asc())
    ).scalars().all()
    sector_by_symbol = _sectors(db, [row.ticker_normalized or row.ticker_raw for row in [*current_rows, *previous_rows]])
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "previous_activity": 0.0, "trades": 0, "trend": defaultdict(float)})
    for row in current_rows:
        symbol = normalize_symbol(row.ticker_normalized or row.ticker_raw)
        sector = sector_by_symbol.get(symbol or "")
        if not sector:
            continue
        value = float(row.value or 0)
        bucket = buckets[sector]
        bucket["trades"] += 1
        bucket["buy" if _is_buy_transaction(row) else "sell"] += value
        if isinstance(row.transaction_date, date):
            bucket["trend"][date(row.transaction_date.year, row.transaction_date.month, 1).strftime("%b %y")] += value if _is_buy_transaction(row) else -value
    for row in previous_rows:
        symbol = normalize_symbol(row.ticker_normalized or row.ticker_raw)
        sector = sector_by_symbol.get(symbol or "")
        if not sector:
            continue
        buckets[sector]["previous_activity"] += float(row.value or 0)
    months = [_add_months(date.today().replace(day=1), offset).strftime("%b %y") for offset in range(-11, 1)]
    payload = []
    for sector, values in buckets.items():
        buy_value = float(values["buy"] or 0)
        sell_value = float(values["sell"] or 0)
        current_activity = buy_value + sell_value
        previous_activity = float(values["previous_activity"] or 0)
        payload.append(
            {
                "sector": sector,
                "current_value": _float_or_int(buy_value - sell_value) or 0,
                "previous_value": _float_or_int(previous_activity) or 0,
                "current_activity_value": _float_or_int(current_activity) or 0,
                "previous_activity_value": _float_or_int(previous_activity) or 0,
                "change_pct": _float_or_int(_change_pct(current_activity, previous_activity)),
                "buy_value": _float_or_int(buy_value) or 0,
                "sell_value": _float_or_int(sell_value) or 0,
                "trades": int(values["trades"] or 0),
                "trend": [{"label": month, "value": _float_or_int(values["trend"].get(month, 0)) or 0} for month in months],
            }
        )
    payload.sort(key=lambda row: (abs(float(row["current_value"])), float(row["current_activity_value"])), reverse=True)
    return payload[:limit]


def _recent_insider_transactions(db: Session, clauses: list[Any], *, limit: int) -> list[dict[str, Any]]:
    rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(*clauses)
        .order_by(InsiderTransactionNormalized.transaction_date.desc(), InsiderTransactionNormalized.id.desc())
        .limit(limit)
    ).scalars().all()
    return _insider_transaction_payloads(db, rows)


def _largest_insider_transactions(db: Session, clauses: list[Any], *, limit: int) -> list[dict[str, Any]]:
    rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(*clauses)
        .order_by(InsiderTransactionNormalized.value.desc().nullslast(), InsiderTransactionNormalized.transaction_date.desc())
        .limit(limit)
    ).scalars().all()
    return _insider_transaction_payloads(db, rows)


def _insider_transaction_payloads(db: Session, rows: list[InsiderTransactionNormalized]) -> list[dict[str, Any]]:
    company_names = _company_names(db, [row.ticker_normalized or row.ticker_raw for row in rows])
    return [_insider_transaction_payload(row, company_names) for row in rows]


def _insider_transaction_payload(row: InsiderTransactionNormalized, company_names: dict[str, str]) -> dict[str, Any]:
    symbol = normalize_symbol(row.ticker_normalized or row.ticker_raw)
    owner_name = row.reporting_owner_name or "Named insider"
    return {
        "id": row.id,
        "time": _date_iso(row.transaction_date or row.filing_date),
        "type": "Insider",
        "profile": owner_name,
        "profile_href": _insider_href(owner_name, row.reporting_owner_cik),
        "symbol": symbol,
        "company": company_names.get(symbol or "") or row.issuer_name or symbol,
        "ticker_href": f"/ticker/{symbol}" if symbol else None,
        "activity": "Open-Market Purchase" if _is_buy_transaction(row) else "Open-Market Sale",
        "value": _float_or_int(row.value),
        "metric": None,
    }


def _is_buy_transaction(row: InsiderTransactionNormalized) -> bool:
    code = (row.transaction_code or "").strip().lower()
    kind = (row.transaction_type_normalized or "").strip().lower()
    return code == "p" or "purchase" in kind


def _insider_role_bucket(officer_title: Any, is_director: Any, is_officer: Any, is_ten_percent_owner: Any) -> str:
    title = str(officer_title or "").strip().lower()
    if "chief executive officer" in title or " ceo" in f" {title}" or "president and ceo" in title:
        return "CEOs"
    if is_director:
        return "Directors"
    if is_ten_percent_owner:
        return "10% Owners"
    if is_officer or title:
        return "Officers"
    return "Other"


def _insider_role(officer_title: Any, is_director: Any, is_officer: Any, is_ten_percent_owner: Any) -> str:
    roles: list[str] = []
    title = str(officer_title).strip() if officer_title else ""
    if title:
        roles.append(title)
    elif is_officer:
        roles.append("Officer")
    if is_director:
        roles.append("Director")
    if is_ten_percent_owner:
        roles.append("10 percent owner")
    return ", ".join(dict.fromkeys(roles)) or "-"


def _recent_event_rows(db: Session, clauses: list[Any], *, limit: int) -> list[dict[str, Any]]:
    rows = db.execute(select(Event).where(*clauses).order_by(Event.ts.desc(), Event.id.desc()).limit(limit)).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [_activity_payload(row, company_names) for row in rows]


def _largest_event_rows(db: Session, clauses: list[Any], *, limit: int) -> list[dict[str, Any]]:
    rows = db.execute(select(Event).where(*clauses).order_by(func.coalesce(Event.amount_max, Event.amount_min).desc().nullslast(), Event.ts.desc()).limit(limit)).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [_activity_payload(row, company_names) for row in rows]


def _cluster_buying(db: Session, *, since: datetime, symbols: set[str] | None = None, limit: int = 8) -> list[dict[str, Any]]:
    owner_key = func.coalesce(InsiderTransactionNormalized.reporting_owner_cik, InsiderTransactionNormalized.reporting_owner_name)
    symbol_key = func.upper(InsiderTransactionNormalized.ticker_normalized)
    clauses = [*_insider_transaction_filters(since=since.date(), symbols=symbols), _insider_side_clause("buy")]
    rows = db.execute(
        select(symbol_key, func.max(InsiderTransactionNormalized.issuer_name), func.count(func.distinct(owner_key)), func.sum(InsiderTransactionNormalized.value), func.max(InsiderTransactionNormalized.transaction_date))
        .where(*clauses, owner_key.is_not(None), symbol_key.is_not(None))
        .group_by(symbol_key)
        .having(func.count(func.distinct(owner_key)) >= 2)
        .order_by(func.count(func.distinct(owner_key)).desc(), func.sum(InsiderTransactionNormalized.value).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [
        {"symbol": row[0], "company": company_names.get(row[0]) or row[1] or row[0], "unique_insiders": int(row[2] or 0), "buy_value": _float_or_int(row[3]), "last_transaction": _date_iso(row[4]), "href": f"/ticker/{row[0]}"}
        for row in rows
    ]


def _institutional_periods(db: Session) -> list[tuple[int, int]]:
    """List reported periods from filings, avoiding a full positions-table scan."""
    rows = db.execute(
        select(InstitutionalFiling.report_year, InstitutionalFiling.report_quarter)
        .group_by(InstitutionalFiling.report_year, InstitutionalFiling.report_quarter)
        .order_by(InstitutionalFiling.report_year.desc(), InstitutionalFiling.report_quarter.desc())
        .limit(12)
    ).all()
    return [(int(row[0]), int(row[1])) for row in rows]


def _latest_institutional_period(
    db: Session,
    *,
    year: int | None = None,
    quarter: int | None = None,
    periods: list[tuple[int, int]] | None = None,
) -> tuple[int, int] | None:
    if year and quarter:
        return int(year), int(quarter)
    periods = periods if periods is not None else _institutional_periods(db)
    complete_periods = _complete_institutional_periods(db, periods)
    return complete_periods[0] if complete_periods else (periods[0] if periods else None)


def _complete_institutional_periods(db: Session, periods: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not periods:
        return []
    coverage = _institutional_period_coverage(db, periods)
    complete_periods = []
    for index, period in enumerate(periods):
        institutions = coverage.get(period, {}).get("institutions", 0)
        if institutions <= 0:
            continue
        previous_period = periods[index + 1] if index + 1 < len(periods) else None
        previous_institutions = coverage.get(previous_period, {}).get("institutions", 0) if previous_period else 0
        minimum_institutions = max(INSTITUTIONAL_PERIOD_MIN_INSTITUTIONS, int(previous_institutions * INSTITUTIONAL_PERIOD_MIN_COVERAGE_RATIO))
        if previous_institutions >= INSTITUTIONAL_PERIOD_MIN_INSTITUTIONS and institutions < minimum_institutions:
            continue
        complete_periods.append(period)
    return complete_periods


def _institutional_period_coverage(db: Session, periods: list[tuple[int, int]]) -> dict[tuple[int, int], dict[str, int]]:
    if not periods:
        return {}
    rows = db.execute(
        select(
            InstitutionalFiling.report_year,
            InstitutionalFiling.report_quarter,
            func.count(func.distinct(InstitutionalFiling.cik)),
            func.count(InstitutionalFiling.id),
        )
        .where(tuple_(InstitutionalFiling.report_year, InstitutionalFiling.report_quarter).in_(periods))  # type: ignore[name-defined]
        .group_by(InstitutionalFiling.report_year, InstitutionalFiling.report_quarter)
    ).all()
    return {
        (int(year), int(quarter)): {"institutions": int(institutions or 0), "positions": int(positions or 0)}
        for year, quarter, institutions, positions in rows
    }


def _previous_comparable_institutional_period(db: Session, year: int, quarter: int) -> tuple[int, int] | None:
    previous = _previous_quarter(year, quarter)
    coverage = _institutional_period_coverage(db, [(int(year), int(quarter)), previous])
    current_institutions = coverage.get((int(year), int(quarter)), {}).get("institutions", 0)
    previous_institutions = coverage.get(previous, {}).get("institutions", 0)
    if current_institutions <= 0 or previous_institutions <= 0:
        return None
    if min(current_institutions, previous_institutions) / max(current_institutions, previous_institutions) < INSTITUTIONAL_PERIOD_MIN_COVERAGE_RATIO:
        return None
    return previous


def _previous_institutional_period_with_data(db: Session, year: int, quarter: int) -> tuple[int, int] | None:
    """Use an available prior filing period for per-institution table comparisons."""
    previous = _previous_quarter(year, quarter)
    count = db.execute(
        select(func.count(InstitutionalPosition.id)).where(
            InstitutionalPosition.report_year == previous[0],
            InstitutionalPosition.report_quarter == previous[1],
        )
    ).scalar_one()
    return previous if count else None


def _previous_quarter(year: int, quarter: int) -> tuple[int, int]:
    return (year - 1, 4) if quarter == 1 else (year, quarter - 1)


def _latest_institutional_value(db: Session) -> float | None:
    period = _latest_institutional_period(db)
    if period is None:
        return None
    year, quarter = period
    return _float_or_int(db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(InstitutionalPosition.report_year == year, InstitutionalPosition.report_quarter == quarter)).scalar_one())


def _top_institutions(db: Session, year: int, quarter: int, *, previous_period: tuple[int, int] | None = None, limit: int = 15) -> list[dict[str, Any]]:
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
    ciks = [str(cik) for cik, *_ in rows if cik]
    top_holdings: dict[str, tuple[Any, Any, Any]] = {}
    if ciks:
        ranked_holdings = (
            select(
                InstitutionalPosition.cik.label("cik"),
                InstitutionalPosition.normalized_symbol.label("symbol"),
                InstitutionalPosition.issuer_name.label("issuer_name"),
                InstitutionalPosition.value_usd.label("value_usd"),
                func.row_number().over(
                    partition_by=InstitutionalPosition.cik,
                    order_by=InstitutionalPosition.value_usd.desc().nullslast(),
                ).label("position_rank"),
            )
            .where(
                InstitutionalPosition.cik.in_(ciks),
                InstitutionalPosition.report_year == year,
                InstitutionalPosition.report_quarter == quarter,
            )
            .subquery()
        )
        top_holdings = {
            str(cik): (symbol, issuer_name, value_usd)
            for cik, symbol, issuer_name, value_usd in db.execute(
                select(
                    ranked_holdings.c.cik,
                    ranked_holdings.c.symbol,
                    ranked_holdings.c.issuer_name,
                    ranked_holdings.c.value_usd,
                ).where(ranked_holdings.c.position_rank == 1)
            ).all()
        }

    previous_values: dict[str, float | None] = {}
    if previous_period and ciks:
        previous_variants = {variant for cik in ciks for variant in _institution_cik_variants(cik)}
        variant_values = {
            str(cik): _float_or_int(value)
            for cik, value in db.execute(
                select(InstitutionalPosition.cik, func.sum(InstitutionalPosition.value_usd))
                .where(
                    InstitutionalPosition.cik.in_(previous_variants),
                    InstitutionalPosition.report_year == previous_period[0],
                    InstitutionalPosition.report_quarter == previous_period[1],
                )
                .group_by(InstitutionalPosition.cik)
            ).all()
        }
        for cik in ciks:
            previous_values[cik] = sum(
                float(variant_values[variant] or 0)
                for variant in _institution_cik_variants(cik)
                if variant in variant_values
            ) or None

    result = []
    for cik, name, value, positions in rows:
        cik_key = str(cik)
        top = top_holdings.get(cik_key)
        previous = previous_values.get(cik_key)
        result.append(
            {
                "name": name or "Institution unavailable",
                "cik": normalize_cik(cik),
                "portfolio_value": _float_or_int(value),
                "previous_value": previous,
                "qoq_change": _change_pct(value, previous),
                "positions": int(positions or 0),
                "largest_holding": {"symbol": top[0], "company": top[1], "value": _float_or_int(top[2])} if top else None,
                "href": f"/institution/{normalize_cik(cik)}" if normalize_cik(cik) else None,
            }
        )
    return result


def _previous_institution_value(db: Session, cik: str, *, previous_period: tuple[int, int] | None) -> float | None:
    if previous_period is None:
        return None
    prev_year, prev_quarter = previous_period
    variants = _institution_cik_variants(cik)
    if not variants:
        return None
    return _float_or_int(db.execute(select(func.sum(InstitutionalPosition.value_usd)).where(InstitutionalPosition.cik.in_(variants), InstitutionalPosition.report_year == prev_year, InstitutionalPosition.report_quarter == prev_quarter)).scalar_one())


def _institution_cik_variants(cik: Any) -> list[str]:
    variants = {str(cik).strip()} if cik is not None and str(cik).strip() else set()
    normalized = normalize_cik(str(cik)) if cik is not None else None
    if normalized:
        variants.add(normalized)
        stripped = normalized.lstrip("0")
        if stripped:
            variants.add(stripped)
    return sorted(variants)


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
    periods = _complete_institutional_periods(db, _institutional_periods(db))[:8]
    if not periods:
        return []
    rows = db.execute(
        select(
            InstitutionalSymbolSummary.report_year,
            InstitutionalSymbolSummary.report_quarter,
            func.upper(InstitutionalSymbolSummary.normalized_symbol),
            InstitutionalSymbolSummary.total_value_usd,
        )
        .where(
            tuple_(InstitutionalSymbolSummary.report_year, InstitutionalSymbolSummary.report_quarter).in_(periods),  # type: ignore[name-defined]
            InstitutionalSymbolSummary.total_value_usd.is_not(None),
        )
    ).all()
    if not rows:
        rows = db.execute(
            select(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter, func.upper(InstitutionalPosition.normalized_symbol), func.sum(InstitutionalPosition.value_usd))
            .where(
                tuple_(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter).in_(periods),  # type: ignore[name-defined]
                InstitutionalPosition.normalized_symbol.is_not(None),
                InstitutionalPosition.value_usd.is_not(None),
            )
            .group_by(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter, func.upper(InstitutionalPosition.normalized_symbol))
        ).all()
    sectors = _sectors(db, [row[2] for row in rows])
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for year, quarter, symbol, value in rows:
        sector = _normalize_sector_label(sectors.get(symbol))
        if not sector:
            continue
        buckets[f"Q{quarter} {year}"][sector] += float(value or 0)
    return _allocation_payload(buckets)


def _institutional_activity_over_time(db: Session) -> list[dict[str, Any]]:
    periods = list(reversed(_complete_institutional_periods(db, _institutional_periods(db))[:8]))
    if not periods:
        return []

    change_rows = db.execute(
        select(
            InstitutionalPositionChange.report_year,
            InstitutionalPositionChange.report_quarter,
            func.sum(case((InstitutionalPositionChange.value_delta_usd > 0, InstitutionalPositionChange.value_delta_usd), else_=0)),
            func.sum(case((InstitutionalPositionChange.value_delta_usd < 0, InstitutionalPositionChange.value_delta_usd), else_=0)),
            func.count(case((InstitutionalPositionChange.shares_delta > 0, 1))),
            func.count(case((InstitutionalPositionChange.shares_delta < 0, 1))),
            func.sum(InstitutionalPositionChange.value_delta_usd),
        )
        .where(tuple_(InstitutionalPositionChange.report_year, InstitutionalPositionChange.report_quarter).in_(periods))  # type: ignore[name-defined]
        .group_by(InstitutionalPositionChange.report_year, InstitutionalPositionChange.report_quarter)
    ).all()
    position_rows = db.execute(
        select(
            InstitutionalPosition.report_year,
            InstitutionalPosition.report_quarter,
            func.count(InstitutionalPosition.id),
            func.count(func.distinct(InstitutionalPosition.cik)),
            func.sum(InstitutionalPosition.value_usd),
        )
        .where(tuple_(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter).in_(periods))  # type: ignore[name-defined]
        .group_by(InstitutionalPosition.report_year, InstitutionalPosition.report_quarter)
    ).all()
    changes = {
        (int(year), int(quarter)): (
            float(increases or 0),
            float(decreases or 0),
            int(increase_count or 0),
            int(decrease_count or 0),
            float(net_change or 0),
        )
        for year, quarter, increases, decreases, increase_count, decrease_count, net_change in change_rows
    }
    positions = {
        (int(year), int(quarter)): (int(total_positions or 0), int(tracked_institutions or 0), float(portfolio_value or 0))
        for year, quarter, total_positions, tracked_institutions, portfolio_value in position_rows
    }
    return [
        {
            "period": f"Q{quarter} {year}",
            "position_increase_value": _float_or_int(changes.get((year, quarter), (0.0, 0.0))[0]) or 0,
            "position_decrease_value": _float_or_int(changes.get((year, quarter), (0.0, 0.0))[1]) or 0,
            "position_increase_count": changes.get((year, quarter), (0.0, 0.0, 0, 0, 0.0))[2],
            "position_decrease_count": changes.get((year, quarter), (0.0, 0.0, 0, 0, 0.0))[3],
            "net_value_change": _float_or_int(changes.get((year, quarter), (0.0, 0.0, 0, 0, 0.0))[4]) or 0,
            "total_positions": positions.get((year, quarter), (0, 0, 0.0))[0],
            "tracked_institutions": positions.get((year, quarter), (0, 0, 0.0))[1],
            "portfolio_value": _float_or_int(positions.get((year, quarter), (0, 0, 0.0))[2]) or 0,
        }
        for year, quarter in periods
    ]


def _most_widely_held(db: Session, year: int, quarter: int, *, limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(
        select(
            func.upper(InstitutionalSymbolSummary.normalized_symbol),
            InstitutionalSymbolSummary.total_holders,
            InstitutionalSymbolSummary.total_value_usd,
        )
        .where(
            InstitutionalSymbolSummary.report_year == year,
            InstitutionalSymbolSummary.report_quarter == quarter,
        )
        .order_by(InstitutionalSymbolSummary.total_holders.desc(), InstitutionalSymbolSummary.total_value_usd.desc().nullslast())
        .limit(limit)
    ).all()
    if not rows:
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
    filing_keys = [(filing.cik, filing.report_year, filing.report_quarter) for filing, _name in rows]
    changes_by_filing = _notable_filing_changes(db, filing_keys)
    positions_by_filing = _notable_filing_positions(db, filing_keys)
    return [
        _recent_filing_payload(
            filing,
            name,
            changes_by_filing.get((filing.cik, filing.report_year, filing.report_quarter)),
            positions_by_filing.get((filing.cik, filing.report_year, filing.report_quarter)),
        )
        for filing, name in rows
    ]


def _notable_filing_changes(db: Session, filing_keys: list[tuple[str, int, int]]) -> dict[tuple[str, int, int], InstitutionalPositionChange]:
    if not filing_keys:
        return {}
    rows = db.execute(
        select(InstitutionalPositionChange)
        .where(
            tuple_(InstitutionalPositionChange.cik, InstitutionalPositionChange.report_year, InstitutionalPositionChange.report_quarter).in_(filing_keys),  # type: ignore[arg-type]
            InstitutionalPositionChange.normalized_symbol.is_not(None),
        )
        .order_by(func.abs(InstitutionalPositionChange.value_delta_usd).desc().nullslast())
    ).scalars()
    result: dict[tuple[str, int, int], InstitutionalPositionChange] = {}
    for row in rows:
        result.setdefault((row.cik, row.report_year, row.report_quarter), row)
    return result


def _notable_filing_positions(db: Session, filing_keys: list[tuple[str, int, int]]) -> dict[tuple[str, int, int], InstitutionalPosition]:
    if not filing_keys:
        return {}
    rows = db.execute(
        select(InstitutionalPosition)
        .where(
            tuple_(InstitutionalPosition.cik, InstitutionalPosition.report_year, InstitutionalPosition.report_quarter).in_(filing_keys),  # type: ignore[arg-type]
            InstitutionalPosition.normalized_symbol.is_not(None),
        )
        .order_by(InstitutionalPosition.value_usd.desc().nullslast())
    ).scalars()
    result: dict[tuple[str, int, int], InstitutionalPosition] = {}
    for row in rows:
        result.setdefault((row.cik, row.report_year, row.report_quarter), row)
    return result


def _recent_filing_payload(
    filing: InstitutionalFiling,
    holder_name: str | None,
    change: InstitutionalPositionChange | None,
    position: InstitutionalPosition | None,
) -> dict[str, Any]:
    symbol = (change.normalized_symbol or change.symbol) if change else (position.normalized_symbol or position.symbol) if position else None
    action = _institutional_filing_action(change) if change else "Reported position" if position else None
    value = change.value_delta_usd if change and change.value_delta_usd is not None else change.curr_value_usd if change else position.value_usd if position else None
    return {
        "cik": normalize_cik(filing.cik),
        "name": holder_name or (change.holder_name if change else None) or "Institution unavailable",
        "symbol": symbol,
        "action": action,
        "value": _float_or_int(value),
        "filing_date": _date_iso(filing.filing_date),
        "report_period": f"Q{filing.report_quarter} {filing.report_year}",
        "form_type": filing.form_type,
        "href": f"/institution/{normalize_cik(filing.cik)}",
        "ticker_href": f"/ticker/{symbol}" if symbol else None,
    }


def _institutional_filing_action(change: InstitutionalPositionChange) -> str:
    change_type = (change.change_type or "").replace("_", " ").strip().lower()
    labels = {
        "new": "New position",
        "new position": "New position",
        "exit": "Exited position",
        "exited": "Exited position",
        "increase": "Increased position",
        "decrease": "Reduced position",
    }
    if change_type in labels:
        return labels[change_type]
    if (change.shares_delta or 0) > 0 or (change.value_delta_usd or 0) > 0:
        return "Increased position"
    if (change.shares_delta or 0) < 0 or (change.value_delta_usd or 0) < 0:
        return "Reduced position"
    return "Position update"


def _top_departments(db: Session, *, since: date | None = None, before: date | None = None, previous_since: date | None = None, limit: int = 10) -> list[dict[str, Any]]:
    current_rows = _department_value_rows(db, since=since, before=before)
    previous_rows = _department_value_rows(db, since=previous_since, before=since) if previous_since and since else {}
    names = list(current_rows.keys())[:limit]
    top_vendors = _top_vendor_by_department(db, names, since=since, before=before)
    return [
        {
            "name": name,
            "href": f"/departments/{department_slug(name)}",
            "contract_value": _float_or_int(values["value"]),
            "previous_value": _float_or_int(previous_rows.get(name, {}).get("value")) if previous_rows else None,
            "change_pct": _float_or_int(_change_pct(values["value"], previous_rows.get(name, {}).get("value"))) if previous_rows else None,
            "contracts": int(values["contracts"] or 0),
            "top_vendor": top_vendors.get(name) or "No vendor concentration",
        }
        for name, values in list(current_rows.items())[:limit]
    ]


def _most_active_departments(db: Session, *, since: date | None = None, before: date | None = None, limit: int = 8) -> list[dict[str, Any]]:
    rows = _department_value_rows(db, since=since, before=before, sort_by="contracts")
    return [
        {
            "name": name,
            "href": f"/departments/{department_slug(name)}",
            "contracts": int(values["contracts"] or 0),
            "contract_value": _float_or_int(values["value"]),
        }
        for name, values in list(rows.items())[:limit]
    ]


def _top_vendors(db: Session, *, since: date | None = None, before: date | None = None, limit: int = 10) -> list[dict[str, Any]]:
    filters = [GovernmentContract.symbol.is_not(None)]
    if since is not None:
        filters.append(GovernmentContract.award_date >= since)
    if before is not None:
        filters.append(GovernmentContract.award_date < before)
    rows = db.execute(
        select(func.upper(GovernmentContract.symbol), func.max(GovernmentContract.recipient_name), func.sum(GovernmentContract.award_amount), func.count(GovernmentContract.id), func.max(GovernmentContract.awarding_agency))
        .where(*filters)
        .group_by(func.upper(GovernmentContract.symbol))
        .order_by(func.sum(GovernmentContract.award_amount).desc().nullslast())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [{"vendor": company_names.get(row[0]) or row[1] or row[0], "symbol": row[0], "href": f"/ticker/{row[0]}" if row[0] else None, "contract_value": _float_or_int(row[2]), "contracts": int(row[3] or 0), "top_department": row[4]} for row in rows]


def _contract_value_by_sector_over_time(db: Session, *, since: date | None = None, before: date | None = None) -> list[dict[str, Any]]:
    today = before or date.today() + timedelta(days=1)
    contract_filters = [GovernmentContract.symbol.is_not(None), GovernmentContract.award_date < today]
    action_filters = [GovernmentContractAction.symbol.is_not(None), GovernmentContractAction.action_date < today]
    if since is not None:
        contract_filters.append(GovernmentContract.award_date >= since)
        action_filters.append(GovernmentContractAction.action_date >= since)
    contract_rows = db.execute(
        select(GovernmentContract.award_date, func.upper(GovernmentContract.symbol), func.sum(GovernmentContract.award_amount))
        .where(*contract_filters)
        .group_by(GovernmentContract.award_date, func.upper(GovernmentContract.symbol))
        .order_by(GovernmentContract.award_date.asc())
    ).all()
    action_rows = db.execute(
        select(GovernmentContractAction.action_date, func.upper(GovernmentContractAction.symbol), func.sum(GovernmentContractAction.obligated_amount))
        .where(*action_filters)
        .group_by(GovernmentContractAction.action_date, func.upper(GovernmentContractAction.symbol))
        .order_by(GovernmentContractAction.action_date.asc())
    ).all()
    rows = [*contract_rows, *action_rows]
    sectors = _sectors(db, [row[1] for row in rows])
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for award_date, symbol, amount in rows:
        if not isinstance(award_date, date):
            continue
        sector = _normalize_sector_label(sectors.get(symbol))
        if not sector:
            continue
        period = f"Q{((award_date.month - 1) // 3) + 1} {award_date.year}"
        buckets[period][sector] += float(amount or 0)
    return _allocation_payload(buckets)


def _largest_recent_awards(db: Session, *, since: date | None = None, before: date | None = None, limit: int = 8) -> list[dict[str, Any]]:
    filters = []
    if since is not None:
        filters.append(GovernmentContract.award_date >= since)
    if before is not None:
        filters.append(GovernmentContract.award_date < before)
    rows = db.execute(select(GovernmentContract).where(*filters).order_by(GovernmentContract.award_date.desc(), GovernmentContract.award_amount.desc()).limit(limit)).scalars().all()
    company_names = _company_names(db, [row.symbol for row in rows])
    return [
        {"symbol": normalize_symbol(row.symbol), "company": company_names.get(normalize_symbol(row.symbol) or "") or row.recipient_name, "department": row.awarding_agency, "department_href": f"/departments/{department_slug(row.awarding_agency)}" if row.awarding_agency else None, "value": _float_or_int(row.award_amount), "date": _date_iso(row.award_date), "description": row.description, "href": f"/ticker/{normalize_symbol(row.symbol)}" if normalize_symbol(row.symbol) else None}
        for row in rows
    ]


def _fastest_growing_vendors(db: Session, *, period_days: int = 365, limit: int = 10) -> list[dict[str, Any]]:
    today = date.today()
    current_since = today - timedelta(days=period_days)
    previous_since = current_since - timedelta(days=period_days)
    current_value = func.coalesce(func.sum(case((GovernmentContract.award_date >= current_since, GovernmentContract.award_amount), else_=0)), 0)
    previous_value = func.coalesce(func.sum(case((GovernmentContract.award_date < current_since, GovernmentContract.award_amount), else_=0)), 0)
    change_value = current_value - previous_value
    rows = db.execute(
        select(
            func.upper(GovernmentContract.symbol),
            current_value,
            previous_value,
        )
        .where(GovernmentContract.award_date >= previous_since, GovernmentContract.award_date < today + timedelta(days=1), GovernmentContract.symbol.is_not(None))
        .group_by(func.upper(GovernmentContract.symbol))
        .order_by(change_value.desc())
        .limit(limit)
    ).all()
    company_names = _company_names(db, [row[0] for row in rows])
    return [
        {
            "symbol": symbol,
            "company": company_names.get(symbol) or symbol,
            "current_value": _float_or_int(current),
            "previous_value": _float_or_int(previous),
            "increase_value": _float_or_int(float(current or 0) - float(previous or 0)),
            "href": f"/ticker/{symbol}",
        }
        for symbol, current, previous in rows
    ]


def _government_contract_period_metrics(db: Session, *, since: date, before: date) -> dict[str, Any]:
    contract_filters = [GovernmentContract.award_date >= since, GovernmentContract.award_date < before]
    action_filters = [GovernmentContractAction.action_date >= since, GovernmentContractAction.action_date < before]
    contract_count = int(db.execute(select(func.count(GovernmentContract.id)).where(*contract_filters)).scalar_one() or 0)
    modification_count = int(db.execute(select(func.count(GovernmentContractAction.id)).where(*action_filters)).scalar_one() or 0)
    contract_value = float(db.execute(select(func.sum(GovernmentContract.award_amount)).where(*contract_filters)).scalar_one() or 0)
    action_value = float(db.execute(select(func.sum(GovernmentContractAction.obligated_amount)).where(*action_filters)).scalar_one() or 0)
    contract_symbols = db.execute(select(func.upper(GovernmentContract.symbol)).where(*contract_filters, GovernmentContract.symbol.is_not(None)).group_by(func.upper(GovernmentContract.symbol))).all()
    action_symbols = db.execute(select(func.upper(GovernmentContractAction.symbol)).where(*action_filters, GovernmentContractAction.symbol.is_not(None)).group_by(func.upper(GovernmentContractAction.symbol))).all()
    agencies = union_all(
        select(func.trim(GovernmentContract.awarding_agency).label("agency")).where(*contract_filters, GovernmentContract.awarding_agency.is_not(None)),
        select(func.trim(GovernmentContractAction.awarding_agency).label("agency")).where(*action_filters, GovernmentContractAction.awarding_agency.is_not(None)),
    ).subquery()
    department_count = int(db.execute(select(func.count(func.distinct(agencies.c.agency))).where(agencies.c.agency != "")).scalar_one() or 0)
    contract_count_total = contract_count + modification_count
    total_value = contract_value + action_value
    return {
        "contract_count": contract_count_total,
        "total_value": _float_or_int(total_value),
        "active_vendors": len({row[0] for row in [*contract_symbols, *action_symbols] if row[0]}),
        "departments": department_count,
        "average_size": _float_or_int(total_value / contract_count_total) if contract_count_total else None,
        "modification_count": modification_count,
    }


def _government_contract_period_value(db: Session, *, since: date, before: date) -> float | None:
    values = union_all(
        select(GovernmentContract.award_amount.label("value")).where(GovernmentContract.award_date >= since, GovernmentContract.award_date < before),
        select(GovernmentContractAction.obligated_amount.label("value")).where(GovernmentContractAction.action_date >= since, GovernmentContractAction.action_date < before),
    ).subquery()
    return _float_or_int(db.execute(select(func.sum(values.c.value))).scalar_one())


def _government_contract_comparison_status(db: Session, *, today: date, period_days: int) -> dict[str, Any]:
    month_end = date(today.year, today.month, 1)
    month_start = _add_months(month_end, -6)
    previous_month_start = _add_months(month_start, -12)
    previous_month_end = _add_months(month_end, -12)
    current = _government_contract_period_metrics(db, since=month_start, before=month_end)
    previous = _government_contract_period_metrics(db, since=previous_month_start, before=previous_month_end)
    current_rows = int(current.get("contract_count") or 0)
    previous_rows = int(previous.get("contract_count") or 0)
    coverage_ratio = (current_rows / previous_rows) if previous_rows else None
    latest_contract_date = db.execute(
        select(func.max(GovernmentContract.award_date)).where(GovernmentContract.award_date < today + timedelta(days=1))
    ).scalar_one_or_none()
    latest_action_date = db.execute(
        select(func.max(GovernmentContractAction.action_date)).where(GovernmentContractAction.action_date < today + timedelta(days=1))
    ).scalar_one_or_none()
    status = "ok"
    message = None
    if (
        coverage_ratio is not None
        and previous_rows >= GOVERNMENT_CONTRACT_COMPARISON_MIN_PRIOR_ROWS
        and coverage_ratio < GOVERNMENT_CONTRACT_COMPARISON_MIN_COVERAGE_RATIO
    ):
        status = "undercovered"
        message = "Comparison paused because recent government-contract ingest coverage is materially below the matching prior-year months."
    return {
        "status": status,
        "label": f"previous {period_days} days" if status == "ok" else "comparison pending ingest backfill",
        "message": message,
        "coverage_ratio": _float_or_int((coverage_ratio * 100.0) if coverage_ratio is not None else None),
        "current_recent_rows": current_rows,
        "previous_recent_rows": previous_rows,
        "latest_contract_date": _date_iso(latest_contract_date),
        "latest_action_date": _date_iso(latest_action_date),
    }


def _add_months(value: date, months: int) -> date:
    month_index = (value.year * 12 + value.month - 1) + months
    return date(month_index // 12, month_index % 12 + 1, 1)


def _department_value_rows(db: Session, *, since: date | None = None, before: date | None = None, sort_by: str = "value") -> dict[str, dict[str, float]]:
    contract_filters = []
    action_filters = []
    if since is not None:
        contract_filters.append(GovernmentContract.award_date >= since)
        action_filters.append(GovernmentContractAction.action_date >= since)
    if before is not None:
        contract_filters.append(GovernmentContract.award_date < before)
        action_filters.append(GovernmentContractAction.action_date < before)

    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"value": 0.0, "contracts": 0.0})
    contract_rows = db.execute(
        select(GovernmentContract.awarding_agency, func.sum(GovernmentContract.award_amount), func.count(GovernmentContract.id))
        .where(*contract_filters)
        .group_by(GovernmentContract.awarding_agency)
    ).all()
    action_rows = db.execute(
        select(GovernmentContractAction.awarding_agency, func.sum(GovernmentContractAction.obligated_amount), func.count(GovernmentContractAction.id))
        .where(*action_filters)
        .group_by(GovernmentContractAction.awarding_agency)
    ).all()
    for agency, value, count in [*contract_rows, *action_rows]:
        name = (agency or "Unspecified Department").strip() or "Unspecified Department"
        buckets[name]["value"] += float(value or 0)
        buckets[name]["contracts"] += int(count or 0)

    sort_key = "contracts" if sort_by == "contracts" else "value"
    return dict(sorted(buckets.items(), key=lambda item: (-item[1][sort_key], item[0])))


def _top_vendor_by_department(db: Session, names: list[str], *, since: date | None = None, before: date | None = None) -> dict[str, str]:
    if not names:
        return {}
    name_set = set(names)
    contract_filters = [GovernmentContract.awarding_agency.in_(names)]
    action_filters = [GovernmentContractAction.awarding_agency.in_(names)]
    if since is not None:
        contract_filters.append(GovernmentContract.award_date >= since)
        action_filters.append(GovernmentContractAction.action_date >= since)
    if before is not None:
        contract_filters.append(GovernmentContract.award_date < before)
        action_filters.append(GovernmentContractAction.action_date < before)

    buckets: dict[str, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {"value": 0.0, "symbol": None, "name": None}))
    contract_rows = db.execute(
        select(GovernmentContract.awarding_agency, func.upper(GovernmentContract.symbol), func.max(GovernmentContract.recipient_name), func.sum(GovernmentContract.award_amount))
        .where(*contract_filters)
        .group_by(GovernmentContract.awarding_agency, func.upper(GovernmentContract.symbol))
    ).all()
    action_rows = db.execute(
        select(GovernmentContractAction.awarding_agency, func.upper(GovernmentContractAction.symbol), func.max(func.coalesce(GovernmentContractAction.company_name, GovernmentContractAction.recipient_name)), func.sum(GovernmentContractAction.obligated_amount))
        .where(*action_filters)
        .group_by(GovernmentContractAction.awarding_agency, func.upper(GovernmentContractAction.symbol))
    ).all()

    symbols: list[str | None] = []
    for agency, symbol, name, value in [*contract_rows, *action_rows]:
        department = (agency or "Unspecified Department").strip() or "Unspecified Department"
        if department not in name_set:
            continue
        key = symbol or name or "Unknown Vendor"
        buckets[department][key]["value"] += float(value or 0)
        buckets[department][key]["symbol"] = symbol
        buckets[department][key]["name"] = name
        symbols.append(symbol)

    company_names = _company_names(db, symbols)
    result: dict[str, str] = {}
    for department, vendors in buckets.items():
        top = max(vendors.values(), key=lambda item: item["value"], default=None)
        if top:
            symbol = normalize_symbol(top.get("symbol"))
            result[department] = company_names.get(symbol or "") or top.get("name") or symbol or "Unknown Vendor"
    return result


def _change_pct(value: Any, previous: Any) -> float | None:
    try:
        if previous in (None, 0):
            return None
        return ((float(value or 0) - float(previous)) / abs(float(previous))) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _government_contract_total(db: Session) -> float | None:
    contracts = db.execute(select(func.sum(GovernmentContract.award_amount))).scalar_one()
    actions = db.execute(select(func.sum(GovernmentContractAction.obligated_amount))).scalar_one()
    return _float_or_int(float(contracts or 0) + float(actions or 0))


def _department_count(db: Session) -> int:
    agencies = select(func.trim(GovernmentContract.awarding_agency).label("agency")).where(GovernmentContract.awarding_agency.is_not(None)).union(
        select(func.trim(GovernmentContractAction.awarding_agency).label("agency")).where(GovernmentContractAction.awarding_agency.is_not(None))
    ).subquery()
    return int(db.execute(select(func.count()).select_from(agencies).where(agencies.c.agency != "")).scalar_one() or 0)


def _active_vendor_count(db: Session) -> int:
    contract_symbols = db.execute(select(func.upper(GovernmentContract.symbol)).where(GovernmentContract.symbol.is_not(None)).group_by(func.upper(GovernmentContract.symbol))).all()
    action_symbols = db.execute(select(func.upper(GovernmentContractAction.symbol)).where(GovernmentContractAction.symbol.is_not(None)).group_by(func.upper(GovernmentContractAction.symbol))).all()
    return len({row[0] for row in [*contract_symbols, *action_symbols] if row[0]})


def _activity_payload(row: Event, company_names: dict[str, str]) -> dict[str, Any]:
    symbol = normalize_symbol(row.symbol)
    payload = _safe_json(row.payload_json)
    kind = _profile_kind(row.event_type)
    company_name = company_names.get(symbol or "") or _payload_first(payload, "company_name", "issuer_name", "recipient_name") or symbol
    profile_name = row.member_name or _payload_first(payload, "insider_name", "holder_name", "institution_name", "department", "agency")
    # Preserve an otherwise useful filing when the source has an issuer but no
    # reporting-person name. It is deliberately not linked as a person profile.
    if not profile_name and kind == "Insider" and company_name:
        profile_name = f"{company_name} insider filing"
        profile_href = None
    else:
        profile_name = profile_name or "Profile unavailable"
        profile_href = _profile_href(kind, profile_name, row.member_bioguide_id, payload)
    value = row.amount_max if row.amount_max is not None else row.amount_min
    return {
        "id": row.id,
        "time": _date_iso(row.ts),
        "type": kind,
        "profile": profile_name,
        "profile_href": profile_href,
        "symbol": symbol,
        "company": company_name,
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
    """Return the underlying action, not the broad source of the activity."""
    normalized_type = (event_type or "").strip().lower()
    normalized_trade = (trade_type or "").replace("_", " ").strip().lower()

    if normalized_type == "government_contract":
        return "Contract Award"
    if normalized_type == "new_institutional_position":
        return "New Position"
    if normalized_type in {
        "institutional_distribution",
        "major_holder_reduction",
        "major_holder_exit",
        "cluster_distribution",
    }:
        return "Decreased"
    if normalized_type in {
        "institutional_accumulation",
        "cluster_accumulation",
        "smart_money_confirmation",
    }:
        return "Increased"
    if normalized_type in {"congress_trade", "insider_trade"}:
        if any(token in normalized_trade for token in ("sale", "sell", "dispose", "disposition")):
            return "Sale"
        return "Purchase"
    return normalized_trade.title() if normalized_trade else normalized_type.replace("_", " ").title()


def _member_href(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = re.sub(r"\s+", " ", name.strip()).upper()
    slug = re.sub(r"[^A-Z0-9 ]", "", cleaned).replace(" ", "_")
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
    sectors: dict[str, str] = {symbol: "Technology" for symbol in normalized if symbol in TECH_PLATFORM_SECTOR_SYMBOLS}
    for symbol, sector in db.execute(select(func.upper(Security.symbol), Security.sector).where(Security.symbol.is_not(None), func.upper(Security.symbol).in_(normalized))).all():
        if symbol and sector and str(symbol) not in sectors:
            sectors[str(symbol)] = sector
    for symbol, sector in db.execute(select(func.upper(TickerMeta.symbol), TickerMeta.sector).where(func.upper(TickerMeta.symbol).in_(normalized))).all():
        if symbol and sector and str(symbol) not in sectors:
            sectors[str(symbol)] = sector
    return sectors


def _symbols_for_sector(db: Session, sector: str) -> set[str]:
    if not sector:
        return set()
    security_rows = db.execute(select(func.upper(Security.symbol)).where(func.lower(func.coalesce(Security.sector, "")) == sector.lower())).all()
    meta_rows = db.execute(select(func.upper(TickerMeta.symbol)).where(func.lower(func.coalesce(TickerMeta.sector, "")) == sector.lower())).all()
    symbols = {row[0] for row in [*security_rows, *meta_rows] if row[0]}
    if sector.lower() == "technology":
        symbols.update(TECH_PLATFORM_SECTOR_SYMBOLS)
    return symbols


def _allocation_payload(buckets: dict[str, dict[str, float]]) -> list[dict[str, Any]]:
    items = []
    for period in sorted(buckets.keys(), key=_period_sort_key)[-8:]:
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


def _period_sort_key(label: str) -> tuple[int, int, str]:
    parts = label.split()
    if len(parts) == 2 and parts[0].startswith("Q"):
        try:
            return int(parts[1]), int(parts[0][1:]), label
        except ValueError:
            pass
    return 0, 0, label


def _quarter_label(value: datetime | None) -> str:
    if not isinstance(value, datetime):
        return "Unknown"
    return f"Q{((value.month - 1) // 3) + 1} {value.year}"


def _quarter_label_for_date(value: date | None) -> str:
    if not isinstance(value, date):
        return "Unknown"
    return f"Q{((value.month - 1) // 3) + 1} {value.year}"


def _normalize_sector_label(value: str | None) -> str | None:
    label = (value or "").strip()
    if not label or label.lower() == "other":
        return None
    return "Technology" if label in {"Technology Services", "Electronic Technology"} else label


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
