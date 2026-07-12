import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import requests
from sqlalchemy import func, select

from app.clients.fmp import FMPClientError, FMPSubscriptionRestrictedError
from app.compute_trade_outcomes import run_compute
from app.db import SessionLocal, engine, ensure_price_cache_volume_columns, ensure_ticker_financials_cache_schema
from app.enrich_members import enrich_members
from app.ingest.government_contracts import DEFAULT_TARGET_SYMBOLS, run_government_contracts_ingest_job
from app.ingest_congress_recent import run_recent_congress_ingest
from app.ingest_house import ingest_house
from app.ingest_insider_trades import insider_ingest_run
from app.ingest_institutional_buys import institutional_ingest_run
from app.populate_fundamentals_cache import populate_fundamentals_cache
from app.ingest_senate import ingest_senate
from app.models import Event, PriceCache, SavedScreenSnapshot, Security, TradeOutcome, WatchlistItem
from app.security.redaction import safe_config_for_log
from app.services.price_lookup import (
    ensure_fresh_price_history,
    get_daily_close_series_with_fallback,
    get_expected_latest_market_date,
)
from app.services.provider_usage import log_provider_budget_summary
from app.services.data_enrichment_queue import enqueue_priority_ticker_prewarm_jobs, process_data_enrichment_jobs
from app.services.saved_screen_monitoring import refresh_due_saved_screen_monitoring
from app.services.confirmation_monitoring import refresh_all_monitored_watchlist_confirmation_monitoring
from app.utils.symbols import normalize_symbol
from app.background_job_guard import background_job_skip_payload, check_background_job_guard

logger = logging.getLogger(__name__)

SAFE_OUTCOME_RETRY_STATUSES = "no_data,no_current_price,provider_429,provider_budget_exceeded,retry_later,no_entry_price,no_execution_price"
UNRESOLVED_SYMBOL_LABEL = "<unresolved>"


def json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, set):
        return sorted(value)
    return str(value)


def _payload_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, default=json_default)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run scheduled ingest jobs.")
    parser.add_argument(
        "--job",
        type=str,
        default=os.getenv("INGEST_JOB", "core"),
        choices=[
            "core",
            "recent-congress",
            "government-contracts-daily",
            "government-contracts-weekly",
            "daily-repair",
            "market-data-refresh-daily",
            "fundamentals-cache-daily",
            "enrichment-queue",
            "priority-ticker-prewarm",
            "all",
        ],
        help="Which scheduled ingest job to run.",
    )
    return parser


def _check_insider_freshness() -> str | None:
    key = os.getenv("FMP_API_KEY")
    if not key:
        logger.warning("FMP_API_KEY not set; skipping insider freshness check")
        return None

    url = (
        "https://financialmodelingprep.com/stable/insider-trading/latest"
        f"?page=0&limit=5&apikey={key}"
    )

    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.warning("FMP insider latest returned %s", response.status_code)
            return None

        data = response.json()
        if isinstance(data, list) and data:
            dates = [item.get("filingDate") for item in data if item.get("filingDate")]
            return max(dates) if dates else None
    except Exception as exc:
        logger.warning("Insider freshness check failed: %s", exc)

    return None


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def _require_data_mount_writable(path: str = "/data") -> None:
    data_path = Path(path)
    if not data_path.exists() or not data_path.is_dir():
        raise RuntimeError(f"Required data mount is missing: {path}")

    probe = data_path / ".ingest_write_probe"
    try:
        probe.write_text("ok")
        probe.unlink(missing_ok=True)
    except Exception as exc:
        raise RuntimeError(f"Required data mount is not writable: {path}") from exc


def _log_startup_config(config: dict[str, object]) -> None:
    logger.info("ingest startup config: %s", json.dumps(safe_config_for_log(config), sort_keys=True))


def _inserted_count(result: dict[str, object]) -> int:
    inserted = result.get("inserted")
    return inserted if isinstance(inserted, int) else 0


def _run_member_enrichment() -> dict[str, object]:
    logger.info("Starting congress member metadata enrichment")
    result = enrich_members()
    logger.info("Finished congress member metadata enrichment: %s", result)
    return result


def _log_member_enrichment_mode(*, do_house: bool, do_senate: bool, do_member_enrich: bool) -> None:
    if not do_member_enrich:
        logger.info("Member enrichment disabled via INGEST_ENRICH_MEMBERS=0")
        return

    if not do_house and not do_senate:
        logger.info(
            "Running member enrichment in repair-only mode (INGEST_ENRICH_MEMBERS=1 with INGEST_DO_HOUSE=0 and INGEST_DO_SENATE=0)"
        )
        return

    logger.info("Running member enrichment as part of ingest run")


def _run_backfill() -> str:
    logger.info("Starting congress events backfill")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "app.backfill_events_from_trades",
            "--log-level",
            "INFO",
        ],
        check=True,
    )
    logger.info("Finished congress events backfill")
    return "run"


def _run_signals_recompute() -> dict[str, object]:
    lookback_days = int(os.getenv("INGEST_SIGNALS_LOOKBACK_DAYS", "30"))
    logger.info("Starting signals recompute lookback_days=%s", lookback_days)
    result = run_compute(
        replace=True,
        limit=None,
        member_id=None,
        event_type="all",
        benchmark_symbol=os.getenv("INGEST_SIGNALS_BENCHMARK", "SPY"),
        lookback_days=lookback_days,
        trade_date_after=None,
        only_missing=False,
        retry_failed_status=None,
        retry_failed_statuses=None,
    )
    logger.info("Finished signals recompute: %s", result)
    return result


def _warm_price_cache() -> dict[str, object]:
    ensure_price_cache_volume_columns(engine)
    lookback_days = int(os.getenv("INGEST_PRICE_CACHE_LOOKBACK_DAYS", "30"))
    symbol_limit = int(os.getenv("INGEST_PRICE_CACHE_SYMBOL_LIMIT", "75"))
    benchmark_symbol = os.getenv("INGEST_SIGNALS_BENCHMARK", "SPY")
    since = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    start_key = since.date().isoformat()
    end_key = datetime.now(timezone.utc).date().isoformat()

    with SessionLocal() as db:
        sort_ts = func.coalesce(Event.event_date, Event.ts)
        symbols = [
            symbol
            for symbol in db.execute(
                select(func.upper(Event.symbol))
                .where(Event.symbol.is_not(None))
                .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
                .where(sort_ts >= since)
                .group_by(func.upper(Event.symbol))
                .order_by(func.max(sort_ts).desc())
                .limit(max(1, symbol_limit))
            ).scalars().all()
            if symbol
        ]

        warmed_symbols = 0
        warmed_points = 0
        for symbol in [*symbols, benchmark_symbol]:
            series = get_daily_close_series_with_fallback(db, symbol, start_key, end_key)
            if series:
                warmed_symbols += 1
                warmed_points += len(series)
        db.commit()

    result = {
        "lookback_days": lookback_days,
        "symbol_candidates": len(symbols),
        "warmed_symbols": warmed_symbols,
        "warmed_points": warmed_points,
    }
    logger.info("Finished price cache warm: %s", result)
    return result


def _add_unique_symbol(symbols: list[str], seen: set[str], raw_symbol: object, *, limit: int) -> None:
    if len(symbols) >= limit:
        return
    normalized = normalize_symbol(str(raw_symbol)) if raw_symbol is not None else None
    if not normalized or normalized in seen:
        return
    symbols.append(normalized)
    seen.add(normalized)


def _market_data_refresh_symbols(db, *, expected_date: date, limit: int) -> list[str]:
    expected_key = expected_date.isoformat()
    symbols: list[str] = []
    seen: set[str] = set()
    benchmark_symbol = os.getenv("INGEST_SIGNALS_BENCHMARK", "SPY")
    priority_symbols = [
        symbol.strip()
        for symbol in os.getenv("MARKET_DATA_REFRESH_PRIORITY_SYMBOLS", "").split(",")
        if symbol.strip()
    ]
    for symbol in [*priority_symbols, benchmark_symbol]:
        _add_unique_symbol(symbols, seen, symbol, limit=limit)

    stale_cache_rows = db.execute(
        select(PriceCache.symbol, func.max(PriceCache.date).label("latest_date"))
        .group_by(PriceCache.symbol)
        .having(func.max(PriceCache.date) < expected_key)
        .order_by(func.max(PriceCache.date).asc(), PriceCache.symbol.asc())
        .limit(max(1, limit))
    ).all()
    for symbol, _latest_date in stale_cache_rows:
        _add_unique_symbol(symbols, seen, symbol, limit=limit)

    since = datetime.now(timezone.utc) - timedelta(
        days=int(os.getenv("MARKET_DATA_REFRESH_EVENT_LOOKBACK_DAYS", "365") or 365)
    )
    event_rows = db.execute(
        select(func.upper(Event.symbol), func.max(func.coalesce(Event.event_date, Event.ts)).label("latest_ts"))
        .where(Event.symbol.is_not(None))
        .where(func.coalesce(Event.event_date, Event.ts) >= since)
        .group_by(func.upper(Event.symbol))
        .order_by(func.max(func.coalesce(Event.event_date, Event.ts)).desc())
        .limit(max(1, limit))
    ).all()
    for symbol, _latest_ts in event_rows:
        _add_unique_symbol(symbols, seen, symbol, limit=limit)

    watchlist_rows = db.execute(
        select(func.upper(Security.symbol))
        .select_from(WatchlistItem)
        .join(Security, Security.id == WatchlistItem.security_id)
        .where(Security.symbol.is_not(None))
        .group_by(func.upper(Security.symbol))
        .limit(max(1, limit))
    ).scalars().all()
    for symbol in watchlist_rows:
        _add_unique_symbol(symbols, seen, symbol, limit=limit)

    saved_screen_rows = db.execute(
        select(func.upper(SavedScreenSnapshot.ticker))
        .where(SavedScreenSnapshot.ticker.is_not(None))
        .group_by(func.upper(SavedScreenSnapshot.ticker))
        .limit(max(1, limit))
    ).scalars().all()
    for symbol in saved_screen_rows:
        _add_unique_symbol(symbols, seen, symbol, limit=limit)

    return symbols


def _run_market_data_refresh_job() -> dict[str, object]:
    ensure_price_cache_volume_columns(engine)
    expected_date = get_expected_latest_market_date()
    lookback_days = int(os.getenv("MARKET_DATA_REFRESH_LOOKBACK_TRADING_DAYS", "15") or 15)
    symbol_limit = int(os.getenv("MARKET_DATA_REFRESH_SYMBOL_LIMIT", "500") or 500)
    failures: list[dict[str, object]] = []
    refreshed = 0
    stale_after = 0
    symbols: list[str] = []

    logger.info(
        "market_data_refresh_start expected_latest_date=%s lookback_days=%s symbol_limit=%s",
        expected_date.isoformat(),
        lookback_days,
        symbol_limit,
    )
    with SessionLocal() as db:
        symbols = _market_data_refresh_symbols(db, expected_date=expected_date, limit=max(1, symbol_limit))
        for symbol in symbols:
            freshness = ensure_fresh_price_history(
                db,
                symbol,
                expected_date=expected_date,
                lookback_days=lookback_days,
            )
            if freshness.get("refresh_attempted"):
                refreshed += 1
            if freshness.get("is_stale"):
                stale_after += 1
                failure = {
                    "symbol": symbol,
                    "latest_date": freshness.get("latest_date"),
                    "expected_latest_date": freshness.get("expected_latest_date"),
                    "status": freshness.get("status"),
                }
                failures.append(failure)
                logger.warning(
                    "market_data_refresh_symbol_stale symbol=%s latest=%s expected=%s status=%s",
                    symbol,
                    failure["latest_date"],
                    failure["expected_latest_date"],
                    failure["status"],
                )

    result = {
        "job": "market-data-refresh-daily",
        "status": "partial" if failures else "ok",
        "expected_latest_date": expected_date.isoformat(),
        "symbol_count": len(symbols),
        "refresh_attempted": refreshed,
        "stale_after_refresh": stale_after,
        "failures": failures[:25],
    }
    logger.info("market_data_refresh_finished result=%s", result)
    return result


def _run_fundamentals_cache_refresh() -> dict[str, object]:
    stale_days = int(os.getenv("INGEST_FUNDAMENTALS_STALE_DAYS", "7"))
    limit = int(os.getenv("INGEST_FUNDAMENTALS_LIMIT", "500"))
    sleep_s = float(os.getenv("INGEST_FUNDAMENTALS_SLEEP_S", "0"))
    logger.info(
        "Starting fundamentals cache refresh stale_days=%s limit=%s sleep_s=%s",
        stale_days,
        limit,
        sleep_s,
    )
    result = populate_fundamentals_cache(
        screener_universe=True,
        stale_days=stale_days,
        limit=limit,
        dry_run=False,
        sleep_s=sleep_s,
    )
    logger.info("Finished fundamentals cache refresh: %s", result)
    return result


def _optional_int_env(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Ignoring invalid integer env %s=%r", name, raw)
        return None
    return value if value > 0 else None


def _positive_int_env(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return max(1, int(default))
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Ignoring invalid integer env %s=%r", name, raw)
        return max(1, int(default))
    return max(1, value)


def _top_missing_symbols_statement(since: datetime):
    return (
        select(Event.symbol, func.count(Event.id))
        .select_from(Event)
        .join(TradeOutcome, TradeOutcome.event_id == Event.id, isouter=True)
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
        .where(func.coalesce(Event.event_date, Event.ts) >= since)
        .where(TradeOutcome.id.is_(None))
        .group_by(Event.symbol)
        .order_by(func.count(Event.id).desc())
    )


def _normalize_symbol_bucket(symbol: object) -> str:
    if symbol is None:
        return UNRESOLVED_SYMBOL_LABEL
    normalized = str(symbol).strip()
    return normalized or UNRESOLVED_SYMBOL_LABEL


def _normalize_top_missing_symbol_rows(rows: list[tuple[object, int]]) -> list[tuple[str, int]]:
    totals: dict[str, int] = {}
    for symbol, count in rows:
        bucket = _normalize_symbol_bucket(symbol)
        totals[bucket] = totals.get(bucket, 0) + int(count or 0)
    return sorted(totals.items(), key=lambda item: item[1], reverse=True)[:10]


def _daily_outcome_coverage_report(*, lookback_days: int) -> dict[str, object]:
    since = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    with SessionLocal() as db:
        congress_missing = db.execute(
            select(func.count(Event.id))
            .select_from(Event)
            .join(TradeOutcome, TradeOutcome.event_id == Event.id, isouter=True)
            .where(Event.event_type == "congress_trade")
            .where(func.coalesce(Event.event_date, Event.ts) >= since)
            .where(TradeOutcome.id.is_(None))
        ).scalar()
        insider_missing = db.execute(
            select(func.count(Event.id))
            .select_from(Event)
            .join(TradeOutcome, TradeOutcome.event_id == Event.id, isouter=True)
            .where(Event.event_type == "insider_trade")
            .where(func.coalesce(Event.event_date, Event.ts) >= since)
            .where(TradeOutcome.id.is_(None))
        ).scalar()
        failed_rows = db.execute(
            select(TradeOutcome.scoring_status, func.count())
            .where(TradeOutcome.trade_date.is_not(None))
            .where(TradeOutcome.trade_date >= since.date())
            .where(TradeOutcome.scoring_status != "ok")
            .group_by(TradeOutcome.scoring_status)
        ).all()
        top_missing_symbol_rows = db.execute(_top_missing_symbols_statement(since)).all()

    failed_statuses = {str(status): int(count) for status, count in failed_rows if status}
    top_missing_symbols = _normalize_top_missing_symbol_rows(top_missing_symbol_rows)
    missing_prices_remaining = sum(
        count
        for status, count in failed_statuses.items()
        if status
        in {
            "no_data",
            "no_current_price",
            "no_entry_price",
            "no_execution_price",
            "provider_429",
            "provider_budget_exceeded",
            "retry_later",
        }
    )
    unresolved_symbols_remaining = sum(
        int(count)
        for symbol, count in top_missing_symbols
        if not symbol or str(symbol) == UNRESOLVED_SYMBOL_LABEL
    )
    provider_errors = {
        status: count
        for status, count in failed_statuses.items()
        if status.startswith("provider_")
    }
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lookback_days": lookback_days,
        "missing_outcomes": {
            "congress_trade": int(congress_missing or 0),
            "insider_trade": int(insider_missing or 0),
        },
        "unresolved_symbols_remaining": unresolved_symbols_remaining,
        "missing_prices_remaining": missing_prices_remaining,
        "failed_statuses": failed_statuses,
        "provider_errors": provider_errors,
        "top_symbols_with_missing_outcomes": [
            {"symbol": str(symbol), "missing": int(count)}
            for symbol, count in top_missing_symbols
        ],
    }


def _run_daily_outcome_repair() -> dict[str, object]:
    started_monotonic = time.monotonic()
    started_at = datetime.now(timezone.utc)
    lookback_days = int(os.getenv("OUTCOME_REPAIR_LOOKBACK_DAYS", "1095"))
    retry_statuses = os.getenv("OUTCOME_REPAIR_RETRY_STATUSES", SAFE_OUTCOME_RETRY_STATUSES)
    max_events = _positive_int_env("DAILY_REPAIR_MAX_EVENTS", default=500)
    limit = _optional_int_env("OUTCOME_REPAIR_LIMIT") or max_events
    max_seconds = _positive_int_env("DAILY_REPAIR_MAX_SECONDS", default=240)
    price_lookup_budget = _positive_int_env("DAILY_REPAIR_PRICE_LOOKUP_BUDGET", default=200)
    benchmark = os.getenv("INGEST_SIGNALS_BENCHMARK", "SPY")
    stages_run: list[str] = []
    price_lookup_attempts_used = 0

    logger.info(
        "Starting daily outcome repair lookback_days=%s limit=%s retry_statuses=%s max_seconds=%s price_lookup_budget=%s",
        lookback_days,
        limit,
        retry_statuses,
        max_seconds,
        price_lookup_budget,
    )

    def remaining_seconds() -> float:
        return max(0.0, float(max_seconds) - (time.monotonic() - started_monotonic))

    def remaining_price_lookup_budget() -> int:
        return max(0, price_lookup_budget - price_lookup_attempts_used)

    def partial_stage(event_type: str, reason: str) -> dict[str, object]:
        return {
            "event_type": event_type,
            "status": "partial",
            "partial_reason": reason,
            "scanned": 0,
            "eligible": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "status_counts": {},
            "skipped_budget": 0,
            "retry_later": 0,
            "price_lookup_attempts": 0,
        }

    def run_stage(event_type: str) -> dict[str, object]:
        nonlocal price_lookup_attempts_used
        if remaining_seconds() <= 0:
            return partial_stage(event_type, "max_seconds_exceeded")
        remaining_budget = remaining_price_lookup_budget()
        if remaining_budget <= 0:
            return partial_stage(event_type, "price_lookup_budget_exceeded")

        stages_run.append(event_type)
        stage = run_compute(
            replace=False,
            limit=limit,
            member_id=None,
            event_type=event_type,
            benchmark_symbol=benchmark,
            lookback_days=lookback_days,
            trade_date_after=None,
            only_missing=True,
            retry_failed_status=None,
            retry_failed_statuses=retry_statuses,
            max_seconds=remaining_seconds(),
            max_price_lookups=remaining_budget,
        )
        attempts = stage.get("price_lookup_attempts")
        if isinstance(attempts, int):
            price_lookup_attempts_used += attempts
        return stage

    congress = run_stage("congress_trade")
    insider = run_stage("insider_trade")
    coverage = _daily_outcome_coverage_report(lookback_days=lookback_days)
    provider_budget_summary = log_provider_budget_summary(reset=True)
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    stage_reports = [congress, insider]
    skipped_budget = sum(int(stage.get("skipped_budget") or 0) for stage in stage_reports)
    retry_later = sum(int(stage.get("retry_later") or 0) for stage in stage_reports)
    status = "partial" if any(stage.get("status") == "partial" for stage in stage_reports) else "ok"
    report = {
        "job": "daily-repair",
        "timestamp": started_at.isoformat(),
        "status": status,
        "stages_run": stages_run,
        "scanned": sum(int(stage.get("scanned") or 0) for stage in stage_reports),
        "updated": sum(int(stage.get("updated") or 0) for stage in stage_reports),
        "skipped_budget": skipped_budget,
        "retry_later": retry_later,
        "duration_seconds": duration_seconds,
        "max_events": limit,
        "max_seconds": max_seconds,
        "price_lookup_budget": price_lookup_budget,
        "price_lookup_attempts": price_lookup_attempts_used,
        "congress": congress,
        "insider": insider,
        "coverage": coverage,
        "provider_budget_summary": provider_budget_summary,
    }
    logger.info("Daily outcome repair coverage report: %s", report)
    return report


def _run_watchlist_confirmation_monitoring_refresh() -> dict[str, object]:
    lookback_days = int(os.getenv("WATCHLIST_CONFIRMATION_MONITORING_LOOKBACK_DAYS", "30"))
    logger.info("Starting scheduled watchlist confirmation monitoring refresh")
    result = refresh_all_monitored_watchlist_confirmation_monitoring(
        SessionLocal,
        lookback_days=lookback_days,
    )
    logger.info("Finished scheduled watchlist confirmation monitoring refresh: %s", result)
    return result


def _run_institutional_ingest(*, pages: int, limit: int, days: int) -> dict[str, object]:
    try:
        return institutional_ingest_run(
            pages=pages,
            limit=limit,
            days=days,
        )
    except FMPSubscriptionRestrictedError as exc:
        logger.warning("institutional_ingest_skipped reason=subscription_restricted error=%s", exc)
        return {"status": "skipped", "reason": "subscription_restricted", "error": str(exc)}
    except FMPClientError as exc:
        if "402" in str(exc):
            logger.warning("institutional_ingest_skipped reason=subscription_restricted error=%s", exc)
            return {"status": "skipped", "reason": "subscription_restricted", "error": str(exc)}
        logger.warning("Institutional ingest skipped after FMP client error: %s", exc)
        return {"status": "skipped_provider_error", "error": str(exc)}


def _run_core_job() -> dict[str, object]:
    do_house = _is_truthy(os.getenv("INGEST_DO_HOUSE", "1"))
    do_senate = _is_truthy(os.getenv("INGEST_DO_SENATE", "1"))
    do_backfill = _is_truthy(os.getenv("INGEST_BACKFILL", "0"))
    do_insider = _is_truthy(os.getenv("INGEST_DO_INSIDER", "1"))
    do_member_enrich = _is_truthy(os.getenv("INGEST_ENRICH_MEMBERS", "1"))
    do_institutional = _is_truthy(os.getenv("INGEST_DO_INSTITUTIONAL", "1"))
    do_signals_recompute = _is_truthy(os.getenv("INGEST_DO_SIGNALS_RECOMPUTE", "1"))
    do_price_cache_warm = _is_truthy(os.getenv("INGEST_DO_PRICE_CACHE_WARM", "1"))
    do_fundamentals_cache_refresh = _is_truthy(os.getenv("INGEST_DO_FUNDAMENTALS_CACHE", "1"))
    do_watchlist_confirmation_monitoring = _is_truthy(os.getenv("INGEST_DO_WATCHLIST_CONFIRMATION_MONITORING", "1"))

    pages = int(os.getenv("INGEST_PAGES", "3"))
    limit = int(os.getenv("INGEST_LIMIT", "200"))
    sleep_s = float(os.getenv("INGEST_SLEEP_S", "0.25"))
    insider_days = int(os.getenv("INGEST_INSIDER_DAYS", "30"))
    institutional_days = int(os.getenv("INGEST_INSTITUTIONAL_DAYS", "30"))

    config = {
        "INGEST_DO_HOUSE": do_house,
        "INGEST_DO_SENATE": do_senate,
        "INGEST_BACKFILL": do_backfill,
        "INGEST_DO_INSIDER": do_insider,
        "INGEST_ENRICH_MEMBERS": do_member_enrich,
        "INGEST_DO_INSTITUTIONAL": do_institutional,
        "INGEST_DO_SIGNALS_RECOMPUTE": do_signals_recompute,
        "INGEST_DO_PRICE_CACHE_WARM": do_price_cache_warm,
        "INGEST_DO_FUNDAMENTALS_CACHE": do_fundamentals_cache_refresh,
        "INGEST_DO_WATCHLIST_CONFIRMATION_MONITORING": do_watchlist_confirmation_monitoring,
        "INGEST_PAGES": pages,
        "INGEST_LIMIT": limit,
        "INGEST_SLEEP_S": sleep_s,
        "INGEST_INSIDER_DAYS": insider_days,
        "INGEST_INSTITUTIONAL_DAYS": institutional_days,
    }
    _log_startup_config(config)

    house_result = {"status": "skipped"}
    senate_result = {"status": "skipped"}
    insider_result = {"status": "skipped"}
    member_enrich_result: dict[str, object] = {"status": "skipped"}
    institutional_result: dict[str, object] = {"status": "skipped"}
    signals_recompute_result: dict[str, object] = {"status": "skipped"}
    price_cache_result: dict[str, object] = {"status": "skipped"}
    fundamentals_cache_result: dict[str, object] = {"status": "skipped"}
    watchlist_confirmation_monitoring_result: dict[str, object] = {"status": "skipped"}

    if do_house:
        house_result = ingest_house(pages=pages, limit=limit, sleep_s=sleep_s)

    if do_senate:
        senate_result = ingest_senate(pages=pages, limit=limit, sleep_s=sleep_s)

    if do_insider:
        insider_result = insider_ingest_run(pages=pages, limit=limit, days=insider_days)
        latest_fmp_date = _check_insider_freshness()

        latest_db_date = None
        try:
            with SessionLocal() as db:
                latest_db_date = db.execute(
                    select(func.max(Event.event_date)).where(Event.event_type == "insider_trade")
                ).scalar()
        except Exception as exc:
            logger.warning("Failed to check DB insider freshness: %s", exc)

        logger.info("FMP latest insider filingDate: %s", latest_fmp_date)
        logger.info("DB latest insider event_date: %s", latest_db_date)

    if do_institutional:
        institutional_result = _run_institutional_ingest(
            pages=pages,
            limit=limit,
            days=institutional_days,
        )

    congress_inserted = _inserted_count(house_result) + _inserted_count(senate_result)
    _log_member_enrichment_mode(
        do_house=do_house,
        do_senate=do_senate,
        do_member_enrich=do_member_enrich,
    )
    if do_member_enrich:
        member_enrich_result = _run_member_enrichment()
    should_run_backfill = do_backfill or congress_inserted > 0
    logger.info(
        "Backfill decision: INGEST_BACKFILL=%s congress_inserted=%s => run=%s",
        do_backfill,
        congress_inserted,
        should_run_backfill,
    )

    backfill_mode = "none"
    if should_run_backfill:
        backfill_mode = _run_backfill()

    if do_signals_recompute:
        signals_recompute_result = _run_signals_recompute()

    if do_price_cache_warm:
        price_cache_result = _warm_price_cache()

    if do_fundamentals_cache_refresh:
        try:
            fundamentals_cache_result = _run_fundamentals_cache_refresh()
        except Exception as exc:
            logger.warning("Fundamentals cache refresh failed: %s", exc)
            fundamentals_cache_result = {"status": "failed", "error": str(exc)}

    max_congress_ts = None
    max_insider_ts = None
    max_institutional_ts = None
    try:
        with SessionLocal() as db:
            max_congress_ts = db.execute(
                select(func.max(Event.ts)).where(Event.event_type == "congress_trade")
            ).scalar()
            max_insider_ts = db.execute(
                select(func.max(Event.ts)).where(Event.event_type == "insider_trade")
            ).scalar()
            max_institutional_ts = db.execute(
                select(func.max(Event.ts)).where(Event.event_type == "institutional_buy")
            ).scalar()
            screen_monitoring_result = refresh_due_saved_screen_monitoring(
                db,
                limit=int(os.getenv("SCREEN_MONITORING_LIMIT", "25")),
            )
            db.commit()
    except Exception as exc:
        logger.warning("Failed to check DB max event ts values: %s", exc)
        screen_monitoring_result = {"refreshed": 0, "generated": 0}

    logger.info("DB max congress_trade ts: %s", max_congress_ts)
    logger.info("DB max insider_trade ts: %s", max_insider_ts)
    logger.info("DB max institutional_buy ts: %s", max_institutional_ts)
    logger.info("Saved screen monitoring: %s", screen_monitoring_result)

    if do_watchlist_confirmation_monitoring:
        try:
            watchlist_confirmation_monitoring_result = _run_watchlist_confirmation_monitoring_refresh()
        except Exception as exc:
            logger.warning("Scheduled watchlist confirmation monitoring refresh failed: %s", exc)
            watchlist_confirmation_monitoring_result = {"status": "failed", "error": str(exc)}

    return {
        "job": "core",
        "house": house_result,
        "senate": senate_result,
        "insider": insider_result,
        "institutional": institutional_result,
        "member_enrich": member_enrich_result,
        "backfill": backfill_mode,
        "signals_recompute": signals_recompute_result,
        "price_cache": price_cache_result,
        "fundamentals_cache": fundamentals_cache_result,
        "screen_monitoring": screen_monitoring_result,
        "watchlist_confirmation_monitoring": watchlist_confirmation_monitoring_result,
    }


def _run_recent_congress_job() -> dict[str, object]:
    days = int(os.getenv("CONGRESS_RECENT_DAYS", "7"))
    pages = int(os.getenv("CONGRESS_RECENT_PAGES", "25"))
    limit = int(os.getenv("CONGRESS_RECENT_LIMIT", "100"))
    sleep_s = float(os.getenv("CONGRESS_RECENT_SLEEP_S", "0.1"))
    return {
        "job": "recent-congress",
        "congress_recent": run_recent_congress_ingest(
            days=days,
            pages=pages,
            limit=limit,
            sleep_s=sleep_s,
        ),
    }


def _run_government_contracts_job(*, lookback_days: int) -> dict[str, object]:
    symbols = [
        symbol.strip()
        for symbol in os.getenv("GOVERNMENT_CONTRACT_SYMBOLS", ",".join(DEFAULT_TARGET_SYMBOLS)).split(",")
        if symbol.strip()
    ]
    result = run_government_contracts_ingest_job(
        lookback_days=lookback_days,
        min_award_amount=float(os.getenv("GOVERNMENT_CONTRACT_MIN_AWARD_AMOUNT", "1000000")),
        max_pages=int(os.getenv("GOVERNMENT_CONTRACT_MAX_PAGES", "10")),
        limit=int(os.getenv("GOVERNMENT_CONTRACT_LIMIT", "100")),
        symbols=symbols,
        recipient=os.getenv("GOVERNMENT_CONTRACT_RECIPIENT") or None,
        batch_size=int(os.getenv("GOVERNMENT_CONTRACT_BATCH_SIZE", "100")),
        sleep_ms=int(os.getenv("GOVERNMENT_CONTRACT_SLEEP_MS", "100")),
    )
    logger.info("Government contracts ingest finished: %s", result)
    return result


def _run_enrichment_queue_job() -> dict[str, object]:
    if os.getenv("ENRICHMENT_QUEUE_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        logger.info("data_enrichment_queue_skipped reason=enrichment_queue_disabled")
        return {
            "job": "enrichment-queue",
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 1,
            "reason": "enrichment_queue_disabled",
        }
    guard = check_background_job_guard("enrichment-queue")
    if not guard.proceed:
        logger.info("data_enrichment_queue_skipped reason=%s guard=%s", guard.reason, guard.to_dict())
        return {
            **background_job_skip_payload("enrichment-queue", guard),
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
        }
    ensure_ticker_financials_cache_schema(engine)
    limit = int(
        os.getenv("DATA_ENRICHMENT_QUEUE_BATCH_SIZE")
        or os.getenv("FMP_ENRICHMENT_WORKERS", "10")
        or 10
    )
    max_seconds = int(os.getenv("DATA_ENRICHMENT_QUEUE_MAX_SECONDS", "20") or 20)
    result = process_data_enrichment_jobs(limit=max(1, limit), max_seconds=max(1, max_seconds))
    logger.info("Data enrichment queue finished: %s", result)
    return {"job": "enrichment-queue", **result}


def _run_priority_ticker_prewarm_job() -> dict[str, object]:
    if os.getenv("PRIORITY_TICKER_PREWARM_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        logger.info("prewarm_ticker_cache_skipped reason=priority_ticker_prewarm_disabled")
        return {
            "job": "priority-ticker-prewarm",
            "status": "skipped",
            "reason": "priority_ticker_prewarm_disabled",
            "symbol_count": 0,
            "attempted": 0,
            "enqueued": 0,
        }
    guard = check_background_job_guard("priority-ticker-prewarm")
    if not guard.proceed:
        logger.info("prewarm_ticker_cache_skipped reason=%s guard=%s", guard.reason, guard.to_dict())
        return {
            **background_job_skip_payload("priority-ticker-prewarm", guard),
            "symbol_count": 0,
            "attempted": 0,
            "enqueued": 0,
        }
    symbol_limit = int(os.getenv("PRIORITY_TICKER_PREWARM_SYMBOL_LIMIT", "25") or 25)
    popular_limit = int(os.getenv("PRIORITY_TICKER_PREWARM_POPULAR_LIMIT", "0") or 0)
    per_user_limit = int(os.getenv("PRIORITY_TICKER_PREWARM_PER_USER_LIMIT", "5") or 5)
    logger.info(
        "prewarm_ticker_cache_start symbol_limit=%s popular_limit=%s per_user_limit=%s",
        symbol_limit,
        popular_limit,
        per_user_limit,
    )
    with SessionLocal() as db:
        result = enqueue_priority_ticker_prewarm_jobs(
            db,
            symbol_limit=symbol_limit,
            popular_limit=popular_limit,
            per_user_limit=per_user_limit,
            source="priority_ticker_prewarm",
        )
    logger.info(
        "prewarm_ticker_cache_selected selected_tickers_count=%s watchlist_tickers=%s recently_viewed_tickers=%s popular_tickers=%s landing_tickers=%s",
        result.get("symbol_count", 0),
        result.get("watchlist_symbol_count", 0),
        result.get("recently_viewed_symbol_count", 0),
        result.get("popular_symbol_count", 0),
        result.get("landing_symbol_count", 0),
    )
    logger.info(
        "prewarm_ticker_cache_jobs jobs_enqueued_by_type=%s attempted_by_type=%s skip_reasons=%s skipped_budget=%s attempted=%s enqueued=%s",
        result.get("enqueued_by_type", {}),
        result.get("attempted_by_type", {}),
        result.get("skip_reasons", {}),
        result.get("skipped_budget", 0),
        result.get("attempted", 0),
        result.get("enqueued", 0),
    )
    logger.info("prewarm_ticker_cache_finished result=%s", result)
    return {"job": "priority-ticker-prewarm", **result}


def _run_job_payload(job: str) -> dict[str, object]:
    if job == "core":
        return _run_core_job()
    if job == "recent-congress":
        return _run_recent_congress_job()
    if job == "government-contracts-daily":
        return {
            "job": job,
            "government_contracts": _run_government_contracts_job(lookback_days=30),
        }
    if job == "government-contracts-weekly":
        return {
            "job": job,
            "government_contracts": _run_government_contracts_job(lookback_days=365),
        }
    if job == "daily-repair":
        return _run_daily_outcome_repair()
    if job == "market-data-refresh-daily":
        return _run_market_data_refresh_job()
    if job == "fundamentals-cache-daily":
        return {
            "job": job,
            "fundamentals_cache": _run_fundamentals_cache_refresh(),
        }
    if job == "enrichment-queue":
        return _run_enrichment_queue_job()
    if job == "priority-ticker-prewarm":
        return _run_priority_ticker_prewarm_job()
    return {
        "job": "all",
        "core": _run_core_job(),
        "government_contracts_daily": _run_government_contracts_job(lookback_days=30),
        "daily_repair": _run_daily_outcome_repair(),
        "market_data_refresh_daily": _run_market_data_refresh_job(),
    }


def _payload_exit_code(payload: dict[str, object]) -> int:
    return 1 if payload.get("status") == "failed" else 0


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _build_parser().parse_args()
    started = time.monotonic()
    try:
        _require_data_mount_writable()
        payload = _run_job_payload(args.job)
    except Exception as exc:
        logger.exception("ingest job failed job=%s", args.job)
        payload = {
            "job": args.job,
            "status": "failed",
            "error": str(exc),
            "duration_seconds": round(time.monotonic() - started, 3),
        }
        print(_payload_json(payload))
        sys.exit(1)

    print(_payload_json(payload))
    sys.exit(_payload_exit_code(payload))


if __name__ == "__main__":
    main()
