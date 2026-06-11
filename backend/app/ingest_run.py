import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import func, select

from app.clients.fmp import FMPClientError
from app.compute_trade_outcomes import run_compute
from app.db import SessionLocal, engine, ensure_price_cache_volume_columns
from app.enrich_members import enrich_members
from app.ingest.government_contracts import DEFAULT_TARGET_SYMBOLS, run_government_contracts_ingest_job
from app.ingest_congress_recent import run_recent_congress_ingest
from app.ingest_house import ingest_house
from app.ingest_insider_trades import insider_ingest_run
from app.ingest_institutional_buys import institutional_ingest_run
from app.populate_fundamentals_cache import populate_fundamentals_cache
from app.ingest_senate import ingest_senate
from app.models import Event, TradeOutcome
from app.security.redaction import safe_config_for_log
from app.services.price_lookup import get_daily_close_series_with_fallback
from app.services.data_enrichment_queue import process_data_enrichment_jobs
from app.services.saved_screen_monitoring import refresh_due_saved_screen_monitoring
from app.services.confirmation_monitoring import refresh_all_monitored_watchlist_confirmation_monitoring

logger = logging.getLogger(__name__)

SAFE_OUTCOME_RETRY_STATUSES = "no_data,no_current_price,provider_429,no_entry_price,no_execution_price"


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
            "fundamentals-cache-daily",
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
        benchmark_symbol=os.getenv("INGEST_SIGNALS_BENCHMARK", "^GSPC"),
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
    benchmark_symbol = os.getenv("INGEST_SIGNALS_BENCHMARK", "^GSPC")
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
        top_missing_symbols = db.execute(
            select(func.coalesce(Event.symbol, "<unresolved>"), func.count(Event.id))
            .select_from(Event)
            .join(TradeOutcome, TradeOutcome.event_id == Event.id, isouter=True)
            .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
            .where(func.coalesce(Event.event_date, Event.ts) >= since)
            .where(TradeOutcome.id.is_(None))
            .group_by(func.coalesce(Event.symbol, "<unresolved>"))
            .order_by(func.count(Event.id).desc())
            .limit(10)
        ).all()

    failed_statuses = {str(status): int(count) for status, count in failed_rows if status}
    missing_prices_remaining = sum(
        count
        for status, count in failed_statuses.items()
        if status in {"no_data", "no_current_price", "no_entry_price", "no_execution_price", "provider_429"}
    )
    unresolved_symbols_remaining = sum(
        int(count)
        for symbol, count in top_missing_symbols
        if not symbol or str(symbol) == "<unresolved>"
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
    lookback_days = int(os.getenv("OUTCOME_REPAIR_LOOKBACK_DAYS", "1095"))
    retry_statuses = os.getenv("OUTCOME_REPAIR_RETRY_STATUSES", SAFE_OUTCOME_RETRY_STATUSES)
    limit = _optional_int_env("OUTCOME_REPAIR_LIMIT")
    benchmark = os.getenv("INGEST_SIGNALS_BENCHMARK", "^GSPC")

    logger.info(
        "Starting daily outcome repair lookback_days=%s limit=%s retry_statuses=%s",
        lookback_days,
        limit,
        retry_statuses,
    )
    congress = run_compute(
        replace=False,
        limit=limit,
        member_id=None,
        event_type="congress_trade",
        benchmark_symbol=benchmark,
        lookback_days=lookback_days,
        trade_date_after=None,
        only_missing=True,
        retry_failed_status=None,
        retry_failed_statuses=retry_statuses,
    )
    insider = run_compute(
        replace=False,
        limit=limit,
        member_id=None,
        event_type="insider_trade",
        benchmark_symbol=benchmark,
        lookback_days=lookback_days,
        trade_date_after=None,
        only_missing=True,
        retry_failed_status=None,
        retry_failed_statuses=retry_statuses,
    )
    coverage = _daily_outcome_coverage_report(lookback_days=lookback_days)
    report = {
        "job": "daily-repair",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "congress": congress,
        "insider": insider,
        "coverage": coverage,
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
    except FMPClientError as exc:
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
    limit = int(os.getenv("FMP_ENRICHMENT_WORKERS", "25") or 25)
    result = process_data_enrichment_jobs(limit=max(1, limit))
    logger.info("Data enrichment queue finished: %s", result)
    return {"job": "enrichment-queue", **result}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _build_parser().parse_args()
    _require_data_mount_writable()

    if args.job == "core":
        payload = _run_core_job()
    elif args.job == "recent-congress":
        payload = _run_recent_congress_job()
    elif args.job == "government-contracts-daily":
        payload = {
            "job": args.job,
            "government_contracts": _run_government_contracts_job(lookback_days=30),
        }
    elif args.job == "government-contracts-weekly":
        payload = {
            "job": args.job,
            "government_contracts": _run_government_contracts_job(lookback_days=365),
        }
    elif args.job == "daily-repair":
        payload = _run_daily_outcome_repair()
    elif args.job == "fundamentals-cache-daily":
        payload = {
            "job": args.job,
            "fundamentals_cache": _run_fundamentals_cache_refresh(),
        }
    elif args.job == "enrichment-queue":
        payload = _run_enrichment_queue_job()
    else:
        payload = {
            "job": "all",
            "core": _run_core_job(),
            "government_contracts_daily": _run_government_contracts_job(lookback_days=30),
            "daily_repair": _run_daily_outcome_repair(),
        }

    print(json.dumps(payload))
