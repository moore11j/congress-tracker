from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import func, inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import (
    CongressDisclosureFiling,
    DataEnrichmentJob,
    Event,
    GovernmentContract,
    InstitutionalFiling,
    PriceCache,
    ProviderSetting,
    ProviderUsageEvent,
    SecForm4Filing,
)
from app.services.provider_registry import FMP_STABLE_BASE_URL, provider_domain_catalog

Status = str

FRESHNESS_WINDOW_SECONDS = 60 * 60 * 6
SECRET_NAMES_BY_PROVIDER: dict[str, list[str]] = {
    "congress_sources": ["FMP_API_KEY"],
    "insider_trades": ["FMP_API_KEY"],
    "market_data": ["FMP_API_KEY"],
    "institutional_13f": ["FMP_API_KEY"],
    "options_flow": ["MASSIVE_API_KEY"],
    "email": ["POSTMARK_SERVER_TOKEN"],
}
SECRET_VALUE_KEYS = {"apikey", "api_key", "token", "access_token", "secret", "key", "authorization"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        candidate = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return candidate.isoformat()
    return str(value)


def _safe_scalar(db: Session, statement, default: Any = None) -> Any:
    try:
        value = db.execute(statement).scalar_one_or_none()
        return default if value is None else value
    except SQLAlchemyError:
        return default


def _safe_rows(db: Session, statement) -> list[Any]:
    try:
        return list(db.execute(statement).scalars().all())
    except SQLAlchemyError:
        return []


def _safe_count(db: Session, model, *filters) -> int | None:
    try:
        statement = select(func.count()).select_from(model)
        for clause in filters:
            statement = statement.where(clause)
        return int(db.execute(statement).scalar_one() or 0)
    except SQLAlchemyError:
        return None


def _p95(values: list[float]) -> int | None:
    clean = sorted(value for value in values if value is not None and value >= 0)
    if not clean:
        return None
    index = max(0, math.ceil(len(clean) * 0.95) - 1)
    return int(round(clean[index]))


def _secret_status(secret_names: list[str]) -> str:
    if not secret_names:
        return "unknown"
    return "configured" if all(os.getenv(name, "").strip() for name in secret_names) else "missing"


def _safe_endpoint_url(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return value.split("?", 1)[0]
    query = [
        (key, item_value)
        for key, item_value in parse_qsl(parts.query, keep_blank_values=True)
        if key.strip().lower() not in SECRET_VALUE_KEYS
    ]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def _status_from_latest(row: ProviderUsageEvent | None) -> Status:
    if row is None:
        return "unknown"
    if row.success:
        return "healthy"
    if row.throttled or str(row.status_code or "").startswith("4"):
        return "degraded"
    return "down"


def _worst_status(statuses: list[Status]) -> Status:
    order = {"down": 4, "degraded": 3, "unknown": 2, "healthy": 1}
    known = [status for status in statuses if status in order]
    if not known:
        return "unknown"
    return max(known, key=lambda item: order[item])


def _latest_provider_event(db: Session, provider: str, *, category_prefixes: tuple[str, ...] = ()) -> ProviderUsageEvent | None:
    try:
        statement = select(ProviderUsageEvent).where(ProviderUsageEvent.provider == provider)
        if category_prefixes:
            statement = statement.where(
                ProviderUsageEvent.category.in_(category_prefixes)
                if len(category_prefixes) == 1
                else ProviderUsageEvent.category.in_(category_prefixes)
            )
        return db.execute(statement.order_by(ProviderUsageEvent.created_at.desc(), ProviderUsageEvent.id.desc()).limit(1)).scalar_one_or_none()
    except SQLAlchemyError:
        return None


def _provider_rows(db: Session, provider: str, *, since: datetime) -> list[ProviderUsageEvent]:
    return _safe_rows(
        db,
        select(ProviderUsageEvent)
        .where(ProviderUsageEvent.provider == provider)
        .where(ProviderUsageEvent.created_at >= since)
        .order_by(ProviderUsageEvent.created_at.desc())
        .limit(500),
    )


def _latest_success_at(rows: list[ProviderUsageEvent]) -> str | None:
    successes = [row.created_at for row in rows if row.success and row.created_at]
    return _iso(max(successes)) if successes else None


def _latest_error(rows: list[ProviderUsageEvent]) -> str | None:
    for row in sorted(rows, key=lambda item: item.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True):
        if not row.success:
            return (row.error or f"HTTP {row.status_code}" if row.status_code else "Provider check failed")[:240]
    return None


def _provider_health(db: Session, provider: str, *, since: datetime) -> dict[str, Any]:
    rows = _provider_rows(db, provider, since=since)
    latest = rows[0] if rows else _latest_provider_event(db, provider)
    return {
        "health": _status_from_latest(latest),
        "p95_latency_ms": _p95([float(row.duration_ms) for row in rows if row.duration_ms is not None]),
        "last_checked_at": _iso(latest.created_at) if latest else None,
        "last_success_at": _latest_success_at(rows),
        "latest_error": _latest_error(rows),
    }


def _provider_settings_by_domain(db: Session) -> dict[str, ProviderSetting]:
    try:
        return {row.domain_key: row for row in db.execute(select(ProviderSetting)).scalars().all()}
    except SQLAlchemyError:
        return {}


def _endpoint_from_setting(db_settings: dict[str, ProviderSetting], domain_key: str, fallback: str | None = None) -> str | None:
    setting = db_settings.get(domain_key)
    if setting:
        return _safe_endpoint_url(setting.primary_endpoint_url or setting.fallback_endpoint_url)
    default = provider_domain_catalog().get(domain_key)
    return _safe_endpoint_url((default.primary_endpoint_url or default.fallback_endpoint_url) if default else fallback)


def _configured_provider(
    *,
    id: str,
    name: str,
    purpose: str,
    safe_endpoint_url: str | None,
    secret_names: list[str],
    health: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": id,
        "name": name,
        "purpose": purpose,
        "safe_endpoint_url": safe_endpoint_url,
        "secret_names": secret_names,
        "secret_status": _secret_status(secret_names),
        **health,
    }


def _build_providers(db: Session, db_settings: dict[str, ProviderSetting], *, since: datetime) -> list[dict[str, Any]]:
    fmp_health = _provider_health(db, "fmp", since=since)
    public_unknown = {"health": "unknown", "p95_latency_ms": None, "last_checked_at": None, "last_success_at": None, "latest_error": None}
    providers = [
        _configured_provider(
            id="congress_sources",
            name="FMP Congress Latest",
            purpose="Primary normalized House/Senate disclosure feed; events retain official disclosure links",
            safe_endpoint_url=f"{FMP_STABLE_BASE_URL}/house-latest?page=0&limit=100 / {FMP_STABLE_BASE_URL}/senate-latest?page=0&limit=100",
            secret_names=SECRET_NAMES_BY_PROVIDER["congress_sources"],
            health=fmp_health,
        ),
        _configured_provider(
            id="sec_form4",
            name="FMP Insider Trading Latest",
            purpose="Primary normalized insider transaction feed; event links retain SEC Form 4 filings",
            safe_endpoint_url=f"{FMP_STABLE_BASE_URL}/insider-trading/latest?page=0&limit=100 / {FMP_STABLE_BASE_URL}/insider-trading/search?symbol={{symbol}}",
            secret_names=SECRET_NAMES_BY_PROVIDER["insider_trades"],
            health=fmp_health,
        ),
        _configured_provider(
            id="market_data",
            name="Market Data Provider",
            purpose="Prices, quotes, historical prices, fundamentals, profiles, and market snapshots",
            safe_endpoint_url=_endpoint_from_setting(db_settings, "prices_intraday", f"{FMP_STABLE_BASE_URL}/historical-chart/1min?symbol={{symbol}}"),
            secret_names=SECRET_NAMES_BY_PROVIDER["market_data"],
            health=fmp_health,
        ),
        _configured_provider(
            id="government_contracts",
            name="Government Contracts",
            purpose="Government contract awards/actions mapped to public company tickers",
            safe_endpoint_url="https://api.usaspending.gov/api/v2/search/spending_by_award/",
            secret_names=[],
            health=public_unknown,
        ),
        _configured_provider(
            id="institutional_13f",
            name="Institutional / 13F",
            purpose="Quarterly reported holdings, filing dates, and quarter-end holdings activity",
            safe_endpoint_url=_endpoint_from_setting(db_settings, "institutional_13f", f"{FMP_STABLE_BASE_URL}/institutional-ownership/latest?page=0&limit=1"),
            secret_names=SECRET_NAMES_BY_PROVIDER["institutional_13f"],
            health=fmp_health,
        ),
        _configured_provider(
            id="options_flow",
            name="Options Flow",
            purpose="Normalized options flow summaries for Pro options activity surfaces",
            safe_endpoint_url="https://api.massive.com/v3/snapshot/options/{symbol}",
            secret_names=SECRET_NAMES_BY_PROVIDER["options_flow"],
            health=_provider_health(db, "massive", since=since),
        ),
        _configured_provider(
            id="email",
            name="Postmark Email",
            purpose="Transactional, digest, and alert email delivery dependency",
            safe_endpoint_url="https://api.postmarkapp.com/email",
            secret_names=SECRET_NAMES_BY_PROVIDER["email"],
            health=_provider_health(db, "postmark", since=since),
        ),
    ]
    return providers


INTERNAL_ROUTE_DEFS = [
    ("/api/account/me", "GET", "Auth/session checks", "Session guard and account state."),
    ("/api/account/entitlements", "GET", "Entitlements", "Feature gates, plan limits, and paid access."),
    ("/api/events", "GET", "Feed/events", "Unified activity feed and ticker/member event surfaces."),
    ("/api/tickers/{symbol}/signals-summary", "GET", "Ticker summary/context", "Ticker signal summary and confirmation context."),
    ("/api/market/quotes", "GET", "Quotes", "Quote cards, feed rows, ticker pages, and screener rows."),
    ("/api/watchlists", "GET", "Watchlists/monitoring", "Watchlist dashboards and monitoring workflows."),
    ("/api/monitoring", "GET", "Watchlists/monitoring", "Monitoring inbox and alert state."),
    ("/api/screener", "GET", "Screener/signals", "Screener rows, filters, exports, and signal columns."),
    ("/api/admin/data-architecture", "GET", "Admin observability", "Read-only architecture snapshot endpoint."),
]


def _events_for_route(db: Session, route: str, *, since: datetime) -> list[ProviderUsageEvent]:
    normalized = route.replace("{symbol}", "")
    try:
        return list(
            db.execute(
                select(ProviderUsageEvent)
                .where(ProviderUsageEvent.route.like(f"{normalized}%") if "{symbol}" in route else ProviderUsageEvent.route == route)
                .where(ProviderUsageEvent.created_at >= since)
                .order_by(ProviderUsageEvent.created_at.desc())
                .limit(300)
            ).scalars().all()
        )
    except SQLAlchemyError:
        return []


def _route_health(rows: list[ProviderUsageEvent]) -> dict[str, Any]:
    if not rows:
        return {"health": "unknown", "p95_latency_ms": None, "error_rate": None, "last_seen_at": None}
    failures = [row for row in rows if not row.success or str(row.status_code or "").startswith(("4", "5"))]
    latest = rows[0]
    error_rate = round(len(failures) / len(rows), 4) if rows else None
    if str(latest.status_code or "").startswith("5"):
        health = "down"
    elif error_rate and error_rate >= 0.05:
        health = "degraded"
    elif latest.success:
        health = "healthy"
    else:
        health = "degraded"
    return {
        "health": health,
        "p95_latency_ms": _p95([float(row.duration_ms) for row in rows if row.duration_ms is not None]),
        "error_rate": error_rate,
        "last_seen_at": _iso(latest.created_at),
    }


def _build_internal_routes(db: Session, *, since: datetime) -> list[dict[str, Any]]:
    routes = []
    for route, method, consumer, notes in INTERNAL_ROUTE_DEFS:
        routes.append(
            {
                "route": route,
                "method": method,
                "consumer": consumer,
                **_route_health(_events_for_route(db, route, since=since)),
                "notes": notes,
            }
        )
    return routes


def _latest_job(db: Session, job_types: tuple[str, ...], statuses: tuple[str, ...] = ()) -> DataEnrichmentJob | None:
    if not job_types:
        return None
    try:
        statement = select(DataEnrichmentJob).where(DataEnrichmentJob.job_type.in_(job_types))
        if statuses:
            statement = statement.where(DataEnrichmentJob.status.in_(statuses))
        return db.execute(statement.order_by(DataEnrichmentJob.updated_at.desc(), DataEnrichmentJob.id.desc()).limit(1)).scalar_one_or_none()
    except SQLAlchemyError:
        return None


def _pipeline_health(last_success: datetime | None, latest_error: str | None, *, stale_after_hours: int = 24) -> Status:
    if latest_error and not last_success:
        return "down"
    if latest_error:
        return "degraded"
    if last_success is None:
        return "unknown"
    candidate = last_success if last_success.tzinfo else last_success.replace(tzinfo=timezone.utc)
    return "degraded" if _now() - candidate > timedelta(hours=stale_after_hours) else "healthy"


def _event_latest(db: Session, event_types: tuple[str, ...]) -> datetime | None:
    return _safe_scalar(db, select(func.max(Event.created_at)).where(Event.event_type.in_(event_types)))


def _latest_table_at(db: Session, model, column) -> datetime | None:
    return _safe_scalar(db, select(func.max(column)).select_from(model))


def _table_exists(db: Session, name: str) -> bool:
    try:
        return bool(inspect(db.get_bind()).has_table(name))
    except SQLAlchemyError:
        return False


def _pipeline(
    *,
    id: str,
    name: str,
    source: str,
    flow: list[str],
    health: Status,
    last_ingest_at: datetime | None,
    last_success_at: datetime | None,
    record_count: int | None,
    latest_error: str | None,
    notes: str | None = None,
) -> dict[str, Any]:
    return {
        "id": id,
        "name": name,
        "source": source,
        "flow": flow,
        "health": health,
        "last_ingest_at": _iso(last_ingest_at),
        "last_success_at": _iso(last_success_at),
        "record_count": record_count,
        "latest_error": latest_error,
        "notes": notes,
    }


def _build_pipelines(db: Session) -> list[dict[str, Any]]:
    congress_success = _latest_table_at(db, CongressDisclosureFiling, CongressDisclosureFiling.parsed_at) or _event_latest(db, ("congress_trade",))
    insider_success = _latest_table_at(db, SecForm4Filing, SecForm4Filing.parsed_at) or _event_latest(db, ("insider_trade",))
    market_success = _latest_table_at(db, PriceCache, PriceCache.updated_at)
    contracts_success = _latest_table_at(db, GovernmentContract, GovernmentContract.updated_at) or _event_latest(db, ("government_contract",))
    institutional_success = _latest_table_at(db, InstitutionalFiling, InstitutionalFiling.processed_at) or _event_latest(
        db,
        (
            "institutional_buy",
            "institutional_accumulation",
            "institutional_distribution",
            "new_institutional_position",
            "major_holder_reduction",
            "major_holder_exit",
        ),
    )
    congress_job = _latest_job(db, ("official_congress_ingest", "congress_recent_ingest", "house_disclosure_ingest", "senate_disclosure_ingest"))
    insider_job = _latest_job(db, ("sec_form4_ingest", "insider_trades_ingest"))
    market_job = _latest_job(db, ("price_cache_refresh", "ticker_hydration", "fundamentals_refresh"))
    contracts_job = _latest_job(db, ("government_contracts_ingest", "government_contract_actions_ingest"))
    institutional_job = _latest_job(db, ("institutional_ingest", "institutional_latest_ingest"))
    pipelines = [
        _pipeline(
            id="congress",
            name="Congress Activity",
            source="FMP house-latest / senate-latest",
            flow=["FMP latest disclosures", "Parser/normalizer", "Normalized Congress trades", "Feed/Ticker/Member pages"],
            health=_pipeline_health(congress_success, congress_job.error if congress_job and congress_job.status == "failed" else None),
            last_ingest_at=congress_job.updated_at if congress_job else congress_success,
            last_success_at=congress_success,
            record_count=None,
            latest_error=congress_job.error if congress_job and congress_job.status == "failed" else None,
            notes="Official House/Senate disclosure links are retained on normalized events.",
        ),
        _pipeline(
            id="insider",
            name="Insider Activity",
            source="FMP insider-trading/latest",
            flow=["FMP latest insider trades", "Normalizer", "Normalized insider trades", "Feed/Ticker/Insider pages"],
            health=_pipeline_health(insider_success, insider_job.error if insider_job and insider_job.status == "failed" else None),
            last_ingest_at=insider_job.updated_at if insider_job else insider_success,
            last_success_at=insider_success,
            record_count=None,
            latest_error=insider_job.error if insider_job and insider_job.status == "failed" else None,
            notes="SEC Form 4 filing links are retained on normalized events.",
        ),
        _pipeline(
            id="market_data",
            name="Market Data",
            source="Market data provider",
            flow=["Provider/bulk refresh", "Cache/database", "Quote endpoints", "Ticker/Feed/Screener"],
            health=_pipeline_health(market_success, market_job.error if market_job and market_job.status == "failed" else None),
            last_ingest_at=market_job.updated_at if market_job else market_success,
            last_success_at=market_success,
            record_count=None,
            latest_error=market_job.error if market_job and market_job.status == "failed" else None,
        ),
        _pipeline(
            id="government_contracts",
            name="Government Contracts",
            source="Government contracts source",
            flow=["Source refresh", "Normalized contract events", "Ticker/government contracts surfaces"],
            health=_pipeline_health(contracts_success, contracts_job.error if contracts_job and contracts_job.status == "failed" else None, stale_after_hours=72),
            last_ingest_at=contracts_job.updated_at if contracts_job else contracts_success,
            last_success_at=contracts_success,
            record_count=None,
            latest_error=contracts_job.error if contracts_job and contracts_job.status == "failed" else None,
        ),
        _pipeline(
            id="institutional",
            name="Institutional Activity",
            source="13F / institutional ownership provider",
            flow=["Quarterly reported holdings", "Filing date normalization", "Quarter-end holdings", "Pro institutional activity UI"],
            health=_pipeline_health(institutional_success, institutional_job.error if institutional_job and institutional_job.status == "failed" else None, stale_after_hours=24 * 35),
            last_ingest_at=institutional_job.updated_at if institutional_job else institutional_success,
            last_success_at=institutional_success,
            record_count=None,
            latest_error=institutional_job.error if institutional_job and institutional_job.status == "failed" else None,
            notes="Reported holdings are filing-date based quarter-end holdings, not live trading.",
        ),
    ]
    if _table_exists(db, "options_flow_summary") or _table_exists(db, "options_flow_events"):
        pipelines.append(
            _pipeline(
                id="options_flow",
                name="Options Flow",
                source="Options provider",
                flow=["Provider refresh", "Normalized options events", "Pro options activity UI"],
                health="unknown",
                last_ingest_at=None,
                last_success_at=None,
                record_count=None,
                latest_error=None,
            )
        )
    return pipelines


def _health_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "healthy": sum(1 for item in items if item.get("health") == "healthy"),
        "degraded": sum(1 for item in items if item.get("health") == "degraded"),
        "down": sum(1 for item in items if item.get("health") == "down"),
        "unknown": sum(1 for item in items if item.get("health") == "unknown"),
    }


def _cache_status(db: Session) -> dict[str, Any]:
    latest_price = _latest_table_at(db, PriceCache, PriceCache.updated_at)
    latest_event = _event_latest(db, ("congress_trade", "insider_trade", "government_contract"))
    latest_success = max([item for item in (latest_price, latest_event) if item is not None], default=None)
    status = _pipeline_health(latest_success, None, stale_after_hours=48)
    return {"status": status, "hit_rate": None, "last_checked_at": _iso(latest_success)}


def _database_status(db: Session) -> dict[str, Any]:
    try:
        db.execute(select(1)).scalar_one()
        status = "healthy"
    except SQLAlchemyError:
        status = "down"
    return {"name": "Production DB", "status": status, "p95_latency_ms": None, "last_checked_at": _iso(_now())}


def _jobs_status(db: Session) -> dict[str, Any]:
    failed = _safe_count(db, DataEnrichmentJob, DataEnrichmentJob.status == "failed") or 0
    running = _safe_count(db, DataEnrichmentJob, DataEnrichmentJob.status.in_(("queued", "running"))) or 0
    latest_success = _latest_job(db, ("price_cache_refresh", "ticker_hydration", "sec_form4_ingest", "official_congress_ingest", "institutional_ingest"), ("done", "completed", "success"))
    status = "degraded" if failed else "healthy" if running >= 0 else "unknown"
    return {"status": status, "queued_or_running": running, "failed": failed, "last_success_at": _iso(latest_success.updated_at) if latest_success else None}


def build_data_architecture_snapshot(db: Session) -> dict[str, Any]:
    generated_at = _now()
    since = generated_at - timedelta(hours=24)
    db_settings = _provider_settings_by_domain(db)
    providers = _build_providers(db, db_settings, since=since)
    internal_routes = _build_internal_routes(db, since=since)
    pipelines = _build_pipelines(db)
    database = _database_status(db)
    cache = _cache_status(db)
    jobs = _jobs_status(db)
    provider_counts = _health_counts(providers)
    route_counts = _health_counts(internal_routes)
    route_latencies = [item["p95_latency_ms"] for item in internal_routes if item.get("p95_latency_ms") is not None]
    provider_last_checked = [item.get("last_checked_at") for item in providers if item.get("last_checked_at")]
    last_successes = [pipeline.get("last_success_at") for pipeline in pipelines if pipeline.get("last_success_at")]
    stale_cutoff = generated_at - timedelta(seconds=FRESHNESS_WINDOW_SECONDS)
    newest_snapshot_time = max([datetime.fromisoformat(value) for value in provider_last_checked if value], default=None)
    stale = newest_snapshot_time is None or newest_snapshot_time < stale_cutoff
    overall = _worst_status(
        [database["status"], cache["status"], jobs["status"]]
        + [item["health"] for item in providers]
        + [item["health"] for item in pipelines]
    )
    return {
        "snapshot_generated_at": _iso(generated_at),
        "stale": stale,
        "freshness_window_seconds": FRESHNESS_WINDOW_SECONDS,
        "overall_status": overall,
        "frontend": {
            "name": "Vercel / Next.js",
            "status": "unknown",
            "notes": "Frontend status is observed indirectly through monitored API/admin route telemetry.",
        },
        "backend": {
            "name": "Fly.io / FastAPI",
            "status": _worst_status([item["health"] for item in internal_routes]),
            "p95_latency_ms": _p95(route_latencies),
            "notes": "Read from cached request/provider telemetry only; no live provider calls are made.",
        },
        "database": database,
        "cache": cache,
        "background_jobs": jobs,
        "summary": {
            "backend_routes": {
                "healthy": route_counts["healthy"],
                "degraded": route_counts["degraded"],
                "down": route_counts["down"],
                "unknown": route_counts["unknown"],
                "p95_latency_ms": _p95(route_latencies),
            },
            "providers": {
                "healthy": provider_counts["healthy"],
                "degraded": provider_counts["degraded"],
                "unavailable": provider_counts["down"],
                "unknown": provider_counts["unknown"],
                "last_snapshot_at": max(provider_last_checked) if provider_last_checked else None,
            },
            "cache_db": {
                "cache_status": cache["status"],
                "db_status": database["status"],
                "background_jobs_status": jobs["status"],
                "last_successful_refresh_at": max(last_successes) if last_successes else None,
            },
        },
        "providers": providers,
        "internal_routes": internal_routes,
        "pipelines": pipelines,
        "recent_events": [
            {
                "source": item["name"],
                "health": item["health"],
                "latest_error": item.get("latest_error"),
                "last_checked_at": item.get("last_checked_at") or item.get("last_success_at"),
            }
            for item in providers + pipelines
            if item.get("latest_error") or item.get("health") in {"degraded", "down", "unknown"}
        ][:12],
        "note": "Read-only architecture view. Configuration changes live in Settings or environment secrets.",
    }
