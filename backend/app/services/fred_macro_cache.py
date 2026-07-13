from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.models import FredObservation, FredSeriesRefresh

logger = logging.getLogger(__name__)

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_SOURCE = "fred"
MACRO_UNIT_LABELS = {
    "Fed Overnight Rate": "annualized %",
    "Core CPI": "YoY",
    "Unemployment": "labor force %",
    "Debt/GDP": "% of GDP",
    "Retail Sales": "USD",
    "GDP Growth": "QoQ annualized",
}
FRED_DEFAULT_TIMEOUT_SECONDS = 10
FRED_DEFAULT_MIN_REFRESH_HOURS = 6
FRED_DEFAULT_STALE_AFTER_HOURS = 36
FRED_DEFAULT_MAX_OBSERVATIONS_PER_SERIES = 5000

FRED_SERIES: dict[str, dict[str, Any]] = {
    "CPILFESL": {"label": "Core CPI", "block": "macro"},
    "FEDFUNDS": {"label": "Fed Overnight Rate", "block": "macro"},
    "UNRATE": {"label": "Unemployment", "block": "macro"},
    "RSAFS": {"label": "Retail Sales", "block": "macro"},
    "GDPC1": {"label": "GDP Growth", "block": "macro"},
    "GFDEGDQ188S": {"label": "Debt/GDP", "block": "macro"},
    "DGS3MO": {"label": "3M Treasury", "block": "treasury"},
    "DGS2": {"label": "2Y Treasury", "block": "treasury"},
    "DGS5": {"label": "5Y Treasury", "block": "treasury"},
    "DGS10": {"label": "10Y Treasury", "block": "treasury"},
    "DGS30": {"label": "30Y Treasury", "block": "treasury"},
}

FRED_REFRESH_SERIES_IDS = tuple(FRED_SERIES.keys())
FRED_ECONOMIC_ORDER = ("FEDFUNDS", "CPILFESL", "UNRATE", "GFDEGDQ188S", "RSAFS", "GDPC1")
FRED_TREASURY_ORDER = ("DGS3MO", "DGS2", "DGS5", "DGS10", "DGS30")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        logger.info("fred_macro_cache invalid integer env name=%s value=%s", name, raw)
        return default


def _timeout_seconds() -> int:
    return max(1, _env_int("FRED_TIMEOUT_SECONDS", FRED_DEFAULT_TIMEOUT_SECONDS))


def _min_refresh_interval() -> timedelta:
    hours = max(1, _env_int("FRED_MIN_REFRESH_INTERVAL_HOURS", FRED_DEFAULT_MIN_REFRESH_HOURS))
    return timedelta(hours=hours)


def _stale_after() -> timedelta:
    hours = max(1, _env_int("FRED_STALE_AFTER_HOURS", FRED_DEFAULT_STALE_AFTER_HOURS))
    return timedelta(hours=hours)


def _max_observations_per_series() -> int:
    return max(2, _env_int("FRED_MAX_OBSERVATIONS_PER_SERIES", FRED_DEFAULT_MAX_OBSERVATIONS_PER_SERIES))


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed == parsed else None
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned or cleaned == ".":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def _fetch_fred_csv(series_id: str) -> str:
    response = requests.get(
        FRED_CSV_URL,
        params={"id": series_id},
        timeout=_timeout_seconds(),
    )
    response.raise_for_status()
    return response.text


def parse_fred_csv(series_id: str, csv_text: str) -> list[dict[str, Any]]:
    rows = csv.DictReader(io.StringIO(csv_text))
    points: list[dict[str, Any]] = []
    seen_dates: set[date] = set()
    for row in rows:
        normalized = {str(key).strip().lstrip("\ufeff").lower(): value for key, value in row.items()}
        observation_date = _parse_date(normalized.get("observation_date") or normalized.get("date"))
        if observation_date is None or observation_date in seen_dates:
            continue
        raw_value = normalized.get(series_id.lower())
        if raw_value is None:
            raw_value = next((value for key, value in normalized.items() if key not in {"observation_date", "date"}), None)
        value = _parse_float(raw_value)
        if value is None:
            continue
        seen_dates.add(observation_date)
        points.append(
            {
                "series_id": series_id,
                "observation_date": observation_date,
                "value": value,
            }
        )
    points.sort(key=lambda point: point["observation_date"], reverse=True)
    return points[: _max_observations_per_series()]


def _observation_insert(db: Session):
    get_bind = getattr(db, "get_bind", None)
    dialect_name = get_bind().dialect.name if callable(get_bind) else "sqlite"
    if dialect_name == "postgresql":
        return postgres_insert(FredObservation.__table__)
    return sqlite_insert(FredObservation.__table__)


def _upsert_observations(db: Session, series_id: str, points: list[dict[str, Any]], now: datetime) -> None:
    if not points:
        return
    values = [
        {
            "series_id": series_id,
            "observation_date": point["observation_date"],
            "value": point["value"],
            "source": FRED_SOURCE,
            "payload_json": json.dumps({"series_id": series_id, "source": FRED_SOURCE}),
            "fetched_at": now,
            "updated_at": now,
        }
        for point in points
    ]
    stmt = _observation_insert(db).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["series_id", "observation_date"],
        set_={
            "value": stmt.excluded.value,
            "source": stmt.excluded.source,
            "payload_json": stmt.excluded.payload_json,
            "fetched_at": stmt.excluded.fetched_at,
            "updated_at": stmt.excluded.updated_at,
        },
    )
    db.execute(stmt)


def _state_for_update(db: Session, series_id: str) -> FredSeriesRefresh:
    state = db.get(FredSeriesRefresh, series_id)
    if state is None:
        state = FredSeriesRefresh(series_id=series_id, source=FRED_SOURCE, status="pending", observation_count=0)
        db.add(state)
        db.flush()
    return state


def _mark_state(
    db: Session,
    *,
    series_id: str,
    status: str,
    now: datetime,
    observation_count: int = 0,
    latest_observation_date: date | None = None,
    error: str | None = None,
) -> None:
    state = _state_for_update(db, series_id)
    state.source = FRED_SOURCE
    state.status = status
    state.observation_count = max(0, observation_count)
    state.latest_observation_date = latest_observation_date
    state.last_refreshed_at = now
    state.error = error
    state.updated_at = now


def _refresh_is_fresh(db: Session, series_ids: tuple[str, ...]) -> bool:
    cutoff = _utcnow() - _min_refresh_interval()
    rows = db.execute(select(FredSeriesRefresh).where(FredSeriesRefresh.series_id.in_(series_ids))).scalars().all()
    by_series = {row.series_id: row for row in rows}
    for series_id in series_ids:
        row = by_series.get(series_id)
        if row is None or row.status != "ok":
            return False
        refreshed_at = _aware(row.last_refreshed_at)
        if refreshed_at is None or refreshed_at < cutoff:
            return False
    return True


def refresh_fred_macro_cache(
    db: Session,
    *,
    series_ids: tuple[str, ...] = FRED_REFRESH_SERIES_IDS,
    force: bool = False,
) -> dict[str, Any]:
    normalized_ids = tuple(series_id.upper().strip() for series_id in series_ids if series_id and series_id.strip())
    if not normalized_ids:
        return {"status": "skipped", "refreshed_series": 0, "series": []}

    if not force and _refresh_is_fresh(db, normalized_ids):
        diagnostics = fred_macro_cache_diagnostics(db, series_ids=normalized_ids)
        return {
            "status": "skipped",
            "reason": "fresh",
            "refreshed_series": 0,
            "last_refresh_at": diagnostics.get("last_refresh_at"),
            "missing_series": diagnostics.get("missing_series", []),
        }

    now = _utcnow()
    refreshed = 0
    failed = 0
    details: list[dict[str, Any]] = []
    for series_id in normalized_ids:
        try:
            points = parse_fred_csv(series_id, _fetch_fred_csv(series_id))
            _upsert_observations(db, series_id, points, now)
            latest_date = points[0]["observation_date"] if points else None
            status = "ok" if points else "empty"
            _mark_state(
                db,
                series_id=series_id,
                status=status,
                now=now,
                observation_count=len(points),
                latest_observation_date=latest_date,
                error=None if points else "no_observations",
            )
            refreshed += 1 if points else 0
            if not points:
                failed += 1
            details.append(
                {
                    "series_id": series_id,
                    "status": status,
                    "observation_count": len(points),
                    "latest_observation_date": latest_date.isoformat() if latest_date else None,
                }
            )
        except Exception as exc:
            failed += 1
            logger.exception("fred_series_refresh_failed series_id=%s", series_id)
            previous_state = db.get(FredSeriesRefresh, series_id)
            _mark_state(
                db,
                series_id=series_id,
                status="error",
                now=now,
                observation_count=int(previous_state.observation_count or 0) if previous_state else 0,
                latest_observation_date=previous_state.latest_observation_date if previous_state else None,
                error=f"{exc.__class__.__name__}: {str(exc)[:200]}",
            )
            details.append({"series_id": series_id, "status": "error", "error": exc.__class__.__name__})

    db.commit()
    status = "ok" if failed == 0 else "partial" if refreshed else "unavailable"
    diagnostics = fred_macro_cache_diagnostics(db, series_ids=normalized_ids)
    return {
        "status": status,
        "refreshed_series": refreshed,
        "failed_series": failed,
        "series": details,
        "last_refresh_at": diagnostics.get("last_refresh_at"),
        "missing_series": diagnostics.get("missing_series", []),
    }


def _series_points(db: Session, series_id: str, *, limit: int = 32) -> list[dict[str, Any]]:
    rows = (
        db.execute(
            select(FredObservation)
            .where(FredObservation.series_id == series_id)
            .order_by(FredObservation.observation_date.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [
        {
            "date": row.observation_date.isoformat() if row.observation_date else None,
            "value": row.value,
        }
        for row in rows
        if row.observation_date is not None and row.value is not None
    ]


def _series_state_map(db: Session, series_ids: tuple[str, ...]) -> dict[str, FredSeriesRefresh]:
    rows = db.execute(select(FredSeriesRefresh).where(FredSeriesRefresh.series_id.in_(series_ids))).scalars().all()
    return {row.series_id: row for row in rows}


def _cache_status(state: FredSeriesRefresh | None) -> str:
    if state is None:
        return "missing"
    if state.status != "ok":
        return "stale" if state.latest_observation_date else "missing"
    refreshed_at = _aware(state.last_refreshed_at)
    if refreshed_at is None or (_utcnow() - refreshed_at) > _stale_after():
        return "stale"
    return "fresh"


def _context_label(state: FredSeriesRefresh | None) -> str:
    status = _cache_status(state)
    if status == "fresh":
        return "FRED cache"
    if status == "stale":
        return "FRED cache stale"
    return "FRED unavailable"


def _macro_point(
    *,
    label: str,
    value: float | None,
    value_format: str,
    date_value: str | None,
    series_id: str,
    state: FredSeriesRefresh | None,
    change_value: float | None = None,
    change_format: str | None = None,
    change_label: str | None = None,
    unit_label: str | None = None,
) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "value_format": value_format,
        "date": date_value,
        "change_value": change_value,
        "change_format": change_format,
        "change_label": change_label,
        "context_label": _context_label(state),
        "source": FRED_SOURCE,
        "series_id": series_id,
        "cache_status": _cache_status(state),
        "unit_label": unit_label or MACRO_UNIT_LABELS.get(label),
    }


def _macro_unavailable(
    label: str,
    *,
    series_id: str,
    state: FredSeriesRefresh | None,
    value_format: str = "percent",
    change_format: str | None = None,
    change_label: str | None = None,
    unit_label: str | None = None,
) -> dict[str, Any]:
    return _macro_point(
        label=label,
        value=None,
        value_format=value_format,
        date_value=None,
        series_id=series_id,
        state=state,
        change_value=None,
        change_format=change_format,
        change_label=change_label,
        unit_label=unit_label,
    )


def _series_change(series: list[dict[str, Any]]) -> float | None:
    if len(series) < 2:
        return None
    return float(series[0]["value"]) - float(series[1]["value"])


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _prior_year_point(point: dict[str, Any], series: list[dict[str, Any]], *, tolerance_days: int = 45) -> dict[str, Any] | None:
    point_date = _parse_iso_date(point.get("date"))
    if point_date is None:
        return None
    target_year = point_date.year - 1
    by_month = {
        (candidate_date.year, candidate_date.month): candidate
        for candidate in series
        if (candidate_date := _parse_iso_date(candidate.get("date"))) is not None
    }
    exact = by_month.get((target_year, point_date.month))
    if exact:
        return exact
    try:
        target_date = point_date.replace(year=target_year)
    except ValueError:
        target_date = date(target_year, point_date.month, 28)
    closest: tuple[int, dict[str, Any]] | None = None
    for candidate in series:
        candidate_date = _parse_iso_date(candidate.get("date"))
        if candidate_date is None:
            continue
        distance = abs((candidate_date - target_date).days)
        if distance > tolerance_days:
            continue
        if closest is None or distance < closest[0]:
            closest = (distance, candidate)
    return closest[1] if closest else None


def _yoy_series(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for point in series:
        prior = _prior_year_point(point, series)
        if prior is None or not prior.get("value"):
            continue
        points.append(
            {
                "date": point["date"],
                "value": ((float(point["value"]) / float(prior["value"])) - 1.0) * 100.0,
            }
        )
    return points


def _direct_percent_point(
    db: Session,
    series_id: str,
    *,
    label: str,
    states: dict[str, FredSeriesRefresh],
    change_format: str = "percentage_points",
) -> dict[str, Any]:
    state = states.get(series_id)
    series = _series_points(db, series_id, limit=4)
    if not series:
        return _macro_unavailable(label, series_id=series_id, state=state, change_format=change_format)
    change = _series_change(series)
    if change is not None and change_format == "bps":
        change *= 100.0
    return _macro_point(
        label=label,
        value=series[0]["value"],
        value_format="percent",
        date_value=series[0]["date"],
        series_id=series_id,
        state=state,
        change_value=change,
        change_format=change_format,
    )


def _core_cpi_point(db: Session, states: dict[str, FredSeriesRefresh]) -> dict[str, Any]:
    series_id = "CPILFESL"
    state = states.get(series_id)
    series = _series_points(db, series_id, limit=30)
    yoy = _yoy_series(series)
    if not yoy:
        return _macro_unavailable("Core CPI", series_id=series_id, state=state, change_format="percentage_points", change_label="YoY")
    return _macro_point(
        label="Core CPI",
        value=yoy[0]["value"],
        value_format="percent",
        date_value=yoy[0]["date"],
        series_id=series_id,
        state=state,
        change_value=_series_change(yoy),
        change_format="percentage_points",
        change_label="YoY",
    )


def _retail_sales_point(db: Session, states: dict[str, FredSeriesRefresh]) -> dict[str, Any]:
    series_id = "RSAFS"
    state = states.get(series_id)
    series = _series_points(db, series_id, limit=4)
    if not series:
        return _macro_unavailable("Retail Sales", series_id=series_id, state=state, value_format="currency", change_format="percent", change_label="MoM")
    latest = series[0]
    change_pct = None
    if len(series) >= 2 and series[1]["value"]:
        change_pct = ((float(latest["value"]) / float(series[1]["value"])) - 1.0) * 100.0
    return _macro_point(
        label="Retail Sales",
        value=float(latest["value"]) * 1_000_000.0,
        value_format="currency",
        date_value=latest["date"],
        series_id=series_id,
        state=state,
        change_value=change_pct,
        change_format="percent",
        change_label="MoM",
    )


def _gdp_growth_point(db: Session, states: dict[str, FredSeriesRefresh]) -> dict[str, Any]:
    series_id = "GDPC1"
    state = states.get(series_id)
    series = _series_points(db, series_id, limit=5)
    if len(series) < 2 or not series[1]["value"]:
        return _macro_unavailable("GDP Growth", series_id=series_id, state=state, change_format="percentage_points", change_label="QoQ annualized")
    latest_growth = ((float(series[0]["value"]) / float(series[1]["value"])) ** 4 - 1.0) * 100.0
    previous_growth = None
    if len(series) >= 3 and series[2]["value"]:
        previous_growth = ((float(series[1]["value"]) / float(series[2]["value"])) ** 4 - 1.0) * 100.0
    return _macro_point(
        label="GDP Growth",
        value=latest_growth,
        value_format="percent",
        date_value=series[0]["date"],
        series_id=series_id,
        state=state,
        change_value=latest_growth - previous_growth if previous_growth is not None else None,
        change_format="percentage_points",
        change_label="QoQ annualized",
    )


def build_fred_economics(db: Session) -> list[dict[str, Any]]:
    states = _series_state_map(db, FRED_ECONOMIC_ORDER)
    return [
        _direct_percent_point(db, "FEDFUNDS", label="Fed Overnight Rate", states=states, change_format="bps"),
        _core_cpi_point(db, states),
        _direct_percent_point(db, "UNRATE", label="Unemployment", states=states),
        _direct_percent_point(db, "GFDEGDQ188S", label="Debt/GDP", states=states),
        _retail_sales_point(db, states),
        _gdp_growth_point(db, states),
    ]


def build_fred_treasury(db: Session) -> list[dict[str, Any]]:
    states = _series_state_map(db, FRED_TREASURY_ORDER)
    items: list[dict[str, Any]] = []
    for series_id in FRED_TREASURY_ORDER:
        config = FRED_SERIES[series_id]
        state = states.get(series_id)
        series = _series_points(db, series_id, limit=4)
        if not series:
            items.append(
                _macro_unavailable(
                    str(config["label"]),
                    series_id=series_id,
                    state=state,
                    value_format="percent",
                    change_format="bps",
                )
            )
            continue
        change = _series_change(series)
        items.append(
            {
                **_macro_point(
                    label=str(config["label"]),
                    value=series[0]["value"],
                    value_format="percent",
                    date_value=series[0]["date"],
                    series_id=series_id,
                    state=state,
                    change_value=change * 100.0 if change is not None else None,
                    change_format="bps",
                ),
                "change": change * 100.0 if change is not None else None,
                "change_unit": "bps",
                "timeframe_label": "1D change",
                "unit_label": "yield",
            }
        )
    return items


def build_fred_macro_sections(db: Session) -> dict[str, Any]:
    diagnostics = fred_macro_cache_diagnostics(db)
    economics = build_fred_economics(db)
    treasury = build_fred_treasury(db)
    return {
        "economics": economics,
        "treasury": treasury,
        "diagnostics": diagnostics,
    }


def _series_row(
    series_id: str,
    state: FredSeriesRefresh | None,
    latest_date: date | str | None,
) -> dict[str, Any]:
    config = FRED_SERIES.get(series_id, {})
    latest_date_text = latest_date.isoformat() if isinstance(latest_date, date) else str(latest_date) if latest_date else None
    return {
        "series_id": series_id,
        "label": config.get("label", series_id),
        "block": config.get("block"),
        "status": state.status if state else "missing",
        "cache_status": _cache_status(state),
        "last_refreshed_at": _aware(state.last_refreshed_at).isoformat() if state and state.last_refreshed_at else None,
        "latest_observation_date": latest_date_text,
        "observation_count": int(state.observation_count or 0) if state else 0,
        "error": state.error if state else None,
    }


def fred_macro_cache_diagnostics(
    db: Session,
    *,
    series_ids: tuple[str, ...] = FRED_REFRESH_SERIES_IDS,
) -> dict[str, Any]:
    normalized_ids = tuple(series_id.upper().strip() for series_id in series_ids if series_id and series_id.strip())
    if not normalized_ids:
        return {
            "source": FRED_SOURCE,
            "status": "skipped",
            "last_refresh_at": None,
            "missing_series": [],
            "stale_series": [],
            "series": [],
        }
    try:
        states = _series_state_map(db, normalized_ids)
        latest_rows = db.execute(
            select(FredObservation.series_id, func.max(FredObservation.observation_date))
            .where(FredObservation.series_id.in_(normalized_ids))
            .group_by(FredObservation.series_id)
        ).all()
        latest_by_series = {row[0]: row[1] for row in latest_rows}
        rows = [_series_row(series_id, states.get(series_id), latest_by_series.get(series_id)) for series_id in normalized_ids]
        last_refresh_at = max(
            (_aware(state.last_refreshed_at) for state in states.values() if state.last_refreshed_at),
            default=None,
        )
        missing_series = [row["series_id"] for row in rows if not row.get("latest_observation_date")]
        stale_series = [row["series_id"] for row in rows if row.get("cache_status") in {"missing", "stale"}]
        error_series = [row["series_id"] for row in rows if row.get("status") == "error"]
        status = "ok"
        if missing_series or error_series:
            status = "unavailable" if len(missing_series) == len(normalized_ids) else "partial"
        elif stale_series:
            status = "stale"
        return {
            "source": FRED_SOURCE,
            "status": status,
            "last_refresh_at": last_refresh_at.isoformat() if last_refresh_at else None,
            "missing_series": missing_series,
            "stale_series": stale_series,
            "error_series": error_series,
            "series": rows,
        }
    except Exception as exc:
        logger.info("fred_macro_cache diagnostics unavailable", exc_info=True)
        return {
            "source": FRED_SOURCE,
            "status": "unavailable",
            "last_refresh_at": None,
            "missing_series": list(normalized_ids),
            "stale_series": list(normalized_ids),
            "error": exc.__class__.__name__,
            "series": [],
        }
