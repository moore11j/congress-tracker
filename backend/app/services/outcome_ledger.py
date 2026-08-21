from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timezone, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import AppSetting, ConfirmationMethodologyVersion, ConfirmationScoreSnapshot, PriceCache, Security, TickerContextBundleCache, TickerMeta
from app.services.confirmation_score import CONFIRMATION_CLASSIFICATION_VERSION, SOURCE_ORDER, confirmation_active_source_count

logger = logging.getLogger(__name__)

OUTCOMES_LEDGER_SETTING_KEY = "outcomes_ledger_enabled"
OUTCOMES_LEDGER_DUPLICATES_KEY = "outcome_ledger_duplicate_attempts_ignored"
OUTCOMES_LEDGER_ERRORS_KEY = "outcome_ledger_persistence_errors"
OUTCOMES_LEDGER_MISSING_PRICE_KEY = "outcome_ledger_missing_reference_prices"
OUTCOMES_LEDGER_STALE_REFERENCE_PRICE_KEY = "outcome_ledger_stale_reference_prices"
OUTCOMES_LEDGER_MISSING_SECURITY_KEY = "outcome_ledger_missing_security_ids"
OUTCOMES_LEDGER_MISSING_SOURCE_PAYLOAD_KEY = "outcome_ledger_missing_source_contribution_payloads"
CURRENT_CONFIRMATION_METHODOLOGY_VERSION = "confirmation-v2"
OUTCOME_HORIZONS = (7, 30, 90, 180, 365)
PriceRowsBySymbol = dict[str, list[PriceCache]]
OUTCOME_SCORE_BANDS = ("0-39", "40-59", "60-64", "65-69", "70-74", "75-79", "80+")
DIRECTIONAL_OUTCOME_SIDES = ("bullish", "bearish")
OUTCOME_LEDGER_CACHE_SYMBOL = "__OUTCOME_LEDGER__"
OUTCOME_LEDGER_CACHE_PREFIX = "outcome-ledger:v2"


@dataclass(frozen=True)
class DirectionalOutcomeEvent:
    snapshot: ConfirmationScoreSnapshot
    closed_at: date | None = None


def outcome_ledger_enabled(db: Session | None = None) -> bool:
    raw_env = os.getenv("OUTCOMES_LEDGER_ENABLED")
    if raw_env is not None:
        return raw_env.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    if db is not None:
        try:
            row = db.get(AppSetting, OUTCOMES_LEDGER_SETTING_KEY)
            if row and row.value is not None:
                return row.value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
        except Exception:
            db.rollback()
            logger.info("outcome_ledger_flag_lookup_failed", exc_info=True)
    return True


def outcome_ledger_cache_ttl_seconds() -> int:
    try:
        return max(60, min(86400, int(os.getenv("OUTCOME_LEDGER_PERSISTENT_CACHE_TTL_SECONDS", "43200") or 43200)))
    except ValueError:
        return 43200


def outcome_ledger_cache_expiry_seconds() -> int:
    try:
        return max(300, min(604800, int(os.getenv("OUTCOME_LEDGER_PERSISTENT_CACHE_EXPIRY_SECONDS", "172800") or 172800)))
    except ValueError:
        return 172800


def public_outcome_ledger_cache_key(kind: str, params: dict[str, Any] | None = None) -> str:
    serialized = json.dumps(params or {}, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:24]
    return f"{OUTCOME_LEDGER_CACHE_PREFIX}:{kind}:{digest}"


def cached_public_outcome_ledger_payload(db: Session, cache_key: str) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    try:
        row = db.get(TickerContextBundleCache, cache_key)
    except Exception:
        db.rollback()
        logger.info("outcome_ledger_persistent_cache_read_failed key=%s", cache_key, exc_info=True)
        return None
    if row is None:
        return None
    expires_at = row.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= now:
        return None
    try:
        payload = json.loads(row.payload_json or "{}")
    except Exception:
        logger.info("outcome_ledger_persistent_cache_decode_failed key=%s", cache_key, exc_info=True)
        return None
    return payload if isinstance(payload, dict) else None


def store_public_outcome_ledger_payload(db: Session, cache_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    stale_after = now + timedelta(seconds=outcome_ledger_cache_ttl_seconds())
    expires_at = now + timedelta(seconds=outcome_ledger_cache_expiry_seconds())
    payload_json = json.dumps(payload, default=str, separators=(",", ":"))
    try:
        row = db.get(TickerContextBundleCache, cache_key)
        if row is None:
            db.add(
                TickerContextBundleCache(
                    cache_key=cache_key,
                    symbol=OUTCOME_LEDGER_CACHE_SYMBOL,
                    user_segment="public",
                    payload_json=payload_json,
                    generated_at=now,
                    stale_after=stale_after,
                    expires_at=expires_at,
                )
            )
        else:
            row.symbol = OUTCOME_LEDGER_CACHE_SYMBOL
            row.user_segment = "public"
            row.payload_json = payload_json
            row.generated_at = now
            row.stale_after = stale_after
            row.expires_at = expires_at
        db.commit()
    except Exception:
        db.rollback()
        logger.info("outcome_ledger_persistent_cache_write_failed key=%s", cache_key, exc_info=True)
    return payload


def current_code_commit_sha() -> str:
    return (
        os.getenv("SOURCE_COMMIT_SHA")
        or os.getenv("GIT_COMMIT_SHA")
        or os.getenv("VERCEL_GIT_COMMIT_SHA")
        or os.getenv("FLY_COMMIT_SHA")
        or "unknown"
    )


def current_methodology_configuration() -> dict[str, Any]:
    return {
        "classification_version": CONFIRMATION_CLASSIFICATION_VERSION,
        "lookback_days": 30,
        "source_order": list(SOURCE_ORDER),
        "score_bands": {
            "inactive": [0, 19],
            "weak": [20, 39],
            "moderate": [40, 59],
            "strong": [60, 79],
            "exceptional": [80, 100],
        },
        "notes": "Confirmation v2 keeps the ticker-page score shape but calibrates direction for 30D outcomes: durable sources carry more weight, short-horizon tape carries less weight, and bearish calls require stronger confirmation.",
        "outcome_target": {
            "primary_horizon": "30D",
            "primary_metric": "directional accuracy and excess return versus SPY",
            "secondary_horizon": "7D",
            "long_horizons": ["90D", "180D", "365D"],
            "long_horizon_policy": "report only after larger 30D-calibrated samples mature",
        },
    }


def register_confirmation_methodology_version(
    db: Session,
    *,
    version: str = CURRENT_CONFIRMATION_METHODOLOGY_VERSION,
    description: str = "Current Walnut confirmation score methodology.",
    configuration: dict[str, Any] | None = None,
    code_commit_sha: str | None = None,
    make_current: bool = True,
) -> ConfirmationMethodologyVersion:
    existing = db.execute(
        select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.version == version)
    ).scalar_one_or_none()
    if existing is not None:
        if make_current and not existing.is_current:
            db.query(ConfirmationMethodologyVersion).filter(ConfirmationMethodologyVersion.id != existing.id).update(
                {ConfirmationMethodologyVersion.is_current: False},
                synchronize_session=False,
            )
            existing.is_current = True
            existing.retired_at = None
            db.flush()
        return existing

    if make_current:
        db.query(ConfirmationMethodologyVersion).update(
            {ConfirmationMethodologyVersion.is_current: False, ConfirmationMethodologyVersion.retired_at: datetime.now(timezone.utc)},
            synchronize_session=False,
        )
    methodology = ConfirmationMethodologyVersion(
        version=version,
        description=description,
        configuration_json=json.dumps(configuration or current_methodology_configuration(), sort_keys=True, separators=(",", ":")),
        code_commit_sha=code_commit_sha or current_code_commit_sha(),
        deployed_at=datetime.now(timezone.utc),
        is_current=make_current,
    )
    db.add(methodology)
    db.flush()
    return methodology


def current_confirmation_methodology(db: Session) -> ConfirmationMethodologyVersion:
    methodology = db.execute(
        select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.is_current.is_(True))
    ).scalar_one_or_none()
    if methodology is not None and methodology.version == CURRENT_CONFIRMATION_METHODOLOGY_VERSION:
        return methodology
    methodology = register_confirmation_methodology_version(db)
    db.commit()
    return methodology


def _increment_counter(db: Session, key: str, amount: int = 1) -> None:
    row = db.get(AppSetting, key)
    if row is None:
        row = AppSetting(key=key, value=str(amount))
        db.add(row)
        return
    try:
        current = int(row.value or "0")
    except ValueError:
        current = 0
    row.value = str(current + amount)


def _counter_value(db: Session, key: str) -> int:
    row = db.get(AppSetting, key)
    try:
        return int(row.value or "0") if row else 0
    except ValueError:
        return 0


def _normalized_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalized_json(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_normalized_json(item) for item in value]
    if isinstance(value, float):
        return round(value, 8)
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def source_contributions_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    contributions: dict[str, Any] = {}
    for key in SOURCE_ORDER:
        source = sources.get(key)
        if not isinstance(source, dict):
            continue
        contributions[key] = {
            "present": source.get("present") is True,
            "direction": source.get("direction"),
            "strength": source.get("strength"),
            "quality": source.get("quality"),
            "score_contribution": source.get("score_contribution"),
            "label": source.get("label"),
            "detail": source.get("detail"),
            "summary": source.get("summary"),
        }
    return contributions


def source_freshness_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    freshness: dict[str, Any] = {}
    for key in SOURCE_ORDER:
        source = sources.get(key)
        if isinstance(source, dict):
            freshness[key] = {"freshness_days": source.get("freshness_days")}
    return freshness


def input_hash_for_confirmation_bundle(bundle: dict[str, Any], methodology: ConfirmationMethodologyVersion) -> str:
    payload = {
        "methodology_version": methodology.version,
        "score": bundle.get("score"),
        "band": bundle.get("band"),
        "direction": bundle.get("direction"),
        "status": bundle.get("status"),
        "active_sources": bundle.get("active_sources") if isinstance(bundle.get("active_sources"), list) else [],
        "source_contributions": source_contributions_from_bundle(bundle),
        "source_freshness": source_freshness_from_bundle(bundle),
        "classification_version": bundle.get("classification_version"),
    }
    encoded = json.dumps(_normalized_json(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _resolve_security(db: Session, symbol: str) -> Security | None:
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol)).scalar_one_or_none()
    if security is not None:
        return security
    ticker_meta = db.get(TickerMeta, symbol)
    security = Security(
        symbol=symbol,
        name=(ticker_meta.company_name if ticker_meta else None) or symbol,
        asset_class="equity",
        sector=(ticker_meta.sector if ticker_meta else None),
    )
    db.add(security)
    db.flush()
    return security


def _latest_reference_price(db: Session, symbol: str, *, as_of_date: date | None = None) -> tuple[float | None, datetime | None, str | None, date]:
    normalized_symbol = symbol.strip().upper()
    query = select(PriceCache).where(PriceCache.symbol == normalized_symbol)
    if as_of_date is not None:
        query = query.where(PriceCache.date <= as_of_date.isoformat())
    row = db.execute(query.order_by(PriceCache.date.desc()).limit(1)).scalar_one_or_none()
    if row is None:
        return None, None, None, datetime.now(timezone.utc).date()
    try:
        market_date = date.fromisoformat(str(row.date)[:10])
    except ValueError:
        market_date = datetime.now(timezone.utc).date()
    price_at = datetime.combine(market_date, time(21, 0), tzinfo=timezone.utc)
    return row.close, price_at, row.price_source or "price_cache", market_date


def _strength_from_bundle(bundle: dict[str, Any]) -> str:
    strength = bundle.get("classification_strength")
    if isinstance(strength, str) and strength:
        return strength
    band = bundle.get("band")
    if band in {"inactive", "weak", "moderate", "strong", "exceptional"}:
        return str(band)
    score = bundle.get("score")
    try:
        score_int = int(score)
    except (TypeError, ValueError):
        return "inactive"
    if score_int >= 80:
        return "exceptional"
    if score_int >= 60:
        return "strong"
    if score_int >= 40:
        return "moderate"
    if score_int >= 20:
        return "weak"
    return "inactive"


def capture_live_confirmation_score_snapshot(
    db: Session,
    symbol: str,
    bundle: dict[str, Any],
    *,
    calculated_at: datetime | None = None,
    calculation_type: str = "live",
) -> ConfirmationScoreSnapshot | None:
    if calculation_type != "live":
        return None
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol or not isinstance(bundle, dict):
        return None
    if not outcome_ledger_enabled(db):
        return None

    try:
        methodology = current_confirmation_methodology(db)
        security = _resolve_security(db, normalized_symbol)
        if security is None:
            _increment_counter(db, OUTCOMES_LEDGER_MISSING_SECURITY_KEY)
            db.commit()
            return None
        calculated = calculated_at or datetime.now(timezone.utc)
        if calculated.tzinfo is None:
            calculated = calculated.replace(tzinfo=timezone.utc)
        reference_price, reference_price_at, reference_price_source, market_date = _latest_reference_price(db, normalized_symbol, as_of_date=calculated.date())
        if reference_price is None:
            _increment_counter(db, OUTCOMES_LEDGER_MISSING_PRICE_KEY)
        max_stale_days = int(os.getenv("OUTCOME_LEDGER_LIVE_REFERENCE_MAX_STALE_DAYS", "5") or 5)
        if reference_price is None or market_date < calculated.date() - timedelta(days=max(1, max_stale_days)):
            _increment_counter(db, OUTCOMES_LEDGER_STALE_REFERENCE_PRICE_KEY)
            db.commit()
            return None
        active_sources = bundle.get("active_sources") if isinstance(bundle.get("active_sources"), list) else []
        source_contributions = source_contributions_from_bundle(bundle)
        source_freshness = source_freshness_from_bundle(bundle)
        if not source_contributions:
            _increment_counter(db, OUTCOMES_LEDGER_MISSING_SOURCE_PAYLOAD_KEY)
        input_hash = input_hash_for_confirmation_bundle(bundle, methodology)

        existing = db.execute(
            select(ConfirmationScoreSnapshot).where(
                ConfirmationScoreSnapshot.security_id == security.id,
                ConfirmationScoreSnapshot.methodology_version_id == methodology.id,
                ConfirmationScoreSnapshot.market_date == market_date,
                ConfirmationScoreSnapshot.input_hash == input_hash,
                ConfirmationScoreSnapshot.calculation_type == calculation_type,
            )
        ).scalar_one_or_none()
        if existing is not None:
            _increment_counter(db, OUTCOMES_LEDGER_DUPLICATES_KEY)
            db.commit()
            return existing

        visible_duplicate = db.execute(
            select(ConfirmationScoreSnapshot).where(
                ConfirmationScoreSnapshot.security_id == security.id,
                ConfirmationScoreSnapshot.methodology_version_id == methodology.id,
                ConfirmationScoreSnapshot.market_date == market_date,
                ConfirmationScoreSnapshot.calculation_type == calculation_type,
            ).order_by(ConfirmationScoreSnapshot.calculated_at.desc(), ConfirmationScoreSnapshot.id.desc()).limit(1)
        ).scalar_one_or_none()
        normalized_direction = str(bundle.get("direction") or "neutral")
        normalized_score = int(bundle.get("score") or 0)
        if visible_duplicate is not None and visible_duplicate.direction == normalized_direction and visible_duplicate.score == normalized_score:
            _increment_counter(db, OUTCOMES_LEDGER_DUPLICATES_KEY)
            db.commit()
            return visible_duplicate

        snapshot = ConfirmationScoreSnapshot(
            security_id=security.id,
            ticker_at_time=normalized_symbol,
            calculated_at=calculated,
            market_date=market_date,
            score=normalized_score,
            direction=normalized_direction,
            strength=_strength_from_bundle(bundle),
            reference_price=reference_price,
            reference_price_at=reference_price_at,
            reference_price_source=reference_price_source,
            active_source_count=confirmation_active_source_count(bundle),
            active_sources_json=json.dumps(_normalized_json(active_sources), sort_keys=True, separators=(",", ":")),
            source_contributions_json=json.dumps(_normalized_json(source_contributions), sort_keys=True, separators=(",", ":")),
            source_freshness_json=json.dumps(_normalized_json(source_freshness), sort_keys=True, separators=(",", ":")),
            input_hash=input_hash,
            methodology_version_id=methodology.id,
            calculation_type=calculation_type,
            code_commit_sha=current_code_commit_sha(),
            supersedes_snapshot_id=visible_duplicate.id if visible_duplicate is not None else None,
        )
        db.add(snapshot)
        db.commit()
        return snapshot
    except IntegrityError:
        db.rollback()
        try:
            _increment_counter(db, OUTCOMES_LEDGER_DUPLICATES_KEY)
            db.commit()
        except Exception:
            db.rollback()
        logger.info("outcome_ledger_duplicate_snapshot symbol=%s", normalized_symbol)
        return None
    except Exception:
        db.rollback()
        try:
            _increment_counter(db, OUTCOMES_LEDGER_ERRORS_KEY)
            db.commit()
        except Exception:
            db.rollback()
        logger.warning("outcome_ledger_snapshot_capture_failed symbol=%s", normalized_symbol, exc_info=True)
        return None


def _price_on_or_after(db: Session, symbol: str, target_date: date) -> PriceCache | None:
    normalized_symbol = symbol.strip().upper()
    return db.execute(
        select(PriceCache)
        .where(
            PriceCache.symbol == normalized_symbol,
            PriceCache.date >= target_date.isoformat(),
        )
        .order_by(PriceCache.date.asc())
        .limit(1)
    ).scalar_one_or_none()


def _price_date(row: PriceCache) -> date | None:
    try:
        return date.fromisoformat(str(row.date)[:10])
    except (TypeError, ValueError):
        return None


def _price_on_or_after_from_rows(rows_by_symbol: PriceRowsBySymbol, symbol: str, target_date: date) -> PriceCache | None:
    for row in rows_by_symbol.get(symbol.strip().upper(), []):
        row_date = _price_date(row)
        if row_date is not None and row_date >= target_date:
            return row
    return None


def _prefetch_outcome_price_rows(
    db: Session,
    snapshots: list[ConfirmationScoreSnapshot],
    *,
    horizons: tuple[int, ...] = OUTCOME_HORIZONS,
) -> PriceRowsBySymbol:
    if not snapshots:
        return {}

    today = datetime.now(timezone.utc).date()
    min_needed_date: date | None = None
    max_needed_date: date | None = None
    symbols = {"SPY"}
    for snapshot in snapshots:
        if snapshot.reference_price is None or snapshot.market_date is None:
            continue
        matured_targets = [
            snapshot.market_date + timedelta(days=days)
            for days in horizons
            if snapshot.market_date + timedelta(days=days) <= today
        ]
        if not matured_targets:
            continue
        symbols.add(snapshot.ticker_at_time.strip().upper())
        matured_targets.append(snapshot.market_date)
        snapshot_min = min(matured_targets)
        snapshot_max = max(matured_targets)
        min_needed_date = snapshot_min if min_needed_date is None else min(min_needed_date, snapshot_min)
        max_needed_date = snapshot_max if max_needed_date is None else max(max_needed_date, snapshot_max)

    if min_needed_date is None or max_needed_date is None:
        return {}

    max_lookup_date = max_needed_date + timedelta(days=7)
    rows = db.execute(
        select(PriceCache)
        .where(
            PriceCache.symbol.in_(symbols),
            PriceCache.date >= min_needed_date.isoformat(),
            PriceCache.date <= max_lookup_date.isoformat(),
        )
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).scalars().all()
    rows_by_symbol: PriceRowsBySymbol = {}
    for row in rows:
        rows_by_symbol.setdefault(str(row.symbol).strip().upper(), []).append(row)
    return rows_by_symbol


def _price_return_pct(start_price: float | None, end_price: float | None) -> float | None:
    if start_price is None or end_price is None or start_price == 0:
        return None
    return round(((end_price - start_price) / start_price) * 100, 2)


def _directional_return_pct(direction: str, raw_return_pct: float | None) -> float | None:
    if raw_return_pct is None:
        return None
    normalized_direction = (direction or "").lower()
    if "bull" in normalized_direction:
        return raw_return_pct
    if "bear" in normalized_direction:
        return round(-raw_return_pct, 2)
    return None


def _directional_side(direction: str | None) -> str | None:
    normalized_direction = (direction or "").strip().lower()
    if "bull" in normalized_direction:
        return "bullish"
    if "bear" in normalized_direction:
        return "bearish"
    return None


def _is_directional_snapshot(snapshot: ConfirmationScoreSnapshot) -> bool:
    return _directional_side(snapshot.direction) in DIRECTIONAL_OUTCOME_SIDES


def _directionally_correct(direction: str, raw_return_pct: float | None) -> bool | None:
    directional_return = _directional_return_pct(direction, raw_return_pct)
    if directional_return is None:
        return None
    return directional_return > 0


def _score_band_for_score(score: int | None) -> str:
    value = int(score or 0)
    if value >= 80:
        return "80+"
    if value >= 75:
        return "75-79"
    if value >= 70:
        return "70-74"
    if value >= 65:
        return "65-69"
    if value >= 60:
        return "60-64"
    if value >= 40:
        return "40-59"
    return "0-39"


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _matches_summary_direction(snapshot: dict[str, Any], direction: str | None) -> bool:
    if not direction or direction == "All":
        return True
    return str(snapshot.get("direction") or "").strip().lower() == direction.strip().lower()


def _matches_summary_score_band(snapshot: dict[str, Any], score_band: str | None) -> bool:
    if not score_band or score_band == "All Scores":
        return True
    return _score_band_for_score(snapshot.get("score")) == score_band


def _matured_summary_outcome(snapshot: dict[str, Any], horizon: str) -> dict[str, Any] | None:
    outcomes = snapshot.get("outcomes")
    outcome = outcomes.get(horizon) if isinstance(outcomes, dict) else None
    if not isinstance(outcome, dict):
        return None
    return outcome if outcome.get("status") == "matured" and isinstance(outcome.get("return_pct"), (int, float)) else None


def _snapshot_outcomes(
    db: Session,
    snapshot: ConfirmationScoreSnapshot,
    *,
    price_rows_by_symbol: PriceRowsBySymbol | None = None,
    closed_at: date | None = None,
) -> dict[str, Any]:
    outcomes: dict[str, Any] = {}
    if not _is_directional_snapshot(snapshot):
        return {
            f"{days}D": {"status": "not_directional", "horizon_days": days}
            for days in OUTCOME_HORIZONS
        }

    if snapshot.reference_price is None or snapshot.market_date is None:
        return {
            f"{days}D": {"status": "missing_reference_price", "horizon_days": days}
            for days in OUTCOME_HORIZONS
        }

    today = datetime.now(timezone.utc).date()
    if snapshot.market_date + timedelta(days=min(OUTCOME_HORIZONS)) > today:
        return {
            f"{days}D": {
                "status": "pending",
                "horizon_days": days,
                "target_date": (snapshot.market_date + timedelta(days=days)).isoformat(),
            }
            for days in OUTCOME_HORIZONS
        }

    symbol = snapshot.ticker_at_time.upper()
    price_lookup = (
        (lambda lookup_symbol, lookup_date: _price_on_or_after_from_rows(price_rows_by_symbol, lookup_symbol, lookup_date))
        if price_rows_by_symbol is not None
        else (lambda lookup_symbol, lookup_date: _price_on_or_after(db, lookup_symbol, lookup_date))
    )
    spy_entry = price_lookup("SPY", snapshot.market_date)
    for days in OUTCOME_HORIZONS:
        label = f"{days}D"
        target_date = snapshot.market_date + timedelta(days=days)
        if closed_at is not None and closed_at <= target_date:
            outcomes[label] = {
                "status": "closed",
                "horizon_days": days,
                "target_date": target_date.isoformat(),
                "closed_at": closed_at.isoformat(),
            }
            continue
        if target_date > today:
            outcomes[label] = {
                "status": "pending",
                "horizon_days": days,
                "target_date": target_date.isoformat(),
            }
            continue

        price_row = price_lookup(symbol, target_date)
        if price_row is None:
            outcomes[label] = {
                "status": "missing_price",
                "horizon_days": days,
                "target_date": target_date.isoformat(),
            }
            continue

        raw_return_pct = _price_return_pct(snapshot.reference_price, price_row.close)
        spy_return_pct = None
        spy_target = price_lookup("SPY", target_date)
        if spy_entry is not None and spy_target is not None:
            spy_return_pct = _price_return_pct(spy_entry.close, spy_target.close)
        outcomes[label] = {
            "status": "matured",
            "horizon_days": days,
            "target_date": target_date.isoformat(),
            "price": price_row.close,
            "price_date": str(price_row.date)[:10],
            "return_pct": raw_return_pct,
            "directional_return_pct": _directional_return_pct(snapshot.direction, raw_return_pct),
            "directionally_correct": _directionally_correct(snapshot.direction, raw_return_pct),
            "spy_return_pct": spy_return_pct,
            "excess_return_pct": round(raw_return_pct - spy_return_pct, 2)
            if raw_return_pct is not None and spy_return_pct is not None
            else None,
            "directional_excess_return_pct": _directional_return_pct(
                snapshot.direction,
                round(raw_return_pct - spy_return_pct, 2)
                if raw_return_pct is not None and spy_return_pct is not None
                else None,
            ),
        }
    return outcomes


def _snapshot_row(
    db: Session,
    snapshot: ConfirmationScoreSnapshot,
    *,
    include_internal: bool = False,
    price_rows_by_symbol: PriceRowsBySymbol | None = None,
    closed_at: date | None = None,
) -> dict[str, Any]:
    row = {
        "id": snapshot.id,
        "ticker": snapshot.ticker_at_time,
        "calculated_at": snapshot.calculated_at.isoformat() if snapshot.calculated_at else None,
        "market_date": snapshot.market_date.isoformat() if snapshot.market_date else None,
        "score": snapshot.score,
        "direction": snapshot.direction,
        "strength": snapshot.strength,
        "reference_price": snapshot.reference_price,
        "reference_price_at": snapshot.reference_price_at.isoformat() if snapshot.reference_price_at else None,
        "reference_price_source": snapshot.reference_price_source,
        "active_source_count": snapshot.active_source_count,
        "active_sources": _json_loads(snapshot.active_sources_json, []),
        "methodology": None,
        "outcomes": _snapshot_outcomes(db, snapshot, price_rows_by_symbol=price_rows_by_symbol, closed_at=closed_at),
        "lifecycle_status": "closed" if closed_at is not None else "open",
        "closed_at": closed_at.isoformat() if closed_at is not None else None,
        "calculation_type": snapshot.calculation_type,
        "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
    }
    if include_internal:
        row.update(
            {
                "security_id": snapshot.security_id,
                "input_hash": snapshot.input_hash,
                "methodology_version_id": snapshot.methodology_version_id,
                "code_commit_sha": snapshot.code_commit_sha,
                "source_contributions": _json_loads(snapshot.source_contributions_json, {}),
                "source_freshness": _json_loads(snapshot.source_freshness_json, {}),
                "supersedes_snapshot_id": snapshot.supersedes_snapshot_id,
                "correction_reason": snapshot.correction_reason,
            }
        )
    return row


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


def _apply_snapshot_filters(query, *, ticker: str | None = None, methodology: str | None = None, calculation_type: str | None = None, start_date: date | None = None, end_date: date | None = None):
    if ticker:
        query = query.where(func.upper(ConfirmationScoreSnapshot.ticker_at_time) == ticker.strip().upper())
    if calculation_type:
        query = query.where(ConfirmationScoreSnapshot.calculation_type == calculation_type.strip())
    if start_date:
        query = query.where(ConfirmationScoreSnapshot.market_date >= start_date)
    if end_date:
        query = query.where(ConfirmationScoreSnapshot.market_date <= end_date)
    if methodology:
        query = query.join(
            ConfirmationMethodologyVersion,
            ConfirmationMethodologyVersion.id == ConfirmationScoreSnapshot.methodology_version_id,
        ).where(ConfirmationMethodologyVersion.version == methodology.strip())
    return query


def _visible_snapshot_key(snapshot: ConfirmationScoreSnapshot) -> tuple[str, int, int, date]:
    return (
        snapshot.calculation_type,
        snapshot.security_id,
        snapshot.methodology_version_id,
        snapshot.market_date,
    )


def _replacement_date(snapshot: ConfirmationScoreSnapshot, rows: list[ConfirmationScoreSnapshot]) -> date | None:
    snapshot_time = snapshot.calculated_at or datetime.combine(snapshot.market_date, time.min, tzinfo=timezone.utc)
    for row in rows:
        if row.id == snapshot.id:
            continue
        if row.calculation_type != snapshot.calculation_type:
            continue
        if row.security_id != snapshot.security_id or row.methodology_version_id != snapshot.methodology_version_id:
            continue
        row_time = row.calculated_at or datetime.combine(row.market_date, time.min, tzinfo=timezone.utc)
        if row_time > snapshot_time:
            return row.market_date
        if row_time == snapshot_time and row.id > snapshot.id:
            return row.market_date
    return None


def _snapshot_event_time(snapshot: ConfirmationScoreSnapshot) -> tuple[datetime, int]:
    snapshot_time = snapshot.calculated_at or datetime.combine(snapshot.market_date, time.min, tzinfo=timezone.utc)
    if snapshot_time.tzinfo is None:
        snapshot_time = snapshot_time.replace(tzinfo=timezone.utc)
    return snapshot_time, int(snapshot.id or 0)


def _directional_event_group_key(snapshot: ConfirmationScoreSnapshot) -> tuple[str, int, int]:
    return (
        snapshot.calculation_type,
        snapshot.security_id,
        snapshot.methodology_version_id,
    )


def _project_directional_outcome_events(rows: list[ConfirmationScoreSnapshot]) -> list[DirectionalOutcomeEvent]:
    grouped: dict[tuple[str, int, int], list[ConfirmationScoreSnapshot]] = {}
    for row in rows:
        grouped.setdefault(_directional_event_group_key(row), []).append(row)

    events: list[DirectionalOutcomeEvent] = []
    for group_rows in grouped.values():
        latest_directional_by_day: dict[date, ConfirmationScoreSnapshot] = {}
        for row in group_rows:
            if not _is_directional_snapshot(row):
                # Mixed/neutral are watch states. They do not open, grade, or close a directional event.
                continue
            current = latest_directional_by_day.get(row.market_date)
            if current is None or _snapshot_event_time(row) > _snapshot_event_time(current):
                latest_directional_by_day[row.market_date] = row

        daily_rows = sorted(latest_directional_by_day.values(), key=_snapshot_event_time)
        for index, row in enumerate(daily_rows):
            side = _directional_side(row.direction)
            closed_at = None
            for later in daily_rows[index + 1 :]:
                later_side = _directional_side(later.direction)
                if later_side is not None and later_side != side:
                    closed_at = later.market_date
                    break
            events.append(DirectionalOutcomeEvent(snapshot=row, closed_at=closed_at))
    return events


def _event_display_sort_key(event: DirectionalOutcomeEvent) -> tuple[int, datetime, int]:
    thirty_day_matured_cutoff = datetime.now(timezone.utc).date() - timedelta(days=30)
    is_30d_matured = int(event.snapshot.market_date <= thirty_day_matured_cutoff)
    event_time, event_id = _snapshot_event_time(event.snapshot)
    return is_30d_matured, event_time, event_id


def list_outcome_snapshots(
    db: Session,
    *,
    page: int = 0,
    limit: int = 25,
    ticker: str | None = None,
    methodology: str | None = None,
    calculation_type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    include_internal: bool = False,
) -> dict[str, Any]:
    bounded_page = max(0, int(page or 0))
    bounded_limit = max(1, min(int(limit or 25), 5000))
    base = _apply_snapshot_filters(
        select(ConfirmationScoreSnapshot),
        ticker=ticker,
        methodology=methodology,
        calculation_type=calculation_type,
        start_date=start_date,
        end_date=end_date,
    )
    ordered_rows = db.execute(
        base.order_by(
            ConfirmationScoreSnapshot.calculated_at.desc(),
            ConfirmationScoreSnapshot.id.desc(),
        )
    ).scalars().all()
    events = sorted(_project_directional_outcome_events(ordered_rows), key=_event_display_sort_key, reverse=True)
    total = len(events)
    paged_events = events[bounded_page * bounded_limit : (bounded_page + 1) * bounded_limit]
    rows = [event.snapshot for event in paged_events]
    methodology_by_id = {
        row.id: row.version
        for row in db.execute(select(ConfirmationMethodologyVersion)).scalars().all()
    }
    price_rows_by_symbol = _prefetch_outcome_price_rows(db, rows)
    items = []
    for event in paged_events:
        snapshot = event.snapshot
        item = _snapshot_row(
            db,
            snapshot,
            include_internal=include_internal,
            price_rows_by_symbol=price_rows_by_symbol,
            closed_at=event.closed_at,
        )
        item["methodology"] = methodology_by_id.get(snapshot.methodology_version_id)
        items.append(item)
    return {
        "items": items,
        "page": bounded_page,
        "limit": bounded_limit,
        "total": total,
        "has_next": (bounded_page + 1) * bounded_limit < total,
    }


def outcome_ledger_summary(
    db: Session,
    *,
    horizon: str = "7D",
    direction: str | None = None,
    score_band: str | None = None,
    methodology: str | None = None,
    calculation_type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    selected_horizon = horizon if horizon in {f"{days}D" for days in OUTCOME_HORIZONS} else "7D"
    selected_horizon_days = int(selected_horizon[:-1])
    base = _apply_snapshot_filters(
        select(ConfirmationScoreSnapshot),
        methodology=methodology,
        calculation_type=calculation_type,
        start_date=start_date,
        end_date=end_date,
    )
    ordered_rows = db.execute(
        base.order_by(
            ConfirmationScoreSnapshot.calculated_at.desc(),
            ConfirmationScoreSnapshot.id.desc(),
        )
    ).scalars().all()
    events = _project_directional_outcome_events(ordered_rows)
    canonical_rows = [event.snapshot for event in events]
    price_rows_by_symbol = _prefetch_outcome_price_rows(db, canonical_rows, horizons=(selected_horizon_days,))

    rows: list[dict[str, Any]] = []
    for event in events:
        snapshot = event.snapshot
        row = _snapshot_row(
            db,
            snapshot,
            include_internal=False,
            price_rows_by_symbol=price_rows_by_symbol,
            closed_at=event.closed_at,
        )
        if not _matches_summary_direction(row, direction):
            continue
        if not _matches_summary_score_band(row, score_band):
            continue
        rows.append(row)

    matured_for_horizon = [
        outcome
        for row in rows
        if (outcome := _matured_summary_outcome(row, selected_horizon)) is not None
    ]
    directional_for_horizon = [
        outcome
        for outcome in matured_for_horizon
        if isinstance(outcome.get("directionally_correct"), bool)
    ]
    directional_returns = [
        float(outcome["directional_return_pct"])
        for outcome in matured_for_horizon
        if isinstance(outcome.get("directional_return_pct"), (int, float))
    ]
    benchmarked = [
        outcome
        for outcome in matured_for_horizon
        if isinstance(outcome.get("directional_return_pct"), (int, float))
        and isinstance(outcome.get("spy_return_pct"), (int, float))
    ]
    average_directional_return = _average(directional_returns)
    average_benchmarked_directional_return = _average([float(outcome["directional_return_pct"]) for outcome in benchmarked])
    average_spy_return = _average([float(outcome["spy_return_pct"]) for outcome in benchmarked])
    accuracy = (
        round((sum(1 for outcome in directional_for_horizon if outcome.get("directionally_correct") is True) / len(directional_for_horizon)) * 100)
        if directional_for_horizon
        else None
    )
    matured_horizon_count = 0
    for row in rows:
        outcomes = row.get("outcomes")
        if not isinstance(outcomes, dict):
            continue
        matured_horizon_count += sum(
            1
            for outcome in outcomes.values()
            if isinstance(outcome, dict) and outcome.get("status") == "matured" and isinstance(outcome.get("return_pct"), (int, float))
        )

    score_band_rows = []
    for band in OUTCOME_SCORE_BANDS:
        band_rows = [row for row in rows if _score_band_for_score(row.get("score")) == band]
        band_outcomes = [
            outcome
            for row in band_rows
            if (outcome := _matured_summary_outcome(row, selected_horizon)) is not None
        ]
        directional_outcomes = [
            outcome
            for outcome in band_outcomes
            if isinstance(outcome.get("directionally_correct"), bool)
        ]
        band_accuracy = (
            round((sum(1 for outcome in directional_outcomes if outcome.get("directionally_correct") is True) / len(directional_outcomes)) * 100)
            if directional_outcomes
            else None
        )
        score_band_rows.append({"band": band, "accuracy": band_accuracy, "count": len(directional_outcomes)})

    return {
        "horizon": selected_horizon,
        "completed_events": len(matured_for_horizon),
        "directional_sample_count": len(directional_for_horizon),
        "accuracy": accuracy,
        "average_directional_return": round(average_directional_return, 2) if average_directional_return is not None else None,
        "average_spy_return": round(average_spy_return, 2) if average_spy_return is not None else None,
        "average_directional_excess_return": round(average_benchmarked_directional_return - average_spy_return, 2)
        if average_benchmarked_directional_return is not None and average_spy_return is not None
        else None,
        "benchmarked_events": len(benchmarked),
        "matured_horizon_count": matured_horizon_count,
        "score_bands": score_band_rows,
    }


def warm_public_outcome_ledger_cache(db: Session, *, snapshot_limit: int = 250) -> dict[str, Any]:
    if not outcome_ledger_enabled(db):
        return {"status": "skipped", "reason": "outcome_ledger_disabled", "warmed": 0}

    started_at = datetime.now(timezone.utc)
    status_key = public_outcome_ledger_cache_key("status")
    store_public_outcome_ledger_payload(db, status_key, outcome_ledger_status(db))
    warmed = 1

    raw_horizons = os.getenv("OUTCOME_LEDGER_CACHE_WARM_HORIZONS", "7D,30D")
    warm_horizons = [
        item.strip().upper()
        for item in raw_horizons.split(",")
        if item.strip().upper() in {f"{days}D" for days in OUTCOME_HORIZONS}
    ] or ["7D", "30D"]
    for horizon in warm_horizons:
        params = {
            "calculation_type": None,
            "direction": None,
            "end_date": None,
            "horizon": horizon,
            "methodology": None,
            "score_band": None,
            "start_date": None,
        }
        store_public_outcome_ledger_payload(
            db,
            public_outcome_ledger_cache_key("summary", params),
            outcome_ledger_summary(db, horizon=horizon),
        )
        warmed += 1

    snapshot_params = {
        "end_date": None,
        "calculation_type": None,
        "limit": snapshot_limit,
        "methodology": None,
        "page": 0,
        "start_date": None,
        "ticker": None,
    }
    store_public_outcome_ledger_payload(
        db,
        public_outcome_ledger_cache_key("snapshots", snapshot_params),
        list_outcome_snapshots(db, page=0, limit=snapshot_limit, include_internal=False),
    )
    warmed += 1

    return {
        "status": "ok",
        "warmed": warmed,
        "horizons": warm_horizons,
        "snapshot_limit": snapshot_limit,
        "duration_ms": round((datetime.now(timezone.utc) - started_at).total_seconds() * 1000, 1),
    }


def outcome_ledger_status(db: Session, *, include_admin: bool = False) -> dict[str, Any]:
    methodology = current_confirmation_methodology(db)
    total_live = db.execute(
        select(func.count()).select_from(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.calculation_type == "live")
    ).scalar() or 0
    unique_securities = db.execute(
        select(func.count(func.distinct(ConfirmationScoreSnapshot.security_id))).where(ConfirmationScoreSnapshot.calculation_type == "live")
    ).scalar() or 0
    first_snapshot = db.execute(
        select(func.min(ConfirmationScoreSnapshot.calculated_at)).where(ConfirmationScoreSnapshot.calculation_type == "live")
    ).scalar()
    latest_snapshot = db.execute(
        select(func.max(ConfirmationScoreSnapshot.calculated_at)).where(ConfirmationScoreSnapshot.calculation_type == "live")
    ).scalar()
    last_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    past_24h = db.execute(
        select(func.count()).select_from(ConfirmationScoreSnapshot).where(
            ConfirmationScoreSnapshot.calculation_type == "live",
            ConfirmationScoreSnapshot.created_at >= last_24h,
        )
    ).scalar() or 0
    missing_prices = db.execute(
        select(func.count()).select_from(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.reference_price.is_(None))
    ).scalar() or 0
    missing_source_payloads = db.execute(
        select(func.count()).select_from(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.source_contributions_json == "{}")
    ).scalar() or 0
    data_quality_status = "ok"
    if missing_prices or missing_source_payloads or _counter_value(db, OUTCOMES_LEDGER_ERRORS_KEY):
        data_quality_status = "review"
    status = {
        "enabled": outcome_ledger_enabled(db),
        "tracking_status": "live" if outcome_ledger_enabled(db) else "disabled",
        "current_methodology_version": methodology.version,
        "first_live_snapshot_date": first_snapshot.isoformat() if first_snapshot else None,
        "most_recent_snapshot_timestamp": latest_snapshot.isoformat() if latest_snapshot else None,
        "unique_securities_captured": unique_securities,
        "total_live_snapshots": total_live,
        "data_quality_status": data_quality_status,
    }
    if include_admin:
        status.update(
            {
                "snapshots_created_past_24h": past_24h,
                "duplicate_attempts_ignored": _counter_value(db, OUTCOMES_LEDGER_DUPLICATES_KEY),
                "persistence_errors": _counter_value(db, OUTCOMES_LEDGER_ERRORS_KEY),
                "missing_reference_prices": missing_prices + _counter_value(db, OUTCOMES_LEDGER_MISSING_PRICE_KEY),
                "stale_reference_prices": _counter_value(db, OUTCOMES_LEDGER_STALE_REFERENCE_PRICE_KEY),
                "missing_security_ids": _counter_value(db, OUTCOMES_LEDGER_MISSING_SECURITY_KEY),
                "missing_source_contribution_payloads": missing_source_payloads + _counter_value(db, OUTCOMES_LEDGER_MISSING_SOURCE_PAYLOAD_KEY),
                "methodology": {
                    "id": methodology.id,
                    "version": methodology.version,
                    "description": methodology.description,
                    "configuration": _json_loads(methodology.configuration_json, {}),
                    "code_commit_sha": methodology.code_commit_sha,
                    "deployed_at": methodology.deployed_at.isoformat() if methodology.deployed_at else None,
                    "retired_at": methodology.retired_at.isoformat() if methodology.retired_at else None,
                    "is_current": methodology.is_current,
                },
            }
        )
    return status


def get_outcome_snapshot_detail(db: Session, snapshot_id: int) -> dict[str, Any]:
    snapshot = db.get(ConfirmationScoreSnapshot, snapshot_id)
    if snapshot is None:
        return {}
    item = _snapshot_row(db, snapshot, include_internal=True)
    methodology = db.get(ConfirmationMethodologyVersion, snapshot.methodology_version_id)
    item["methodology"] = methodology.version if methodology else None
    return item
