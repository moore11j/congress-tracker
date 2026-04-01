from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import TickerGovernmentExposure

KNOWN_EXPOSURE_LEVELS = {"high", "moderate", "limited"}


@dataclass(frozen=True)
class GovernmentExposureSummary:
    symbol: str
    has_government_exposure: bool
    contract_exposure_level: str | None
    recent_award_activity: bool | None
    summary_label: str
    source_context: str
    confidence: str
    as_of: str | None
    latest_notable_award: dict[str, str | float | bool | None] | None

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "has_government_exposure": self.has_government_exposure,
            "contract_exposure_level": self.contract_exposure_level,
            "recent_award_activity": self.recent_award_activity,
            "summary_label": self.summary_label,
            "source_context": self.source_context,
            "confidence": self.confidence,
            "as_of": self.as_of,
            "latest_notable_award": self.latest_notable_award,
        }


def government_exposure_signal_boost(summary: GovernmentExposureSummary) -> float:
    """Reusable weighting hook for later signal/feed/watchlist ranking layers."""

    boost = 0.0
    if summary.has_government_exposure:
        boost += 1.5

    if summary.contract_exposure_level == "high":
        boost += 1.0
    elif summary.contract_exposure_level == "moderate":
        boost += 0.5

    if summary.recent_award_activity:
        boost += 0.5

    return boost


def _iso_date(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.date().isoformat()


def _default_summary(symbol: str) -> GovernmentExposureSummary:
    return GovernmentExposureSummary(
        symbol=symbol,
        has_government_exposure=False,
        contract_exposure_level=None,
        recent_award_activity=None,
        summary_label="No known contract exposure in current data",
        source_context="Coverage is limited to currently ingested contract/award mapping.",
        confidence="none",
        as_of=None,
        latest_notable_award=None,
    )


def _latest_award_snapshot(source_details_json: str | None) -> dict[str, str | float | bool | None] | None:
    if not source_details_json:
        return None
    try:
        payload = json.loads(source_details_json)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    latest = payload.get("latest_notable_award")
    if not isinstance(latest, dict):
        return None
    return {
        "awarding_agency": latest.get("awarding_agency"),
        "awarding_department": latest.get("awarding_department"),
        "award_amount": latest.get("award_amount"),
        "award_date": latest.get("award_date"),
        "award_description": latest.get("award_description"),
        "award_id": latest.get("award_id"),
        "contract_id": latest.get("contract_id"),
        "is_notable": latest.get("is_notable"),
    }


def get_ticker_government_exposure(db: Session, symbol: str) -> GovernmentExposureSummary:
    sym = symbol.strip().upper()
    if not sym:
        return _default_summary(symbol)

    row = db.execute(
        select(TickerGovernmentExposure).where(TickerGovernmentExposure.symbol == sym)
    ).scalar_one_or_none()
    if row is None:
        return _default_summary(sym)

    raw_level = (row.contract_exposure_level or "").strip().lower() or None
    level = raw_level if raw_level in KNOWN_EXPOSURE_LEVELS else None

    recent_award_activity = bool(row.recent_award_activity) if row.recent_award_activity is not None else None
    has_exposure = bool(row.has_government_exposure) or bool(recent_award_activity)

    summary_label = (row.summary_label or "").strip()
    if not summary_label or (has_exposure and recent_award_activity and "No known contract exposure" in summary_label):
        if has_exposure and recent_award_activity:
            summary_label = "Government contract exposure present · Recent award activity detected"
        elif has_exposure:
            summary_label = "Government contract exposure present"
        else:
            summary_label = "No known contract exposure in current data"

    source_context = (
        (row.source_context or "").strip()
        or "Coverage is limited to currently ingested contract/award mapping."
    )

    confidence = "observed" if has_exposure else "none"

    return GovernmentExposureSummary(
        symbol=sym,
        has_government_exposure=has_exposure,
        contract_exposure_level=level,
        recent_award_activity=recent_award_activity,
        summary_label=summary_label,
        source_context=source_context,
        confidence=confidence,
        as_of=_iso_date(row.updated_at),
        latest_notable_award=_latest_award_snapshot(row.source_details_json),
    )


def get_ticker_government_exposure_for_symbols(
    db: Session,
    symbols: list[str],
) -> dict[str, GovernmentExposureSummary]:
    normalized_symbols = sorted({(symbol or "").strip().upper() for symbol in symbols if symbol and symbol.strip()})
    if not normalized_symbols:
        return {}

    rows = db.execute(
        select(TickerGovernmentExposure).where(TickerGovernmentExposure.symbol.in_(normalized_symbols))
    ).scalars().all()

    summaries: dict[str, GovernmentExposureSummary] = {}
    for row in rows:
        raw_level = (row.contract_exposure_level or "").strip().lower() or None
        level = raw_level if raw_level in KNOWN_EXPOSURE_LEVELS else None

        recent_award_activity = bool(row.recent_award_activity) if row.recent_award_activity is not None else None
        has_exposure = bool(row.has_government_exposure) or bool(recent_award_activity)

        summary_label = (row.summary_label or "").strip()
        if not summary_label or (has_exposure and recent_award_activity and "No known contract exposure" in summary_label):
            if has_exposure and recent_award_activity:
                summary_label = "Government contract exposure present · Recent award activity detected"
            elif has_exposure:
                summary_label = "Government contract exposure present"
            else:
                summary_label = "No known contract exposure in current data"

        source_context = (
            (row.source_context or "").strip()
            or "Coverage is limited to currently ingested contract/award mapping."
        )
        confidence = "observed" if has_exposure else "none"

        summaries[row.symbol] = GovernmentExposureSummary(
            symbol=row.symbol,
            has_government_exposure=has_exposure,
            contract_exposure_level=level,
            recent_award_activity=recent_award_activity,
            summary_label=summary_label,
            source_context=source_context,
            confidence=confidence,
            as_of=_iso_date(row.updated_at),
            latest_notable_award=_latest_award_snapshot(row.source_details_json),
        )
    return summaries
