from __future__ import annotations

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

    def as_dict(self) -> dict[str, str | bool | None]:
        return {
            "symbol": self.symbol,
            "has_government_exposure": self.has_government_exposure,
            "contract_exposure_level": self.contract_exposure_level,
            "recent_award_activity": self.recent_award_activity,
            "summary_label": self.summary_label,
            "source_context": self.source_context,
            "confidence": self.confidence,
            "as_of": self.as_of,
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
    )


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

    has_exposure = bool(row.has_government_exposure)
    recent_award_activity = row.recent_award_activity

    summary_label = (row.summary_label or "").strip()
    if not summary_label:
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
    )
