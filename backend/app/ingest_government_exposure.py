from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.clients.usaspending import USAspendingClientError, fetch_recipient_contract_spending
from app.db import Base, SessionLocal, engine
from app.models import Security, TickerGovernmentExposure

logger = logging.getLogger(__name__)

SOURCE_TAG = "usaspending_recipient_v1"
_SUFFIXES = {
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "co",
    "company",
    "ltd",
    "limited",
    "llc",
    "plc",
    "holdings",
    "holding",
    "group",
    "technologies",
    "technology",
    "systems",
    "system",
    "international",
}


@dataclass(frozen=True)
class ExposureComputation:
    symbol: str
    total_amount: float
    recent_amount: float
    award_count: int
    recent_award_count: int
    matched_recipients: list[str]
    match_confidence: str


def _normalize_company_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", (value or "").upper())
    tokens = [token for token in cleaned.split() if token and token.lower() not in _SUFFIXES]
    return " ".join(tokens)


def _amount(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    return parsed if parsed == parsed and parsed > 0 else 0.0


def _count(value: Any) -> int:
    try:
        parsed = int(value)
    except Exception:
        return 0
    return parsed if parsed > 0 else 0


def _resolve_symbol_map(db: Session) -> tuple[dict[str, str], set[str]]:
    rows = db.execute(select(Security.symbol, Security.name).where(Security.symbol.is_not(None))).all()
    exact: dict[str, str] = {}
    ambiguous: set[str] = set()

    for symbol, name in rows:
        sym = (symbol or "").strip().upper()
        if not sym:
            continue
        normalized = _normalize_company_name(name or "")
        if not normalized:
            continue
        current = exact.get(normalized)
        if current and current != sym:
            ambiguous.add(normalized)
            exact.pop(normalized, None)
            continue
        if normalized not in ambiguous:
            exact[normalized] = sym

    return exact, ambiguous


def _match_symbol(recipient_name: str, exact_map: dict[str, str], ambiguous: set[str]) -> tuple[str | None, str]:
    normalized = _normalize_company_name(recipient_name)
    if not normalized or normalized in ambiguous:
        return None, "none"

    symbol = exact_map.get(normalized)
    if symbol:
        return symbol, "high"

    return None, "none"


def _exposure_level(total_amount: float, award_count: int) -> str | None:
    if total_amount <= 0:
        return None
    if total_amount >= 5_000_000_000 or award_count >= 150:
        return "high"
    if total_amount >= 500_000_000 or award_count >= 40:
        return "moderate"
    return "limited"


def _summary_label(has_exposure: bool, recent_award_activity: bool) -> str:
    if has_exposure and recent_award_activity:
        return "Government contract exposure present · Recent award activity detected"
    if has_exposure:
        return "Government contract exposure present"
    return "No known contract exposure in current data"


def _compute_exposures(
    *,
    totals: list[dict[str, Any]],
    recents: list[dict[str, Any]],
    exact_map: dict[str, str],
    ambiguous: set[str],
) -> dict[str, ExposureComputation]:
    by_symbol: dict[str, ExposureComputation] = {}

    def _merge(rows: list[dict[str, Any]], is_recent: bool) -> None:
        for row in rows:
            recipient = str(row.get("recipient_name") or "").strip()
            if not recipient:
                continue

            symbol, confidence = _match_symbol(recipient, exact_map, ambiguous)
            if not symbol:
                continue

            amount = _amount(row.get("amount"))
            award_count = _count(row.get("award_count"))
            if amount <= 0 and award_count <= 0:
                continue

            existing = by_symbol.get(symbol)
            if existing is None:
                existing = ExposureComputation(
                    symbol=symbol,
                    total_amount=0.0,
                    recent_amount=0.0,
                    award_count=0,
                    recent_award_count=0,
                    matched_recipients=[],
                    match_confidence=confidence,
                )

            matched = existing.matched_recipients
            if recipient not in matched:
                matched = [*matched, recipient]

            by_symbol[symbol] = ExposureComputation(
                symbol=symbol,
                total_amount=existing.total_amount + (0.0 if is_recent else amount),
                recent_amount=existing.recent_amount + (amount if is_recent else 0.0),
                award_count=existing.award_count + (0 if is_recent else award_count),
                recent_award_count=existing.recent_award_count + (award_count if is_recent else 0),
                matched_recipients=matched,
                match_confidence=existing.match_confidence,
            )

    _merge(totals, False)
    _merge(recents, True)
    return by_symbol


def _upsert_exposures(db: Session, computed: dict[str, ExposureComputation]) -> dict[str, int]:
    inserted = updated = 0

    for symbol, exposure in computed.items():
        existing = db.execute(
            select(TickerGovernmentExposure).where(TickerGovernmentExposure.symbol == symbol)
        ).scalar_one_or_none()

        has_exposure = exposure.total_amount > 0 or exposure.award_count > 0
        recent_award_activity = (exposure.recent_amount > 0) or (exposure.recent_award_count > 0)
        level = _exposure_level(exposure.total_amount, exposure.award_count)
        label = _summary_label(has_exposure, recent_award_activity)

        source_context = (
            "Derived from USAspending recipient-level contract aggregates with conservative "
            "name-to-ticker exact matching. Coverage is partial and mapped only."
        )

        details = {
            "source": SOURCE_TAG,
            "match_confidence": exposure.match_confidence,
            "matched_recipients": exposure.matched_recipients,
            "totals": {
                "obligated_amount": round(exposure.total_amount, 2),
                "award_count": exposure.award_count,
            },
            "recent_window": {
                "obligated_amount": round(exposure.recent_amount, 2),
                "award_count": exposure.recent_award_count,
            },
        }

        if existing is None:
            db.add(
                TickerGovernmentExposure(
                    symbol=symbol,
                    has_government_exposure=has_exposure,
                    contract_exposure_level=level,
                    recent_award_activity=recent_award_activity,
                    summary_label=label,
                    source_context=source_context,
                    source_details_json=json.dumps(details, sort_keys=True),
                )
            )
            inserted += 1
        else:
            existing.has_government_exposure = has_exposure
            existing.contract_exposure_level = level
            existing.recent_award_activity = recent_award_activity
            existing.summary_label = label
            existing.source_context = source_context
            existing.source_details_json = json.dumps(details, sort_keys=True)
            updated += 1

    db.commit()
    return {"inserted": inserted, "updated": updated}


def _fetch_window(
    *,
    start_date: date,
    end_date: date,
    max_pages: int,
    per_page: int,
    fetcher: Callable[..., dict[str, Any]],
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        payload = fetcher(start_date=start_date, end_date=end_date, page=page, limit=per_page)
        rows = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            break
        all_rows.extend([row for row in rows if isinstance(row, dict)])
        if not payload.get("has_next"):
            break
    return all_rows


def ingest_usaspending_government_exposure(
    *,
    db: Session,
    lookback_days: int = 365,
    recent_days: int = 90,
    max_pages: int = 20,
    per_page: int = 100,
    fetcher: Callable[..., dict[str, Any]] = fetch_recipient_contract_spending,
    as_of: date | None = None,
) -> dict[str, int | str]:
    effective_as_of = as_of or datetime.now(timezone.utc).date()
    if lookback_days < 30:
        lookback_days = 30
    if recent_days < 7:
        recent_days = 7

    total_start = effective_as_of - timedelta(days=lookback_days)
    recent_start = effective_as_of - timedelta(days=recent_days)

    totals = _fetch_window(
        start_date=total_start,
        end_date=effective_as_of,
        max_pages=max_pages,
        per_page=per_page,
        fetcher=fetcher,
    )
    recents = _fetch_window(
        start_date=recent_start,
        end_date=effective_as_of,
        max_pages=max_pages,
        per_page=per_page,
        fetcher=fetcher,
    )

    exact_map, ambiguous = _resolve_symbol_map(db)
    computed = _compute_exposures(totals=totals, recents=recents, exact_map=exact_map, ambiguous=ambiguous)
    upsert_stats = _upsert_exposures(db, computed)

    return {
        "status": "ok",
        "source": SOURCE_TAG,
        "as_of": effective_as_of.isoformat(),
        "rows_total": len(totals),
        "rows_recent": len(recents),
        "symbols_mapped": len(computed),
        **upsert_stats,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest ticker government contract exposure from USAspending")
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--recent-days", type=int, default=90)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    Base.metadata.create_all(bind=engine)

    with SessionLocal() as db:
        try:
            result = ingest_usaspending_government_exposure(
                db=db,
                lookback_days=args.lookback_days,
                recent_days=args.recent_days,
                max_pages=args.max_pages,
                per_page=args.per_page,
            )
        except USAspendingClientError as exc:
            raise SystemExit(str(exc))

    logger.info("USAspending government exposure ingest complete: %s", result)


if __name__ == "__main__":
    main()
