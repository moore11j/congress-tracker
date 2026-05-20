from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import func, select  # noqa: E402

from app.db import SessionLocal  # noqa: E402
from app.models import Event, Security  # noqa: E402
from app.services.congress_assets import (  # noqa: E402
    CANONICAL_OTHER_BUCKET,
    CONGRESS_EQUITY_EVENT_TYPE,
    canonical_asset_bucket,
)


def _payload(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _text(*values: object | None) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _clean_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected YYYY-MM-DD") from exc


def run(*, since_report_date: date | None = None, limit: int = 500) -> dict[str, int]:
    db = SessionLocal()
    try:
        securities = db.execute(select(Security.symbol, Security.name, Security.asset_class).where(Security.symbol.is_not(None))).all()
        exact_by_name = {
            _clean_key(name): {"symbol": str(symbol).strip().upper(), "asset_class": asset_class}
            for symbol, name, asset_class in securities
            if symbol and name and _clean_key(name)
        }

        query = (
            select(Event)
            .where(Event.event_type == CONGRESS_EQUITY_EVENT_TYPE)
            .where(Event.symbol.is_(None))
            .order_by(func.coalesce(Event.event_date, Event.ts).desc(), Event.id.desc())
            .limit(limit)
        )
        rows = db.execute(query).scalars().all()
        reported = 0
        repair = 0
        keep = 0
        for event in rows:
            payload = _payload(event.payload_json)
            report_date = _text(payload.get("report_date"), payload.get("filing_date"))
            if since_report_date and (not report_date or date.fromisoformat(report_date[:10]) < since_report_date):
                continue
            raw_description = _text(
                payload.get("security_description"),
                payload.get("securityDescription"),
                payload.get("description"),
                payload.get("security_name"),
                payload.get("securityName"),
                payload.get("company_name"),
                payload.get("companyName"),
            )
            bucket = canonical_asset_bucket(
                event_type=event.event_type,
                asset_class=_text(payload.get("asset_class"), payload.get("assetClass")),
                instrument_type=_text(payload.get("instrument_type"), payload.get("instrumentType")),
                symbol=event.symbol,
                security_description=raw_description,
                company_name=_text(payload.get("company_name"), payload.get("companyName")),
            )
            if bucket != CANONICAL_OTHER_BUCKET:
                continue
            candidate = exact_by_name.get(_clean_key(raw_description))
            action = "repair" if candidate else "keep_unresolved"
            repair += 1 if candidate else 0
            keep += 0 if candidate else 1
            reported += 1
            print(
                json.dumps(
                    {
                        "event_id": event.id,
                        "raw_description": raw_description,
                        "member": event.member_name,
                        "report_date": report_date,
                        "candidate_ticker": candidate["symbol"] if candidate else None,
                        "action": action,
                    },
                    sort_keys=True,
                )
            )
        return {"reported": reported, "repair": repair, "keep_unresolved": keep}
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run report for unresolved Other Congress events.")
    parser.add_argument("--since-report-date", type=_parse_date)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    print(json.dumps({"summary": run(since_report_date=args.since_report_date, limit=args.limit)}, sort_keys=True))


if __name__ == "__main__":
    main()
