# backend/app/ingest_house.py
from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any, Optional

import requests
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Filing, Member, Security, Transaction

FMP_BASE = "https://financialmodelingprep.com/stable/house-trades"
DEFAULT_LIMIT = 100
DEFAULT_PAGES = 3  # keep small for MVP; bump later
PROGRESS_EVERY_PAGES = 10


def _get_api_key() -> str:
    # read at runtime so setting $env:FMP_API_KEY later still works
    return os.getenv("FMP_API_KEY", "").strip()


def _parse_date(value: Any) -> Optional[date]:
    """Accepts 'YYYY-MM-DD', ISO datetime, or None. Returns a date or None."""
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        v = value.strip()
        try:
            return datetime.strptime(v[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _amount_to_range(amount: Any) -> tuple[Optional[float], Optional[float]]:
    """Parse "$1,001 - $15,000" or numeric values into (min,max)."""
    if amount is None:
        return None, None
    if isinstance(amount, (int, float)):
        v = float(amount)
        return v, v
    if isinstance(amount, str):
        s = amount.replace(",", "").replace("$", "").strip()
        if "-" in s:
            parts = [p.strip() for p in s.split("-", 1)]
            try:
                lo = float(parts[0]) if parts[0] else None
            except Exception:
                lo = None
            try:
                hi = float(parts[1]) if parts[1] else None
            except Exception:
                hi = None
            return lo, hi
        try:
            v = float(s)
            return v, v
        except Exception:
            return None, None
    return None, None


def _fetch_page(page: int, limit: int) -> list[dict[str, Any]]:
    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError(
            "Missing FMP_API_KEY. Set it locally: $env:FMP_API_KEY='...' "
            "and on Fly: fly secrets set FMP_API_KEY='...'"
        )

    params = {"page": page, "limit": limit, "apikey": api_key}
    r = requests.get(FMP_BASE, params=params, timeout=30)
    if r.status_code in {400, 404}:
        # FMP can return out-of-range responses for pagination termination.
        return []
    if r.status_code in {401, 403}:
        raise RuntimeError(f"House ingest authorization failed ({r.status_code}): {r.text}")
    r.raise_for_status()
    data = r.json()

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    return []


def _member_key_and_fields(row: dict[str, Any]) -> tuple[str, Optional[str], Optional[str], str, Optional[str]]:
    """
    For FMP stable/house-trades, we get:
      firstName, lastName, office, district (e.g. IL01)

    We use district as stable member key:
      FMP_HOUSE_IL01

    Returns: (member_key, first_name, last_name, chamber, state)
    """
    first_name = _safe_str(row.get("firstName") or row.get("first_name"))
    last_name = _safe_str(row.get("lastName") or row.get("last_name"))

    office = _safe_str(row.get("office"))  # often full name
    district = _safe_str(row.get("district"))  # e.g. "IL01"

    if district:
        member_key = f"FMP_HOUSE_{district.upper()}"
    else:
        # fallback: still deterministic
        base = office or f"{first_name or ''} {last_name or ''}".strip() or "UNKNOWN"
        member_key = f"FMP_HOUSE_{base.upper().replace(' ', '_')}"

    chamber = "house"

    # Fill state reliably: IL01 -> IL
    state = _safe_str(row.get("state"))
    if not state and district and len(district) >= 2:
        state = district[:2].upper()

    return member_key, first_name, last_name, chamber, state


def ingest_house(pages: int = DEFAULT_PAGES, limit: int = DEFAULT_LIMIT, sleep_s: float = 0.25) -> dict[str, Any]:
    inserted = 0
    skipped = 0
    pages_processed = 0

    db = SessionLocal()
    try:
        for page in range(pages):
            rows = _fetch_page(page=page, limit=limit)
            if not rows:
                break

            pages_processed += 1

            for row in rows:
                # -------------------
                # Member upsert
                # -------------------
                member_key, first_name, last_name, chamber, state = _member_key_and_fields(row)

                member = db.execute(select(Member).where(Member.bioguide_id == member_key)).scalar_one_or_none()
                if member is None:
                    member = Member(
                        bioguide_id=member_key,
                        first_name=first_name,
                        last_name=last_name,
                        chamber=chamber,
                        party=None,  # FMP endpoint doesn't provide party
                        state=state,
                    )
                    db.add(member)
                    db.flush()
                else:
                    # Backfill missing
                    member.first_name = member.first_name or first_name
                    member.last_name = member.last_name or last_name
                    member.chamber = member.chamber or chamber
                    member.state = member.state or state

                member_id = member.id

                # -------------------
                # Security upsert
                # -------------------
                symbol = _safe_str(row.get("symbol") or row.get("ticker"))
                asset_name = _safe_str(row.get("assetDescription") or row.get("asset") or row.get("company"))
                asset_class = _safe_str(row.get("assetType") or row.get("asset_class") or "stock") or "stock"
                sector = _safe_str(row.get("sector"))

                security_id = None
                if symbol:
                    sec = db.execute(select(Security).where(Security.symbol == symbol)).scalar_one_or_none()
                    if sec is None:
                        sec = Security(
                            symbol=symbol,
                            name=asset_name or symbol,
                            asset_class=asset_class,
                            sector=sector,
                        )
                        db.add(sec)
                        db.flush()
                    else:
                        sec.name = sec.name or (asset_name or symbol)
                        sec.asset_class = sec.asset_class or asset_class
                        sec.sector = sec.sector or sector
                    security_id = sec.id

                # -------------------
                # Filing idempotency
                # -------------------
                filing_date = _parse_date(row.get("disclosureDate") or row.get("reportDate") or row.get("filingDate"))
                doc_url = _safe_str(row.get("link") or row.get("pdf") or row.get("documentUrl") or row.get("document_url"))

                tx_date_key = _safe_str(row.get("transactionDate") or row.get("tradeDate")) or ""
                tx_type_key = _safe_str(row.get("type") or row.get("transactionType")) or ""
                amount_key = _safe_str(row.get("amount") or row.get("amountRange")) or ""

                # Stable composite key so reruns don't duplicate
                filing_key = f"{member_key}|{filing_date}|{symbol}|{tx_date_key}|{tx_type_key}|{amount_key}|{doc_url or ''}"

                existing = db.execute(
                    select(Filing).where(Filing.document_hash == f"fmp:{filing_key}")
                ).scalar_one_or_none()
                if existing:
                    skipped += 1
                    continue

                filing = Filing(
                    member_id=member_id,
                    source="house_fmp",
                    filing_date=filing_date,
                    document_url=doc_url,
                    document_hash=f"fmp:{filing_key}",
                )
                db.add(filing)
                db.flush()

                # -------------------
                # Transaction insert
                # -------------------
                tx_type = (_safe_str(row.get("type") or row.get("transactionType")) or "unknown").lower()
                owner_type = (_safe_str(row.get("owner") or row.get("ownerType")) or "self").lower()

                trade_date = _parse_date(row.get("transactionDate") or row.get("tradeDate"))
                report_date = filing_date

                lo, hi = _amount_to_range(row.get("amount") or row.get("amountRange"))
                desc = _safe_str(row.get("comment") or row.get("description"))

                tx = Transaction(
                    filing_id=filing.id,
                    member_id=member_id,
                    security_id=security_id,
                    owner_type=owner_type,
                    transaction_type=tx_type,
                    trade_date=trade_date,
                    report_date=report_date,
                    amount_range_min=lo,
                    amount_range_max=hi,
                    description=desc,
                )
                db.add(tx)
                inserted += 1

            db.commit()
            if pages_processed % PROGRESS_EVERY_PAGES == 0:
                print(
                    f"[house] progress pages={pages_processed} inserted={inserted} skipped={skipped}",
                    flush=True,
                )
            time.sleep(sleep_s)

        return {
            "status": "ok",
            "inserted": inserted,
            "skipped": skipped,
            "pages_processed": pages_processed,
        }

    finally:
        db.close()


if __name__ == "__main__":
    # Allow overrides for backfills
    pages = int(os.getenv("INGEST_PAGES", str(DEFAULT_PAGES)))
    limit = int(os.getenv("INGEST_LIMIT", str(DEFAULT_LIMIT)))
    print(ingest_house(pages=pages, limit=limit))
