# backend/app/ingest_senate.py
from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any, Optional

import requests
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Filing, Member, Security, Transaction


FMP_BASE = "https://financialmodelingprep.com/stable/senate-latest"

DEFAULT_LIMIT = 100
DEFAULT_PAGES = 3  # keep it small for MVP; increase later
PROGRESS_EVERY_PAGES = 10


def _parse_date(value: Any) -> Optional[date]:
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


def _guess_party(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    r = raw.strip().upper()
    if r in {"D", "R", "I"}:
        return r
    if "DEMO" in r:
        return "D"
    if "REPU" in r or "GOP" in r:
        return "R"
    if "INDEP" in r:
        return "I"
    return None


def _amount_to_range(amount: Any) -> tuple[Optional[float], Optional[float]]:
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


def _get_api_key() -> str:
    return os.getenv("FMP_API_KEY", "").strip()


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
        raise RuntimeError(f"Senate ingest authorization failed ({r.status_code}): {r.text}")
    r.raise_for_status()
    data = r.json()

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    return []


def ingest_senate(pages: int = DEFAULT_PAGES, limit: int = DEFAULT_LIMIT, sleep_s: float = 0.25) -> dict[str, Any]:
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
                # --- Member fields ---
                first_name = _safe_str(row.get("firstName") or row.get("first_name"))
                last_name = _safe_str(row.get("lastName") or row.get("last_name"))

                full_name = _safe_str(
                    row.get("office")
                    or row.get("senator")
                    or row.get("member")
                    or row.get("name")
                )

                if (not first_name or not last_name) and full_name:
                    parts = [p for p in full_name.replace(",", " ").split() if p.strip()]
                    if parts:
                        first_name = first_name or parts[0]
                        last_name = last_name or (parts[-1] if len(parts) > 1 else None)

                state = _safe_str(row.get("state"))
                party = _guess_party(_safe_str(row.get("party")))

                # If API provides no stable ID, build a stable-ish surrogate:
                # Prefer state + name so you don't end up with FMP_UNKNOWN.
                base_name = full_name or f"{first_name or ''} {last_name or ''}".strip() or "UNKNOWN"
                member_key = _safe_str(row.get("bioguideId") or row.get("bioguide_id") or row.get("memberId") or row.get("member_id"))
                if not member_key:
                    member_key = f"FMP_SENATE_{(state or 'XX')}_{base_name.upper().replace(' ', '_')}"

                chamber = "senate"

                member = db.execute(select(Member).where(Member.bioguide_id == member_key)).scalar_one_or_none()
                if not member:
                    member = Member(
                        bioguide_id=member_key,
                        first_name=first_name,
                        last_name=last_name,
                        chamber=chamber,
                        party=party,
                        state=state,
                    )
                    db.add(member)
                    db.flush()
                else:
                    member.first_name = member.first_name or first_name
                    member.last_name = member.last_name or last_name
                    member.party = member.party or party
                    member.state = member.state or state
                    member.chamber = member.chamber or chamber

                # --- Security fields ---
                symbol = _safe_str(row.get("symbol") or row.get("ticker"))
                asset_name = _safe_str(row.get("assetDescription") or row.get("asset") or row.get("company"))
                asset_class = _safe_str(row.get("assetType") or row.get("asset_class") or "stock") or "stock"
                sector = _safe_str(row.get("sector"))

                security_id = None
                if symbol:
                    sec = db.execute(select(Security).where(Security.symbol == symbol)).scalar_one_or_none()
                    if not sec:
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

                # --- Filing fields ---
                filing_date = _parse_date(row.get("disclosureDate") or row.get("reportDate") or row.get("filingDate"))
                doc_url = _safe_str(row.get("link") or row.get("pdf") or row.get("documentUrl") or row.get("document_url"))

                # Idempotency key:
                # Prefer a provided id; else fall back to link (best unique); else name+date+symbol+type
                filing_key = _safe_str(row.get("id") or row.get("filingId") or row.get("filing_id"))
                if not filing_key and doc_url:
                    filing_key = doc_url
                if not filing_key:
                    filing_key = f"{member_key}_{filing_date}_{symbol}_{row.get('type')}"

                existing = db.execute(
                    select(Filing).where(Filing.document_hash == f"fmp_senate:{filing_key}")
                ).scalar_one_or_none()
                if existing:
                    skipped += 1
                    continue

                filing = Filing(
                    member_id=member.id,
                    source="senate_fmp",
                    filing_date=filing_date,
                    document_url=doc_url,
                    document_hash=f"fmp_senate:{filing_key}",
                )
                db.add(filing)
                db.flush()

                # --- Transaction fields ---
                tx_type = (_safe_str(row.get("type") or row.get("transactionType")) or "unknown").lower()
                owner_type = (_safe_str(row.get("owner") or row.get("ownerType")) or "self").lower()

                trade_date = _parse_date(row.get("transactionDate") or row.get("tradeDate"))
                report_date = filing_date

                amt = row.get("amount") or row.get("amountRange")
                lo, hi = _amount_to_range(amt)
                desc = _safe_str(row.get("comment") or row.get("description"))

                tx = Transaction(
                    filing_id=filing.id,
                    member_id=member.id,
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
                    f"[senate] progress pages={pages_processed} inserted={inserted} skipped={skipped}",
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
    print(ingest_senate(pages=pages, limit=limit))
