# backend/app/ingest_house.py
from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any, Optional

import requests
from sqlalchemy import select

from app.main import SessionLocal, Member, Security, Filing, Transaction


FMP_API_KEY = os.getenv("FMP_API_KEY", "").strip()
FMP_BASE = "https://financialmodelingprep.com/stable/house-latest"  # docs: stable/house-latest :contentReference[oaicite:3]{index=3}

DEFAULT_LIMIT = 100
DEFAULT_PAGES = 3  # keep it small for MVP; increase later


def _parse_date(value: Any) -> Optional[date]:
    """
    Accepts 'YYYY-MM-DD', ISO datetime, or None.
    Returns a date or None.
    """
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        v = value.strip()
        try:
            # YYYY-MM-DD
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
    # Sometimes APIs give full names
    if "DEMO" in r:
        return "D"
    if "REPU" in r or "GOP" in r:
        return "R"
    if "INDEP" in r:
        return "I"
    return None


def _amount_to_range(amount: Any) -> tuple[Optional[float], Optional[float]]:
    """
    Some feeds provide strings like "$1,001 - $15,000" or numeric values.
    We try to parse a min/max.
    """
    if amount is None:
        return None, None
    if isinstance(amount, (int, float)):
        # If they give a single number, store as min=max
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
        # single-ish string
        try:
            v = float(s)
            return v, v
        except Exception:
            return None, None
    return None, None


def _fetch_page(page: int, limit: int) -> list[dict[str, Any]]:
    if not FMP_API_KEY:
        raise RuntimeError(
            "Missing FMP_API_KEY. Set it locally: $env:FMP_API_KEY='...' "
            "and on Fly: fly secrets set FMP_API_KEY='...'"
        )

    params = {"page": page, "limit": limit, "apikey": FMP_API_KEY}
    r = requests.get(FMP_BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Expecting a list; if API returns an object, normalize
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    return []


def ingest_house(pages: int = DEFAULT_PAGES, limit: int = DEFAULT_LIMIT, sleep_s: float = 0.25) -> dict[str, Any]:
    inserted = 0
    skipped = 0

    db = SessionLocal()
    try:
        for page in range(pages):
            rows = _fetch_page(page=page, limit=limit)
            if not rows:
                break

            for row in rows:
                # --- Member fields (best-effort, varies by provider) ---
                rep_name = _safe_str(row.get("representative") or row.get("member") or row.get("name"))
                first_name = None
                last_name = None
                if rep_name and "," in rep_name:
                    # "Last, First"
                    parts = [p.strip() for p in rep_name.split(",", 1)]
                    last_name = parts[0] or None
                    first_name = parts[1] or None
                elif rep_name and " " in rep_name:
                    # "First Last"
                    parts = rep_name.split()
                    first_name = parts[0]
                    last_name = parts[-1] if len(parts) > 1 else None

                # If we don’t have a real bioguide_id, create a deterministic surrogate
                # (You can replace later when you add a proper member directory ingest)
                member_key = _safe_str(row.get("bioguideId") or row.get("bioguide_id"))
                if not member_key:
                    member_key = f"FMP_{(rep_name or 'UNKNOWN').upper().replace(' ', '_')}"

                chamber = (_safe_str(row.get("chamber")) or "house").lower()
                party = _guess_party(_safe_str(row.get("party")))
                state = _safe_str(row.get("state"))

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
                    # Light “upsert” refresh
                    member.first_name = member.first_name or first_name
                    member.last_name = member.last_name or last_name
                    member.party = member.party or party
                    member.state = member.state or state

                # --- Security fields ---
                symbol = _safe_str(row.get("ticker") or row.get("symbol"))
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

                # Create a stable hash/id for idempotency
                filing_key = _safe_str(row.get("id") or row.get("filingId") or row.get("filing_id"))
                if not filing_key:
                    # fallback: rep + date + symbol + type
                    filing_key = f"{member_key}_{filing_date}_{symbol}_{row.get('type')}"

                existing_filing = db.execute(select(Filing).where(Filing.document_hash == f"fmp:{filing_key}")).scalar_one_or_none()
                if existing_filing:
                    skipped += 1
                    continue

                filing = Filing(
                    member_id=member.id,
                    source="house_fmp",
                    filing_date=filing_date,
                    document_url=doc_url,
                    document_hash=f"fmp:{filing_key}",
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
            time.sleep(sleep_s)

        return {"status": "ok", "inserted": inserted, "skipped": skipped}

    finally:
        db.close()


if __name__ == "__main__":
    result = ingest_house()
    print(result)
