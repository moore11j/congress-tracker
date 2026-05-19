# backend/app/ingest_house.py
from __future__ import annotations

import os
import time
from argparse import ArgumentParser
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import requests
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Filing, Member, Security, Transaction
from app.services.congress_metadata import get_congress_metadata_resolver
from app.services.congress_assets import classify_congress_disclosure_asset
from app.utils.symbols import canonical_symbol

FMP_BASE = "https://financialmodelingprep.com/stable/house-latest"
DEFAULT_LIMIT = 100
DEFAULT_PAGES = 3  # keep small for MVP; bump later
DEFAULT_RECENT_PAGES = 25
PROGRESS_EVERY_PAGES = 10
NON_EQUITY_DESCRIPTION_TERMS = (
    "treasury",
    " t-bill",
    "tbill",
    " bill",
    " note",
    " bond",
    " debenture",
    " coupon",
    " cpn",
    "zero cpn",
)
NON_EQUITY_ASSET_CLASSES = {
    "bond",
    "bonds",
    "corporate bond",
    "government security",
    "government securities",
    "municipal security",
    "municipal securities",
    "us treasury",
    "u.s. treasury",
}


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


def _is_non_equity_security(asset_name: str | None, asset_class: str | None) -> bool:
    if classify_congress_disclosure_asset(security_description=asset_name, asset_class=asset_class):
        return True
    class_value = (asset_class or "").strip().lower()
    if class_value in NON_EQUITY_ASSET_CLASSES:
        return True
    description = f" {asset_name or ''} ".lower()
    return any(term in description for term in NON_EQUITY_DESCRIPTION_TERMS)


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


def _guess_party(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    r = raw.strip().upper()
    if r in {"D", "R", "I"}:
        return {"D": "Democrat", "R": "Republican", "I": "Independent"}[r]
    if "DEMO" in r:
        return "Democrat"
    if "REPU" in r or "GOP" in r:
        return "Republican"
    if "INDEP" in r:
        return "Independent"
    return raw.strip() or None


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


def _transaction_identity(
    *,
    filing_id: int,
    member_id: int,
    security_id: int | None,
    owner_type: str,
    transaction_type: str,
    trade_date: date | None,
    report_date: date | None,
    amount_min: float | None,
    amount_max: float | None,
    description: str | None = None,
) -> tuple:
    return (
        filing_id,
        member_id,
        security_id,
        description if security_id is None else None,
        owner_type,
        transaction_type,
        trade_date.isoformat() if trade_date else None,
        report_date.isoformat() if report_date else None,
        amount_min,
        amount_max,
    )


def _matching_transaction_exists(
    db,
    *,
    filing_id: int,
    member_id: int,
    security_id: int | None,
    owner_type: str,
    transaction_type: str,
    trade_date: date | None,
    report_date: date | None,
    amount_min: float | None,
    amount_max: float | None,
    description: str | None = None,
) -> bool:
    q = (
        select(Transaction.id)
        .where(Transaction.filing_id == filing_id)
        .where(Transaction.member_id == member_id)
        .where(Transaction.owner_type == owner_type)
        .where(Transaction.transaction_type == transaction_type)
        .where(Transaction.amount_range_min == amount_min)
        .where(Transaction.amount_range_max == amount_max)
    )
    q = (
        q.where(Transaction.security_id == security_id)
        if security_id is not None
        else q.where(Transaction.security_id.is_(None)).where(Transaction.description == description)
    )
    q = (
        q.where(Transaction.trade_date == trade_date)
        if trade_date is not None
        else q.where(Transaction.trade_date.is_(None))
    )
    q = (
        q.where(Transaction.report_date == report_date)
        if report_date is not None
        else q.where(Transaction.report_date.is_(None))
    )
    return db.execute(q.limit(1)).scalar_one_or_none() is not None


def _matching_transaction(
    db,
    *,
    filing_id: int,
    member_id: int,
    security_id: int | None,
    owner_type: str,
    transaction_type: str,
    trade_date: date | None,
    report_date: date | None,
    amount_min: float | None,
    amount_max: float | None,
    description: str | None = None,
) -> Transaction | None:
    base = (
        select(Transaction)
        .where(Transaction.filing_id == filing_id)
        .where(Transaction.owner_type == owner_type)
        .where(Transaction.transaction_type == transaction_type)
        .where(Transaction.amount_range_min == amount_min)
        .where(Transaction.amount_range_max == amount_max)
    )
    base = (
        base.where(Transaction.security_id == security_id)
        if security_id is not None
        else base.where(Transaction.security_id.is_(None)).where(Transaction.description == description)
    )
    base = (
        base.where(Transaction.trade_date == trade_date)
        if trade_date is not None
        else base.where(Transaction.trade_date.is_(None))
    )
    base = (
        base.where(Transaction.report_date == report_date)
        if report_date is not None
        else base.where(Transaction.report_date.is_(None))
    )
    exact = db.execute(base.where(Transaction.member_id == member_id).limit(1)).scalar_one_or_none()
    if exact is not None:
        return exact
    return db.execute(base.limit(1)).scalar_one_or_none()


def upsert_house_transaction_from_row(
    db,
    row: dict[str, Any],
    *,
    metadata=None,
    seen_transaction_keys: set[tuple] | None = None,
) -> dict[str, Any]:
    metadata = metadata or get_congress_metadata_resolver()
    seen_transaction_keys = seen_transaction_keys if seen_transaction_keys is not None else set()

    member_key, first_name, last_name, chamber, state = _member_key_and_fields(row)
    district = _safe_str(row.get("district"))
    full_name = f"{first_name or ''} {last_name or ''}".strip() or None
    party = _guess_party(_safe_str(row.get("party")))
    if not party:
        fallback = metadata.resolve(
            bioguide_id=member_key,
            first_name=first_name,
            last_name=last_name,
            full_name=full_name,
            chamber=chamber,
            state=state,
            house_district=district,
        )
        if fallback:
            party = fallback.party
            state = state or fallback.state

    member = db.execute(select(Member).where(Member.bioguide_id == member_key)).scalar_one_or_none()
    if member is None:
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
        member.chamber = member.chamber or chamber
        member.party = member.party or party
        member.state = member.state or state

    raw_symbol = _safe_str(row.get("symbol") or row.get("ticker"))
    asset_name = _safe_str(row.get("assetDescription") or row.get("asset") or row.get("company"))
    asset_class = _safe_str(row.get("assetType") or row.get("asset_class") or "stock") or "stock"
    sector = _safe_str(row.get("sector"))
    classification = classify_congress_disclosure_asset(
        security_description=asset_name,
        asset_class=asset_class,
        raw_symbol=raw_symbol,
    )
    non_equity = bool(classification) or _is_non_equity_security(asset_name, asset_class)
    symbol = None if non_equity else canonical_symbol(raw_symbol)

    security = None
    if symbol:
        security = db.execute(select(Security).where(Security.symbol == symbol)).scalar_one_or_none()
        if security is None:
            security = Security(
                symbol=symbol,
                name=asset_name or symbol,
                asset_class=asset_class,
                sector=sector,
            )
            db.add(security)
            db.flush()
        else:
            security.name = security.name or (asset_name or symbol)
            security.asset_class = security.asset_class or asset_class
            security.sector = security.sector or sector

    filing_date = _parse_date(row.get("disclosureDate") or row.get("reportDate") or row.get("filingDate"))
    doc_url = _safe_str(row.get("link") or row.get("pdf") or row.get("documentUrl") or row.get("document_url"))

    doc_id = None
    if doc_url and doc_url.endswith(".pdf"):
        doc_id = doc_url.split("/")[-1].replace(".pdf", "")

    if doc_id:
        filing_key = f"house:{doc_id}"
    else:
        filing_key = f"house:{member_key}|{filing_date}|{doc_url or ''}"

    filing_created = False
    filing = db.execute(select(Filing).where(Filing.document_hash == f"fmp:{filing_key}")).scalar_one_or_none()
    if filing is None:
        filing = Filing(
            member_id=member.id,
            source="house_fmp",
            filing_date=filing_date,
            document_url=doc_url,
            document_hash=f"fmp:{filing_key}",
        )
        db.add(filing)
        db.flush()
        filing_created = True
    else:
        filing.filing_date = filing.filing_date or filing_date
        filing.document_url = filing.document_url or doc_url

    tx_type = (_safe_str(row.get("type") or row.get("transactionType")) or "unknown").lower()
    owner_type = (_safe_str(row.get("owner") or row.get("ownerType")) or "self").lower()
    trade_date = _parse_date(row.get("transactionDate") or row.get("tradeDate"))
    report_date = filing_date
    lo, hi = _amount_to_range(row.get("amount") or row.get("amountRange"))
    desc = _safe_str(row.get("comment") or row.get("description"))
    if non_equity and not desc:
        desc = asset_name

    identity = _transaction_identity(
        filing_id=filing.id,
        member_id=member.id,
        security_id=security.id if security else None,
        description=desc,
        owner_type=owner_type,
        transaction_type=tx_type,
        trade_date=trade_date,
        report_date=report_date,
        amount_min=lo,
        amount_max=hi,
    )
    existing_tx = _matching_transaction(
        db,
        filing_id=filing.id,
        member_id=member.id,
        security_id=security.id if security else None,
        description=desc,
        owner_type=owner_type,
        transaction_type=tx_type,
        trade_date=trade_date,
        report_date=report_date,
        amount_min=lo,
        amount_max=hi,
    )
    duplicate_in_batch = identity in seen_transaction_keys
    if existing_tx is not None or duplicate_in_batch:
        if not duplicate_in_batch:
            seen_transaction_keys.add(identity)
        return {
            "filing": filing,
            "member": member,
            "security": security,
            "transaction": existing_tx,
            "transaction_inserted": False,
            "filing_created": filing_created,
            "duplicate_in_batch": duplicate_in_batch,
            "non_equity_symbol_skipped": bool(non_equity and raw_symbol and not symbol),
        }

    seen_transaction_keys.add(identity)
    tx = Transaction(
        filing_id=filing.id,
        member_id=member.id,
        security_id=security.id if security else None,
        owner_type=owner_type,
        transaction_type=tx_type,
        trade_date=trade_date,
        report_date=report_date,
        amount_range_min=lo,
        amount_range_max=hi,
        description=desc,
    )
    db.add(tx)
    db.flush()
    return {
        "filing": filing,
        "member": member,
        "security": security,
        "transaction": tx,
        "transaction_inserted": True,
        "filing_created": filing_created,
        "duplicate_in_batch": False,
        "non_equity_symbol_skipped": bool(non_equity and raw_symbol and not symbol),
    }


def ingest_house(
    pages: int = DEFAULT_PAGES,
    limit: int = DEFAULT_LIMIT,
    sleep_s: float = 0.25,
    dry_run: bool = False,
    recent_days: int | None = None,
) -> dict[str, Any]:
    inserted = 0
    skipped = 0
    skipped_old = 0
    rows_scanned = 0
    pages_processed = 0
    filings_created = 0
    non_equity_symbol_skipped = 0
    latest_report_date: date | None = None
    filings_seen: set[int] = set()
    seen_transaction_keys: set[tuple] = set()
    cutoff = (
        datetime.now(timezone.utc).date() - timedelta(days=max(recent_days, 0))
        if recent_days is not None
        else None
    )

    db = SessionLocal()
    try:
        metadata = get_congress_metadata_resolver()
        for page in range(pages):
            rows = _fetch_page(page=page, limit=limit)
            if not rows:
                break

            pages_processed += 1
            rows_scanned += len(rows)
            page_report_dates = [
                _parse_date(row.get("disclosureDate") or row.get("reportDate") or row.get("filingDate"))
                for row in rows
            ]
            for report_date in page_report_dates:
                if report_date and (latest_report_date is None or report_date > latest_report_date):
                    latest_report_date = report_date

            for row in rows:
                row_report_date = _parse_date(row.get("disclosureDate") or row.get("reportDate") or row.get("filingDate"))
                if cutoff is not None and (row_report_date is None or row_report_date < cutoff):
                    skipped_old += 1
                    continue
                outcome = upsert_house_transaction_from_row(
                    db,
                    row,
                    metadata=metadata,
                    seen_transaction_keys=seen_transaction_keys,
                )
                filing = outcome.get("filing")
                if filing is not None:
                    filings_seen.add(filing.id)
                if outcome["filing_created"]:
                    filings_created += 1
                if outcome["transaction_inserted"]:
                    inserted += 1
                else:
                    skipped += 1
                if outcome.get("non_equity_symbol_skipped"):
                    non_equity_symbol_skipped += 1

            if dry_run:
                db.rollback()
            else:
                db.commit()
            if pages_processed % PROGRESS_EVERY_PAGES == 0:
                print(
                    f"[house] progress pages={pages_processed} inserted={inserted} skipped={skipped}",
                    flush=True,
                )
            time.sleep(sleep_s)
            if cutoff is not None and page_report_dates and max([d for d in page_report_dates if d], default=date.min) < cutoff:
                break

        return {
            "status": "ok",
            "inserted": inserted,
            "skipped": skipped,
            "skipped_old": skipped_old,
            "rows_scanned": rows_scanned,
            "filings_scanned": len(filings_seen),
            "filings_created": filings_created,
            "non_equity_symbol_skipped": non_equity_symbol_skipped,
            "latest_report_date": latest_report_date.isoformat() if latest_report_date else None,
            "pages_processed": pages_processed,
            "dry_run": dry_run,
            "recent_days": recent_days,
        }

    finally:
        db.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--pages", type=int, default=int(os.getenv("INGEST_PAGES", str(DEFAULT_PAGES))))
    parser.add_argument("--limit", type=int, default=int(os.getenv("INGEST_LIMIT", str(DEFAULT_LIMIT))))
    parser.add_argument("--sleep-s", type=float, default=float(os.getenv("INGEST_SLEEP_S", "0.25")))
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env_dry_run = os.getenv("INGEST_DRY_RUN", "").strip().lower() in {"1", "true", "yes"}
    print(
        ingest_house(
            pages=args.pages,
            limit=args.limit,
            sleep_s=args.sleep_s,
            dry_run=args.dry_run or env_dry_run,
            recent_days=args.recent_days,
        )
    )
