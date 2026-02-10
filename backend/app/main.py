from __future__ import annotations

import logging
import json
import os
import subprocess

from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel

from app.db import Base, DATABASE_URL, SessionLocal, engine, ensure_event_columns, get_db
from app.models import Event, Filing, Member, Security, Transaction, Watchlist, WatchlistItem
from app.routers.events import router as events_router
from app.routers.signals import router as signals_router

logger = logging.getLogger(__name__)

def _extract_district(member: Member) -> str | None:
    if (member.chamber or "").lower() != "house":
        return None
    bioguide = (member.bioguide_id or "").upper()
    if not bioguide.startswith("FMP_HOUSE_"):
        return None
    suffix = bioguide[len("FMP_HOUSE_"):]
    if len(suffix) < 4:
        return None
    state = suffix[:2]
    district = suffix[2:]
    if not state.isalpha() or not district.isdigit():
        return None
    return district


def _member_payload(member: Member) -> dict:
    return {
        "bioguide_id": member.bioguide_id,
        "member_id": member.id,
        "name": f"{member.first_name or ''} {member.last_name or ''}".strip(),
        "party": member.party,
        "state": member.state,
        "district": _extract_district(member),
        "chamber": member.chamber,
    }

def _top_member_payload(member: Member) -> dict:
    member_identifier = (member.bioguide_id or "").strip()
    payload = {
        "member_id": member_identifier,
        "name": f"{member.first_name or ''} {member.last_name or ''}".strip(),
        "party": member.party,
        "state": member.state,
        "district": _extract_district(member),
        "chamber": member.chamber,
    }
    if member_identifier and not member_identifier.upper().startswith("FMP_"):
        payload["bioguide_id"] = member_identifier
    return payload


# --- App --------------------------------------------------------------------

app = FastAPI(title="Congress Tracker API", version="0.1.0")

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://congress-tracker-two.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class WatchlistPayload(BaseModel):
    name: str

def _autoheal_if_empty() -> dict:
    """
    Boot-time self-heal: if DB has 0 transactions, run ingest pipeline.
    This prevents the "machine restarted -> empty feed until I remember token" problem.
    """
    # Allow turning off via env if you ever want it
    if os.getenv("AUTOHEAL_ON_STARTUP", "1").strip() not in ("1", "true", "TRUE", "yes", "YES"):
        return {"status": "skipped", "reason": "AUTOHEAL_ON_STARTUP disabled"}

    db = SessionLocal()
    try:
        tx_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
    finally:
        db.close()

    if tx_count and tx_count > 0:
        return {"status": "ok", "did_ingest": False, "transactions": tx_count}

    # Empty -> run ingest chain (same as /admin/ensure_data but no token)
    steps = ["app.ingest_house", "app.ingest_senate", "app.enrich_members", "app.write_last_updated"]
    results = []
    for mod in steps:
        r = _run_module(mod)
        results.append(r)
        if r["returncode"] != 0:
            print("AUTOHEAL FAILED:", {"step": mod, "results": results})
            return {"status": "failed", "step": mod, "results": results}

    # Recount
    db2 = SessionLocal()
    try:
        tx_count2 = db2.execute(select(func.count()).select_from(Transaction)).scalar_one()
    finally:
        db2.close()

    print("AUTOHEAL OK:", {"transactions": tx_count2})
    return {"status": "ok", "did_ingest": True, "transactions": tx_count2, "results": results}


def _needs_event_repair(db: Session) -> bool:
    missing_clause = or_(
        Event.member_name.is_(None),
        Event.member_bioguide_id.is_(None),
        Event.chamber.is_(None),
        Event.party.is_(None),
        Event.trade_type.is_(None),
        Event.amount_min.is_(None),
        Event.amount_max.is_(None),
        Event.event_date.is_(None),
        Event.symbol.is_(None),
    )
    row = db.execute(
        select(Event.id)
        .where(Event.event_type == "congress_trade")
        .where(missing_clause)
        .limit(1)
    ).scalar_one_or_none()
    return row is not None


@app.on_event("startup")
def _startup_create_tables():
    # Creates tables if missing. Does NOT delete or overwrite data.
    Base.metadata.create_all(engine)
    ensure_event_columns()

    if os.getenv("AUTO_REPAIR_EVENTS_ON_STARTUP", "1").strip() in ("1", "true", "TRUE", "yes", "YES"):
        db = SessionLocal()
        try:
            if _needs_event_repair(db):
                from app.backfill_events_from_trades import repair_events

                repair_events(db)
        finally:
            db.close()

    # NEW: self-heal if the DB is empty (prevents empty feed after restarts/autostop)
    try:
        _autoheal_if_empty()
    except Exception as e:
        # Don't crash the app on boot — log and keep serving (you can still call /admin/ensure_data)
        print("AUTOHEAL EXCEPTION:", repr(e))

    if os.getenv("AUTO_BACKFILL_EVENTS_ON_STARTUP", "1").strip() in (
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    ):
        db = SessionLocal()
        try:
            tx_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
            event_count = db.execute(
                select(func.count())
                .select_from(Event)
                .where(Event.event_type == "congress_trade")
            ).scalar_one()
        finally:
            db.close()

        if tx_count > 0 and event_count == 0:
            logger.info("Auto-backfill triggered: transactions=%s events=0", tx_count)
            try:
                from app.backfill_events_from_trades import run_backfill

                results = run_backfill(
                    dry_run=False,
                    limit=None,
                    replace=False,
                    repair=False,
                )
                logger.info(
                    "Auto-backfill done: scanned=%s inserted=%s skipped=%s",
                    results.get("scanned", 0),
                    results.get("inserted", 0),
                    results.get("skipped", 0),
                )
            except Exception:
                logger.exception("Auto-backfill failed")


def _sqlite_path_from_database_url(database_url: str) -> str | None:
    """
    Supports:
      sqlite:////absolute/path.db
      sqlite:///relative-or-absolute/path.db
      sqlite:relative.db
    Returns an absolute-ish path string to the sqlite file, or None if not sqlite.
    """
    if not database_url or not database_url.startswith("sqlite:"):
        return None

    rest = database_url[len("sqlite:"):]

    # sqlite:////data/db.sqlite  -> /data/db.sqlite
    if rest.startswith("////"):
        return rest[3:]  # keep one leading slash

    # sqlite:///app/db.sqlite -> /app/db.sqlite (absolute)
    if rest.startswith("///"):
        return rest[2:]  # keep one leading slash

    # sqlite://relative.db -> relative.db
    if rest.startswith("//"):
        return rest[2:]

    # sqlite:relative.db -> relative.db
    return rest


def _utc_iso_from_mtime(path: str) -> str | None:
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        return None
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/seed-demo")
def seed_demo(db: Session = Depends(get_db)):
    existing = db.execute(select(Member).where(Member.bioguide_id == "DEMO0001")).scalar_one_or_none()
    if existing:
        return {"status": "ok", "message": "Demo data already seeded."}

    m = Member(
        bioguide_id="DEMO0001",
        first_name="Demo",
        last_name="Member",
        chamber="house",
        party="I",
        state="CA",
    )
    s = Security(
        symbol="NVDA",
        name="NVIDIA Corporation",
        asset_class="stock",
        sector="Technology",
    )
    db.add_all([m, s])
    db.flush()

    f = Filing(
        member_id=m.id,
        source="house",
        filing_date=date(2026, 1, 9),
        document_url="https://example.com",
        document_hash="demo-1",
    )
    db.add(f)
    db.flush()

    tx = Transaction(
        filing_id=f.id,
        member_id=m.id,
        security_id=s.id,
        owner_type="self",
        transaction_type="buy",
        trade_date=date(2025, 12, 1),
        report_date=date(2026, 1, 9),
        amount_range_min=15000,
        amount_range_max=50000,
        description="Purchase - Demo",
    )
    db.add(tx)
    db.commit()

    return {"status": "ok", "message": "Seeded demo member + NVDA trade."}


@app.get("/api/feed")
def feed(
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    tape: str = Query("congress"),
    symbol: str | None = None,
    member: str | None = None,
    chamber: str | None = None,
    transaction_type: str | None = None,
    min_amount: float | None = None,
    whale: int | None = Query(default=None),
    recent_days: int | None = None,
):
    tape_value = (tape or "congress").strip().lower()
    if tape_value not in {"congress", "insider", "all"}:
        raise HTTPException(status_code=400, detail="tape must be one of: congress, insider, all")

    if tape_value == "congress":
        from datetime import timedelta

        q = (
            select(Transaction, Member, Security)
            .join(Member, Transaction.member_id == Member.id)
            .outerjoin(Security, Transaction.security_id == Security.id)
        )

        if whale:
            min_amount = max(min_amount or 0, 250000)

        if recent_days is not None:
            if recent_days < 1:
                raise HTTPException(status_code=400, detail="recent_days must be >= 1")
            cutoff = date.today() - timedelta(days=recent_days)
            q = q.where(Transaction.report_date >= cutoff)

        if symbol:
            q = q.where(Security.symbol == symbol.strip().upper())
        if chamber:
            q = q.where(Member.chamber == chamber.strip().lower())
        if transaction_type:
            q = q.where(Transaction.transaction_type == transaction_type.strip().lower())
        if min_amount is not None:
            q = q.where(
                or_(
                    Transaction.amount_range_max >= min_amount,
                    and_(Transaction.amount_range_max.is_(None), Transaction.amount_range_min >= min_amount),
                )
            )
        if member:
            term = f"%{member.strip().lower()}%"
            q = q.where(
                or_(
                    Member.first_name.ilike(term),
                    Member.last_name.ilike(term),
                    (Member.first_name + " " + Member.last_name).ilike(term),
                )
            )

        if cursor:
            try:
                cursor_date_str, cursor_id_str = cursor.split("|", 1)
                cursor_id = int(cursor_id_str)
                cursor_date = date.fromisoformat(cursor_date_str)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid cursor format. Expected YYYY-MM-DD|id")
            q = q.where(
                or_(
                    Transaction.report_date < cursor_date,
                    and_(Transaction.report_date == cursor_date, Transaction.id < cursor_id),
                )
            )

        q = q.order_by(Transaction.report_date.desc(), Transaction.id.desc()).limit(limit + 1)
        rows = db.execute(q).all()

        items = []
        for tx, m, s in rows[:limit]:
            security_payload = {
                "symbol": s.symbol if s is not None else None,
                "name": s.name if s is not None else "Unknown",
                "asset_class": s.asset_class if s is not None else "Unknown",
                "sector": s.sector if s is not None else None,
            }
            items.append(
                {
                    "id": tx.id,
                    "event_type": "congress_trade",
                    "member": {
                        "bioguide_id": m.bioguide_id,
                        "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                        "chamber": m.chamber,
                        "party": m.party,
                        "state": m.state,
                    },
                    "security": security_payload,
                    "transaction_type": tx.transaction_type,
                    "owner_type": tx.owner_type,
                    "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
                    "report_date": tx.report_date.isoformat() if tx.report_date else None,
                    "amount_range_min": tx.amount_range_min,
                    "amount_range_max": tx.amount_range_max,
                    "is_whale": bool(tx.amount_range_max is not None and tx.amount_range_max >= 250000),
                }
            )

        next_cursor = None
        if len(rows) > limit:
            tx_last = rows[limit - 1][0]
            if tx_last.report_date:
                next_cursor = f"{tx_last.report_date.isoformat()}|{tx_last.id}"

        return {"items": items, "next_cursor": next_cursor}

    event_types = ["insider_trade"] if tape_value == "insider" else ["congress_trade", "insider_trade"]
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    q = select(Event).where(Event.event_type.in_(event_types))

    if symbol:
        q = q.where(func.upper(Event.symbol) == symbol.strip().upper())
    if transaction_type:
        q = q.where(func.lower(Event.transaction_type) == transaction_type.strip().lower())
    if recent_days is not None:
        if recent_days < 1:
            raise HTTPException(status_code=400, detail="recent_days must be >= 1")
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=recent_days)
        q = q.where(sort_ts >= cutoff_dt)

    if cursor:
        try:
            cursor_ts_str, cursor_id_str = cursor.split("|", 1)
            cursor_id = int(cursor_id_str)
            cursor_ts = datetime.fromisoformat(cursor_ts_str.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor format. Expected ISO8601|id")
        q = q.where(or_(sort_ts < cursor_ts, and_(sort_ts == cursor_ts, Event.id < cursor_id)))

    q = q.order_by(sort_ts.desc(), Event.id.desc()).limit(limit + 1)
    rows = db.execute(q).scalars().all()

    items = []
    for event in rows[:limit]:
        try:
            payload = json.loads(event.payload_json)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        items.append(
            {
                "id": event.id,
                "event_type": event.event_type,
                "member": {
                    "bioguide_id": event.member_bioguide_id,
                    "name": event.member_name,
                    "chamber": event.chamber,
                    "party": event.party,
                    "state": None,
                },
                "security": {
                    "symbol": event.symbol,
                    "name": payload.get("security_name") or payload.get("insider_name") or event.symbol or "Unknown",
                    "asset_class": payload.get("asset_class") or "stock",
                    "sector": payload.get("sector"),
                },
                "transaction_type": event.transaction_type or event.trade_type,
                "owner_type": payload.get("owner_type") or "insider",
                "trade_date": payload.get("transaction_date") or payload.get("trade_date"),
                "report_date": payload.get("filing_date") or payload.get("report_date"),
                "amount_range_min": event.amount_min,
                "amount_range_max": event.amount_max,
                "is_whale": bool(event.amount_max is not None and event.amount_max >= 250000),
                "source": event.source,
            }
        )

    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        cursor_ts = last.event_date or last.ts
        next_cursor = f"{cursor_ts.isoformat()}|{last.id}"

    return {"items": items, "next_cursor": next_cursor}



@app.get("/api/meta")
def meta():
    # IMPORTANT: use the same resolved DATABASE_URL the app uses (not env-only),
    # so meta works even when DATABASE_URL isn't explicitly set.
    db_file = _sqlite_path_from_database_url(DATABASE_URL)

    last_updated_utc = None
    if db_file:
        if not db_file.startswith("/"):
            db_file = os.path.abspath(db_file)
        last_updated_utc = _utc_iso_from_mtime(db_file)

    # Fallback if not sqlite OR file missing:
    if last_updated_utc is None:
        db = SessionLocal()
        try:
            latest = db.execute(select(func.max(Filing.filing_date))).scalar_one_or_none()
            if latest:
                dt = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
                last_updated_utc = dt.isoformat().replace("+00:00", "Z")
        finally:
            db.close()

    return {"last_updated_utc": last_updated_utc}

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

def _require_admin(token: str | None):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not configured")
    if not token or token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _run_module(module: str) -> dict:
    """
    Runs: python3 -m <module>
    Returns stdout/stderr and exit code.
    """
    p = subprocess.run(
        ["python3", "-m", module],
        capture_output=True,
        text=True,
        cwd="/app",
    )
    return {
        "module": module,
        "returncode": p.returncode,
        "stdout": p.stdout[-4000:],  # keep it small
        "stderr": p.stderr[-4000:],
    }


@app.post("/admin/ensure_data")
def ensure_data(token: str | None = Query(default=None), db: Session = Depends(get_db)):
    """
    If transactions == 0, run ingest_house + ingest_senate + enrich_members + write_last_updated.
    Safe to call repeatedly.
    """
    _require_admin(token)

    tx_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
    if tx_count and tx_count > 0:
        return {"status": "ok", "did_ingest": False, "transactions": tx_count}

    # DB empty -> run ingest chain
    results = []
    for mod in ["app.ingest_house", "app.ingest_senate", "app.enrich_members", "app.write_last_updated"]:
        r = _run_module(mod)
        results.append(r)
        if r["returncode"] != 0:
            raise HTTPException(status_code=500, detail={"status": "failed", "step": mod, "results": results})

    # Re-check count
    tx_count2 = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
    return {"status": "ok", "did_ingest": True, "transactions": tx_count2, "results": results}


@app.get("/api/members/{bioguide_id}")
def member_profile(bioguide_id: str, db: Session = Depends(get_db)):
    # Fetch member
    member = db.execute(
        select(Member).where(Member.bioguide_id == bioguide_id)
    ).scalar_one_or_none()

    if not member:
        raise HTTPException(status_code=404, detail="Member not found")

    # Fetch their trades (latest first)
    q = (
        select(Transaction, Security)
        .outerjoin(Security, Transaction.security_id == Security.id)
        .where(Transaction.member_id == member.id)
        .order_by(Transaction.report_date.desc(), Transaction.id.desc())
        .limit(200)
    )

    rows = db.execute(q).all()

    trades = []
    ticker_counts = {}

    for tx, s in rows:
        symbol = s.symbol if s else None

        if symbol:
            ticker_counts[symbol] = ticker_counts.get(symbol, 0) + 1

        trades.append({
            "id": tx.id,
            "symbol": symbol,
            "security_name": s.name if s else "Unknown",
            "transaction_type": tx.transaction_type,
            "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
            "report_date": tx.report_date.isoformat() if tx.report_date else None,
            "amount_range_min": tx.amount_range_min,
            "amount_range_max": tx.amount_range_max,
        })

    top_tickers = sorted(
        ticker_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]

    return {
        "member": _member_payload(member),
        "top_tickers": [{"symbol": s, "trades": n} for s, n in top_tickers],
        "trades": trades,
    }

@app.get("/api/tickers/{symbol}")
def ticker_profile(symbol: str, db: Session = Depends(get_db)):
    sym = symbol.upper().strip()

    security = db.execute(
        select(Security).where(Security.symbol == sym)
    ).scalar_one_or_none()

    if not security:
        raise HTTPException(status_code=404, detail="Ticker not found")

    q = (
        select(Transaction, Member)
        .join(Member, Transaction.member_id == Member.id)
        .where(Transaction.security_id == security.id)
        .order_by(Transaction.report_date.desc(), Transaction.id.desc())
        .limit(200)
    )

    rows = db.execute(q).all()

    trades = []
    member_counts: dict[int, int] = {}
    members_by_id: dict[int, Member] = {}

    for tx, m in rows:
        member_counts[m.id] = member_counts.get(m.id, 0) + 1
        members_by_id[m.id] = m

        trades.append({
            "id": tx.id,
            "member": {
                "bioguide_id": m.bioguide_id,
                "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                "chamber": m.chamber,
                "party": m.party,
                "state": m.state,
            },
            "transaction_type": tx.transaction_type,
            "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
            "report_date": tx.report_date.isoformat() if tx.report_date else None,
            "amount_range_min": tx.amount_range_min,
            "amount_range_max": tx.amount_range_max,
        })

    top_members = sorted(
        member_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    )[:10]

    return {
        "ticker": {
            "symbol": security.symbol,
            "name": security.name,
            "asset_class": security.asset_class,
            "sector": security.sector,
        },
        "top_members": [
            {
                **_top_member_payload(members_by_id[member_id]),
                "trade_count": trade_count,
            }
            for member_id, trade_count in top_members
        ],
        "trades": trades,
    }


@app.post("/api/watchlists")
def create_watchlist(payload: WatchlistPayload, db: Session = Depends(get_db)):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Watchlist name is required")

    existing = db.execute(select(Watchlist).where(Watchlist.name == name)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    w = Watchlist(name=name)
    db.add(w)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Watchlist name already exists")
    return {"id": w.id, "name": w.name}


@app.get("/api/watchlists")
def list_watchlists(db: Session = Depends(get_db)):
    rows = db.execute(select(Watchlist)).scalars().all()
    return [{"id": w.id, "name": w.name} for w in rows]


@app.delete("/api/watchlists/{watchlist_id}", status_code=204)
def delete_watchlist(watchlist_id: int, db: Session = Depends(get_db)):
    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == watchlist_id)
    ).scalar_one_or_none()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    db.execute(
        WatchlistItem.__table__.delete().where(
            WatchlistItem.watchlist_id == watchlist_id
        )
    )
    db.delete(watchlist)
    db.commit()

    return None


@app.put("/api/watchlists/{watchlist_id}")
def rename_watchlist(watchlist_id: int, payload: WatchlistPayload, db: Session = Depends(get_db)):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Watchlist name is required")

    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == watchlist_id)
    ).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    existing = db.execute(
        select(Watchlist).where(and_(Watchlist.name == name, Watchlist.id != watchlist_id))
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    watchlist.name = name
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    return {"id": watchlist.id, "name": watchlist.name}


@app.post("/api/watchlists/{watchlist_id}/add")
def add_to_watchlist(watchlist_id: int, symbol: str, db: Session = Depends(get_db)):
    sec = db.execute(
        select(Security).where(Security.symbol == symbol.upper())
    ).scalar_one_or_none()

    if not sec:
        raise HTTPException(404, "Ticker not found")

    item = WatchlistItem(
        watchlist_id=watchlist_id,
        security_id=sec.id,
    )
    db.add(item)
    db.commit()
    return {"status": "added", "symbol": symbol.upper()}


@app.delete("/api/watchlists/{watchlist_id}/remove")
def remove_from_watchlist(watchlist_id: int, symbol: str, db: Session = Depends(get_db)):
    sec = db.execute(
        select(Security).where(Security.symbol == symbol.upper())
    ).scalar_one_or_none()

    if not sec:
        raise HTTPException(404, "Ticker not found")

    db.execute(
        WatchlistItem.__table__.delete().where(
            and_(
                WatchlistItem.watchlist_id == watchlist_id,
                WatchlistItem.security_id == sec.id,
            )
        )
    )
    db.commit()

    return {"status": "removed", "symbol": symbol.upper()}


@app.get("/api/watchlists/{watchlist_id}")
def get_watchlist(watchlist_id: int, db: Session = Depends(get_db)):
    q = (
        select(Security.symbol, Security.name)
        .join(WatchlistItem, WatchlistItem.security_id == Security.id)
        .where(WatchlistItem.watchlist_id == watchlist_id)
    )

    rows = db.execute(q).all()

    return {
        "watchlist_id": watchlist_id,
        "tickers": [
            {"symbol": s, "name": n} for s, n in rows
        ],
    }


@app.get("/api/watchlists/{watchlist_id}/feed")
def watchlist_feed(
    watchlist_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,

    # allow same filters as /api/feed
    whale: int | None = Query(default=None),
    recent_days: int | None = None,
):
    """
    Feed filtered to tickers inside a watchlist.

    IMPORTANT: WatchlistItem stores security_id (not symbol), so we join:
      WatchlistItem -> Security -> Transaction
    """

    # 1) Get security_ids in this watchlist
    watch_security_ids = db.execute(
        select(WatchlistItem.security_id).where(WatchlistItem.watchlist_id == watchlist_id)
    ).scalars().all()

    if not watch_security_ids:
        return {"items": [], "next_cursor": None}

    # 2) Build same base query shape as /api/feed
    q = (
        select(Transaction, Member, Security)
        .join(Member, Transaction.member_id == Member.id)
        .outerjoin(Security, Transaction.security_id == Security.id)
        .where(Transaction.security_id.in_(watch_security_ids))
    )

    # 3) Apply whale + recent_days shortcuts (same logic style as /api/feed)
    if whale == 1:
        # "big trades" shortcut; tune the threshold as you like
        q = q.where(
            or_(
                Transaction.amount_range_max >= 100000,
                and_(
                    Transaction.amount_range_max.is_(None),
                    Transaction.amount_range_min >= 100000,
                ),
            )
        )

    if recent_days is not None:
        # filter by report_date (safe, since your ordering uses report_date)
        cutoff = date.today() - timedelta(days=int(recent_days))
        q = q.where(Transaction.report_date.is_not(None)).where(Transaction.report_date >= cutoff)

    # 4) Cursor pagination (report_date DESC, id DESC)
    if cursor:
        try:
            cursor_date_str, cursor_id_str = cursor.split("|", 1)
            cursor_id = int(cursor_id_str)
            cursor_date = date.fromisoformat(cursor_date_str)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor format. Expected YYYY-MM-DD|id")

        q = q.where(
            or_(
                Transaction.report_date < cursor_date,
                and_(
                    Transaction.report_date == cursor_date,
                    Transaction.id < cursor_id,
                ),
            )
        )

    q = q.order_by(Transaction.report_date.desc(), Transaction.id.desc()).limit(limit + 1)
    rows = db.execute(q).all()

    items = []
    for tx, m, s in rows[:limit]:
        if s is not None:
            security_payload = {
                "symbol": s.symbol,
                "name": s.name,
                "asset_class": s.asset_class,
                "sector": s.sector,
            }
        else:
            security_payload = {
                "symbol": None,
                "name": "Unknown",
                "asset_class": "Unknown",
                "sector": None,
            }

        items.append(
            {
                "id": tx.id,
                "member": {
                    "bioguide_id": m.bioguide_id,
                    "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                    "chamber": m.chamber,
                    "party": m.party,
                    "state": m.state,
                },
                "security": security_payload,
                "transaction_type": tx.transaction_type,
                "owner_type": tx.owner_type,
                "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
                "report_date": tx.report_date.isoformat() if tx.report_date else None,
                "amount_range_min": tx.amount_range_min,
                "amount_range_max": tx.amount_range_max,
                "is_whale": bool(
                    tx.amount_range_max is not None and tx.amount_range_max >= 100000
                ) or bool(
                    tx.amount_range_max is None and tx.amount_range_min is not None and tx.amount_range_min >= 100000
                ),
            }
        )

    next_cursor = None
    if len(rows) > limit:
        tx_last = rows[limit - 1][0]
        if tx_last.report_date:
            next_cursor = f"{tx_last.report_date.isoformat()}|{tx_last.id}"

    return {"items": items, "next_cursor": next_cursor}


app.include_router(events_router, prefix="/api")
app.include_router(signals_router, prefix="/api")
