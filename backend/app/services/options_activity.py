"""Bounded, explicitly scoped historical activity. No buy/sell or sentiment inference."""
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import TickerContentCache
from app.services import options_alpaca as alpaca
from app.services import options_calculator as reference

WINDOW = "options-activity-v1"


def cached_summaries(db, symbols):
    rows = db.scalars(select(TickerContentCache).where(
        TickerContentCache.content_type == "options_activity",
        TickerContentCache.symbol.in_(symbols), TickerContentCache.window_key == WINDOW))
    result = {}
    for row in rows:
        if row.fetched_at is None:
            continue
        stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
        if timedelta(0) <= datetime.now(timezone.utc) - stamp < timedelta(hours=24):
            try:
                value = json.loads(row.payload_json)
            except (ValueError, TypeError):
                continue
            if not isinstance(value, dict) or value.get("data_basis") != "historical_daily_bars":
                continue
            # Unsigned historical activity must never confirm price direction.
            value.update(can_confirm=False, directional_evidence=False)
            result[row.symbol] = value
    return result


def refresh(symbol: str) -> dict:
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", symbol):
        raise reference.OptionsDataError("Invalid ticker.", 422)
    if not alpaca.enabled():
        raise reference.OptionsDataError("Free historical options data is not configured.")
    with reference.SessionLocal() as db:
        existing = cached_summaries(db, [symbol]).get(symbol)
        if existing:
            return existing
    now = datetime.now(timezone.utc)
    end = min(now.replace(hour=0, minute=0, second=0, microsecond=0), now - timedelta(minutes=16))
    start = end - timedelta(days=30)
    # Pick the first listed expiration at least three weeks away, then fetch all
    # its standard contracts. Never mix a truncated chain into activity totals.
    first = reference._request("/v3/reference/options/contracts", {
        "underlying_ticker": symbol, "expired": "false", "expiration_date.gte": (now + timedelta(days=21)).date().isoformat(),
        "limit": 1, "sort": "expiration_date", "order": "asc"})
    dates = [r.get("expiration_date") for r in first.get("results", []) if r.get("expiration_date")]
    if not dates:
        raise reference.OptionsDataError("No listed expiration found for this ticker.", 404)
    expiration = dates[0]
    chain = reference.contracts(symbol, expiration)
    contracts = {c["ticker"][2:]: c for c in chain["contracts"]}
    cursors = set()
    if chain.get("truncated") and not chain.get("next_cursor"):
        raise reference.OptionsDataError("The contract list is incomplete. No partial totals were saved.")
    while chain.get("next_cursor"):
        cursor = chain["next_cursor"]
        if cursor in cursors or len(contracts) >= 1000:
            raise reference.OptionsDataError("This expiration is too large for the free activity view. No partial totals were saved.")
        cursors.add(cursor)
        chain = reference.contracts(symbol, expiration, cursor)
        contracts.update({c["ticker"][2:]: c for c in chain["contracts"]})
        if chain.get("truncated") and not chain.get("next_cursor"):
            raise reference.OptionsDataError("The contract list is incomplete. No partial totals were saved.")
    if not contracts or len(contracts) > 1000:
        raise reference.OptionsDataError("No complete standard chain within the free activity limit.", 404)
    bars = {}
    symbols = sorted(contracts)
    for offset in range(0, len(symbols), 100):
        batch = symbols[offset:offset + 100]
        params = {"symbols": ",".join(batch), "timeframe": "1Day", "start": start.isoformat(),
                  "end": end.isoformat(), "limit": 10000, "sort": "asc"}
        seen = set()
        for page in range(10):
            payload = alpaca._request(params)
            for ticker, values in payload["bars"].items():
                if ticker not in batch:
                    continue
                for bar in values:
                    try:
                        stamp = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))
                    except (KeyError, ValueError, TypeError, AttributeError):
                        continue
                    if stamp.tzinfo is not None and start <= stamp < end:
                        bars[(ticker, stamp.date().isoformat())] = bar
            token = payload.get("next_page_token")
            if not token:
                break
            if token in seen or page == 9:
                raise reference.OptionsDataError("Activity history is incomplete. Retry later; no partial totals were saved.")
            seen.add(token)
            params = {**params, "page_token": token}
    result = summarize(symbol, expiration, contracts, bars, now)
    with reference.SessionLocal() as db:
        row = db.scalar(select(TickerContentCache).where(TickerContentCache.content_type == "options_activity",
            TickerContentCache.symbol == symbol, TickerContentCache.window_key == WINDOW))
        if row is None:
            row = TickerContentCache(content_type="options_activity", symbol=symbol, window_key=WINDOW,
                cache_key=f"options_activity:{symbol}:{WINDOW}", source="alpaca", status="ok", item_count=len(contracts))
            db.add(row)
        row.payload_json = json.dumps(result)
        row.fetched_at = now
        db.commit()
    return result


def summarize(symbol, expiration, contracts, bars, now):
    daily = defaultdict(lambda: {"call_volume": 0, "put_volume": 0, "call_premium": 0.0, "put_premium": 0.0})
    observed = set()
    for (ticker, day), bar in bars.items():
        volume, price = bar.get("v"), bar.get("vw")
        if ticker not in contracts:
            continue
        if (isinstance(volume, bool) or not isinstance(volume, (float, int))
                or not math.isfinite(volume) or volume < 0 or volume != int(volume)):
            raise reference.OptionsDataError("Activity returned invalid volume. No incomplete totals were saved.")
        if volume == 0:
            continue
        # No closing-price substitution: premium needs VWAP across executed trades.
        if isinstance(price, bool) or not isinstance(price, (float, int)) or not math.isfinite(price) or price < 0:
            raise reference.OptionsDataError("Activity volume was returned without a valid VWAP. No incomplete premium totals were saved.")
        kind = contracts[ticker]["kind"]
        daily[day][kind + "_volume"] += int(volume)
        daily[day][kind + "_premium"] += volume * price * 100
        observed.add(ticker)
    if not daily:
        raise reference.OptionsDataError("No historical activity found for the selected expiration.", 404)
    history = [{"date": day, **{k: round(v, 2) for k, v in daily[day].items()}} for day in sorted(daily)]
    calls = sum(d["call_premium"] for d in history)
    puts = sum(d["put_premium"] for d in history)
    cv = sum(d["call_volume"] for d in history)
    pv = sum(d["put_volume"] for d in history)
    state = "call_heavy" if calls > puts * 1.6 else "put_heavy" if puts > calls * 1.6 else "mixed"
    label = {"call_heavy": "Call-heavy activity", "put_heavy": "Put-heavy activity", "mixed": "Mixed call/put activity"}[state]
    previous = [d["call_volume"] + d["put_volume"] for d in history[:-1]][-20:]
    baseline = sum(previous) / len(previous) if len(previous) >= 5 else None
    multiple = (history[-1]["call_volume"] + history[-1]["put_volume"]) / baseline if baseline else None
    freshness = (now.date() - datetime.fromisoformat(history[-1]["date"]).date()).days
    return {"ticker": symbol, "lookback_days": 30, "state": state, "direction": state, "label": label,
        "active": True, "is_active": True, "confidence": "low", "can_confirm": False, "directional_evidence": False,
        "score": None, "intensity": "high" if multiple is not None and multiple >= 2 else "low", "status": "ok",
        "provider": "alpaca", "source": "alpaca", "latest_flow_date": history[-1]["date"], "freshness_days": freshness,
        "summary": label + ". Buying versus selling is unknown.", "signals": [label, "No directional confirmation"],
        "total_premium": round(calls + puts, 2), "call_put_premium_ratio": round(calls / puts, 2) if puts else None,
        "metrics": {"put_call_premium_ratio": round(puts / calls, 2) if calls else None,
            "net_premium_skew": round(calls - puts, 2), "total_premium": round(calls + puts, 2),
            "call_volume": cv, "put_volume": pv, "call_premium": round(calls, 2), "put_premium": round(puts, 2),
            "recent_contract_volume": cv + pv, "observed_contracts": len(observed), "freshness_days": freshness,
            "volume_multiple": round(multiple, 2) if multiple is not None else None, "baseline_sessions": len(previous)},
        "coverage": {"expiration": expiration, "listed_contracts": len(contracts), "scope": "single_expiration",
            "note": "Currently listed standard contracts for this expiration only; excludes other expirations and adjusted contracts."},
        "history": history, "as_of": now.isoformat(), "data_basis": "historical_daily_bars"}
