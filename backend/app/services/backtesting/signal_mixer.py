"""Bounded disclosure-based event study; separate from portfolio simulation."""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from math import isfinite
from statistics import median
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AnalystGradeEvent, Event, GovernmentContract
from app.services.backtesting.queries import (
    event_reporting_cik, first_text, is_buy_like_entry, load_price_histories,
    parse_iso_date, parse_payload,
)
from app.utils.symbols import normalize_symbol

MAX_EVENTS = 20_000
MAX_SYMBOLS = 500
HORIZONS = (30, 90, 365)


class SignalMixerConfig(BaseModel):
    trigger: Literal["insider", "congress"] = "insider"
    confirmation: Literal["congress", "insider", "government_contract", "analyst_upgrade"] = "government_contract"
    window_days: int = Field(default=30, ge=1, le=90)
    minimum_buyers: int = Field(default=1, ge=1, le=5)
    above_sma50: bool = False
    start_date: date
    end_date: date
    fee_bps: float = Field(default=0, ge=0, le=100, allow_inf_nan=False)
    slippage_bps: float = Field(default=10, ge=0, le=500, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_window(self):
        if not 0 < (self.end_date - self.start_date).days <= 1826:
            raise ValueError("Choose a study window between 1 day and 5 years.")
        if self.end_date > datetime.now(timezone.utc).date():
            raise ValueError("The study end date cannot be in the future.")
        if self.trigger == self.confirmation:
            raise ValueError("Choose a confirmation source different from the trigger.")
        return self


@dataclass(frozen=True)
class MixerEvent:
    symbol: str
    day: date
    event_id: str
    actor: str = ""


def _bounded_rows(db: Session, query):
    rows = db.execute(query.limit(MAX_EVENTS + 1)).scalars().all()
    if len(rows) > MAX_EVENTS:
        raise HTTPException(422, "Too many source records. Choose a shorter study window.")
    return rows


def load_mixer_events(db: Session, kind: str, start: date, end: date) -> tuple[list[MixerEvent], int]:
    start_ts = datetime.combine(start, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc)
    events: list[MixerEvent] = []
    missing_dates = 0
    if kind in {"insider", "congress"}:
        rows = _bounded_rows(db, select(Event).where(Event.event_type == f"{kind}_trade", Event.ts >= start_ts, Event.ts < end_ts).order_by(Event.ts, Event.id))
        for row in rows:
            payload = parse_payload(row.payload_json)
            if not is_buy_like_entry(row, payload):
                continue
            # Never substitute the transaction date or generic event_date.
            day = parse_iso_date(first_text(payload, "filing_date", "filingDate", "report_date", "reportDate"))
            if day is None:
                missing_dates += 1
                continue
            symbol = normalize_symbol(row.symbol or "")
            actor = (event_reporting_cik(payload) or first_text(payload, "insider_name", "insiderName", "reporting_owner_name") or "") if kind == "insider" else (row.member_bioguide_id or row.member_name or "")
            if symbol and start <= day <= end:
                events.append(MixerEvent(symbol, day, f"event:{row.id}", actor.strip().lower()))
    elif kind == "government_contract":
        rows = _bounded_rows(db, select(GovernmentContract).where(GovernmentContract.created_at >= start_ts, GovernmentContract.created_at < end_ts, GovernmentContract.award_amount > 0).order_by(GovernmentContract.created_at, GovernmentContract.id))
        for row in rows:
            day = max(row.award_date, row.created_at.date())
            symbol = normalize_symbol(row.symbol)
            if symbol and start <= day <= end:
                events.append(MixerEvent(symbol, day, f"contract:{row.id}"))
    else:
        rows = _bounded_rows(db, select(AnalystGradeEvent).where(AnalystGradeEvent.published_date >= start, AnalystGradeEvent.published_date <= end).order_by(AnalystGradeEvent.published_date, AnalystGradeEvent.id))
        for row in rows:
            if (row.action or "").strip().lower() not in {"upgrade", "upgraded"}:
                continue
            symbol = normalize_symbol(row.symbol)
            if symbol:
                events.append(MixerEvent(symbol, row.published_date, f"analyst:{row.id}"))
    return events, missing_dates


def select_setups(config: SignalMixerConfig, triggers: list[MixerEvent], confirmations: list[MixerEvent], prices: dict[str, dict[str, float]]):
    confirmations_by_symbol: dict[str, list[MixerEvent]] = defaultdict(list)
    for event in sorted(confirmations, key=lambda event: event.day):
        confirmations_by_symbol[event.symbol].append(event)
    confirmation_days = {symbol: [event.day for event in events] for symbol, events in confirmations_by_symbol.items()}
    prior: dict[str, list[MixerEvent]] = defaultdict(list)
    for event in sorted(triggers, key=lambda event: event.day):
        prior[event.symbol].append(event)
    prior_days = {symbol: [event.day for event in events] for symbol, events in prior.items()}
    selected = []
    counts: Counter = Counter()
    seen = set()
    for event in sorted(triggers, key=lambda event: (event.day, event.symbol, event.event_id)):
        if not config.start_date <= event.day <= config.end_date:
            continue
        counts["triggers"] += 1
        key = (event.symbol, event.day)
        if key in seen:
            counts["duplicate_symbol_day"] += 1
            continue
        seen.add(key)
        start = event.day - timedelta(days=config.window_days)
        buyer_days = prior_days[event.symbol]
        buyer_window = prior[event.symbol][bisect_left(buyer_days, start):bisect_right(buyer_days, event.day)]
        buyers = {item.actor for item in buyer_window if item.actor}
        if config.minimum_buyers > 1 and len(buyers) < config.minimum_buyers:
            counts["insufficient_distinct_buyers"] += 1
            continue
        # Exclude same-day confirmations: date-only records cannot establish ordering.
        days = confirmation_days.get(event.symbol, [])
        match_index = bisect_left(days, event.day) - 1
        if match_index < 0 or days[match_index] < start:
            counts["no_prior_confirmation"] += 1
            continue
        if config.above_sma50:
            history = prices.get(event.symbol, {})
            days = sorted(day for day, value in history.items() if day <= event.day.isoformat() and isfinite(value) and value > 0)
            if len(days) < 50 or (event.day - date.fromisoformat(days[-1])).days > 7:
                counts["missing_sma_history"] += 1
                continue
            if history[days[-1]] <= sum(history[day] for day in days[-50:]) / 50:
                counts["below_sma50"] += 1
                continue
        selected.append((event, confirmations_by_symbol[event.symbol][match_index]))
    return selected, dict(counts)


def evaluate_setups(config: SignalMixerConfig, setups, prices: dict[str, dict[str, float]]):
    spy = prices.get("SPY", {})
    cost = (config.fee_bps + config.slippage_bps) / 10_000
    setups = sorted(setups, key=lambda pair: (pair[0].day, pair[0].symbol, pair[0].event_id))
    common_days = {
        symbol: sorted(day for day, value in prices.get(symbol, {}).items() if day in spy and isfinite(value) and value > 0 and isfinite(spy[day]) and spy[day] > 0 and day <= config.end_date.isoformat())
        for symbol in {event.symbol for event, _ in setups}
    }
    horizons = []
    for horizon in HORIZONS:
        outcomes = []
        counts: Counter = Counter()
        occupied_until: dict[str, date] = {}
        for event, confirmation in setups:
            history = prices.get(event.symbol, {})
            days = common_days[event.symbol]
            index = bisect_right(days, event.day.isoformat())
            if index >= len(days) or (date.fromisoformat(days[index]) - event.day).days > 7:
                counts["missing_entry_prices"] += 1
                continue
            entry = date.fromisoformat(days[index])
            if entry <= occupied_until.get(event.symbol, date.min):
                counts["overlapping_setups"] += 1
                continue
            target = entry + timedelta(days=horizon)
            occupied_until[event.symbol] = target
            if target > config.end_date:
                counts["pending"] += 1
                continue
            exit_index = bisect_left(days, target.isoformat())
            if exit_index >= len(days) or (date.fromisoformat(days[exit_index]) - target).days > 7:
                counts["missing_exit_prices"] += 1
                continue
            entry_key, exit_key = entry.isoformat(), days[exit_index]
            occupied_until[event.symbol] = date.fromisoformat(exit_key)
            gross = (history[exit_key] / history[entry_key] - 1) * 100
            net = (history[exit_key] * (1 - cost) / (history[entry_key] * (1 + cost)) - 1) * 100
            benchmark = (spy[exit_key] * (1 - cost) / (spy[entry_key] * (1 + cost)) - 1) * 100
            outcomes.append({"symbol": event.symbol, "signal_date": event.day.isoformat(), "trigger_id": event.event_id, "confirmation_id": confirmation.event_id, "confirmation_date": confirmation.day.isoformat(), "entry_date": entry_key, "exit_date": exit_key, "gross_return_pct": round(gross, 4), "net_return_pct": round(net, 4), "spy_return_pct": round(benchmark, 4), "excess_return_pct": round(net - benchmark, 4)})
        n = len(outcomes)
        net_returns = [row["net_return_pct"] for row in outcomes]
        excess = [row["excess_return_pct"] for row in outcomes]
        horizons.append({"days": horizon, "sample_size": n, "positive_return_rate_pct": round(sum(value > 0 for value in net_returns) / n * 100, 2) if n else None, "beat_spy_rate_pct": round(sum(value > 0 for value in excess) / n * 100, 2) if n else None, "median_net_return_pct": round(median(net_returns), 4) if n else None, "median_excess_return_pct": round(median(excess), 4) if n else None, "worst_return_pct": min(net_returns) if n else None, "loss_count": sum(value < 0 for value in net_returns), "exclusions": dict(counts), "examples": outcomes[:50]})
    return horizons


def run_signal_mixer(db: Session, config: SignalMixerConfig):
    start = config.start_date - timedelta(days=config.window_days)
    triggers, missing_trigger_dates = load_mixer_events(db, config.trigger, start, config.end_date)
    confirmations, missing_confirmation_dates = load_mixer_events(db, config.confirmation, start, config.end_date)
    symbols = sorted({event.symbol for event in triggers})
    if len(symbols) > MAX_SYMBOLS:
        raise HTTPException(422, "Too many matching companies. Choose a shorter study window.")
    prices = load_price_histories(db, symbols + ["SPY"], start - timedelta(days=150), config.end_date)
    setups, diagnostics = select_setups(config, triggers, confirmations, prices)
    return {
        "methodology_version": "signal-mixer-event-study-v1",
        "config": config.model_dump(mode="json"),
        "matched_setups": len(setups),
        "diagnostics": {**diagnostics, "missing_trigger_filing_dates": missing_trigger_dates, "missing_confirmation_filing_dates": missing_confirmation_dates},
        "horizons": evaluate_setups(config, setups, prices),
        "assumptions": [
            "Historical event study, not a capital-constrained portfolio or evidence of predictive alpha. No live rule monitoring is enabled by running this study.",
            "Purchases require an explicit filing/report date. Transaction dates and records with missing filing dates are excluded. Same-day confirmations are excluded because their order is unknown.",
            "Contracts use the later of award date and first recorded observation in Walnut, not an inferred public announcement date. This limits historical contract coverage.",
            "Analyst upgrades use the provider's published date; an upgrade is not an earnings-estimate revision. Historical coverage and later corrections can affect the sample.",
            "Entry uses the next available common daily close after disclosure, within 7 calendar days. SMA uses only closes available through the signal date. Horizons are calendar days from entry; exits use the next common close within 7 days.",
            "Fees and slippage apply on entry and exit to both the stock and SPY. Taxes, dividends, market impact and liquidity constraints are not modeled. Existing cached close prices and their corporate-action treatment are used.",
            "One setup per company/disclosure day; overlapping holdings for the same company are excluded separately per horizon. Recent, incomplete horizons are pending, never shortened.",
            "Results use only stored records and prices, not a survivorship-free universe. Missing/delisted price histories can bias results; exclusions and sample sizes are reported. Small samples and repeated filter searches do not establish a reliable edge.",
        ],
    }
