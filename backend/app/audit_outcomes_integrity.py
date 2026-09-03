from __future__ import annotations

import argparse
import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import ConfirmationScoreSnapshot, PriceCache


getcontext().prec = 34
MARKET_TZ = ZoneInfo("America/New_York")
UTC = timezone.utc
HORIZONS = (7, 30, 90, 180, 365)
AUDIT_VERSION = "outcomes-integrity-v1"


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _return_pct(start: Decimal | None, end: Decimal | None) -> Decimal | None:
    if start is None or end is None or start <= 0 or end <= 0:
        return None
    return ((end / start) - Decimal("1")) * Decimal("100")


def _side(direction: str | None) -> str | None:
    value = str(direction or "").lower()
    if "bull" in value:
        return "bullish"
    if "bear" in value:
        return "bearish"
    return None


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_json(raw: str | None, fallback: Any) -> Any:
    try:
        value = json.loads(raw or "")
    except Exception:
        return fallback
    return value


def _price_day(row: PriceCache) -> date | None:
    try:
        return date.fromisoformat(str(row.date)[:10])
    except (TypeError, ValueError):
        return None


def _adjustment_ratio(row: PriceCache) -> Decimal:
    adjusted = _decimal(row.adjusted_close)
    raw = _decimal(row.raw_close)
    if adjusted is not None and raw is not None and adjusted > 0 and raw > 0:
        return adjusted / raw
    return Decimal("1")


def _basis_price(row: PriceCache, kind: str) -> Decimal | None:
    if kind == "close":
        return _decimal(row.adjusted_close if row.adjusted_close is not None else row.close)
    raw_value = {
        "open": row.open_price,
        "high": row.high_price,
        "low": row.low_price,
    }[kind]
    value = _decimal(raw_value)
    if value is None:
        return None
    # FMP supplies raw OHLC beside adjusted close. Massive supplies all fields on
    # the same adjusted basis and has no raw_close, so the ratio remains one.
    return value * _adjustment_ratio(row)


class PriceIndex:
    def __init__(self, rows: Iterable[PriceCache]) -> None:
        self.rows: dict[str, list[PriceCache]] = defaultdict(list)
        for row in rows:
            self.rows[str(row.symbol or "").upper()].append(row)
        self.days: dict[str, list[date]] = {}
        for symbol, symbol_rows in self.rows.items():
            symbol_rows.sort(key=lambda row: str(row.date))
            self.days[symbol] = [_price_day(row) or date.min for row in symbol_rows]

    def exact(self, symbol: str, day: date) -> PriceCache | None:
        rows = self.rows.get(symbol.upper(), [])
        days = self.days.get(symbol.upper(), [])
        index = bisect_left(days, day)
        return rows[index] if index < len(rows) and days[index] == day else None

    def on_or_after(self, symbol: str, day: date, *, strict: bool = False) -> PriceCache | None:
        rows = self.rows.get(symbol.upper(), [])
        days = self.days.get(symbol.upper(), [])
        index = bisect_right(days, day) if strict else bisect_left(days, day)
        return rows[index] if index < len(rows) else None


@dataclass
class AuditedRow:
    event_id: int
    ticker: str
    score: int
    classification: str
    calculation_type: str
    event_timestamp: str | None
    event_timestamp_et: str | None
    evidence_cutoff: str | None
    stored_entry_timestamp: str | None
    audited_entry_timestamp: str | None
    stored_entry_price: str | None
    audited_entry_price: str | None
    entry_difference: str | None
    stored_entry_session: str | None
    audited_entry_session: str | None
    entry_price_source: str | None
    entry_price_type: str
    adjustment_type: str | None
    stored_returns: dict[str, str | None]
    audited_returns: dict[str, str | None]
    stored_spy_returns: dict[str, str | None]
    audited_spy_returns: dict[str, str | None]
    stored_excess_returns: dict[str, str | None]
    audited_excess_returns: dict[str, str | None]
    status: str
    failures: list[str]
    root_cause: str
    provenance_status: str


def _canonical_entry_row(index: PriceIndex, snapshot: ConfirmationScoreSnapshot) -> PriceCache | None:
    calculated = _utc(snapshot.calculated_at)
    if calculated is None:
        return None
    local = calculated.astimezone(MARKET_TZ)
    # A premarket event may use that session's open because it is still in the
    # future. At/after 09:30 ET, daily data cannot establish an executable
    # intraday quote, so use the following valid session's open.
    strict = local.timetz().replace(tzinfo=None) >= time(9, 30)
    return index.on_or_after(snapshot.ticker_at_time, local.date(), strict=strict)


def _session_open_at(day: date) -> datetime:
    return datetime.combine(day, time(9, 30), tzinfo=MARKET_TZ).astimezone(UTC)


def _session_close_at(day: date) -> datetime:
    return datetime.combine(day, time(16, 0), tzinfo=MARKET_TZ).astimezone(UTC)


def _current_outcome(
    index: PriceIndex,
    snapshot: ConfirmationScoreSnapshot,
    days: int,
    today: date,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if snapshot.market_date is None or snapshot.market_date + timedelta(days=days) > today:
        return None, None, None
    entry = _decimal(snapshot.reference_price)
    exit_row = index.on_or_after(snapshot.ticker_at_time, snapshot.market_date + timedelta(days=days))
    spy_entry = index.on_or_after("SPY", snapshot.market_date)
    spy_exit = index.on_or_after("SPY", snapshot.market_date + timedelta(days=days))
    security_return = _return_pct(entry, _basis_price(exit_row, "close") if exit_row else None)
    spy_return = _return_pct(
        _basis_price(spy_entry, "close") if spy_entry else None,
        _basis_price(spy_exit, "close") if spy_exit else None,
    )
    excess = security_return - spy_return if security_return is not None and spy_return is not None else None
    return security_return, spy_return, excess


def _audited_outcome(
    index: PriceIndex,
    snapshot: ConfirmationScoreSnapshot,
    entry_row: PriceCache | None,
    days: int,
    today: date,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    entry_day = _price_day(entry_row) if entry_row else None
    entry_price = _basis_price(entry_row, "open") if entry_row else None
    if entry_day is None or entry_day + timedelta(days=days) > today:
        return None, None, None
    target = entry_day + timedelta(days=days)
    spy_entry = index.exact("SPY", entry_day)
    spy_exit = index.on_or_after("SPY", target)
    spy_exit_day = _price_day(spy_exit) if spy_exit else None
    exit_row = index.exact(snapshot.ticker_at_time, spy_exit_day) if spy_exit_day else None
    security_return = _return_pct(entry_price, _basis_price(exit_row, "close") if exit_row else None)
    spy_return = _return_pct(
        _basis_price(spy_entry, "open") if spy_entry else None,
        _basis_price(spy_exit, "close") if spy_exit else None,
    )
    excess = security_return - spy_return if security_return is not None and spy_return is not None else None
    return security_return, spy_return, excess


def _string(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _provenance_status(snapshot: ConfirmationScoreSnapshot) -> str:
    contributions = _parse_json(snapshot.source_contributions_json, {})
    active_sources = _parse_json(snapshot.active_sources_json, [])
    if not isinstance(contributions, dict) or not isinstance(active_sources, list) or not active_sources:
        return "missing"
    for source in active_sources:
        payload = contributions.get(source)
        if not isinstance(payload, dict):
            return "missing"
        evidence = payload.get("evidence") or payload.get("evidence_ids")
        available_at = payload.get("available_at") or payload.get("evidence_cutoff_at")
        if not evidence or not available_at:
            return "unverifiable"
    return "verifiable"


def _visible_directional_snapshots(rows: list[ConfirmationScoreSnapshot]) -> tuple[list[ConfirmationScoreSnapshot], int]:
    latest: dict[tuple[str, int, int, date], ConfirmationScoreSnapshot] = {}
    same_day_duplicates = 0
    for row in rows:
        if _side(row.direction) is None or row.market_date is None:
            continue
        key = (row.calculation_type, row.security_id, row.methodology_version_id, row.market_date)
        current = latest.get(key)
        if current is not None:
            same_day_duplicates += 1
        current_time = (_utc(current.calculated_at) or datetime.min.replace(tzinfo=UTC), int(current.id or 0)) if current else None
        row_time = (_utc(row.calculated_at) or datetime.min.replace(tzinfo=UTC), int(row.id or 0))
        if current_time is None or row_time > current_time:
            latest[key] = row
    return list(latest.values()), same_day_duplicates


def _overlap_count(rows: list[ConfirmationScoreSnapshot]) -> int:
    groups: dict[tuple[str, int, int], list[ConfirmationScoreSnapshot]] = defaultdict(list)
    for row in rows:
        groups[(row.calculation_type, row.security_id, row.methodology_version_id)].append(row)
    overlaps = 0
    for group in groups.values():
        ordered = sorted(group, key=lambda row: (_utc(row.calculated_at) or datetime.min.replace(tzinfo=UTC), int(row.id or 0)))
        previous_side = None
        for row in ordered:
            side = _side(row.direction)
            if side == previous_side:
                overlaps += 1
            previous_side = side
    return overlaps


def _metric(rows: list[AuditedRow], horizon: str, *, audited: bool) -> dict[str, Any]:
    returns_key = "audited_returns" if audited else "stored_returns"
    spy_key = "audited_spy_returns" if audited else "stored_spy_returns"
    excess_key = "audited_excess_returns" if audited else "stored_excess_returns"
    values: list[Decimal] = []
    spy_values: list[Decimal] = []
    excess_values: list[Decimal] = []
    wins = 0
    for row in rows:
        raw = _decimal(getattr(row, returns_key).get(horizon))
        spy = _decimal(getattr(row, spy_key).get(horizon))
        excess = _decimal(getattr(row, excess_key).get(horizon))
        if raw is None:
            continue
        direction_value = raw if row.classification == "bullish" else -raw
        values.append(direction_value)
        if direction_value > 0 or (excess is not None and (excess if row.classification == "bullish" else -excess) > 0):
            wins += 1
        if spy is not None:
            spy_values.append(spy)
        if excess is not None:
            excess_values.append(excess if row.classification == "bullish" else -excess)
    mean = lambda items: (sum(items) / Decimal(len(items))) if items else None
    return {
        "observations": len(values),
        "directional_accuracy_pct": _string(Decimal(wins) / Decimal(len(values)) * 100) if values else None,
        "average_directional_return_pct": _string(mean(values)),
        "average_spy_return_pct": _string(mean(spy_values)),
        "average_directional_excess_return_pct": _string(mean(excess_values)),
        "benchmarked_observations": len(excess_values),
    }


def _provider_price_rows(
    symbols: set[str],
    *,
    start_day: date,
    end_day: date,
    workers: int,
) -> tuple[list[Any], dict[str, Any]]:
    from app.services.price_lookup import _fetch_provider_eod_price_bars

    rows: list[Any] = []
    failures: list[str] = []
    source_counts: Counter[str] = Counter()

    def fetch(symbol: str) -> tuple[str, dict[str, Any]]:
        bars, _provider_symbol = _fetch_provider_eod_price_bars(
            symbol,
            start_day.isoformat(),
            end_day.isoformat(),
            allow_user_request=True,
            require_adjusted=True,
        )
        return symbol, bars

    with ThreadPoolExecutor(max_workers=max(1, min(int(workers or 1), 16))) as executor:
        futures = {executor.submit(fetch, symbol): symbol for symbol in sorted(symbols)}
        for completed, future in enumerate(as_completed(futures), start=1):
            symbol = futures[future]
            try:
                resolved_symbol, bars = future.result()
            except Exception:
                failures.append(symbol)
                continue
            if not bars:
                failures.append(symbol)
                continue
            for bar in bars.values():
                source_counts[str(bar.price_source or "unknown")] += 1
                rows.append(
                    SimpleNamespace(
                        symbol=resolved_symbol,
                        date=bar.date,
                        close=bar.close,
                        adjusted_close=bar.adjusted_close,
                        raw_close=bar.raw_close,
                        open_price=bar.open_price,
                        high_price=bar.high_price,
                        low_price=bar.low_price,
                        price_source=bar.price_source,
                        adjustment_status=bar.adjustment_status,
                    )
                )
            if completed % 100 == 0:
                print(json.dumps({"provider_progress": completed, "provider_total": len(symbols)}), flush=True)
    return rows, {
        "requested_symbols": len(symbols),
        "verified_symbols": len(symbols) - len(failures),
        "missing_symbols": len(failures),
        "missing_symbol_examples": failures[:100],
        "price_rows": len(rows),
        "source_counts": dict(sorted(source_counts.items())),
    }


def audit(
    db: Session,
    *,
    today: date | None = None,
    provider_verify: bool = False,
    provider_workers: int = 6,
) -> tuple[dict[str, Any], list[AuditedRow]]:
    audit_today = today or datetime.now(UTC).date()
    snapshots = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
    visible, same_day_duplicates = _visible_directional_snapshots(snapshots)
    symbols = {str(row.ticker_at_time).upper() for row in visible} | {"SPY"}
    if visible:
        earliest = min((_utc(row.calculated_at) or datetime.now(UTC)).date() for row in visible) - timedelta(days=7)
        price_rows = db.execute(
            select(PriceCache)
            .where(PriceCache.symbol.in_(symbols))
            .where(PriceCache.date >= earliest.isoformat())
            .where(PriceCache.date <= (audit_today + timedelta(days=7)).isoformat())
            .order_by(PriceCache.symbol, PriceCache.date)
        ).scalars().all()
    else:
        price_rows = []
    provider_report: dict[str, Any] | None = None
    if provider_verify and visible:
        provider_rows, provider_report = _provider_price_rows(
            symbols,
            start_day=earliest,
            end_day=audit_today + timedelta(days=7),
            workers=provider_workers,
        )
        provider_keys = {(row.symbol, row.date) for row in provider_rows}
        price_rows = [row for row in price_rows if (str(row.symbol).upper(), str(row.date)) not in provider_keys]
        price_rows.extend(provider_rows)
    index = PriceIndex(price_rows)
    results: list[AuditedRow] = []
    failure_counts: Counter[str] = Counter()
    horizon_observations = 0

    for snapshot in visible:
        failures: list[str] = []
        calculated = _utc(snapshot.calculated_at)
        stored_at = _utc(snapshot.reference_price_at)
        stored_entry = _decimal(snapshot.reference_price)
        stored_session = snapshot.market_date
        stored_price_row = index.exact(snapshot.ticker_at_time, stored_session) if stored_session else None
        canonical_row = _canonical_entry_row(index, snapshot)
        canonical_day = _price_day(canonical_row) if canonical_row else None
        canonical_price = _basis_price(canonical_row, "open") if canonical_row else None

        if calculated is None:
            failures.append("TIMESTAMP_ERROR")
        if stored_at is None or (calculated is not None and stored_at < calculated):
            failures.append("TIMESTAMP_ERROR")
        if stored_entry is None or stored_entry <= 0:
            failures.append("INVALID_ENTRY_PRICE")
        if stored_price_row is None:
            failures.append("MISSING_DATA")
        else:
            low = _basis_price(stored_price_row, "low")
            high = _basis_price(stored_price_row, "high")
            if low is not None and high is not None and stored_entry is not None and not (low <= stored_entry <= high):
                failures.append("INVALID_ENTRY_PRICE")
            elif low is None or high is None:
                failures.append("MISSING_DATA")
        if canonical_row is None or canonical_day is None or canonical_price is None:
            failures.append("MISSING_DATA")
        elif calculated is not None and _session_open_at(canonical_day) < calculated:
            failures.append("TIMESTAMP_ERROR")
        elif stored_session != canonical_day or stored_entry != canonical_price:
            failures.append("INVALID_ENTRY_PRICE")

        provenance = _provenance_status(snapshot)
        if provenance != "verifiable":
            failures.append("LOOKAHEAD_UNVERIFIABLE")

        stored_returns: dict[str, str | None] = {}
        audited_returns: dict[str, str | None] = {}
        stored_spy: dict[str, str | None] = {}
        audited_spy: dict[str, str | None] = {}
        stored_excess: dict[str, str | None] = {}
        audited_excess: dict[str, str | None] = {}
        for horizon_days in HORIZONS:
            label = f"{horizon_days}D"
            current_return, current_spy, current_excess = _current_outcome(index, snapshot, horizon_days, audit_today)
            audited_return, audited_benchmark, audited_excess_value = _audited_outcome(index, snapshot, canonical_row, horizon_days, audit_today)
            stored_returns[label] = _string(current_return)
            audited_returns[label] = _string(audited_return)
            stored_spy[label] = _string(current_spy)
            audited_spy[label] = _string(audited_benchmark)
            stored_excess[label] = _string(current_excess)
            audited_excess[label] = _string(audited_excess_value)
            if current_return is not None:
                horizon_observations += 1
            if current_return is not None and audited_return is None:
                failures.append("MISSING_DATA")
            elif current_return is not None and audited_return is not None and abs(current_return - audited_return) > Decimal("0.005"):
                failures.append("CALCULATION_DIFFERENCE")
            if current_spy is not None and audited_benchmark is not None and abs(current_spy - audited_benchmark) > Decimal("0.005"):
                failures.append("BENCHMARK_ERROR")

        failures = list(dict.fromkeys(failures))
        for failure in failures:
            failure_counts[failure] += 1
        primary_order = (
            "TIMESTAMP_ERROR",
            "INVALID_ENTRY_PRICE",
            "CORPORATE_ACTION_ERROR",
            "BENCHMARK_ERROR",
            "CALCULATION_ERROR",
            "INVALID_EXIT_PRICE",
            "MISSING_DATA",
            "LOOKAHEAD_UNVERIFIABLE",
            "CALCULATION_DIFFERENCE",
        )
        status = next((item for item in primary_order if item in failures), "PASS")
        root_cause = "; ".join(failures) if failures else "Reproduced from point-in-time executable prices"
        event_et = calculated.astimezone(MARKET_TZ) if calculated else None
        audited_at = _session_open_at(canonical_day) if canonical_day else None
        results.append(
            AuditedRow(
                event_id=int(snapshot.id),
                ticker=snapshot.ticker_at_time,
                score=int(snapshot.score),
                classification=_side(snapshot.direction) or str(snapshot.direction),
                calculation_type=snapshot.calculation_type,
                event_timestamp=_iso(calculated),
                event_timestamp_et=_iso(event_et),
                evidence_cutoff=None,
                stored_entry_timestamp=_iso(stored_at),
                audited_entry_timestamp=_iso(audited_at),
                stored_entry_price=_string(stored_entry),
                audited_entry_price=_string(canonical_price),
                entry_difference=_string(canonical_price - stored_entry) if canonical_price is not None and stored_entry is not None else None,
                stored_entry_session=_iso(stored_session),
                audited_entry_session=_iso(canonical_day),
                entry_price_source=snapshot.reference_price_source,
                entry_price_type="next_executable_session_open",
                adjustment_type=str(canonical_row.adjustment_status) if canonical_row and canonical_row.adjustment_status else None,
                stored_returns=stored_returns,
                audited_returns=audited_returns,
                stored_spy_returns=stored_spy,
                audited_spy_returns=audited_spy,
                stored_excess_returns=stored_excess,
                audited_excess_returns=audited_excess,
                status=status,
                failures=failures,
                root_cause=root_cause,
                provenance_status=provenance,
            )
        )

    passing = sum(1 for row in results if row.status == "PASS")
    report = {
        "audit_version": AUDIT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "as_of_date": audit_today.isoformat(),
        "methodology": {
            "entry": "First valid session open at or after a premarket event; otherwise first valid session open after the event",
            "horizons": "Calendar days from executable entry session; first valid close on or after target",
            "benchmark": "SPY official open on the entry session and first valid close on or after the same target",
            "return_basis": "Adjusted price return on a single internally consistent price basis",
            "timezone": "America/New_York for session logic; UTC for persisted timestamps",
        },
        "counts": {
            "total_stored_snapshots": len(snapshots),
            "total_directional_events": len(results),
            "total_horizon_observations": horizon_observations,
            "pass_count": passing,
            "fail_count": len(results) - passing,
            "pass_percentage": (passing / len(results) * 100) if results else None,
            "same_day_duplicate_snapshots": same_day_duplicates,
            "overlapping_same_direction_events": _overlap_count(visible),
        },
        "failure_categories": dict(sorted(failure_counts.items())),
        "provider_verification": provider_report,
        "before_metrics": {f"{days}D": _metric(results, f"{days}D", audited=False) for days in HORIZONS},
        "after_metrics": {f"{days}D": _metric(results, f"{days}D", audited=True) for days in HORIZONS},
        "examples": [asdict(row) for row in results if row.status != "PASS"][:25],
    }
    return report, results


def _write_csv(path: Path, rows: list[AuditedRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    scalar_fields = [field for field in AuditedRow.__dataclass_fields__ if field not in {
        "stored_returns", "audited_returns", "stored_spy_returns", "audited_spy_returns", "stored_excess_returns", "audited_excess_returns", "failures"
    }]
    dynamic_fields = []
    for prefix in ("stored_return", "audited_return", "stored_spy_return", "audited_spy_return", "stored_excess_return", "audited_excess_return"):
        dynamic_fields.extend(f"{prefix}_{days}D" for days in HORIZONS)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=scalar_fields + dynamic_fields + ["failures"])
        writer.writeheader()
        for row in rows:
            payload = {field: getattr(row, field) for field in scalar_fields}
            for days in HORIZONS:
                label = f"{days}D"
                payload[f"stored_return_{label}"] = row.stored_returns[label]
                payload[f"audited_return_{label}"] = row.audited_returns[label]
                payload[f"stored_spy_return_{label}"] = row.stored_spy_returns[label]
                payload[f"audited_spy_return_{label}"] = row.audited_spy_returns[label]
                payload[f"stored_excess_return_{label}"] = row.stored_excess_returns[label]
                payload[f"audited_excess_return_{label}"] = row.audited_excess_returns[label]
            payload["failures"] = "|".join(row.failures)
            writer.writerow(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only full Outcome Ledger integrity audit")
    parser.add_argument("--json", dest="json_path", default="artifacts/outcomes-integrity-audit.json")
    parser.add_argument("--csv", dest="csv_path", default="artifacts/outcomes-integrity-audit.csv")
    parser.add_argument("--as-of", dest="as_of", default=None)
    parser.add_argument("--provider-verify", action="store_true")
    parser.add_argument("--provider-workers", type=int, default=6)
    args = parser.parse_args()
    with SessionLocal() as db:
        report, rows = audit(
            db,
            today=date.fromisoformat(args.as_of) if args.as_of else None,
            provider_verify=args.provider_verify,
            provider_workers=args.provider_workers,
        )
    json_path = Path(args.json_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(Path(args.csv_path), rows)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
