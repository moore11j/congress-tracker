"""Daily model accounting from recorded decisions, separate from research backtests."""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math

from sqlalchemy import select
from app.models import PriceCache, StrategyEvaluationRun, StrategyTrade, StrategyModelChart, StrategyVersion
from app.services.price_lookup import is_market_trading_day, get_expected_latest_market_date
from app.services.outcome_integrity import adjusted_price
from app.utils.symbols import classify_symbol

METHODOLOGY = 'recorded_decisions_next_open_v1'
INITIAL_VALUE = 100000.0
# Same common-stock CUSIP; name/ticker change, not a conversion or inferred peer.
# SEC 8-K filed July 21, 2026, Items 5.03/8.01:
# https://www.sec.gov/Archives/edgar/data/1759546/000149315226034092/form8-k.htm
VERIFIED_PRICE_SUCCESSORS = {'NRDE': ('SNFI', date(2026, 7, 21))}


def session_on_or_after(day):
    while not is_market_trading_day(day):
        day += timedelta(days=1)
    return day


def trading_days(start, end):
    while start <= end:
        if is_market_trading_day(start):
            yield start
        start += timedelta(days=1)


def load_decisions(db, strategy_id):
    runs = db.execute(select(StrategyEvaluationRun).where(
        StrategyEvaluationRun.strategy_id == strategy_id,
        StrategyEvaluationRun.status == 'completed',
    ).order_by(StrategyEvaluationRun.evaluation_date, StrategyEvaluationRun.id)).scalars().all()
    trades = db.execute(select(StrategyTrade).where(StrategyTrade.strategy_id == strategy_id)
        .order_by(StrategyTrade.id)).scalars().all()
    by_run = defaultdict(list)
    for t in trades:
        if classify_symbol(t.symbol)[0] == 'eligible' and t.status != 'invalidated':
            by_run[t.strategy_run_id].append(t)
    versions = {v.id: json.loads(v.rules_json or "{}") for v in db.execute(select(StrategyVersion).where(StrategyVersion.strategy_id == strategy_id)).scalars()}
    targets = {}; schedules = {}
    for run in runs:
        changes = by_run[run.id]
        for t in changes:
            if t.action == 'sell':
                targets.pop(t.symbol, None)
            elif t.action in ('buy', 'rebalance'):
                targets[t.symbol] = float(t.weight_pct or 0) / 100
        meta = json.loads(run.metadata_json or '{}')
        if 'targetWeights' in meta:
            targets = {s: float(w) / 100 for s,w in meta['targetWeights'].items() if classify_symbol(s)[0] == 'eligible'}
        elif 'candidateSymbols' in meta and versions.get(run.strategy_version_id, {}).get('candidate_source') in {'congress_member_disclosures', 'disclosure_portfolio', 'cross_source_disclosure_alignment'}:
            # Legacy evaluator compared with original buy weights and sometimes
            # omitted necessary rebalance rows. These versioned resolvers use equal
            # weights; the complete recorded membership is the reliable target.
            members = meta['candidateSymbols']
            targets = {s: 1 / len(members) for s in members if classify_symbol(s)[0] == 'eligible'} if members else {}
        if changes:
            # Historical rows sometimes use a weekend or same-day sale date.
            # Every recorded daily decision executes no earlier than next session.
            execution = session_on_or_after(run.evaluation_date + timedelta(days=1))
            # Executed-at prevents a backdated run from trading before it existed.
            if run.executed_at:
                from zoneinfo import ZoneInfo
                stamp = run.executed_at.replace(tzinfo=timezone.utc) if run.executed_at.tzinfo is None else run.executed_at
                local = stamp.astimezone(ZoneInfo('America/New_York'))
                floor = local.date() + (timedelta(days=1) if (local.hour, local.minute) >= (9, 30) else timedelta())
                execution = max(execution, session_on_or_after(floor))
            schedules[execution] = dict(targets)
    start = runs[0].evaluation_date if runs else None
    return start, schedules, [r.id for r in runs]


def required_prices(start, schedules, end):
    needed = defaultdict(set)
    if start is None:
        return needed
    held = set()
    for day in trading_days(start, end):
        target = schedules.get(day)
        needed['SPY'].add(day)
        symbols = held | set(target or {})
        for symbol in symbols:
            needed[symbol].add(day)
        if target is not None:
            held = set(target)
    for old, (new, available) in VERIFIED_PRICE_SUCCESSORS.items():
        if old in needed:
            needed[new].update(d for d in needed[old] if d >= available)
    return needed


def load_bars(db, needed):
    bars = {}
    symbols = sorted(needed)
    days = [d for dates in needed.values() for d in dates]
    if not days:
        return bars
    start, end = min(days), max(days)
    for offset in range(0, len(symbols), 100):
        batch = symbols[offset:offset+100]
        rows = db.execute(select(PriceCache).where(PriceCache.symbol.in_(batch),
            PriceCache.date >= start.isoformat(), PriceCache.date <= end.isoformat())).scalars()
        for row in rows:
            if row.adjustment_status != 'split_adjusted_price_return':
                continue
            opened, closed = adjusted_price(row, 'open'), adjusted_price(row, 'close')
            opened = float(opened) if opened and opened > 0 and math.isfinite(opened) else None
            closed = float(closed) if closed and closed > 0 and math.isfinite(closed) else None
            if opened is not None or closed is not None:
                bars[(row.symbol, row.date)] = (opened, closed)
    for old, (new, available) in VERIFIED_PRICE_SUCCESSORS.items():
        for day in trading_days(start, end) if old in needed else []:
            key = day.isoformat()
            if day >= available and (old, key) not in bars and (new, key) in bars:
                bars[(old, key)] = bars[(new, key)]
    return bars


def chart_price_requirements(db, strategy_id, end):
    start, schedules, _ = load_decisions(db, strategy_id)
    needed = required_prices(start, schedules, end)
    # A missing execution price can postpone an exit. Continue pricing the actual
    # retained shares, including beyond their originally scheduled sale date.
    _, gaps = simulate(start, schedules, end, load_bars(db, needed), requirements=needed)
    for gap in gaps:
        needed[gap['symbol']].update(trading_days(date.fromisoformat(gap['date']), end))
    for old, (new, available) in VERIFIED_PRICE_SUCCESSORS.items():
        if old in needed:
            needed[new].update(d for d in needed[old] if d >= available)
    return needed


def simulate(start, schedules, end, bars, *, unfilled=None, stale_marks=None, skipped_rebalances=None, requirements=None):
    points = []; shares = {}; cash = INITIAL_VALUE; peak = INITIAL_VALUE
    missing = []; benchmark_start = None
    unfilled = unfilled if unfilled is not None else []
    stale_marks = stale_marks if stale_marks is not None else []
    skipped_rebalances = skipped_rebalances if skipped_rebalances is not None else []
    last_marks = {}
    for day in trading_days(start, end) if start else []:
        key = day.isoformat(); target = schedules.get(day)
        if requirements is not None:
            for symbol in set(shares) | set(target or {}) | {'SPY'}:
                requirements[symbol].add(day)
        gaps = []
        if target is not None:
            unavailable = [symbol for symbol in sorted(shares) if bars.get((symbol, key), (None, None))[0] is None]
            if unavailable:
                # No fictional fills: retain executed shares/cash for this decision.
                skipped_rebalances.append({'date': key, 'symbols': unavailable})
                target = None
        spy = bars.get(('SPY', key), (None, None))[1]
        if spy is None:
            gaps.append({'symbol': 'SPY', 'date': key, 'field': 'close'})
        if target is not None:
            if any(w < 0 or not math.isfinite(w) for w in target.values()) or sum(target.values()) > 1.000001:
                raise ValueError('Invalid recorded model weights')
            # Existing holdings cannot be liquidated/rebalanced at an invented open.
            for symbol in sorted(shares):
                if bars.get((symbol, key), (None, None))[0] is None:
                    gaps.append({'symbol': symbol, 'date': key, 'field': 'open'})
            executable = {}
            for symbol, weight in target.items():
                if bars.get((symbol, key), (None, None))[0] is None:
                    if symbol not in shares:
                        unfilled.append({'symbol': symbol, 'date': key, 'weightPct': weight * 100})
                else:
                    executable[symbol] = weight
            marked = set(executable)
        else:
            marked = set(shares)
        day_closes = {}
        for symbol in sorted(marked):
            close = bars.get((symbol, key), (None, None))[1]
            if close is not None:
                day_closes[symbol] = close
                last_marks[symbol] = (day, close)
            elif symbol in shares and symbol in last_marks and len(list(trading_days(last_marks[symbol][0] + timedelta(days=1), day))) <= 3:
                mark_day, close = last_marks[symbol]
                day_closes[symbol] = close
                stale_marks.append({'symbol': symbol, 'date': key, 'priceAsOf': mark_day.isoformat()})
            else:
                gaps.append({'symbol': symbol, 'date': key, 'field': 'close'})
        if gaps:
            missing = gaps
            break  # Never pad a missing-price day with a fictitious flat return.
        if target is not None:
            at_open = cash + sum(q * bars[(symbol, key)][0] for symbol, q in shares.items())
            shares = {symbol: at_open * w / bars[(symbol, key)][0] for symbol, w in executable.items() if w > 0}
            cash = max(0, at_open * (1 - sum(executable.values())))
        value = cash + sum(q * day_closes[symbol] for symbol, q in shares.items())
        benchmark_start = benchmark_start or spy
        benchmark = INITIAL_VALUE * spy / benchmark_start
        peak = max(peak, value)
        points.append({'date': key, 'strategyValue': value, 'benchmarkValue': benchmark,
                       'drawdownPct': (value / peak - 1) * 100, 'activeHoldings': len(shares), 'cash': cash, 'staleHoldings': sum(1 for x in stale_marks if x['date'] == key)})
    return points, missing


def build_chart(db, strategy_id, *, as_of=None):
    end = as_of or get_expected_latest_market_date()
    start, schedules, run_ids = load_decisions(db, strategy_id)
    needed = required_prices(start, schedules, end)
    bars = load_bars(db, needed)
    unfilled = []; stale_marks = []; skipped = []
    points, missing = simulate(start, schedules, end, bars, unfilled=unfilled, stale_marks=stale_marks, skipped_rebalances=skipped)
    return {'methodology': METHODOLOGY, 'source': 'recorded_model_decisions',
            'expectedThrough': end.isoformat(), 'through': points[-1]['date'] if points else None,
            'startedOn': start.isoformat() if start else None,
            'status': ('partial_coverage' if unfilled or stale_marks or skipped else 'current') if points and points[-1]['date'] == end.isoformat() else 'price_gap' if missing else 'awaiting_first_session' if start and start > end else 'awaiting_evaluation',
            'points': points, 'missingPrices': missing, 'unfilledOrders': unfilled, 'staleMarks': stale_marks, 'skippedRebalances': skipped, 'runIds': run_ids,
            'decisionHash': hashlib.sha256(json.dumps({str(k): v for k,v in schedules.items()}, sort_keys=True).encode()).hexdigest(),
            'assumptions': 'Reconstructed recorded model decisions and versioned equal-weight targets; next-session opens, fractional shares, uninvested cash earns zero. Split-adjusted price returns; dividends, fees and slippage excluded. Unpriced entry orders remain cash. Rebalances without all required opens are skipped. Existing holdings may use a labeled last close for at most three trading sessions; longer gaps stop valuation. Annual reported holdings are not opening balances.',
            'updatedAt': datetime.now(timezone.utc).isoformat()}


def refresh_chart(db, strategy_id, *, as_of=None):
    payload = build_chart(db, strategy_id, as_of=as_of)
    row = db.get(StrategyModelChart, strategy_id)
    if row is None:
        row = StrategyModelChart(strategy_id=strategy_id)
        db.add(row)
    row.payload_json = json.dumps(payload, allow_nan=False)
    db.commit()
    return payload


def chart_payload(db, strategy_id, *, period='max'):
    row = db.get(StrategyModelChart, strategy_id)
    payload = json.loads(row.payload_json) if row else {'points': [], 'status': 'awaiting_refresh', 'source': 'recorded_model_decisions'}
    expected = get_expected_latest_market_date().isoformat()
    if payload.get('status') in {'current', 'partial_coverage'} and payload.get('through') != expected:
        payload['status'] = 'stale'
    payload['expectedThrough'] = expected
    points = payload.get('points', [])
    days = {'30d': 30, '1y': 365, '2y': 730, '3y': 1095}.get(period)
    if points and days:
        cutoff = date.fromisoformat(points[-1]['date']) - timedelta(days=days)
        points = [p for p in points if date.fromisoformat(p['date']) >= cutoff]
    payload['points'] = points
    # Private decision identities and missing ticker details are not public chart metadata.
    payload.pop('runIds', None); payload.pop('decisionHash', None)
    payload['missingPriceCount'] = len(payload.pop('missingPrices', []))
    payload['unfilledSymbolCount'] = len({x['symbol'] for x in payload.pop('unfilledOrders', [])})
    payload['staleMarkCount'] = len(payload.pop('staleMarks', []))
    payload['skippedRebalanceCount'] = len(payload.pop('skippedRebalances', []))
    perf = {'period': period, 'asOfDate': payload.get('through')}
    if len(points) >= 2:
        a,b = points[0],points[-1]
        total = (b['strategyValue']/a['strategyValue']-1)*100
        bench = (b['benchmarkValue']/a['benchmarkValue']-1)*100
        elapsed = (date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days
        peak=a['strategyValue']; drawdown=0
        for p in points:
            peak=max(peak,p['strategyValue']);drawdown=min(drawdown,(p['strategyValue']/peak-1)*100)
        perf.update(totalReturnPct=total, benchmarkReturnPct=bench, maxDrawdownPct=drawdown,
                    cagrPct=((1+total/100)**(365.25/elapsed)-1)*100 if elapsed>=365 else None,
                    alphaCagrPct=None, winRatePct=None)
    payload['performance'] = perf
    return payload
