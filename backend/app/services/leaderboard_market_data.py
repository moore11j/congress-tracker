"""Scheduled market-data enrichment; never called while serving a page."""
from __future__ import annotations

import logging
import json
from datetime import date, datetime, timedelta, timezone
from math import isfinite

from app.services.fundamentals_cache import selected_fundamentals_provider
from sqlalchemy import func, select

from app.clients.fmp import fetch_batch_market_capitalization, fetch_company_profile
from app.models import FundamentalsCache, PriceCache

logger = logging.getLogger(__name__)


def _positive(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def enrich_leaderboard_market_data(db, candidates: list[dict], *, now: datetime) -> None:
    """Fill market caps in batches and average volume from recent daily bars.

    Profile fallback is bounded to the 100 strongest qualifying candidates.
    Only market fields are repaired; statement dates and metrics stay intact.
    All writes commit together with the finished leaderboard snapshot.
    """
    if not candidates:
        return
    if selected_fundamentals_provider() == 'sec_edgar':
        _use_verified_selected_market_data(db, candidates, now=now)
        return
    by_symbol = {row['symbol']: row for row in candidates}
    symbols = list(by_symbol)
    today = now.date()
    cutoff = today - timedelta(days=7)
    for offset in range(0, len(symbols), 100):
        batch = symbols[offset:offset + 100]
        try:
            caps = fetch_batch_market_capitalization(symbols=batch, timeout_s=15)
        except Exception as exc:
            logger.warning('leaderboard_market_caps_unavailable error_type=%s', type(exc).__name__)
            continue
        for item in caps:
            symbol = str(item.get('symbol') or '').upper()
            cap = _positive(item.get('marketCap'))
            try:
                as_of = date.fromisoformat(str(item.get('date'))[:10])
            except ValueError:
                continue
            if symbol not in batch or cap is None or not cutoff <= as_of <= today:
                continue
            row = by_symbol[symbol]
            row['market_cap'] = cap
            row['market_cap_as_of'] = as_of.isoformat()
            row['market_cap_source'] = 'fmp_market_capitalization'

    # Coalesce the two known daily-volume columns, excluding missing and invalid
    # observations. A single day's volume must never masquerade as an average.
    volume = func.coalesce(PriceCache.volume, PriceCache.day_volume)
    bars = db.execute(select(PriceCache.symbol, PriceCache.date, volume.label('volume'))
        .where(PriceCache.symbol.in_(symbols), PriceCache.date >= (today - timedelta(days=45)).isoformat(),
               PriceCache.date <= today.isoformat(), volume > 0)
        .order_by(PriceCache.symbol, PriceCache.date.desc())).all()
    histories = {}
    for symbol, day, value in bars:
        history = histories.setdefault(symbol, [])
        if len(history) < 20 and _positive(value) is not None:
            history.append((day, float(value)))
    for symbol, history in histories.items():
        if len(history) < 10 or history[0][0] < cutoff.isoformat():
            continue
        row = by_symbol[symbol]
        row['avg_volume'] = sum(value for _, value in history) / len(history)
        row['avg_volume_as_of'] = history[0][0]
        row['avg_volume_observations'] = len(history)
        row['avg_volume_source'] = 'cached_daily_volume'

    leaders = sorted((row for row in candidates if row['confirmation'].get('direction') == 'bullish'
                      and row['confirmation'].get('score', 0) >= 20),
                     key=lambda row: (-row['confirmation']['score'], -(_positive(row.get('market_cap')) or 0), row['symbol']))[:100]
    for row in leaders:
        if _positive(row.get('market_cap')) and _positive(row.get('avg_volume')):
            continue
        try:
            profiles = fetch_company_profile(symbol=row['symbol'])
        except Exception as exc:
            logger.warning('leaderboard_profile_unavailable symbol=%s error_type=%s', row['symbol'], type(exc).__name__)
            continue
        profile = next((item for item in profiles if item.get('symbol') == row['symbol']), {})
        for field, keys in [('market_cap', ('marketCap',)), ('avg_volume', ('averageVolume', 'avgVolume'))]:
            value = next((_positive(profile.get(key)) for key in keys if _positive(profile.get(key)) is not None), None)
            if not _positive(row.get(field)) and value is not None:
                row[field] = value
                row[f'{field}_as_of'] = today.isoformat()
                row[f'{field}_source'] = 'fmp_profile'

    cache_rows = db.scalars(select(FundamentalsCache).where(
        FundamentalsCache.provider == selected_fundamentals_provider(), FundamentalsCache.symbol.in_(symbols))).all()
    for cached in cache_rows:
        row = by_symbol[cached.symbol]
        for field in ('market_cap', 'avg_volume'):
            if row.get(f'{field}_source') and _positive(row.get(field)):
                setattr(cached, field, row[field])
    logger.info('leaderboard_market_data total=%s with_cap=%s with_average_volume=%s', len(candidates),
                sum(bool(_positive(row.get('market_cap'))) for row in candidates),
                sum(bool(_positive(row.get('avg_volume'))) for row in candidates))


def _use_verified_selected_market_data(db, candidates, *, now):
    """Selected fundamentals refresh owns acquisition and its price provenance."""
    symbols = [row['symbol'] for row in candidates]
    cached = {row.symbol: row for row in db.scalars(select(FundamentalsCache).where(
        FundamentalsCache.provider == 'sec_edgar', FundamentalsCache.status == 'ok',
        FundamentalsCache.symbol.in_(symbols)))}
    now = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)
    for row in candidates:
        for field in ('market_cap', 'avg_volume'):
            row[field] = None
            for suffix in ('_as_of', '_source', '_observations'):
                row.pop(field + suffix, None)
        source = cached.get(row['symbol'])
        if source is None:
            continue
        try:
            evidence = json.loads(source.source_evidence_json or '{}')
            market = evidence.get('market') or {}
            observed = datetime.fromisoformat(market.get('observed_at', ''))
            cap = _positive((market.get('reference') or {}).get('market_cap'))
            if (market.get('provider') == 'massive' and observed.tzinfo is not None
                    and 0 <= (now - observed).total_seconds() <= 7 * 86400
                    and cap is not None and cap == source.market_cap):
                row.update(market_cap=cap, market_cap_as_of=observed.isoformat(), market_cap_source='massive_reference')
            daily = evidence.get('daily_market') or {}
            day = date.fromisoformat(daily.get('as_of', ''))
            volumes = [float(bar['volume']) for bar in (daily.get('bars') or {}).values()]
            if (daily.get('provider') == 'massive' and 0 <= (now.date() - day).days <= 7
                    and len(volumes) == 20 and all(isfinite(v) and v >= 0 for v in volumes)
                    and source.avg_volume == sum(volumes) / 20):
                row.update(avg_volume=source.avg_volume, avg_volume_as_of=str(day), avg_volume_observations=20,
                           avg_volume_source='massive_daily_bars')
        except (TypeError, ValueError, KeyError):
            continue
