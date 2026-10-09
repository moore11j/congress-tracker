"""Materialize prepared company headlines once for all watchlist delivery paths."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
import re

from sqlalchemy import select, text
from app.models import Event, InsightsSnapshot
from app.services.finnhub_research import canonical_news_url, selected_news_provider
from app.utils.symbols import normalize_symbol


def _stamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime):
        raise ValueError('Missing timestamp')
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _title(value):
    return re.sub(r'\s+', ' ', value.strip()).casefold() if isinstance(value, str) else ''


def existing_news_keys(db, symbol):
    """Both provider writers share this lock and identity view during rollback."""
    if db.get_bind().dialect.name == 'postgresql':
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': 'company-news:' + symbol}):
            return None
    prior = list(db.scalars(select(Event).where(Event.symbol == symbol, Event.event_type == 'news_article').limit(10001)))
    if len(prior) > 10000:
        return None
    urls, titles = set(), set()
    for event in prior:
        try:
            old = json.loads(event.payload_json or '{}')
        except ValueError:
            old = {}
        url = canonical_news_url(event.source_document_url or old.get('url'))
        if url:
            urls.add(url)
        if old.get('title') and event.event_date:
            titles.add((_title(old['title']), _stamp(event.event_date).date()))
    return urls, titles


def sync_news_events(db, symbols, *, limit=20, now=None):
    if selected_news_provider() != 'finnhub' or os.getenv('FINNHUB_NEWS_PUBLICATION_ENABLED', '0') != '1':
        return 0
    try:
        since = date.fromisoformat(os.environ['NEWS_PUBLISH_SINCE'])
        activation = os.getenv('NEWS_PUBLISH_AFTER')
        after = datetime.fromisoformat(activation.replace('Z', '+00:00')) if activation is not None else None
        if after is not None and (after.tzinfo is None or after.utcoffset() is None):
            return 0
    except (KeyError, TypeError, ValueError):
        return 0
    now = now or datetime.now(timezone.utc)
    created = 0
    for symbol in sorted({s for raw in symbols if (s := normalize_symbol(raw))}):
        row = db.get(InsightsSnapshot, f'finnhub-news:company:{symbol}')
        if row is None or row.source != 'finnhub' or not timedelta(0) <= now - _stamp(row.fetched_at) <= timedelta(hours=24):
            continue
        try:
            payload = json.loads(row.payload_json)
            if payload.get('source') != 'finnhub' or not isinstance(payload.get('items'), list):
                continue
        except (ValueError, TypeError):
            continue
        # Cross-provider reconciliation preserves existing event IDs and dates.
        # Refuse unbounded scans rather than assuming a partial result is complete.
        keys = existing_news_keys(db, symbol)
        if keys is None:
            continue
        urls, titles = keys
        for item in payload['items'][:min(100, max(1, limit))]:
            try:
                url = canonical_news_url(item.get('url'))
                title = _title(item.get('title'))
                published, observed = _stamp(item.get('published_at')), _stamp(item.get('observed_at'))
                if (item.get('source') != 'finnhub' or item.get('symbol') != symbol or not url or not title
                        or not item.get('site') or published.date() < since
                        or (after is not None and published < after)
                        or not now - timedelta(days=7) <= published <= now
                        or not published <= observed <= now or now - observed > timedelta(hours=24)):
                    continue
            except (ValueError, TypeError, AttributeError):
                continue
            title_key = (title, published.date())
            if url in urls or title_key in titles:
                continue
            key = 'company-news:' + hashlib.sha256((symbol + '|' + url).encode()).hexdigest()
            if db.scalar(select(Event.id).where(Event.source_filing_id == key).limit(1)) is not None:
                continue
            content = {'content_event_key': key, 'symbol': symbol, 'title': item['title'], 'url': url,
                       'publisher': item['site'], 'provider': 'finnhub', 'data_category': 'news',
                       'published_at': published.isoformat(), 'first_observed_at': observed.isoformat(),
                       'summary': None, 'image_url': None, 'coverage': 'publisher_headline_link',
                       'can_confirm': False, 'direction': 'neutral', 'is_market_trade': False,
                       'source_availability': {'basis': 'direct_publication', 'date': observed.date().isoformat(), 'observed_at': observed.isoformat()}}
            db.add(Event(event_type='news_article', symbol=symbol, ts=observed, event_date=published,
                source='finnhub_news_article', source_provider='finnhub', data_source='finnhub',
                source_filing_id=key, source_document_url=url, impact_score=0, payload_json=json.dumps(content, sort_keys=True)))
            db.flush()
            urls.add(url); titles.add(title_key)
            created += 1
    return created
