from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Event, InsightsSnapshot
from app.services.replacement_news_events import sync_news_events


@pytest.mark.parametrize('boundary,expected', [
    ('2026-10-09T21:00:00Z', 2),
    ('2026-10-09T14:00:00-07:00', 2),
    ('2026-10-09T21:30:00+00:00', 1),
    ('2026-10-10T00:00:00Z', 0),
    ('2026-10-09T21:00:00', 0),
    ('not-a-time', 0),
    ('', 0),
])
def test_activation_excludes_same_day_backlog_and_keeps_repeat_identity(monkeypatch, boundary, expected):
    now = datetime(2026, 10, 9, 22, tzinfo=timezone.utc)
    monkeypatch.setenv('NEWS_PROVIDER', 'finnhub')
    monkeypatch.setenv('FINNHUB_NEWS_PUBLICATION_ENABLED', '1')
    monkeypatch.setenv('NEWS_PUBLISH_SINCE', '2026-10-09')
    monkeypatch.setenv('NEWS_PUBLISH_AFTER', boundary)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[Event.__table__, InsightsSnapshot.__table__])
    items = [{'source': 'finnhub', 'symbol': 'ABC', 'site': 'Issuer',
        'title': 'Report '+str(i), 'url': 'https://example.com/report/'+str(i),
        'published_at': stamp.isoformat(), 'observed_at': now.isoformat()}
        for i, stamp in enumerate([now-timedelta(hours=2), now-timedelta(hours=1), now-timedelta(minutes=10)])]
    with Session(engine) as db:
        db.add(InsightsSnapshot(kind='finnhub-news:company:ABC', source='finnhub', fetched_at=now,
            payload_json=json.dumps({'source':'finnhub', 'items':items})))
        db.commit()
        assert sync_news_events(db, ['ABC'], now=now) == expected
        db.commit()
        first = [(row.id, row.event_date, row.ts, row.payload_json) for row in db.scalars(select(Event))]
        assert sync_news_events(db, ['ABC'], now=now) == 0
        assert first == [(row.id, row.event_date, row.ts, row.payload_json) for row in db.scalars(select(Event))]
    engine.dispose()
