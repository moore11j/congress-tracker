"""No-send, isolated replay of captured SEC releases through alert builders."""
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from unittest.mock import patch
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, Security, UserAccount, Watchlist, WatchlistItem, NotificationSubscription, MonitoringAlert, EmailDelivery, ResearchSourceDocument, ResearchEvidenceEvent
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import select_feed_source
from app.services.sec_earnings_store import stage_earnings_material
from app.services.sec_press_events import FEED, publish_sec_release
from app.services.monitoring_alerts import _ensure_alert_for_event
from app.services import email_digests as digests
from app.services.email_intraday import _watchlist_candidate, _intraday_key


def forbidden(*args, **kwargs):
    raise AssertionError('Transport or delivery attempted during saved-source replay')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sources', type=Path, required=True)
    parser.add_argument('--publish-since', type=date.fromisoformat, required=True)
    parser.add_argument('--expect-published', nargs='+', help='Independent expected symbols; fail on zero/partial publication')
    parser.add_argument('--prepare-research', action='store_true', help='Also bind canonical research documents; no model calls')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = json.loads(args.sources.read_text())['sources']
    bundles = []
    for row in rows:
        symbol = row['symbol']
        bundle = {}
        for key, suffix, hash_key in [('company_raw', '-company.json', 'company_sha256'),
                                      ('submission_raw', '.txt', 'source_sha256'),
                                      ('index_raw', '-index.html', 'index_sha256')]:
            raw = (args.sources.parent / f'{symbol}{suffix}').read_bytes()
            if hashlib.sha256(raw).hexdigest() != row[hash_key]:
                raise ValueError('Captured source hash mismatch')
            bundle[key] = raw
        bundles.append({**bundle, 'symbol': symbol, 'cik': row['cik'],
                        'accession': row['filing']['accession_number']})
    os.environ['FMP_PROVIDER_DISABLED'] = '1'
    os.environ['PRESS_RELEASE_PROVIDER'] = 'sec_edgar'
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with patch('requests.sessions.Session.request', forbidden), patch('httpx.Client.send', forbidden), \
         patch.object(digests, '_send_digest', forbidden), \
         patch.object(digests, '_upcoming_calendar_events_for_digest', return_value=([], 'Outside isolated replay', None)), \
         Session(engine) as db:
        user = UserAccount(email='sec-replay@example.test', first_name='Replay', entitlement_tier='premium', watchlist_activity_notifications=True)
        db.add(user); db.flush()
        watchlist = Watchlist(name='SEC release replay', owner_user_id=user.id)
        db.add(watchlist); db.flush()
        for row in rows:
            security = Security(symbol=row['symbol'], name=row['symbol'], asset_class='stock')
            db.add(security); db.flush()
            db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type='ticker'))
        db.add(NotificationSubscription(email=user.email, source_type='watchlist', source_id=str(watchlist.id),
            source_name=watchlist.name, frequency='daily', only_if_new=True, active=True,
            source_payload_json='{}', alert_triggers_json='["press_releases"]'))
        db.commit()
        select_feed_source(db, feed=FEED, provider='sec_edgar', publish_since=args.publish_since,
                           expected_generation=0, reason='isolated saved-source replay')
        db.commit()
        passes = []
        for cycle in range(2):
            results = {}
            for bundle in bundles:
                document = stage_earnings_material(db, **bundle); db.commit()
                results[bundle['symbol']] = publish_sec_release(db, document.id)
                db.commit()
                if args.prepare_research:
                    from app.services.sec_press_research import prepare_sec_research_document
                    security_id = db.scalar(select(Security.id).where(Security.symbol == bundle['symbol']))
                    research = prepare_sec_research_document(db, security_id=security_id, accession=bundle['accession'])
                    db.commit()
                    results[bundle['symbol']]['research_status'] = research['status']
            events = list(db.scalars(select(Event).order_by(Event.id)))
            if args.expect_published is not None and {event.symbol for event in events} != set(args.expect_published):
                raise AssertionError('Published symbols differ from expected source coverage')
            for event in events:
                _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
            db.commit()
            start = datetime.combine(datetime.now(timezone.utc).date(), datetime.min.time(), tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            monitor = digests.build_monitoring_digest(db, user, watchlist, start, window_end=end)
            daily = digests.build_signal_alert_digest(db, user, start, window_end=end)
            activity = digests.build_watchlist_activity_digest(db, user, watchlist, start)
            if not monitor.items_count == daily.items_count == activity.items_count == len(events):
                raise AssertionError('Digest event count differs')
            candidates = [_watchlist_candidate(db, user, watchlist, event) for event in events]
            if any(c.skip_reason or not c.context['event_date'].startswith('Filed ') for c in candidates):
                raise AssertionError('Intraday eligibility or source date changed')
            counts = {model.__tablename__: db.scalar(select(func.count()).select_from(model)) for model in (
                DirectFeedDocument, DirectFeedRevision, DirectFeedPublication, Event, MonitoringAlert, EmailDelivery,
                ResearchSourceDocument, ResearchEvidenceEvent)}
            if counts['email_deliveries']:
                raise AssertionError('Unexpected delivery record')
            research_documents = list(db.scalars(select(ResearchSourceDocument).order_by(ResearchSourceDocument.external_id)))
            if args.prepare_research and len(research_documents) != len(events):
                raise AssertionError('Canonical research coverage differs from published releases')
            state = {'events': [(event.id, event.source_filing_id, event.ts, event.event_date, event.payload_json) for event in events],
                     'research_documents': [(r.id, r.external_id, r.source_provider, r.content_hash, r.source_url, r.published_at) for r in research_documents],
                     'delivery_keys': [_intraday_key(c) for c in candidates], 'counts': counts,
                     'monitoring_html': monitor.context['items_html'], 'daily_text': daily.context['press_releases_text'],
                     'activity_text': activity.context['items_text']}
            passes.append({'results': results, 'counts': counts, 'digest_items': monitor.items_count,
                           'state_sha256': hashlib.sha256(dumps(state).encode()).hexdigest()})
        if passes[0]['state_sha256'] != passes[1]['state_sha256']:
            raise AssertionError('Repeat changed events, alerts, delivery identity or digest content')
        report = {'replayed_at': datetime.now(timezone.utc).isoformat(), 'passes': passes,
                  'repeat_unchanged': True, 'network_requests': 0, 'emails_sent': 0,
                  'expected_published': args.expect_published,
                  'research_prepared': args.prepare_research, 'model_calls': 0,
                  'event_evidence': [{'symbol': event.symbol, 'filing_id': event.source_filing_id,
                      'first_observed_at': json.loads(event.payload_json)['first_observed_at'],
                      'sec_accepted_at': json.loads(event.payload_json)['sec_accepted_at'],
                      'acceptance_evidence': json.loads(event.payload_json)['acceptance_evidence']}
                      for event in events],
                  'limitation': 'Isolated empty legacy baseline; no production parity or scheduled reliability claim.'}
        args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + '\n')
        print(json.dumps(report))
    engine.dispose()


if __name__ == '__main__':
    main()
