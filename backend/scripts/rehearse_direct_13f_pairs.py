"""Offline rehearsal of captured real SEC pairs and identifier evidence."""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sources', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--comparison-staging', type=Path, required=True)
    parser.add_argument('--identifier-manifest', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--extra-sources', type=Path, action='append', default=[])
    parser.add_argument('--guarded-worker', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2] / 'artifacts' / 'direct-feeds'
    folder = args.sources.parent.resolve()
    assert folder.is_relative_to(root.resolve())
    os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
    from sqlalchemy import select, func
    from app.db import Base, engine, SessionLocal
    from app.models import (InstitutionalFiling, InstitutionalPosition, InstitutionalHolder,
        InstitutionalPositionChange, InstitutionalSymbolSummary, InstitutionalActivityEvent, Event, EmailDelivery)
    from app.services.direct_13f_publication import rehearse_new_13f
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_feed_store import dumps, discover, record_document
    from app.services.direct_13f_worker import publish_13f_document
    from app.services.feed_source_control import select_feed_source
    from scripts.compare_direct_sec_baseline import load_baseline
    Base.metadata.create_all(engine)
    manifest = json.loads(args.sources.read_bytes())
    documents = []
    for row in manifest['documents']:
        path = (folder / row['source_file']).resolve()
        assert path.parent == folder
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == row['content_hash']
        history = (folder / row['history_file']).read_bytes()
        assert hashlib.sha256(history).hexdigest() == row['history_sha256']
        documents.append({**row, 'raw': raw})
    for extra_path in args.extra_sources:
        extra_folder = extra_path.parent.resolve()
        assert extra_folder.is_relative_to(root.resolve())
        for row in json.loads(extra_path.read_bytes())['documents']:
            source_path = (extra_folder / row['source_file']).resolve()
            history_path = (extra_folder / row['history_file']).resolve()
            assert source_path.parent == history_path.parent == extra_folder
            raw = source_path.read_bytes()
            assert hashlib.sha256(raw).hexdigest() == row['content_hash']
            assert hashlib.sha256(history_path.read_bytes()).hexdigest() == row['history_sha256']
            documents.append({**row, 'raw': raw})
    identifiers = []
    for cik, accession in [('1592900', '0001592900-26-002759'), ('1650149', '0000894189-26-009017'),
                           ('811030', '0001193125-26-287933')]:
        raw = (folder / (accession + '.source')).read_bytes()
        identifiers.append({'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest(),
            'url': f'https://www.sec.gov/Archives/edgar/data/{cik}/{accession}.txt'})
    if args.identifier_manifest:
        extra = json.loads(args.identifier_manifest.read_bytes())
        for item in extra['documents']:
            path = (args.identifier_manifest.parent / item['source_file']).resolve()
            assert path.parent == args.identifier_manifest.parent.resolve()
            raw = path.read_bytes()
            assert hashlib.sha256(raw).hexdigest() == item['content_hash']
            identifiers.append({'raw': raw, 'content_hash': item['content_hash'], 'url': item['url']})
    comparisons = []
    with sqlite3.connect(args.comparison_staging.resolve().as_uri() + '?mode=ro', uri=True) as saved:
        for meta, digest in saved.execute("SELECT metadata_json,content_hash FROM direct_feed_documents WHERE feed='sec_13f'"):
            raw = (args.comparison_staging.parent / (digest + '.source')).read_bytes()
            comparisons.append({'feed': 'sec_13f', 'metadata': json.loads(meta), 'raw': raw, 'content_hash': digest})
    # Compare share counts independently of ticker mapping and market prices.
    pairs = defaultdict(dict)
    for document in documents:
        _, parsed, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
        assert not reasons
        totals = defaultdict(Decimal)
        for row in parsed['positions']:
            if not row.get('putCall') and row.get('shareType') == 'SH':
                totals[row['cusip']] += Decimal(str(row['shares']))
        pairs[parsed['metadata']['cik']][parsed['metadata']['report_quarter']] = totals
    share_changes = {}
    for cik, periods in pairs.items():
        prior, current = periods[2], periods[3]
        share_changes[cik] = {cusip: str(current[cusip] - prior[cusip]) for cusip in sorted(prior.keys() | current.keys())}
    baseline = json.loads(args.baseline.read_bytes())
    assert baseline['transaction_read_only']
    models = (InstitutionalFiling, InstitutionalPosition, InstitutionalHolder, InstitutionalPositionChange,
              InstitutionalSymbolSummary, InstitutionalActivityEvent, Event, EmailDelivery)
    with patch('socket.socket.connect', side_effect=AssertionError('Offline network attempt')), \
            patch('app.services.email_digests._send_digest', side_effect=AssertionError('No-send rehearsal')), SessionLocal() as db:
        load_baseline(db, baseline)
        staged_ids = {}
        if args.guarded_worker:
            for document in documents:
                source, parsed, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
                staged = discover(db, 'sec_13f', document['metadata'])
                record_document(db, staged, document['raw'], source, parsed, reasons=reasons)
                staged_ids[document['metadata']['key']] = staged.id
            db.commit()
            select_feed_source(db, feed='sec_13f', provider='sec_edgar', publish_since=date(2026,10,6),
                expected_generation=0, reason='In-memory real-pair worker rehearsal')
            db.commit()
        def run():
            receipts = []
            for document in sorted(documents, key=lambda d: (d['metadata']['filing_date'], d['metadata']['key'])):
                if args.guarded_worker:
                    result = publish_13f_document(db, staged_ids[document['metadata']['key']],
                        identifier_documents=identifiers, comparison_documents=comparisons)
                else:
                    result = rehearse_new_13f(db, document, publish_since=date(2026, 10, 6),
                        identifier_documents=identifiers, comparison_documents=comparisons)
                db.commit()
                receipts.append({'accession': document['metadata']['key'], 'cik': document['metadata']['cik'], **result})
            return receipts
        def fingerprint():
            state = {table.name: sorted([dict(row._mapping) for row in db.execute(select(table))], key=dumps)
                     for table in Base.metadata.sorted_tables}
            return hashlib.sha256(dumps(state).encode()).hexdigest()
        first = run()
        # Real source-derived events only; never lower materiality thresholds.
        from app.models import UserAccount, Watchlist, Security, WatchlistItem, MonitoringAlert
        from app.services.monitoring_alerts import _ensure_alert_for_event
        from app.services import email_digests as digests
        qualifying = [e for e in db.scalars(select(Event)) if json.loads(e.payload_json or '{}').get('sec_verification', {}).get('feed') == 'sec_13f']
        previews, alert_ids = [], []
        if qualifying:
            user = UserAccount(email='real-13f-preview@example.test', entitlement_tier='pro', watchlist_activity_notifications=True)
            restricted = UserAccount(email='real-13f-premium@example.test', entitlement_tier='premium')
            db.add_all([user, restricted]); db.flush()
            watchlist = Watchlist(name='Synthetic real 13F preview', owner_user_id=user.id)
            db.add(watchlist); db.flush()
            for symbol in {e.symbol for e in qualifying}:
                security = Security(symbol=symbol, name=symbol, asset_class='stock')
                db.add(security); db.flush()
                db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
            for event in qualifying:
                assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
                assert not _ensure_alert_for_event(db, user_id=restricted.id, watchlist=watchlist, event=event)
            db.commit()
            alert_ids = list(db.scalars(select(MonitoringAlert.id)))
            def build_previews():
                since, end = datetime(2026,10,5,tzinfo=timezone.utc), datetime(2026,10,8,23,tzinfo=timezone.utc)
                with patch.object(digests, '_upcoming_calendar_events_for_digest', return_value=([], 'Calendar outside 13F validation')):
                    builds = [digests.build_monitoring_digest(db, user, watchlist, since, window_end=end),
                              digests.build_signal_alert_digest(db, user, since, window_end=end),
                              digests.build_watchlist_activity_digest(db, user, watchlist, since)]
                assert all(b.items_count > 0 for b in builds)
                return [{'items_count': b.items_count, 'context': b.context} for b in builds]
            previews = build_previews()
        after = fingerprint()
        repeat = run()
        assert fingerprint() == after
        assert not any(r['inserted_filings'] or r['inserted_positions'] or r['feed_events'] for r in repeat)
        assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        if qualifying:
            for event in qualifying:
                assert not _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
            db.commit()
            assert previews == build_previews()
            assert fingerprint() == after
        tetrad = list(db.scalars(select(InstitutionalPositionChange).where(InstitutionalPositionChange.cik == '0000883597')))
        assert len(tetrad) == 3 and all(row.change_type == 'unchanged' and row.shares_delta == 0 for row in tetrad)
        samson = list(db.scalars(select(InstitutionalPositionChange).where(InstitutionalPositionChange.cik == '0001788558')))
        real_changes = []
        for row in samson:
            assert Decimal(str(row.shares_delta)) == Decimal(share_changes['0001788558'][row.cusip])
            real_changes.append({'symbol': row.normalized_symbol, 'cusip': row.cusip,
                                 'change_type': row.change_type, 'shares_delta': row.shares_delta})
        report = {'source_documents': len(documents), 'source_rows': sum(d['rows'] for d in documents),
            'states': dict(Counter(r.get('derived_state', r['status']) for r in first)),
            'totals': {key: sum(r.get(key, 0) for r in first) for key in
                ('inserted_filings', 'inserted_positions', 'changes', 'summaries', 'activity_events', 'feed_events')},
            'share_changes_by_cik': share_changes, 'publications': first,
            'samson_canonical_changes': real_changes,
            'changed_position_diagnostics': [{c.name: getattr(row, c.name) for c in row.__table__.columns
                if c.name not in {'created_at', 'updated_at'}} for row in db.scalars(
                    select(InstitutionalPositionChange).where(InstitutionalPositionChange.change_type != 'unchanged',
                        InstitutionalPositionChange.cik.in_(list(share_changes))))],
            'guarded_worker': args.guarded_worker, 'synthetic_alerts': len(alert_ids),
            'qualifying_events': [{'symbol': e.symbol, 'event_type': e.event_type, 'payload': json.loads(e.payload_json)} for e in qualifying],
            'no_send_previews': previews,
            'unmapped_positions': [{'cik': row.cik, 'cusip': row.cusip, 'issuer': row.issuer_name}
                for row in db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.normalized_symbol.is_(None)))],
            'tetrad_unchanged_share_records': len(tetrad), 'repeat_state_identical': True,
            'state_sha256': after, 'email_deliveries': 0, 'production_writes': 0, 'cutover_ready': False}
        output = args.output.resolve() if args.output else folder / 'pair-rehearsal.json'
        assert output.is_relative_to(root.resolve())
        output.write_text(dumps(report), encoding='utf-8')
        print(dumps({k: v for k, v in report.items() if k not in {'publications', 'share_changes_by_cik', 'unmapped_positions', 'qualifying_events', 'no_send_previews'}}))


if __name__ == '__main__':
    main()
