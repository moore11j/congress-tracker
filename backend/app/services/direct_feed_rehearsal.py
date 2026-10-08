"""Source-bound correction plans and isolated canonical-table rehearsals.

No production apply entry point exists. Plans preserve IDs and provider hashes;
Existing events can be corrected in memory; portfolio weights, downstream
caches and alerts are not rebuilt.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal
import hashlib
import json
import math

from sqlalchemy import select

from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, InstitutionalFiling, InstitutionalPosition, MonitoringAlert
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import dumps, insider_identity, _insider_details, _number


MODELS = {model.__tablename__: model for model in (InsiderTransactionNormalized, InstitutionalPosition, Event, InsiderTransaction, MonitoringAlert)}
FIELDS = {'insider_transactions_normalized': {'price', 'value', 'is_derivative'},
          'institutional_positions': {'value_usd'},
          'events': {'payload_json', 'amount_min', 'amount_max', 'trade_type', 'source_provider',
                     'source_filing_id', 'source_document_url', 'parser_version'},
          'monitoring_alerts': {'payload_json', 'title', 'body'}}


def snapshot(row):
    # Server-managed timestamps are not source identity. All other fields,
    # including canonical IDs, provider hashes and mappings, guard stale plans.
    return json.loads(dumps({c.name: getattr(row, c.name) for c in row.__table__.columns
                             if c.name not in {'created_at', 'updated_at'}}))


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def _core(item):
    return insider_identity(item)[:5] + _insider_details(item)


def _operation(row, updates, source, refs):
    before = snapshot(row)
    changes = {field: value for field, value in updates.items() if before[field] != value}
    return {'table': row.__tablename__, 'id': row.id, 'source': source, 'source_rows': refs,
            'before': before, 'after': {**before, **changes}, 'changes': changes} if changes else None


def plan_corrections(db, documents):
    """Reparse raw bytes and allow only unique, source-proven existing matches.

    documents contain feed, metadata, raw and content_hash, never trusted parsed
    JSON. A source checksum or identity failure aborts planning that document.
    """
    operations, held, counts = [], [], Counter()
    seen = set()
    for document in documents:
        feed, metadata, raw = document['feed'], document['metadata'], document['raw']
        source = {'feed': feed, 'accession': metadata['key'], 'url': metadata['url'],
                  'sha256': hashlib.sha256(raw).hexdigest()}
        if source['sha256'] != document['content_hash']:
            raise ValueError('Source hash changed')
        key = (feed, metadata['key'])
        if key in seen:
            raise ValueError('Repeated source accession in plan')
        seen.add(key)
        try:
            _, parsed, reasons = parse_document(feed, raw, metadata)
        except Exception as exc:
            held.append({'source': source, 'reason': str(exc)})
            continue
        if reasons:
            held.append({'source': source, 'reason': '; '.join(reasons)})
            continue
        if feed == 'sec_form4':
            existing = list(db.scalars(select(InsiderTransactionNormalized).where(
                InsiderTransactionNormalized.accession_number == metadata['key'],
                InsiderTransactionNormalized.is_duplicate.is_(False))))
            candidates, reverse = {}, Counter()
            for index, item in enumerate(parsed['transactions'], 1):
                matches = [row for row in existing if _core(row) == _core(item)]
                candidates[index] = matches
                reverse.update(row.id for row in matches)
            used = set()
            for index, item in enumerate(parsed['transactions'], 1):
                matches = candidates[index]
                if len(matches) != 1 or reverse[matches[0].id] != 1:
                    held.append({'source': source, 'row': index,
                                 'reason': 'No unique one-to-one existing transaction',
                                 'candidate_ids': [row.id for row in matches]})
                    continue
                row = matches[0]
                used.add(row.id)
                # Both original and derivative prices come directly from the
                # transaction price element, never the option exercise price.
                price, shares = item['price'], item['shares']
                if (shares is None or not math.isfinite(shares) or shares < 0 or
                        (price is not None and (not math.isfinite(price) or price < 0))):
                    held.append({'source': source, 'row': index, 'reason': 'Invalid economic value'})
                    continue
                value = shares * price if price is not None else None
                if value is not None and not math.isfinite(value):
                    raise ValueError('Non-finite transaction value')
                op = _operation(row, {'price': price, 'value': value,
                                      'is_derivative': item['is_derivative']}, source, [str(index)])
                if op:
                    operations.append(op)
                else:
                    counts['unchanged_insider_rows'] += 1
            leftover = [row.id for row in existing if row.id not in used]
            if leftover:
                held.append({'source': source, 'reason': 'Existing rows lack a unique source match',
                             'existing_ids': leftover})
        elif feed == 'sec_13f':
            filing = db.scalar(select(InstitutionalFiling).where(
                InstitutionalFiling.accession_number == metadata['key']))
            meta = parsed['metadata']
            if (filing is None or filing.is_amendment or filing.superseded_by is not None or
                    filing.cik.lstrip('0') != meta['cik'].lstrip('0') or
                    str(filing.filing_date) != meta['filing_date'] or
                    str(filing.report_period_end) != meta['report_period']):
                held.append({'source': source, 'reason': 'No matching active original filing'})
                continue
            groups = defaultdict(list)
            for item in parsed['positions']:
                groups[(item['cusip'].upper(), (item.get('putCall') or '').upper())].append(item)
            existing = defaultdict(list)
            for row in db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)):
                existing[((row.cusip or '').upper(), (row.put_call or '').upper())].append(row)
            pending = []
            if groups.keys() != existing.keys():
                held.append({'source': source, 'reason': 'Filing security coverage differs'})
                continue
            for security, items in groups.items():
                rows = existing[security]
                shares = sum(Decimal(str(item['shares'])) for item in items)
                value = sum(Decimal(str(item['valueUsd'])) for item in items)
                if len(rows) != 1 or _number(rows[0].shares) != _number(shares):
                    held.append({'source': source, 'reason': 'Ambiguous position identity/shares', 'security': security})
                    break
                op = _operation(rows[0], {'value_usd': float(value)}, source,
                                [item['source_line_ref'] for item in items])
                if op:
                    # This bounded rehearsal corrects the observed unit defect;
                    # unrelated valuation differences remain explicit holds.
                    if rows[0].value_usd is None or Decimal(str(rows[0].value_usd)) * 1000 != value:
                        held.append({'source': source, 'reason': 'Value discrepancy is not the verified unit defect'})
                        break
                    pending.append(op)
            else:
                operations.extend(pending)
                counts['unchanged_position_rows'] += len(existing) - len(pending)
    targets = [(op['table'], op['id']) for op in operations]
    if len(targets) != len(set(targets)):
        raise ValueError('Multiple sources propose changing the same canonical row')
    guards = []
    selectors = set()
    for op in operations:
        if op['table'] == 'insider_transactions_normalized':
            selectors.add((op['table'], 'accession_number', op['source']['accession']))
        else:
            selectors.add((op['table'], 'filing_id', op['before']['filing_id']))
            selectors.add(('institutional_filings', 'id', op['before']['filing_id']))
    for table, field, value in sorted(selectors):
        model = InstitutionalFiling if table == 'institutional_filings' else MODELS[table]
        rows = db.scalars(select(model).where(getattr(model, field) == value).order_by(model.id)).all()
        guards.append({'table': table, 'field': field, 'value': value, 'rows': [snapshot(row) for row in rows]})
    plan = {'version': 1, 'operations': operations, 'guards': guards, 'held': held, 'counts': dict(counts),
            'scope': 'isolated canonical-table corrections; no events, alerts or provider cutover'}
    return {**plan, 'sha256': digest(plan)}


def apply_rehearsal(db, plan):
    """Apply to an in-memory SQLite baseline only, with all-or-nothing checks."""
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Rehearsal requires an in-memory SQLite database')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Rehearsal session has unrelated pending changes')
    if plan['sha256'] != digest({k: v for k, v in plan.items() if k != 'sha256'}):
        raise ValueError('Plan hash changed')
    expected_after = {(op['table'], op['id']): op['after'] for op in plan['operations']}
    for guard in plan['guards']:
        model = InstitutionalFiling if guard['table'] == 'institutional_filings' else MODELS[guard['table']]
        observed = list(db.scalars(select(model).where(getattr(model, guard['field']) == guard['value']).order_by(model.id)))
        if [row.id for row in observed] != [row['id'] for row in guard['rows']]:
            raise ValueError('Canonical filing population changed since planning')
        for row, before in zip(observed, guard['rows']):
            after = expected_after.get((guard['table'], row.id), before)
            if snapshot(row) not in (before, after):
                raise ValueError('Canonical filing state changed since planning')
    pending, skipped, targets = [], 0, set()
    for op in plan['operations']:
        target = (op['table'], op['id'])
        if target in targets:
            raise ValueError('Duplicate canonical target')
        targets.add(target)
        if op['table'] not in FIELDS or not set(op['changes']).issubset(FIELDS[op['table']]):
            raise ValueError('Unsupported correction fields')
        if op['after'] != {**op['before'], **op['changes']} or op['before']['id'] != op['id']:
            raise ValueError('Invalid correction snapshot')
        row = db.get(MODELS[op['table']], op['id'])
        if row is None:
            raise ValueError('Canonical row disappeared')
        current = snapshot(row)
        if current == op['after']:
            skipped += 1
        elif current == op['before']:
            pending.append((row, op))
        else:
            raise ValueError('Canonical row changed since planning')
    with db.begin_nested():
        for row, op in pending:
            for field, value in op['changes'].items():
                setattr(row, field, value)
        db.flush()
    return {'updated': len(pending), 'skipped': skipped, 'inserted': 0, 'deleted': 0,
            'plan_sha256': plan['sha256'], 'production_writes': 0}


def include_event_corrections(db, canonical_plan):
    """Follow immutable provider IDs into existing events; never insert events.

    Raw provider rows/payloads are retained as evidence. SEC values and provenance
    become explicit top-level projections without changing historical event dates.
    """
    from app.backfill_legacy_insider_normalized import _build_normalized_payload
    if canonical_plan['sha256'] != digest({k: v for k, v in canonical_plan.items() if k != 'sha256'}):
        raise ValueError('Canonical plan hash changed')
    plan = json.loads(dumps(canonical_plan))
    by_hash = defaultdict(list)
    raw_rows = list(db.scalars(select(InsiderTransaction).order_by(InsiderTransaction.id)))
    for row in raw_rows:
        _, normalized = _build_normalized_payload(row)
        by_hash[normalized['normalized_hash']].append(row)
    events = list(db.scalars(select(Event).where(Event.event_type == 'insider_trade').order_by(Event.id)))
    by_external = defaultdict(list)
    for event in events:
        payload = json.loads(event.payload_json)
        by_external[payload.get('external_id')].append(event)
    event_operations, used = [], set()
    for op in canonical_plan['operations']:
        if op['table'] != 'insider_transactions_normalized':
            continue
        after = op['after']
        raw_matches = by_hash[after['normalized_hash']]
        matching_events = by_external[raw_matches[0].external_id] if len(raw_matches) == 1 else []
        if len(raw_matches) != 1 or len(matching_events) != 1 or matching_events[0].id in used:
            plan['held'].append({'source': op['source'], 'normalized_id': op['id'],
                                 'reason': 'No unique raw-provider-to-event identity'})
            continue
        raw, event = raw_matches[0], matching_events[0]
        payload = json.loads(event.payload_json)
        if (event.symbol != after['ticker_normalized'] or
                payload.get('transaction_date') != after['transaction_date'] or
                payload.get('filing_date') != after['filing_date']):
            plan['held'].append({'source': op['source'], 'event_id': event.id,
                                 'reason': 'Existing event symbol/dates conflict with SEC identity'})
            continue
        market = after['transaction_code'] in {'P', 'S'} and not after['is_derivative']
        side = ('purchase' if after['transaction_code'] == 'P' else 'sale') if market else None
        projected = {**payload, 'price': after['price'], 'value': after['value'],
                     'transaction_code': after['transaction_code'],
                     'is_derivative': after['is_derivative'], 'is_market_trade': market,
                     'trade_type_canonical': side, 'accession_number': after['accession_number'],
                     'normalized_transaction_id': after['id'],
                     'sec_verification': {**op['source'], 'source_rows': op['source_rows'],
                                          'raw_provider_id': raw.id}}
        amount = int(round(after['value'])) if after['value'] is not None else None
        event_op = _operation(event, {'payload_json': dumps(projected), 'amount_min': amount,
                                     'amount_max': amount, 'trade_type': side,
                                     'source_provider': 'sec_edgar',
                                     'source_filing_id': event.source_filing_id or after['normalized_hash'],
                                     'source_document_url': op['source']['url'],
                                     'parser_version': 'direct_sec_event_rehearsal_v1'},
                              op['source'], op['source_rows'])
        used.add(event.id)
        if event_op:
            event_operations.append(event_op)
    if event_operations:
        plan['guards'].extend([
            {'table': 'events', 'field': 'event_type', 'value': 'insider_trade',
             'rows': [snapshot(row) for row in events]},
        ])
        # Guard every input that established the legacy hash, including provider
        # payloads. Changing a raw record invalidates the linked-event plan.
        for raw in raw_rows:
            plan['guards'].append({'table': 'insider_transactions', 'field': 'id', 'value': raw.id,
                                   'rows': [snapshot(raw)]})
    plan['operations'].extend(event_operations)
    plan['counts']['event_corrections'] = len(event_operations)
    plan['scope'] = 'isolated canonical and existing-event corrections; no new events, alerts or provider cutover'
    plan.pop('sha256')
    return {**plan, 'sha256': digest(plan)}


def include_monitoring_corrections(db, event_plan):
    """Rehearse copies of corrected events without touching delivery/read state."""
    from app.services.monitoring_alerts import _event_title, _event_body, _redact_premium_signal_payload
    if event_plan['sha256'] != digest({k: v for k, v in event_plan.items() if k != 'sha256'}):
        raise ValueError('Event plan hash changed')
    plan = json.loads(dumps(event_plan))
    count = 0
    for op in event_plan['operations']:
        if op['table'] != 'events':
            continue
        event = Event(**{k: v for k, v in op['after'].items() if k not in {'ts', 'event_date'}})
        payload = json.loads(event.payload_json)
        alerts = list(db.scalars(select(MonitoringAlert).where(MonitoringAlert.event_id == event.id).order_by(MonitoringAlert.id)))
        plan['guards'].append({'table': 'monitoring_alerts', 'field': 'event_id', 'value': event.id,
                               'rows': [snapshot(row) for row in alerts]})
        for alert in alerts:
            wrapper = json.loads(alert.payload_json or '{}')
            # SEC evidence cannot promote an old free alert to paid context.
            previous = wrapper.get('event') or {}
            projected = {**_redact_premium_signal_payload(payload), **{k: previous[k] for k in previous
                         if k not in _redact_premium_signal_payload(previous)}}
            alert_op = _operation(alert, {'payload_json': dumps({**wrapper, 'event': projected}),
                                         'title': _event_title(event, projected), 'body': _event_body(event, projected)},
                                  op['source'], op['source_rows'])
            if alert_op:
                plan['operations'].append(alert_op)
                count += 1
    plan['counts']['monitoring_corrections'] = count
    plan.pop('sha256')
    return {**plan, 'sha256': digest(plan)}
