"""Bounded, reviewed historical corrections with whole-population guards.

Explicit reviewed PostgreSQL entry points bind staged originals, the selected
source generation, and every affected canonical population. No automatic
caller, event/alert insertion, customer delivery, provider switch, or schedule.
"""
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import copy
import json
import os
import time

from sqlalchemy import DateTime, Date, Float, Text, and_, bindparam, or_, select, update
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base
from app.models import (InstitutionalFiling, InstitutionalPosition,
    InstitutionalPositionChange, InstitutionalSymbolSummary, InstitutionalActivityEvent, Event)
from app.services.direct_feed_rehearsal import digest, snapshot
from app.services.feed_source_control import lock_feed, require_selected_source
from app.services.institutional_source_reconciliation import plan_historical_positions
from app.services.institutional_history_guard import InstitutionalHistoricalScope, install_scopes, remove_scopes


MODELS = {m.__tablename__: m for m in (InstitutionalFiling, InstitutionalPosition,
    InstitutionalPositionChange, InstitutionalSymbolSummary, InstitutionalActivityEvent, Event)}
IMMUTABLE = {'id', 'cik', 'symbol', 'normalized_symbol', 'cusip', 'report_year', 'report_quarter',
             'filing_date', 'filing_id', 'provider_row_hash', 'put_call'}
EVENT_FIELDS = {'payload_json', 'member_name', 'impact_score', 'member_bioguide_id', 'trade_type',
                'transaction_type', 'amount_min', 'amount_max', 'data_source'}
DATE_KEYS = ('filing_date', 'filingDate', 'report_date', 'reportDate')
PROOF_KEYS = ('source_availability', 'sec_verification', 'identifier_reference_evidence', 'security_transition_evidence')
MAX_ROWS = 150_000
MAX_OPERATIONS = 15_000
MAX_BYTES = 30_000_000


class InstitutionalRepairReceipt(Base):
    __tablename__ = 'institutional_repair_receipts'
    plan_hash: Mapped[str] = mapped_column(primary_key=True)
    plan_json: Mapped[str] = mapped_column(Text)
    status: Mapped[str]
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


def canonical(model, row):
    """Normalize timestamp representations, never embedded evidence JSON."""
    result = snapshot(row) if not isinstance(row, dict) else copy.deepcopy(row)
    result.pop('created_at', None)
    result.pop('updated_at', None)
    for col in model.__table__.columns:
        value = result.get(col.name)
        if value is not None and isinstance(col.type, DateTime):
            parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
            result[col.name] = parsed.replace(tzinfo=timezone.utc).isoformat() if parsed.tzinfo is None else parsed.astimezone(timezone.utc).isoformat()
        elif value is not None and isinstance(col.type, Float):
            result[col.name] = float(value)
        elif isinstance(value, date):
            result[col.name] = value.isoformat()
    return result


def _ownership(plan):
    metadata = json.loads(plan['filing'].get('raw_metadata_json') or '{}')
    if any(k in metadata for k in ('_walnut_position_repair', '_walnut_direct_13f')):
        raise ValueError('Existing source receipt requires separate reconciliation')
    metadata.update(_walnut_position_source='sec_edgar_reconciled', _walnut_position_repair={
        'source_sha256': plan['proposal']['source_sha256'], 'plan_sha256': plan['sha256']})
    return json.dumps(metadata, sort_keys=True, default=str)


def _validate_event(before, after):
    left, right = json.loads(before['payload_json'] or '{}'), json.loads(after['payload_json'] or '{}')
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise ValueError('Invalid historical event evidence')
    if isinstance(left.get('sec_verification'), dict) and left['sec_verification'].get('feed') == 'sec_13f' and before != after:
        raise ValueError('Published SEC event evidence is immutable')
    for key in DATE_KEYS + PROOF_KEYS:
        if (key in left) != (key in right) or left.get(key) != right.get(key):
            raise ValueError('Historical effective date or provenance changed')


def build_bundle(before, after, source_plans, *, scopes, publish_since):
    """Package actual isolated-runtime output, not arbitrary proposed inserts."""
    if not isinstance(publish_since, date) or isinstance(publish_since, datetime):
        raise ValueError('Historical boundary required')
    if not 1 <= len(source_plans) <= 40 or not 1 <= len(scopes) <= 1000:
        raise ValueError('Source or summary scope exceeds reviewed bounds')
    if set(before) != set(MODELS) or set(after) != set(MODELS):
        raise ValueError('Complete canonical model populations required')
    if len({p['filing']['id'] for p in source_plans}) != len(source_plans):
        raise ValueError('Duplicate original filing')
    source_positions, source_filings = {}, {}
    for plan in source_plans:
        if plan['sha256'] != digest({k:v for k,v in plan.items() if k != 'sha256'}) or plan['publish_since'] != publish_since.isoformat():
            raise ValueError('Source plan or historical boundary changed')
        source_filings[plan['filing']['id']] = plan
        for op in plan['proposal']['operations']:
            if op['id'] in source_positions:
                raise ValueError('Duplicate source correction')
            source_positions[op['id']] = op
    guards, operations, counts = {}, [], Counter()
    for name, model in MODELS.items():
        left = sorted((canonical(model, row) for row in before[name]), key=lambda r:r['id'])
        right = sorted((canonical(model, row) for row in after[name]), key=lambda r:r['id'])
        if [r['id'] for r in left] != [r['id'] for r in right] or len({r['id'] for r in left}) != len(left):
            raise ValueError('Historical repair cannot insert, delete, or duplicate canonical identities')
        if name == 'institutional_filings':
            for row in right:
                if row['id'] in source_filings:
                    row['raw_metadata_json'] = _ownership(source_filings[row['id']])
        for old, new in zip(left, right, strict=True):
            if set(old) != set(new):
                raise ValueError('Canonical row shape changed')
            fields = {k:new[k] for k in old if old[k] != new[k]}
            if not fields:
                continue
            immutable = IMMUTABLE - ({'filing_date'} if name == 'institutional_activity_events' else set())
            if fields.keys() & immutable:
                raise ValueError('Historical identity or date changed')
            if name == 'institutional_positions':
                expected = source_positions.get(old['id'])
                if not expected or old != canonical(model, expected['before']) or new != canonical(model, expected['after']):
                    raise ValueError('Position operation differs from complete original')
            elif name == 'institutional_filings':
                if old['id'] not in source_filings or set(fields) != {'raw_metadata_json'} or old != canonical(model, source_filings[old['id']]['filing']):
                    raise ValueError('Filing operation is not source ownership protection')
            elif name == 'events':
                if not fields.keys() <= EVENT_FIELDS:
                    raise ValueError('Historical event identity, date, or source changed')
                _validate_event(old, new)
            elif name == 'institutional_position_changes':
                if not fields.keys() <= {'prev_value_usd', 'curr_value_usd', 'value_delta_usd', 'value_delta_pct'}:
                    raise ValueError('Position-change classification or shares changed')
            elif name == 'institutional_activity_events':
                # The complete runtime may refresh the derived activity date;
                # saved published event dates remain separately immutable.
                pass
            operations.append(dict(table=name, id=old['id'], before=old, after=new, changes=fields))
            counts[name] += 1
        guards[name] = dict(ids=[r['id'] for r in left], count=len(left), before=digest(left), after=digest(right))
    if counts['institutional_positions'] != len(source_positions) or counts['institutional_filings'] != len(source_filings):
        raise ValueError('Incomplete original correction or ownership coverage')
    if sum(g['count'] for g in guards.values()) > MAX_ROWS or not 1 <= len(operations) <= MAX_OPERATIONS:
        raise ValueError('Historical repair exceeds row bounds')
    bundle = dict(version=1, publish_since=publish_since.isoformat(), scopes=sorted([list(s) for s in scopes]),
        source_plans=source_plans, guards=guards, operations=operations,
        event_sources=sorted({r['source_filing_id'] for r in before['events']}))
    if len(json.dumps(bundle).encode()) > MAX_BYTES:
        raise ValueError('Historical repair exceeds byte bound')
    return {**bundle, 'sha256':digest(bundle)}


def verify_sources(bundle, documents, *, comparison_documents, prepared_evidence=None):
    if len(documents) != len(bundle['source_plans']):
        raise ValueError('Complete original document set required')
    by_hash = {d['content_hash']:d for d in documents}
    if len(by_hash) != len(documents):
        raise ValueError('Duplicate original source')
    for plan in bundle['source_plans']:
        doc = by_hash.get(plan['proposal']['source_sha256'])
        if not doc:
            raise ValueError('Reviewed original missing')
        rebuilt = plan_historical_positions(doc, plan['filing'], plan['positions'],
            publish_since=date.fromisoformat(bundle['publish_since']), comparison_documents=comparison_documents,
            prepared_evidence=prepared_evidence)
        if rebuilt != plan:
            raise ValueError('Whole original no longer reproduces reviewed correction')


def _staged_sources(db, bundle):
    from sqlalchemy import func
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    from app.services.direct_feed_worker import DirectFeedPublication
    from app.services.direct_13f_priors import _saved_source
    from app.clients.direct_sources import DirectSourceError
    accessions = [p['proposal']['accession'] for p in bundle['source_plans']]
    rows = list(db.scalars(select(DirectFeedDocument).where(
        DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.source_key.in_(accessions))))
    if len(rows) != len(accessions):
        raise ValueError('Complete reviewed staged originals are unavailable')
    by_key = {row.source_key:row for row in rows}
    if db.scalar(select(DirectFeedPublication.id).where(DirectFeedPublication.document_id.in_([r.id for r in rows])).limit(1)):
        raise ValueError('Already published original requires publication receipt reconciliation')
    lengths = list(db.execute(select(DirectFeedRevision.document_id, func.length(DirectFeedRevision.source_bytes)).join(
        DirectFeedDocument, (DirectFeedRevision.document_id == DirectFeedDocument.id) &
        (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash)).where(DirectFeedDocument.id.in_([r.id for r in rows]))))
    if len(lengths) != len(rows) or any(not size for _,size in lengths) or sum(size for _,size in lengths) > 20_000_000:
        raise ValueError('Staged original bytes missing or exceed the reviewed total bound')
    parsed_sources = []
    for plan in bundle['source_plans']:
        staged = by_key[plan['proposal']['accession']]
        if staged.status != 'parsed' or staged.content_hash != plan['proposal']['source_sha256']:
            raise ValueError('Staged original changed or is held')
        try:
            _, _, parsed, metadata = _saved_source(db, staged)
        except DirectSourceError as exc:
            raise ValueError(str(exc)) from exc
        if digest(metadata) != plan['source_metadata_sha256']:
            raise ValueError('Staged original metadata changed')
        parsed_sources.append(parsed)
    return parsed_sources


def inspect_staged_bundle(db, bundle, *, expected_plan_hash):
    """Read-only inspection; only compact counts and hashes should be emitted."""
    _validate_review(bundle, expected_plan_hash)
    if db.new or db.dirty or db.deleted:
        raise ValueError('Inspection has pending unrelated changes')
    _staged_sources(db, bundle)
    receipt = db.get(InstitutionalRepairReceipt, expected_plan_hash)
    if receipt and (receipt.plan_json != json.dumps(bundle, sort_keys=True) or receipt.status not in {'applied', 'rolled_back'}):
        raise ValueError('Repair receipt differs from reviewed operation bundle')
    state = 'after' if receipt and receipt.status == 'applied' else 'before'
    _check_population(db, bundle, state)
    return dict(status='existing' if state == 'after' else 'ready', plan_sha256=expected_plan_hash,
                originals=len(bundle['source_plans']), operations=len(bundle['operations']),
                guarded_rows=sum(g['count'] for g in bundle['guards'].values()), production_writes=0, emails=0)


def _population(db, bundle, name):
    model = MODELS[name]
    if name == 'institutional_filings':
        predicate = model.id.in_(bundle['guards'][name]['ids'])
    elif name == 'events':
        # Also guard previously unpublished activity: a later legacy refresh
        # must not silently add historical events with fresh ingestion dates.
        activity_rows = db.execute(select(InstitutionalActivityEvent.id, InstitutionalActivityEvent.event_type,
            InstitutionalActivityEvent.report_year, InstitutionalActivityEvent.report_quarter).where(
            InstitutionalActivityEvent.id.in_(bundle['guards']['institutional_activity_events']['ids'])))
        sources = set(bundle['event_sources']) | {
            f'institutional:{ident}:{kind}:{year}q{quarter}' for ident, kind, year, quarter in activity_rows}
        predicate = or_(model.id.in_(bundle['guards'][name]['ids']), model.source_filing_id.in_(sorted(sources)))
    else:
        # Tuple IN expands into hundreds of repeated period bitmap scans on
        # the real multi-period table. Factor the common period explicitly,
        # retaining the exact symbol/period pairs and complete filing guard.
        periods = defaultdict(set)
        for symbol, year, quarter in bundle['scopes']:
            periods[year, quarter].add(symbol)
        predicate = or_(*(and_(model.report_year == year, model.report_quarter == quarter,
            model.normalized_symbol.in_(sorted(symbols))) for (year, quarter), symbols in sorted(periods.items())))
        if name == 'institutional_positions':
            predicate = or_(predicate, model.filing_id.in_([p['filing']['id'] for p in bundle['source_plans']]))
    columns = [c for c in model.__table__.columns if c.name not in {'created_at', 'updated_at'}]
    rows = [canonical(model, dict(row)) for row in db.execute(
        select(*columns).where(predicate).order_by(model.id).limit(MAX_ROWS + 1)).mappings()]
    if len(rows) > MAX_ROWS:
        raise ValueError('Canonical population exceeds reviewed bounds')
    return rows


def _check_population(db, bundle, state):
    for name in MODELS:
        rows = _population(db, bundle, name)
        guard = bundle['guards'][name]
        if len(rows) != guard['count'] or [r['id'] for r in rows] != guard['ids'] or digest(rows) != guard[state]:
            raise ValueError('Historical repair population changed: ' + name)


def _typed(model, fields):
    result = dict(fields)
    for name, value in result.items():
        col = model.__table__.columns[name]
        if value is not None and isinstance(col.type, DateTime):
            result[name] = datetime.fromisoformat(value)
        elif value is not None and isinstance(col.type, Date):
            result[name] = date.fromisoformat(value)
    return result


def _validate_review(bundle, expected_plan_hash):
    if not expected_plan_hash or bundle['sha256'] != expected_plan_hash or digest({k:v for k,v in bundle.items() if k != 'sha256'}) != expected_plan_hash:
        raise ValueError('Reviewed historical operation bundle changed')
    if (bundle.get('version') != 1 or set(bundle['guards']) != set(MODELS)
            or not 1 <= len(bundle['source_plans']) <= 40 or not 1 <= len(bundle['scopes']) <= 1000
            or not 1 <= len(bundle['operations']) <= MAX_OPERATIONS
            or sum(g['count'] for g in bundle['guards'].values()) > MAX_ROWS
            or len(json.dumps(bundle).encode()) > MAX_BYTES):
        raise ValueError('Historical operation bundle exceeds bounds')


def rehearse_bundle(db, bundle, *, expected_plan_hash, rollback=False):
    """Explicit local-only transaction; caller must verify_sources beforehand.

    Hash approval binds the complete source and downstream runtime output.
    Production activation requires a separately reviewed staged-source wrapper.
    """
    bind = db.get_bind()
    local = (bind.dialect.name == 'sqlite' and bind.engine.url.database == ':memory:') or (
        bind.dialect.name == 'postgresql' and bind.engine.url.host in {'127.0.0.1', '::1'})
    if not local:
        raise ValueError('Bundle rehearsal requires a fresh isolated local session')
    return _transaction(db, bundle, expected_plan_hash=expected_plan_hash, rollback=rollback)


def apply_reviewed_bundle(db, bundle, *, expected_plan_hash, expected_generation, rollback=False):
    """One reviewed atomic repair, or guarded undo; no source activation."""
    if db.get_bind().dialect.name != 'postgresql' or type(expected_generation) is not int or expected_generation < 0:
        raise ValueError('Reviewed PostgreSQL source generation required')
    if os.getenv('DIRECT_13F_PUBLICATION_ENABLED', 'false').lower() not in {'false', '0'}:
        raise ValueError('Institutional publication must remain disabled during correction')
    return _transaction(db, bundle, expected_plan_hash=expected_plan_hash, rollback=rollback,
                        staged=True, expected_generation=expected_generation)


def _transaction(db, bundle, *, expected_plan_hash, rollback, staged=False, expected_generation=None):
    if db.in_transaction() or db.new or db.dirty or db.deleted:
        raise ValueError('Repair requires a fresh canonical transaction')
    _validate_review(bundle, expected_plan_hash)
    bind = db.get_bind()
    started = time.monotonic()
    with db.begin():
        lock_feed(db, 'sec_13f')
        control = require_selected_source(db, 'sec_13f', 'fmp')
        if staged and (control.generation if control else 0) != expected_generation:
            raise ValueError('Selected source generation changed since review')
        if bind.dialect.name == 'postgresql':
            from sqlalchemy import text
            db.execute(text("SET LOCAL lock_timeout = '100ms'"))
            db.execute(text("SET LOCAL statement_timeout = '20s'"))
            preparer = bind.dialect.identifier_preparer
            models = [*MODELS.values(), InstitutionalRepairReceipt, InstitutionalHistoricalScope]
            if staged:
                from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
                from app.services.direct_feed_worker import DirectFeedPublication
                models.extend((DirectFeedDocument, DirectFeedRevision, DirectFeedPublication))
            tables = ', '.join(preparer.format_table(m.__table__) for m in models)
            db.execute(text('LOCK TABLE ' + tables + ' IN SHARE ROW EXCLUSIVE MODE NOWAIT'))
        parsed_sources = _staged_sources(db, bundle) if staged else []
        receipt = db.get(InstitutionalRepairReceipt, expected_plan_hash)
        if receipt and (receipt.plan_json != json.dumps(bundle, sort_keys=True) or receipt.status not in {'applied', 'rolled_back'}):
            raise ValueError('Historical repair receipt differs from reviewed plan')
        if rollback and receipt is None:
            raise ValueError('Rollback requires an applied repair receipt')
        desired = 'rolled_back' if rollback else 'applied'
        if receipt and receipt.status == desired:
            _check_population(db, bundle, 'before' if rollback else 'after')
            added = 0 if rollback else install_scopes(db, bundle)
            removed = remove_scopes(db, expected_plan_hash) if rollback else 0
            if time.monotonic() - started > 45:
                raise ValueError('Historical repair exceeded the bounded transaction time')
            return dict(status='existing_' + desired, updated=0, history_guards_added=added,
                        history_guards_removed=removed, inserted_events=0, emails=0)
        _check_population(db, bundle, 'after' if rollback else 'before')
        if time.monotonic() - started > 45:
            raise ValueError('Historical repair exceeded the bounded transaction time')
        groups = {}
        for op in bundle['operations']:
            model = MODELS[op['table']]
            fields = {k:op['before'][k] for k in op['changes']} if rollback else op['changes']
            groups.setdefault((op['table'], tuple(sorted(fields))), []).append({'repair_id':op['id'], **_typed(model, fields)})
        connection = db.connection()
        for (name, fields), rows in groups.items():
            table = MODELS[name].__table__
            statement = update(table).where(table.c.id == bindparam('repair_id')).values({k:bindparam(k) for k in fields})
            for offset in range(0, len(rows), 500):
                connection.execute(statement, rows[offset:offset+500])
        db.expire_all()
        _check_population(db, bundle, 'before' if rollback else 'after')
        if staged and not rollback:
            from app.services.direct_feed_store import reconcile_institutional
            for parsed in parsed_sources:
                result = reconcile_institutional(db, parsed)
                if any(result.get(k) for k in ('unmatched', 'ambiguous', 'existing_only_ids')):
                    raise ValueError('Historical repair failed complete original reconciliation')
        if receipt is None:
            receipt = InstitutionalRepairReceipt(plan_hash=expected_plan_hash, plan_json=json.dumps(bundle, sort_keys=True), status=desired)
            db.add(receipt)
        else:
            receipt.status = desired
        db.flush()
        added = 0 if rollback else install_scopes(db, bundle)
        removed = remove_scopes(db, expected_plan_hash) if rollback else 0
        if staged:
            from app.services.feed_cache_epoch import bump_feed_events_epoch
            bump_feed_events_epoch(reason='reviewed_historical_13f_rollback' if rollback else 'reviewed_historical_13f_correction', db=db)
        if time.monotonic() - started > 45:
            raise ValueError('Historical repair exceeded the bounded transaction time')
    return dict(status=desired, updated=len(bundle['operations']), history_guards_added=added,
                history_guards_removed=removed, inserted_events=0, deleted_events=0, emails=0)
