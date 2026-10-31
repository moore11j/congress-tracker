"""Atomic worker-only discovery checkpoints, disabled until explicitly wired.

Only small filing-cover receipts are stored. No raw originals, financial values,
canonical events, scores or deliveries are written by this service.
"""
from datetime import datetime,timezone
import json
import os
import re
import time
from sqlalchemy import update,text
from sqlalchemy.exc import IntegrityError
from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.services import sec_financial_discovery as discovery
from app.utils.symbols import canonical_symbol


QUALIFICATION_BATCH_LIMIT = 3
QUALIFICATION_CONTINUE_SECONDS = 5
QUALIFICATION_CONTINUE_CPU_SECONDS = 2


class _QualificationBudget(discovery.DiscoveryError):
    pass


class DiscoveryConflict(discovery.DiscoveryError):
    pass


def enabled():
    return os.getenv('SEC_FINANCIAL_DISCOVERY_ENABLED','0')=='1'


def key_for(symbol,cik):
    symbol=canonical_symbol(symbol);cik=str(cik).zfill(10)
    if (not symbol or not re.fullmatch(r'[A-Z][A-Z0-9./-]{0,14}',symbol)
            or not re.fullmatch(r'[0-9]{10}',cik) or int(cik)==0):
        raise discovery.DiscoveryError('discovery_checkpoint_identity')
    return f'sec-financial-discovery:{symbol}:{cik}:v1'


def _query_limits(db):
    if db.get_bind().dialect.name=='postgresql':
        db.execute(text("SET LOCAL statement_timeout='3s'"))
        db.execute(text("SET LOCAL lock_timeout='1s'"))


def _read(key):
    with SessionLocal() as db:
        _query_limits(db)
        row=db.get(InsightsSnapshot,key)
        if row is None:return None,None
        if row.source!='sec_edgar' or len(row.payload_json.encode())>discovery.MAX_STATE_BYTES:
            raise discovery.DiscoveryError('discovery_checkpoint_invalid')
        try:state=json.loads(row.payload_json)
        except (ValueError,TypeError) as exc:
            raise discovery.DiscoveryError('discovery_checkpoint_invalid') from exc
        if not isinstance(state,dict) or not isinstance(state.get('receipts'),dict):
            raise discovery.DiscoveryError('discovery_checkpoint_invalid')
        return state,row.payload_json


def read(symbol,cik):
    """Return a checkpoint, not a validated source choice. Off means no query."""
    if not enabled():return None
    return _read(key_for(symbol,cik))[0]


def _worker_guard():
    if not enabled():raise discovery.DiscoveryError('discovery_disabled')
    from app.request_priority import get_request_context
    if get_request_context():
        raise discovery.DiscoveryError('discovery_worker_only')


def _persist(key,state,expected,observed_at):
    payload=discovery._json(state)
    if len(payload.encode())>discovery.MAX_STATE_BYTES:raise discovery.DiscoveryError('discovery_state_bound')
    if not enabled():raise discovery.DiscoveryError('discovery_disabled_during_scan')
    if payload==expected:return False
    with SessionLocal() as db:
        _query_limits(db)
        if expected is None:
            db.add(InsightsSnapshot(kind=key,source='sec_edgar',fetched_at=observed_at,payload_json=payload))
        else:
            changed=db.execute(update(InsightsSnapshot).where(InsightsSnapshot.kind==key,
                InsightsSnapshot.source=='sec_edgar',InsightsSnapshot.payload_json==expected)
                .values(payload_json=payload,fetched_at=observed_at))
            if changed.rowcount!=1:
                db.rollback();raise DiscoveryConflict('discovery_checkpoint_changed')
        try:db.commit()
        except IntegrityError as exc:
            db.rollback();raise DiscoveryConflict('discovery_checkpoint_changed') from exc
    return True


def stage(company_raw, *, symbol, cik, available_by, client, observed_at=None):
    """Advance one batch and commit with compare-and-swap after network closes.

    An interrupted/failed batch cannot advance the durable checkpoint. Concurrent
    writers cannot overwrite each other's progress. The caller must still supply
    the existing job's load, transport, execution-time and scheduling guards.
    """
    _worker_guard()
    observed_at=observed_at or datetime.now(timezone.utc)
    key=key_for(symbol,cik);previous,expected=_read(key)
    result=discovery.advance(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                             observed_at=observed_at,client=client,previous=previous)
    return {**result,'checkpoint_written':_persist(key,result['state'],expected,observed_at)}


def progress(company_raw,facts_raw,*,symbol,cik,available_by,client,observed_at=None):
    """One slot: three covers OR up to three qualifications within six reads."""
    _worker_guard()
    from app.services import sec_financial_discovery_reader as reader
    observed_at=observed_at or datetime.now(timezone.utc)
    key=key_for(symbol,cik);previous,expected=_read(key)
    common=dict(symbol=symbol,cik=cik,available_by=available_by,observed_at=observed_at)
    plan=discovery.inspect(company_raw,previous=previous,**common)
    if plan['remaining']:
        result=discovery.advance(company_raw,previous=previous,client=client,**common)
        written=_persist(key,result['state'],expected,observed_at)
        return {**reader.selection_plan(company_raw,checkpoint=result['state'],**common),
                'phase':'covers','checkpoint_written':written,'inspected':result['inspected_this_run']}
    selection=reader.selection_plan(company_raw,checkpoint=plan['state'],**common)
    if selection['next_accession'] is None:
        written=_persist(key,plan['state'],expected,observed_at)
        return {**selection,'phase':'complete','checkpoint_written':written,'inspected':0}
    # Share the existing six-original ceiling across filings. Do not start
    # another filing after the continuation allowance; a single existing
    # qualification retains its own source/parser limits and may take longer.
    started=time.monotonic();cpu_started=time.process_time();requests=0
    class BoundedClient:
        def get(self,url):
            nonlocal requests
            if requests>=reader.MAX_ORIGINALS:
                raise _QualificationBudget('discovery_batch_original_bound')
            requests+=1
            return client.get(url)
    bounded=BoundedClient();completed=[];stop_reason=None
    for _ in range(QUALIFICATION_BATCH_LIMIT):
        if completed:
            if not enabled():
                raise discovery.DiscoveryError('discovery_disabled_during_scan')
            if requests>=reader.MAX_ORIGINALS:
                stop_reason='original_budget';break
            if (time.monotonic()-started>=QUALIFICATION_CONTINUE_SECONDS
                    or time.process_time()-cpu_started>=QUALIFICATION_CONTINUE_CPU_SECONDS):
                stop_reason='continuation_budget';break
        selection=reader.selection_plan(company_raw,checkpoint=plan['state'],**common)
        if selection['next_accession'] is None:break
        try:
            result=reader.inspect_candidate(company_raw,facts_raw,checkpoint=plan['state'],
                accession=selection['next_accession'],client=bounded,**common)
        except _QualificationBudget:
            # Only complete, independently validated receipts can be committed.
            # The unfinished candidate stays first in line for the next slot.
            if not completed:raise
            stop_reason='original_budget';break
        except discovery.DiscoveryError as exc:
            if str(exc)!='discovery_cover_changed':raise
            if completed:
                stop_reason='legacy_cover_reobservation';break
            # Upgrade a legacy hint in its own phase. No qualification or
            # financial value is admitted during this re-observation.
            fingerprint,hint=next((k,r) for k,r in plan['state']['receipts'].items()
                if r['candidate']['accession']==selection['next_accession'])
            if hint.get('document_sha256') or 'qualification' in hint:raise
            fresh=discovery._observe(bounded.get(hint['candidate']['cover_url']),hint['candidate'],observed_at)
            plan['state']['receipts'][fingerprint]=fresh
            reobserved=[hint['candidate']['accession']]
            # Upgrade additional legacy hints in the same observation-only
            # phase. Never qualify these newly observed covers in this slot.
            legacy=sorted(((k,r) for k,r in plan['state']['receipts'].items()
                if r['status']=='observed' and not r.get('document_sha256')
                and 'qualification' not in r and r['candidate']['accession'] not in reobserved),
                key=lambda item:(item[1]['candidate']['filed'],item[1]['candidate']['accession']),reverse=True)
            for other_key,other_hint in legacy[:QUALIFICATION_BATCH_LIMIT-1]:
                if not enabled():raise discovery.DiscoveryError('discovery_disabled_during_scan')
                if (requests>=reader.MAX_ORIGINALS
                        or time.monotonic()-started>=QUALIFICATION_CONTINUE_SECONDS
                        or time.process_time()-cpu_started>=QUALIFICATION_CONTINUE_CPU_SECONDS):break
                raw=bounded.get(other_hint['candidate']['cover_url'])
                plan['state']['receipts'][other_key]=discovery._observe(raw,other_hint['candidate'],observed_at)
                reobserved.append(other_hint['candidate']['accession'])
                del raw
            written=_persist(key,plan['state'],expected,observed_at)
            return {**reader.selection_plan(company_raw,checkpoint=plan['state'],**common),
                    'phase':'cover_reobserved','checkpoint_written':written,'inspected':len(reobserved),
                    'reobserved_accessions':reobserved,'original_requests':requests}
        receipt=result['receipt']
        del result  # Release complete originals before inspecting another filing.
        fingerprint=discovery._fingerprint(receipt['candidate'])
        plan['state']['receipts'][fingerprint]['qualification']=receipt
        completed.append({'accession':receipt['candidate']['accession'],'status':receipt['status']})
    written=_persist(key,plan['state'],expected,observed_at)
    return {**reader.selection_plan(company_raw,checkpoint=plan['state'],**common),
            'phase':'qualification','checkpoint_written':written,'inspected':len(completed),
            'qualified_accession':completed[-1]['accession'],'qualification_status':completed[-1]['status'],
            'qualifications':completed,'original_requests':requests,'qualification_stop_reason':stop_reason}
