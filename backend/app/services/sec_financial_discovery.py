"""Resumable, bounded SEC filing-cover discovery. Never financial admission.

Receipts are hints for existing original-statement readers, not financial facts.
The caller owns durable atomic storage and scheduling. This module has no DB,
environment flags, provider selection, background trigger or publication path.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import re
from lxml import html

VERSION = 'sec_financial_cover_discovery_v1'
BATCH_LIMIT = 3
MAX_CANDIDATES = 256
MAX_COVER_BYTES = 2_000_000
MAX_STATE_BYTES = 262_144
MAX_LINKS = 8
IDENTITY_FIELDS = ('symbol','cik','accession','form','filed','report_date','cover_url')
STATUSES = {'observed', 'cover_size_hold', 'cover_parse_hold', 'cover_scope_hold', 'link_scope_hold', 'link_count_hold'}


class DiscoveryError(ValueError):
    pass


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True)


def document_proof(raw):
    from app.services.sec_original_integrity import POLICY, document_digest
    try: digest=document_digest(raw)
    except ValueError:return {}
    return {'document_sha256':digest,'document_integrity_policy':POLICY}


def valid_document_proof(proof):
    from app.services.sec_original_integrity import POLICY
    present={'document_sha256','document_integrity_policy'} & proof.keys()
    return not present or (len(present)==2 and proof['document_integrity_policy']==POLICY
        and isinstance(proof['document_sha256'],str)
        and re.fullmatch(r'[0-9a-f]{64}',proof['document_sha256']) is not None)


def same_document(first,second):
    return (valid_document_proof(first) and valid_document_proof(second)
        and first.get('document_sha256') is not None
        and first.get('document_sha256')==second.get('document_sha256')
        and first.get('document_integrity_policy')==second.get('document_integrity_policy'))


def _fingerprint(candidate):
    # Changed issuer or filing metadata changes identity; unrelated metadata
    # must not repeatedly erase progress through the same immutable accessions.
    return hashlib.sha256(_json(candidate).encode()).hexdigest()


def _inventory(company_raw, *, symbol, cik, available_by):
    from app.services.sec_document_sets import candidates
    rows=candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    if len(rows)>MAX_CANDIDATES:
        raise DiscoveryError('discovery_candidate_bound')
    # Discovery goes newest first. A submitted report date may be an event date;
    # only the later full statement parser may establish a financial period.
    rows.sort(key=lambda c:(c['filed'],c['accession']),reverse=True)
    return [{k:c[k] for k in IDENTITY_FIELDS} for c in rows]


def _link_in_scope(url, candidate):
    prefix=candidate['cover_url'].rsplit('/',1)[0]+'/'
    return (isinstance(url,str) and url.startswith(prefix)
            and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',url[len(prefix):]) is not None)


def _state(previous, *, symbol, cik, rows, now):
    state={'version':VERSION,'symbol':symbol,'cik':cik,'receipts':{}}
    if previous is None:return state
    try:
        if len(_json(previous).encode())>MAX_STATE_BYTES:
            raise DiscoveryError('discovery_state_bound')
        if (not isinstance(previous,dict) or previous.get('symbol')!=symbol or previous.get('cik')!=cik
                or not isinstance(previous.get('receipts'),dict) or len(previous['receipts'])>MAX_CANDIDATES):
            raise DiscoveryError('discovery_state_identity')
        # A reviewed parser-version change invalidates observations, not identity.
        if previous.get('version')!=VERSION:return state
        current={_fingerprint(c):c for c in rows}
        for key,receipt in previous['receipts'].items():
            if key not in current:continue
            candidate=current[key]
            observed=datetime.fromisoformat(receipt['observed_at'])
            links=receipt['links']
            kinds=receipt['link_kinds']
            if (not valid_document_proof(receipt) or receipt['candidate']!=candidate or receipt['status'] not in STATUSES
                    or not re.fullmatch(r'[0-9a-f]{64}',receipt['cover_sha256'])
                    or observed.tzinfo is None or observed>now
                    or type(receipt['bytes']) is not int or receipt['bytes']<=0
                    or (receipt['status']=='observed' and receipt['bytes']>MAX_COVER_BYTES)
                    or type(receipt['primary_statement']) is not bool
                    or not isinstance(links,list) or len(links)>MAX_LINKS
                    or links!=sorted(set(links)) or any(not _link_in_scope(u,candidate) for u in links)
                    or not isinstance(kinds,dict) or set(kinds)!=set(links)
                    or any(k not in {'financial_statement','financial_results','press_release'} for k in kinds.values())
                    or (receipt['status']!='observed' and (links or receipt['primary_statement']))):
                raise DiscoveryError('discovery_receipt_invalid')
            retained=deepcopy(receipt)
            if 'qualification' in retained:
                from app.services.sec_financial_discovery_reader import validate_receipt
                qualified=validate_receipt(retained['qualification'],retained,now=now)
                if qualified is None:retained.pop('qualification')
                else:retained['qualification']=qualified
            state['receipts'][key]=retained
    except (TypeError,KeyError,AttributeError,OverflowError,ValueError) as exc:
        if isinstance(exc,DiscoveryError):raise
        raise DiscoveryError('discovery_state_invalid') from exc
    return state


def _observe(raw, candidate, now):
    if not isinstance(raw,bytes) or not raw:
        raise DiscoveryError('discovery_source_empty')
    receipt={'candidate':candidate,'cover_sha256':hashlib.sha256(raw).hexdigest(),
        'bytes':len(raw),'observed_at':now.isoformat(),'status':'observed',
        'primary_statement':False,'links':[],'link_kinds':{},**document_proof(raw)}
    if len(raw)>MAX_COVER_BYTES:
        receipt['status']='cover_size_hold';return receipt
    body=raw
    if raw.lstrip().upper().startswith(b'<DOCUMENT>'):
        # Some SEC archive URLs return one complete SGML document wrapper.
        # Its form and filename must match this exact directory candidate.
        wrapper=re.fullmatch(rb'\s*<DOCUMENT>(.*?)<TEXT>(.*)</TEXT>\s*</DOCUMENT>\s*',raw,re.I|re.S)
        if wrapper is None or len(wrapper[1])>4096 or len(re.findall(rb'<DOCUMENT>',raw,re.I))!=1:
            receipt['status']='cover_scope_hold';return receipt
        forms=re.findall(rb'^<TYPE>([^\r\n]+)',wrapper[1],re.I|re.M)
        names=re.findall(rb'^<FILENAME>([^\r\n]+)',wrapper[1],re.I|re.M)
        if forms!=[candidate['form'].encode()] or names!=[candidate['cover_url'].rsplit('/',1)[1].encode()]:
            receipt['status']='cover_scope_hold';return receipt
        body=wrapper[2]
    try:tree=html.fromstring(body)
    except (ValueError,html.etree.ParserError):
        receipt['status']='cover_parse_hold';return receipt
    if tree.tag!='html' or tree.xpath('//base'):
        receipt['status']='cover_scope_hold';return receipt
    for node in tree.xpath('//script|//style|//template|//blockquote'):
        node.drop_tree()
    from app.services.sec_tabular_reporting import primary_statement_candidate,statement_link_candidates
    labelled=statement_link_candidates(tree,candidate['cover_url'])
    links=[r['url'] for r in labelled]
    if len(links)>MAX_LINKS:
        receipt['status']='link_count_hold';return receipt
    if any(not _link_in_scope(url,candidate) for url in links):
        receipt['status']='link_scope_hold';return receipt
    receipt.update(primary_statement=primary_statement_candidate(tree),links=links,
                   link_kinds={r['url']:r['kind'] for r in labelled})
    return receipt


def inspect(company_raw, *, symbol, cik, available_by, observed_at, previous=None):
    """Reconcile current inventory and stored receipts without any transport."""
    if (not isinstance(observed_at,datetime) or observed_at.tzinfo is None
            or available_by>observed_at.astimezone(timezone.utc).date()):
        raise DiscoveryError('discovery_observation_time')
    rows=_inventory(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    from app.utils.symbols import canonical_symbol
    symbol=canonical_symbol(symbol);cik=str(cik).zfill(10)
    state=_state(previous,symbol=symbol,cik=cik,rows=rows,now=observed_at)
    return _summary(company_raw,rows,state,0)


def _summary(company_raw,rows,state,inspected):
    receipts=[state['receipts'][_fingerprint(c)] for c in rows if _fingerprint(c) in state['receipts']]
    hints=[r for r in receipts if r['status']=='observed' and (r['primary_statement'] or r['links'])]
    pending=[c for c in rows if _fingerprint(c) not in state['receipts']]
    return {'version':VERSION,'state':state,'inspected_this_run':inspected,
        'candidate_count':len(rows),'remaining':len(pending),'all_candidate_covers_observed':not pending,
        'next_batch':pending[:BATCH_LIMIT],
        'unresolved_covers':[r for r in receipts if r['status']!='observed'],
        'statement_candidates':hints,'directory_sha256':hashlib.sha256(company_raw).hexdigest(),
        'financial_publication_eligible':False,
        'limitations':['Cover observations are discovery hints, not financial qualification.',
            'No identified statement link is not proof that a filing has no financial information.',
            'Existing readers must reverify complete originals, newer statements and amendments.']}


def advance(company_raw, *, symbol, cik, available_by, observed_at, client, previous=None):
    """Observe at most three new covers; return state only after all succeed.

    No exhibit is fetched here. A transport denial/failure propagates, leaving
    caller-owned previous state intact. New filings enter the next batch while
    unchanged observations survive unrelated directory updates. An amendment
    has its own accession and can never inherit the original's receipt.
    """
    plan=inspect(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                 observed_at=observed_at,previous=previous)
    state=plan['state'];batch=plan['next_batch']
    for candidate in batch:
        raw=client.get(candidate['cover_url'])
        state['receipts'][_fingerprint(candidate)]=_observe(raw,candidate,observed_at)
    if len(_json(state).encode())>MAX_STATE_BYTES:
        raise DiscoveryError('discovery_state_bound')
    result=inspect(company_raw,symbol=symbol,cik=cik,available_by=available_by,
                   observed_at=observed_at,previous=state)
    result['inspected_this_run']=len(batch)
    return result
