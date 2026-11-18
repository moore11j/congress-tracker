"""Recover one missing IFRS annual anchor from its complete primary original.

The current statement retains priority. Currency, entity, directory, comparative
revenue concepts and every overlapping observation must agree before merging.
"""
from copy import deepcopy
from datetime import date
import hashlib
import json
import re

VERSION = 'sec_native_original_annual_anchor_v1'
REVENUE = ('Revenue', 'RevenueFromContractsWithCustomers')


def _fingerprint(proof):
    return hashlib.sha256(json.dumps({k:v for k,v in proof.items() if k!='fingerprint'},
        sort_keys=True,separators=(',',':')).encode()).hexdigest()


def _points(parsed):
    rows={}
    for tag,concept in parsed['facts'].items():
        for unit,points in concept['units'].items():
            for p in points:
                key=(tag,unit,p.get('start'),p['end'])
                if key in rows and rows[key]!=p['val']:raise ValueError('native_annual_duplicate')
                rows[key]=p['val']
    return rows


def _digest(parsed):
    value={'facts':parsed['facts'],'evidence':parsed['evidence'],
        'provenance':[[list(k),v] for k,v in sorted(parsed['provenance'].items(),key=lambda x:repr(x[0]))]}
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def plan(payload,company,current,*,available_by):
    from app.services import sec_inline_financials as inline,sec_statement_currency as native
    from app.services import sec_fundamentals as sec,sec_tagged_foreign as foreign
    if current.get('annual_history'):return None
    basis=native.reporting_basis(current)
    if not basis or basis['namespace']!='ifrs-full':return None
    evidence=current['evidence'];cik=evidence['cik'];end=sec._day(evidence['report_date'])
    if evidence['form'] not in {'6-K','6-K/A'} or not end or end.month not in {3,6,9}:return None
    if not 0<=(available_by-end).days<=180:return None
    if any(str(x.get('cik','')).zfill(10)!=cik for x in (company,payload)):raise ValueError('native_annual_identity')
    effective=inline.merge(payload,current,available_by=available_by)
    def records(tag):
        return sec._records(effective,'revenue',available_by,concepts=(tag,),unit=basis['currency'],namespace='ifrs-full',forms=foreign.FORMS)
    total,contract=(records(t) for t in REVENUE)
    if sec._ttm(total,end) is not None:return None
    annual_key=(date(end.year-1,1,1),date(end.year-1,12,31))
    # An explicit conflict/nil/zero is present evidence, not a missing anchor.
    if annual_key in total or annual_key in contract:return None
    for year in (end.year-1,end.year):
        key=(date(year,1,1),date(year,end.month,end.day))
        left,right=total.get(key),contract.get(key)
        if not left or not right or any(left.get(k)!=right.get(k) for k in ('value','unit','filed','accession')):return None
        if left['accession']!=evidence['accession'] or left['filed']!=evidence['filed']:return None
    recent=company.get('filings',{}).get('recent',{})
    arrays=[recent.get(k,[]) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays})!=1:raise ValueError('native_annual_directory_shape')
    candidates={}
    for accession,form,filed,report,filename in zip(*arrays):
        if form not in {'20-F','20-F/A'} or report!=str(annual_key[1]):continue
        if not sec._day(filed) or not report<=filed<=str(available_by):continue
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',accession) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename):
            raise ValueError('native_annual_directory_identity')
        item=dict(cik=cik,accession=accession,form=form,filed=filed,report_date=report,
            url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-","")}/{filename}')
        if accession in candidates and candidates[accession]!=item:raise ValueError('native_annual_directory_conflict')
        candidates[accession]=item
    if not candidates:return None
    latest=max(r['filed'] for r in candidates.values())
    chosen=[r for r in candidates.values() if r['filed']==latest]
    if len(chosen)!=1 or latest>evidence['filed']:return None
    return dict(filing=chosen[0],basis=basis,annual_start=str(annual_key[0]),annual_end=str(annual_key[1]))


def _overlaps(current,annual,*,currency,annual_end):
    left,right=_points(current),_points(annual)
    shared=sorted(left.keys()&right.keys(),key=repr)
    if any(left[k]!=right[k] for k in shared):raise ValueError('native_annual_statement_overlap_conflict')
    required={'Assets','CurrentAssets','CurrentLiabilities','EquityAttributableToOwnersOfParent'}
    present={k[0] for k in shared if k[1]==currency and k[2] is None and k[3]==annual_end}
    if not required<=present:raise ValueError('native_annual_overlap_missing')
    return [dict(tag=k[0],unit=k[1],start=k[2],end=k[3],value=left[k]) for k in shared]


def _proof(current,annual,*,filing,basis):
    from app.services import sec_statement_currency as native
    evidence=annual['evidence'];source=current['evidence']
    declaration=native.bind(annual,evidence['statement_currency_declaration'])
    if any(declaration[k]!=source[k] for k in ('cik','currency','namespace')):raise ValueError('native_annual_basis_mismatch')
    if any(evidence.get(k)!=v for k,v in filing.items()):raise ValueError('native_annual_original_identity')
    if 'annual_history' in annual or 'native_annual_history' in evidence:raise ValueError('native_annual_nested_history')
    points=_points(annual);start=filing['report_date'][:4]+'-01-01';end=filing['report_date']
    values=[points.get((tag,basis['currency'],start,end)) for tag in REVENUE]
    if values[0] is None or values[0]!=values[1]:raise ValueError('native_annual_revenue_concept_conflict')
    overlaps=_overlaps(current,annual,currency=basis['currency'],annual_end=end)
    proof=dict(methodology=VERSION,cik=source['cik'],namespace='ifrs-full',currency=basis['currency'],
        current_url=source['url'],current_sha256=source['sha256'],current_accession=source['accession'],current_period=source['report_date'],
        filing=filing,source_sha256=evidence['sha256'],annual_revenue=values[0],annual_start=start,annual_end=end,
        overlaps=overlaps,observations=[dict(tag=k[0],unit=k[1],start=k[2],end=k[3],value=v)
            for k,v in sorted(points.items(),key=lambda x:repr(x[0]))],
        parsed_digest=_digest(annual),statement_currency_declaration=declaration)
    proof['fingerprint']=_fingerprint(proof)
    return proof


def _existing_agreement(payload,current,annual,*,available_by):
    from app.services import sec_inline_financials as inline,sec_fundamentals as sec,sec_tagged_foreign as foreign
    effective=inline.merge(payload,current,available_by=available_by)
    series={}
    for (tag,unit,start,end),value in _points(annual).items():
        group=(tag,unit,start is None)
        if group not in series:
            series[group]=sec._records(effective,'equity' if start is None else 'revenue',available_by,
                concepts=(tag,),unit=unit,namespace='ifrs-full',forms=foreign.FORMS)
        existing=series[group];key=(sec._day(start),sec._day(end))
        if key in existing and (not existing[key] or existing[key]['value']!=value):
            raise ValueError('native_annual_existing_observation_conflict')


def acquire(payload,company,current,*,available_by,client):
    from app.services import sec_statement_currency as native,sec_inline_financials as inline
    wanted=plan(payload,company,current,available_by=available_by)
    if wanted is None:return None
    raw=client.get(wanted['filing']['url'])
    # Explicit accounting changes need their own scope qualification.
    from lxml import html
    current_raw=client.get(current['evidence']['url'])
    if hashlib.sha256(current_raw).hexdigest()!=current['evidence']['sha256']:raise ValueError('native_annual_current_original_changed')
    for original in (raw,current_raw):
        text=' '.join(html.fromstring(original).text_content().split())
        if re.search(r'(?i)discontinued operations|reverse acquisition|accounting predecessor',text):return None
    basis,declaration=native.accounting_basis(raw,url=wanted['filing']['url'])
    if any(basis[k]!=wanted['basis'][k] for k in ('currency','namespace')):raise ValueError('native_annual_basis_changed')
    annual=inline.parse(raw,**wanted['filing'],basis=basis)
    annual['evidence']['statement_currency_declaration']=native.bind(annual,declaration)
    for point in annual['provenance'].values():point['native_annual_anchor']=VERSION
    proof=_proof(current,annual,filing=wanted['filing'],basis=basis)
    # Preserve every existing fact. An annual original cannot silently revise a
    # currently selected historical observation while filling the missing year.
    _existing_agreement(payload,current,annual,available_by=available_by)
    return dict(parsed=annual,proof=proof)


def validate(payload,company,current,*,available_by):
    history=current.get('annual_history');saved=current['evidence'].get('native_annual_history')
    if history is None and saved is None:return None
    if not isinstance(history,dict) or not isinstance(saved,dict):raise ValueError('native_annual_proof_missing')
    baseline=deepcopy(current);baseline.pop('annual_history');baseline['evidence'].pop('native_annual_history')
    wanted=plan(payload,company,baseline,available_by=available_by)
    if wanted is None:raise ValueError('native_annual_plan_changed')
    if _proof(baseline,history,filing=wanted['filing'],basis=wanted['basis'])!=saved:
        raise ValueError('native_annual_proof_changed')
    _existing_agreement(payload,baseline,history,available_by=available_by)
    return history


def cached_valid(evidence):
    source=evidence.get('inline_filing_supplement') or {}
    if not isinstance(source,dict):return False
    proof=source.get('native_annual_history')
    def marked(value):
        if isinstance(value,dict):
            if 'native_annual_anchor' in value:yield value
            for child in value.values():yield from marked(child)
        elif isinstance(value,list):
            for child in value:yield from marked(child)
    selected=list(marked(evidence.get('current',{})))+list(marked(evidence.get('prior',{})))
    if proof is None:return not selected
    if not isinstance(proof,dict):return False
    try:
        if proof.get('fingerprint')!=_fingerprint(proof):return False
        if proof['methodology']!=VERSION or proof['namespace']!='ifrs-full':return False
        if any(proof[k]!=source[k] for k in ('cik','namespace','currency')):return False
        if any(proof[a]!=source[b] for a,b in [('current_url','url'),('current_sha256','sha256'),('current_accession','accession'),('current_period','report_date')]):return False
        for k in ('source_sha256','parsed_digest'):
            if not re.fullmatch(r'[0-9a-f]{64}',proof[k]):return False
        filing=proof['filing'];selection=source['native_reporting']
        if filing['cik']!=source['cik'] or filing['report_date']!=proof['annual_end'] or filing['filed']>source['filed']:return False
        if not any(all(r.get(k)==filing[k] for k in ('accession','form','filed','report_date','url')) for r in selection['filings']):return False
        current_year=date.fromisoformat(source['report_date']).year
        if proof['annual_start']!=f'{current_year-1}-01-01' or proof['annual_end']!=f'{current_year-1}-12-31':return False
        overlaps=proof['overlaps']
        tags={p['tag'] for p in overlaps if p['unit']==proof['currency'] and p['start'] is None and p['end']==proof['annual_end']}
        if not {'Assets','CurrentAssets','CurrentLiabilities','EquityAttributableToOwnersOfParent'}<=tags:return False
        observations={(p['tag'],p['unit'],p['start'],p['end']):p['value'] for p in proof['observations']}
        if len(observations)!=len(proof['observations']):return False
        return all(p.get('native_annual_anchor')==VERSION and p.get('source_sha256')==proof['source_sha256']
            and p.get('source_cik')==proof['cik'] and p.get('source_url')==filing['url']
            and p.get('accession')==filing['accession'] and p.get('filed')==filing['filed']
            and observations.get((p.get('tag'),p.get('unit'),p.get('start'),p.get('end')))==p.get('value') for p in selected)
    except (KeyError,TypeError,ValueError):return False
