"""Discover standard USD GAAP foreign statements by filing evidence, not ticker.

A financial accession requires both balance and duration facts at the exact
submission period. Event-only 6-Ks never qualify merely by their form name.
"""
from copy import deepcopy
from datetime import date
import math
import re

VERSION = 'sec_tagged_foreign_v1'
FORMS = frozenset({'20-F','20-F/A','40-F','40-F/A','6-K','6-K/A'})
BALANCE = frozenset({'Assets','StockholdersEquity','AssetsCurrent','LiabilitiesCurrent'})
DURATION = frozenset({'Revenues','RevenuesNetOfInterestExpense','RegulatedAndUnregulatedOperatingRevenue',
    'RevenueFromContractWithCustomerExcludingAssessedTax','SalesRevenueNet',
    'RevenueFromContractWithCustomerIncludingAssessedTax','NetIncomeLoss','OperatingIncomeLoss',
    'NetCashProvidedByUsedInOperatingActivities'})


class TaggedForeignError(ValueError):
    pass


def _day(value):
    try:return date.fromisoformat(value)
    except (ValueError,TypeError):return None


def plan(payload, company, *, symbol, cik, available_by):
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_currency_ratios import basis
    if reviewed(symbol,cik) or basis(symbol,cik):return None
    from app.utils.symbols import canonical_symbol
    cik=str(cik).zfill(10)
    if (not company.get('name') or not cik.isdigit() or len(cik)!=10 or any(str(x.get('cik','')).zfill(10)!=cik for x in (payload,company))
            or [canonical_symbol(s) for s in company.get('tickers',[])].count(canonical_symbol(symbol))!=1):
        raise TaggedForeignError('tagged_foreign_identity')
    recent=company.get('filings',{}).get('recent',{})
    if not any(f in FORMS for f in recent.get('form',[])):return None
    keys=('accessionNumber','form','filingDate','reportDate','primaryDocument')
    arrays=[recent.get(k,[]) for k in keys]
    if len({len(a) for a in arrays})!=1:raise TaggedForeignError('tagged_foreign_directory_shape')
    groups={}
    for tag in BALANCE|DURATION:
        for point in payload.get('facts',{}).get('us-gaap',{}).get(tag,{}).get('units',{}).get('USD',[]):
            value=point.get('val');end,filed=_day(point.get('end')),_day(point.get('filed'))
            if (point.get('form') not in FORMS or isinstance(value,bool) or not isinstance(value,(int,float))
                    or not math.isfinite(value) or not end or not filed or not end<=filed<=available_by):continue
            groups.setdefault(point.get('accn'),[]).append((tag,point))
    candidates={}
    for acc,form,filed,report,filename in zip(*arrays):
        if form not in FORMS:continue
        end,filing=_day(report),_day(filed)
        if not end or not filing or not end<=filing<=available_by:continue
        if (not isinstance(acc,str) or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',acc)
                or not isinstance(filename,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename)):continue
        rows=[(tag,p) for tag,p in groups.get(acc,[]) if p.get('form')==form and p.get('filed')==filed and p.get('end')==report]
        balance={tag for tag,p in rows if tag in BALANCE and not p.get('start')}
        duration={tag for tag,p in rows if tag in DURATION and _day(p.get('start'))
            and 60<=(end-_day(p['start'])).days+1<=380}
        if not balance or not duration:continue
        item={'accession':acc,'form':form,'filed':filed,'report_date':report,
            'url':f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace("-","")}/{filename}',
            'balance_tags':sorted(balance),'duration_tags':sorted(duration)}
        if acc in candidates and candidates[acc]!=item:raise TaggedForeignError('tagged_foreign_duplicate_accession')
        candidates[acc]=item
    if not candidates:return None
    latest=max((x['report_date'],x['filed']) for x in candidates.values())
    selected=[x for x in candidates.values() if (x['report_date'],x['filed'])==latest]
    if len(selected)!=1:raise TaggedForeignError('tagged_foreign_ambiguous_current')
    current=selected[0]
    from app.services.sec_reporting_policy import DOMESTIC_FORMS
    if any(form in DOMESTIC_FORMS and (_day(report) or date.min)>=_day(current['report_date'])
            and _day(filed) and _day(filed)<=available_by
            for acc,form,filed,report,filename in zip(*arrays)):
        return None  # Preserve issuers already reporting on domestic forms.
    if (available_by-_day(current['report_date'])).days>180:return None
    # Do not discard a newer current-period observation just because its
    # accession fails the full-statement qualification above.
    if any(p['end']>=current['report_date'] and p['filed']>=current['filed']
           for acc,rows in groups.items() if acc not in candidates for _,p in rows):
        raise TaggedForeignError('tagged_foreign_newer_unqualified_facts')
    return {'methodology':VERSION,'cik':cik,'namespace':'us-gaap','currency':'USD',
        'current':current,'filings':sorted(candidates.values(),key=lambda x:(x['report_date'],x['filed'],x['accession'])),
        'limitations':['Standard entity-wide USD GAAP facts only; no foreign exchange or ADS/per-share inference.',
            'A current tagged statement is required; event-only 6-Ks do not establish financial reporting periods.']}


def acquire(payload,company,*,symbol,cik,available_by,client):
    selection=plan(payload,company,symbol=symbol,cik=cik,available_by=available_by)
    if selection is None:return None
    from app.services.sec_inline_financials import parse
    current=selection['current'];raw=client.get(current['url'])
    parsed=parse(raw,cik=selection['cik'],accession=current['accession'],filed=current['filed'],
        report_date=current['report_date'],form=current['form'],url=current['url'])
    parsed['evidence']['standard_foreign_reporting']=selection
    return parsed


def authorized(payload,company,*,symbol,cik,available_by,supplement):
    """Recompute qualification and bind it to the independently parsed original."""
    selection=plan(payload,company,symbol=symbol,cik=cik,available_by=available_by)
    if selection is None:return None
    proof=(supplement or {}).get('evidence',{})
    current=selection['current']
    if (proof.get('standard_foreign_reporting')!=selection or proof.get('cik')!=selection['cik']
            or proof.get('namespace')!='us-gaap' or proof.get('currency')!='USD'
            or any(proof.get(k)!=current[k] for k in ('accession','filed','report_date','form','url'))
            or not isinstance(proof.get('sha256'),str)
            or not re.fullmatch(r'[0-9a-f]{64}',proof['sha256'])):
        raise TaggedForeignError('tagged_foreign_original_proof_required')
    return selection


def filtered(payload,selection):
    """Keep only exact directory-qualified foreign accessions; never mix aliases."""
    accepted={x['accession']:x for x in selection['filings']}
    out=deepcopy(payload)
    for namespace in out.get('facts',{}).values():
        for concept in namespace.values():
            for unit,rows in concept.get('units',{}).items():
                concept['units'][unit]=[p for p in rows if p.get('accn') in accepted
                    and p.get('form')==accepted[p['accn']]['form'] and p.get('filed')==accepted[p['accn']]['filed']]
    return out
