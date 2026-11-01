"""Resolve an explicitly stated GAAP basis using exact filed comparatives.

This never supplies current statement values or guesses a dollar currency.
"""
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

POLICY='sec_exact_annual_comparative_basis_v1'
ANCHORS={'current_assets':'AssetsCurrent','total_assets':'Assets',
         'current_liabilities':'LiabilitiesCurrent','total_equity':'StockholdersEquity'}
ANNUAL={'20-F','20-F/A','40-F','40-F/A'}


class ComparativeBasisError(ValueError):
    pass


def need(condition,reason):
    if not condition:raise ComparativeBasisError('comparative_basis_'+reason)


def number(value):
    need(type(value) in (int,float),'number')
    value=Decimal(str(value));need(value.is_finite(),'number')
    return format(value.normalize(),'f')


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def definition(text):
    # A generic GAAP label, non-GAAP heading or US-dollar declaration alone
    # cannot establish the current statement's accounting basis.
    if re.search(r'\bIFRS\b|\b(?:non[- ]US|Israeli|Japanese|Canadian|Taiwan(?:ese)?)\s+GAAP\b',text,re.I):return None
    pattern=(r'\b(?:financial results (?:are |were )?(?:prepared(?: and presented)? )?'
             r'|results (?:are |were )?prepared(?: and presented)? )in accordance with '
             r'(?:generally accepted accounting principles,? (?:or )?)?GAAP\b')
    found=[]
    for match in re.finditer(pattern,text,re.I):
        if re.search(r'\b(?:not|never|unlike|hypothetical|forecast|projected|expected)\s+(?:\w+\s+){0,3}$',
                     text[max(0,match.start()-60):match.start()],re.I):continue
        # The unqualified GAAP sentence must end here. Jurisdiction qualifiers,
        # hypothetical predicates and other continuations need explicit review.
        suffix=text[match.end():].lstrip()
        if suffix and suffix[0] not in '.;,)':continue
        found.append(match.group(0))
    return list(dict.fromkeys(found)) or None


def prove(text,observations,*,company_raw,facts_raw,available_by):
    """Bind four printed comparative balances to the latest periodic US GAAP facts.

    Return provenance, not financial records. The caller must independently
    reparse complete statement/cover originals and validate every consumer.
    """
    try:
        declarations=definition(text);need(declarations,'current_declaration')
        need(isinstance(company_raw,bytes) and 0<len(company_raw)<=5_000_000,'company_size')
        need(isinstance(facts_raw,bytes) and 0<len(facts_raw)<=20_000_000,'facts_size')
        company=json.loads(company_raw);facts=json.loads(facts_raw);source=observations['source']
        cik=source['cik'];need(re.fullmatch(r'[0-9]{10}',cik),'identity')
        need(all(str(p.get('cik','')).zfill(10)==cik for p in (company,facts)),'identity')
        need(not observations['conflicts'] and observations['facts'],'observations')
        need({p['currency'] for p in observations['facts']}=={'USD'},'explicit_currency')
        end=max(date.fromisoformat(p['end']) for p in observations['facts'] if p['start'] is None)
        statement_filed=date.fromisoformat(source['filed'])
        need(end<=statement_filed<=available_by and 0<=(available_by-end).days<=180,'statement_date')
        recent=company['filings']['recent']
        keys=('accessionNumber','form','filingDate','reportDate','primaryDocument')
        arrays=[recent[k] for k in keys]
        need(all(isinstance(a,list) for a in arrays) and len({len(a) for a in arrays})==1
             and len(arrays[0])<=10000,'directory')
        reports=[]
        for accession,form,filed,period,document in zip(*arrays):
            if form not in ANNUAL | {'10-K','10-K/A','10-Q','10-Q/A'}:continue
            filing=date.fromisoformat(filed)
            if filing>available_by:continue
            report=date.fromisoformat(period)
            need(report<=filing,'periodic_date')
            if form.startswith(('10-K','10-Q')):
                need(report<end and filing<statement_filed,'domestic_boundary')
                continue
            reports.append(dict(accession=accession,form=form,filed=filed,period=period,document=document))
        need(reports,'annual_missing')
        rank=lambda r:(r['period'],r['filed'])
        latest=max(map(rank,reports));selected=[r for r in reports if rank(r)==latest]
        need(len(selected)==1,'annual_ambiguous');annual=selected[0]
        need(re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',annual['accession']) and
             re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',annual['document']),'periodic_identity')
        period=date.fromisoformat(annual['period']);filed=date.fromisoformat(annual['filed'])
        need(annual['form'] in {'20-F','40-F'},'amendment')
        need(period<end and (end-period).days<=366 and filed<=statement_filed,'annual_boundary')
        taxonomy=facts['facts'];need(isinstance(taxonomy,dict),'taxonomy')
        for concept in taxonomy.get('ifrs-full',{}).values():
            need(not any(p.get('accn')==annual['accession'] for rows in concept['units'].values() for p in rows),'mixed_taxonomy')
        def annual_points(tag):
            points=taxonomy.get('us-gaap',{}).get(tag,{}).get('units',{}).get('USD',[])
            need(isinstance(points,list) and len(points)<=20000,'points_bound')
            return [p for p in points if p.get('accn')==annual['accession'] and p.get('form')==annual['form']
                    and p.get('filed')==annual['filed'] and p.get('end')==annual['period']]
        anchors=[]
        for metric,tag in ANCHORS.items():
            printed=[p for p in observations['facts'] if p['metric']==metric and p['start'] is None and p['end']==annual['period']]
            need(len(printed)==1 and printed[0]['currency']=='USD','comparative_missing')
            points=[p for p in annual_points(tag) if not p.get('start')]
            values={number(p['val']) for p in points};need(len(values)==1,'annual_anchor_missing_or_conflicting')
            value=number(printed[0]['value']);need(value in values,'comparative_mismatch')
            anchors.append(dict(metric=metric,tag=tag,value=value))
        income=[p for p in annual_points('NetIncomeLoss') if p.get('start')
                and 350<=(period-date.fromisoformat(p['start'])).days+1<=380]
        need(income and len({(p['start'],number(p['val'])) for p in income})==1,'income_taxonomy_missing_or_conflicting')
        binding=dict(cik=cik,annual=annual,namespace='us-gaap',currency='USD',anchors=anchors,
                     income_start=income[0]['start'])
        proof=dict(policy=POLICY,**binding,binding_sha256=digest(binding),
            companyfacts_url=f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
            companyfacts_sha256=hashlib.sha256(facts_raw).hexdigest(),
            annual_url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{annual["accession"].replace("-","")}/{annual["document"]}',
            basis='Current GAAP statement with four exact comparative balances reconciled to the latest filed US GAAP annual taxonomy; no annual numeric values merged.')
        return dict(accounting_basis='US GAAP',currency='USD',accounting_declarations=declarations,
                    accounting_basis_origin='annual_comparative_reconciliation',
                    currency_basis='single_explicit_statement_currency',translations=[],comparative_basis=proof)
    except ComparativeBasisError:raise
    except (KeyError,TypeError,ValueError,AttributeError,InvalidOperation,OverflowError) as exc:
        raise ComparativeBasisError('comparative_basis_malformed') from exc


def cached_valid(proof,source,current_period):
    try:
        annual=proof['annual'];cik=source['cik'];anchors=proof['anchors']
        binding={k:proof[k] for k in ('cik','annual','namespace','currency','anchors','income_start')}
        period=date.fromisoformat(annual['period']);current=date.fromisoformat(current_period)
        filed=date.fromisoformat(annual['filed']);statement_filed=date.fromisoformat(source['filed'])
        income_start=date.fromisoformat(proof['income_start'])
        return (proof['policy']==POLICY and proof['cik']==cik and proof['namespace']=='us-gaap'
            and re.fullmatch(r'[0-9]{10}',cik) is not None
            and proof['currency']=='USD' and proof['binding_sha256']==digest(binding)
            and annual['form'] in {'20-F','40-F'}
            and re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',annual['accession']) is not None
            and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',annual['document']) is not None
            and 0<(current-period).days<=366 and period<=filed<=statement_filed and current<=statement_filed
            and 350<=(period-income_start).days+1<=380
            and [(a['metric'],a['tag']) for a in anchors]==list(ANCHORS.items())
            and all(isinstance(a['value'],str) and Decimal(a['value']).is_finite() for a in anchors)
            and re.fullmatch(r'[0-9a-f]{64}',proof['companyfacts_sha256']) is not None
            and proof['companyfacts_url']==f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
            and proof['annual_url']==f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{annual["accession"].replace("-","")}/{annual["document"]}')
    except (KeyError,TypeError,ValueError,AttributeError,InvalidOperation):return False
