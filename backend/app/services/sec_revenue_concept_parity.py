"""One reviewed revenue series, with exact total/contract overlap and annual proof."""
from datetime import date
from functools import lru_cache
import json
from pathlib import Path

VERSION = 'sec_revenue_concept_parity_v1'
PROFILES = {'AEVA': '0001789029', 'ALLE': '0001579241'}
TOTAL = 'Revenues'
CONTRACT = 'RevenueFromContractWithCustomerExcludingAssessedTax'


@lru_cache(maxsize=1)
def reviews():
    return json.loads((Path(__file__).resolve().parents[1]/'config/revenue_concepts.json').read_text())


def apply(records, payload, company, *, symbol, cik, available_by, supplement=None):
    from app.services import sec_fundamentals as sec
    if symbol not in PROFILES:
        return records
    def hold(reason):
        raise sec.SecFundamentalsCoverageError('revenue_concept_'+reason)
    if (str(cik).zfill(10) != PROFILES[symbol]
            or str(payload.get('cik', '')).zfill(10) != PROFILES[symbol]
            or str(company.get('cik', '')).zfill(10) != PROFILES[symbol]
            or company.get('tickers', []).count(symbol) != 1):
        hold('identity')
    recent=company.get('filings', {}).get('recent', {})
    filings=[]
    for i, form in enumerate(recent.get('form', [])):
        if form not in {'10-K','10-Q','10-K/A','10-Q/A'}:
            continue
        try:
            end=date.fromisoformat(recent['reportDate'][i]);filed=date.fromisoformat(recent['filingDate'][i]);acc=recent['accessionNumber'][i]
        except (KeyError,IndexError,TypeError,ValueError):
            hold('filing_identity')
        if end <= filed <= available_by and acc:
            filings.append((end,filed,acc))
    if not filings:
        hold('filing_missing')
    end,filed,acc=max(filings)
    if (available_by-end).days > 180:
        hold('stale')
    if len({row[2] for row in filings if row[:2] == (end,filed)}) != 1:
        hold('ambiguous_filing')
    nil=(supplement or {}).get('evidence', {}).get('nil_facts', [])
    if any(p['tag'] in {TOTAL,CONTRACT} and p['end']==str(end) for p in nil):
        hold('nil_current_revenue')
    ordinary=records.get('revenue', {})
    if not ordinary or any(p and p['tag'] != TOTAL for p in ordinary.values()):
        return records
    if max(e for _,e in ordinary) != end:
        hold('latest_total_missing')
    if sec._ttm(ordinary,end) is not None:
        return records
    review=reviews()[symbol]
    if review['cik'] != PROFILES[symbol]:
        hold('review_identity')
    annual=review['annual_facts']
    floor=min(date.fromisoformat(p['start']) for p in annual)
    candidate=sec._records(payload,'revenue',available_by,concepts=(CONTRACT,))
    candidate={key:p for key,p in candidate.items() if key[0] >= floor}
    if not candidate or max(e for _,e in candidate) != end:
        hold('candidate_period')
    overlaps=[]
    for key,left in ordinary.items():
        if key[0] < floor:
            continue
        right=candidate.get(key)
        if not left or not right or any(left[k] != right[k] for k in ('value','unit','filed','accession')):
            hold('total_contract_mismatch')
        overlaps.append({'start':left['start'],'end':left['end'],'value':left['value'],
                         'accession':left['accession'],'filed':left['filed']})
    if len(overlaps)<2:
        hold('insufficient_overlap')
    current=sec._ttm(candidate,end)
    if not current or not any(p['end']==str(end) and p['accession']==acc and p['filed']==str(filed) for p in current['inputs']):
        hold('current_filing_missing')
    # No annual concept bridge: the selected annual values must be the exact
    # single-concept amounts reviewed in the complete original statement.
    annual_count=0
    for (start,finish),fact in candidate.items():
        if not 350 <= (finish-start).days+1 <= 380:
            continue
        matches=[p for p in annual if p.get('start')==str(start) and p['end']==str(finish)]
        if (not fact or len(matches)!=1 or any(fact[k] != matches[0][v]
                for k,v in (('value','val'),('accession','accn'),('filed','filed')))):
            hold('annual_review_mismatch')
        annual_count+=1
    if not annual_count:
        hold('annual_missing')
    proof={'methodology':VERSION,'selected_concept':CONTRACT,'superseded_concept':TOTAL,
        'basis_spliced':False,'sources':review['sources'],'overlap':overlaps,
        'annual_periods_verified':annual_count,
        'basis':'Reviewed income-statement total; exact same-filing total/contract overlap; one unchanged contract-revenue history'}
    return {**records,'revenue':{key:{**p,'revenue_selection':proof} if p else None for key,p in candidate.items()}}
