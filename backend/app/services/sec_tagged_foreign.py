"""Discover standard USD GAAP foreign statements by filing evidence, not ticker.

A financial accession requires both balance and duration facts at the exact
submission period. Event-only 6-Ks never qualify merely by their form name.
"""
from copy import deepcopy
from datetime import date
import math
import re

VERSION = 'sec_tagged_foreign_v1'
CURRENCY_POLICY = 'single_current_statement_currency_v1'
FORMS = frozenset({'20-F','20-F/A','40-F','40-F/A','6-K','6-K/A'})
BALANCE = frozenset({'Assets','StockholdersEquity','AssetsCurrent','LiabilitiesCurrent'})
DURATION = frozenset({'Revenues','RevenuesNetOfInterestExpense','RegulatedAndUnregulatedOperatingRevenue',
    'RevenueFromContractWithCustomerExcludingAssessedTax','SalesRevenueNet',
    'RevenueFromContractWithCustomerIncludingAssessedTax','NetIncomeLoss','OperatingIncomeLoss',
    'NetCashProvidedByUsedInOperatingActivities'})


class TaggedForeignError(ValueError):
    pass


class TaggedCurrencyError(TaggedForeignError):
    """Verified filing has no reviewed unambiguous USD statement basis."""


def _day(value):
    try:return date.fromisoformat(value)
    except (ValueError,TypeError):return None


def plan(payload, company, *, symbol, cik, available_by, statement_basis=None):
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_currency_ratios import basis
    if statement_basis is None and (reviewed(symbol,cik) or basis(symbol,cik)):return None
    namespace = statement_basis['namespace'] if statement_basis else 'us-gaap'
    currency = statement_basis['currency'] if statement_basis else 'USD'
    balance_tags, duration_tags = BALANCE, DURATION
    if statement_basis:
        from app.services.sec_statement_currency import INSTANT_METRICS
        balance_tags = {t for k,ts in statement_basis['tags'].items() if k in INSTANT_METRICS for t in ts}
        duration_tags = {t for k,ts in statement_basis['tags'].items() if k not in INSTANT_METRICS for t in ts}
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
    for tag in balance_tags|duration_tags:
        for point in payload.get('facts',{}).get(namespace,{}).get(tag,{}).get('units',{}).get(currency,[]):
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
        balance={tag for tag,p in rows if tag in balance_tags and not p.get('start')}
        duration={tag for tag,p in rows if tag in duration_tags and _day(p.get('start'))
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
    return {'methodology':VERSION,'currency_policy':(statement_basis['policy'] if statement_basis else CURRENCY_POLICY),'cik':cik,'namespace':namespace,'currency':currency,
        'current':current,'filings':sorted(candidates.values(),key=lambda x:(x['report_date'],x['filed'],x['accession'])),
        'limitations':[(f'Standard entity-wide {currency} {namespace} facts with explicit statement-currency evidence; no foreign exchange or ADS/per-share inference.' if statement_basis else 'Standard entity-wide USD GAAP facts only; no foreign exchange or ADS/per-share inference.'),
            'A current tagged statement is required; event-only 6-Ks do not establish financial reporting periods.']}


def verify_currency(proof):
    observation = proof.get('currency_observation') if isinstance(proof, dict) else None
    if (not isinstance(observation, dict)
            or observation.get('methodology') != 'entity_wide_statement_currencies_v1'
            or observation.get('current_statement_currencies') != ['USD']):
        raise TaggedCurrencyError('unreviewed_foreign_statement_currency')


def cached_currency_valid(evidence):
    """Reject old generic admissions without invalidating unrelated readers."""
    if not isinstance(evidence, dict):
        return False
    if evidence.get('methodology') == 'sec_tabular_reporting_v1' and 'table_reporting' not in evidence:
        return False
    if 'table_reporting' in evidence:
        from app.services.sec_tabular_reporting import cached_valid
        return (bool(evidence.get('cik')) and cached_valid(evidence['table_reporting'], cik=evidence['cik'])
                and evidence.get('reporting_currency') == evidence['table_reporting'].get('currency'))
    proof = evidence.get('inline_filing_supplement') or {}
    if not isinstance(proof, dict):
        return False
    if 'reconciled_revenue_periods' in proof or proof.get('methodology') == 'sec_reconciled_revenue_periods_v1':
        from app.services.sec_revenue_periods import cached_valid as periods_valid
        if not periods_valid(proof.get('reconciled_revenue_periods',{}), cik=proof.get('cik')):
            return False
    if 'revenue_series_selection' in proof:
        from app.services.sec_revenue_series import cached_valid as revenue_valid
        if not revenue_valid(proof['revenue_series_selection'], cik=proof.get('cik')):
            return False
    if 'operating_statement_history' in proof or proof.get('methodology') == 'sec_original_operating_history_v1':
        from app.services.sec_operating_history import cached_valid as operating_valid
        if not operating_valid(proof.get('operating_statement_history'), cik=proof.get('cik')):
            return False
    if 'primary_consolidated_income' in proof or proof.get('methodology') == 'sec_primary_consolidated_income_v1':
        from app.services.sec_primary_income import cached_valid as income_valid
        if not income_valid(proof.get('primary_consolidated_income'), cik=proof.get('cik')):
            return False
    if ('domestic_linked_statement' in proof or (proof.get('form') in {'10-K','10-K/A','10-Q','10-Q/A'}
            and proof.get('cover_evidence',{}).get('methodology')=='sec_inline_document_set_v1')):
        from app.services.sec_document_sets import domestic_cached_valid
        if not domestic_cached_valid(proof):return False
    if 'native_reporting' in proof:
        from app.services.sec_statement_currency import cached_currency_valid as native_valid
        return native_valid(evidence)
    if 'standard_foreign_reporting' not in proof:
        return True
    selection = proof['standard_foreign_reporting']
    try:
        verify_currency(proof)
        return isinstance(selection, dict) and selection.get('currency_policy') == CURRENCY_POLICY
    except TaggedForeignError:
        return False


def acquire(payload,company,*,symbol,cik,available_by,client,company_raw=None):
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_currency_ratios import basis
    if reviewed(symbol,cik) or basis(symbol,cik):return None
    selection=plan(payload,company,symbol=symbol,cik=cik,available_by=available_by)
    if selection is None:
        return _document_set(payload,company,company_raw,symbol=symbol,cik=cik,available_by=available_by,client=client)
    from app.services.sec_inline_financials import parse
    current=selection['current'];raw=client.get(current['url'])
    parsed=parse(raw,cik=selection['cik'],accession=current['accession'],filed=current['filed'],
        report_date=current['report_date'],form=current['form'],url=current['url'])
    verify_currency(parsed['evidence'])
    parsed['evidence']['standard_foreign_reporting']=selection
    return parsed


def authorized(payload,company,*,symbol,cik,available_by,supplement):
    """Recompute qualification and bind it to the independently parsed original."""
    proof=(supplement or {}).get('evidence',{})
    if proof.get('cover_evidence',{}).get('methodology')=='sec_inline_document_set_v1' or proof.get('unindexed_primary_discovery'):
        from app.services.sec_inline_financials import merge
        payload=merge(payload,supplement,available_by=available_by)
    selection=plan(payload,company,symbol=symbol,cik=cik,available_by=available_by)
    if selection is None:return None
    verify_currency(proof)
    current=selection['current']
    expected_url=current['url']
    primary=proof.get('unindexed_primary_discovery')
    if primary is not None:
        if (primary.get('cover_url')!=current['url'] or primary.get('cik')!=selection['cik']
                or any(primary.get(k)!=current[k] for k in ('accession','filed','report_date','form'))
                or not re.fullmatch(r'[0-9a-f]{64}',str(primary.get('directory_sha256','')))):
            raise TaggedForeignError('primary_original_proof_required')
    cover=proof.get('cover_evidence',{})
    if cover.get('methodology')=='sec_inline_document_set_v1':
        discovery=proof.get('document_set_discovery',{})
        expected_url=discovery.get('statement_url')
        if (cover.get('source_url')!=current['url'] or discovery.get('cover_url')!=current['url']
                or cover.get('exhibit_url')!=expected_url or cover.get('exhibit_sha256')!=proof.get('sha256')
                or any(discovery.get(k)!=current[k] for k in ('accession','filed','report_date','form'))
                or discovery.get('cik')!=selection['cik']
                or not re.fullmatch(r'[0-9a-f]{64}',str(cover.get('source_sha256','')))
                or not re.fullmatch(r'[0-9a-f]{64}',str(discovery.get('directory_sha256','')))):
            raise TaggedForeignError('document_set_original_proof_required')
    if (proof.get('standard_foreign_reporting')!=selection or proof.get('cik')!=selection['cik']
            or proof.get('namespace')!='us-gaap' or proof.get('currency')!='USD'
            or any(proof.get(k)!=current[k] for k in ('accession','filed','report_date','form'))
            or proof.get('url')!=expected_url
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



def _document_set(payload,company,company_raw,*,symbol,cik,available_by,client,candidate_accessions=None):
    """Recover an indexed foreign statement omitted from companyfacts.

    Only the existing USD US GAAP reporting class is admitted here. Native
    currency/IFRS basis selection remains separate from document discovery.
    """
    if company_raw is None or not any(f in {'6-K','6-K/A'} for f in company.get('filings',{}).get('recent',{}).get('form',[])):return None
    from app.services import sec_document_sets as sets,sec_inline_financials as inline,sec_foreign_statements as foreign
    if foreign.profile(symbol,cik):return None
    concepts=payload.get('facts',{}).get('us-gaap',{})
    if not any(concepts.get(tag,{}).get('units',{}).get('USD') for tag in BALANCE) or not any(concepts.get(tag,{}).get('units',{}).get('USD') for tag in DURATION):return None
    candidates=sets.candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    # Preserve the existing domestic reporting boundary even when companyfacts
    # has no qualifying foreign statement. Old 6-Ks must not supersede a 10-Q.
    from app.services.sec_reporting_policy import DOMESTIC_FORMS
    recent=company.get('filings',{}).get('recent',{})
    domestic_ends=[_day(end) for form,end,filed in zip(recent.get('form',[]),recent.get('reportDate',[]),recent.get('filingDate',[]))
        if form in DOMESTIC_FORMS and _day(end) and _day(filed) and _day(end)<=_day(filed)<=available_by]
    if domestic_ends:
        candidates=[c for c in candidates if _day(c['report_date'])>max(domestic_ends)]
    # A lagged report period is more likely to be a statement than a same-day
    # event. This controls bounded discovery order, never financial admission.
    candidates.sort(key=lambda c:(c['report_date']<c['filed'],c['report_date'],c['filed']),reverse=True)
    for candidate in sets.bounded_candidates(candidates,candidate_accessions):
        try:
            supplement=sets.acquire_candidate(candidate,company_raw,symbol=symbol,cik=cik,
                available_by=available_by,client=client)
        except inline.InlineFactsError as exc:
            if str(exc) in {'filing_namespaces','latest_reported_revenue_unavailable'}:continue
            raise
        if supplement is None:continue
        verify_currency(supplement['evidence'])
        effective=inline.merge(payload,supplement,available_by=available_by)
        selection=plan(effective,company,symbol=symbol,cik=cik,available_by=available_by)
        if selection is None:continue
        # A parsed older exhibit cannot authorize an unrelated current filing.
        if selection['current']['accession']!=candidate['accession']:
            raise TaggedForeignError('document_set_not_current_selection')
        supplement['evidence']['document_set_discovery_limit']={'candidate_bound':3,'total_candidates':len(candidates),'statement_complete':False}
        supplement['evidence']['standard_foreign_reporting']=selection
        return supplement
    return None
