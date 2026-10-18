"""Qualify complete HTML statements and normalize source-bound record series.

No invented XBRL tags, FX conversion, annualization or provider selection.
"""
from datetime import date
import hashlib
import json
import re
from lxml import html
from app.services import sec_tabular_financials as tables

VERSION = 'sec_tabular_reporting_v1'
METRICS = {
    'revenue': 'revenue', 'gross_profit': 'gross_profit',
    'operating_income': 'operating_income', 'parent_income': 'net_income',
    'current_assets': 'current_assets', 'current_liabilities': 'current_liabilities',
    'total_assets': 'statement_assets', 'operating_cash_flow': 'operating_cash_flow',
}
# Consolidated income and total equity are retained separately; neither is an
# implicit parent/common-holder numerator or denominator.
EXTRA = {'consolidated_income': 'consolidated_income', 'total_equity': 'consolidated_equity',
         'total_liabilities': 'total_liabilities'}


class TabularReportingError(ValueError):
    pass


def declaration(raw, observations):
    tree = html.fromstring(raw)
    for node in tree.xpath('//script|//style|//template|//blockquote'):
        node.drop_tree()
    text = tables.clean(' '.join(tree.itertext()))
    # Require an explicit basis for the financial statements/performance/results,
    # not a bare GAAP label or a reference to the jurisdiction of the issuer.
    subject = r'(?:financial statements|financial information|financial performance|results)'
    standard = (r'(?:U\.?S\.?\s+GAAP|accounting principles generally accepted in '
                r'the United States(?: of America)?|generally accepted accounting principles in '
                r'the United States(?: of America)?)')
    verb = r'(?:prepared(?: and presented)?|presented) in accordance with'
    pattern = subject + r'(?:(?![.;]).){0,100}?\b' + verb + r'\s+' + standard
    matches = [m.group(0) for m in re.finditer(pattern, text, re.I)
               if not re.search(r'\b(?:not|never|unlike)\b', m.group(0), re.I)]
    if not matches:
        raise TabularReportingError('table_accounting_basis_unproved')
    if re.search(subject + r'.{0,120}(?:' + verb + r'|comply with).{0,40}(?:IFRS|International Financial Reporting Standards)', text, re.I):
        raise TabularReportingError('table_accounting_basis_conflict')
    units = {f['currency'] for f in observations['facts']}
    translations = []
    if re.search(r'convenience of the readers?', text, re.I):
        # A full explicit from/to declaration is necessary. A parenthetical USD
        # amount or a market exchange-rate mention cannot select native currency.
        # Scan declaration verbs once, then resolve each explicit currency.
        # Re-scanning the entire release for every currency pair adds hundreds
        # of regex passes to each scheduled preparation.
        atom = r'(?<![A-Za-z])(?:' + '|'.join(tables.CURRENCIES.values()) + r')(?![A-Za-z])'
        currency = atom + r'(?:\s*\(' + atom + r'\))?'
        pattern = (r'\b(?:translations?\s+(?:of (?:(?![.;]).){0,230}?\s+)?from|conversion\s+of)\s+'
                   r'(?P<source>' + currency + r')\s+(?:amounts?\s+)?(?:into|to)\s+'
                   r'(?P<destination>' + currency + r')')
        for match in re.finditer(pattern, text, re.I):
            context = text[max(0, match.start()-250):match.end()+350]
            prefix = text[max(0, match.start()-40):match.start()]
            if re.search(r'\b(?:no|not|never|hypothetical)\s+(?:\w+\s+){0,3}$', prefix, re.I):
                continue
            origin = tables.currencies(match.group('source'))
            destination = tables.currencies(match.group('destination'))
            # A parenthetical alias must identify the same currency. Mixed
            # aliases, ambiguous directions and non-convenience FX stay held.
            if len(origin) != 1 or len(destination) != 1 or origin == destination:
                raise TabularReportingError('table_translation_basis_unproved')
            if re.search(r'convenience of the readers?', context, re.I):
                translations.append({'currency':next(iter(origin)),
                    'convenience_currency':next(iter(destination)), 'declaration':match.group(0)})
        pairs = {(r['currency'], r['convenience_currency']) for r in translations}
        if len(pairs) != 1:
            raise TabularReportingError('table_translation_basis_unproved')
        currency, convenience = next(iter(pairs))
        if currency not in units or not units <= {currency, convenience}:
            raise TabularReportingError('table_translation_columns_unproved')
    elif len(units) == 1:
        currency = next(iter(units))
    else:
        raise TabularReportingError('table_reporting_currency_unproved')
    return {'version': VERSION, 'accounting_basis': 'US GAAP', 'currency': currency,
            'accounting_declarations': list(dict.fromkeys(matches)),
            'currency_basis': 'explicit_translation' if translations else 'single_explicit_statement_currency',
            'translations': translations}


def normalize(raw, *, source_url, company_raw, cover_raw, cover_url, symbol, cik, available_by):
    """Reparse complete originals; caller-supplied extracted values are not trusted.

    Records match Walnut's period-keyed record contract but use honest html:
    labels and provenance rather than claiming these values were tagged XBRL.
    """
    observations = tables.extract(raw, source_url=source_url, company_raw=company_raw,
        cover_raw=cover_raw, cover_url=cover_url, symbol=symbol, cik=cik, available_by=available_by)
    if not observations['tables'] and not observations['holds']:
        raise TabularReportingError('table_no_statements')
    basis = declaration(raw, observations)
    source = observations['source']
    chosen = [f for f in observations['facts'] if f['currency'] == basis['currency']]
    if not chosen:
        raise TabularReportingError('table_native_observations_unavailable')
    balance_ends = {f['end'] for f in chosen if f['start'] is None}
    income_ends = {f['end'] for f in chosen if f['metric'] in {'revenue','consolidated_income','parent_income'}}
    if not balance_ends or not income_ends or max(balance_ends) != max(income_ends):
        raise TabularReportingError('table_current_statement_alignment')
    end = date.fromisoformat(max(balance_ends))
    if not 0 <= (available_by - end).days <= 180:
        raise TabularReportingError('table_current_statement_stale')
    # Never let a historic 6-K replace a newer mandatory periodic statement.
    company = json.loads(company_raw)
    recent = company['filings']['recent']
    for form, period, filed in zip(recent['form'], recent['reportDate'], recent['filingDate']):
        if form not in {'10-K','10-Q','10-K/A','10-Q/A','20-F','20-F/A','40-F','40-F/A'}:
            continue
        try: report, filing = date.fromisoformat(period), date.fromisoformat(filed)
        except (ValueError, TypeError): continue
        if end < report <= filing <= available_by:
            raise TabularReportingError('table_newer_periodic_statement_exists')
    records = {name:{} for name in set(METRICS.values()) | set(EXTRA.values()) | {'equity','capex','eps'}}
    roles = METRICS | EXTRA
    for fact in chosen:
        role = roles.get(fact['metric'])
        if role is None:
            continue
        start = date.fromisoformat(fact['start']) if fact['start'] else None
        finish = date.fromisoformat(fact['end'])
        records[role][start,finish] = {'value':fact['value'], 'tag':'html:'+fact['metric'],
            'unit':basis['currency'], 'start':fact['start'], 'end':fact['end'],
            'filed':source['filed'], 'accession':source['accession'], 'priority':0,
            'source_kind':'html_statement', 'source_url':source_url,
            'source_sha256':source['statement_sha256'], 'source_cik':source['cik'],
            'source_occurrences':fact['occurrences']}
    for conflict in observations['conflicts']:
        if conflict['currency'] != basis['currency'] or conflict['metric'] not in roles:
            continue
        key = (date.fromisoformat(conflict['start']) if conflict['start'] else None,
               date.fromisoformat(conflict['end']))
        records[roles[conflict['metric']]][key] = None
    return {'records': records, 'evidence': {'methodology':VERSION, **basis,
        'source': source, 'current_period':str(end), 'available_by':str(available_by),
        'native_observation_count':len(chosen), 'source_observation_count':len(observations['facts']),
        'conflicts':observations['conflicts'], 'holds':observations['holds'],
        'limits':['No companyfacts merge, inferred parent/common income, FX, EPS or annualization.',
                  'This normalization does not establish complete filing discovery or publication readiness.']}}



def acquire(company_raw, *, symbol, cik, available_by, client):
    """Inspect at most three filing covers and one exhibit each, caller budgeted."""
    from urllib.parse import urljoin
    from app.services import sec_document_sets as sets
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_foreign_statements import profile
    from app.services.sec_currency_ratios import basis
    if reviewed(symbol,cik) or profile(symbol,cik) or basis(symbol,cik):
        return None
    if not any(f in {'6-K','6-K/A'} for f in json.loads(company_raw).get('filings',{}).get('recent',{}).get('form',[])):
        return None
    candidates=sets.candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    candidates.sort(key=lambda c:(c['report_date']<c['filed'],c['report_date'],c['filed']),reverse=True)
    accepted=[];holds=[]
    for candidate in candidates[:3]:
        cover=client.get(candidate['cover_url'])
        if len(cover)>2_000_000:
            holds.append({'accession':candidate['accession'],'reason':'cover_size'});continue
        tree=html.fromstring(cover)
        if tree.xpath('//base'):
            raise TabularReportingError('table_cover_base')
        links=set(); statement_links=set()
        for node in tree.xpath('//a[@href]'):
            label=tables.clean(' '.join(node.itertext()))
            row=node.xpath('ancestor::tr[1]')
            if row and len(row[0].xpath('.//a[@href]'))==1:
                label=tables.clean(' '.join(row[0].itertext()))
            if re.search(r'\b(?:to|will)\s+(?:announce|report)\b|\b(?:date|schedule)\s+(?:of|for)\b',label,re.I):
                continue
            if re.search(r'\bfinancial\s+(?:statements?|reports?|results?)\b|\b(?:press|earnings)\s+release\b',label,re.I):
                url=urljoin(candidate['cover_url'],node.get('href'));links.add(url)
                if re.search(r'\bfinancial statements?\b',label,re.I):statement_links.add(url)
        links=statement_links or links
        if not links:
            continue
        if len(links)!=1:
            holds.append({'accession':candidate['accession'],'reason':'table_exhibit_ambiguous'});continue
        url=next(iter(links))
        from app.services.sec_statement_currency import source_identity
        if source_identity(url)!=(candidate['cik'],candidate['accession']):
            raise TabularReportingError('table_exhibit_scope')
        raw=client.get(url)
        args=dict(source_url=url,company_raw=company_raw,cover_raw=cover,
            cover_url=candidate['cover_url'],symbol=symbol,cik=cik,available_by=available_by)
        try: result=normalize(raw,**args)
        except (tables.TabularFinancialError,TabularReportingError) as exc:
            if str(exc)!='table_no_statements':
                holds.append({'accession':candidate['accession'],'filed':candidate['filed'],'reason':str(exc)})
            continue
        accepted.append((result,raw,args))
    if not accepted:
        return None
    # Current and amended statements are ordered by their printed period then
    # filing availability. Equal-date disagreements need explicit review.
    rank=lambda item:(item[0]['evidence']['current_period'],item[0]['evidence']['source']['filed'])
    latest=max(rank(item) for item in accepted)
    selected=[item for item in accepted if rank(item)==latest]
    if len(selected)!=1:
        raise TabularReportingError('table_current_selection_ambiguous')
    result,raw,args=selected[0]
    if any(h.get('filed','9999')>result['evidence']['current_period'] for h in holds):
        # A possibly relevant inspected statement was not qualified; do not
        # fall back to an older result while concealing that gap.
        raise TabularReportingError('table_discovery_has_unqualified_statement')
    proof=result['evidence']
    proof['discovery']={'candidate_bound':3,'total_candidates':len(candidates),
        'inspected':len(candidates[:3]),'holds':holds,'whole_universe_complete':False}
    return {'evidence':{'table_reporting':proof},'table_original':raw,
            'table_args':args}


def validate(supplement, *, company_raw, symbol, cik, available_by):
    """Recompute normalized values from originals before consumer use."""
    if not isinstance(supplement,dict) or 'table_reporting' not in supplement.get('evidence',{}):
        raise TabularReportingError('table_reporting_required')
    if not isinstance(supplement.get('table_args'),dict) or not isinstance(supplement.get('table_original'),bytes):
        raise TabularReportingError('table_original_required')
    args=dict(supplement['table_args'])
    if args['company_raw']!=company_raw or args['symbol']!=symbol or str(args['cik']).zfill(10)!=str(cik).zfill(10):
        raise TabularReportingError('table_consumer_identity')
    args['available_by']=available_by
    result=normalize(supplement['table_original'],**args)
    proof=dict(supplement['evidence']['table_reporting']);proof.pop('discovery',None)
    if result['evidence']!=proof:
        raise TabularReportingError('table_original_evidence_changed')
    return result


def core(facts_raw,company_raw,*,symbol,cik,observed_at,supplement):
    """Publish supported canonical metrics without annualizing shorter periods."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from app.services.sec_fundamentals import SecFundamentalsCoverageError
    if observed_at.tzinfo is None:
        raise TabularReportingError('table_observation_naive')
    available=observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    result=validate(supplement,company_raw=company_raw,symbol=symbol,cik=cik,available_by=available)
    proof=result['evidence'];records=result['records'];end=date.fromisoformat(proof['current_period'])
    company=json.loads(company_raw)
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=str(cik).zfill(10):
        raise TabularReportingError('table_companyfacts_identity')
    assets=records['current_assets'].get((None,end));liabilities=records['current_liabilities'].get((None,end))
    if not assets or not liabilities or assets['value']<0 or liabilities['value']<=0:
        raise SecFundamentalsCoverageError('No currently supported table financial ratios')
    tickers,exchanges=company['tickers'],company.get('exchanges',[])
    evidence={'methodology':VERSION,'financial_status':'partial','cik':str(cik).zfill(10),
        'reporting_currency':proof['currency'],'available_by':str(available),'period_end':str(end),
        'current':{},'prior':{},'instant':{'current_assets':assets,'current_liabilities':liabilities},
        'calculations':{'current_ratio':'period-end current assets / current liabilities'},
        'table_reporting':proof,'limitations':['TTM, cash, income, per-share and market ratios require separately qualified history.']}
    return {'symbol':symbol,'provider':'sec_edgar','company_name':company['name'],
        'fetched_at':observed_at,'period_date':end,'status':'ok','error':None,
        'exchange':exchanges[tickers.index(symbol)] if len(exchanges)==len(tickers) else None,
        'current_ratio':assets['value']/liabilities['value'],
        'source_evidence_json':json.dumps(evidence,sort_keys=True)}


def panel(facts_raw,company_raw,*,symbol,cik,observed_at,supplement):
    """Use the existing public panel shape, preserving partial source coverage."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from app.services.ticker_financials import _unavailable,_subsection
    from app.services.sec_financial_statements import period_value
    from app.services.sec_fundamentals import SecFundamentalsCoverageError
    if observed_at.tzinfo is None:
        raise TabularReportingError('table_observation_naive')
    available=observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    result=validate(supplement,company_raw=company_raw,symbol=symbol,cik=cik,available_by=available)
    records=result['records'];proof=result['evidence'];currency=proof['currency']
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=str(cik).zfill(10):
        raise TabularReportingError('table_companyfacts_identity')
    rows={'annual':[],'quarterly':[],'semiannual':[]};periods={}
    for (start,end),revenue in sorted(records['revenue'].items()):
        if not start or not revenue:
            continue
        days=(end-start).days+1
        kind='annual' if 350<=days<=380 else 'quarterly' if 70<=days<=110 else 'semiannual' if 160<=days<=200 else None
        if kind is None:continue
        points={name:period_value(records[name],start,end) for name in ('revenue','gross_profit','operating_income','net_income','operating_cash_flow')}
        val=lambda name:points[name]['value'] if points[name] else None
        margin=lambda name:val(name)/revenue['value']*100 if val(name) is not None and revenue['value']>0 else None
        row={'period':('Year' if kind=='annual' else 'Quarter' if kind=='quarterly' else 'Half-year')+' ended '+str(end),
            'date':str(end),'periodStart':str(start),'currency':currency,'source':'sec_edgar',
            'revenue':revenue['value'],'netIncome':val('net_income'),'eps':None,
            'grossMargin':margin('gross_profit'),'operatingMargin':margin('operating_income'),
            'operatingCashFlow':val('operating_cash_flow'),'capex':None,'freeCashFlow':None}
        rows[kind].append(row);periods[kind+':'+str(end)]=points
    for kind in rows:rows[kind]=rows[kind][-6 if kind=='annual' else -12:]
    # Existing product convention shows half-years when no quarters exist.
    if rows['quarterly']:rows['semiannual']=[]
    present=any(rows.values())
    payload=_unavailable(symbol,message='Reported financial statements with partial coverage; TTM and per-share values require additional evidence.',reason='partial_sec_coverage')
    payload.update(**rows,source='sec_edgar',companyName=json.loads(company_raw)['name'],
        updatedAt=observed_at.isoformat(),status='partial' if present else 'unavailable',unavailable=not present,
        reportingCurrency=currency,preparationVersion=VERSION,
        reportingBasis='Reported '+currency+' '+proof['accounting_basis']+' financial tables. Net income is shown only for explicit parent-holder income.',
        sourceEvidence={'methodology':VERSION,'tableReporting':proof,'periods':periods})
    if present:
        payload['sections']['income']='partial';payload['subsections']['income']=_subsection(status='limited',reason_code='partial_sec_coverage',data=rows)
    if any(row['operatingCashFlow'] is not None for group in rows.values() for row in group):
        payload['sections']['cashFlow']='partial';payload['subsections']['cash_flow']=_subsection(status='limited',reason_code='partial_sec_coverage',data=rows)
    try:values=core(facts_raw,company_raw,symbol=symbol,cik=cik,observed_at=observed_at,supplement=supplement)
    except SecFundamentalsCoverageError:values={}
    if 'current_ratio' in values:
        payload['summary']['currentRatio']=values['current_ratio'];payload['health']['currentRatio']=values['current_ratio']
        payload['sections']['health']='partial';payload['subsections']['health']=_subsection(status='limited',reason_code='partial_sec_coverage',data=payload['health'])
    return payload



def cached_valid(proof, *, symbol=None, cik=None):
    if not isinstance(proof,dict):return False
    try:
        source=proof['source']
        from app.services.sec_statement_currency import source_identity
        return (proof['methodology']==VERSION and proof['version']==VERSION
            and proof['accounting_basis']=='US GAAP' and proof['accounting_declarations']
            and proof['currency'] in tables.CURRENCIES
            and (symbol is None or source.get('symbol') == symbol)
            and (cik is None or source['cik'] == str(cik).zfill(10))
            and source_identity(source['statement_url'])==(source['cik'],source['accession'])
            and source_identity(source['cover_url'])==(source['cik'],source['accession'])
            and all(re.fullmatch(r'[0-9a-f]{64}',source[k]) for k in ('statement_sha256','cover_sha256','directory_sha256'))
            and date.fromisoformat(proof['current_period'])<=date.fromisoformat(source['filed'])<=date.fromisoformat(proof['available_by']))
    except (KeyError,TypeError,ValueError):return False
