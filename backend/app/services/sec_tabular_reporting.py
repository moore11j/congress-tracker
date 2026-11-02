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
    text = tables.visible_text(tree)
    # Require an explicit basis for the financial statements/performance/results,
    # not a bare GAAP label or a reference to the jurisdiction of the issuer.
    subject = r'(?:financial statements|financial information|financial performance|results)'
    standard = (r'(?:U\.?S\.?\s+GAAP|accounting principles generally accepted in '
                r'the United States(?: of America)?|generally accepted accounting principles in '
                r'the United States(?: of America)?|(?:U\.?S\.?|United States) generally accepted accounting principles)')
    verb = r'(?:prepared(?: and presented)?|presented) in accordance with'
    pattern = subject + r'(?:(?![.;]).){0,100}?\b' + verb + r'\s+' + standard
    # A release can explicitly name the US GAAP basis of its comparable
    # financial measures without repeating the prepared-in-accordance wording.
    comparable = r'most directly comparable financial measures under\s+' + standard
    matches = [m.group(0) for m in re.finditer(pattern + '|' + comparable, text, re.I)
               if not re.search(r'\b(?:not|never|unlike)\b', m.group(0), re.I)
               and not re.search(r'\b(?:not|never|unlike)\s+(?:\w+\s+){0,3}$',
                                 text[max(0,m.start()-70):m.start()],re.I)]
    from app.services.sec_statement_declarations import declarations, POLICY
    ifrs_matches = declarations(text)
    accounting_basis = 'US GAAP'
    if not matches:
        if not ifrs_matches:
            raise TabularReportingError('table_accounting_basis_unproved')
        matches = ifrs_matches
        accounting_basis = 'IFRS'
    elif ifrs_matches:
        raise TabularReportingError('table_accounting_basis_conflict')
    if accounting_basis == 'US GAAP' and re.search(subject + r'.{0,120}(?:' + verb + r'|comply with).{0,40}(?:IFRS|International Financial Reporting Standards)', text, re.I):
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
        legacy_start = r'(?:translations?\s+(?:of (?:(?![.;]).){0,230}?\s+)?from|conversion\s+of)'
        pattern = (r'\b(?:translations?\s+of(?:\s+certain)?|' + legacy_start + r')\s+'
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
        # Check every explicit direction above, but preserve established proof
        # bytes when an already supported declaration exists. Newly recognized
        # equivalent wording must not invalidate an otherwise valid cache.
        legacy = [r for r in translations if re.match(legacy_start + r'\b', r['declaration'], re.I)]
        translations = legacy or translations
    elif len(units) == 1:
        currency = next(iter(units))
    else:
        raise TabularReportingError('table_reporting_currency_unproved')
    return {'version': VERSION, 'accounting_basis': accounting_basis, 'currency': currency,
            **({'accounting_policy': POLICY} if accounting_basis == 'IFRS' else {}),
            'accounting_declarations': list(dict.fromkeys(matches)),
            'currency_basis': 'explicit_translation' if translations else 'single_explicit_statement_currency',
            'translations': translations}


def normalize(raw, *, source_url, company_raw, cover_raw, cover_url, symbol, cik, available_by, basis_facts_raw=None):
    """Reparse complete originals; caller-supplied extracted values are not trusted.

    Records match Walnut's period-keyed record contract but use honest html:
    labels and provenance rather than claiming these values were tagged XBRL.
    """
    observations = tables.extract(raw, source_url=source_url, company_raw=company_raw,
        cover_raw=cover_raw, cover_url=cover_url, symbol=symbol, cik=cik, available_by=available_by)
    if not observations['tables'] and not observations['holds']:
        raise TabularReportingError('table_no_statements')
    try:
        basis = declaration(raw, observations)
    except TabularReportingError as exc:
        if str(exc)!='table_accounting_basis_unproved' or basis_facts_raw is None:raise
        from app.services import sec_comparative_basis
        tree=html.fromstring(raw)
        for node in tree.xpath('//script|//style|//template|//blockquote'):node.drop_tree()
        try:
            basis={'version':VERSION,**sec_comparative_basis.prove(tables.visible_text(tree),observations,
                company_raw=company_raw,facts_raw=basis_facts_raw,available_by=available_by)}
        except sec_comparative_basis.ComparativeBasisError:
            raise exc
    from app.services.sec_foreign_statements import profile
    annual_profile=profile(symbol,cik)
    if annual_profile and (basis['currency']!=annual_profile['currency']
            or basis['accounting_basis']!={'us-gaap':'US GAAP','ifrs-full':'IFRS'}.get(annual_profile['namespace'])):
        raise TabularReportingError('table_annual_reporting_basis_conflict')
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
    from app.services import sec_statement_shareholder as shareholder
    try:
        note = shareholder.enrich(raw,observations,issuer=company['name'],
            filed=date.fromisoformat(source['filed']),currency=basis['currency'])
    except ValueError as exc:
        # A note hold cannot relabel consolidated profit or erase independent
        # primary balance/cash-flow evidence.
        note={'policy':shareholder.POLICY,'status':'held','reason':str(exc),'facts':[]}
    if note:
        chosen=chosen+note['facts']
    records = {name:{} for name in set(METRICS.values()) | set(EXTRA.values()) | {'equity','capex','eps'}}
    roles = METRICS | EXTRA | {'eps':'eps'}
    for fact in chosen:
        role = roles.get(fact['metric'])
        if role is None:
            continue
        start = date.fromisoformat(fact['start']) if fact['start'] else None
        finish = date.fromisoformat(fact['end'])
        records[role][start,finish] = {'value':fact['value'], 'tag':'html:'+fact['metric'],
            'unit':basis['currency']+('/shares' if fact['metric']=='eps' else ''), 'start':fact['start'], 'end':fact['end'],
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
        **({'shareholder_note':note} if note else {}),
        'limits':[('No companyfacts merge, inferred parent/common income, FX, share-count inference or annualization.'
                   if note and note['facts'] else 'No companyfacts merge, inferred parent/common income, FX, EPS or annualization.'),
                  'This normalization does not establish complete filing discovery or publication readiness.']}}



def primary_statement_candidate(tree):
    # Only an explicit statement heading triggers primary-document inspection;
    # ordinary cover tables and shareholder-vote tables are not statements.
    for node in tree.xpath('//*[self::p or self::div or self::td or self::th]'
                           '[not(ancestor::blockquote)][not(.//table)]'
                           '[not(.//p)][not(.//div)]'):
        text=tables.visible_text(node)
        if len(text)<=240 and tables.table_kind(text) is not None:return True
    return False


def statement_link_candidates(tree, cover_url):
    """Return explicitly labelled links, preferring actual financial statements.

    This identifies candidates only. Original identity, accounting basis,
    currency and printed periods still require full statement validation.
    """
    from urllib.parse import urljoin, urldefrag
    links={}
    priority={'financial_statement':0,'financial_results':1,'press_release':2}
    for node in tree.xpath('//a[@href]'):
        label=tables.visible_text(node)
        row=node.xpath('ancestor::tr[1]')
        if row and len(row[0].xpath('.//a[@href]'))==1:
            label=tables.clean(' '.join(row[0].itertext()))
        if re.search(r'\b(?:to|will)\s+(?:announce|report)\b|\b(?:date|schedule)\s+(?:of|for)\b',label,re.I):
            continue
        periodic=re.search(r'\bquarterly\s+statements?\b|\bhalf[ -]year\s+reports?\b',label,re.I)
        if periodic or re.search(r'\bfinancial\s+(?:statements?|reports?|results?)\b|\b(?:press|earnings)\s+release\b',label,re.I):
            url=urljoin(cover_url,node.get('href'))
            if urldefrag(url)[0]==cover_url:continue
            kind=('financial_statement' if periodic or re.search(r'\bfinancial statements?\b',label,re.I)
                  else 'financial_results' if re.search(r'\bfinancial\s+(?:reports?|results?)\b|\bearnings\s+release\b',label,re.I)
                  else 'press_release')
            if url not in links or priority[kind]<priority[links[url]]:links[url]=kind
    if 'financial_statement' in links.values():
        links={u:k for u,k in links.items() if k=='financial_statement'}
    return [{'url':u,'kind':links[u]} for u in sorted(links)]


def statement_links(tree, cover_url):
    return {item['url'] for item in statement_link_candidates(tree,cover_url)}


def acquire(company_raw, *, symbol, cik, available_by, client, candidate_accessions=None, inspection=None, annual_rollover=False, basis_facts_raw=None):
    """Inspect at most three filing covers and one exhibit each, caller budgeted."""
    from app.services import sec_document_sets as sets
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_foreign_statements import profile
    from app.services.sec_currency_ratios import basis
    if reviewed(symbol,cik) or (profile(symbol,cik) and not annual_rollover) or basis(symbol,cik):
        return None
    if not any(f in {'6-K','6-K/A'} for f in json.loads(company_raw).get('filings',{}).get('recent',{}).get('form',[])):
        return None
    candidates=sets.candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    candidates.sort(key=lambda c:(c['report_date']<c['filed'],c['report_date'],c['filed']),reverse=True)
    selected_candidates=sets.bounded_candidates(candidates,candidate_accessions)
    accepted=[];holds=[];held_sources={}
    def prepare_original(raw,args):
        result=normalize(raw,**args,basis_facts_raw=basis_facts_raw)
        # Retain annual originals only when this source used reconciliation.
        # Existing explicit-basis supplements retain their exact shape.
        if 'comparative_basis' in result['evidence']:
            args={**args,'basis_facts_raw':basis_facts_raw}
        return result,args
    issuer=json.loads(company_raw)['name']
    def statement_hold(raw,candidate,reason,source_url):
        hold={'accession':candidate['accession'],'filed':candidate['filed'],'reason':reason}
        if reason in {'table_accounting_basis_unproved','table_reporting_currency_unproved','table_native_observations_unavailable'}:
            held_sources[id(hold)]=(raw,source_url)
        return hold
    for candidate in selected_candidates:
        cover=client.get(candidate['cover_url'])
        if len(cover)>tables.MAX_BYTES:
            holds.append({'accession':candidate['accession'],'reason':'source_size'});continue
        tree=html.fromstring(cover)
        if tree.xpath('//base'):
            raise TabularReportingError('table_cover_base')
        primary=primary_statement_candidate(tree)
        if primary:
            args=dict(source_url=candidate['cover_url'],company_raw=company_raw,cover_raw=cover,
                cover_url=candidate['cover_url'],symbol=symbol,cik=cik,available_by=available_by)
            try:result,args=prepare_original(cover,args)
            except (tables.TabularFinancialError,TabularReportingError) as exc:
                if str(exc)!='table_no_statements':
                    holds.append(statement_hold(cover,candidate,str(exc),candidate['cover_url']))
            else:accepted.append((result,cover,args))
        elif len(cover)>2_000_000:
            holds.append({'accession':candidate['accession'],'reason':'cover_size'});continue
        links=statement_links(tree,candidate['cover_url'])
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
        try: result,args=prepare_original(raw,args)
        except (tables.TabularFinancialError,TabularReportingError) as exc:
            if str(exc)!='table_no_statements':
                holds.append(statement_hold(raw,candidate,str(exc),url))
            continue
        accepted.append((result,raw,args))
    if inspection is not None:
        if not isinstance(inspection,dict):raise TabularReportingError('table_inspection_invalid')
        inspection.update(inspected_accessions=[c['accession'] for c in selected_candidates],
            holds=[dict(h) for h in holds],accepted_count=len(accepted),complete_universe_coverage=False)
    if not accepted:
        return None
    # Current and amended statements are ordered by their printed period then
    # filing availability. Equal-date disagreements need explicit review.
    rank=lambda item:(item[0]['evidence']['current_period'],item[0]['evidence']['source']['filed'])
    latest=max(rank(item) for item in accepted)
    selected=[item for item in accepted if rank(item)==latest]
    corroboration=None;corroborating_originals=[]
    if len(selected)>1:
        from app.services.sec_tabular_corroboration import select
        chosen,corroboration=select(selected)
        corroborating_originals=[{'raw':raw,'args':args} for i,(_,raw,args) in enumerate(selected) if i!=chosen]
    else:chosen=0
    result,raw,args=selected[chosen]
    def superseded(hold):
        # Reparse only a hold that could actually block an accepted newer filing.
        # Failed-only scopes and already older disclosure dates need no second pass.
        original=held_sources.get(id(hold))
        if original is None or hold.get('filed','9999')>=result['evidence']['source']['filed']:return False
        raw,source_url=original
        period=tables.inspect_statement_period(raw,issuer=issuer,filed=date.fromisoformat(hold['filed']))
        if period is None:return False
        hold['period_evidence']={**period,'source_url':source_url}
        return period['end']<=result['evidence']['current_period']
    if any(h.get('filed','9999')>result['evidence']['current_period'] and not superseded(h) for h in holds):
        # A possibly relevant inspected statement was not qualified; do not
        # fall back to an older result while concealing that gap.
        raise TabularReportingError('table_discovery_has_unqualified_statement')
    proof=result['evidence']
    proof['discovery']={'candidate_bound':3,'total_candidates':len(candidates),
        'inspected':len(selected_candidates),'holds':holds,'whole_universe_complete':False}
    if candidate_accessions is not None:
        proof['discovery']['explicit_candidate_scope']=list(candidate_accessions)
    supplement={'evidence':{'table_reporting':proof},'table_original':raw,'table_args':args}
    if corroboration is not None:
        proof['discovery']['corroboration']=corroboration
        supplement['table_corroborating_originals']=corroborating_originals
    return supplement


def validate(supplement, *, company_raw, symbol, cik, available_by, facts_raw=None):
    """Recompute normalized values from originals before consumer use."""
    if not isinstance(supplement,dict) or 'table_reporting' not in supplement.get('evidence',{}):
        raise TabularReportingError('table_reporting_required')
    if not isinstance(supplement.get('table_args'),dict) or not isinstance(supplement.get('table_original'),bytes):
        raise TabularReportingError('table_original_required')
    args=dict(supplement['table_args'])
    if 'basis_facts_raw' in args and facts_raw is not None and args['basis_facts_raw']!=facts_raw:
        raise TabularReportingError('table_comparative_facts_changed')
    if args['company_raw']!=company_raw or args['symbol']!=symbol or str(args['cik']).zfill(10)!=str(cik).zfill(10):
        raise TabularReportingError('table_consumer_identity')
    args['available_by']=available_by
    result=normalize(supplement['table_original'],**args)
    proof=dict(supplement['evidence']['table_reporting']);discovery=proof.pop('discovery',{})
    peers=supplement.get('table_corroborating_originals')
    corroboration=discovery.get('corroboration')
    if peers is not None or corroboration is not None:
        if not isinstance(peers,list) or not 1<=len(peers)<=5 or not corroboration:
            raise TabularReportingError('table_corroboration_originals_required')
        items=[(result,supplement['table_original'],args)]
        for peer in peers:
            peer_args=dict(peer['args'])
            if peer_args['company_raw']!=company_raw or peer_args['symbol']!=symbol or str(peer_args['cik']).zfill(10)!=str(cik).zfill(10):
                raise TabularReportingError('table_consumer_identity')
            if 'basis_facts_raw' in peer_args and facts_raw is not None and peer_args['basis_facts_raw']!=facts_raw:
                raise TabularReportingError('table_comparative_facts_changed')
            peer_args['available_by']=available_by
            items.append((normalize(peer['raw'],**peer_args),peer['raw'],peer_args))
        from app.services.sec_tabular_corroboration import select
        chosen,checked=select(items)
        if chosen!=0 or checked!=corroboration:
            raise TabularReportingError('table_corroboration_evidence_changed')
    if result['evidence']!=proof:
        raise TabularReportingError('table_original_evidence_changed')
    if corroboration is not None:
        # Preserve only the proof recomputed from complete originals above.
        result['evidence']['corroboration']=checked
    return result


def core(facts_raw,company_raw,*,symbol,cik,observed_at,supplement):
    """Publish supported canonical metrics without annualizing shorter periods."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from app.services.sec_fundamentals import SecFundamentalsCoverageError
    if observed_at.tzinfo is None:
        raise TabularReportingError('table_observation_naive')
    available=observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    result=validate(supplement,company_raw=company_raw,symbol=symbol,cik=cik,available_by=available,facts_raw=facts_raw)
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
    result=validate(supplement,company_raw=company_raw,symbol=symbol,cik=cik,available_by=available,facts_raw=facts_raw)
    records=result['records'];proof=result['evidence'];currency=proof['currency']
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=str(cik).zfill(10):
        raise TabularReportingError('table_companyfacts_identity')
    rows={'annual':[],'quarterly':[],'semiannual':[]};periods={}
    period_keys={(start,end) for name in ('revenue','net_income','eps','operating_cash_flow')
                 for (start,end),point in records[name].items() if start and point}
    for start,end in sorted(period_keys):
        revenue=records['revenue'].get((start,end))
        days=(end-start).days+1
        kind='annual' if 350<=days<=380 else 'quarterly' if 70<=days<=110 else 'semiannual' if 160<=days<=200 else None
        if kind is None:continue
        points={name:period_value(records[name],start,end) for name in ('revenue','gross_profit','operating_income','net_income','operating_cash_flow')}
        if records['eps']:
            # Per-share amounts are never summed or derived from YTD differences.
            points['eps']=records['eps'].get((start,end))
        val=lambda name:points.get(name)['value'] if points.get(name) else None
        margin=lambda name:val(name)/revenue['value']*100 if val(name) is not None and revenue and revenue['value']>0 else None
        row={'period':('Year' if kind=='annual' else 'Quarter' if kind=='quarterly' else 'Half-year')+' ended '+str(end),
            'date':str(end),'periodStart':str(start),'currency':currency,'source':'sec_edgar',
            'revenue':revenue['value'] if revenue else None,'netIncome':val('net_income'),'eps':val('eps'),
            'grossMargin':margin('gross_profit'),'operatingMargin':margin('operating_income'),
            'operatingCashFlow':val('operating_cash_flow'),'capex':None,'freeCashFlow':None}
        rows[kind].append(row);periods[kind+':'+str(end)]=points
    for kind in rows:rows[kind]=rows[kind][-6 if kind=='annual' else -12:]
    # Existing product convention shows half-years when no quarters exist.
    # Preserve old revenue-led presentation; a partial income/OCF panel must
    # retain half-year source values that its quarterly rows cannot express.
    if rows['quarterly'] and records['revenue']:rows['semiannual']=[]
    present=any(rows.values())
    payload=_unavailable(symbol,message='Reported financial statements with partial coverage; TTM and per-share values require additional evidence.',reason='partial_sec_coverage')
    payload.update(**rows,source='sec_edgar',companyName=json.loads(company_raw)['name'],
        updatedAt=observed_at.isoformat(),status='partial' if present else 'unavailable',unavailable=not present,
        reportingCurrency=currency,preparationVersion=VERSION,
        reportingBasis='Reported '+currency+' '+proof['accounting_basis']+' financial tables. Net income is shown only for explicit parent-holder income.',
        sourceEvidence={'methodology':VERSION,'tableReporting':proof,'periods':periods})
    if records['eps']:
        payload['reportingBasis']+=' EPS is reported diluted earnings per common share in '+currency+'.'
    if proof.get('accounting_basis_origin')=='annual_comparative_reconciliation':
        payload['reportingBasis']='Reported USD GAAP financial tables; US GAAP basis supported by exact annual comparative reconciliation. Net income requires explicit parent-holder income.'
    has_income=any(any(row[k] is not None for k in ('revenue','netIncome','eps','grossMargin','operatingMargin'))
                   for group in rows.values() for row in group)
    if has_income:
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
        if ('comparative_basis' in proof) != (proof.get('accounting_basis_origin')=='annual_comparative_reconciliation'):return False
        if 'comparative_basis' in proof:
            if proof['accounting_basis']!='US GAAP' or proof['currency']!='USD':return False
            from app.services.sec_comparative_basis import cached_valid as comparative_valid
            if not comparative_valid(proof['comparative_basis'],source,proof['current_period']):return False
        if 'corroboration' in proof:
            from app.services.sec_tabular_corroboration import cached_valid as corroboration_valid
            if not corroboration_valid(proof['corroboration'],source):return False
        from app.services.sec_statement_currency import source_identity
        return (proof['methodology']==VERSION and proof['version']==VERSION
            and (proof['accounting_basis']=='US GAAP' or (proof['accounting_basis']=='IFRS'
                 and proof.get('accounting_policy')=='explicit_ifrs_statement_declarations_v1')) and proof['accounting_declarations']
            and proof['currency'] in tables.CURRENCIES
            and (symbol is None or source.get('symbol') == symbol)
            and (cik is None or source['cik'] == str(cik).zfill(10))
            and source_identity(source['statement_url'])==(source['cik'],source['accession'])
            and source_identity(source['cover_url'])==(source['cik'],source['accession'])
            and all(re.fullmatch(r'[0-9a-f]{64}',source[k]) for k in ('statement_sha256','cover_sha256','directory_sha256'))
            and date.fromisoformat(proof['current_period'])<=date.fromisoformat(source['filed'])<=date.fromisoformat(proof['available_by']))
    except (KeyError,TypeError,ValueError):return False
