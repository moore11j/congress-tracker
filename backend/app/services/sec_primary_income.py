"""Reported consolidated income verified against complete primary statements.

No implicit zero allocations, inferred total earnings, or parent-metric changes.
"""
from datetime import date,datetime,timezone
from copy import deepcopy
import math,re
from lxml import html
from app.services import sec_inline_financials as inline,sec_tabular_financials as tables

VERSION="sec_primary_consolidated_income_v1"
BASIS="Reported total earnings in the issuer-owned consolidated income statement, reconciled to its same-column pretax earnings and income tax."

PRETAX={'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'}
LABELS={'pretax':{'income before income taxes','loss before income taxes','income (loss) before income taxes','income before taxes','loss before taxes'},
        'tax':{'income tax provision','income tax expense','provision for income taxes','income tax benefit','income tax expense (benefit)','provision for (benefit from) income taxes','federal and state income taxes','tax expense'}}
TAGS={'pretax':PRETAX,'tax':{'us-gaap:IncomeTaxExpenseBenefit'},'income':{'us-gaap:NetIncomeLoss'}}

LABELS['pretax'].update({'income from continuing operations before income taxes','loss from continuing operations before income taxes'})
REVERSED_TAX_LABELS={'income tax benefit (expense)','income tax benefit (provision)'}
LABELS['tax'].update(REVERSED_TAX_LABELS)
DISCONTINUED_LABELS={'discontinued operations, net of income taxes','discontinued operations, net of tax'}
TAGS['discontinued']={'us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax',
                      'us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity'}


def _statement_issuer(tree,filing,issuer,jurisdiction):
    """Resolve only a metadata jurisdiction suffix corroborated by this cover."""
    match=re.fullmatch(r'(.+)/([A-Z]{2})',issuer)
    if not match or match[2]!=jurisdiction:return issuer,None
    entity_contexts=set()
    for context in inline.nodes(tree,'context'):
        ids=inline.nodes(context,'identifier')
        if (not inline.nodes(context,'segment') and not inline.nodes(context,'scenario')
                and len(ids)==1 and ids[0].get('scheme')=='http://www.sec.gov/CIK'
                and ids[0].text_content().strip().zfill(10)==filing['cik']):
            entity_contexts.add(context.get('id'))
    names=[n for n in tree.iter() if n.tag=='ix:nonnumeric' and n.get('name')=='dei:EntityRegistrantName']
    if (len(names)!=1 or names[0].get('contextref') not in entity_contexts
            or names[0].get('continuedat') or inline._nil_fact(names[0])
            or tables.clean(names[0].text_content())!=issuer):return issuer,None
    # DEI identity metadata may be in ix:hidden. Amounts below must still be
    # visible in the independently named consolidated statement itself.
    return match[1],dict(registry_name=issuer,statement_name=match[1],jurisdiction=jurisdiction,
                        context=names[0].get('contextref'),cik=filing['cik'],
                        cover_name_hidden=any(a.tag=='ix:hidden' for a in names[0].iterancestors()))


def statement_rows(raw,filing,issuer,*,jurisdiction=None):
    parsed=inline.parse(raw,**filing);tree=html.fromstring(raw);contexts={}
    issuer,issuer_proof=_statement_issuer(tree,filing,issuer,jurisdiction)
    for n in inline.nodes(tree,'context'):
        if inline.nodes(n,'segment') or inline.nodes(n,'scenario'):continue
        ids,starts,ends=(inline.nodes(n,k) for k in ('identifier','startdate','enddate'))
        if len(ids)==len(starts)==len(ends)==1 and ids[0].get('scheme')=='http://www.sec.gov/CIK' and ids[0].text_content().strip().zfill(10)==filing['cik']:
            contexts[n.get('id')]=(starts[0].text_content().strip(),ends[0].text_content().strip())
    units={n.get('id') for n in inline.nodes(tree,'unit') if [x.text_content().strip() for x in inline.nodes(n,'measure')]==['iso4217:USD'] and not inline.nodes(n,'divide')}
    tables_seen=set();out={}
    for fact in inline.nodes(tree,'nonfraction'):
        if fact.get('name')!='us-gaap:NetIncomeLoss' or not fact.xpath('ancestor::table'):continue
        table=fact.xpath('ancestor::table')[-1]
        if table in tables_seen or table.xpath('.//table|ancestor::blockquote'):continue
        tables_seen.add(table)
        try:panel=tables.parse_table(table,issuer=issuer,filed=date.fromisoformat(filing['filed']),currency_scope={'issuer':issuer,'currency':'USD','basis':'Each accepted component separately verified against entity-wide inline USD units.'},_fiscal_periods=tuple(contexts.values()))
        except ValueError:continue
        if not panel or panel['kind']!='income':continue
        geometry=tables.grid(table);rows=table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
        for point in panel['observations']:
            if point['metric']!='consolidated_income':continue
            period=(point['start'],point['end']);components={};positions={};ambiguous=False
            for ri,row in enumerate(rows[:point['row']+1]):
                cells=row.xpath('./td|./th');line=geometry[ri]
                if not cells or not line:continue
                label=tables.clean(line[0]['text']).lower().rstrip(':')
                label=re.sub(r'\s+\(\s*notes?\s+\d+(?:\s*(?:,|and|&|-)\s*\d+)*\s*\)$','',label)
                # Parent allocations cannot precede an admitted consolidated total.
                if any(word in label for word in ('attributable','noncontrolling','non-controlling','redeemable')):
                    ambiguous=True;break
                if 'discontinued' in label and label not in DISCONTINUED_LABELS:
                    ambiguous=True;break
                kind='income' if ri==point['row'] else 'discontinued' if label in DISCONTINUED_LABELS else next((k for k,v in LABELS.items() if label in v),None)
                if kind is None:continue
                own=[c for c in line if c['row']==ri]
                if len(own)!=len(cells):continue
                locations={id(c):p for c,p in zip(cells,own)};accepted=[]
                for n in inline.nodes(row,'nonfraction'):
                    if n.get('name') not in TAGS[kind] or n.get('unitref') not in units or contexts.get(n.get('contextref'))!=period or any(a.tag=='ix:hidden' for a in n.iterancestors()):continue
                    parent_cells=n.xpath('ancestor::td|ancestor::th');pos=locations.get(id(parent_cells[-1])) if parent_cells else None
                    if not pos or not point['column_start']<=pos['start']<pos['end']<=point['column_end']:continue
                    value=inline._number(n,tree)
                    displayed=tables.amount(line,point['column_start'],point['column_end'],point['scale'])
                    if displayed is None and kind=='discontinued' and value==0:
                        printed=''.join(c['text'] for c in line if point['column_start']<=c['start']<point['column_end']).strip()
                        # A visible dash is zero only when its same-cell inline
                        # fact explicitly decodes to zero under a valid transform.
                        if printed in {'-','\u2014','\u2013'} and tables.clean(n.text_content())==printed:displayed=0
                    sign=-1 if kind=='tax' and label in REVERSED_TAX_LABELS else 1
                    if displayed is None or value!=sign*displayed or (kind=='discontinued' and value!=0):continue
                    accepted.append((value,n.get('contextref'),n.get('name')))
                if len(set(accepted))!=1 or kind in components:ambiguous=True;break
                components[kind]=accepted[0];positions[kind]=ri
            if ambiguous or set(components)-{'discontinued'}!={'pretax','tax','income'} or not positions['pretax']<positions['tax']<positions['income']:continue
            if 'discontinued' in positions and not positions['tax']<positions['discontinued']<positions['income']:continue
            pretax,tax,income=(components[k][0] for k in ('pretax','tax','income'))
            if not math.isfinite(income) or pretax-tax!=income or income!=point['value']:continue
            proof={**{k:filing[k] for k in ('cik','accession','filed','url')},'start':period[0],'end':period[1],
                'value':income,'pretax':pretax,'tax':tax,'source_sha256':parsed['evidence']['sha256'],
                'heading':panel['heading'],'row_label':point['label'],'context':components['income'][1],
                'methodology':VERSION,'unit':'USD','basis':BASIS,'missing_allocations':'Not assumed zero; reported consolidated statement total is independently reconciled.'}
            if issuer_proof:proof['issuer_jurisdiction']=issuer_proof
            if 'discontinued' in components:
                proof['reported_zero_discontinued']=dict(value=0,context=components['discontinued'][1],tag=components['discontinued'][2])
            key=period+(filing['accession'],)
            if key in out and out[key]!=proof:raise ValueError('Competing primary statement totals')
            out[key]=proof
    return out


def plan(payload, company, *, symbol, cik, available_by):
    from app.services import sec_fundamentals as sec, sec_leverage, sec_parent_income, sec_statement_income
    if any(str(x.get('cik','')).zfill(10)!=cik for x in (payload,company)):
        raise ValueError('primary_income_identity')
    try: sic=int(company.get('sic',''))
    except (ValueError,TypeError): return None
    if not 100<=sic<=8999 or 6000<=sic<=6499 or 6700<=sic<=6799:
        return None
    recent=company.get('filings',{}).get('recent',{})
    arrays=[recent.get(k,[]) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays})!=1:raise ValueError('primary_income_directory')
    filings={}
    for accession,form,filed,report,name in zip(*arrays):
        if form not in {'10-K','10-Q','10-K/A','10-Q/A'} or not sec._day(report) or not sec._day(filed) or not report<=filed<=str(available_by):continue
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',accession) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',name):
            raise ValueError('primary_income_directory_identity')
        item=dict(cik=cik,accession=accession,form=form,filed=filed,report_date=report,
            url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-","")}/{name}')
        if accession in filings and filings[accession]!=item:raise ValueError('primary_income_directory_conflict')
        filings[accession]=item
    if not filings:return None
    latest=max((f['report_date'],f['filed']) for f in filings.values())
    if sum((f['report_date'],f['filed'])==latest for f in filings.values())!=1:return None
    if any(form in {'20-F','20-F/A','40-F','40-F/A','6-K','6-K/A'} and sec._day(report) and sec._day(filed)
           and latest[0]<=report<=filed<=str(available_by) for _,form,filed,report,_ in zip(*arrays)):return None
    end=date.fromisoformat(latest[0])
    if not 0<=(available_by-end).days<=180:return None
    tags={**sec_leverage.DURATION,**sec_parent_income.TAGS,'net_income':('NetIncomeLoss',)}
    records={name:sec._records(payload,name,available_by,concepts=concepts) for name,concepts in tags.items()}
    records,_=sec_parent_income.consolidated_for_leverage(records)
    records,_=sec_statement_income.apply(records,symbol=symbol,cik=cik)
    if sec._ttm(records['leverage_net_income'],end):return None
    parts={name:sec._ttm(records[name],end) for name in ('net_income','leverage_tax','leverage_interest','leverage_da')}
    if not all(parts.values()) or not sec_leverage._matching_legs(parts):return None
    # Negative EBITDA cannot produce a retained leverage ratio; avoid acquiring
    # a statement solely to construct an inapplicable candidate.
    if sum(v['value'] for v in parts.values())<=0:return None
    rows=[]
    for fact in parts['net_income']['inputs']:
        key=(date.fromisoformat(fact['start']),date.fromisoformat(fact['end']))
        if key in records['leverage_net_income']:continue
        if fact.get('derived') or fact['tag']!='NetIncomeLoss':continue
        if any(key in records.get(n,{}) and (not records[n][key] or records[n][key]['value']!=0)
               for n in ('parent_income_noncontrolling','parent_income_redeemable')):continue
        filing=filings.get(fact['accession'])
        if not filing or filing['filed']!=fact['filed'] or fact['end']>filing['report_date']:continue
        rows.append({'fact':fact,'filing':filing})
    if not rows or len({r['filing']['url'] for r in rows})>3:return None
    result={'cik':cik,'period_end':str(end),'rows':rows}
    jurisdiction=company.get('stateOfIncorporation')
    if isinstance(jurisdiction,str) and re.fullmatch(r'[A-Z]{2}',jurisdiction) and company.get('name','').endswith('/'+jurisdiction):
        result['issuer_jurisdiction']=jurisdiction
    return result


def qualify(expected, documents, *, issuer):
    verified={}
    for filing in {r['filing']['url']:r['filing'] for r in expected['rows']}.values():
        verified.update(statement_rows(documents[filing['url']],filing,issuer,jurisdiction=expected.get('issuer_jurisdiction')))
    rows=[]
    for item in expected['rows']:
        f=item['fact'];p=verified.get((f['start'],f['end'],f['accession']))
        if p and all(p[k]==f[k] for k in ('start','end','filed','accession','value','unit')):rows.append(p)
    return {'methodology':VERSION,'cik':expected['cik'],'period_end':expected['period_end'],'rows':rows}


def acquire(payload,company,*,symbol,cik,available_by,client):
    expected=plan(payload,company,symbol=symbol,cik=cik,available_by=available_by)
    if expected is None:return None
    documents={r['filing']['url']:None for r in expected['rows']}
    for url in documents:documents[url]=client.get(url)
    proof=qualify(expected,documents,issuer=company['name'])
    return {'documents':documents,'evidence':proof} if proof['rows'] else None


def apply(records,payload,company,*,symbol,cik,available_by,supplement):
    item=(supplement or {}).get('primary_income')
    if not item:return records
    expected=plan(payload,company,symbol=symbol,cik=cik,available_by=available_by)
    if (expected is None or qualify(expected,item['documents'],issuer=company['name'])!=item['evidence']
        or supplement.get('evidence',{}).get('primary_consolidated_income')!=item['evidence']):
        raise inline.InlineFactsError('primary_income_proof_mismatch')
    result=dict(records);consolidated=dict(records.get('leverage_net_income',{}))
    for proof in item['evidence']['rows']:
        key=(date.fromisoformat(proof['start']),date.fromisoformat(proof['end']))
        fact=records.get('net_income',{}).get(key)
        if key in consolidated or not fact or fact.get('derived') or fact.get('tag')!='NetIncomeLoss':continue
        if any(fact.get(k)!=proof[k] for k in ('start','end','filed','accession','value','unit')):continue
        if any(k in fact and fact[k]!=proof[p] for k,p in [('source_cik','cik'),('source_url','url'),('source_sha256','source_sha256'),('context_id','context')]):continue
        if any(key in records.get(n,{}) and (not records[n][key] or records[n][key]['value']!=0)
               for n in ('parent_income_noncontrolling','parent_income_redeemable')):continue
        consolidated[key]={**fact,'primary_consolidated_statement':deepcopy(proof)}
    result['leverage_net_income']=consolidated
    return result


def cached_valid(proof,*,cik):
    from app.services.sec_statement_currency import source_identity
    try:
        if proof['methodology']!=VERSION or proof['cik']!=cik or not 1<=len(proof['rows'])<=3:return False
        end=date.fromisoformat(proof['period_end']);seen=set();hashes={}
        for r in proof['rows']:
            key=(r['start'],r['end'])
            if key in seen or not date.fromisoformat(r['start'])<=date.fromisoformat(r['end'])<=end or date.fromisoformat(r['end'])>date.fromisoformat(r['filed']):return False
            seen.add(key)
            if source_identity(r['url'])!=(cik,r['accession']) or r['cik']!=cik or r['unit']!='USD' or r['methodology']!=VERSION or r['basis']!=BASIS:return False
            if not re.fullmatch(r'[0-9a-f]{64}',r['source_sha256']) or not isinstance(r['context'],str) or not r['context']:return False
            if not isinstance(r['heading'],list) or not r['heading'] or any(not isinstance(h,str) or not h.strip() for h in r['heading']):return False
            if 'issuer_jurisdiction' in r:
                owner=r['issuer_jurisdiction']
                if (not isinstance(owner,dict) or owner.get('cik')!=cik
                        or not isinstance(owner.get('jurisdiction'),str) or not re.fullmatch(r'[A-Z]{2}',owner['jurisdiction'])
                        or not isinstance(owner.get('statement_name'),str) or not owner['statement_name']
                        or owner.get('registry_name')!=owner['statement_name']+'/'+owner['jurisdiction']
                        or not isinstance(owner.get('context'),str) or not owner['context']
                        or type(owner.get('cover_name_hidden')) is not bool
                        or not any(tables.company_key(h)==tables.company_key(owner['statement_name']) for h in r['heading'])):return False
            if 'reported_zero_discontinued' in r:
                zero=r['reported_zero_discontinued']
                if (not isinstance(zero,dict) or type(zero.get('value')) not in (int,float) or zero['value']!=0
                        or zero.get('context')!=r['context'] or zero.get('tag') not in TAGS['discontinued']):return False
            if r['row_label'].lower() not in {'net income','net loss','net (loss) income','net income (loss)'}:return False
            if any(type(r[k]) not in (int,float) or not math.isfinite(r[k]) for k in ('value','pretax','tax')) or r['pretax']-r['tax']!=r['value']:return False
            if r['url'] in hashes and hashes[r['url']]!=r['source_sha256']:return False
            hashes[r['url']]=r['source_sha256']
        return len(hashes)<=3
    except (KeyError,ValueError,TypeError,AttributeError):return False
