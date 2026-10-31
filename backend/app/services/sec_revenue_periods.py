"""Reconcile contradictory duration tags from complete annual statements.

Normalized observations only: no source JSON mutation or calendar-year inference.
The filing must explicitly name the consolidated owner and fiscal-year ends.
"""
from datetime import date
import hashlib
import math
import re
from lxml import html
from app.services import sec_inline_financials as inline, sec_tabular_financials as tables
from app.services.sec_statement_currency import source_identity

VERSION = 'sec_reconciled_revenue_periods_v1'
TAG = 'RevenueFromContractWithCustomerExcludingAssessedTax'
MEMBERS = {'products': 'us-gaap:ProductMember', 'services': 'us-gaap:ServiceMember'}
TITLE = re.compile(r'(?:CONDENSED )?CONSOLIDATED STATEMENTS? OF (?:OPERATIONS|INCOME|EARNINGS)', re.I)


def _visible(node):
    return not any(isinstance(a.tag, str) and
        (a.tag in {'ix:hidden', 'ix:header', 'blockquote', 'script', 'style'} or
         a.get('hidden') is not None or a.get('aria-hidden') == 'true' or
         re.search(r'display\s*:\s*none|visibility\s*:\s*hidden', a.get('style', ''), re.I))
        for a in (node, *node.iterancestors()))


def _blocks(tree):
    return [tables.visible_text(n) for n in tree.xpath(
        '//*[self::p or self::div][not(.//table)][not(.//p)][not(.//div)]') if _visible(n)]


def _declarations(tree, issuer):
    owner = re.compile(r'Our Consolidated Financial Statements include the accounts of '
                       r'(.{1,140}?) and its consolidated subsidiaries\.', re.I)
    fiscal = re.compile(r'(' + tables.MONTH_PATTERN + r') (\d{1,2}), (20\d{2}) '
                        r'\([\u201c\u201d\u2018\u2019\"\']?fiscal (20\d{2})[\u201c\u201d\u2018\u2019\"\']?\)', re.I)
    declarations = []; ends = {}; owners = []
    for text in _blocks(tree):
        for match in owner.finditer(text):
            if re.search(r'\b(?:not|never|if|hypothetical)\b', text[max(0, match.start()-60):match.start()], re.I):
                raise ValueError('revenue_period_owner_qualified')
            owners.append(match)
        if not re.search(r'\bfiscal years? (?:ended|ends)\b', text, re.I):
            continue
        for match in fiscal.finditer(text):
            month, day, year, label = match.groups()
            end = str(date(int(year), tables.MONTHS[month.lower()], int(day)))
            if label in ends and ends[label] != end:
                raise ValueError('revenue_period_fiscal_declaration_conflict')
            ends[label] = end
            declarations.append(match.group(0))
    if not owners or any(tables.company_key(m[1]) != tables.company_key(issuer) for m in owners):
        raise ValueError('revenue_period_owner_unproved')
    if len(ends) < 2:
        raise ValueError('revenue_period_fiscal_declaration_unproved')
    return {'issuer': issuer, 'declaration': owners[0].group(0)}, ends, sorted(set(declarations))


def _caption(table):
    # Direct local siblings only. No document-wide narrative/title search.
    for n in table.itersiblings(preceding=True):
        if not isinstance(n.tag, str):
            continue
        if n.tag not in {'span', 'div', 'p', 'h1', 'h2', 'h3', 'br'} or n.xpath('.//table'):
            return None
        text = tables.visible_text(n)
        if text:
            return text if _visible(n) and TITLE.fullmatch(text) else None
    return None


def _columns(table):
    geometry = tables.grid(table)
    # Fiscal headers must precede every reported amount, not appear in a note.
    rows = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
    first = next((i for i, r in enumerate(rows) if inline.nodes(r, 'nonfraction')), len(rows))
    candidates = []
    for ri, row in enumerate(geometry[:first]):
        anchors = [c for c in row if re.fullmatch(r'20\d{2}', c['text'])]
        if 2 <= len(anchors) <= 4 and all(any(
            h['text'].lower() == 'fiscal year' and h['start'] <= c['start'] and c['end'] <= h['end']
            for line in geometry[:ri] for h in line) for c in anchors):
            candidates.append(anchors)
    if len(candidates) != 1:
        raise ValueError('revenue_period_column_header_unproved')
    columns = candidates[0]
    if len({c['text'] for c in columns}) != len(columns):
        raise ValueError('revenue_period_duplicate_year')
    scale_headers = [c['text'].lower() for row in geometry[:first] for c in row if c['start'] == 0 and re.search(r'\bin (?:millions|thousands)\b', c['text'], re.I)]
    if scale_headers != ['(in millions, except per share amounts)'] and scale_headers != ['(in millions)']:
        raise ValueError('revenue_period_scale_unproved')
    return rows, geometry, columns


def _point(tree, rows, geometry, ri, column, contexts, units):
    cells = rows[ri].xpath('./td|./th'); own = [c for c in geometry[ri] if c['row'] == ri]
    if len(cells) != len(own): raise ValueError('revenue_period_cell_geometry')
    nodes = []
    for cell, pos in zip(cells, own):
        if column['start'] <= pos['start'] < pos['end'] <= column['end']:
            nodes.extend(inline.nodes(cell,'nonfraction'))
    if len(nodes) != 1: raise ValueError('revenue_period_ambiguous_cell')
    n = nodes[0]; context = contexts.get(n.get('contextref'))
    if not context or n.get('name') != 'us-gaap:'+TAG or n.get('unitref') not in units or not _visible(n):
        raise ValueError('revenue_period_fact_identity')
    value = inline._number(n, tree)
    if not math.isfinite(value) or value < 0 or value != tables.amount(geometry[ri],column['start'],column['end'],1_000_000):
        raise ValueError('revenue_period_printed_value')
    return {**context,'context_id':n.get('contextref'),'value':value,'unit':'USD'}

def qualify(raw, filing, *, issuer):
    """Return annual observations and exact original contradictory contexts.

    This intentionally supports only a demonstrable products/services equation.
    Other revenue structures require their own explicit evidence, not aliases.
    """
    if filing['form'] not in {'10-K', '10-K/A'} or source_identity(filing['url']) != (filing['cik'], filing['accession']):
        raise ValueError('revenue_period_filing_identity')
    if not date.fromisoformat(filing['report_date']) <= date.fromisoformat(filing['filed']):
        raise ValueError('revenue_period_filing_date')
    parsed = inline.parse(raw, **filing)
    tree = html.fromstring(raw, parser=html.HTMLParser(encoding='utf-8'))
    namespaces = {'xbrli': 'http://www.xbrl.org/2003/instance', 'xbrldi': 'http://xbrl.org/2006/xbrldi',
                  'iso4217': 'http://www.xbrl.org/2003/iso4217'}
    for prefix in ('srt', 'us-gaap'):
        value = tree.get('xmlns:'+prefix, '')
        if not re.fullmatch(r'https?://fasb.org/'+prefix+r'/20\d{2}', value):
            raise ValueError('revenue_period_dimension_namespace')
        namespaces[prefix] = value
    for node in tree.iter():
        for prefix, value in namespaces.items():
            if node.get('xmlns:'+prefix, value) != value:
                raise ValueError('revenue_period_namespace_shadow')
    if any(tree.get('xmlns:'+k) != v for k,v in namespaces.items()):
        raise ValueError('revenue_period_namespace')
    owner, fiscal_ends, declarations = _declarations(tree, issuer)
    contexts = {}
    for n in inline.nodes(tree, 'context'):
        key = n.get('id')
        if not key or key in contexts:
            raise ValueError('revenue_period_duplicate_context')
        ids, starts, ends = (inline.nodes(n,k) for k in ('identifier','startdate','enddate'))
        members = [[m.get('dimension'), tables.visible_text(m)] for m in n.iter()
                   if m.tag == 'xbrldi:explicitmember']
        if (len(ids) != 1 or ids[0].get('scheme') != 'http://www.sec.gov/CIK'
                or tables.visible_text(ids[0]).zfill(10) != filing['cik']
                or len(starts) != 1 or len(ends) != 1 or inline.nodes(n,'scenario')
                or any(m.tag == 'xbrldi:typedmember' for m in n.iter())
                or any(child.tag != 'xbrldi:explicitmember' for seg in inline.nodes(n,'segment') for child in seg)):
            contexts[key] = None
            continue
        contexts[key] = {'start':tables.visible_text(starts[0]),'end':tables.visible_text(ends[0]),'members':members}
    units = {n.get('id') for n in inline.nodes(tree,'unit') if not inline.nodes(n,'divide')
             and [tables.visible_text(m) for m in inline.nodes(n,'measure')] == ['iso4217:USD']}
    candidates = [t for t in tree.xpath('//table') if not t.xpath('.//table') and _visible(t) and _caption(t)]
    if len(candidates) != 1:
        raise ValueError('revenue_period_primary_statement_ambiguous')
    table = candidates[0]; rows, geometry, columns = _columns(table)
    selected = {}
    for ri, line in enumerate(geometry):
        label = line[0]['text'].lower().rstrip(':') if line else ''
        if label not in {'revenue', 'products', 'services', 'total revenue'}:
            continue
        if label in selected:
            # Cost-of-revenue products/services are below the total; stop there.
            if 'total revenue' in selected: break
            raise ValueError('revenue_period_duplicate_component')
        selected[label] = ri
        if label == 'total revenue': break
    if list(selected) != ['revenue', 'products', 'services', 'total revenue']:
        raise ValueError('revenue_period_component_structure')
    for ri in range(selected['revenue']+1,selected['total revenue']):
        if ri not in selected.values() and any(c['text'] for c in geometry[ri]):
            raise ValueError('revenue_period_extra_component')
    annual = []
    for column in columns:
        components = {k:_point(tree,rows,geometry,selected[k],column,contexts,units) for k in ('products','services','total revenue')}
        products, services, total = components.values()
        if any(components[k]['members'] != [['srt:ProductOrServiceAxis',v]] for k,v in MEMBERS.items()) or total['members']:
            raise ValueError('revenue_period_component_dimension')
        period = (products['start'],products['end'])
        start, end = map(date.fromisoformat,period)
        if (period != (services['start'],services['end']) or not 350 <= (end-start).days+1 <= 380
                or total['end'] != str(end) or fiscal_ends.get(column['text']) != str(end)
                or str(end) > filing['report_date']
                or products['value']+services['value'] != total['value']):
            raise ValueError('revenue_period_equation_or_period')
        original_days = (end-date.fromisoformat(total['start'])).days+1
        if total['start'] != str(start) and not 75 <= original_days <= 105:
            raise ValueError('revenue_period_unexpected_total_duration')
        annual.append({'start':str(start),'end':str(end),'value':total['value'],'unit':'USD',
                       'fiscal_year':column['text'],'column':[column['start'],column['end']],
                       'components':components,'tag':TAG})
    ordered = sorted(annual,key=lambda p:p['end'])
    if max(p['end'] for p in annual) != filing['report_date'] or any(
        (date.fromisoformat(b['start'])-date.fromisoformat(a['end'])).days != 1
        for a,b in zip(ordered,ordered[1:])):
        raise ValueError('revenue_period_noncontiguous_fiscal_years')
    # These are evidence, not authorization to replace source data. The caller
    # must match exact accession, original period, concept, value and filed date.
    contradictions = {}; inspected = {}
    for node in inline.nodes(tree,'nonfraction'):
        context = contexts.get(node.get('contextref'))
        if (node.get('name') != 'us-gaap:'+TAG or node.get('unitref') not in units
                or not context or context['members'] or not _visible(node)):
            continue
        days = (date.fromisoformat(context['end'])-date.fromisoformat(context['start'])).days+1
        matches = [p for p in annual if p['end'] == context['end'] and p['value'] == inline._number(node,tree)]
        if not 75 <= days <= 105 or len(matches) != 1: continue
        parents = node.xpath('ancestor::table')
        if not parents or parents[-1].xpath('.//table'): continue
        candidate = parents[-1]
        if candidate not in inspected:
            try: inspected[candidate] = _columns(candidate)
            except ValueError: inspected[candidate] = None
        layout = inspected[candidate]
        if layout is None: continue
        candidate_rows,candidate_geometry,candidate_columns = layout
        annual_point = matches[0]
        for ri,row in enumerate(candidate_rows):
            if node not in inline.nodes(row,'nonfraction'): continue
            if not candidate_geometry[ri] or candidate_geometry[ri][0]['text'].lower().rstrip(':') != 'total revenue': continue
            for col in candidate_columns:
                if col['text'] != annual_point['fiscal_year']: continue
                try: observed = _point(tree,candidate_rows,candidate_geometry,ri,col,contexts,units)
                except ValueError: continue
                if observed['context_id'] != node.get('contextref') or observed['value'] != annual_point['value']: continue
                contradictions[observed['context_id']] = {
                    'start':observed['start'],'end':observed['end'],'context_id':observed['context_id'],
                    'value':observed['value'],'annual_start':annual_point['start'],'tag':TAG,'unit':'USD',
                    'fiscal_year':col['text'],'column':[col['start'],col['end']],'row_label':'total revenue'}
    if any(p['components']['total revenue']['start'] != p['start'] and
           p['components']['total revenue']['context_id'] not in contradictions for p in annual):
        raise ValueError('revenue_period_primary_conflict_unbound')
    return {'methodology':VERSION,'filing':dict(filing),'source_sha256':hashlib.sha256(raw).hexdigest(),
            'source_bytes':len(raw),'owner':owner,'fiscal_declarations':declarations,
            'heading':_caption(table),'annual':annual,'contradictions':list(contradictions.values())}



def plan(payload, company, *, cik, available_by):
    """One current annual original, only for a missing annual revenue period."""
    from app.services import sec_fundamentals as sec
    if any(str(x.get('cik','')).zfill(10) != cik for x in (payload,company)) or not company.get('name'):
        raise ValueError('revenue_period_company_identity')
    recent=company.get('filings',{}).get('recent',{})
    arrays=[recent.get(k,[]) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays}) != 1: raise ValueError('revenue_period_directory_shape')
    candidates=[]
    for accession,form,filed,end,name in zip(*arrays):
        if form not in {'10-K','10-K/A'} or not sec._day(filed) or not sec._day(end) or not end<=filed<=str(available_by): continue
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',accession) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',name):
            raise ValueError('revenue_period_directory_identity')
        candidates.append(dict(cik=cik,accession=accession,form=form,filed=filed,report_date=end,
            url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{name}'))
    if not candidates: return None
    latest=max((f['report_date'],f['filed']) for f in candidates)
    choices=[f for f in candidates if (f['report_date'],f['filed'])==latest]
    if len(choices)!=1: raise ValueError('revenue_period_ambiguous_latest')
    filing=choices[0]; annual_end=date.fromisoformat(filing['report_date'])
    if not 0<=(available_by-annual_end).days<=550: return None
    records=sec._records(payload,'revenue',available_by)
    if not records: return None
    end=max(e for _,e in records)
    if not 0<=(available_by-end).days<=180: return None
    if sec._ttm(records,annual_end) is not None: return None
    suspect=[p for (start,finish),p in records.items() if p and start and finish==annual_end
        and 75<=(finish-start).days+1<=105 and p['tag']==TAG
        and p['accession']==filing['accession'] and p['filed']==filing['filed'] and p['unit']=='USD']
    return filing if len(suspect)==1 else None


def _issuer(company):
    # SEC's directory sometimes appends its explicit incorporation code to the
    # legal name. Remove only the exact matching directory field, not an alias.
    name=company['name']
    state=company.get('stateOfIncorporation','')
    if re.fullmatch(r'[A-Z]{2}',state):
        name=re.sub(r'\s*/'+re.escape(state)+r'/\s*$', '', name)
    return name


def acquire(payload, company, *, cik, available_by, client):
    expected=plan(payload,company,cik=cik,available_by=available_by)
    if expected is None: return None
    raw=client.get(expected['url'])
    proof=qualify(raw,expected,issuer=_issuer(company))
    return {'raw':raw,'evidence':proof} if proof['contradictions'] else None


def apply(records, payload, company, *, cik, available_by, supplement):
    from copy import deepcopy
    item=(supplement or {}).get('revenue_periods')
    if not item: return records
    expected=plan(payload,company,cik=cik,available_by=available_by)
    if expected is None or not isinstance(item.get('raw'),bytes):
        raise ValueError('revenue_period_supplement_scope')
    proof=qualify(item['raw'],expected,issuer=_issuer(company))
    if proof!=item.get('evidence') or proof!=(supplement or {}).get('evidence',{}).get('reconciled_revenue_periods'):
        raise ValueError('revenue_period_supplement_tamper')
    if not records.get('revenue'): return records
    result=dict(records); series=deepcopy(records['revenue']); withdrawn=[]
    for conflict in proof['contradictions']:
        key=(date.fromisoformat(conflict['start']),date.fromisoformat(conflict['end']))
        original=series.get(key)
        if original is None: raise ValueError('revenue_period_ambiguous_original')
        if any(original.get(k)!=conflict[k] for k in ('start','end','tag','unit','value')) or any(
                original.get(k)!=expected[k] for k in ('accession','filed')):
            raise ValueError('revenue_period_original_changed')
        withdrawn.append(deepcopy(original));del series[key]
    for annual in proof['annual']:
        key=(date.fromisoformat(annual['start']),date.fromisoformat(annual['end']))
        original=series.get(key)
        if key in series and (not original or any(original.get(k)!=annual[k] for k in ('start','end','tag','unit','value'))):
            raise ValueError('revenue_period_annual_conflict')
        # Existing, later filed annual evidence stays authoritative and intact.
        if original and (original['accession']!=expected['accession'] or original['filed']!=expected['filed']):
            continue
        metadata={'methodology':VERSION,'source_sha256':proof['source_sha256'],'source_url':expected['url'],
                  'source_cik':cik,'heading':proof['heading'],'owner':proof['owner'],
                  'fiscal_declarations':proof['fiscal_declarations'],'annual':annual,
                  'withdrawn_period_observations':[p for p in withdrawn if p['end']==annual['end']]}
        if original is None:
            original={k:annual[k] for k in ('start','end','tag','unit','value')}
            original.update(filed=expected['filed'],accession=expected['accession'],priority=3,
                source_url=expected['url'],source_sha256=proof['source_sha256'],source_cik=cik,
                derived='original_statement_fiscal_period_reconciliation')
        series[key]={**original,'period_reconciliation':metadata}
    result['revenue']=series
    return result



def cached_valid(proof, *, cik):
    """Check persisted proof consistency; full originals are revalidated on refresh."""
    try:
        filing=proof['filing']
        if (proof['methodology']!=VERSION or filing['cik']!=cik or filing['form'] not in {'10-K','10-K/A'}
                or source_identity(filing['url'])!=(cik,filing['accession'])
                or not re.fullmatch(r'[0-9a-f]{64}',proof['source_sha256'])
                or type(proof['source_bytes']) is not int or not 0<proof['source_bytes']<=inline.MAX_INLINE_BYTES
                or not TITLE.fullmatch(proof['heading']) or not proof['owner']['issuer']): return False
        reported=date.fromisoformat(filing['report_date'])
        if reported>date.fromisoformat(filing['filed']): return False
        # Recheck the exact named declaration and fiscal labels without deriving
        # dates from calendar years. They are persisted independently of rows.
        wrapper=html.Element('div')
        owner=html.Element('p');owner.text=proof['owner']['declaration'];wrapper.append(owner)
        for declaration in proof['fiscal_declarations']:
            n=html.Element('p');n.text='The fiscal year ended '+declaration;wrapper.append(n)
        _,fiscal_ends,_=_declarations(wrapper,proof['owner']['issuer'])
        if not 2<=len(proof['annual'])<=4 or not 1<=len(proof['contradictions'])<=12: return False
        seen=set();annual={};positions=[]
        for p in proof['annual']:
            start,end=date.fromisoformat(p['start']),date.fromisoformat(p['end'])
            if (p['end'] in seen or not 350<=(end-start).days+1<=380 or end>reported or p['tag']!=TAG
                    or p['unit']!='USD' or fiscal_ends.get(p['fiscal_year'])!=p['end']
                    or type(p['value']) not in (int,float) or not math.isfinite(p['value']) or p['value']<0
                    or len(p['column'])!=2 or not 0<=p['column'][0]<p['column'][1]<=128): return False
            if any(max(p['column'][0],a)<min(p['column'][1],b) for a,b in positions): return False
            positions.append(p['column']);seen.add(p['end']);annual[p['end']]=p
            if set(p['components'])!={'products','services','total revenue'}: return False
            for label,c in p['components'].items():
                members=[['srt:ProductOrServiceAxis',MEMBERS[label]]] if label in MEMBERS else []
                if (json_members(c['members'])!=members or c['unit']!='USD' or c['end']!=p['end']
                        or not c['context_id'] or type(c['value']) not in (int,float)
                        or not math.isfinite(c['value']) or c['value']<0): return False
                if label in MEMBERS and c['start']!=p['start']: return False
                if label=='total revenue' and c['start']!=p['start'] and not 75<=(end-date.fromisoformat(c['start'])).days+1<=105: return False
            parts=p['components']
            if parts['products']['value']+parts['services']['value']!=p['value'] or parts['total revenue']['value']!=p['value']: return False
        ordered=sorted(proof['annual'],key=lambda p:p['end'])
        if ordered[-1]['end']!=filing['report_date'] or any((date.fromisoformat(b['start'])-date.fromisoformat(a['end'])).days!=1 for a,b in zip(ordered,ordered[1:])): return False
        contexts=set()
        for c in proof['contradictions']:
            p=annual.get(c['end']);days=(date.fromisoformat(c['end'])-date.fromisoformat(c['start'])).days+1
            if (not p or not 75<=days<=105 or c['context_id'] in contexts or not c['context_id']
                    or c['annual_start']!=p['start'] or c['value']!=p['value'] or c['tag']!=TAG
                    or c['unit']!='USD' or c['row_label']!='total revenue' or c['fiscal_year']!=p['fiscal_year']
                    or len(c['column'])!=2 or not 0<=c['column'][0]<c['column'][1]<=128): return False
            contexts.add(c['context_id'])
        return all(p['components']['total revenue']['start']==p['start'] or p['components']['total revenue']['context_id'] in contexts for p in proof['annual'])
    except (KeyError,ValueError,TypeError,AttributeError,IndexError):
        return False


def json_members(members):
    return [list(m) for m in members]
