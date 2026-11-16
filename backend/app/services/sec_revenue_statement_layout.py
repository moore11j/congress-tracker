"""Original-bound revenue totals with titles and subtotals inside the table.

This qualifies the existing single-concept history. It never creates a missing
period or splices a total and a component into a new revenue series.
"""
import math
import re
import hashlib
from lxml import html

POLICY = 'sec_primary_revenue_layout_v1'
CONTRACT = 'RevenueFromContractWithCustomerExcludingAssessedTax'
TOTAL = 'Revenues'
TITLE = re.compile(r'(?:condensed )?consolidated statements? of (?:operations|income|earnings)(?: and comprehensive (?:income|loss))?:?', re.I)
COMPONENTS = {'service revenues': 'us-gaap:ServiceMember',
              'royalty, product and other revenues': 'us-gaap:RoyaltyMember'}


def _visible(node):
    return not any(isinstance(n.tag,str) and
        (n.tag in {'ix:hidden','ix:header','script','style','blockquote'}
         or n.get('hidden') is not None or n.get('aria-hidden') == 'true'
         or re.search(r'display\s*:\s*none|visibility\s*:\s*hidden',n.get('style',''),re.I))
        for n in (node,*node.iterancestors()))


def heading_valid(value):
    return isinstance(value,list) and len(value)==1 and isinstance(value[0],str) and bool(TITLE.fullmatch(value[0]))


def valid(proof, *, point):
    """Validate persisted original evidence without fetching in a public reader."""
    if not isinstance(proof,dict) or not isinstance(point,dict):return False
    if (proof.get('layout_policy') != POLICY or not heading_valid(proof.get('statement_heading'))
            or proof.get('selected_concept') != CONTRACT
            or proof.get('statement_concept') not in {CONTRACT,TOTAL}):
        return False
    if proof.get('row_label') in {'total revenue','total revenues'}:
        return 'subtotal_equation' not in proof
    if proof.get('row_label') != '' or proof.get('statement_concept') != CONTRACT:
        return False
    equation=proof.get('subtotal_equation',{})
    if not isinstance(equation,dict):return False
    parts=equation.get('components',[])
    if not isinstance(parts,list) or any(not isinstance(p,dict) for p in parts):return False
    if (equation.get('formula') != 'service revenues + royalty, product and other revenues = reported unlabeled total'
            or equation.get('total') != point['value'] or len(parts)!=2
            or {p.get('label') for p in parts} != set(COMPONENTS)):
        return False
    for part in parts:
        if (part.get('member') != COMPONENTS.get(part.get('label'))
                or part.get('axis') != 'srt:ProductOrServiceAxis'
                or part.get('concept') != CONTRACT or part.get('unit') != 'USD'
                or any(part.get(k) != point[k] for k in ('start','end'))
                or not isinstance(part.get('context_id'),str) or not part['context_id']
                or type(part.get('value')) not in (int,float)
                or not math.isfinite(part['value']) or part['value']<0):
            return False
    return math.isclose(sum(p['value'] for p in parts),point['value'],rel_tol=1e-12,abs_tol=.01)


def statement_rows(raw, filing, parsed):
    from app.services import sec_inline_financials as inline
    from app.services.sec_tabular_financials import clean
    if (parsed['evidence'].get('sha256') != hashlib.sha256(raw).hexdigest()
            or any(parsed['evidence'].get(k) != filing[k] for k in ('cik','accession','filed','form','report_date'))):
        raise ValueError('revenue_layout_original_identity')
    tree=html.fromstring(raw,parser=html.HTMLParser(encoding='utf-8'))
    contexts={}
    for node in inline.nodes(tree,'context'):
        key=node.get('id')
        if not key or key in contexts:raise ValueError('revenue_layout_duplicate_context')
        contexts[key]=None
        ids,starts,ends=(inline.nodes(node,k) for k in ('identifier','startdate','enddate'))
        if (len(ids)!=1 or ids[0].get('scheme')!='http://www.sec.gov/CIK'
                or ids[0].text_content().strip().zfill(10)!=filing['cik']
                or len(starts)!=1 or len(ends)!=1 or inline.nodes(node,'scenario')
                or any(n.tag=='xbrldi:typedmember' for n in node.iter())):continue
        members=[(n.get('dimension'),n.text_content().strip()) for n in node.iter() if n.tag=='xbrldi:explicitmember']
        if any(n.tag!='xbrldi:explicitmember' for segment in inline.nodes(node,'segment') for n in segment):continue
        contexts[key]=(starts[0].text_content().strip(),ends[0].text_content().strip(),members)
    units={n.get('id') for n in inline.nodes(tree,'unit') if not inline.nodes(n,'divide')
           and [x.text_content().strip() for x in inline.nodes(n,'measure')]==['iso4217:USD']}
    selected={(p.get('start'),p['end']):p for p in parsed['facts'].get(CONTRACT,{}).get('units',{}).get('USD',[])}
    accepted={}
    for table in tree.xpath('//table'):
        if table.xpath('.//table') or not _visible(table):continue
        rows=table.xpath('./tr|./tbody/tr|./thead/tr')
        titles=[]
        for row in rows:
            if inline.nodes(row,'nonfraction'):break
            for cell in row.xpath('./td|./th'):
                text=clean(' '.join(cell.itertext()))
                if TITLE.fullmatch(text) and _visible(cell) and all(_visible(n) for n in cell.iter()):titles.append(text)
        if len(titles)!=1:continue
        section=[];started=False;ended=False
        for row in rows:
            cells=row.xpath('./td|./th')
            if not cells:continue
            label=clean(' '.join(cells[0].itertext())).lower().rstrip(':')
            if label=='revenues' and not inline.nodes(row,'nonfraction'):started=True;continue
            if started and label=='operating expenses':ended=True;break
            if started and inline.nodes(row,'nonfraction'):section.append((row,label))
        if not ended:continue
        for ri,(row,label) in enumerate(section):
            if label not in {'total revenue','total revenues',''}:continue
            if label=='' and (len(section)!=3 or ri!=2 or [r[1] for r in section[:2]]!=list(COMPONENTS)):continue
            for node in inline.nodes(row,'nonfraction'):
                concept=node.get('name','').removeprefix('us-gaap:')
                context=contexts.get(node.get('contextref'))
                if (node.get('name')!='us-gaap:'+concept or concept not in {TOTAL,CONTRACT}
                        or not context or context[2] or node.get('unitref') not in units or not _visible(node)):continue
                period=context[:2];point=selected.get(period)
                if not point or inline._number(node,tree)!=point['val']:continue
                proof=dict(start=period[0],end=period[1],value=point['val'],unit='USD',
                    filed=filing['filed'],accession=filing['accession'],context_id=node.get('contextref'),
                    row_label=label,statement_heading=titles,source_url=filing['url'],
                    source_sha256=parsed['evidence']['sha256'],source_cik=filing['cik'],
                    layout_policy=POLICY,statement_concept=concept,selected_concept=CONTRACT)
                if label=='':
                    parts=[]
                    for part_row,part_label in section[:2]:
                        for n in inline.nodes(part_row,'nonfraction'):
                            ctx=contexts.get(n.get('contextref'))
                            if (ctx and ctx[:2]==period and ctx[2]==[('srt:ProductOrServiceAxis',COMPONENTS[part_label])]
                                    and n.get('name')=='us-gaap:'+CONTRACT and n.get('unitref') in units and _visible(n)):
                                parts.append(dict(label=part_label,value=inline._number(n,tree),start=period[0],end=period[1],unit='USD',
                                    concept=CONTRACT,axis='srt:ProductOrServiceAxis',member=COMPONENTS[part_label],context_id=n.get('contextref')))
                    # The standard member names must have their real namespaces.
                    if any(not re.fullmatch(r'https?://fasb.org/'+prefix+r'/20\d{2}',tree.get('xmlns:'+prefix,'')) for prefix in ('srt','us-gaap')):continue
                    if any(n.get('xmlns:'+prefix,tree.get('xmlns:'+prefix))!=tree.get('xmlns:'+prefix)
                           for n in tree.iter() for prefix in ('srt','us-gaap')):continue
                    proof['subtotal_equation']=dict(formula='service revenues + royalty, product and other revenues = reported unlabeled total',components=parts,total=point['val'])
                if not valid(proof,point=proof):continue
                if period in accepted and accepted[period]!=proof:raise ValueError('revenue_layout_conflicting_totals')
                accepted[period]=proof
    return accepted
