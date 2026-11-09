"""Current IFRS statements with one entity-wide tagged USD currency.

This is an alternative original-bound proof, not a fabricated prose declaration.
It never infers dollars from a listing, a table's dollar sign or an older filing.
"""
from datetime import date
import hashlib
import json
import math
import re
from lxml import html

POLICY = 'current_ifrs_single_tagged_usd_v1'
ANCHORS = ('Assets', 'CurrentAssets', 'CurrentLiabilities', 'Revenue', 'ProfitLoss')
BASIS = ('Current IFRS statement with entity-wide tagged USD balance and duration '
         'facts; no foreign exchange, convenience-currency or per-share inference.')


def need(condition):
    from app.services.sec_statement_currency import StatementCurrencyError
    if not condition:
        raise StatementCurrencyError('tagged_ifrs_currency_unproved')


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def current_declarations(raw):
    from app.services.sec_statement_declarations import IFRS
    from app.services.sec_tabular_financials import visible_text
    tree=html.fromstring(raw)
    for node in tree.xpath('//script|//style|//template|//blockquote'):
        node.drop_tree()
    text=visible_text(tree)
    # A current interim statement assertion is required. An annual-report
    # cross reference or a future accounting-standard adoption is insufficient.
    standard=IFRS.replace('International Accounting Standard(?:', 'International Accounting Standards?(?:')
    subject=r'(?:interim (?:condensed )?(?:consolidated )?|condensed (?:consolidated )?interim )financial statements'
    pattern=subject+r'(?:(?![.;]).){0,100}?\b(?:have been |are |were )?(?:prepared(?: and presented)?|presented) in accordance with\s+(?:the\s+)?'+standard+r'\b'
    found=[]
    for match in re.finditer(pattern,text,re.I):
        prefix=text[max(0,match.start()-70):match.start()]
        if re.search(r'\b(?:not|never|unlike|hypothetical|will|would|could|should|expected|planned|if)\b',match.group(0),re.I):continue
        if re.search(r'\b(?:not|never|unlike|hypothetical|if)\s+(?:\w+\s+){0,6}$',prefix,re.I):continue
        found.append(match.group(0))
    return list(dict.fromkeys(found))


def anchors(parsed):
    evidence=parsed['evidence'];end=evidence['report_date'];out=[]
    for tag in ANCHORS:
        rows=parsed['facts'].get(tag,{}).get('units',{}).get('USD',[])
        selected=[p for p in rows if p['end']==end]
        need(selected)
        for point in selected:
            need(all(point.get(k)==evidence[e] for k,e in (('accn','accession'),('filed','filed'),('form','form'))))
            start=point.get('start')
            need((start is None) if tag in ANCHORS[:3] else
                 (isinstance(start,str) and 60<=(date.fromisoformat(end)-date.fromisoformat(start)).days+1<=366))
            value=point['val'];need(type(value) in (float,int) and math.isfinite(value))
            out.append(dict(tag=tag,start=start,end=end,value=value))
    return sorted(out,key=lambda p:(p['tag'],p['start'] or '',p['end']))


def bind(parsed,proof):
    """Bind the alternative proof to the parsed original or its cached evidence."""
    from app.services.sec_statement_currency import source_identity, StatementCurrencyError
    try:
        evidence=parsed['evidence'];cik,accession=source_identity(proof['source_url'])
        fields=('cik','accession','report_date','filed','namespace','currency')
        need(proof['methodology']==POLICY and proof['basis']==BASIS and proof['status']=='tagged')
        need(proof['namespace']=='ifrs-full' and proof['currency']=='USD' and evidence['form'] in {'6-K','6-K/A'})
        need(proof['cik']==cik and proof['accession']==accession and all(proof[k]==evidence[k] for k in fields))
        need(proof['source_url']==evidence['url'] and proof['source_sha256']==evidence['sha256']
             and re.fullmatch(r'[0-9a-f]{64}',proof['source_sha256']) is not None)
        need(evidence['currency_observation']==dict(methodology='entity_wide_statement_currencies_v1',current_statement_currencies=['USD']))
        need(proof['statement_declarations'] and all(isinstance(s,str) and current_declarations(('<html>'+s+'</html>').encode())==[s] for s in proof['statement_declarations']))
        need(proof['anchors'] and {p['tag'] for p in proof['anchors']}==set(ANCHORS))
        for point in proof['anchors']:
            need(point['end']==proof['report_date'] and type(point['value']) in (int,float) and math.isfinite(point['value']))
            if point['tag'] in ANCHORS[:3]:need(point['start'] is None)
            else:need(60<=(date.fromisoformat(point['end'])-date.fromisoformat(point['start'])).days+1<=366)
        need(date.fromisoformat(proof['report_date'])<=date.fromisoformat(proof['filed']))
        binding={k:v for k,v in proof.items() if k!='binding_sha256'}
        need(proof['binding_sha256']==digest(binding))
        if 'facts' in parsed:need(proof['anchors']==anchors(parsed))
        return dict(proof)
    except StatementCurrencyError:raise
    except (KeyError,TypeError,ValueError,AttributeError,OverflowError) as exc:
        raise StatementCurrencyError('tagged_ifrs_currency_unproved') from exc


def acquire_candidate(metadata,company_raw,*,symbol,cik,available_by,client):
    from app.services import sec_document_sets as sets,sec_statement_currency as native
    cover=client.get(metadata['cover_url'])
    need(native.declaration(cover,url=metadata['cover_url'])['status']=='unavailable')
    links=sets.statement_exhibits(cover,metadata)
    if links:
        need(len(links)==1);url=links[0]['url'];raw=client.get(url)
    else:
        url=metadata['cover_url'];raw=cover
    # Explicit currency evidence has precedence, including conflicts. Existing
    # declared-currency readers remain byte-for-byte unchanged.
    need(native.declaration(raw,url=url)['status']=='unavailable')
    tree=html.fromstring(raw)
    used={n.get('name','').split(':',1)[0] for n in tree.iter() if n.tag=='ix:nonfraction'}
    need('ifrs-full' in used and 'us-gaap' not in used)
    need(re.fullmatch(r'https?://xbrl\.ifrs\.org/taxonomy/[0-9]{4}-[0-9]{2}-[0-9]{2}/ifrs-full',tree.get('xmlns:ifrs-full','')) is not None)
    statements=current_declarations(raw);need(statements)
    from app.services import sec_tabular_reporting as reporting
    try:
        prose=reporting.declaration(raw,{'facts':[{'currency':'USD'}]})
    except reporting.TabularReportingError as exc:
        # The stricter interim declaration above supports additional IAS 34
        # wording, but never suppresses a conflicting declaration/translation.
        need(str(exc)=='table_accounting_basis_unproved')
    else:
        need(prose['accounting_basis']=='IFRS' and prose['currency']=='USD' and not prose['translations'])
    parsed=sets.acquire_candidate(metadata,company_raw,symbol=symbol,cik=cik,
        available_by=available_by,client=client,basis=dict(namespace='ifrs-full',currency='USD',tags=native.IFRS_TAGS))
    need(parsed is not None)
    evidence=parsed['evidence'];need(evidence['url']==url)
    proof=dict(methodology=POLICY,status='tagged',basis=BASIS,source_url=url,
        source_sha256=hashlib.sha256(raw).hexdigest(),statement_declarations=statements,
        anchors=anchors(parsed),**{k:evidence[k] for k in ('cik','accession','report_date','filed','namespace','currency')})
    proof['binding_sha256']=digest(proof)
    parsed['evidence']['statement_currency_declaration']=bind(parsed,proof)
    return parsed
