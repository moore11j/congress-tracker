"""Validate a linked SEC inline document set without issuer-specific rules.

The cover supplies identity and shared contexts/units; financial numeric facts
must be in the linked exhibit. Both complete originals remain separately hashed.
"""
import hashlib
import re
from urllib.parse import urljoin,urldefrag
from lxml import html

VERSION='sec_inline_document_set_v1'


def combined_tree(raw,cover_raw,*,url,cover_url,cik,accession,form):
    from app.services.sec_inline_financials import InlineFactsError
    if form not in {'6-K','6-K/A'}:
        raise InlineFactsError('document_set_form')
    cik=str(cik).zfill(10)
    if not re.fullmatch(r'[0-9]{10}',cik) or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',accession):
        raise InlineFactsError('document_set_identity')
    prefix=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/'
    for address in (url,cover_url):
        if not isinstance(address,str) or not address.startswith(prefix) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',address[len(prefix):]):
            raise InlineFactsError('document_set_url')
    if url==cover_url or not isinstance(raw,bytes) or not isinstance(cover_raw,bytes) or not raw or not cover_raw or len(cover_raw)>2_000_000 or len(raw)+len(cover_raw)>20_000_000:
        raise InlineFactsError('document_set_size_or_duplicate')
    try:cover,exhibit=html.fromstring(cover_raw),html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise InlineFactsError('document_set_html') from exc
    if cover.tag!='html' or exhibit.tag!='html' or cover.xpath('//base') or exhibit.xpath('//base'):
        raise InlineFactsError('document_set_root_or_base')
    links={urljoin(cover_url,a.get('href')) for a in cover.xpath('//a[@href]')}
    if url not in links:
        raise InlineFactsError('document_set_link_required')
    namespaces={}
    for tree in (cover,exhibit):
        for node in tree.iter():
            for key,value in node.attrib.items():
                if not key.startswith('xmlns:'):continue
                if key in namespaces and namespaces[key]!=value:
                    raise InlineFactsError('document_set_namespace_conflict')
                namespaces[key]=value
    for tree in (cover,exhibit):
        if tree.get('xmlns:ix')!='http://www.xbrl.org/2013/inlineXBRL' or tree.get('xmlns:xbrli')!='http://www.xbrl.org/2003/instance':
            raise InlineFactsError('document_set_namespaces')
    # Preserve numeric-fact provenance: the parser's primary hash identifies
    # the exhibit. A cover containing accounting numeric facts needs a richer
    # source mapping and is explicitly held, never silently attributed here.
    if any(n.tag=='ix:nonfraction' and n.get('name','').split(':',1)[0] in {'us-gaap','ifrs-full'} for n in cover.iter()):
        raise InlineFactsError('document_set_cover_financial_facts')
    for key,value in namespaces.items():cover.set(key,value)
    cover.append(exhibit)
    return cover,{'methodology':VERSION,'source_url':cover_url,'source_sha256':hashlib.sha256(cover_raw).hexdigest(),
        'exhibit_url':url,'exhibit_sha256':hashlib.sha256(raw).hexdigest(),
        'basis':'Linked SEC cover supplies identity and shared contexts/units; numeric accounting facts are in the exhibit.'}



def candidates(company_raw,*,symbol,cik,available_by):
    """Return filing candidates, never infer a financial period from an event."""
    import json
    from datetime import date
    from app.utils.symbols import canonical_symbol
    from app.services.sec_inline_financials import InlineFactsError
    if not isinstance(company_raw,bytes) or not company_raw or len(company_raw)>5_000_000:
        raise InlineFactsError('document_set_directory_size')
    try:company=json.loads(company_raw)
    except (ValueError,TypeError) as exc:raise InlineFactsError('document_set_directory_json') from exc
    if not isinstance(company,dict) or not isinstance(company.get('tickers'),list) or not all(isinstance(v,str) for v in company['tickers']) or not isinstance(company.get('filings'),dict) or not isinstance(company['filings'].get('recent'),dict):
        raise InlineFactsError('document_set_directory_shape')
    cik=str(cik).zfill(10)
    if not re.fullmatch(r'[0-9]{10}',cik) or not company.get('name') or str(company.get('cik','')).zfill(10)!=cik or [canonical_symbol(s) for s in company.get('tickers',[])].count(canonical_symbol(symbol))!=1:
        raise InlineFactsError('document_set_directory_identity')
    recent=company.get('filings',{}).get('recent',{})
    arrays=[recent.get(k) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if any(not isinstance(a,list) for a in arrays) or len({len(a) for a in arrays})!=1 or len(arrays[0])>10000:
        raise InlineFactsError('document_set_directory_shape')
    found={};digest=hashlib.sha256(company_raw).hexdigest()
    for acc,form,filed,end,filename in zip(*arrays):
        if form not in {'6-K','6-K/A'}:continue
        try:period,filing=date.fromisoformat(end),date.fromisoformat(filed)
        except (ValueError,TypeError):continue
        if not period<=filing<=available_by or (available_by-period).days>180:continue
        if not isinstance(acc,str) or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',acc) or not isinstance(filename,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename):
            raise InlineFactsError('document_set_directory_filing')
        item={'symbol':canonical_symbol(symbol),'cik':cik,'accession':acc,'form':form,'filed':filed,'report_date':end,
            'cover_url':f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace("-", "")}/{filename}',
            'directory_sha256':digest,'directory_url':f'https://data.sec.gov/submissions/CIK{cik}.json'}
        if acc in found and found[acc]!=item:raise InlineFactsError('document_set_directory_duplicate')
        found[acc]=item
    return sorted(found.values(),key=lambda x:(x['report_date'],x['filed'],x['accession']),reverse=True)


def bounded_candidates(candidates, accessions=None):
    """Select a bounded internal scope without changing directory qualification."""
    from app.services.sec_inline_financials import InlineFactsError
    if accessions is None:return candidates[:3]
    if (not isinstance(accessions,tuple) or not 1<=len(accessions)<=3
            or any(not isinstance(a,str) for a in accessions)
            or len(set(accessions))!=len(accessions)):
        raise InlineFactsError('discovery_candidate_scope_invalid')
    indexed={c['accession']:c for c in candidates}
    if any(a not in indexed for a in accessions):
        raise InlineFactsError('discovery_candidate_not_in_directory')
    return [indexed[a] for a in accessions]


def statement_exhibits(raw,metadata):
    """Discover explicitly labelled statement links within this one filing."""
    from app.services.sec_inline_financials import InlineFactsError
    # A directory primary may itself contain full financial statements.
    # Respect the existing original-document limit rather than a cover-only cap.
    if not isinstance(raw,bytes) or not raw or len(raw)>20_000_000:
        raise InlineFactsError('document_set_cover_size')
    try:tree=html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise InlineFactsError('document_set_cover_html') from exc
    if tree.xpath('//base'):raise InlineFactsError('document_set_root_or_base')
    prefix=metadata['cover_url'].rsplit('/',1)[0]+'/'
    out={}
    for node in tree.xpath('//a[@href]'):
        label=' '.join(node.text_content().split())
        if not re.search(r'\bfinancial\s+(?:statements?|reports?)\b',label,re.I):continue
        url=urljoin(metadata['cover_url'],node.get('href'))
        # A table-of-contents anchor stays in the already retained primary.
        if urldefrag(url)[0]==metadata['cover_url']:continue
        if not url.startswith(prefix) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',url[len(prefix):]):
            raise InlineFactsError('document_set_statement_link_scope')
        out[url]={'url':url,'label':label,'cover_sha256':hashlib.sha256(raw).hexdigest()}
    return [out[url] for url in sorted(out)]


def acquire_candidate(metadata,company_raw,*,symbol,cik,available_by,client,basis=None):
    """Fetch one candidate and its uniquely linked statement, then prove identity.

    This is not a universe scanner or provider selector. Callers must retain
    candidate coverage and supply a verified accounting basis where required.
    """
    from app.services.sec_inline_financials import parse,InlineFactsError
    if metadata not in candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by):
        raise InlineFactsError('document_set_candidate_not_in_directory')
    cover=client.get(metadata['cover_url'])
    try:
        primary=parse(cover,cik=metadata['cik'],accession=metadata['accession'],filed=metadata['filed'],
            report_date=metadata['report_date'],form=metadata['form'],url=metadata['cover_url'],basis=basis)
    except InlineFactsError as exc:
        # Identity can be in the companion document. The final combined parse
        # still requires exact CIK, form and date, including conflicts.
        if str(exc) not in {'filing_namespaces','filing_identity','latest_reported_revenue_unavailable'}:
            raise
    else:
        primary['evidence']['unindexed_primary_discovery']=dict(metadata)
        return primary
    links=statement_exhibits(cover,metadata)
    if not links:return None
    if len(links)!=1:raise InlineFactsError('document_set_statement_ambiguous')
    url=links[0]['url'];raw=client.get(url)
    result=parse(raw,cik=metadata['cik'],accession=metadata['accession'],filed=metadata['filed'],
        report_date=metadata['report_date'],form=metadata['form'],url=url,basis=basis,
        cover_raw=cover,cover_url=metadata['cover_url'],document_set=True)
    result['evidence']['document_set_discovery']={**metadata,'statement_url':url,'statement_label':links[0]['label']}
    return result
