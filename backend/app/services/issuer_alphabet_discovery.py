"""Alphabet quarterly report index and independently verified call pages."""
import calendar
from datetime import date,timedelta
import hashlib,json,re
from urllib.parse import urlencode
from lxml import html
from app.clients.direct_sources import DirectSourceError,parse_issuer_material

KIND='alphabet_quarterly_v1'
QUARTERS=('First Quarter','Second Quarter','Third Quarter','Fourth Quarter')
MONTHS=('January','February','March','April','May','June','July','August','September','October','November','December')
DATE_PATTERN=r'('+'|'.join(MONTHS)+r')\s+(\d{1,2}),\s*(20\d{2})'


def check_company(company):
    if (company.get('symbol')!='GOOGL' or company.get('discovery',{}).get('kind')!=KIND
            or company.get('company_website')!='https://abc.xyz/'
            or company.get('investor_website')!='https://abc.xyz/investor/'):
        raise DirectSourceError('Unreviewed Alphabet discovery identity')


def index_url(company,year):
    check_company(company)
    if type(year) is not int or not 2000<=year<=2100:
        raise DirectSourceError('Invalid Alphabet report year')
    params=[('LanguageId',1),('reportTypes','|'.join(QUARTERS)),
        *[(key,label) for key in ('reportSubType','reportSubTypeList') for label in QUARTERS],
        ('pageSize',10),('pageNumber',0),('tagList',''),('includeTags','true'),('year',year),('excludeSelection',1)]
    return 'https://abc.xyz/feed/FinancialReport.svc/GetFinancialReportList?'+urlencode(params)


def publication_pattern(day):
    return rf'{MONTHS[day.month-1]}\s+{day.day},\s*{day.year}'


def parse_index(raw,company,year,observed):
    source_url=index_url(company,year)
    if not raw or len(raw)>2_000_000:
        raise DirectSourceError('Alphabet index size invalid')
    try:reports=json.loads(raw)['GetFinancialReportListResult']
    except (ValueError,KeyError,TypeError) as exc:
        raise DirectSourceError('Invalid Alphabet quarterly index') from exc
    if not isinstance(reports,list) or len(reports)>=10:
        raise DirectSourceError('Alphabet index pagination bound')
    periods=set();found={}
    for report in reports:
        if not isinstance(report,dict):raise DirectSourceError('Invalid Alphabet report row')
        label=report.get('ReportSubType')
        if (label not in QUARTERS or type(report.get('ReportYear')) is not int
                or report['ReportYear']!=year or report.get('ReportTitle')!=f'{label} {year}'
                or report.get('LanguageId')!=1):
            raise DirectSourceError('Alphabet report fiscal identity mismatch')
        quarter=QUARTERS.index(label)+1
        end=date(year,quarter*3,calendar.monthrange(year,quarter*3)[1])
        if report.get('ReportDate')!=end.strftime('%m/%d/%Y 00:00:00') or end>observed.date():
            raise DirectSourceError('Alphabet report date mismatch')
        periods.add(quarter)
        docs=report.get('Documents')
        if not isinstance(docs,list) or len(docs)>30 or any(not isinstance(d,dict) for d in docs):
            raise DirectSourceError('Alphabet document bound or type invalid')
        # The issuer splits some quarters over separate release and filing rows.
        # Coalesce only the identical transcript link, never competing versions.
        matches=[d for d in docs if 'transcript' in str(d.get('DocumentTitle','')).lower()]
        for item in matches:
            url=item.get('DocumentPath')
            allowed=rf'https://abc\.xyz/investor/events/event-details/{year}/{year}-Q{quarter}-Earnings-Call-{year}-[A-Za-z0-9_-]+/default\.aspx'
            if (not isinstance(url,str) or not re.fullmatch(allowed,url)
                    or item.get('DocumentTitle')!='Webcast & Transcript'
                    or item.get('DocumentCategory')!='webcast' or item.get('DocumentType')!='Online'
                    or item.get('DocumentFileType') is not None or item.get('LanguageId')!=1):
                raise DirectSourceError('Unreviewed Alphabet transcript link or type')
            metadata=dict(key=f'GOOGL:earnings_transcript:{year}:Q{quarter}',symbol='GOOGL',
                document_type='earnings_transcript',fiscal_year=year,fiscal_quarter=quarter,
                url=url,source_format='html',company_website=company['company_website'],
                investor_website=company['investor_website'],company_pattern=r'\bAlphabet\b',
                transcript_publisher='Alphabet Inc.',publisher_pattern=r'\bAlphabet\b',
                period_pattern=rf"\bAlphabet(?:['’]s)?\s+{label}\s+{year}\s+Earnings(?:\s+Conference)?\s+Call\b",
                transcript_pattern=r'\btranscript\b',publication_pattern=DATE_PATTERN,
                qa_pattern=r'\bOperator:\s*[^:]{0,500}\b(?:Our|Your) first question comes from\b',
                source_page_url=source_url,source_page_sha256=hashlib.sha256(raw).hexdigest(),
                discovery_kind=KIND,discovery_year=year,period_end=end.isoformat())
            if quarter in found and found[quarter]!=metadata:
                raise DirectSourceError('Conflicting Alphabet transcript period')
            found[quarter]=metadata
    if periods and (not found or max(found)!=max(periods)):
        raise DirectSourceError('Latest Alphabet report transcript not listed')
    return [found[q] for q in sorted(found,reverse=True)]


def parse_candidate(raw,metadata,observed):
    if (metadata.get('symbol')!='GOOGL' or metadata.get('discovery_kind')!=KIND
            or not raw or len(raw)>2_000_000):
        raise DirectSourceError('Unreviewed Alphabet transcript page')
    try:tree=html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:
        raise DirectSourceError('Invalid Alphabet transcript HTML') from exc
    for node in tree.xpath('//script|//style|//template|//blockquote'):node.drop_tree()
    def nodes(token):
        return tree.xpath('//*[contains(concat(" ",normalize-space(@class)," "),$token)]',token=' '+token+' ')
    titles=nodes('evergreen-event-title');dates=nodes('evergreen-event-date-time-text');bodies=nodes('evergreen-event-body')
    year,quarter=metadata['fiscal_year'],metadata['fiscal_quarter']
    if len(titles)!=1 or len(dates)!=1 or len(bodies)!=1:
        raise DirectSourceError('Ambiguous Alphabet call page sections')
    if ' '.join(titles[0].text_content().split())!=f'{year} Q{quarter} Earnings Call':
        raise DirectSourceError('Alphabet call page fiscal title mismatch')
    stamp=' '.join(dates[0].text_content().split())
    match=re.fullmatch(DATE_PATTERN+r'\s+(?:[1-9]|1[0-2]):[0-5]\d\s+(?:AM|PM) PT',stamp)
    if not match:raise DirectSourceError('Alphabet call page date missing')
    try:publication=date(int(match[3]),MONTHS.index(match[1])+1,int(match[2]))
    except ValueError as exc:raise DirectSourceError('Invalid Alphabet call date') from exc
    end=date(year,quarter*3,calendar.monthrange(year,quarter*3)[1])
    if metadata['period_end']!=end.isoformat() or not end<=publication<=min(observed.date(),end+timedelta(days=100)):
        raise DirectSourceError('Alphabet call date outside fiscal window')
    body=' '.join(bodies[0].text_content().split())
    if not all(re.search(metadata[key],body,re.I) for key in ('period_pattern','transcript_pattern','qa_pattern')):
        raise DirectSourceError('Alphabet call body lacks independent period or Q&A')
    resolved={**metadata,'published_at':publication.isoformat(),'publication_pattern':publication_pattern(publication)}
    text,parsed=parse_issuer_material(raw,resolved)
    return resolved,text,parsed
