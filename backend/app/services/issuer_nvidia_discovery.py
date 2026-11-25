"""NVIDIA fiscal-year indexes and two independently checked transcript layouts."""
import calendar
from datetime import date,timedelta
import hashlib,json,re
from urllib.parse import urlencode
from app.clients.direct_sources import DirectSourceError,parse_issuer_material

KIND='nvidia_reports_v1'
BASE='https://investor.nvidia.com'
CDN='https://s201.q4cdn.com/141608511/files/'
QUARTERS=('First Quarter','Second Quarter','Third Quarter','Fourth Quarter')
MONTHS=('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
FULL_MONTHS=('January','February','March','April','May','June','July','August','September','October','November','December')
FACTSET_DATE=r'(\d{1,2})\s*-\s*('+'|'.join(MONTHS)+r')\s*-\s*(20\d{2})'

def check_company(company):
    if (company.get('symbol')!='NVDA' or company.get('investor_website')!=BASE+'/'
            or company.get('discovery',{}).get('kind')!=KIND):
        raise DirectSourceError('Unreviewed NVIDIA discovery policy')

def index_years(observed):
    # Fiscal Q4 of the named year is reported in February of that calendar year.
    # Keep both indexes around the calendar rollover; documents prove call dates.
    return (observed.year+1,observed.year)

def index_url(company,year):
    check_company(company)
    if type(year) is not int or not 2000<=year<=2099:raise DirectSourceError('Invalid NVIDIA fiscal year')
    labels=(*QUARTERS,'')
    params=[('LanguageId',1),('reportTypes','|'.join(labels)),
            *[(key,label) for key in ('reportSubType','reportSubTypeList') for label in labels],
            ('pageSize',10),('pageNumber',0),('tagList',''),('includeTags','true'),('year',year),('excludeSelection',1)]
    return BASE+'/feed/FinancialReport.svc/GetFinancialReportList?'+urlencode(params)

def source_format(url,year,quarter):
    prefix=re.escape(CDN)
    # Both current and historical paths are published by the same reviewed tenant.
    factset=(prefix+rf'(?:doc_financials/{year}/q{quarter}/NVDA|content_files/TRANSCRIPT_-NVIDIA-Corp-NVDA-US)'
             +rf'-Q{quarter}-{year}-Earnings-Call-\d{{1,2}}-[A-Za-z]+-20\d{{2}}-5_00-PM-ET\.pdf')
    bloomberg=prefix+rf'content_files/NVDA-{quarter}Q{str(year)[2:]}-Earnings-Transcript\.pdf'
    if re.fullmatch(factset,url):return 'factset_v1'
    if re.fullmatch(bloomberg,url):return 'bloomberg_v1'
    raise DirectSourceError('Unreviewed NVIDIA transcript path')

def publication_pattern(metadata,day):
    if metadata['transcript_format']=='factset_v1':
        return rf'\b{day.day}\s*-\s*{MONTHS[day.month-1]}\s*-\s*{day.year}\b'
    return rf'\bFINAL TRANSCRIPT\s+{day.isoformat()}\b'

def parse_index(raw,company,year,observed):
    url=index_url(company,year)
    if not raw or len(raw)>2_000_000:raise DirectSourceError('Invalid NVIDIA index size')
    try:reports=json.loads(raw)['GetFinancialReportListResult']
    except (ValueError,TypeError,KeyError) as exc:raise DirectSourceError('Invalid NVIDIA index') from exc
    if not isinstance(reports,list) or len(reports)>4:raise DirectSourceError('NVIDIA fiscal inventory exceeds bound')
    found=[];seen=set()
    for report in reports:
        if not isinstance(report,dict):raise DirectSourceError('Invalid NVIDIA report')
        label=report.get('ReportSubType')
        if (label not in QUARTERS or report.get('ReportYear')!=year or report.get('LanguageId')!=1
                or report.get('ReportTitle')!=f'{label} {year}'):
            raise DirectSourceError('NVIDIA fiscal label mismatch')
        quarter=QUARTERS.index(label)+1
        if quarter in seen:raise DirectSourceError('Duplicate NVIDIA fiscal period')
        seen.add(quarter)
        # Q4 stores calendar-shaped placeholders for FISCAL labels. These are
        # deliberately never a publication date, observation date or period end.
        placeholder=date(year,quarter*3,calendar.monthrange(year,quarter*3)[1]).strftime('%m/%d/%Y 00:00:00')
        if report.get('ReportDate')!=placeholder:raise DirectSourceError('NVIDIA fiscal index label changed')
        docs=report.get('Documents')
        if not isinstance(docs,list) or len(docs)>30 or any(not isinstance(d,dict) for d in docs):
            raise DirectSourceError('NVIDIA document inventory invalid')
        matches=[d for d in docs if d.get('DocumentTitle')=='Transcript']
        if len(matches)>1:raise DirectSourceError('Ambiguous NVIDIA transcript')
        for item in matches:
            if (item.get('DocumentCategory')!='transcript' or item.get('DocumentType') not in ('File','file')
                    or item.get('DocumentFileType')!='PDF' or item.get('LanguageId')!=1
                    or not isinstance(item.get('DocumentPath'),str)):
                raise DirectSourceError('NVIDIA transcript label or type mismatch')
            form=source_format(item['DocumentPath'],year,quarter)
            factset=form=='factset_v1'
            found.append(dict(key=f'NVDA:earnings_transcript:{year}:Q{quarter}',symbol='NVDA',
                document_type='earnings_transcript',fiscal_year=year,fiscal_quarter=quarter,url=item['DocumentPath'],
                source_format='pdf',company_website=company['company_website'],investor_website=company['investor_website'],
                company_pattern=r'\bNVIDIA Corp\.?\s*\(\s*NVDA(?: US Equity)?\s*\)',
                publisher_pattern=r'FactSet CallStreet, LLC' if factset else r'COPYRIGHT 20\d{2}, BLOOMBERG LP\.',
                transcript_publisher='FactSet CallStreet' if factset else 'Bloomberg',
                period_pattern=rf'\bQ{quarter}\s+{year}\s+Earnings Call\b',
                qa_pattern=r'\bQUESTION AND ANSWER SECTION\s+Operator:' if factset else r'\bQuestions And Answers\s+Operator\s+\(Question And Answer\)',
                transcript_pattern='Corrected Transcript' if factset else 'FINAL TRANSCRIPT',
                publication_pattern=FACTSET_DATE if factset else r'FINAL TRANSCRIPT\s+(20\d{2}-\d{2}-\d{2})',
                transcript_format=form,source_page_url=url,source_page_sha256=hashlib.sha256(raw).hexdigest(),
                discovery_kind=KIND,discovery_year=year,index_fiscal_label=placeholder))
    return sorted(found,key=lambda m:(m['fiscal_year'],m['fiscal_quarter']))

def parse_candidate(raw,metadata,observed):
    year,quarter=metadata.get('fiscal_year'),metadata.get('fiscal_quarter')
    if (metadata.get('symbol')!='NVDA' or metadata.get('discovery_kind')!=KIND
            or type(year) is not int or type(quarter) is not int or not 2000<=year<=2099 or not 1<=quarter<=4):
        raise DirectSourceError('Unreviewed NVIDIA fiscal metadata')
    form=source_format(metadata.get('url',''),year,quarter)
    if form!=metadata.get('transcript_format'):raise DirectSourceError('NVIDIA publisher layout changed')
    provisional={**metadata,'publication_pattern':FACTSET_DATE if form=='factset_v1' else r'FINAL TRANSCRIPT\s+(20\d{2}-\d{2}-\d{2})'}
    provisional.pop('published_at',None)
    text,_=parse_issuer_material(raw,provisional)
    if form=='factset_v1':
        heading=(r'^Corrected Transcript 1-877-FACTSET www\.callstreet\.com Total Pages: \d+ Copyright . '
                 r'2001-20\d{2} FactSet CallStreet, LLC '+FACTSET_DATE
                 +rf'\s+NVIDIA Corp\.\s*\(NVDA\s*\)\s+Q{quarter} {year} Earnings Call\b')
        match=re.match(heading,text)
        if not match:raise DirectSourceError('NVIDIA FactSet heading mismatch')
        day=date(int(match[3]),MONTHS.index(match[2])+1,int(match[1]))
        filename=re.search(r'Earnings-Call-(\d{1,2})-([A-Za-z]+)-(20\d{2})-',metadata['url'])
        if not filename or filename[2] not in FULL_MONTHS or day!=date(int(filename[3]),FULL_MONTHS.index(filename[2])+1,int(filename[1])):
            raise DirectSourceError('NVIDIA call date differs from reviewed source path')
    else:
        # The first page ends at its printed page marker. Reject a valid fiscal
        # label or company footer appearing only later in an unrelated PDF.
        first=text.split('Page 1 of ',1)[0]
        if len(first)>6000 or first==text:raise DirectSourceError('NVIDIA first-page boundary missing')
        titles=re.findall(r'\bQ([1-4]) (20\d{2}) Earnings Call\b',first)
        match=re.search(r'FINAL TRANSCRIPT (20\d{2}-\d{2}-\d{2}) NVIDIA Corp \(NVDA US Equity\) . COPYRIGHT 20\d{2}, BLOOMBERG LP\.',first)
        if titles!=[(str(quarter),str(year))] or not match:raise DirectSourceError('NVIDIA Bloomberg heading mismatch')
        day=date.fromisoformat(match[1])
    # This is a bounded issuer publication window, not an invented fiscal end.
    start=date(year if quarter==4 else year-1,(5,8,11,2)[quarter-1],1)
    if not start<=day<=min(observed.date(),start+timedelta(days=60)):
        raise DirectSourceError('NVIDIA call date outside reviewed fiscal window')
    resolved={**provisional,'published_at':day.isoformat(),'publication_pattern':publication_pattern(provisional,day)}
    text,parsed=parse_issuer_material(raw,resolved)
    return resolved,text,parsed
