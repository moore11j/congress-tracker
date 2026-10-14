"""Original-bound SPCX annual history with an exact interim accounting bridge.

Registration statements are not generally accepted as periodic financials.
The reviewed 424B4 is used only after its comparative quarters match the
current registrant's SEC facts. Per-share/pro-forma columns are excluded.
"""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
import re
from zoneinfo import ZoneInfo

from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_spcx_registration_v1'
PROJECTION_SHA256 = '49fad0fb434bf18ac7e4ff5efb1cfba2ae66f05e2b1a294950ea0934c53ec38d'
SYMBOL = 'SPCX'
CIK = '0001181412'
ACCESSION = '0001628280-26-042639'
FILED = '2026-06-12'
URL = 'https://www.sec.gov/Archives/edgar/data/1181412/000162828026042639/spaceexplorationtechnologi.htm'
SHA256 = '782a77c750517fde51c274def5753456d63e4aba186248e7341bc6970c3dce01'
CURRENT_ACCESSION = '0001628280-26-052535'
CURRENT_FILED = '2026-08-04'
CURRENT_END = '2026-06-30'
CURRENT_URL = 'https://www.sec.gov/Archives/edgar/data/1181412/000162828026052535/spcx-20260630.htm'
CURRENT_SHA256 = 'b21adff93ed92113baf1c1d7a454aa9f1bf2448de5e2fe2db59c808711010280'
TAGS = {
 'revenue': 'RevenueFromContractWithCustomerExcludingAssessedTax',
 'operating_income': 'OperatingIncomeLoss', 'net_income': 'NetIncomeLoss',
 'common_income': 'NetIncomeLossAvailableToCommonStockholdersBasic',
 'operating_cash_flow': 'NetCashProvidedByUsedInOperatingActivities',
 'capex': 'PaymentsToAcquirePropertyPlantAndEquipment',
 'equity': 'StockholdersEquity', 'current_assets': 'AssetsCurrent',
 'current_liabilities': 'LiabilitiesCurrent',
}
PATH = '/html/body/document/type/sequence/filename/description/text/div/div'
TABLES = {'annual': PATH+'[314]/div[3]/div[5]/div/div/table',
 'cash': PATH+'[317]/div[3]/div[5]/div/div/table',
 'balance': PATH+'[313]/div[3]/div[5]/div/div/table',
 'interim': PATH+'[373]/div[3]/div[6]/div/div/table'}


class RegistrationHistoryError(SecFundamentalsCoverageError):
    pass


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol != SYMBOL or str(cik).zfill(10) != CIK
        or str(company.get('cik', '')).zfill(10) != CIK
        or company.get('tickers', []).count(SYMBOL) != 1
        or company.get('name', '').rstrip('.').casefold() != 'space exploration technologies corp'):
        raise RegistrationHistoryError('registration_company_identity')
    if observed_at.tzinfo is None:
        raise RegistrationHistoryError('registration_observation_timezone')
    available = observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    if not 0 <= (available-date.fromisoformat(CURRENT_END)).days <= 180:
        raise RegistrationHistoryError('registration_current_period_stale_or_future')
    recent = company.get('filings', {}).get('recent', {})
    arrays = [recent.get(k, []) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays}) != 1:
        raise RegistrationHistoryError('registration_filing_directory_shape')
    rows = list(zip(*arrays))
    for required in [(ACCESSION,'424B4',FILED,'','spaceexplorationtechnologi.htm'),
                     (CURRENT_ACCESSION,'10-Q',CURRENT_FILED,CURRENT_END,'spcx-20260630.htm')]:
        if rows.count(required) != 1 or required[2] > str(available):
            raise RegistrationHistoryError('registration_reviewed_filing_unavailable')
    for acc, form, filed, end, _ in rows:
        if form in {'10-K','10-Q','10-K/A','10-Q/A'} and filed <= str(available):
            if end > CURRENT_END or (end == CURRENT_END and acc != CURRENT_ACCESSION):
                raise RegistrationHistoryError('registration_newer_or_amended_report_requires_review')
    return available


def _rows(tree, name, count):
    found = tree.xpath(TABLES[name])
    if len(found) != 1:
        raise RegistrationHistoryError('registration_table_missing')
    result = {}
    for tr in found[0].xpath('./tr|./tbody/tr'):
        cells = [' '.join(''.join(c.itertext()).split()) for c in tr.xpath('./td|./th')]
        cells = [c for c in cells if c]
        if len(cells) < 2:
            continue
        label = re.sub(r'\s*\.{2,}.*$', '', cells[0]).strip()
        if label in result:
            # Per-share tables repeat Basic/Diluted; none is selected below.
            result[label] = None
        else:
            result[label] = cells[1:]
    def get(prefix):
        matches = [(k,v) for k,v in result.items() if k == prefix or k.startswith(prefix+' (related party')]
        if len(matches) != 1 or matches[0][1] is None:
            raise RegistrationHistoryError('registration_row_ambiguous:'+prefix)
        label, cells = matches[0]
        if len(cells) != count:
            raise RegistrationHistoryError('registration_column_shape:'+prefix)
        values = []
        for cell in cells:
            token = cell.replace('$','').strip()
            if not re.fullmatch(r'\(?[0-9]{1,3}(?:,[0-9]{3})*\)?|\(?[0-9]+\)?', token) or token.startswith('(') != token.endswith(')'):
                raise RegistrationHistoryError('registration_amount_missing_or_invalid:'+prefix)
            values.append(int(token.replace(',','').replace('(','-').replace(')',''))*1_000_000)
        return label, values
    return get


def parse(raw):
    if not raw or len(raw)>15_000_000 or hashlib.sha256(raw).hexdigest() != SHA256:
        raise RegistrationHistoryError('registration_original_changed')
    from lxml import html
    tree = html.fromstring(raw)
    annual, cash, balance, interim = (_rows(tree,n,c) for n,c in [('annual',3),('cash',3),('balance',2),('interim',2)])
    specs = {
      'revenue': (annual,'Revenue'), 'operating_income': (annual,'Income (loss) from operations'),
      'net_income': (annual,'Net income (loss)'),
      'common_income': (annual,'Net income (loss) attributable to shareholders - basic'),
      'operating_cash_flow': (cash,'Net cash provided by operating activities'),
      'capex': (cash,'Purchases of property, plant, and equipment'),
      'equity': (balance,'Total shareholdersÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ equity'),
      'current_assets': (balance,'Total current assets'), 'current_liabilities': (balance,'Total current liabilities'),
    }
    facts, provenance = {}, {}
    for metric,(getter,label) in specs.items():
        if metric == 'equity':
            label = 'Total shareholders'+chr(8217)+' equity'
        original, values = getter(label)
        records = []
        for year,value in zip((2025,2024,2023),values):
            instant = metric in {'equity','current_assets','current_liabilities'}
            start, end = (None if instant else str(year)+'-01-01'), str(year)+'-12-31'
            if metric == 'capex':
                if value >= 0: raise RegistrationHistoryError('registration_capex_sign')
                value = -value
            fact = dict(end=end,val=value,accn=ACCESSION,filed=FILED,form='424B4')
            if start: fact['start'] = start
            records.append(fact)
            provenance[(TAGS[metric],'USD',start,end)] = {
                'source_url':URL,'source_sha256':SHA256,'source_form':'424B4',
                'source_label':original,'source_cik':CIK,'registration_history':VERSION,
                'amount_basis':'reported USD millions; capex uses positive cash-purchase magnitude',
            }
        facts[TAGS[metric]] = {'units':{'USD':records}}
    # Operating and net-income reconciliation rejects selecting summary,
    # pro-forma or per-share columns even within the reviewed original.
    rev=annual('Revenue')[1]; costs=annual('Total costs and expenses')[1]
    op=annual('Income (loss) from operations')[1]; pretax=annual('Income (loss) before income taxes')[1]
    tax=annual('Provision for (benefit from) income taxes')[1]; net=annual('Net income (loss)')[1]
    if any(r-c!=o or p-t!=n for r,c,o,p,t,n in zip(rev,costs,op,pretax,tax,net)):
        raise RegistrationHistoryError('registration_income_reconciliation')
    bridge = {m:interim(label)[1] for m,label in {
        'revenue':'Revenue','operating_income':'Income (loss) from operations',
        'net_income':'Net loss','common_income':'Net loss attributable to shareholders - basic and diluted'}.items()}
    return {'facts':facts,'provenance':provenance,'registration_bridge':bridge,
        'evidence':{'cik':CIK,'accession':ACCESSION,'filed':FILED,'form':'424B4',
                    'url':URL,'sha256':SHA256,'methodology':VERSION,'namespace':'us-gaap',
                    'current_review_url':CURRENT_URL,'current_review_sha256':CURRENT_SHA256}}


def projection_digest(supplement):
    return hashlib.sha256(json.dumps({'facts':supplement['facts'],
        'bridge':supplement['registration_bridge'],'evidence':supplement['evidence'],
        'provenance':sorted((list(k),v) for k,v in supplement['provenance'].items())},
        sort_keys=True,separators=(',',':')).encode()).hexdigest()


def validate_bridge(payload, supplement, *, available_by):
    if projection_digest(supplement) != PROJECTION_SHA256:
        raise RegistrationHistoryError('registration_projection_changed')
    from app.services import sec_fundamentals as sec
    if (str(payload.get('cik','')).zfill(10) != CIK
        or supplement['evidence'].get('sha256') != SHA256
        or supplement['evidence'].get('methodology') != VERSION
        or available_by < date.fromisoformat(CURRENT_FILED)):
        raise RegistrationHistoryError('registration_bridge_identity_or_availability')
    if set(supplement.get('registration_bridge', {})) != {'revenue','operating_income','net_income','common_income'}:
        raise RegistrationHistoryError('registration_bridge_incomplete')
    # Any later periodic observation for a historical period remains decisive.
    # Conflicts require a new accounting review rather than mixing revisions.
    for metric,tag in TAGS.items():
        series = sec._records(payload,metric,available_by,concepts=(tag,),unit='USD')
        for row in supplement['facts'][tag]['units']['USD']:
            key = (date.fromisoformat(row['start']) if row.get('start') else None,date.fromisoformat(row['end']))
            if key in series and (series[key] is None or series[key]['value'] != row['val']):
                raise RegistrationHistoryError('registration_historical_period_conflict:'+metric)
    proof = []
    for metric, expected in supplement['registration_bridge'].items():
        series = sec._records(payload,metric,available_by,concepts=(TAGS[metric],),unit='USD')
        for year,value in zip((2026,2025),expected):
            first = series.get((date(year,1,1),date(year,6,30)))
            second = series.get((date(year,4,1),date(year,6,30)))
            if (not first or not second or first['value']-second['value']!=value
                or any(p['accession']!=CURRENT_ACCESSION or p['filed']!=CURRENT_FILED for p in (first,second))):
                raise RegistrationHistoryError('registration_interim_accounting_conflict:'+metric)
            proof.append({'metric':metric,'year':year,'march_value':value,'inputs':[deepcopy(first),deepcopy(second)]})
    for metric in ('equity','current_assets','current_liabilities'):
        series = sec._records(payload,metric,available_by,concepts=(TAGS[metric],),unit='USD')
        point = series.get((None,date(2025,12,31)))
        expected = supplement['facts'][TAGS[metric]]['units']['USD'][0]['val']
        if not point or point['value']!=expected or point['accession']!=CURRENT_ACCESSION:
            raise RegistrationHistoryError('registration_balance_accounting_conflict:'+metric)
        proof.append({'metric':metric,'year':2025,'value':expected,'inputs':[deepcopy(point)]})
    return {'methodology':VERSION,'original_url':URL,'original_sha256':SHA256,
            'accounting_basis':'reviewed common-control combined historical statements',
            'overlap_checks':proof,'limitations':['Prior June 2024 interim history is unavailable; no prior TTM growth inferred.',
              'Per-share, pro-forma, adjusted free-cash-flow and unreviewed newer periods are not substituted.']}


def acquire(facts_raw, company_raw, *, symbol, cik, observed_at, client):
    available = validate_company(json.loads(company_raw),symbol=symbol,cik=cik,observed_at=observed_at)
    result = parse(client.get(URL))
    validate_bridge(json.loads(facts_raw),result,available_by=available)
    return result
