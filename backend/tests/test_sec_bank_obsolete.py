"""Abandoned bank totals cannot mask current, explicitly owned components."""
from copy import deepcopy
from datetime import date,datetime,timezone
from pathlib import Path
import gzip,hashlib,json
import pytest
from app.services import sec_bank_revenue as bank,sec_fundamentals as sec,sec_financial_statements as statements
from test_sec_bank_revenue import bank_sources,AVAILABLE

NOW=datetime(2026,10,18,6,10,tzinfo=timezone.utc)
ROOT=Path(__file__).parent/'fixtures/sec_bank_obsolete'
TOTALS={'ASB':1609335000.,'FMBM':58601000.,'HTH':1276403000.,'NEWT':294308000.,'PEBK':91918000.,'TCBI':1327317000.,'UBSI':1282108000.}
PRIORS={'ASB':1104940000.,'FMBM':48647000.,'HTH':1228325000.,'NEWT':274727000.,'TCBI':996954000.,'UBSI':1119730000.}


def inputs(symbol):
    manifest=json.loads((ROOT/'sources.json').read_text(encoding='utf-8'))[symbol]
    raw=[]
    for kind in ('facts','company'):
        item=manifest[kind];body=gzip.decompress((ROOT/item['file']).read_bytes())
        assert hashlib.sha256(body).hexdigest()==item['sha256'] and len(body)==item['bytes']
        raw.append(body)
    return *raw,str(json.loads(raw[1])['cik']).zfill(10)


def synthetic():
    data,company=bank_sources()
    data['facts']['us-gaap']['Revenues']={'units':{'USD':[dict(start='2020-01-01',end='2020-12-31',filed='2021-02-01',accn='0000000001-21-000001',form='10-K',val=999)]}}
    company['filings']={'recent':{'form':['10-Q'],'reportDate':['2026-06-30'],'filingDate':['2026-08-01'],'accessionNumber':['0000000001-26-000001']}}
    return data,company


def applied(data,company,available=AVAILABLE):
    records={'revenue':sec._records(data,'revenue',available)}
    return records,bank.apply(records,data,company,available_by=available)


def test_current_owned_bank_basis_wholly_replaces_abandoned_total():
    data,company=synthetic();original=deepcopy(data)
    old,result=applied(data,company)
    assert result is not old and data==original
    point=sec._ttm(result['revenue'],date(2026,6,30));assert point['value']==143
    assert sec._ttm(result['revenue'],date(2025,6,30))['value']==121
    for row in point['inputs']:
        proof=row['revenue_selection'];assert proof['method']=='complete_bank_basis_replaces_obsolete_reported_total_v1'
        assert not proof['basis_spliced'] and proof['superseded_last_period']=='2020-12-31'
        assert proof['current_report']['accession']=='0000000001-26-000001'
        assert sum(r['value'] for r in row['inputs'])==row['value']


@pytest.mark.parametrize('kind',['recent_total','same_period','newer_period','conflict','nonbank','wrong_currency','missing_component','unmatched_filing','stale_components','no_filings','newer_report','newer_amendment','ambiguous_report','malformed_report','wrong_cik','unsupported_tag','newer_transition','newer_foreign_report'])
def test_abandoned_total_recovery_cannot_bypass_current_or_ambiguous_evidence(kind):
    data,company=synthetic();gaap=data['facts']['us-gaap'];old=gaap['Revenues']['units']['USD'][0];recent=company['filings']['recent']
    if kind=='recent_total':old.update(start='2026-01-01',end='2026-03-31',filed='2026-05-01')
    elif kind=='same_period':old.update(start='2026-01-01',end='2026-06-30',filed='2026-08-01')
    elif kind=='newer_period':old.update(start='2026-01-01',end='2026-09-30',filed='2026-10-01')
    elif kind=='conflict':gaap['Revenues']['units']['USD'].append({**old,'val':1000})
    elif kind=='nonbank':company['sic']='7372'
    elif kind=='wrong_currency':gaap[bank.FEES]['units']['EUR']=gaap[bank.FEES]['units'].pop('USD')
    elif kind=='missing_component':gaap[bank.FEES]['units']['USD'].pop()
    elif kind=='unmatched_filing':gaap[bank.FEES]['units']['USD'][-1]['accn']='0000000001-26-000099'
    elif kind=='stale_components':
        for tag in (bank.INTEREST,bank.FEES):gaap[tag]['units']['USD']=[r for r in gaap[tag]['units']['USD'] if r['end']<'2026-01-01']
    elif kind=='no_filings':company.pop('filings')
    elif kind in ('newer_transition','newer_foreign_report'):
        recent['form'].insert(0,'10-KT' if kind=='newer_transition' else '20-F');recent['reportDate'].insert(0,'2026-09-30');recent['filingDate'].insert(0,'2026-10-01');recent['accessionNumber'].insert(0,'0000000001-26-000099')
    elif kind=='newer_report':recent['reportDate'][0]='2026-09-30';recent['filingDate'][0]='2026-10-01'
    elif kind=='newer_amendment':recent['form'][0]='10-Q/A';recent['filingDate'][0]='2026-10-01';recent['accessionNumber'][0]='0000000001-26-000099'
    elif kind=='ambiguous_report':
        for key in recent:recent[key].append(recent[key][0])
        recent['accessionNumber'][1]='0000000001-26-000099'
    elif kind=='malformed_report':recent['reportDate'][0]='2026-06-35'
    elif kind=='wrong_cik':company['cik']='2'
    elif kind=='unsupported_tag':gaap['RegulatedAndUnregulatedOperatingRevenue']=gaap.pop('Revenues')
    if kind=='wrong_cik':
        with pytest.raises(sec.SecFundamentalsError,match='identity'):applied(data,company)
    else:
        old,result=applied(data,company);assert result is old


@pytest.mark.parametrize('symbol',list(TOTALS))
def test_complete_original_bank_totals_and_provenance(symbol):
    facts,company,cik=inputs(symbol)
    value=sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW)
    evidence=json.loads(value['source_evidence_json'])
    assert evidence['current']['revenue']['value']==TOTALS[symbol]
    assert evidence['revenue_basis']==bank.BASIS
    if symbol in PRIORS:
        assert evidence['prior']['revenue']['value']==PRIORS[symbol]
        assert value['revenue_growth']==pytest.approx((TOTALS[symbol]/PRIORS[symbol]-1)*100)
    else:assert 'revenue_growth' not in value
    for row in evidence['current']['revenue']['inputs']:
        assert row['revenue_selection']['basis_spliced'] is False
        assert sum(r['value'] for r in row['inputs'])==row['value']
        assert len({(r['filed'],r['accession'],r['start'],r['end'],r['unit']) for r in row['inputs']})==1
    assert evidence['net_debt_policy']['reason']=='financial_industry_requires_separate_policy'
    assert 'net_debt_to_ebitda' not in value and 'operating_margin' not in value
    panel=statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW)
    assert panel['summary']['revenueTtm']==TOTALS[symbol] and panel['revenueBasis']==bank.BASIS
    assert sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW)==value


@pytest.mark.parametrize('symbol',['ASB','FMBM'])
def test_source_score_watchlist_recovery_crossing_repeat_rollback(symbol,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    def original(name):
        facts,company,cik=inputs(name)
        return facts,company,cik,{}
    expected={name:{'metrics':{'revenue_growth':TOTALS[name]/PRIORS[name]*100-100}} for name in ('ASB','FMBM')}
    monkeypatch.setattr(consumer,'originals',original);monkeypatch.setattr(consumer,'NOW',NOW);monkeypatch.setattr(consumer,'EXPECTED',expected)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,'revenue_growth',monkeypatch)
