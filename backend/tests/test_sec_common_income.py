from copy import deepcopy
from datetime import datetime
import json
import pytest
from app.services import sec_common_income as common,sec_fundamentals as sec
from test_sec_fundamentals import sources,project,NOW


def resolve(facts, company=None, **kw):
    company = company or sources()[1]
    evidence = json.loads(project(facts,company)['source_evidence_json'])
    return common.resolve(json.dumps(facts).encode(),json.dumps(company).encode(),evidence,
                          symbol='ABC',cik='0000000001',observed_at=NOW,**kw)


def reported_sources():
    facts,company = sources()
    rows=deepcopy(facts['facts']['us-gaap']['NetIncomeLoss']['units']['USD'])
    for row in rows:row['val']-=2
    facts['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']={'units':{'USD':rows}}
    return facts,company


def test_reported_common_earnings_are_distinct_from_parent_roe():
    facts,company=reported_sources();before=deepcopy(facts)
    point=resolve(facts,company)
    assert point['status']=='available' and point['point']['value']==11
    assert json.loads(project(facts)['source_evidence_json'])['current']['net_income']['value']==13
    assert facts==before and resolve(facts,company)==point


@pytest.mark.parametrize('defect',['absent','partial','conflict','currency','future','period'])
def test_no_parent_fallback_for_missing_ambiguous_or_misaligned_common_income(defect):
    facts,_=reported_sources();g=facts['facts']['us-gaap'];rows=g['NetIncomeLossAvailableToCommonStockholdersBasic']['units']['USD']
    if defect=='absent':del g['NetIncomeLossAvailableToCommonStockholdersBasic']
    elif defect=='partial':rows.pop()
    elif defect=='conflict':rows.append({**rows[-1],'val':999})
    elif defect=='currency':g['NetIncomeLossAvailableToCommonStockholdersBasic']['units']={'EUR':rows}
    elif defect=='future':rows[-1]['filed']='2026-10-09'
    elif defect=='period':rows[-1]['start']='2026-04-01'
    result=resolve(facts)
    assert result['status']=='unavailable' and result['point'] is None
    assert 'roe' in project(facts)


def test_identity_rejection_and_non_usd_hold():
    facts,company=reported_sources();evidence=json.loads(project(facts)['source_evidence_json'])
    for key,value in [('cik','2'),('tickers',['WRONG'])]:
        changed={**company,key:value}
        with pytest.raises(ValueError,match='mismatch'):
            common.resolve(json.dumps(facts).encode(),json.dumps(changed).encode(),evidence,symbol='ABC',cik='1',observed_at=NOW)
    result=common.resolve(b'',b'',{**evidence,'reporting_currency':'EUR'},symbol='ABC',cik='1',observed_at=NOW)
    assert result['status']=='unavailable' and result['reason']=='common_income_currency_not_usd'


@pytest.mark.parametrize('income',[11,0,-11,None])
def test_market_price_ratios_use_common_income_and_preserve_loss_yield(monkeypatch,income):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    from app.services import sec_inline_financials as inline
    facts,company=reported_sources()
    if income is None:del facts['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']
    else:
        for r in facts['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']['units']['USD']:
            r['val']*=income/11
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'ABC':{'cik':'0000000001'}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:json.dumps(facts if 'companyfacts' in url else company).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:None)
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':'1','market_cap':1100})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('ABC');e=json.loads(result['source_evidence_json'])
    assert result['roe']==pytest.approx(13/110*100)
    assert result['price_to_sales']==pytest.approx(1100/130)
    if income is None:
        assert 'earnings_yield' not in result and 'trailing_pe' not in result
        assert e['market_income']['status']=='unavailable'
    else:
        assert result['earnings_yield']==pytest.approx(income/1100*100)
        if income>0:assert result['trailing_pe']==pytest.approx(1100/income)
        else:assert 'trailing_pe' not in result
        assert 'common shareholders' in e['calculations']['earnings_yield']


def test_inline_common_observation_is_recovered_without_parent_substitution():
    from test_sec_inline_financials import document,fact,parse
    facts,_=reported_sources()
    rows=facts['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']['units']['USD'];rows.pop()
    supplement=parse(document(primary=fact('70',tag='RevenueFromContractWithCustomerExcludingAssessedTax'),extra=fact('5',tag='NetIncomeLossAvailableToCommonStockholdersBasic')))
    result=resolve(facts,filing_supplement=supplement)
    assert result['status']=='available' and result['point']['value']==11


def reviewed_jef_sources():
    import hashlib
    from datetime import date
    from test_sec_parent_statement_income import fixture
    from app.services import sec_parent_statement_income as reviewed
    records, profile = fixture('JEF')
    parent, _ = reviewed.apply(records, symbol='JEF', cik=profile['cik'])
    point = sec._ttm(parent['net_income'], date(2026,8,31))
    rows = []
    for proof in profile['periods']:
        expected = next(x for x in proof['statement_outputs'] if x['tag']=='PreferredStockDividendsIncomeStatementImpact')
        rows.append(dict(start=proof['start'],end=proof['end'],filed=proof['filed'],accn=proof['accession'],form='10-K' if proof['start']=='2024-12-01' and proof['end']=='2025-11-30' else '10-Q',val=expected['value']))
    facts = {'cik':96223,'facts':{'us-gaap':{'PreferredStockDividendsIncomeStatementImpact':{'units':{'USD':rows}}}}}
    company = {'cik':'96223','tickers':['JEF']}
    evidence = {'cik':profile['cik'],'period_start':point['start'],'period_end':point['end'],
        'available_by':'2026-10-13','current':{'net_income':point}}
    return facts,company,evidence


def resolve_jef(facts,company,evidence):
    import hashlib
    raw=json.dumps(facts).encode()
    evidence={**evidence,'facts':{'sha256':hashlib.sha256(raw).hexdigest()}}
    return common.resolve(raw,json.dumps(company).encode(),evidence,symbol='JEF',cik='96223',
        observed_at=datetime.fromisoformat('2026-10-14T21:31:00+00:00'))


def test_reviewed_jef_common_income_matches_original_statement():
    facts,company,evidence=reviewed_jef_sources()
    result=resolve_jef(facts,company,evidence)
    assert result['point']['value']==852597000
    assert evidence['current']['net_income']['value']==936650000
    assert result==resolve_jef(facts,company,evidence)


@pytest.mark.parametrize('defect',['missing','value','conflict','filed','accession','currency','parent_identity','proof','reported_conflict'])
def test_reviewed_common_income_does_not_override_source_changes_or_conflicts(defect):
    facts,company,evidence=reviewed_jef_sources()
    concept=facts['facts']['us-gaap']['PreferredStockDividendsIncomeStatementImpact']
    rows=concept['units']['USD'];row=next(r for r in rows if r['end']=='2026-08-31')
    if defect=='missing': rows.remove(row)
    elif defect=='value': row['val']+=1
    elif defect=='conflict': rows.append({**row,'val':row['val']+1})
    elif defect=='filed': row['filed']='2026-10-13'
    elif defect=='accession': row['accn']='unreviewed'
    elif defect=='currency': concept['units']={'EUR':rows}
    elif defect in {'parent_identity','proof'}:
        parent=next(r for r in evidence['current']['net_income']['inputs'] if r['end']=='2026-08-31')
        if defect=='parent_identity':parent['accession']='unreviewed'
        else:parent['parent_statement_review']['source_sha256']='changed'
    elif defect=='reported_conflict':
        facts['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']={'units':{'USD':[{**row,'val':1},{**row,'val':2}]}}
    assert resolve_jef(facts,company,evidence)['status']=='unavailable'


@pytest.mark.parametrize('key',['source_sha256','source_url','context_id','source_cik','derived'])
def test_reviewed_preferred_dividend_retains_original_identity(monkeypatch,key):
    from app.services import sec_financial_continuity as continuity
    facts,company,evidence=reviewed_jef_sources();original=continuity.record_sets
    def changed(*a,**kw):
        rows,proof=original(*a,**kw)
        for value in rows['common_preferred_dividends'].values():
            if value and value['end']=='2026-08-31':value[key]=True if key=='derived' else 'changed'
        return rows,proof
    monkeypatch.setattr(continuity,'record_sets',changed)
    assert resolve_jef(facts,company,evidence)['status']=='unavailable'


def test_common_income_uses_only_reviewed_matching_accounting_history():
    from test_sec_financial_continuity import originals,NOW as CONTINUITY_NOW
    current,old,company=originals()
    for payload in [current,old]:
        payload['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']=deepcopy(payload['facts']['us-gaap']['NetIncomeLoss'])
    raw=json.dumps(current).encode();company_raw=json.dumps(company).encode();history=json.dumps(old).encode()
    projected=sec.project_fundamentals(raw,company_raw,symbol='XOM',cik='2115436',observed_at=CONTINUITY_NOW,history_raw=history)
    evidence=json.loads(projected['source_evidence_json'])
    def run(prior):return common.resolve(raw,company_raw,evidence,symbol='XOM',cik='2115436',observed_at=CONTINUITY_NOW,history_raw=prior)
    assert run(None)['status']=='unavailable'
    result=run(history);assert result['point']['value']==13
    assert result['financial_continuity']['metrics']['common_income']['status']=='matched'
    assert {r['source_cik'] for r in result['point']['inputs']}=={'0002115436','0000034088'}
    old['facts']['us-gaap']['NetIncomeLossAvailableToCommonStockholdersBasic']['units']['USD'][-1]['val']+=1
    assert run(json.dumps(old).encode())['status']=='unavailable'


def test_common_evidence_cannot_claim_future_availability_or_different_facts():
    facts,company=reported_sources();e=json.loads(project(facts)['source_evidence_json'])
    args=dict(symbol='ABC',cik='1',observed_at=NOW)
    for changed,reason in [({**e,'available_by':'2026-10-30'},'availability'),({**e,'facts':{'sha256':'changed'}},'facts')]:
        with pytest.raises(ValueError,match=reason):common.resolve(json.dumps(facts).encode(),json.dumps(company).encode(),changed,**args)
