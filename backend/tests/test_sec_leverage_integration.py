import json
from datetime import date
from sqlalchemy import select, func

from app.models import InsightsSnapshot, FundamentalsCache, Event
from app.services import sec_fundamentals_preparation as prep
from test_sec_fundamentals_preparation import fixture
from test_sec_leverage import prepared_sources
from test_sec_inline_financials import document, fact, parse


def test_inline_filings_retain_leverage_facts_with_original_provenance():
    extra = fact('7', tag='InterestExpenseNonoperating')
    extra += '<xbrli:context id="balance"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">1</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-06-30</xbrli:instant></xbrli:period></xbrli:context>'
    extra += fact('50', ctx='balance', tag='DebtLongtermAndShorttermCombinedAmount')
    result = parse(document(extra))
    assert result['facts']['InterestExpenseNonoperating']['units']['USD'][0]['val'] == 7
    point = result['facts']['DebtLongtermAndShorttermCombinedAmount']['units']['USD'][0]
    assert 'start' not in point and point['val'] == 50
    proof = result['provenance'][('DebtLongtermAndShorttermCombinedAmount','USD',None,'2026-06-30')]
    assert proof['context_id'] == 'balance' and proof['source_sha256'] == result['evidence']['sha256']


def test_versioned_preparation_keeps_leverage_out_of_canonical_fields_and_reuses_cache(fixture):
    factory, facts, company, calls = fixture
    facts.update(prepared_sources()[0])
    first = prep.prepare('ABC')
    assert first['version'] == prep.version_for_symbol('ABC')
    assert 'net_debt_to_ebitda' not in first['values']
    leverage = first['calculation_evidence']['leverage_preparation']
    assert leverage['status'] == 'preparation_only'
    assert leverage['ebitda']['status'] == 'components_complete'
    assert any('net_debt_to_ebitda' in row for row in leverage['candidates'])
    # The old format must not masquerade as a verified new evidence panel.
    with factory() as db:
        row = db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1')
        old = json.loads(row.payload_json)
        old['version'] = 'sec_ratio_preparation_v4'
        old['calculation_evidence'].pop('leverage_preparation')
        row.payload_json = json.dumps(old)
        db.commit()
    count = len(calls)
    second = prep.prepare('ABC')
    assert len(calls) > count
    assert second['values'] == first['values']
    assert second['calculation_evidence']['leverage_preparation'] == leverage
    count = len(calls)
    assert prep.prepare('ABC') == second and len(calls) == count
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(FundamentalsCache)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_auxiliary_rounded_fact_keeps_finer_value_in_either_order():
    import pytest
    from app.services.sec_inline_financials import InlineFactsError
    precise = fact('11819', tag='IncomeTaxExpenseBenefit', attrs='scale="6" decimals="-6"')
    rounded = fact('11.8', tag='IncomeTaxExpenseBenefit', attrs='scale="9" decimals="-8"')
    for extra in [precise+rounded, rounded+precise]:
        result = parse(document(extra))
        assert result['facts']['IncomeTaxExpenseBenefit']['units']['USD'][0]['val'] == 11819000000
        proof = result['evidence']['auxiliary_rounded_duplicates']
        assert len(proof) == 1 and proof[0]['selected_value'] == 11819000000
    for other in [fact('12.0', tag='IncomeTaxExpenseBenefit', attrs='scale="9" decimals="-8"'),
                  fact('11.8', tag='IncomeTaxExpenseBenefit', attrs='scale="9"'),
                  fact('11820', tag='IncomeTaxExpenseBenefit', attrs='scale="6" decimals="-6"')]:
        held = parse(document(precise+other))
        assert 'IncomeTaxExpenseBenefit' not in held['facts']
        assert held['evidence']['auxiliary_holds'][0]['tag'] == 'IncomeTaxExpenseBenefit'


def test_core_precision_accepts_containment_but_keeps_real_conflicts_held():
    import pytest
    from app.services.sec_inline_financials import InlineFactsError
    primary = fact('70.1', attrs='decimals="1"')
    result = parse(document(fact('70', attrs='decimals="0"'), primary=primary))
    assert result['facts']['RegulatedAndUnregulatedOperatingRevenue']['units']['USD'][0]['val'] == 70.1
    with pytest.raises(InlineFactsError, match='conflicting_reported_facts'):
        parse(document(fact('71', attrs='decimals="0"'), primary=primary))


import pytest

@pytest.mark.parametrize('extra', [
    fact('0', tag='IncomeTaxExpenseBenefit', attrs='xsi:nil="true"'),
    fact('unknown', tag='IncomeTaxExpenseBenefit'),
    fact('7', tag='IncomeTaxExpenseBenefit')+fact('99', tag='IncomeTaxExpenseBenefit'),
])
def test_bad_auxiliary_fact_preserves_core_and_blocks_older_component_fallback(extra):
    from datetime import datetime, timezone
    from app.services.sec_financial_continuity import record_sets
    from app.services.sec_leverage import TAGS
    from test_sec_inline_financials import TAG, ACC
    p = parse(document(extra))
    assert p['facts'][TAG]['units']['USD'][0]['val'] == 70
    assert p['evidence']['auxiliary_holds']
    facts, company = prepared_sources()
    records, _ = record_sets(facts,symbol='ABC',cik='1',available_by=date(2026,10,7),
        observed_at=datetime(2026,10,8,tzinfo=timezone.utc), company=company,
        metrics={'leverage_tax':(TAGS['leverage_tax'],'USD')},filing_supplement=p)
    assert records['leverage_tax'][(date(2026,1,1),date(2026,6,30))] is None
    # Another reporting period is neither discarded nor replaced.
    assert records['leverage_tax'][(date(2025,1,1),date(2025,12,31))]['value'] == pytest.approx(2.4)
