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
