"""Current retained IFRS original through discovery, public, and alert consumers."""
from copy import deepcopy
import json
import pytest
from test_sec_tagged_ifrs_currency import (source,Client,candidate,supplement,offline,
    COMPANY,FACTS,COVER,STATEMENT,CIK,ACCESSION,NOW,DAY,MANIFEST)


def test_exact_discovery_qualification_requires_complete_original_and_stays_unpublished():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    company=source(COMPANY)
    plan=discovery.inspect(company,symbol='GFS',cik=CIK,available_by=DAY,observed_at=NOW)
    c={k:candidate()[k] for k in discovery.IDENTITY_FIELDS}
    observed=discovery._observe(source(COVER),c,NOW)
    checkpoint=plan['state'];checkpoint['receipts'][discovery._fingerprint(c)]=observed
    client=Client()
    args=dict(symbol='GFS',cik=CIK,available_by=DAY,observed_at=NOW,checkpoint=checkpoint,accession=ACCESSION)
    result=reader.inspect_candidate(company,source(FACTS),client=client,**args)
    assert client.calls==[COVER,STATEMENT]
    receipt=result['receipt'];assert receipt['status']=='qualified' and receipt['kind']=='native_inline'
    assert receipt['current_period']=='2026-09-30' and not receipt['financial_publication_eligible']
    assert not receipt['holds'] and len(receipt['originals'])==2
    assert reader.inspect_candidate(company,source(FACTS),client=Client(),**args)==result
    # Qualifying one filing does not mark the unobserved whole inventory ready.
    checkpoint['receipts'][discovery._fingerprint(c)]['qualification']=receipt
    assert reader.selection_plan(company,symbol='GFS',cik=CIK,available_by=DAY,observed_at=NOW,checkpoint=checkpoint)['status']=='pending'
    old=deepcopy(receipt);old.update(status='held',kind=None,current_period=None,
        holds=[dict(accession=ACCESSION,filed=c['filed'],reason='table_reporting_currency_unproved')])
    # Supply all independent previous policy versions, isolating this retry.
    from app.services.sec_statement_sections import POLICY as section
    from app.services.sec_statement_layout import POLICY as layout,CAPTION_ROWS_POLICY
    from app.services.sec_statement_font_context import POLICY as font
    old.update(statement_section_policy=section,statement_layout_policy=layout,
        statement_caption_rows_policy=CAPTION_ROWS_POLICY,statement_font_policy=font)
    assert reader.validate_receipt(old,observed,now=NOW) is None
    from app.services.sec_tagged_ifrs_currency import POLICY
    old['tagged_currency_policy']=POLICY
    assert reader.validate_receipt(old,observed,now=NOW)==old


def originals(symbol):
    assert symbol=='GFS'
    return source(FACTS),source(COMPANY),CIK,{u:source(u) for u in MANIFEST['sources']}


@pytest.mark.parametrize('metric',['current_ratio','revenue_growth'])
def test_existing_score_watchlist_recovery_repeat_crossing_and_rollback(metric,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    monkeypatch.setattr(consumer,'originals',originals)
    monkeypatch.setattr(consumer,'EXPECTED',{'GFS':{'metrics':{'current_ratio':5758/2236,'revenue_growth':5.27168310999852}}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('GFS',metric,monkeypatch)


def test_existing_preparation_cache_public_reader_and_route_repeat(monkeypatch):
    import test_sec_native_financials as consumer
    monkeypatch.setattr(consumer,'originals',originals)
    monkeypatch.setattr(consumer,'NOW',NOW)
    monkeypatch.setattr(consumer,'EXPECTED',{'GFS':{'current_ratio':5758/2236,'free_cash_flow':430_000_000}})
    consumer.test_source_calculation_cache_preparation_and_public_repeat('GFS',True,monkeypatch)
    consumer.test_public_route_normalization_preserves_original_periods_and_currency('GFS')


@pytest.mark.parametrize('field',['anchors','source_sha256','statement_declarations'])
def test_changed_original_basis_never_reaches_core(field):
    from app.services import sec_fundamentals as core,sec_statement_currency as native
    parsed=supplement();proof=parsed['evidence']['statement_currency_declaration']
    proof[field]=[] if field!='source_sha256' else '0'*64
    with pytest.raises(native.StatementCurrencyError):
        core.project_fundamentals(source(FACTS),source(COMPANY),symbol='GFS',cik=CIK,observed_at=NOW,filing_supplement=parsed)
