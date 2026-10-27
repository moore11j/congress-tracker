from copy import deepcopy
import hashlib,json
import pytest
from app.clients.direct_sources import DirectSourceError
from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
from test_sec_financial_discovery import source,complete,Client,COMPANY,FACTS,COVER,STATEMENT,NOW,DAY,no_network

ACCESSION='0001104659-26-094844'


def inspect(client=None,checkpoint=None,company=None,facts=None,accession=ACCESSION):
    return reader.inspect_candidate(company or source(COMPANY),facts or source(FACTS),symbol='NBIS',
        cik='0001513845',available_by=DAY,observed_at=NOW,checkpoint=checkpoint or complete()[0]['state'],
        accession=accession,client=client or Client())


def test_actual_hidden_statement_passes_existing_readers_and_consumers_with_two_originals():
    from app.services import sec_fundamentals as core,sec_financial_statements as panels
    client=Client();result=inspect(client);receipt=result['receipt']
    assert client.calls==[COVER,STATEMENT]
    assert receipt['status']=='qualified' and receipt['kind']=='html_table'
    assert receipt['current_period']=='2026-06-30' and not receipt['financial_publication_eligible']
    assert len(receipt['originals'])==2 and not receipt['holds']
    kw=dict(symbol='NBIS',cik='0001513845',observed_at=NOW,filing_supplement=result['supplement'])
    values=core.project_fundamentals(source(FACTS),source(COMPANY),**kw)
    panel=panels.project(source(FACTS),source(COMPANY),**kw)
    assert values['current_ratio']==pytest.approx(4.029839487029043)
    assert str(values['period_date'])=='2026-06-30' and 'roe' not in values
    repeated=inspect()
    assert repeated==result
    kw['filing_supplement']=repeated['supplement']
    assert core.project_fundamentals(source(FACTS),source(COMPANY),**kw)==values
    assert panels.project(source(FACTS),source(COMPANY),**kw)==panel
    assert len(json.dumps(receipt))<3000  # No original blobs or financial values in checkpoint.


def test_changed_original_is_rejected_before_exhibit_or_financial_parsing():
    client=Client({COVER:source(COVER)+b' '})
    with pytest.raises(discovery.DiscoveryError,match='cover_changed'):inspect(client)
    assert client.calls==[COVER]


def test_unobserved_or_changed_directory_candidate_cannot_use_old_hint():
    company=json.loads(source(COMPANY));recent=company['filings']['recent']
    i=recent['accessionNumber'].index(ACCESSION);recent['primaryDocument'][i]='changed.htm'
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='not_observed'):
        inspect(client,company=json.dumps(company).encode())
    assert not client.calls


def test_wrong_facts_cik_rejected_without_source_read():
    facts=json.loads(source(FACTS));facts['cik']=123
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='facts_identity'):
        inspect(client,facts=json.dumps(facts).encode())
    assert not client.calls


def test_exhibit_transport_failure_propagates_without_classifying_absence():
    client=Client({STATEMENT:DirectSourceError('Source HTTP 403: '+STATEMENT)})
    with pytest.raises(DirectSourceError,match='403'):inspect(client)
    assert client.calls==[COVER,STATEMENT]


def test_missing_accounting_basis_stays_held_not_a_completed_empty_scan():
    raw=source(STATEMENT)
    import re
    changed=re.sub(rb'US GAAP|U\.S\. GAAP|U\.S\. generally accepted accounting principles|United States generally accepted accounting principles|accounting principles generally accepted in the United States of America',
                   b'unreviewed accounting policy',raw,flags=re.I)
    assert changed!=raw
    result=inspect(Client({STATEMENT:changed}))
    assert result['receipt']['status']=='held' and result['supplement'] is None
    assert result['receipt']['holds'] and not result['receipt']['financial_publication_eligible']


@pytest.mark.parametrize('scope',[(),[],('missing',),('x','x'),('a','b','c','d')])
def test_invalid_candidate_scope_cannot_widen_existing_reader(scope):
    from app.services import sec_document_sets as sets,sec_inline_financials as inline
    candidates=sets.candidates(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)
    with pytest.raises(inline.InlineFactsError):sets.bounded_candidates(candidates,scope)


def test_explicit_scope_preserves_current_directory_metadata_and_default_window():
    from app.services import sec_document_sets as sets
    candidates=sets.candidates(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)
    assert sets.bounded_candidates(candidates)==candidates[:3]
    selected=sets.bounded_candidates(candidates,(ACCESSION,))
    assert len(selected)==1 and selected[0] in candidates
    assert selected[0]['directory_sha256']==hashlib.sha256(source(COMPANY)).hexdigest()
