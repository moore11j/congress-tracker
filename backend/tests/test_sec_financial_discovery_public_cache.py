from datetime import datetime,timedelta
import pytest
from app.services import sec_financial_discovery_store as store,sec_financial_discovery_reader as reader
from app.services import sec_financial_statements as panels,sec_directory,data_enrichment_queue as queue
from app.services.sec_tabular_reporting import TabularReportingError
from test_sec_financial_discovery import source,COMPANY,FACTS,DAY,no_network
from test_sec_financial_discovery_progress import Client,NOW,progress
from test_sec_financial_discovery_store import database


def test_pending_discovery_is_cached_warming_and_public_expiry_only_queues_work(database,monkeypatch):
    from app import request_priority
    assert progress()['phase']=='covers'
    clock=[NOW];client=Client();enqueued=[]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock[0]
    monkeypatch.setattr(panels,'datetime',Clock)
    monkeypatch.setattr(panels,'SessionLocal',database)
    monkeypatch.setattr(panels,'DirectSourceClient',lambda:client)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'NBIS':{'cik':'0001513845'}})
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','1')
    monkeypatch.setattr(queue,'enqueue_data_enrichment_job',lambda **kwargs:enqueued.append(kwargs))
    result=panels.prepared('NBIS')
    assert result['status']=='warming' and result['reason']=='financial_discovery_pending'
    assert client.calls==[COMPANY,FACTS] and not enqueued
    assert result['summary']['currentRatio'] is None
    assert panels.prepared('NBIS')==result and client.calls==[COMPANY,FACTS]
    with database() as db:
        assert panels.cached_payload(db,'NBIS',now=NOW+timedelta(minutes=19))==result
        assert panels.cached_payload(db,'NBIS',now=NOW+timedelta(minutes=20)) is None
    clock[0]+=timedelta(minutes=21)
    monkeypatch.setattr(request_priority,'get_request_context',lambda:{'path':'/api/ticker/NBIS/financials'})
    public=panels.prepared('NBIS')
    assert public['status']=='warming' and len(enqueued)==1
    assert enqueued[0]['job_type']=='ticker_financials' and client.calls==[COMPANY,FACTS]


def test_pending_choice_uses_existing_consumer_error_contract(database,monkeypatch):
    progress();monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','1')
    client=Client()
    with pytest.raises(TabularReportingError,match='financial_discovery_pending'):
        reader.prepared_choice(source(COMPANY),source(FACTS),symbol='NBIS',cik='0001513845',
            available_by=DAY,observed_at=NOW,client=client)
    assert not client.calls
