import json,os,subprocess,sys
from pathlib import Path
import pytest

@pytest.mark.parametrize('analyst,retired,suffix', [
    ('fmp','0',''),('finnhub','0','_analyst_display_only_v1'),
    ('fmp','1','_fmp_retired_v1'),('finnhub','1','_analyst_display_only_v1_fmp_retired_v1')])
def test_fresh_provider_workers_register_distinct_methodology_and_reject_old_bundle(analyst,retired,suffix):
    script = """
import json
from datetime import datetime,timezone,date
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db import Base
from app.models import ConfirmationMethodologyVersion,ConfirmationScoreSnapshot
from app.services import outcome_ledger as ledger
engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
with Session(engine) as db:
    old=ConfirmationMethodologyVersion(version='prior-provider-fixture',description='Original',configuration_json='{}',deployed_at=datetime(2026,1,1,tzinfo=timezone.utc),is_current=True)
    db.add(old);db.flush()
    snapshot=ConfirmationScoreSnapshot(security_id=1,ticker_at_time='ABC',calculated_at=datetime(2026,10,9,tzinfo=timezone.utc),market_date=date(2026,10,9),score=80,direction='bullish',strength='strong',active_source_count=3,input_hash='original',methodology_version_id=old.id,calculation_type='live')
    db.add(snapshot);db.commit()
    before={c.name:str(getattr(snapshot,c.name)) for c in snapshot.__table__.columns}
    current=ledger.current_confirmation_methodology(db)
    assert current.id != old.id
    assert json.loads(current.configuration_json)['scoring_version']==ledger.CONFIRMATION_SCORING_VERSION
    assert before=={c.name:str(getattr(snapshot,c.name)) for c in snapshot.__table__.columns}
    assert ledger.current_confirmation_methodology(db).id==current.id
    if ledger.replacement_analysts_selected() or ledger.fmp_provider_disabled():
        assert ledger.capture_live_confirmation_score_snapshot(object(),'ABC',{'score':80,'scoring_version':'prior'}) is None
    print(json.dumps({'version':current.version,'history_unchanged':True}))
"""
    result=subprocess.run([sys.executable,'-c',script],cwd=Path(__file__).resolve().parents[1],env={**os.environ,'DATABASE_URL':'sqlite:///:memory:','ANALYST_PROVIDER':analyst,'FMP_PROVIDER_DISABLED':retired},capture_output=True,text=True,timeout=45,check=True)
    payload=json.loads(result.stdout.splitlines()[-1])
    assert payload['version']=='confirmation-v8-agreement-coverage'+suffix
    assert payload['history_unchanged']
