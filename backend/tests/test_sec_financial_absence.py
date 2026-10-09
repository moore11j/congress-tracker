from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
import pytest

from app.db import Base
from app.models import InsightsSnapshot
from app.clients.direct_sources import DirectSourceError
from app.services import sec_directory, sec_financial_statements as statements


@pytest.mark.parametrize('status', [404, 403, 429, 503])
def test_only_exact_facts_absence_is_cached(monkeypatch, status):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__])
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(statements, 'SessionLocal', factory)
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'SPY': {'cik': '0000884394'}})
    calls = []
    def fetch(self, url):
        calls.append(url)
        if 'companyfacts' in url:
            raise DirectSourceError(f'Source HTTP {status}: {url}')
        return b'{}'
    monkeypatch.setattr(statements.DirectSourceClient, 'get', fetch)
    if status == 404:
        result = statements.prepared('SPY')
        assert result['status'] == 'unavailable' and result['source'] == 'sec_edgar'
        assert result['sourceEvidence']['responseStatus'] == 404
        assert statements.prepared('SPY') == result
        assert len(calls) == 2
    else:
        with pytest.raises(DirectSourceError):
            statements.prepared('SPY')
        with factory() as db:
            assert db.scalar(select(InsightsSnapshot)) is None
    engine.dispose()
