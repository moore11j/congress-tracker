from app.models import ResearchSourceDocument, ResearchSourceCoverage, Security
from app.services import operational_intelligence as service
from test_operational_intelligence import make_db


def test_headline_only_feed_never_becomes_fmp_research_evidence(monkeypatch):
    db, engine = make_db()
    monkeypatch.setenv('NEWS_PROVIDER', 'finnhub')
    monkeypatch.setenv('RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED', 'true')
    def forbidden(*args, **kwargs):
        raise AssertionError('Headline-only research must not fetch or extract')
    monkeypatch.setattr(service, 'get_stock_news', forbidden)
    monkeypatch.setattr(service, 'extract_document_events', forbidden)
    try:
        security = Security(symbol='AAPL', name='Apple', asset_class='stock')
        db.add(security); db.commit()
        result = service.refresh_operational_intelligence(db, security_id=security.id, source_types={'news_article'})
        assert result['skipped'] == 1 and result['documents'] == result['events'] == 0
        coverage = db.get(ResearchSourceCoverage, (security.id, 'news_article'))
        assert coverage.status == 'unavailable' and coverage.failure_reason == 'headline_only_feed'
        assert service._ingest_article(db, security=security, document_type='news_article',
            item={'source':'finnhub', 'title':'A sufficiently long issuer headline'})['skipped'] == 1
        assert db.query(ResearchSourceDocument).count() == 0
        panel = service.ticker_operational_intelligence(db, security=security)
        item = next(c for c in panel['coverage'] if c['source_type'] == 'news_article')
        assert item['provider'] == 'finnhub' and item['status'] == 'unavailable'
        assert item['reason'] == 'headline_only_feed' and 'Full article text' in item['message']
    finally:
        db.close(); engine.dispose()
