from copy import deepcopy

import pytest

from app.utils.institution_names import institution_display_name, normalize_article_institution_names


@pytest.mark.parametrize("raw,expected", [
    ("GOLDMAN SACHS GROUP INC", "Goldman Sachs Group Inc"),
    ("JPMORGAN CHASE & CO", "JPMorgan Chase & Co"),
    ("BLACKROCK, INC.", "BlackRock, Inc."),
    ("FMR LLC", "FMR LLC"), ("UBS GROUP AG", "UBS Group AG"),
    ("BANK OF AMERICA CORP", "Bank of America Corp"),
    ("  STATE   STREET CORP ", "State Street Corp"),
    ("BNY MELLON", "BNY Mellon"), ("RBC CAPITAL MARKETS LLC", "RBC Capital Markets LLC"),
    ("O'SHAUGHNESSY ASSET MANAGEMENT", "O'Shaughnessy Asset Management"),
    ("Baillie Gifford & Co", "Baillie Gifford & Co"), ("", None), (None, None),
])
def test_display_names(raw, expected):
    assert institution_display_name(raw) == expected
    assert institution_display_name(expected) == expected


def test_article_normalization_is_entity_scoped_and_preserves_identity_urls_and_numbers():
    context = {"primary": {"ownership": {"holders": [{"holder_name": "BLACKROCK, INC."}]}}}
    article = {"title": "BLACKROCK, INC. added shares", "primary_ticker": "NVDA",
               "slug": "BLACKROCK, INC.", "sections": [{"heading": "BLACKROCK, INC.",
               "body_markdown": "BLACKROCK, INC. added 1,234 shares. EBITDA: $12.3B. [BLACKROCK, INC.](https://example.com/BLACKROCK,INC.)"}],
               "source_links": [{"url": "https://example.com/BLACKROCK,INC."}],
               "suggested_card": {"preview_body": "BLACKROCK, INC. bought shares."},
               "risks": ["BLACKROCK, INC. could reduce its stake."]}
    before = deepcopy(article)
    result = normalize_article_institution_names(article, context)
    assert article == before
    assert result["title"] == "BlackRock, Inc. added shares"
    assert result["slug"] == article["slug"]
    assert result["source_links"] == article["source_links"]
    assert "1,234 shares. EBITDA: $12.3B." in result["sections"][0]["body_markdown"]
    assert "[BlackRock, Inc.](https://example.com/BLACKROCK,INC.)" in result["sections"][0]["body_markdown"]
    assert result["suggested_card"]["preview_body"] == "BlackRock, Inc. bought shares."
    assert normalize_article_institution_names(result, context) == result
    assert normalize_article_institution_names(article, {}) == article


def test_approved_draft_read_keeps_schedule_and_approval():
    from app.services.research_briefs import _draft_with_comparison_tickers
    draft = {"id": "test", "status": "approved_scheduled", "scheduled_at": "2026-09-12T18:18:48+00:00",
             "approved_at": "2026-09-11T18:31:09+00:00",
             "research_context": {"holder_name": "STATE STREET CORP"},
             "article": {"title": "STATE STREET CORP increased its stake."}}
    before = deepcopy(draft)
    result = _draft_with_comparison_tickers(draft)
    assert result["article"]["title"] == "State Street Corp increased its stake."
    for key in ("id", "status", "scheduled_at", "approved_at"):
        assert result[key] == before[key]
