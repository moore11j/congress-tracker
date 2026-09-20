from app.services.research_links import imprecise_release_sources, invalid_ticker_links, repair_research_links


def test_repairs_inline_and_source_links_without_mutating_original():
    article = {"sections": [{"body_markdown": "See [earnings](/ticker/NVDA/earnings) and [financials](https://walnutmarkets.com/ticker/nvda/financials)."}],
               "source_links": [{"url": "https://app.walnutmarkets.com/ticker/NVDA/financials/"}]}
    repaired = repair_research_links(article)
    assert repaired["sections"][0]["body_markdown"].count("https://app.walnutmarkets.com/ticker/NVDA#financials") == 2
    assert repaired["source_links"][0]["url"] == "https://app.walnutmarkets.com/ticker/NVDA#financials"
    assert "/earnings)" in article["sections"][0]["body_markdown"]
    assert invalid_ticker_links(repaired) == []


def test_foreign_links_and_valid_tabs_are_untouched():
    body = "[Source](https://example.com/ticker/NVDA/earnings) [Research](https://app.walnutmarkets.com/ticker/NVDA#research)"
    assert repair_research_links(body) == body
    assert invalid_ticker_links({"sections": [{"body_markdown": body}]}) == []
    assert invalid_ticker_links({"sections": None, "source_links": None}) == []


def test_unknown_ticker_subpages_are_detected_for_publication_gate():
    article = {"sections": [{"body_markdown": "[Made up](/ticker/NVDA/forecast)"}], "source_links": [{"url": "https://app.walnutmarkets.com/ticker/NVDA/estimates"}]}
    assert len(invalid_ticker_links(article)) == 2
    assert invalid_ticker_links(repair_research_links(article)) == invalid_ticker_links(article)


def test_release_label_needs_a_specific_document_not_generic_ir_homepage():
    article = {"source_links": [
        {"label": "NVIDIA Q1 FY2027 financial results release", "url": "https://investor.nvidia.com/"},
        {"label": "NVIDIA Investor Relations", "url": "https://investor.nvidia.com/"},
        {"label": "NVIDIA Q1 FY2027 financial results", "url": "https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-first-quarter-fiscal-2027"},
    ]}
    assert imprecise_release_sources(article) == ["NVIDIA Q1 FY2027 financial results release"]
