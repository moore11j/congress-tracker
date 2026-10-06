import pytest
from app.services.research_ownership import compare_holder, period_table, matched_shares
from app.services import research_briefs as briefs


def row(shares, cusip="67066G104", **kw):
    return dict(cusip=cusip, shares=shares, shareType="SH", putCall=None,
                sourceUrl="https://www.sec.gov/Archives/test/table.xml", **kw)


class Client:
    def __init__(self, kind="NEW HOLDINGS"):
        self.kind = kind
    def fetch_13f_filing_metadata(self, *, report_quarter, **kw):
        rows = [{"accessionNumber": str(report_quarter), "formType": "13F-HR", "filingDate": "2026-08-01"}]
        return rows + ([{"accessionNumber": "supplement", "formType": "13F-HR/A", "filingDate": "2026-08-26"}] if report_quarter == 2 else [])
    def fetch_13f_amendment_type(self, **kw):
        return self.kind
    def fetch_13f_information_table(self, *, accession_number, **kw):
        return {"1": [row(100)], "2": [row(97)], "supplement": [row(42, "OTHER")] }[accession_number]


def test_new_holdings_does_not_erase_original_or_create_exit():
    result = compare_holder("1330387", "Amundi", "67066G104", 2026, 2, Client())
    assert result["curr_shares"] == 97
    assert result["shares_delta"] == -3
    assert result["change_type"] == "decrease"
    assert result["prior_source_urls"] and result["source_urls"]


def test_restatement_replaces_original_without_double_count():
    tables, _ = period_table("1330387", 2026, 2, Client("RESTATEMENT"))
    assert [r["cusip"] for r in tables] == ["OTHER"]
    with pytest.raises(ValueError, match="absence is not an exit"):
        compare_holder("1330387", "Amundi", "67066G104", 2026, 2, Client("RESTATEMENT"))


def test_unknown_amendment_fails_closed():
    with pytest.raises(ValueError, match="Unresolved amendment"):
        period_table("1330387", 2026, 2, Client(None))


@pytest.mark.parametrize("bad", [{"shareType": "PRN"}, {"shareType": None}, {"shares": float('nan')}, {"shares": -1}, {"sourceUrl": "https://example.com"}])
def test_invalid_or_unsourced_share_rows_cannot_be_facts(bad):
    r = row(100); r.update(bad)
    with pytest.raises(ValueError):
        matched_shares([r], "67066G104")


def test_options_and_other_cusips_are_not_combined():
    option = row(999); option["putCall"] = "CALL"
    assert matched_shares([row(97), row(3), option, row(700, "OTHER")], "67066G104")[0] == 100


def test_unverified_old_draft_is_a_publication_hard_stop():
    warnings = briefs._ownership_answer_warnings({"title": "Who is buying NVDA stock in the latest 13F filings?"}, {})
    assert warnings[0]["code"] == "ownership_evidence_unverified"
    assert briefs._publish_hard_stop_warnings({"warnings": warnings})


def test_fallback_cannot_claim_buying_without_evidence():
    with pytest.raises(Exception, match="Verified SEC share comparisons"):
        briefs._institutional_activity_fallback_article({}, {}, symbol="NVDA", company="NVIDIA", reader_company="NVIDIA", sources=[])


def test_bearish_sample_fallback_does_not_assert_accumulation():
    comparison = compare_holder("1330387", "Amundi", "67066G104", 2026, 2, Client())
    context = {"primary": {"institutional_ownership_detail": {"verification": "sec_matched_share_pairs_v1", "comparisons": [comparison], "reporting_period": "Q2 2026", "coverage": "One manager only."}}}
    article = briefs._institutional_activity_fallback_article({}, context, symbol="NVDA", company="NVIDIA", reader_company="NVIDIA", sources=[])
    assert "institutions are adding" not in str(article).lower()
    assert "-3 shares" in str(article)
    assert "market-wide net buying" in article["summary"]
