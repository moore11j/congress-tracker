from types import SimpleNamespace
from xml.etree import ElementTree as etree

from app.ingest_house_annual_disclosures import _index_row_from_xml, parse_holdings_from_pdf_text


def test_index_row_resolves_members_with_current_metadata_resolver_signature():
    class Resolver:
        def resolve(self, *, bioguide_id, first_name, last_name, chamber, state, house_district, full_name):
            assert bioguide_id is None
            assert (first_name, last_name, chamber, state, house_district, full_name) == (
                "Brian",
                "Babin",
                "house",
                "TX",
                "36",
                "Brian Babin",
            )
            return SimpleNamespace(bioguide_id="B001291")

    row = _index_row_from_xml(
        etree.fromstring(
            "<Member><First>Brian</First><Last>Babin</Last><StateDst>TX36</StateDst>"
            "<FilingType>O</FilingType><FilingDate>05/15/2026</FilingDate><DocID>123</DocID></Member>"
        ),
        year=2025,
        metadata=Resolver(),
    )

    assert row.member_bioguide_id == "B001291"


def test_parse_house_annual_asset_table_with_corrupt_heading_and_equity_rows():
    text = """
S\x00\x00\x00 A: A\x00\x00\x00 \x00\x00\x00 "U\x00\x00\x00" I\x00\x00\x00
Asset Owner Value of Asset Income Type(s) Income Tx. > $1,000?
AllianceBernstein Holding L.P. Units (AB) [OL] SP $1,000,001 - $5,000,000 Partnership Income $100,001 - $1,000,000
Alphabet Inc. - Class A (GOOGL) [ST] SP $5,000,001 - $25,000,000 Dividends $15,001 - $50,000
Amazon.com, Inc. (AMZN) [ST] SP $5,000,001 - $25,000,000 None
Apple Inc. (AAPL) [ST] SP $25,000,001 - $50,000,000 Capital Gains, Dividends Over $5,000,000
Liabilities
"""

    holdings = parse_holdings_from_pdf_text(text)

    assert [holding.symbol for holding in holdings] == ["AB", "GOOGL", "AMZN", "AAPL"]
    assert holdings[1].asset_name == "Alphabet Inc. - Class A"
    assert holdings[1].owner == "sp"
    assert holdings[1].asset_type == "stock"
    assert holdings[1].value_range == "$5,000,001 - $25,000,000"
    assert holdings[1].value_min == 5_000_001.0
    assert holdings[1].value_max == 25_000_000.0
    assert holdings[3].income_type == "Capital Gains, Dividends"
    assert holdings[3].income_range == "Over $5,000,000"


def test_parse_house_annual_asset_table_with_wrapped_value_range():
    text = """
Asset Owner Value of Asset Income Type(s) Income Tx. > $1,000?
Alphabet Inc. - Class A (GOOGL) [ST] SP $5,000,001 -
$25,000,000 Dividends $15,001 - $50,000
Liabilities
"""

    holdings = parse_holdings_from_pdf_text(text)

    assert len(holdings) == 1
    assert holdings[0].symbol == "GOOGL"
    assert holdings[0].value_range == "$5,000,001 - $25,000,000"
