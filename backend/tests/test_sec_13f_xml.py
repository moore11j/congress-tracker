import xml.etree.ElementTree as ET

import pytest

from app.clients.direct_sources import parse_13f_submission
from app.clients.sec_edgar import parse_13f_information_table
from test_direct_13f_publication import document


@pytest.mark.parametrize("namespace", ["", ' xmlns="urn:sec:table"'])
def test_namespace_duplicates_and_split_parent_fields_keep_first_xpath_match(namespace):
    payload = f'''<informationTable{namespace} xmlns:x="urn:other">
      <infoTable>
        <nameOfIssuer> First issuer </nameOfIssuer><x:nameOfIssuer>Later</x:nameOfIssuer>
        <cusip> 000361105 </cusip><value>1,234</value><value>9999</value>
        <titleOfClass> Common </titleOfClass><investmentDiscretion> SOLE </investmentDiscretion>
        <putCall/><putCall>CALL</putCall>
        <shrsOrPrnAmt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <x:shrsOrPrnAmt><x:sshPrnamt> 12 </x:sshPrnamt><sshPrnamtType>PRN</sshPrnamtType></x:shrsOrPrnAmt>
        <shrsOrPrnAmt><sshPrnamt>99</sshPrnamt></shrsOrPrnAmt>
        <votingAuthority><Sole/></votingAuthority>
        <votingAuthority><Sole>99</Sole><Shared>5</Shared><None>7</None></votingAuthority>
      </infoTable>
      <infoTable><cusip/><cusip>SHOULD_NOT_FALL_THROUGH</cusip></infoTable>
    </informationTable>'''.encode()
    assert parse_13f_information_table(payload, accession_number="accession", source_url="source") == [{
        "cusip": "000361105", "issuerName": "First issuer", "titleOfClass": "Common",
        "shareType": "SH", "sourceUrl": "source", "shares": 12.0, "valueUsd": 1234.0,
        "putCall": None, "investmentDiscretion": "SOLE",
        "votingAuthority": {"sole": None, "shared": 5.0, "none": 7.0},
        "source": "sec_edgar", "accessionNumber": "accession",
    }]


def test_submission_parses_each_attachment_once_and_retains_normalized_rows(monkeypatch):
    source = document()
    original = ET.fromstring
    seen = []
    def counted(payload, *args, **kwargs):
        seen.append(payload)
        return original(payload, *args, **kwargs)
    monkeypatch.setattr(ET, "fromstring", counted)
    text, parsed = parse_13f_submission(source["raw"], source["metadata"])
    assert text == source["raw"].decode()
    assert len(seen) == len(set(seen)) == 2
    assert parsed["positions"] and parsed["positions"][0]["source_line_ref"] == "1"


def test_malformed_information_table_still_raises():
    with pytest.raises(ET.ParseError):
        parse_13f_information_table(b"<informationTable>", accession_number="a", source_url="s")
