import xml.etree.ElementTree as ET
import pytest
from app.clients.direct_sources import DirectSourceError, parse_13f_submission
from test_direct_sec_collection import submission, META


def test_validation_and_normalization_read_each_row_once(monkeypatch):
    paths=[]
    class Element(ET.Element):
        def findtext(self,path,*args,**kwargs):
            if self.tag.rsplit('}',1)[-1]=='infoTable':paths.append(path)
            return super().findtext(path,*args,**kwargs)
    original=ET.fromstring
    def parsed(payload):
        return original(payload,parser=ET.XMLParser(target=ET.TreeBuilder(element_factory=Element)))
    monkeypatch.setattr(ET,'fromstring',parsed)
    _,result=parse_13f_submission(submission(),META)
    assert result['metadata']['table_value_total_usd']=='300'
    assert [r['shares'] for r in result['positions']]==[10,20]
    assert paths==[], 'Validation must reuse the extracted fields'


@pytest.mark.parametrize('value',[b'',b'-1',b'NaN',b'Infinity',b'1E999'])
@pytest.mark.parametrize('tag',[b'value',b'sshPrnamt'])
def test_invalid_first_numeric_leaf_cannot_fall_through_to_valid_duplicate(tag,value):
    old=b'<'+tag+b'>'+ (b'100' if tag==b'value' else b'10') + b'</'+tag+b'>'
    new=b'<'+tag+b'>'+value+b'</'+tag+b'>'+old
    raw=submission().replace(old,new,1)
    with pytest.raises(DirectSourceError):parse_13f_submission(raw,META)


def test_split_parents_and_duplicate_fields_keep_first_matching_leaf():
    raw=submission().replace(b'<value>100</value>',b'<value> 100 </value><value>999</value>',1)
    raw=raw.replace(b'<shrsOrPrnAmt><sshPrnamt>10</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>',
        b'<shrsOrPrnAmt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt><shrsOrPrnAmt><sshPrnamt>10</sshPrnamt><sshPrnamtType>PRN</sshPrnamtType></shrsOrPrnAmt>',1)
    _,result=parse_13f_submission(raw,META)
    assert result['positions'][0]['shareType']=='SH'
    assert result['positions'][0]['shares']==10
    assert result['positions'][0]['valueUsd']==100


@pytest.mark.parametrize('old,new',[(b'<cusip>000361105</cusip>',b'<cusip/><cusip>000361105</cusip>'),
    (b'<sshPrnamtType>SH</sshPrnamtType>',b'<sshPrnamtType/><sshPrnamtType>SH</sshPrnamtType>')])
def test_blank_first_required_leaf_remains_invalid(old,new):
    with pytest.raises(DirectSourceError):parse_13f_submission(submission().replace(old,new,1),META)
