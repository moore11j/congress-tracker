from datetime import datetime, timezone
import pytest
from app.services.issuer_release_financials import AZO_URL, IssuerFinancialError, prepare_autozone_annual

NOW = datetime(2026,10,12,tzinfo=timezone.utc)


def release():
    rows = [
        ['4th Quarter, FY2026'], ['Net sales','999','888'],
        ['Fiscal Year 2026'], ['(in thousands, except per share data)'],
        ['', 'GAAP Results'], ['', '52 Weeks Ended', '', '52 Weeks Ended'],
        ['', 'August 29, 2026', '', 'August 30, 2025'],
        ['Net sales','$','2000','','$', '1000'],
        ['Cost of sales','1000','500'], ['Gross profit','1000','500'],
        ['Operating, SG&A expenses','400','300'], ['Operating profit (EBIT)','600','200'],
        ['Interest expense, net','100','50'], ['Income before taxes','500','150'],
        ['Income tax expense','100','30'], ['Net income','400','120'],
        ['Net income per share:'], ['Diluted','$','4.00','','$', '2.00'],
        ['Weighted average shares outstanding:'], ['Diluted','100','60'],
        ['Selected Balance Sheet Information'], ['Net sales','555','444'],
    ]
    table='<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in row)+'</tr>' for row in rows)+'</table>'
    return ('<html><p>MEMPHIS, Tenn., Sept. 22, 2026 AutoZone, Inc. (NYSE: AZO)</p>'+table+
            '<table><tr><td>Adjusted debt to EBITDAR</td><td>2.5</td></tr></table></html>').encode()


def test_annual_values_units_and_ratio_semantics():
    result=prepare_autozone_annual(release(),source_url=AZO_URL,observed_at=NOW)
    assert result['inputs']['revenue']=={'current':2_000_000,'prior':1_000_000,'unit':'USD'}
    assert result['inputs']['diluted_eps']['current']==4
    assert result['values']==dict(gross_margin=50,operating_margin=30,net_margin=20,
        revenue_growth=100,eps_growth=100,operating_margin_expansion=10)
    assert result['period_start']=='2025-08-31' and result['prior_period_start']=='2024-09-01'
    assert result['publication_eligible'] is False
    assert not {'net_debt_to_ebitda','forward_pe','free_cash_flow'} & result['values'].keys()
    assert result==prepare_autozone_annual(release(),source_url=AZO_URL,observed_at=NOW)


@pytest.mark.parametrize('before,after', [
    (b'NYSE: AZO',b'NYSE: OTHER'), (b'Sept. 22, 2026',b'Sept. 23, 2026'),
    (b'Fiscal Year 2026',b'Fiscal Year 2025'), (b'GAAP Results',b'Adjusted Results'),
    (b'in thousands',b'in millions'), (b'52 Weeks Ended',b'16 Weeks Ended'),
    (b'August 29, 2026',b'August 30, 2026'), (b'<td>Cost of sales</td>',b'<td>Unknown</td>'),
    (b'<td>2000</td>',b'<td>9000</td>'), (b'<td>2000</td>',b'<td>2e3</td>'),
    (b'<td>2000</td>',b'<td>NaN</td>'), (b'<td>2000</td>',b'<td>-2000</td>'),
])
def test_wrong_identity_period_units_or_accounting_are_held(before,after):
    with pytest.raises(IssuerFinancialError):
        prepare_autozone_annual(release().replace(before,after),source_url=AZO_URL,observed_at=NOW)


def test_duplicate_annual_tables_are_held():
    with pytest.raises(IssuerFinancialError,match='ambiguous'):
        prepare_autozone_annual(release()+release(),source_url=AZO_URL,observed_at=NOW)


@pytest.mark.parametrize('url,observed', [
    (AZO_URL+'?other=1',NOW), (AZO_URL,NOW.replace(tzinfo=None)),
    (AZO_URL,datetime(2026,9,22,23,tzinfo=timezone.utc)),
    (AZO_URL,datetime(2026,9,21,tzinfo=timezone.utc)),
    (AZO_URL,datetime(2026,9,23,2,tzinfo=timezone.utc)),
])
def test_unreviewed_source_and_unavailable_dates_are_held(url,observed):
    with pytest.raises(IssuerFinancialError):
        prepare_autozone_annual(release(),source_url=url,observed_at=observed)
