import unittest
import math
from sec_filing_features import extract_series, build_features, known_periods


def fact(value,end='2024-03-31',filed='2024-05-01',start='2024-01-01',acc='one'):
    r={'val':value,'end':end,'filed':filed,'accn':acc,'form':'10-Q'}
    if start: r['start']=start
    return r


def payload(revenues,assets=None):
    return {'facts':{'us-gaap':{'Revenues':{'units':{'USD':revenues}},'Assets':{'units':{'USD':assets or []}}}}}


class FilingTimeInvariants(unittest.TestCase):
    def test_same_day_and_future_restatements_are_unavailable(self):
        series=extract_series(payload([fact(100),fact(999,filed='2024-07-01',acc='restated')]))
        self.assertEqual(known_periods(series['revenue'],'2024-05-01'),[])
        self.assertEqual(known_periods(series['revenue'],'2024-06-01')[0]['value'],100)
        self.assertEqual(known_periods(series['revenue'],'2024-07-02')[0]['value'],999)

    def test_annual_and_ytd_not_misread_as_quarters(self):
        series=extract_series(payload([fact(100),fact(200,end='2024-06-30',filed='2024-08-01'),fact(400,end='2024-12-31',filed='2025-02-01')]))
        self.assertEqual(len(series['revenue']),1)

    def test_filing_dated_growth_and_provenance(self):
        rr=[fact(100),fact(80,end='2023-03-31',filed='2023-05-01',start='2023-01-01')]
        f,eligible,used=build_features(extract_series(payload(rr)),'2024-05-02')
        self.assertTrue(eligible)
        self.assertAlmostEqual(f['revenue_growth'],.25)
        self.assertTrue(all(r['filed']<'2024-05-02' for r in used))

    def test_stale_values_not_usable_current_inputs(self):
        f,eligible,_=build_features(extract_series(payload([fact(100)])),'2025-05-01')
        self.assertFalse(eligible)
        self.assertTrue(math.isnan(f['revenue_growth']))

    def test_instant_assets_do_not_require_quarter_duration(self):
        f,eligible,_=build_features(extract_series(payload([], [fact(1000,start=None)])),'2024-05-02')
        self.assertTrue(eligible)
        self.assertEqual(f['available:assets'],1.)

    def test_mismatched_income_period_cannot_make_margin(self):
        data=payload([fact(100)])
        data['facts']['us-gaap']['NetIncomeLoss']={'units':{'USD':[fact(10,end='2023-12-31',start='2023-10-01')]}}
        f,_,_=build_features(extract_series(data),'2024-05-02')
        self.assertTrue(math.isnan(f['net_margin']))


if __name__=='__main__': unittest.main()
