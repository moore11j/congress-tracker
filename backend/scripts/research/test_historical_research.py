import unittest
import numpy as np
from analyze_historical_panel import raw_open, price_features, split_for, predictions
from evaluate_historical_transfer import frozen_features
from datetime import date, timedelta
from analyze_cot_models import available_date
from analyze_ranked_bullish_gate import ranked_gate
from analyze_disclosure_models import disclosure_key, disclosure_features, DISCLOSURES


class HistoricalResearchInvariants(unittest.TestCase):
    def test_adjusted_open_is_converted_before_raw_return(self):
        row=['X','2020-01-01',100,80,72,None,None,None,None]
        self.assertEqual(raw_open(row),90)
        self.assertEqual(row[4],72)

    def test_missing_history_and_extreme_basis_are_not_features(self):
        self.assertIsNone(price_features(np.ones(252)))
        p=np.ones(253);p[-1]=np.nan
        self.assertIsNone(price_features(p))
        p[-1]=80
        self.assertIsNone(price_features(p))

    def test_validation_labels_must_mature_before_test(self):
        r={'entry_date':'2025-06-16','outcomes':{'30':{'target_date':'2025-07-16'}}}
        self.assertEqual(split_for(r),'purged')
        r['entry_date']='2025-05-19';r['outcomes']['30']['target_date']='2025-06-18'
        self.assertEqual(split_for(r),'validation')

    def test_bull_gate_never_relabels_rejected_bulls_as_bears(self):
        p=np.array([[.8,.9],[.4,.7]])
        np.testing.assert_array_equal(predictions(p,'bull_gate',.6),[1,0])
        np.testing.assert_array_equal(predictions(p,'directional',0),[-1,-1])

    def test_transfer_cannot_see_close_after_intraday_snapshot(self):
        days=[date(2025,1,1)+timedelta(days=i) for i in range(600)]
        days=[d.isoformat() for d in days if d.weekday()<5 and d<=date(2026,8,5)]
        prices={s:{d:[s,d,100+i*.1,100+i*.1,100+i*.1,1000] for i,d in enumerate(days)} for s in ['SPY','QQQ','X']}
        row={'ticker':'X','calculated_at':'2026-08-05T13:42:00+00:00'}
        before=frozen_features(row,prices)
        for s in prices:prices[s]['2026-08-05'][2]=100000
        self.assertEqual(before,frozen_features(row,prices))

    def test_cot_reports_are_delayed_until_available(self):
        self.assertEqual(available_date('2026-07-28'),'2026-08-04')
        self.assertEqual(available_date('2025-10-07'),'2026-01-06')
        self.assertEqual(available_date('2023-02-07'),'2023-04-01')

    def test_ranking_preserves_bears_and_uses_no_outcomes(self):
        rows=[{'ticker':s,'entry_date':'2026-08-06','direction':d} for s,d in [('A','bullish'),('B','bullish'),('C','bearish')]]
        np.testing.assert_array_equal(ranked_gate(rows,np.array([.2,.9,.8]),True),[0,1,-1])

    def test_disclosures_use_filing_date_and_exclude_awards(self):
        row={'ticker':'X','type':'insider_trade','side':'purchase','dates':{'filing_date':'2026-08-05','transaction_date':'2026-08-01'}}
        self.assertEqual(disclosure_key(row)[1],'2026-08-05')
        row['side']='a-award';self.assertIsNone(disclosure_key(row))
        row['side']='purchase';row['dates']['filing_date']='2026-07-31';self.assertIsNone(disclosure_key(row))

    def test_same_day_filing_not_available_without_timestamp(self):
        key=('TEST_RESEARCH_ONLY','insider_trade','buy')
        DISCLOSURES[key]=['2026-08-05']
        try:
            self.assertEqual(disclosure_features(key[0],'2026-08-05')['filings:insider_trade:buy:days_30'],0)
            self.assertGreater(disclosure_features(key[0],'2026-08-06')['filings:insider_trade:buy:days_30'],0)
        finally:del DISCLOSURES[key];disclosure_features.cache_clear()


if __name__=='__main__':unittest.main()
