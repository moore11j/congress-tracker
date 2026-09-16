from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from forward_bullish_core import challenger,new_decisions,measure,price_books,PROVIDERS,session_on_or_after
from forward_bullish_study import append


def trade(ratio=2,filing='2026-09-14',actor='known',side='buy',low=100,high=100):
    return {'source':'insider','ticker':'TEST','filing':filing,'side':side,'actor':actor,
            'unusual_ratio':ratio,'prior_purchase_count':3,'low':low,'high':high}


def event(id='1',created='2026-09-16T15:00:00+00:00',closed=None):
    return {'anchor':{'id':id,'ticker_at_time':'TEST','created_at':created,'calculated_at':created,'direction':'bullish'},'closed_at':closed}


class ForwardInvariants(unittest.TestCase):
    def test_known_ordinary_purchase_abstains_but_missing_history_falls_back(self):
        now='2026-09-15T23:00:00+00:00'
        self.assertFalse(challenger('TEST','bullish',[trade(1)],now)['keep'])
        self.assertEqual(challenger('TEST','bullish',[trade(None)],now)['status'],'unknown_fallback')
        self.assertTrue(challenger('TEST','bullish',[trade(2)],now)['keep'])

    def test_unknown_actor_and_amount_cannot_create_conviction(self):
        now='2026-09-15T23:00:00+00:00'
        for rr in [[trade(actor=None)],[trade(low=None)]]:
            self.assertEqual(challenger('TEST','bullish',rr,now)['status'],'unknown_fallback')

    def test_same_day_and_future_filings_are_unavailable(self):
        now='2026-09-15T23:00:00+00:00'
        for day in ['2026-09-15','2026-09-16']:
            self.assertEqual(challenger('TEST','bullish',[trade(filing=day)],now)['status'],'unknown_fallback')

    def test_net_buying_required_and_bearish_unchanged(self):
        rr=[trade(),trade(side='sell',low=200,high=200)]
        now='2026-09-15T23:00:00+00:00'
        self.assertFalse(challenger('TEST','bullish',rr,now)['keep'])
        self.assertTrue(challenger('TEST','bearish',rr,now)['keep'])

    def test_only_new_open_prospective_anchors_enroll(self):
        payload={'captured_at':'2026-09-16T23:00:00+00:00','confirmations':[event('seen'),event('new'),event('old','2026-09-14T15:00:00+00:00'),event('closed',closed='2026-09-16'),event('future','2026-09-17T15:00:00+00:00')]}
        rows,audit=new_decisions(payload,{'seen'},'2026-09-15T23:00:00+00:00','2026-09-16T23:01:00+00:00',[])
        self.assertEqual([r['id'] for r in rows],['new']);self.assertEqual(sum(audit.values()),3)
        self.assertEqual(rows[0]['entry_date'],'2026-09-17')
        self.assertEqual(rows[0]['targets']['30'],'2026-10-19')

    def test_late_processing_cannot_backdate_predictions(self):
        with self.assertRaises(ValueError):new_decisions({'captured_at':'2026-09-16T23:00:00+00:00'},set(),'2026-09-15T00:00:00+00:00','2026-09-17T01:00:00+00:00',[])

    def test_bounded_calendar_skips_weekends_not_missing_prices(self):
        self.assertEqual(session_on_or_after('2026-11-15'),'2026-11-16')
        with self.assertRaises(ValueError):session_on_or_after('2026-12-25')

    def test_prices_need_same_provider_and_actual_target(self):
        d={'ticker':'TEST','direction':'bullish','entry_date':'2026-09-16','targets':{'7':'2026-09-23'}}
        rows=[[s,day,price,price,price,1,PROVIDERS[0]] for s in ['TEST','SPY'] for day,price in [('2026-09-16',100),('2026-09-24',120)]]
        self.assertEqual(measure(d,7,price_books(rows),'2026-09-25T23:00:00+00:00')['status'],'missing_consistent_prices')
        for s in ['TEST','SPY']:rows.append([s,'2026-09-23',110,110,110,1,PROVIDERS[1]])
        self.assertEqual(measure(d,7,price_books(rows),'2026-09-25T23:00:00+00:00')['status'],'missing_consistent_prices')
        for s in ['TEST','SPY']:rows.append([s,'2026-09-23',110,110,110,1,PROVIDERS[0]])
        self.assertEqual(measure(d,7,price_books(rows),'2026-09-23T19:59:00+00:00')['status'],'immature')
        self.assertTrue(measure(d,7,price_books(rows),'2026-09-23T20:01:00+00:00')['correct'])

    def test_append_refuses_rewriting_prediction(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'prediction.json';append(path,{'keep':True});append(path,{'keep':True})
            with self.assertRaises(ValueError):append(path,{'keep':False})


if __name__=='__main__':unittest.main()
