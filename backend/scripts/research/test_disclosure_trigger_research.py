import unittest
from analyze_disclosure_triggers import cooldown, measure, PROVIDERS


def row(symbol, day, close=100, adjusted=100, opening=100):
    return [symbol, day, close, adjusted, opening, 1000, PROVIDERS[0], None, None]


class DisclosureTriggerInvariants(unittest.TestCase):
    def test_missing_outcome_still_consumes_cooldown_and_boundary_is_inclusive(self):
        signals = [{'ticker': 'A', 'filing': d} for d in ['2026-01-01', '2026-02-01', '2026-04-01']]
        self.assertEqual([r['filing'] for r in cooldown(signals)], ['2026-01-01', '2026-04-01'])

    def test_filing_cannot_enter_same_session_and_target_rolls_forward(self):
        days = ['2026-08-07', '2026-08-10', '2026-08-17']
        prices = {p: {s: {d: row(s, d) for d in days} for s in ['A', 'SPY']} for p in PROVIDERS}
        out = measure({'ticker': 'A', 'filing': '2026-08-07'}, 7, prices, days)
        self.assertEqual(out['entry_date'], '2026-08-10')
        self.assertEqual(out['target_date'], '2026-08-17')

    def test_endpoints_cannot_mix_provider_bases(self):
        days = ['2026-08-10', '2026-08-17']
        prices = {p: {'A': {}, 'SPY': {d: row('SPY', d) for d in days}} for p in PROVIDERS}
        prices[PROVIDERS[0]]['A'][days[0]] = row('A', days[0])
        prices[PROVIDERS[1]]['A'][days[1]] = row('A', days[1])
        self.assertEqual(measure({'ticker': 'A', 'filing': '2026-08-07'}, 7, prices, days)['status'], 'missing_consistent_prices')

    def test_adjusted_open_and_existing_excess_grading(self):
        days = ['2026-08-10', '2026-08-17']
        prices = {p: {} for p in PROVIDERS}
        prices[PROVIDERS[0]] = {'A': {days[0]: row('A', days[0], 100, 50, 50), days[1]: row('A', days[1], 99)}, 'SPY': {days[0]: row('SPY', days[0]), days[1]: row('SPY', days[1], 98)}}
        out = measure({'ticker': 'A', 'filing': '2026-08-07'}, 7, prices, days)
        self.assertAlmostEqual(out['entry_price'], 100)
        self.assertTrue(out['correct']); self.assertFalse(out['raw_correct'])

    def test_immature_distinct_from_missing_price(self):
        out = measure({'ticker': 'A', 'filing': '2026-09-10'}, 30, {}, ['2026-09-11'])
        self.assertEqual(out['status'], 'immature')

    def test_calendar_gap_is_not_silently_skipped(self):
        out = measure({'ticker': 'A', 'filing': '2026-08-07'}, 7, {}, ['2026-09-11'])
        self.assertEqual(out['status'], 'entry_calendar_gap')


if __name__ == '__main__': unittest.main()
