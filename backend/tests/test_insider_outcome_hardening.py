import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.services.member_performance import INSIDER_METHODOLOGY_VERSION, compute_insider_trade_outcomes


class InsiderOutcomeHardeningTests(unittest.TestCase):
    def _event(self, *, event_id: int, symbol: str, trade_type: str | None, payload: dict) -> SimpleNamespace:
        ts = datetime(2024, 1, 10, tzinfo=timezone.utc)
        return SimpleNamespace(
            id=event_id,
            symbol=symbol,
            trade_type=trade_type,
            payload_json=payload,
            event_date=ts,
            ts=ts,
            source="fmp",
            amount_min=None,
            amount_max=None,
            member_bioguide_id=None,
            member_name=None,
        )

    @patch("app.services.member_performance.get_current_prices_meta_db")
    @patch("app.services.member_performance._entry_price_for_congress_event")
    def test_non_market_insider_trade_is_classified_and_skips_entry_scoring(self, entry_lookup, quote_lookup):
        event = self._event(
            event_id=1,
            symbol="AAPL",
            trade_type="m-exempt",
            payload={
                "symbol": "AAPL",
                "transaction_date": "2024-01-02",
                "transaction_type": "M-EXEMPT",
                "is_market_trade": False,
            },
        )

        quote_lookup.return_value = {"^GSPC": {"price": 5000.0, "asof_ts": datetime(2024, 1, 10)}}
        rows = compute_insider_trade_outcomes(db=SimpleNamespace(), events=[event], benchmark_symbol="^GSPC")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["scoring_status"], "insider_non_market")
        self.assertIn("excluded from insider analytics", rows[0]["scoring_error"])
        entry_lookup.assert_not_called()
        quote_lookup.assert_called_once_with(unittest.mock.ANY, ["^GSPC"])

    @patch("app.services.member_performance._benchmark_entry_close_for_trade_date", return_value=4000.0)
    @patch("app.services.member_performance.get_current_prices_meta_db")
    @patch("app.services.member_performance._entry_price_for_congress_event")
    def test_market_insider_sale_still_uses_insider_v1_path(self, entry_lookup, quote_lookup, _benchmark_lookup):
        event = self._event(
            event_id=2,
            symbol="AAPL",
            trade_type="sale",
            payload={
                "symbol": "AAPL",
                "transaction_date": "2024-01-02",
                "transaction_type": "S-SALE",
                "is_market_trade": True,
            },
        )

        entry_lookup.return_value = {"close": 100.0, "status": "ok", "error": None, "symbol": "AAPL"}

        def quote_side_effect(_db, symbols):
            if symbols == ["AAPL"]:
                return {"AAPL": {"price": 90.0, "asof_ts": datetime(2024, 1, 10)}}
            if symbols == ["^GSPC"]:
                return {"^GSPC": {"price": 4200.0, "asof_ts": datetime(2024, 1, 10)}}
            return {}

        quote_lookup.side_effect = quote_side_effect

        rows = compute_insider_trade_outcomes(db=SimpleNamespace(), events=[event], benchmark_symbol="^GSPC")

        self.assertEqual(rows[0]["scoring_status"], "ok")
        self.assertEqual(rows[0]["methodology_version"], INSIDER_METHODOLOGY_VERSION)
        entry_lookup.assert_called_once()
        self.assertEqual(rows[0]["entry_price"], 100.0)

    @patch("app.services.member_performance._benchmark_entry_close_for_trade_date", return_value=4000.0)
    @patch("app.services.member_performance.get_current_prices_meta_db")
    @patch("app.services.member_performance._entry_price_for_congress_event")
    def test_outlier_insider_transaction_price_falls_back_to_market_close(self, entry_lookup, quote_lookup, _benchmark_lookup):
        event = self._event(
            event_id=4,
            symbol="TCBI",
            trade_type="purchase",
            payload={
                "symbol": "TCBI",
                "transaction_date": "2026-02-03",
                "transaction_type": "P-Purchase",
                "is_market_trade": True,
                "price": 24.40,
            },
        )

        entry_lookup.return_value = {"close": 105.98, "status": "ok", "error": None, "symbol": "TCBI"}

        def quote_side_effect(_db, symbols):
            if symbols == ["TCBI"]:
                return {"TCBI": {"price": 92.67, "asof_ts": datetime(2026, 3, 29)}}
            if symbols == ["^GSPC"]:
                return {"^GSPC": {"price": 4200.0, "asof_ts": datetime(2026, 3, 29)}}
            return {}

        quote_lookup.side_effect = quote_side_effect

        rows = compute_insider_trade_outcomes(db=SimpleNamespace(), events=[event], benchmark_symbol="^GSPC")

        self.assertEqual(rows[0]["scoring_status"], "ok")
        self.assertAlmostEqual(rows[0]["entry_price"], 105.98, places=2)
        self.assertLess(rows[0]["return_pct"], 0)

    @patch("app.services.member_performance._benchmark_entry_close_for_trade_date", return_value=4000.0)
    @patch("app.services.member_performance.get_current_prices_meta_db")
    @patch("app.services.member_performance._entry_price_for_congress_event")
    def test_plausible_insider_transaction_price_is_still_used(self, entry_lookup, quote_lookup, _benchmark_lookup):
        event = self._event(
            event_id=5,
            symbol="JPM",
            trade_type="purchase",
            payload={
                "symbol": "JPM",
                "transaction_date": "2026-02-03",
                "transaction_type": "P-Purchase",
                "is_market_trade": True,
                "price": 105.80,
            },
        )

        entry_lookup.return_value = {"close": 106.00, "status": "ok", "error": None, "symbol": "JPM"}

        def quote_side_effect(_db, symbols):
            if symbols == ["JPM"]:
                return {"JPM": {"price": 102.0, "asof_ts": datetime(2026, 3, 29)}}
            if symbols == ["^GSPC"]:
                return {"^GSPC": {"price": 4200.0, "asof_ts": datetime(2026, 3, 29)}}
            return {}

        quote_lookup.side_effect = quote_side_effect

        rows = compute_insider_trade_outcomes(db=SimpleNamespace(), events=[event], benchmark_symbol="^GSPC")

        self.assertEqual(rows[0]["scoring_status"], "ok")
        self.assertAlmostEqual(rows[0]["entry_price"], 105.80, places=2)

    @patch("app.services.member_performance._latest_eod_close_with_meta", return_value={"close": None, "status": "provider_429"})
    @patch("app.services.member_performance.get_current_prices_meta_db")
    @patch("app.services.member_performance._entry_price_for_congress_event")
    def test_market_trade_provider_429_remains_explicit(self, entry_lookup, quote_lookup, _latest_eod):
        event = self._event(
            event_id=3,
            symbol="AAPL",
            trade_type="sale",
            payload={
                "symbol": "AAPL",
                "transaction_date": "2024-01-02",
                "transaction_type": "sale",
                "is_market_trade": True,
            },
        )

        entry_lookup.return_value = {"close": 100.0, "status": "ok", "error": None, "symbol": "AAPL"}

        def quote_side_effect(_db, symbols):
            if symbols == ["AAPL"]:
                return {"AAPL": {"price": None, "asof_ts": None, "status": "provider_429"}}
            if symbols == ["^GSPC"]:
                return {"^GSPC": {"price": 4200.0, "asof_ts": datetime(2024, 1, 10)}}
            return {}

        quote_lookup.side_effect = quote_side_effect

        rows = compute_insider_trade_outcomes(db=SimpleNamespace(), events=[event], benchmark_symbol="^GSPC")

        self.assertEqual(rows[0]["scoring_status"], "provider_429")
        self.assertIn("Provider quote lookup failed", rows[0]["scoring_error"])


if __name__ == "__main__":
    unittest.main()
