import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.services.member_performance import _latest_eod_close_with_meta
from app.services.price_lookup import get_eod_close_with_meta


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class PriceLookupDateClampingTests(unittest.TestCase):
    def _db(self):
        return SimpleNamespace(
            get=Mock(return_value=None),
            execute=Mock(),
            commit=Mock(),
            rollback=Mock(),
        )

    @patch("app.services.price_lookup._fetch_with_backoff")
    @patch("app.services.price_lookup.effective_lookup_max_date", return_value=date(2026, 3, 16))
    @patch.dict("os.environ", {"FMP_API_KEY": "test-key"})
    def test_explicit_future_date_is_clamped_for_eod_lookup(self, _max_date, fetch_with_backoff):
        fetch_with_backoff.return_value = _FakeResponse(200, [{"date": "2026-03-16", "close": 123.45}])
        result = get_eod_close_with_meta(self._db(), "MCS", "2026-03-17")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["close"], 123.45)
        _, params = fetch_with_backoff.call_args[0]
        self.assertEqual(params["from"], "2026-03-16")
        self.assertEqual(params["to"], "2026-03-16")

    @patch("app.services.member_performance.get_eod_close_with_meta")
    @patch("app.services.member_performance.effective_lookup_max_date", return_value=date(2026, 3, 16))
    def test_latest_eod_scan_uses_app_local_day_after_utc_rollover(self, _max_date, get_eod_close):
        get_eod_close.return_value = {"close": 10.0, "status": "ok", "error": None}
        result = _latest_eod_close_with_meta(self._db(), "MCS")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["date"], "2026-03-16")
        get_eod_close.assert_called_once_with(unittest.mock.ANY, "MCS", "2026-03-16")

    @patch("app.services.price_lookup._fetch_with_backoff")
    @patch("app.services.price_lookup.effective_lookup_max_date", return_value=date(2026, 3, 16))
    @patch.dict("os.environ", {"FMP_API_KEY": "test-key"})
    def test_past_date_lookup_is_unchanged(self, _max_date, fetch_with_backoff):
        fetch_with_backoff.return_value = _FakeResponse(200, [{"date": "2026-03-14", "close": 99.0}])
        result = get_eod_close_with_meta(self._db(), "MCS", "2026-03-14")
        self.assertEqual(result["status"], "ok")
        _, params = fetch_with_backoff.call_args[0]
        self.assertEqual(params["from"], "2026-03-14")
        self.assertEqual(params["to"], "2026-03-14")

    @patch("app.services.price_lookup._fetch_with_backoff")
    @patch.dict("os.environ", {"FMP_API_KEY": "test-key"})
    def test_non_trading_day_uses_prior_close_from_full_series_retry(self, fetch_with_backoff):
        fetch_with_backoff.side_effect = [
            _FakeResponse(200, []),
            _FakeResponse(
                200,
                [
                    {"date": "2026-02-27", "close": 44.0},
                    {"date": "2026-02-26", "close": 43.0},
                ],
            ),
        ]
        result = get_eod_close_with_meta(self._db(), "WFC", "2026-03-01")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["close"], 44.0)
        self.assertEqual(fetch_with_backoff.call_count, 2)

    @patch("app.services.price_lookup._fetch_with_backoff")
    @patch.dict("os.environ", {"FMP_API_KEY": "test-key"})
    def test_coarse_month_start_date_can_resolve_to_prior_trading_day(self, fetch_with_backoff):
        fetch_with_backoff.side_effect = [
            _FakeResponse(200, []),
            _FakeResponse(
                200,
                [
                    {"date": "2025-08-29", "close": 63.5},
                    {"date": "2025-08-28", "close": 63.0},
                ],
            ),
        ]
        result = get_eod_close_with_meta(self._db(), "WFC", "2025-09-01")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["close"], 63.5)


if __name__ == "__main__":
    unittest.main()
