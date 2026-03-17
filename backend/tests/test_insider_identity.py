import unittest
from types import SimpleNamespace

from app.routers.events import _insider_company_name, _insider_display_name, _insider_role


class InsiderIdentityFallbackTests(unittest.TestCase):
    def test_display_name_falls_back_to_raw_reporting_name(self):
        event = SimpleNamespace(member_name=None, symbol="AAPL")
        payload = {
            "raw": {
                "reportingName": "MEREDITH COOK",
            }
        }
        self.assertEqual(_insider_display_name(event, payload), "MEREDITH COOK")

    def test_role_falls_back_to_raw_position_fields(self):
        payload = {
            "raw": {
                "position": "Chief Financial Officer",
            }
        }
        self.assertEqual(_insider_role(payload), "Chief Financial Officer")

    def test_company_name_rejects_placeholder_security_title(self):
        event = SimpleNamespace(symbol="AAPL")
        payload = {
            "company_name": "Common Stock",
            "raw": {
                "issuerName": "Apple Inc.",
            },
        }
        self.assertEqual(_insider_company_name(event, payload), "Apple Inc.")


if __name__ == "__main__":
    unittest.main()
