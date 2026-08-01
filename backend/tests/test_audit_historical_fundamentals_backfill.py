from __future__ import annotations

from app.jobs.audit_historical_fundamentals_backfill import _field_availability


def test_field_availability_separates_statement_history_from_current_fields():
    payload = {
        "annual": [{"date": "2025-12-31", "revenue": 1000, "eps": 2.0}],
        "quarterly": [
            {"date": "2024-03-31", "revenue": 100, "eps": 1.0, "freeCashFlow": 10, "grossMargin": 0.45},
            {"date": "2024-06-30", "revenue": 110, "eps": 1.1, "freeCashFlow": 11, "grossMargin": 0.46},
            {"date": "2024-09-30", "revenue": 120, "eps": 1.2, "freeCashFlow": 12, "grossMargin": 0.47},
            {"date": "2024-12-31", "revenue": 130, "eps": 1.3, "freeCashFlow": 13, "grossMargin": 0.48},
            {"date": "2025-03-31", "revenue": 150, "eps": 1.5, "freeCashFlow": 20, "grossMargin": 0.49},
        ],
        "summary": {"forwardPE": 22.0, "debtToEquity": 1.2},
        "valuation_metrics": {"forward_pe": 22.0, "as_of": "2026-01-01"},
        "health": {"debtToEquity": 1.2},
        "sections": {"valuation": "ok"},
        "subsections": {"health": {"status": "ok", "data": {"debtToEquity": 1.2}}},
    }

    availability = _field_availability(payload)

    assert availability["statement_counts"]["quarterly_revenue"] == 5
    assert availability["derived_growth_counts"]["revenue_growth"] == 1
    assert availability["derived_growth_counts"]["eps_growth"] == 1
    assert availability["current_or_derived_counts"]["summary_forward_pe"] == 1
    assert availability["current_or_derived_counts"]["health_debt_to_equity"] == 1
    assert availability["has_valuation_as_of"] is True
    assert availability["subsection_statuses"] == {"health": "ok"}
