"""Annual growth from one provider series; never mix currencies or TTM periods."""
from datetime import date, timedelta
import math
from app.services.finnhub_research import FinnhubUnavailable

MAX_ANNUAL_AGE = timedelta(days=460)


def annual_ebitda_growth(series, observed_at):
    annual = series.get('annual', {})
    if not isinstance(annual, dict):
        raise FinnhubUnavailable('invalid_annual_metric_series')
    rows = annual.get('ebitda', [])
    if not isinstance(rows, list) or len(rows) > 1000:
        raise FinnhubUnavailable('invalid_annual_metric_series')
    points = {}
    for row in rows:
        try:
            period = date.fromisoformat(row['period'])
            value = row.get('v')
        except (TypeError, ValueError, KeyError, AttributeError):
            raise FinnhubUnavailable('invalid_annual_metric_period') from None
        if period > observed_at.date():
            raise FinnhubUnavailable('future_annual_metric_period')
        if value is not None:
            try:
                valid = type(value) in (int, float) and math.isfinite(value)
            except OverflowError:
                valid = False
            if not valid:
                raise FinnhubUnavailable('invalid_annual_metric_number')
        if period in points and points[period] != value:
            raise FinnhubUnavailable('conflicting_annual_metric_period')
        points[period] = value
    periods = sorted(points, reverse=True)
    if len(periods) < 2:
        return None
    end, prior_end = periods[:2]
    current, prior = points[end], points[prior_end]
    if (observed_at.date() - end > MAX_ANNUAL_AGE or not 350 <= (end-prior_end).days <= 380
            or current is None or prior is None or prior == 0):
        return None
    growth = (current-prior) / abs(prior) * 100
    if not math.isfinite(growth):
        raise FinnhubUnavailable('invalid_annual_metric_number')
    return growth, {'source': 'finnhub', 'path': 'series.annual.ebitda',
        'period_date': str(end), 'prior_period_date': str(prior_end),
        'basis': 'annual_yoy', 'unit': 'percent', 'scale': 100,
        'observed_at': observed_at.isoformat(),
        'formula': '(current annual EBITDA - prior annual EBITDA) / abs(prior annual EBITDA) * 100',
        'inputs': [{'period': str(day), 'v': points[day]} for day in (end, prior_end)],
        'limitations': ['Provider-defined EBITDA; not asserted equivalent to another vendor or adjusted EBITDA.',
            'Annual series only; not TTM or five-year CAGR. Same-series units cancel in the ratio.',
            'Period end is not a filing or historical availability date.']}
