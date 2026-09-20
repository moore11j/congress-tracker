"""Conservative filing-date SEC features, separate from live fundamentals."""
from datetime import date
import math

TAGS={
    'revenue':['RevenueFromContractWithCustomerExcludingAssessedTax','Revenues','SalesRevenueNet'],
    'net_income':['NetIncomeLoss'],
    'operating_income':['OperatingIncomeLoss'],
    'assets':['Assets'], 'liabilities':['Liabilities'],
    'cash':['CashAndCashEquivalentsAtCarryingValue','CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents'],
    'current_assets':['AssetsCurrent'], 'current_liabilities':['LiabilitiesCurrent'],
}
QUARTERLY={'revenue','net_income','operating_income'}


def extract_series(payload):
    gaap=payload.get('facts',{}).get('us-gaap',{}); result={}
    for key,tags in TAGS.items():
        rows=[]
        for priority,tag in enumerate(tags):
            for r in gaap.get(tag,{}).get('units',{}).get('USD',[]):
                if r.get('form') not in ('10-K','10-Q','10-K/A','10-Q/A'): continue
                if not all(r.get(k) for k in ['end','filed','accn']): continue
                try:
                    end=date.fromisoformat(r['end']); filed=date.fromisoformat(r['filed']); value=float(r['val'])
                    start=date.fromisoformat(r['start']) if r.get('start') else None
                except (KeyError,ValueError,TypeError): continue
                if not math.isfinite(value) or end>filed: continue
                if key in QUARTERLY and (start is None or not 75<=(end-start).days<=110): continue
                if key not in QUARTERLY and start is not None: continue
                rows.append({'end':end.isoformat(),'start':start.isoformat() if start else None,
                             'filed':filed.isoformat(),'accn':r['accn'],'value':value,'tag':tag,'priority':priority})
        result[key]=rows
    return result


def known_periods(series,asof):
    """Select revisions per period only from filings strictly before decision date."""
    by_period={}
    for r in series:
        if r['filed']>=asof or r['end']>asof: continue
        key=(r['start'],r['end']); old=by_period.get(key)
        if old is None or r['priority']<old['priority'] or (r['priority']==old['priority'] and (r['filed'],r['accn'])>(old['filed'],old['accn'])):
            by_period[key]=r
    return sorted(by_period.values(),key=lambda r:(r['end'],r['filed']))


def comparison(periods,current,days,tolerance):
    if current is None: return None
    end=date.fromisoformat(current['end'])
    candidates=[r for r in periods if abs((end-date.fromisoformat(r['end'])).days-days)<=tolerance]
    return min(candidates,key=lambda r:abs((end-date.fromisoformat(r['end'])).days-days)) if candidates else None


def same_period(periods,current):
    if current is None: return None
    return next((r for r in reversed(periods) if r['start']==current['start'] and r['end']==current['end']),None)


def build_features(series,asof):
    today=date.fromisoformat(asof)
    periods={k:known_periods(series.get(k,[]),asof) for k in TAGS}
    current={k:(rr[-1] if rr and (today-date.fromisoformat(rr[-1]['end'])).days<=200 else None) for k,rr in periods.items()}
    names=['revenue_growth','revenue_growth_change','net_margin','operating_margin','net_margin_change',
           'income_growth_scaled','quarterly_roa','liabilities_assets','cash_assets','current_ratio','filing_age_days']
    f={name:float('nan') for name in names}; used=[]
    def use(r):
        if r is not None: used.append({k:r[k] for k in ['tag','start','end','filed','accn']})
        return r['value'] if r else None
    def ratio(a,b):
        return a/b if a is not None and b is not None and b>0 else float('nan')
    rev=current['revenue']; revenue=use(rev)
    old_rev=comparison(periods['revenue'],rev,365,35); yr_revenue=use(old_rev)
    if revenue is not None and yr_revenue is not None and yr_revenue>0:
        f['revenue_growth']=revenue/yr_revenue-1
    prior_rev=comparison(periods['revenue'],rev,91,25)
    prior_yr=comparison(periods['revenue'],prior_rev,365,35)
    if prior_rev and prior_yr and prior_yr['value']>0:
        f['revenue_growth_change']=f['revenue_growth']-(use(prior_rev)/use(prior_yr)-1)
    income=same_period(periods['net_income'],rev); operating=same_period(periods['operating_income'],rev)
    f['net_margin']=ratio(use(income),revenue); f['operating_margin']=ratio(use(operating),revenue)
    old_income=same_period(periods['net_income'],old_rev)
    if old_income and yr_revenue is not None:
        f['net_margin_change']=f['net_margin']-ratio(use(old_income),yr_revenue)
    inc=current['net_income']; old_inc=comparison(periods['net_income'],inc,365,35)
    if inc and old_inc and abs(old_inc['value'])>0:
        f['income_growth_scaled']=(use(inc)-use(old_inc))/abs(old_inc['value'])
    asset=current['assets']; assets=use(asset)
    if inc and asset and inc['end']==asset['end']: f['quarterly_roa']=ratio(use(inc),assets)
    for k,name in [('liabilities','liabilities_assets'),('cash','cash_assets')]:
        r=current[k]
        if r and asset and r['end']==asset['end']: f[name]=ratio(use(r),assets)
    ca=current['current_assets']; cl=current['current_liabilities']
    if ca and cl and ca['end']==cl['end']: f['current_ratio']=ratio(use(ca),use(cl))
    fresh=[r for r in current.values() if r]
    if fresh: f['filing_age_days']=min((today-date.fromisoformat(r['filed'])).days for r in fresh)
    for key,r in current.items(): f['available:'+key]=float(r is not None)
    for name,value in list(f.items()):
        if math.isfinite(value): f[name]=min(value,365.) if name=='filing_age_days' else max(-10.,min(10.,value))
    eligible=rev is not None or asset is not None
    if any(r['filed']>=asof for r in used): raise AssertionError('Future SEC filing leaked')
    return f,eligible,used
