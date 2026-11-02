"""Continue a reported bank revenue basis using two exact annual anchors.

Never splice totals with component periods, reinterpret contract fees, or skip
a newer filing. The caller already verifies bank classification and issuer CIK.
"""
from datetime import date

def select(ordinary, combined, company, available_by):
    from app.services.sec_fundamentals import _ttm, _prior_end
    if not ordinary or any(not r or r.get('tag') not in {'Revenues','RevenuesNetOfInterestExpense'} for r in ordinary.values()):
        return None
    old_end=max(e for _,e in ordinary)
    end=max((e for _,e in combined),default=None)
    if end is None or end<=old_end or not 0<=(available_by-end).days<=180 or (available_by-old_end).days>380:
        return None
    current=_ttm(combined,end);prior_end=_prior_end(combined,end)
    prior=_ttm(combined,prior_end) if prior_end else None
    if not current or not prior:return None
    recent=company.get('filings',{}).get('recent',{})
    filings=[]
    for i,form in enumerate(recent.get('form',[])):
        if form not in {'10-K','10-Q','10-K/A','10-Q/A','10-KT','10-QT','10-KT/A','10-QT/A','20-F','20-F/A','40-F','40-F/A'}:continue
        try:
            report=date.fromisoformat(recent['reportDate'][i]);filed=date.fromisoformat(recent['filingDate'][i]);acc=recent['accessionNumber'][i]
        except (ValueError,TypeError,KeyError,IndexError):return None
        if report<=filed<=available_by and acc:filings.append((report,filed,acc,form))
    if not filings:return None
    report,filed,acc,form=max(filings)
    if report!=end or form not in {'10-K','10-Q','10-K/A','10-Q/A'} or len({(a,f) for e,d,a,f in filings if (e,d)==(report,filed)})!=1:return None
    closing=[r for r in current['inputs'] if r['end']==str(end)]
    if not closing or any((r['filed'],r['accession'])!=(str(filed),acc) for r in closing):return None
    needed={tuple(date.fromisoformat(r[k]) for k in ('start','end')):r for t in (current,prior) for r in t['inputs']}
    floor=min(s for s,_ in needed)
    anchors=[]
    for key,left in ordinary.items():
        if key[1]<floor:continue
        right=combined.get(key)
        if not right or any(left.get(k)!=right.get(k) for k in ('value','unit','start','end','filed','accession')):return None
        if key in needed and 350<=(key[1]-key[0]).days+1<=380:
            anchors.append(dict(reported=left,bank_components=right['inputs']))
    if len(anchors)<2:return None
    anchored={(a['reported']['start'],a['reported']['end']) for a in anchors}
    if any(not any((r['start'],r['end']) in anchored for r in t['inputs']) for t in (current,prior)):return None
    selection=dict(method='reconciled_bank_reported_total_continuity_v1',
        superseded_last_period=str(old_end),superseded_concepts=sorted({r['tag'] for r in ordinary.values()}),
        current_report=dict(end=str(report),filed=str(filed),accession=acc,form=form),
        annual_anchors=anchors,basis_spliced=False)
    return {key:{**row,'revenue_selection':selection} if row else None for key,row in combined.items()}
