"""Frozen prospective challenger and grading. Pure local research functions."""
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
import math
from statistics import mean
from conviction_features import build_trades
from options_activity_pilot import ny_timezone

PROVIDERS=['fmp:historical-price-eod/full+corporate_actions','massive:grouped-daily-adjusted']
ENROLL_END='2026-10-15'
REPORT_DATE='2026-11-17'


def stamp(value):
    d=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('A real timezone-aware timestamp is required')
    return d


def session_on_or_after(day):
    d=date.fromisoformat(day)
    if not date(2026,9,15)<=d<=date(2026,11,17):raise ValueError('Outside verified study calendar')
    while d.weekday()>=5:d+=timedelta(days=1)
    return d.isoformat()


def prepare_trades(payload,capture):
    today=stamp(capture).astimezone(ny_timezone()).date().isoformat()
    raw=payload['trade_inputs']
    # Date-only filings become usable the next local calendar day. Also removes
    # impossible future-dated records before prior-purchase histories are built.
    def before(r,key):return str(r.get(key) or '')[:10]<today and bool(r.get(key))
    filtered={'events':[r for r in raw['events'] if before(r,'filing_date')],
              'normalized':[r for r in raw['normalized'] if before(r,'filing_date')]}
    return build_trades(filtered,payload['security_types'])


def challenger(ticker,direction,trades,capture):
    if direction.lower()!='bullish':return {'keep':True,'status':'unchanged_nonbullish'}
    today=stamp(capture).astimezone(ny_timezone()).date()
    start=(today-timedelta(days=90)).isoformat()
    rr=[r for r in trades if r['source']=='insider' and r['ticker']==ticker and start<=r['filing']<today.isoformat()]
    buys=[r for r in rr if r['side']=='buy' and r['actor'] and r['unusual_ratio'] is not None and r['prior_purchase_count']>=3]
    if not buys or not all(r['low'] is not None and r['high'] is not None for r in rr):
        return {'keep':True,'status':'unknown_fallback','known_buyer_filings':len(buys)}
    net=sum(r['low'] for r in rr if r['side']=='buy')-sum(r['high'] for r in rr if r['side']=='sell')
    ratio=max(r['unusual_ratio'] for r in buys)
    passed=net>0 and ratio>=2
    return {'keep':passed,'status':'qualified' if passed else 'filtered','net_known_dollars_90d':net,
            'max_purchase_ratio':ratio,'known_buyer_filings':len(buys),
            'evidence':[r for r in buys if r['unusual_ratio']>=2]}


def new_decisions(payload,seen,start,recorded_at,trades):
    capture=stamp(payload['captured_at']);recorded=stamp(recorded_at)
    if recorded<capture or (recorded-capture).total_seconds()>3600:
        raise ValueError('Capture must be processed within one hour, without backdating')
    localday=recorded.astimezone(ny_timezone()).date()
    accepted=[];excluded=Counter()
    for event in payload['confirmations']:
        a=event['anchor'];key=str(a['id'])
        if key in seen:continue
        reason=None
        if localday.isoformat()>ENROLL_END:reason='enrollment_closed'
        elif stamp(a['created_at'])<stamp(start) or stamp(a['calculated_at'])<stamp(start):reason='older_or_backfilled_event'
        elif stamp(a['created_at'])>capture or stamp(a['calculated_at'])>capture:reason='future_timestamp'
        elif event['closed_at'] is not None:reason='already_closed'
        elif a['direction'].lower() not in ['bullish','bearish']:reason='nondirectional'
        if reason:excluded[reason]+=1;continue
        entry=session_on_or_after((localday+timedelta(days=1)).isoformat())
        targets={str(h):session_on_or_after((date.fromisoformat(entry)+timedelta(days=h)).isoformat()) for h in [7,30]}
        accepted.append({'id':key,'ticker':a['ticker_at_time'],'direction':a['direction'].lower(),
                         'captured_at':payload['captured_at'],'recorded_at':recorded_at,'entry_date':entry,'targets':targets,
                         'baseline_keep':True,'challenger':challenger(a['ticker_at_time'],a['direction'],trades,payload['captured_at']),
                         'original_confirmation':event})
    return accepted,dict(excluded)


def price_books(rows):
    books={p:defaultdict(dict) for p in PROVIDERS}
    for r in rows:
        if r[6] in books and all(isinstance(r[i],(int,float)) and math.isfinite(r[i]) and r[i]>0 for i in [2,3,4]):
            books[r[6]][r[0]][r[1]]=r
    return books


def measure(decision,h,books,asof):
    entry=decision['entry_date'];target=decision['targets'][str(h)]
    close=datetime.combine(date.fromisoformat(target),time(16),ny_timezone())
    if stamp(asof)<close:return {'status':'immature','entry_date':entry,'target_date':target}
    for provider in PROVIDERS:
        stock=books[provider].get(decision['ticker'],{});spy=books[provider].get('SPY',{})
        if not all(d in b for b in [stock,spy] for d in [entry,target]):continue
        e=stock[entry];b=spy[entry];ep=e[4]*e[2]/e[3];bp=b[4]*b[2]/b[3]
        raw=100*(stock[target][2]/ep-1);br=100*(spy[target][2]/bp-1);excess=round(raw-br,2)
        sign=-1 if decision['direction']=='bearish' else 1
        dr=round(-raw,2) if sign<0 else raw;de=round(-excess,2) if sign<0 else excess
        return {'status':'measured','entry_date':entry,'target_date':target,'provider':provider,
                'raw_return':raw,'spy_return':br,'directional_return':dr,'directional_excess':de,
                'correct':dr>0 or de>0,'raw_correct':dr>0,'measured_at':asof,
                'price_evidence':{'stock_entry':e,'stock_target':stock[target],'spy_entry':b,'spy_target':spy[target]}}
    return {'status':'missing_consistent_prices','entry_date':entry,'target_date':target}


def summary(decisions,outcomes,h,challenger_only=False):
    eligible=[d for d in decisions if outcomes[d['id']][str(h)]['status']=='measured']
    retained=[d for d in eligible if not challenger_only or d['challenger']['keep']]
    rr=[outcomes[d['id']][str(h)] for d in retained];n=len(rr)
    from analyze_disclosure_triggers import wilson
    wins=sum(r['correct'] for r in rr)
    return {'eligible_measured':len(eligible),'retained':n,'correct':wins,'accuracy':100*wins/n if n else None,
            'coverage':100*n/len(eligible) if eligible else None,'raw_positive_accuracy':100*sum(r['raw_correct'] for r in rr)/n if n else None,
            'average_directional_return':mean(r['directional_return'] for r in rr) if n else None,
            'average_excess':mean(r['directional_excess'] for r in rr) if n else None,
            'dates':dict(Counter(d['entry_date'] for d in retained)),'tickers':len({d['ticker'] for d in retained}),
            'wilson95_descriptive':wilson(wins,n),'feature_status':dict(Counter(d['challenger']['status'] for d in decisions)),
            'outcome_status':dict(Counter(outcomes[d['id']][str(h)]['status'] for d in decisions))}
