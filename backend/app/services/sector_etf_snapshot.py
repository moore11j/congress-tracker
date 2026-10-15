"""Explicit EOD sector ETF price returns, never an all-company sector average."""
import math
import os
from sqlalchemy import select
from app.models import AppSetting, PriceCache

VERSION = 'sector_etf_eod_v1'
SOURCE = 'eod_etf_proxy'
TARGETS = (('Technology','XLK'),('Financials','XLF'),('Energy','XLE'),
    ('Health Care','XLV'),('Consumer Discretionary','XLY'),('Consumer Staples','XLP'),
    ('Industrials','XLI'),('Utilities','XLU'),('Materials','XLB'),
    ('Real Estate','XLRE'),('Communication Services','XLC'))
CURSOR_KEY = 'sector_etf_warming_cursor_v1'


def selected():
    return os.getenv('INSIGHTS_SECTOR_PROVIDER','fmp').strip().lower() == 'etf_proxy'


def snapshot_kind(kind):
    return f'{kind}:sectors:{VERSION}' if kind == 'macro-snapshot' and selected() else kind


def _positive(value):
    try:
        return type(value) in (int,float) and math.isfinite(value) and value > 0
    except OverflowError:
        return False


def build(db, *, now=None):
    from app.services.price_lookup import get_expected_latest_market_date, previous_market_trading_day
    end=get_expected_latest_market_date(now)
    prior=previous_market_trading_day(end)
    dates=(prior.isoformat(),end.isoformat())
    rows=db.execute(select(PriceCache).where(PriceCache.symbol.in_([symbol for _,symbol in TARGETS]),
        PriceCache.date.in_(dates))).scalars().all()
    prices={(row.symbol,row.date):row for row in rows}
    items=[]
    for sector,symbol in TARGETS:
        left,right=(prices.get((symbol,day)) for day in dates)
        item={'sector':f'{sector} ({symbol} ETF proxy)','symbol':symbol,'change_pct':None,
            'date':dates[1],'previous_date':dates[0],'unit_label':'%', 'source':SOURCE,
            'is_proxy':True,'methodology':VERSION,'return_basis':'split_adjusted_price_return',
            'timeframe_label':'Completed-session price change; excludes dividends',
            'status':'unavailable','reason':'missing_completed_sessions'}
        if left is not None and right is not None:
            if (any(row.adjustment_status != 'split_adjusted_price_return' or not _positive(row.close)
                    or row.provider_symbol != symbol or not row.price_source for row in (left,right))
                    or left.price_source != right.price_source):
                item['reason']='unverified_or_mixed_price_basis'
            else:
                value=(right.close/left.close-1)*100
                if math.isfinite(value):
                    item.update(change_pct=value,status='ok',reason=None,price_source=right.price_source,
                        previous_close=left.close,close=right.close)
                else:
                    item['reason']='nonfinite_return'
        items.append(item)
    available=sum(item['status']=='ok' for item in items)
    coverage={'status':'ok' if available==len(TARGETS) else 'partial' if available else 'unavailable',
        'source':SOURCE,'methodology':VERSION,'expected_count':len(TARGETS),'available_count':available,
        'missing_count':len(TARGETS)-available,'date':dates[1],'previous_date':dates[0]}
    return items,coverage


def warm(db, *, now=None):
    """At most two existing price hydrations per background refresh; fair retries."""
    from app.request_priority import get_request_context
    if (not selected() or os.getenv('INSIGHTS_SECTOR_WARMING_ENABLED','false').lower() != 'true'
            or str((get_request_context() or {}).get('path') or '').startswith('/api/')):
        return {'status':'disabled','attempted':0}
    from app.services.price_lookup import hydrate_split_adjusted_ohlc
    items,_=build(db,now=now)
    missing={item['symbol']:item for item in items if item['status']!='ok'}
    cursor=db.get(AppSetting,CURSOR_KEY)
    symbols=[symbol for _,symbol in TARGETS]
    offset=(symbols.index(cursor.value)+1)%len(symbols) if cursor and cursor.value in symbols else 0
    ordered=symbols[offset:]+symbols[:offset]
    result={'status':'ok','attempted':0,'failed':0}
    for symbol in [value for value in ordered if value in missing][:2]:
        item=missing[symbol]
        try:
            hydrate_split_adjusted_ohlc(db,symbol,item['previous_date'],item['date'])
        except Exception:
            db.rollback()
            result['failed']+=1
        cursor=db.get(AppSetting,CURSOR_KEY)
        if cursor is None:
            cursor=AppSetting(key=CURSOR_KEY)
            db.add(cursor)
        cursor.value=symbol
        db.commit()
        result['attempted']+=1
    return result
