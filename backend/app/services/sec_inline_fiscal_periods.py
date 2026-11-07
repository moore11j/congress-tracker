"""Printed fiscal columns corroborated by issuer-wide inline XBRL periods.

Only primary-income validation supplies these already verified contexts. The
accepted amounts must separately share the exact context period and reconcile
in the same printed column. Untagged statement readers retain calendar rules.
"""
from datetime import date
import re
from app.services import sec_tabular_financials as tables


def shared_annual_header(header, rows):
    """Corroborate a lone printed annual heading with each column's XBRL period."""
    durations={tables.clean(c['text']).lower() for row in rows for c in row
               if re.search(r'\b(?:months?|years?|quarters?|weeks?|periods?)\b',c['text'],re.I)}
    if (len(durations)==1 and next(iter(durations)) in
            {'year ended','years ended','for the year ended','for the years ended'}
            and not re.search(r'\b(?:months?|years?|quarters?|weeks?|periods?)\b',header,re.I)):
        return next(iter(durations))+' '+header
    return header


def resolve(header,kind,filed,periods):
    def held():raise tables.TabularFinancialError('inline_fiscal_period_unproved')
    if kind!='income':held()
    years=set(re.findall(r'\b(?:19|20)\d{2}\b',header))
    dates={(tables.MONTHS[m.lower()],int(d)) for m,d in re.findall(
        r'\b('+tables.MONTH_PATTERN+r')\s*,?\s*(\d{1,2})\b',header,re.I)}
    if len(years)!=1 or len(dates)!=1:held()
    month,day=next(iter(dates))
    try:end=date(int(next(iter(years))),month,day)
    except ValueError:held()
    if end>filed:held()
    durations={tables.DURATIONS[n.lower()] for n in re.findall(
        r'\b(three|six|nine|twelve|3|6|9|12)\s+months?\s+ended',header,re.I)}
    if re.search(r'\byears?\s+ended\b',header,re.I):
        if re.search(r'\b(?:[0-9]+|one|two|three|four|five|six|seven|eight|nine|ten|twelve)\s+years?\s+ended\b',header,re.I):held()
        durations.add(12)
    if len(durations)!=1:held()
    months=next(iter(durations));low,high={3:(75,105),6:(165,195),9:(255,285),12:(350,380)}[months]
    candidates=set()
    for start_text,end_text in periods:
        try:start,finish=date.fromisoformat(start_text),date.fromisoformat(end_text)
        except (ValueError,TypeError):held()
        if finish==end and low<=(finish-start).days+1<=high:candidates.add((start,finish))
    if len(candidates)!=1:held()
    start,finish=next(iter(candidates))
    return start,finish,months
