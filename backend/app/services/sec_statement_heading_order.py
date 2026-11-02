"""Bounded statement heading grammar for otherwise empty qualified originals."""
import re

POLICY='explicit_statement_heading_order_v1'
CAPTION=r'consolidated (?:(?:unaudited|audited|interim|condensed) ){0,4}(?:balance sheets?|statements? of|income statements?)'


def normalized_heading(text):
    # Remove only known title modifiers, never arbitrary intervening prose.
    text=re.sub(r'\bconsolidated (?:(?:unaudited|audited|interim|condensed) ){1,4}',
                'consolidated ',text,flags=re.I)
    return re.sub(r'\bconsolidated income statements?\b',
                  'consolidated statements of income',text,flags=re.I)


def has_heading(tree):
    from app.services.sec_tabular_financials import visible_text,table_kind
    nodes=tree.xpath('//*[self::p or self::div or self::td or self::th]'
        '[not(ancestor::blockquote)][not(.//table)][not(.//p)][not(.//div)]')
    return any(len(text)<=240 and table_kind(text,expanded=True) is not None
               for node in nodes if (text:=visible_text(node)))


def additional_metric(label,kind):
    # Exact statement labels only. Consolidated profit is not parent income;
    # acquisitions including investee interests are not capital expenditure.
    if kind=='cashflow':
        return {'net cash generated from operating activities':'operating_cash_flow'}.get(label)
    if kind!='income':return None
    return {
        'profit (loss) for the period':'consolidated_income',
        'profit for the period':'consolidated_income',
        'loss for the period':'consolidated_income',
        'operating profit (loss)':'operating_income',
        'operating profit':'operating_income',
        'results from operating activities':'operating_income',
        'income from voyages and related services':'revenue',
    }.get(label)
