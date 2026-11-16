"""Prove a whole revenue history against exact primary-statement totals.

Including/excluding-tax tags are never assumed interchangeable. Each selected
period must equal a reported entity-wide primary total in the same original.
"""
from app.services import sec_revenue_series as series
from app.services.sec_revenue_statement_layout import TITLE, _visible, heading_valid
import re

POLICY='sec_primary_total_tax_tag_equality_v1'


def heading(table):
    # A single table wrapper immediately follows the title block. Do not scan
    # past notes, unrelated content or another table to borrow an earlier title.
    if not _visible(table):return []
    wrapper=table.getparent()
    if wrapper is None or wrapper.tag!='div' or list(wrapper)!=[table]:return []
    title=wrapper.getprevious()
    if title is None or title.tag!='div' or not _visible(title) or any(n.tag=='ix:nonfraction' for n in title.iter()):
        return []
    title_tables=title.xpath('.//table')
    if len(title_tables)>1 or (title_tables and len(title_tables[0].xpath('.//tr'))>2):return []
    full=' '.join(title.text_content().split())
    if len(full)>400 or re.search(r'notes to|guarantor|non.?gaap|adjusted|pro.?forma|reconciliation',full,re.I):return []
    matches=[' '.join(n.text_content().split()) for n in title.iter('span')
             if _visible(n) and not n.xpath('.//span') and TITLE.fullmatch(' '.join(n.text_content().split()))]
    return matches if len(matches)==1 else []


def valid(proof, *, point):
    reconciliation=proof.get('tax_tag_reconciliation') or {}
    return (reconciliation.get('policy')==POLICY
        and reconciliation.get('selected_concept')==series.INCLUDING
        and reconciliation.get('primary_concept') in {series.CONTRACT,series.INCLUDING}
        and point.get('tag')==series.INCLUDING
        and all(reconciliation.get(k)==point.get(k)==proof.get(k)
                for k in series.IDENTITY+('start','end'))
        and proof.get('row_label') in series.LABELS-{'revenue from services'}
        and heading_valid(proof.get('statement_heading')))


def statement_rows(raw, filing, parsed):
    selected={(p.get('start'),p['end']):p for p in
              parsed['facts'].get(series.INCLUDING,{}).get('units',{}).get('USD',[])}
    accepted={}
    for concept in (series.INCLUDING,series.CONTRACT):
        for key,primary in series.statement_rows(raw,filing,parsed,concept=concept,heading_resolver=heading).items():
            target=selected.get(key)
            if target is None or primary['row_label']=='revenue from services':continue
            if (target['val']!=primary['value'] or target['filed']!=primary['filed']
                    or target['accn']!=primary['accession']):
                raise ValueError('revenue_tax_tag_primary_conflict')
            proof={**primary,'tax_tag_reconciliation':dict(policy=POLICY,
                selected_concept=series.INCLUDING,primary_concept=concept,
                **{k:primary[k] for k in series.IDENTITY+('start','end')})}
            if key in accepted and any(accepted[key][k]!=proof[k] for k in series.IDENTITY):
                raise ValueError('revenue_tax_tag_primary_conflict')
            accepted.setdefault(key,proof)
    return accepted
