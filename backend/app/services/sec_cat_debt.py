"""CAT debt from the complete consolidated balance-sheet liability rows.

The industrial and finance rows are disjoint within this statement. Validate
both liability subtotals before deriving debt; missing components never mean zero.
Generic companyfacts/inline readers continue to exclude segment observations.
"""
from copy import deepcopy
from datetime import date
import hashlib
import math
import re
from lxml import html
from app.services import sec_inline_financials as inline

VERSION = 'sec_cat_balance_debt_v1'
CIK = '0000018230'
FP = 'cat:FinancialProductsMember'
IND = 'cat:MachineryPowerEnergyMember'
CURRENT_TAG = 'us-gaap:LongTermDebtAndCapitalLeaseObligationsCurrent'
LONG_TAG = 'us-gaap:LongTermDebtAndCapitalLeaseObligations'
SHORT_TAG = 'us-gaap:ShortTermBorrowings'
# Exact complete liabilities section, in statement order. Unknown/new rows hold
# the adapter for review instead of disappearing from an apparently complete sum.
LAYOUT = (
    (SHORT_TAG, FP, 'Financial Products'),
    ('us-gaap:AccountsPayableCurrent', None, 'Accounts payable'),
    ('us-gaap:AccruedLiabilitiesCurrent', None, 'Accrued expenses'),
    ('us-gaap:EmployeeRelatedLiabilitiesCurrent', None, 'Accrued wages, salaries and employee benefits'),
    ('us-gaap:ContractWithCustomerLiabilityCurrent', None, 'Customer advances'),
    ('us-gaap:DividendsPayableCurrent', None, 'Dividends payable'),
    ('us-gaap:OtherLiabilitiesCurrent', None, 'Other current liabilities'),
    (CURRENT_TAG, IND, 'Machinery, Power & Energy'),
    (CURRENT_TAG, FP, 'Financial Products'),
    ('us-gaap:LiabilitiesCurrent', None, 'Total current liabilities'),
    (LONG_TAG, IND, 'Machinery, Power & Energy'),
    (LONG_TAG, FP, 'Financial Products'),
    ('us-gaap:PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent', None, 'Liability for postemployment benefits'),
    ('us-gaap:OtherLiabilitiesNoncurrent', None, 'Other liabilities'),
    ('us-gaap:Liabilities', None, 'Total liabilities'),
)
NAMES = ('leverage_current_debt', 'leverage_noncurrent_debt_with_finance_leases')


class CatDebtError(ValueError):
    pass


def parse(raw, source):
    if hashlib.sha256(raw).hexdigest() != source['sha256']:
        raise CatDebtError('source_hash')
    inline.parse(raw, cik=CIK, accession=source['accession'], filed=source['filed'],
                 report_date=source['period'], form=source['form'], url=source['url'])
    tree = html.fromstring(raw)
    if (tree.get('xmlns:cat') != 'http://www.cat.com/'+source['period'].replace('-', '')
            or tree.get('xmlns:xbrldi') != 'http://xbrl.org/2006/xbrldi'
            or not re.fullmatch(r'http://fasb\.org/srt/[0-9]{4}', tree.get('xmlns:srt', ''))):
        raise CatDebtError('dimension_namespaces')
    contexts = {n.get('id'): n for n in inline.nodes(tree, 'context')}
    units = {n.get('id'): n for n in inline.nodes(tree, 'unit')}
    tables = [t for t in tree.xpath('//table') if
              {'us-gaap:Assets', 'us-gaap:LiabilitiesCurrent', CURRENT_TAG} <=
              {n.get('name') for n in inline.nodes(t, 'nonfraction')}]
    if len(tables) != 1:
        raise CatDebtError('ambiguous_balance_statement')
    table = tables[0]
    statement = ' '.join(table.text_content().split())
    if any(label not in statement for label in ('Short-term borrowings:',
            'Long-term debt due within one year:', 'Long-term debt due after one year:')):
        raise CatDebtError('debt_maturity_labels')

    def fact(node, member, label):
        context = contexts[node.get('contextref')]
        identifiers = inline.nodes(context, 'identifier')
        if (len(identifiers) != 1 or identifiers[0].text_content().strip() != CIK
                or identifiers[0].get('scheme') != 'http://www.sec.gov/CIK'):
            raise CatDebtError('issuer_context')
        instant = inline.nodes(context, 'instant')
        if (len(instant) != 1 or instant[0].text_content() != source['period']
                or inline.nodes(context, 'startdate') or inline.nodes(context, 'enddate')
                or inline.nodes(context, 'scenario')):
            raise CatDebtError('instant_scope')
        members = [n for n in context.iter() if str(n.tag).lower() == 'xbrldi:explicitmember']
        segments = inline.nodes(context, 'segment')
        if member is None:
            if members or segments:
                raise CatDebtError('consolidated_context')
        elif (len(members) != 1 or len(segments) != 1 or list(segments[0]) != members
              or list(members[0]) or members[0].get('dimension') != 'srt:ProductOrServiceAxis'
              or members[0].text_content().strip() != member):
            raise CatDebtError('disjoint_member')
        if any(str(n.tag).lower() == 'xbrldi:typedmember' for n in context.iter()):
            raise CatDebtError('typed_member')
        unit = units[node.get('unitref')]
        if ([n.text_content() for n in inline.nodes(unit, 'measure')] != ['iso4217:USD']
                or inline.nodes(unit, 'divide')):
            raise CatDebtError('unit')
        row = next(node.iterancestors('tr'))
        if not ' '.join(row.text_content().split()).startswith(label):
            raise CatDebtError('statement_label')
        value = inline._number(node, tree)
        if not math.isfinite(value) or value < 0:
            raise CatDebtError('invalid_liability')
        return dict(value=value, start=None, end=source['period'], unit='USD',
                    filed=source['filed'], accession=source['accession'], source_cik=CIK,
                    source_url=source['url'], source_sha256=source['sha256'],
                    tag=node.get('name').removeprefix('us-gaap:'), member=member,
                    fact_id=node.get('id'), context_id=node.get('contextref'))

    current = []
    for node in inline.nodes(table, 'nonfraction'):
        context = contexts[node.get('contextref')]
        instant = inline.nodes(context, 'instant')
        if len(instant) == 1 and instant[0].text_content() == source['period']:
            current.append(node)
    tags = [n.get('name') for n in current]
    if tags.count(SHORT_TAG) != 1 or tags.count('us-gaap:Liabilities') != 1:
        raise CatDebtError('liabilities_section')
    section = current[tags.index(SHORT_TAG):tags.index('us-gaap:Liabilities')+1]
    if [n.get('name') for n in section] != [t for t, _, _ in LAYOUT]:
        raise CatDebtError('incomplete_liabilities_layout')
    rows = [fact(n, member, label) for n, (_, member, label) in zip(section, LAYOUT)]
    if (sum(p['value'] for p in rows[:9]) != rows[9]['value']
            or sum(p['value'] for p in rows[9:14]) != rows[14]['value']):
        raise CatDebtError('liabilities_do_not_reconcile')
    cash_nodes = [n for n in current if n.get('name') == 'us-gaap:CashAndCashEquivalentsAtCarryingValue']
    if len(cash_nodes) != 1:
        raise CatDebtError('ambiguous_cash')
    cash = fact(cash_nodes[0], None, 'Cash and cash equivalents')
    parts = ([rows[i] for i in (0, 7, 8)], rows[10:12])
    derived = {}
    for name, inputs in zip(NAMES, parts):
        derived[name] = {**{k: inputs[0][k] for k in ('start', 'end', 'unit', 'filed',
            'accession', 'source_cik', 'source_url', 'source_sha256')},
            'tag': VERSION+'_'+name, 'value': sum(p['value'] for p in inputs),
            'derived': True, 'inputs': inputs,
            'formula': 'Complete disjoint consolidated balance-sheet debt rows; capital leases included in long-term debt tags.'}
    return {'methodology': VERSION, 'status': 'statement_verified', 'source': dict(source),
            'records': derived, 'cash': cash, 'liability_rows': rows,
            'current_liabilities_sum': rows[9]['value'], 'total_liabilities_sum': rows[14]['value']}


def apply(records, proof, *, available_by, allowed):
    """Require exact cash/source binding and retain every existing observation."""
    if not proof:
        return records, None
    evidence = {k: deepcopy(v) for k, v in proof.items() if k != 'records'}
    def held(reason):
        return records, {**evidence, 'status': 'held', 'reason': reason}
    if proof.get('status') != 'statement_verified' or proof.get('methodology') != VERSION:
        return held('unverified_statement')
    source = proof['source']
    if (source['filed'] > str(available_by) or
            (source['accession'],source['filed'],source['sha256'],source['url']) not in allowed):
        return held('unverified_source')
    key = (None, date.fromisoformat(source['period']))
    if any(key in records.get(name, {}) for name in NAMES):
        return held('reported_debt_or_conflict_retained')
    cash = records.get('leverage_cash', {}).get(key)
    original = proof['cash']
    if (cash is None or any(cash.get(k) != original[k] for k in
            ('tag','value','start','end','unit','filed','accession'))
            or cash.get('source_cik', CIK) != CIK
            or any(k in cash and cash[k] != original[k] for k in ('source_url','source_sha256'))):
        return held('cash_original_mismatch')
    result = deepcopy(records)
    for name in NAMES:
        point = proof['records'][name]
        if (any(point.get(k) != original[k] for k in
                ('start','end','unit','filed','accession','source_cik','source_url','source_sha256'))
                or point.get('value') != sum(p['value'] for p in point.get('inputs', []))):
            return held('debt_original_mismatch')
        result.setdefault(name, {})[key] = deepcopy(point)
    result['leverage_cash'][key].update({k: original[k] for k in ('source_url','source_sha256','source_cik')})
    return result, {**evidence, 'status': 'prepared'}
