"""Evidence of disjoint reported borrowings and finance lease liabilities.

Bounded original qualification and exact observation binding. No storage or
publication. A complete instrument sum must independently cover gross
borrowings; a missing lease disclosure never implies zero. The existing debt
policy independently validates the resulting candidate before selection.
"""
from decimal import Decimal
from copy import deepcopy
import re
from lxml import html

VERSION = 'sec_disjoint_borrowing_lease_evidence_v1'
INSTRUMENTS = {'us-gaap:SeniorNotes', 'us-gaap:CommercialPaper',
               'us-gaap:LongtermCommercialPaperCurrentAndNoncurrent'}
TOTAL = 'us-gaap:DebtLongtermAndShorttermCombinedAmount'
LEASES = {'current': 'us-gaap:FinanceLeaseLiabilityCurrent',
          'noncurrent': 'us-gaap:FinanceLeaseLiabilityNoncurrent'}
LOCATIONS = {'current': {'accrued expenses and other', 'other current liabilities', 'accrued liabilities'},
             'noncurrent': {'other long-term liabilities', 'other noncurrent liabilities', 'other non-current liabilities'}}


class LeaseDebtEvidenceError(ValueError):
    pass


def _text(node):
    return ' '.join(node.text_content().replace('\u200b', ' ').split())


def _contexts(tree, *, cik, end):
    from app.services.sec_inline_financials import nodes
    result = {}
    for node in nodes(tree, 'context'):
        identifiers = nodes(node, 'identifier')
        instants = nodes(node, 'instant')
        if (len(identifiers) != 1 or identifiers[0].get('scheme') != 'http://www.sec.gov/CIK'
                or identifiers[0].text_content().strip().zfill(10) != cik
                or len(instants) != 1 or instants[0].text_content().strip() != end
                or nodes(node, 'startdate') or nodes(node, 'enddate') or nodes(node, 'scenario')):
            continue
        segments = nodes(node, 'segment')
        member = None
        if segments:
            if len(segments) != 1:
                continue
            children = list(segments[0])
            if (len(children) != 1 or children[0].tag != 'xbrldi:explicitmember'
                    or tree.get('xmlns:xbrldi') != 'http://xbrl.org/2006/xbrldi'
                    or children[0].get('dimension') != 'us-gaap:DebtInstrumentAxis'):
                continue
            member = children[0].text_content().strip()
            prefix, _, name = member.partition(':')
            if not tree.get('xmlns:'+prefix) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.-]*', name):
                continue
        result[node.get('id')] = member
    return result


def _facts(node, tree, contexts, usd_units, *, tags, segmented):
    from app.services.sec_inline_financials import _number, _nil_fact
    result = []
    for fact in node.iter('ix:nonfraction'):
        context = fact.get('contextref')
        if fact.get('name') not in tags or context not in contexts:
            continue
        member = contexts[context]
        if segmented is not None and bool(member) != segmented:
            continue
        if fact.get('unitref') not in usd_units or _nil_fact(fact):
            raise LeaseDebtEvidenceError('invalid_monetary_component')
        value = _number(fact, tree)
        if value < 0:
            raise LeaseDebtEvidenceError('negative_component')
        result.append({'tag': fact.get('name'), 'context_id': context, 'member': member, 'value': value})
    return result


def qualify(raw, *, cik, accession, filed, report_date, form, url):
    from app.services.sec_inline_financials import parse, nodes
    from app.services.sec_statement_currency import source_identity
    cik = str(cik).zfill(10)
    if source_identity(url) != (cik, accession):
        raise LeaseDebtEvidenceError('source_identity')
    # Reuse the complete original's namespace, issuer, filing-period, numeric,
    # duplicate-fact and source-integrity validation before interpreting tables.
    parsed = parse(raw, cik=cik, accession=accession, filed=filed,
                   report_date=report_date, form=form, url=url)
    tree = html.fromstring(raw)
    contexts = _contexts(tree, cik=cik, end=report_date)
    usd_units = set()
    for unit in nodes(tree, 'unit'):
        measures = nodes(unit, 'measure')
        if len(measures) == 1 and measures[0].text_content().strip() == 'iso4217:USD' and not nodes(unit, 'divide'):
            usd_units.add(unit.get('id'))
    borrowing_proofs, lease_proofs = [], []
    for table in tree.xpath('//table'):
        if len(_text(table)) > 16000 or table.xpath('.//table'):
            continue
        rows = table.xpath('./tr|./tbody/tr')
        totals = _facts(table, tree, contexts, usd_units, tags={TOTAL}, segmented=False)
        if totals:
            if len(totals) != 1:
                raise LeaseDebtEvidenceError('ambiguous_borrowing_total')
            total = totals[0]
            if total['value'] <= 0:
                raise LeaseDebtEvidenceError('nonpositive_borrowing_total')
            components = []
            found_total = False
            for row in rows:
                row_total = _facts(row, tree, contexts, usd_units, tags={TOTAL}, segmented=False)
                if row_total:
                    # Keep principal and carrying amount distinct. This proof
                    # qualifies only the explicitly labelled gross total.
                    label = _text(row).lower()
                    if not label.startswith('total debt before discounts and debt issuance costs'):
                        raise LeaseDebtEvidenceError('unqualified_borrowing_basis')
                    found_total = True
                    break
                components.extend(_facts(row, tree, contexts, usd_units, tags=INSTRUMENTS, segmented=None))
            members = [(p['tag'], p['member']) for p in components]
            instruments = [p['member'] for p in components if p['member'] is not None]
            if (not found_total or len(components) < 2 or len(components) > 100
                    or len(set(members)) != len(members)
                    or len(set(instruments)) != len(instruments)):
                raise LeaseDebtEvidenceError('incomplete_or_duplicate_instruments')
            if sum(Decimal(str(p['value'])) for p in components) != Decimal(str(total['value'])):
                raise LeaseDebtEvidenceError('instruments_do_not_cover_total')
            borrowing_proofs.append({'basis': 'reported gross principal before discounts and debt issuance costs',
                                    'total': total, 'components': components})
        if not re.search(r'\bclassification\b', _text(table), re.I):
            continue
        located = {}
        for kind, tag in LEASES.items():
            for row in rows:
                facts = _facts(row, tree, contexts, usd_units, tags={tag}, segmented=False)
                if not facts:
                    continue
                if kind in located or len(facts) != 1:
                    raise LeaseDebtEvidenceError('ambiguous_lease_classification')
                cells = [_text(cell) for cell in row.xpath('./td|./th') if _text(cell)]
                if len(cells) < 3 or cells[0].lower() != 'finance' or cells[1].lower() not in LOCATIONS[kind]:
                    raise LeaseDebtEvidenceError('lease_not_separately_classified')
                located[kind] = {'location': cells[1], 'fact': facts[0]}
        if located:
            if set(located) != set(LEASES):
                raise LeaseDebtEvidenceError('incomplete_lease_maturities')
            lease_proofs.append(located)
    if len(borrowing_proofs) != 1 or len(lease_proofs) != 1:
        raise LeaseDebtEvidenceError('complete_disjoint_disclosure_unavailable')
    borrowings = borrowing_proofs[0]
    leases = lease_proofs[0]
    lease_total = sum(Decimal(str(item['fact']['value'])) for item in leases.values())
    # This original-derived evidence never fills a missing canonical ratio.
    return {'version': VERSION, 'status': 'qualified_evidence', 'cik': cik,
            'accession': accession, 'filed': filed, 'period_date': report_date,
            'source_url': url, 'source_sha256': parsed['evidence']['sha256'], 'unit': 'USD',
            'borrowings': borrowings, 'finance_leases': leases, 'finance_lease_total': float(lease_total),
            'gross_borrowings_plus_finance_leases': float(Decimal(str(borrowings['total']['value']))+lease_total),
            'canonical_selection': False,
            'limitation': 'Original borrowing/lease separation only; does not select leverage, prove current coverage or infer absent disclosures as zero.'}


NAMES = ('leverage_borrowings', 'leverage_finance_leases_current', 'leverage_finance_leases_noncurrent')


def acquire(payload, *, company, available_by, end, forms, cik, client):
    """One optional original, only for an otherwise complete lease addition."""
    from app.services import sec_fundamentals as sec, sec_leverage as leverage
    if any(str(source.get('cik', '')).zfill(10) != str(cik).zfill(10) for source in (payload, company)):
        return None
    if not 0 <= (available_by-end).days <= 180:
        return None
    instant = {name: sec._records(payload, 'equity', available_by, concepts=tags,
        forms=forms).get((None, end)) for name, tags in leverage.INSTANT.items()}
    points = [instant[name] for name in NAMES]
    if (not all(points) or not leverage._compatible(points)
            or any(not leverage._finite(p['value']) or p['value'] < 0 for p in points)
            or instant['leverage_debt_with_finance_leases']
            or (instant['leverage_current_debt'] and instant['leverage_noncurrent_debt_with_finance_leases'])):
        return None
    recent = company.get('filings', {}).get('recent', {})
    arrays = [recent.get(k, []) for k in ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')]
    if len({len(a) for a in arrays}) != 1:
        return None
    matches = {(acc, form, filed, report, filename) for acc, form, filed, report, filename in zip(*arrays)
        if acc == points[0]['accession'] and filed == points[0]['filed'] and report == str(end)
        and form in forms and form in {'10-K', '10-Q', '10-K/A', '10-Q/A'}}
    if len(matches) != 1:
        return None
    accession, form, filed, report, filename = next(iter(matches))
    if (not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}', accession)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filename)):
        return None
    url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}'
    # Transport failures retain the collector's normal backoff. A document that
    # fails optional accounting qualification cannot erase valid core facts.
    raw = client.get(url)
    try:
        proof = qualify(raw, cik=cik, accession=accession, filed=filed, report_date=report, form=form, url=url)
    except ValueError:
        return None
    if not matches_points(proof, points):
        return None
    return {'debt_precision_only': True, 'facts': {}, 'provenance': {},
        'evidence': {'methodology': VERSION, 'cik': str(cik).zfill(10), 'accession': accession,
            'filed': filed, 'report_date': report, 'sha256': proof['source_sha256'],
            'disjoint_borrowing_leases': proof}}


def matches_points(proof, points):
    """Bind the complete original proof to three exact selected observations."""
    from app.services.sec_leverage import _compatible, _finite
    from app.services.sec_statement_currency import source_identity
    try:
        if (proof['version'] != VERSION or proof['status'] != 'qualified_evidence'
                or proof['unit'] != 'USD' or len(points) != 3 or not _compatible(points)
                or not re.fullmatch(r'[0-9a-f]{64}', proof['source_sha256'])
                or source_identity(proof['source_url']) != (proof['cik'], proof['accession'])):
            return False
        originals = [proof['borrowings']['total'], proof['finance_leases']['current']['fact'],
                     proof['finance_leases']['noncurrent']['fact']]
        for point, original, tag in zip(points, originals, (TOTAL, LEASES['current'], LEASES['noncurrent'])):
            if (point['tag'] != tag.split(':', 1)[1] or original['tag'] != tag
                    or point['start'] is not None or point['end'] != proof['period_date']
                    or point['filed'] != proof['filed'] or point['accession'] != proof['accession']
                    or point['unit'] != 'USD' or point.get('source_cik', proof['cik']) != proof['cik']
                    or not _finite(point['value']) or point['value'] < 0 or point['value'] != original['value']
                    or original['member'] is not None or not original['context_id']):
                return False
        components = proof['borrowings']['components']
        keys = [(p['tag'], p['member']) for p in components]
        members = [p['member'] for p in components if p['member'] is not None]
        if (not 2 <= len(components) <= 100 or len(set(keys)) != len(keys) or len(set(members)) != len(members)
                or any(p['tag'] not in INSTRUMENTS or not p['context_id']
                    or not _finite(p['value']) or p['value'] < 0 for p in components)
                or sum(Decimal(str(p['value'])) for p in components) != Decimal(str(points[0]['value']))
                or proof['borrowings']['basis'] != 'reported gross principal before discounts and debt issuance costs'
                or any(proof['finance_leases'][kind]['location'].lower() not in LOCATIONS[kind] for kind in LEASES)):
            return False
        total = sum(Decimal(str(p['value'])) for p in points[1:])
        return (_finite(proof['finance_lease_total']) and _finite(proof['gross_borrowings_plus_finance_leases'])
            and Decimal(str(proof['finance_lease_total'])) == total
            and Decimal(str(proof['gross_borrowings_plus_finance_leases'])) == Decimal(str(points[0]['value']))+total)
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def annotate(records, supplement):
    from datetime import date
    proof = supplement.get('evidence', {}).get('disjoint_borrowing_leases')
    if not isinstance(proof, dict):
        return records
    try:
        evidence = supplement['evidence']
        if (any(proof[k] != evidence[k] for k in ('cik', 'filed', 'accession'))
                or proof['source_sha256'] != evidence['sha256'] or proof['period_date'] != evidence['report_date']):
            return records
        end = date.fromisoformat(proof['period_date'])
        points = [records.get(name, {}).get((None, end)) for name in NAMES]
        if matches_points(proof, points):
            points[0]['disjoint_lease_evidence'] = deepcopy(proof)
    except (KeyError, TypeError, ValueError):
        pass
    return records


def reconciliation(instant):
    points = [instant.get(name) for name in NAMES]
    proof = (points[0] or {}).get('disjoint_lease_evidence')
    total = instant.get('leverage_finance_leases')
    from app.services.sec_leverage import _compatible, _finite
    if (not isinstance(proof, dict) or not matches_points(proof, points) or not total
            or not _compatible([*points, total]) or not _finite(total['value'])
            or total['value'] != proof['finance_lease_total']):
        return None
    return {'status': 'original_disjoint_components_verified', 'components': ['leverage_borrowings', 'leverage_finance_leases'],
        'inputs': [points[0], total], 'original_inputs': points, 'sum': proof['gross_borrowings_plus_finance_leases'],
        'requirement': 'Complete original gross borrowing instrument sum and separately classified current/noncurrent finance leases.'}


def verifies_candidate(row, check):
    try:
        points = check['original_inputs']
        if len(points) != 3 or row['debt_inputs'] != check['inputs'] or len(row['debt_inputs']) != 2:
            return False
        instant = dict(zip(NAMES, points))
        instant['leverage_finance_leases'] = row['debt_inputs'][1]
        rebuilt = reconciliation(instant)
        return rebuilt is not None and rebuilt == check and row['debt'] == rebuilt['sum']
    except (KeyError, TypeError, ValueError, IndexError):
        return False
